use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use fs_protocol::pb;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

mod admission;
mod events;
mod publication;
mod self_notify;

pub use admission::{InternalMetadataNotifier, StorageInvalidationGate, StorageRequestGuard};
pub use self_notify::InternalMetadataWrite;

use events::handle_notify_result;
#[cfg(test)]
use events::handle_storage_event;
use publication::{FullResyncWorker, InvalidationPublisher, NatsInvalidationPublisher};

const SELF_NOTIFY_TTL: Duration = Duration::from_secs(2);
const FULL_RESYNC_RETRY_INITIAL: Duration = Duration::from_millis(50);
const FULL_RESYNC_RETRY_MAX: Duration = Duration::from_secs(1);

pub struct StorageInvalidationWatcher {
    _watcher: RecommendedWatcher,
    _full_resync_worker: FullResyncWorker,
}

pub fn start_storage_invalidation_watcher<F>(
    connection: nats::Connection,
    mount: String,
    roots: Vec<PathBuf>,
    gate: StorageInvalidationGate,
    next_full_resync: F,
) -> Result<Option<StorageInvalidationWatcher>>
where
    F: Fn() -> Option<pb::Invalidation> + Send + 'static,
{
    start_watcher(
        watched_roots(roots),
        gate,
        NatsInvalidationPublisher { connection, mount },
        next_full_resync,
    )
}

fn start_watcher<P, F>(
    roots: Vec<PathBuf>,
    gate: StorageInvalidationGate,
    publisher: P,
    next_full_resync: F,
) -> Result<Option<StorageInvalidationWatcher>>
where
    P: InvalidationPublisher + Send + 'static,
    F: Fn() -> Option<pb::Invalidation> + Send + 'static,
{
    if roots.is_empty() {
        return Ok(None);
    }

    let full_resync_worker = FullResyncWorker::spawn(gate.clone(), publisher, next_full_resync);

    let mut watcher = notify::recommended_watcher(move |result| {
        if let Err(error) = handle_notify_result(&gate, result) {
            tracing::warn!(error = ?error, "storage watcher error");
        }
    })
    .context("create storage invalidation watcher")?;

    for root in &roots {
        watcher
            .watch(root, RecursiveMode::Recursive)
            .with_context(|| format!("watch storage root {}", root.display()))?;
    }

    Ok(Some(StorageInvalidationWatcher {
        _watcher: watcher,
        _full_resync_worker: full_resync_worker,
    }))
}

fn watched_roots(roots: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for root in roots {
        if seen.insert(root.clone()) {
            unique.push(root);
        }
    }
    unique
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabricfs_transport::{command_subject, FileSystemServer};
    use fs_core::{Dispatch, RpcMetadata};
    use fs_protocol::{
        encode_request, path as proto_path, InvalidationKind, Operation, RequestEnvelope,
        RequestPayload, ResponseEnvelope, ResponsePayload,
    };
    use notify::event::{AccessKind, AccessMode, DataChange, EventAttributes, ModifyKind};
    use notify::{Event, EventKind};
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn metadata_root() -> TempDir {
        TempDir::new().expect("metadata tempdir")
    }

    fn storage_path(root: &TempDir, relative: &str) -> PathBuf {
        root.path().join(relative.trim_start_matches('/'))
    }

    fn write_bytes(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("test metadata parent exists");
        }
        fs::write(path, bytes).expect("test metadata file exists");
    }

    fn create_dir(path: &Path) {
        fs::create_dir_all(path).expect("test metadata dir exists");
    }

    fn encoded_write_request(request_id: &str, namespace: &str) -> Vec<u8> {
        let request = RequestEnvelope::new(
            request_id,
            namespace,
            0,
            pb::TraceContext::default(),
            RequestPayload::Write(pb::WriteRequest {
                path: Some(proto_path("/file.txt").expect("fixture path is valid")),
                handle: 7,
                offset: 0,
                data: b"hello".to_vec(),
            }),
        )
        .expect("fixture request is valid");
        encode_request(&request).expect("fixture request encodes")
    }

    #[derive(Default)]
    struct BlockingDispatch {
        state: Mutex<BlockingDispatchState>,
        ready: Condvar,
    }

    #[derive(Default)]
    struct BlockingDispatchState {
        call_order: Vec<String>,
        first_call_blocked: bool,
        release_first_call: bool,
    }

    impl BlockingDispatch {
        fn wait_for_first_call_blocked(&self) {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut state = self.state.lock().expect("blocking dispatch lock");
            loop {
                if state.first_call_blocked {
                    return;
                }
                let now = Instant::now();
                assert!(now < deadline, "timed out waiting for first dispatch");
                let timeout = deadline.saturating_duration_since(now);
                state = match self.ready.wait_timeout(state, timeout) {
                    Ok((state, _)) => state,
                    Err(poisoned) => {
                        let (state, _) = poisoned.into_inner();
                        state
                    }
                };
            }
        }

        fn release_first_call(&self) {
            let mut state = self.state.lock().expect("blocking dispatch lock");
            state.release_first_call = true;
            self.ready.notify_all();
        }

        fn call_order(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("blocking dispatch lock")
                .call_order
                .clone()
        }
    }

    impl Dispatch for BlockingDispatch {
        fn dispatch_request(
            &self,
            request: RequestEnvelope,
            _metadata: RpcMetadata,
        ) -> ResponseEnvelope {
            let mut state = self.state.lock().expect("blocking dispatch lock");
            state.call_order.push(request.request_id.clone());
            if state.call_order.len() == 1 {
                state.first_call_blocked = true;
                self.ready.notify_all();
                while !state.release_first_call {
                    state = match self.ready.wait(state) {
                        Ok(state) => state,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                }
            }
            drop(state);

            let bytes_written = match &request.payload {
                RequestPayload::Write(write) => write.data.len() as u32,
                _ => 0,
            };
            ResponseEnvelope::success_for(
                &request,
                ResponsePayload::Write(pb::WriteResponse { bytes_written }),
                Vec::new(),
            )
            .expect("fixture response matches fixture request")
        }
    }

    #[test]
    fn storage_watcher_publishes_full_resync_for_mutating_events() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(7);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });

        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");

        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(
            invalidations[0].kind,
            InvalidationKind::FullResync.wire_value()
        );
        assert_eq!(invalidations[0].sequence, 7);
    }

    #[test]
    fn storage_watcher_ignores_read_only_access_events() {
        let gate = StorageInvalidationGate::new();

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Access(AccessKind::Read),
                paths: vec![PathBuf::from("/storage/file.txt")],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(!queued);
    }

    #[test]
    fn storage_watcher_publishes_full_resync_for_ambiguous_other_events() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Other,
                paths: Vec::new(),
                attrs: EventAttributes::new(),
            },
        )
        .expect("ambiguous watch event is handled");

        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(
            invalidations[0].kind,
            InvalidationKind::FullResync.wire_value()
        );
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_coalesces_events_seen_during_active_requests() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(1);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });
        let request = gate.start_request();

        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");

        assert!(queued);
        assert!(publisher.invalidations().is_empty());
        request.finish();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_retries_idle_full_resync_after_publish_failure() {
        let publisher = FailOnceThenRecordPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(1);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });

        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");

        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
        assert_eq!(publisher.attempts(), 2);
    }

    #[test]
    fn storage_watcher_retries_deferred_full_resync_after_publish_failure_without_follow_on_request(
    ) {
        let publisher = FailOnceThenRecordPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(1);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });
        let request = gate.start_request();

        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");

        assert!(queued);
        assert!(publisher.invalidations().is_empty());
        request.finish();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
        assert_eq!(publisher.attempts(), 2);
    }

    #[test]
    fn storage_watcher_fences_waiting_requests_until_failed_full_resync_is_delivered() {
        let publisher = FailOnceThenBlockRetryPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(1);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });
        let first_request = gate.start_request();

        let gate_for_follow_on = gate.clone();
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let follow_on = std::thread::spawn(move || {
            waiting_tx
                .send(())
                .expect("follow-on request announces before waiting");
            let request = gate_for_follow_on.start_request();
            started_tx
                .send(())
                .expect("follow-on request announces when fence clears");
            request.finish();
        });

        waiting_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("follow-on request should start waiting while the first request owns the lane");
        std::thread::yield_now();
        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");

        assert!(queued);
        assert!(
            started_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "waiting request must stay blocked while the owed full-resync is still pending"
        );

        first_request.finish();
        publisher.wait_for_attempts(1);
        assert!(
            started_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "waiting request must stay fenced after a failed publish attempt"
        );
        publisher.wait_for_retry_waiter();
        assert!(
            started_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "waiting request must stay fenced while the retry publish is still owed"
        );

        publisher.allow_retry_success();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("follow-on request should start after the owed full-resync is delivered");
        follow_on.join().expect("follow-on request joins cleanly");
        assert_eq!(publisher.attempts(), 2);
    }

    #[test]
    fn storage_watcher_fences_same_namespace_worker_before_transport_admission() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let next_sequence = AtomicU64::new(1);
        gate.activate();
        let _worker = start_full_resync_worker(gate.clone(), publisher.clone(), move || {
            Some(full_resync(next_sequence.fetch_add(1, Ordering::SeqCst)))
        });

        let dispatch = Arc::new(BlockingDispatch::default());
        let server =
            Arc::new(FileSystemServer::new(dispatch.clone()).with_expected_namespace("fabricfs"));
        let subject = command_subject("fabricfs", Operation::Write.as_str());
        let first_request = encoded_write_request("first-write", "fabricfs");
        let second_request = encoded_write_request("second-write", "fabricfs");

        let first_gate = gate.clone();
        let first_server = server.clone();
        let first_subject = subject.clone();
        let first = std::thread::spawn(move || {
            let request_guard = first_gate.start_request();
            let response = first_server.handle_subject_bytes(&first_subject, &first_request);
            request_guard.finish();
            response.expect("first request should complete");
        });
        dispatch.wait_for_first_call_blocked();

        let second_gate = gate.clone();
        let second_server = server.clone();
        let second_subject = subject.clone();
        let (second_ready_tx, second_ready_rx) = mpsc::channel();
        let (second_start_tx, second_start_rx) = mpsc::channel();
        let (second_admitted_tx, second_admitted_rx) = mpsc::channel();
        let second = std::thread::spawn(move || {
            second_ready_tx
                .send(())
                .expect("second worker announces before storage admission attempt");
            second_start_rx
                .recv()
                .expect("second worker receives storage admission release");
            let request_guard = second_gate.start_request();
            second_admitted_tx
                .send(())
                .expect("second request announces storage admission");
            let response = second_server.handle_subject_bytes(&second_subject, &second_request);
            request_guard.finish();
            response.expect("second request should complete");
        });

        second_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second worker should reach the pre-admission barrier");
        second_start_tx
            .send(())
            .expect("second worker should be released into storage admission");
        gate.wait_for_request_waiters(1);
        assert!(matches!(
            second_admitted_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let queued = handle_storage_event(&gate, mutating_event()).expect("watch event is handled");
        assert!(queued);
        gate.wait_for_request_waiters(1);
        assert!(matches!(
            second_admitted_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(
            dispatch.call_order(),
            vec!["first-write".to_string()],
            "the second request must not reach the transport namespace lock before full-resync delivery"
        );

        dispatch.release_first_call();
        first.join().expect("first request thread joins cleanly");
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
        second_admitted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second request should pass storage admission after full-resync delivery");
        second.join().expect("second request thread joins cleanly");
        assert_eq!(
            dispatch.call_order(),
            vec!["first-write".to_string(), "second-write".to_string()],
            "same-namespace dispatch stays ordered behind the delivered full resync"
        );
    }

    #[test]
    fn storage_watcher_recovers_deferred_full_resync_after_request_unwind_without_follow_on_request(
    ) {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));
        let gate_for_unwind = gate.clone();

        let unwind = std::panic::catch_unwind(|| {
            let _request = gate_for_unwind.start_request();
            let queued =
                handle_storage_event(&gate_for_unwind, mutating_event()).expect("watch event");
            assert!(queued);
            panic!("simulated request unwind after queued watch debt");
        });

        assert!(unwind.is_err());
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_publishes_external_internal_metadata_events() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![PathBuf::from(
                    "/storage/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
                )],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(queued);
        assert_eq!(publisher.wait_for_count(1).len(), 1);
    }

    #[test]
    fn storage_watcher_publishes_external_tombstone_events() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![PathBuf::from(
                    "/storage/.fabricfs_tombstones/gone.txt/.fabricfs_tombstone",
                )],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(queued);
        assert_eq!(publisher.wait_for_count(1).len(), 1);
    }

    #[test]
    fn storage_watcher_suppresses_registered_self_notify_internal_metadata_events() {
        let root = metadata_root();
        let path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        write_bytes(&path, b"owned");
        let gate = StorageInvalidationGate::new();
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            b"owned".to_vec(),
        ));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(!queued);
    }

    #[test]
    fn storage_watcher_does_not_suppress_different_internal_metadata_paths() {
        let publisher = RecordingPublisher::default();
        let root = metadata_root();
        let tracked_path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        let external_path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f37.json",
        );
        write_bytes(&tracked_path, b"owned");
        let gate = StorageInvalidationGate::new();
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            tracked_path,
            b"owned".to_vec(),
        ));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![external_path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(queued);
        assert_eq!(publisher.wait_for_count(1).len(), 1);
    }

    #[test]
    fn storage_watcher_keeps_user_visible_events_when_self_notify_paths_are_also_reported() {
        let publisher = RecordingPublisher::default();
        let root = metadata_root();
        let hidden_path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        write_bytes(&hidden_path, b"owned");
        let gate = StorageInvalidationGate::new();
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            hidden_path.clone(),
            b"owned".to_vec(),
        ));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![PathBuf::from("/storage/file.txt"), hidden_path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("watch event is handled");

        assert!(queued);
        assert_eq!(publisher.wait_for_count(1).len(), 1);
    }

    #[test]
    fn storage_watcher_publishes_after_remove_self_notify_is_consumed() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        gate.activate();
        let path =
            PathBuf::from("/storage/.fabricfs_xattrs/000000000000001c/0000000000620f36.json");
        gate.record_completed_self_notify_write(InternalMetadataWrite::remove_file(path.clone()));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(2)));

        let suppressed = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("self-notify event is handled");

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external follow-on event is handled");

        assert!(!suppressed);
        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 2);
    }

    #[test]
    fn storage_watcher_suppresses_multi_event_write_file_burst() {
        let publisher = RecordingPublisher::default();
        let root = metadata_root();
        let path = storage_path(&root, "/.fabricfs_tombstones/gone.txt/.fabricfs_tombstone");
        let metadata_dir = storage_path(&root, "/.fabricfs_tombstones/gone.txt");
        create_dir(&metadata_dir);
        write_bytes(&path, b"");
        let gate = StorageInvalidationGate::new();
        gate.activate();
        gate.record_completed_self_notify_write(
            InternalMetadataWrite::write_file(path.clone(), Vec::new())
                .with_created_dirs([metadata_dir.clone()]),
        );
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(2)));

        for event in [
            Event {
                kind: EventKind::Create(notify::event::CreateKind::Folder),
                paths: vec![metadata_dir.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::From)),
                paths: vec![metadata_dir.clone()],
                attrs: EventAttributes::new(),
            },
        ] {
            let queued =
                handle_storage_event(&gate, event).expect("metadata directory event is handled");
            assert!(
                !queued,
                "metadata directory burst event should be suppressed"
            );
        }

        for event in [
            EventKind::Create(notify::event::CreateKind::File),
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            EventKind::Access(AccessKind::Close(AccessMode::Write)),
        ] {
            let queued = handle_storage_event(
                &gate,
                Event {
                    kind: event,
                    paths: vec![path.clone()],
                    attrs: EventAttributes::new(),
                },
            )
            .expect("self-notify burst event is handled");
            assert!(!queued, "write-file burst event should be suppressed");
        }

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![metadata_dir],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external follow-on event is handled");

        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 2);
    }

    #[test]
    fn storage_watcher_preserves_deferred_external_event_when_atomic_replace_budget_appears_later()
    {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let tmp_path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.tmp",
        );
        let final_path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        let created_dirs = vec![storage_path(&root, "/.fabricfs_xattrs/000000000000001c")];
        let metadata_dir = created_dirs[0].clone();
        write_bytes(&final_path, br#"{"version":1,"entries":{}}"#);
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));
        let request = gate.start_request();

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![final_path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external metadata event is handled");
        assert!(queued);

        gate.record_completed_self_notify_write(
            InternalMetadataWrite::atomic_replace(
                tmp_path.clone(),
                final_path.clone(),
                br#"{"version":1,"entries":{}}"#.to_vec(),
            )
            .with_created_dirs(created_dirs),
        );

        for event in [
            Event {
                kind: EventKind::Create(notify::event::CreateKind::Folder),
                paths: vec![metadata_dir.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::From)),
                paths: vec![metadata_dir.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::To)),
                paths: vec![metadata_dir.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![tmp_path.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![tmp_path.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Access(AccessKind::Close(AccessMode::Write)),
                paths: vec![tmp_path.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![tmp_path.clone()],
                attrs: EventAttributes::new(),
            },
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![final_path.clone()],
                attrs: EventAttributes::new(),
            },
        ] {
            let queued =
                handle_storage_event(&gate, event).expect("post-success self-notify burst");
            assert!(!queued);
        }

        request.finish();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_preserves_deferred_external_event_when_same_path_budget_appears_later() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));
        let request = gate.start_request();

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external metadata event is handled");
        assert!(queued);

        gate.record_completed_self_notify_write(InternalMetadataWrite::remove_file(path.clone()));

        let suppressed = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("self-notify event is handled");

        assert!(!suppressed);
        request.finish();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_publishes_full_resync_for_external_same_path_edit_after_write_is_armed() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        write_bytes(&path, b"owned");
        gate.activate();
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(1)));
        let request = gate.start_request();

        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            b"owned".to_vec(),
        ));

        write_bytes(&path, b"external");
        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external same-path metadata event is handled");
        assert!(queued);

        let follow_on = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Access(AccessKind::Close(AccessMode::Write)),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("follow-on self-notify edge is handled");
        assert!(follow_on);

        request.finish();
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 1);
    }

    #[test]
    fn storage_watcher_does_not_accumulate_same_path_self_notify_credits_across_writes() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        write_bytes(&path, b"owned");
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            b"owned".to_vec(),
        ));
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            b"owned".to_vec(),
        ));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(2)));

        let suppressed = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("coalesced self-notify event is handled");

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("external follow-on event is handled");

        assert!(!suppressed);
        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 2);
    }

    #[test]
    fn storage_watcher_replaces_remove_budget_with_later_write_burst_on_same_path() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let path = storage_path(&root, "/.fabricfs_tombstones/gone.txt/.fabricfs_tombstone");
        write_bytes(&path, b"");
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::remove_file(path.clone()));
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            Vec::new(),
        ));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(2)));

        let suppressed = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("same-path self-notify write is handled");

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("same-path external remove is handled");

        assert!(!suppressed);
        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 2);
    }

    #[test]
    fn storage_watcher_replaces_write_budget_with_later_remove_burst_on_same_path() {
        let publisher = RecordingPublisher::default();
        let gate = StorageInvalidationGate::new();
        let root = metadata_root();
        let path = storage_path(
            &root,
            "/.fabricfs_xattrs/000000000000001c/0000000000620f36.json",
        );
        write_bytes(&path, b"owned");
        gate.activate();
        gate.record_completed_self_notify_write(InternalMetadataWrite::write_file(
            path.clone(),
            b"owned".to_vec(),
        ));
        fs::remove_file(&path).expect("same-path file removed before remove budget is recorded");
        gate.record_completed_self_notify_write(InternalMetadataWrite::remove_file(path.clone()));
        let _worker =
            start_full_resync_worker(gate.clone(), publisher.clone(), || Some(full_resync(2)));

        let suppressed = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![path.clone()],
                attrs: EventAttributes::new(),
            },
        )
        .expect("same-path self-notify remove is handled");

        let queued = handle_storage_event(
            &gate,
            Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![path],
                attrs: EventAttributes::new(),
            },
        )
        .expect("same-path external recreate is handled");

        assert!(!suppressed);
        assert!(queued);
        let invalidations = publisher.wait_for_count(1);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].sequence, 2);
    }

    #[derive(Default)]
    struct RecordedInvalidations {
        invalidations: Mutex<Vec<pb::Invalidation>>,
        ready: Condvar,
    }

    impl RecordedInvalidations {
        fn snapshot(&self) -> Vec<pb::Invalidation> {
            self.invalidations
                .lock()
                .expect("recorded invalidations lock")
                .clone()
        }

        fn push(&self, invalidation: pb::Invalidation) {
            self.invalidations
                .lock()
                .expect("recorded invalidations lock")
                .push(invalidation);
            self.ready.notify_all();
        }

        fn wait_for_count(&self, expected: usize) -> Vec<pb::Invalidation> {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut invalidations = self
                .invalidations
                .lock()
                .expect("recorded invalidations lock");
            loop {
                if invalidations.len() >= expected {
                    return invalidations.clone();
                }
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "timed out waiting for {expected} invalidation(s); observed {}",
                    invalidations.len()
                );
                let timeout = deadline.saturating_duration_since(now);
                invalidations = match self.ready.wait_timeout(invalidations, timeout) {
                    Ok((invalidations, _)) => invalidations,
                    Err(poisoned) => {
                        let (invalidations, _) = poisoned.into_inner();
                        invalidations
                    }
                };
            }
        }
    }

    #[derive(Clone, Default)]
    struct RecordingPublisher {
        state: Arc<RecordedInvalidations>,
    }

    impl RecordingPublisher {
        fn invalidations(&self) -> Vec<pb::Invalidation> {
            self.state.snapshot()
        }

        fn wait_for_count(&self, expected: usize) -> Vec<pb::Invalidation> {
            self.state.wait_for_count(expected)
        }
    }

    impl InvalidationPublisher for RecordingPublisher {
        fn publish_next<F>(&self, next_full_resync: F) -> Result<bool>
        where
            F: FnOnce() -> Option<pb::Invalidation>,
        {
            let Some(invalidation) = next_full_resync() else {
                return Ok(false);
            };
            self.state.push(invalidation);
            Ok(true)
        }
    }

    struct FailOnceThenRecordPublisherState {
        attempts: AtomicU64,
        failures_remaining: Mutex<u8>,
        invalidations: RecordedInvalidations,
    }

    #[derive(Clone)]
    struct FailOnceThenRecordPublisher {
        state: Arc<FailOnceThenRecordPublisherState>,
    }

    impl Default for FailOnceThenRecordPublisher {
        fn default() -> Self {
            Self {
                state: Arc::new(FailOnceThenRecordPublisherState {
                    attempts: AtomicU64::new(0),
                    failures_remaining: Mutex::new(1),
                    invalidations: RecordedInvalidations::default(),
                }),
            }
        }
    }

    impl FailOnceThenRecordPublisher {
        fn invalidations(&self) -> Vec<pb::Invalidation> {
            self.state.invalidations.snapshot()
        }

        fn attempts(&self) -> u64 {
            self.state.attempts.load(Ordering::SeqCst)
        }

        fn wait_for_count(&self, expected: usize) -> Vec<pb::Invalidation> {
            self.state.invalidations.wait_for_count(expected)
        }
    }

    impl InvalidationPublisher for FailOnceThenRecordPublisher {
        fn publish_next<F>(&self, next_full_resync: F) -> Result<bool>
        where
            F: FnOnce() -> Option<pb::Invalidation>,
        {
            self.state.attempts.fetch_add(1, Ordering::SeqCst);
            let mut failures_remaining = self
                .state
                .failures_remaining
                .lock()
                .expect("failing publisher lock");
            if *failures_remaining == 0 {
                let Some(invalidation) = next_full_resync() else {
                    return Ok(false);
                };
                self.state.invalidations.push(invalidation);
                return Ok(true);
            }
            *failures_remaining -= 1;
            Err(anyhow::anyhow!("injected publish failure"))
        }
    }

    struct FailOnceThenBlockRetryPublisherState {
        attempts: AtomicU64,
        retry_waiting: Mutex<bool>,
        allow_retry_success: Mutex<bool>,
        ready: Condvar,
        invalidations: RecordedInvalidations,
    }

    #[derive(Clone)]
    struct FailOnceThenBlockRetryPublisher {
        state: Arc<FailOnceThenBlockRetryPublisherState>,
    }

    impl Default for FailOnceThenBlockRetryPublisher {
        fn default() -> Self {
            Self {
                state: Arc::new(FailOnceThenBlockRetryPublisherState {
                    attempts: AtomicU64::new(0),
                    retry_waiting: Mutex::new(false),
                    allow_retry_success: Mutex::new(false),
                    ready: Condvar::new(),
                    invalidations: RecordedInvalidations::default(),
                }),
            }
        }
    }

    impl FailOnceThenBlockRetryPublisher {
        fn attempts(&self) -> u64 {
            self.state.attempts.load(Ordering::SeqCst)
        }

        fn wait_for_attempts(&self, expected: u64) {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                if self.attempts() >= expected {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for {expected} publish attempt(s); observed {}",
                    self.attempts()
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        fn wait_for_retry_waiter(&self) {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut retry_waiting = self.state.retry_waiting.lock().expect("retry waiting lock");
            loop {
                if *retry_waiting {
                    return;
                }
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "timed out waiting for the retry publish attempt to block"
                );
                let timeout = deadline.saturating_duration_since(now);
                retry_waiting = match self.state.ready.wait_timeout(retry_waiting, timeout) {
                    Ok((retry_waiting, _)) => retry_waiting,
                    Err(poisoned) => {
                        let (retry_waiting, _) = poisoned.into_inner();
                        retry_waiting
                    }
                };
            }
        }

        fn allow_retry_success(&self) {
            *self
                .state
                .allow_retry_success
                .lock()
                .expect("retry success lock") = true;
            self.state.ready.notify_all();
        }

        fn wait_for_count(&self, expected: usize) -> Vec<pb::Invalidation> {
            self.state.invalidations.wait_for_count(expected)
        }
    }

    impl InvalidationPublisher for FailOnceThenBlockRetryPublisher {
        fn publish_next<F>(&self, next_full_resync: F) -> Result<bool>
        where
            F: FnOnce() -> Option<pb::Invalidation>,
        {
            let attempt = self.state.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                return Err(anyhow::anyhow!("injected publish failure"));
            }

            let mut retry_waiting = self.state.retry_waiting.lock().expect("retry waiting lock");
            *retry_waiting = true;
            self.state.ready.notify_all();
            drop(retry_waiting);

            let mut allow_retry_success = self
                .state
                .allow_retry_success
                .lock()
                .expect("retry success lock");
            while !*allow_retry_success {
                allow_retry_success = match self.state.ready.wait(allow_retry_success) {
                    Ok(allow_retry_success) => allow_retry_success,
                    Err(poisoned) => poisoned.into_inner(),
                };
            }
            drop(allow_retry_success);

            let Some(invalidation) = next_full_resync() else {
                return Ok(false);
            };
            self.state.invalidations.push(invalidation);
            Ok(true)
        }
    }

    fn start_full_resync_worker<P, F>(
        gate: StorageInvalidationGate,
        publisher: P,
        next_full_resync: F,
    ) -> FullResyncWorker
    where
        P: InvalidationPublisher + Send + 'static,
        F: Fn() -> Option<pb::Invalidation> + Send + 'static,
    {
        FullResyncWorker::spawn(gate, publisher, next_full_resync)
    }

    fn full_resync(sequence: u64) -> pb::Invalidation {
        pb::Invalidation {
            namespace: "fabricfs".into(),
            sequence,
            kind: InvalidationKind::FullResync.wire_value(),
            path: String::new(),
            old_path: String::new(),
            new_path: String::new(),
            inode: 0,
            handle: 0,
            request_id: format!("storage-watch-{sequence}"),
        }
    }

    fn mutating_event() -> Event {
        Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            paths: vec![PathBuf::from("/storage/file.txt")],
            attrs: EventAttributes::new(),
        }
    }
}
