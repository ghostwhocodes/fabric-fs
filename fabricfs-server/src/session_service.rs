mod import_idempotency;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fabricfs_observability::{AtomicHistogram, HistogramSnapshot, LATENCY_BUCKETS_MICROS};
use fabricfs_session_protocol::session::{
    decode_session_message, encode_session_message, SessionOp, SESSION_SUBJECT_PREFIX,
};
use fabricfs_session_protocol::session_proto as pb;
use nats::Connection;

use self::import_idempotency::{imported_session_id, snapshots_equivalent};
use crate::published_store::{PublishedCheckpointStore, PublishedStore};
use crate::session_storage::{SessionError, SessionStore};

pub struct SessionService<P = PublishedStore> {
    nats: Option<Connection>,
    store: SessionStore,
    published: P,
    metrics: Arc<SessionMetrics>,
}

struct ExistingImportOptions {
    mode: pb::ImportMode,
    conflict_policy: pb::ConflictPolicy,
    expected_overlay_version: Option<i64>,
}

struct SessionMetrics {
    handled_total: AtomicU64,
    failed_total: AtomicU64,
    unknown_subject_total: AtomicU64,
    request_latency_micros: AtomicHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricsSnapshot {
    pub handled_total: u64,
    pub failed_total: u64,
    pub unknown_subject_total: u64,
    pub request_latency_micros: HistogramSnapshot,
}

#[derive(Clone)]
pub struct SessionMetricsHandle {
    metrics: Arc<SessionMetrics>,
}

impl Default for SessionMetrics {
    fn default() -> Self {
        Self {
            handled_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
            unknown_subject_total: AtomicU64::new(0),
            request_latency_micros: AtomicHistogram::new(LATENCY_BUCKETS_MICROS),
        }
    }
}

impl SessionMetrics {
    fn record(&self, ok: bool, started: Instant) {
        self.handled_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.failed_total.fetch_add(1, Ordering::Relaxed);
        }
        let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.request_latency_micros.record(latency);
    }

    fn snapshot(&self) -> SessionMetricsSnapshot {
        SessionMetricsSnapshot {
            handled_total: self.handled_total.load(Ordering::Relaxed),
            failed_total: self.failed_total.load(Ordering::Relaxed),
            unknown_subject_total: self.unknown_subject_total.load(Ordering::Relaxed),
            request_latency_micros: self.request_latency_micros.snapshot(),
        }
    }
}

impl<P> SessionService<P>
where
    P: PublishedCheckpointStore,
{
    pub fn new(nats: Connection, store: SessionStore, published: P) -> Self {
        SessionService {
            nats: Some(nats),
            store,
            published,
            metrics: Arc::new(SessionMetrics::default()),
        }
    }

    #[cfg(test)]
    fn new_for_tests(store: SessionStore, published: P) -> Self {
        SessionService {
            nats: None,
            store,
            published,
            metrics: Arc::new(SessionMetrics::default()),
        }
    }

    pub fn metrics(&self) -> SessionMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn metrics_handle(&self) -> SessionMetricsHandle {
        SessionMetricsHandle {
            metrics: Arc::clone(&self.metrics),
        }
    }

    pub fn run(&self) -> anyhow::Result<()> {
        let subject = format!("{}.*", SESSION_SUBJECT_PREFIX);
        let subscription = self
            .nats
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session transport is not configured"))?
            .subscribe(&subject)?;

        for msg in subscription.messages() {
            if let Err(err) = self.dispatch(msg) {
                tracing::error!(error = ?err, "session message handling failed");
            }
        }
        Ok(())
    }

    fn dispatch(&self, msg: nats::Message) -> anyhow::Result<()> {
        let started = Instant::now();
        let Some(op) = SessionOp::from_subject(&msg.subject) else {
            self.metrics
                .unknown_subject_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        let _span = tracing::debug_span!("session_request", operation = op.as_str(), subject = %msg.subject).entered();

        match op {
            SessionOp::CreateSession => {
                let response = match decode_request::<pb::CreateSessionRequest>(&msg.data) {
                    Ok(req) => self.handle_create_request(req),
                    Err(err) => pb::CreateSessionResponse {
                        status: Some(err.status()),
                        metadata: None,
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::ListSessions => {
                let response = self.handle_list_sessions();
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::GetSession => {
                let response = match decode_request::<pb::GetSessionRequest>(&msg.data) {
                    Ok(req) => self.handle_get_session(req),
                    Err(err) => pb::GetSessionResponse {
                        status: Some(err.status()),
                        snapshot: None,
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::DeleteSession => {
                let response = match decode_request::<pb::DeleteSessionRequest>(&msg.data) {
                    Ok(req) => self.handle_delete_session(req),
                    Err(err) => pb::DeleteSessionResponse {
                        status: Some(err.status()),
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::InitSession => {
                let response = match decode_request::<pb::InitSessionRequest>(&msg.data) {
                    Ok(req) => self.handle_init_session(req),
                    Err(err) => pb::InitSessionResponse {
                        status: Some(err.status()),
                        metadata: None,
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::UpdateOverlay => {
                let response = match decode_request::<pb::UpdateOverlayRequest>(&msg.data) {
                    Ok(req) => self.handle_update_overlay(req),
                    Err(err) => pb::UpdateOverlayResponse {
                        status: Some(err.status()),
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::ListOverlayEntries => {
                let response = match decode_request::<pb::ListOverlayEntriesRequest>(&msg.data) {
                    Ok(req) => self.handle_list_overlay(req),
                    Err(err) => pb::ListOverlayEntriesResponse {
                        status: Some(err.status()),
                        entries: Vec::new(),
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::CheckpointSession => {
                let response = match decode_request::<pb::CheckpointSessionRequest>(&msg.data) {
                    Ok(req) => self.handle_checkpoint(req),
                    Err(err) => pb::CheckpointSessionResponse {
                        status: Some(err.status()),
                        checkpoint: None,
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::ListCheckpoints => {
                let response = match decode_request::<pb::ListCheckpointsRequest>(&msg.data) {
                    Ok(req) => self.handle_list_checkpoints(req),
                    Err(err) => pb::ListCheckpointsResponse {
                        status: Some(err.status()),
                        checkpoints: Vec::new(),
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::PublishCheckpoint => {
                let response = match decode_request::<pb::PublishCheckpointRequest>(&msg.data) {
                    Ok(req) => self.handle_publish_request(req),
                    Err(err) => pb::PublishCheckpointResponse {
                        status: Some(err.status()),
                        remote_checkpoint_id: String::new(),
                    },
                };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::ListPublishedCheckpoints => {
                let response = self.handle_list_published_request();
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
            SessionOp::ImportPublishedCheckpoint => {
                let response =
                    match decode_request::<pb::ImportPublishedCheckpointRequest>(&msg.data) {
                        Ok(req) => self.handle_import_request(req),
                        Err(err) => pb::ImportPublishedCheckpointResponse {
                            status: Some(err.status()),
                            session: None,
                        },
                    };
                self.record_metrics(
                    response.status.as_ref().is_some_and(|status| status.ok),
                    started,
                );
                self.reply(&msg, response)?;
            }
        }
        Ok(())
    }

    fn record_metrics(&self, ok: bool, started: Instant) {
        self.metrics.record(ok, started);
    }

    fn handle_create_request(&self, req: pb::CreateSessionRequest) -> pb::CreateSessionResponse {
        match self.store.create_session(&req) {
            Ok(meta) => pb::CreateSessionResponse {
                status: Some(status_ok()),
                metadata: Some(meta),
            },
            Err(err) => pb::CreateSessionResponse {
                status: Some(err.status()),
                metadata: None,
            },
        }
    }

    fn handle_list_sessions(&self) -> pb::ListSessionsResponse {
        match self.store.list_sessions() {
            Ok(sessions) => pb::ListSessionsResponse {
                status: Some(status_ok()),
                sessions,
            },
            Err(err) => pb::ListSessionsResponse {
                status: Some(err.status()),
                sessions: Vec::new(),
            },
        }
    }

    fn handle_get_session(&self, req: pb::GetSessionRequest) -> pb::GetSessionResponse {
        match self
            .store
            .get_snapshot(&req.session_id, req.password.as_ref())
        {
            Ok(snapshot) => pb::GetSessionResponse {
                status: Some(status_ok()),
                snapshot: Some(snapshot),
            },
            Err(err) => pb::GetSessionResponse {
                status: Some(err.status()),
                snapshot: None,
            },
        }
    }

    fn handle_delete_session(&self, req: pb::DeleteSessionRequest) -> pb::DeleteSessionResponse {
        let status = match self
            .store
            .delete_session(&req.session_id, req.password.as_ref())
        {
            Ok(_) => status_ok(),
            Err(err) => err.status(),
        };
        pb::DeleteSessionResponse {
            status: Some(status),
        }
    }

    fn handle_init_session(&self, req: pb::InitSessionRequest) -> pb::InitSessionResponse {
        match self
            .store
            .init_session(&req.session_id, req.password.as_ref())
        {
            Ok(meta) => pb::InitSessionResponse {
                status: Some(status_ok()),
                metadata: Some(meta),
            },
            Err(err) => pb::InitSessionResponse {
                status: Some(err.status()),
                metadata: None,
            },
        }
    }

    fn handle_update_overlay(&self, req: pb::UpdateOverlayRequest) -> pb::UpdateOverlayResponse {
        let status = match self.store.update_overlay(&req) {
            Ok(_) => status_ok(),
            Err(err) => err.status(),
        };
        pb::UpdateOverlayResponse {
            status: Some(status),
        }
    }

    fn handle_list_overlay(
        &self,
        req: pb::ListOverlayEntriesRequest,
    ) -> pb::ListOverlayEntriesResponse {
        match self.store.list_overlay(&req) {
            Ok(entries) => pb::ListOverlayEntriesResponse {
                status: Some(status_ok()),
                entries,
            },
            Err(err) => pb::ListOverlayEntriesResponse {
                status: Some(err.status()),
                entries: Vec::new(),
            },
        }
    }

    fn handle_checkpoint(
        &self,
        req: pb::CheckpointSessionRequest,
    ) -> pb::CheckpointSessionResponse {
        match self.store.checkpoint(&req) {
            Ok(meta) => pb::CheckpointSessionResponse {
                status: Some(status_ok()),
                checkpoint: Some(meta),
            },
            Err(err) => pb::CheckpointSessionResponse {
                status: Some(err.status()),
                checkpoint: None,
            },
        }
    }

    fn handle_list_checkpoints(
        &self,
        req: pb::ListCheckpointsRequest,
    ) -> pb::ListCheckpointsResponse {
        match self
            .store
            .list_checkpoints(&req.session_id, req.password.as_ref())
        {
            Ok(checkpoints) => pb::ListCheckpointsResponse {
                status: Some(status_ok()),
                checkpoints,
            },
            Err(err) => pb::ListCheckpointsResponse {
                status: Some(err.status()),
                checkpoints: Vec::new(),
            },
        }
    }

    pub fn handle_publish_request(
        &self,
        req: pb::PublishCheckpointRequest,
    ) -> pb::PublishCheckpointResponse {
        let mut response = pb::PublishCheckpointResponse {
            status: Some(status_ok()),
            remote_checkpoint_id: String::new(),
        };

        let checkpoint =
            self.store
                .read_checkpoint(&req.session_id, &req.checkpoint_id, req.password.as_ref());

        match checkpoint {
            Ok((meta, snapshot)) => {
                let remote_id = if req.remote_checkpoint_id.trim().is_empty() {
                    meta.checkpoint_id.clone()
                } else {
                    req.remote_checkpoint_id.clone()
                };

                let published = pb::PublishedCheckpoint {
                    checkpoint: Some(meta),
                    snapshot: Some(snapshot),
                };

                match self.published.publish(&remote_id, &published) {
                    Ok(_) => response.remote_checkpoint_id = remote_id,
                    Err(err) => response.status = Some(err.status()),
                }
            }
            Err(err) => response.status = Some(err.status()),
        }

        response
    }

    pub fn handle_list_published_request(&self) -> pb::ListPublishedCheckpointsResponse {
        match self.published.list() {
            Ok(checkpoints) => pb::ListPublishedCheckpointsResponse {
                status: Some(status_ok()),
                checkpoints,
            },
            Err(err) => pb::ListPublishedCheckpointsResponse {
                status: Some(err.status()),
                checkpoints: Vec::new(),
            },
        }
    }

    pub fn handle_import_request(
        &self,
        req: pb::ImportPublishedCheckpointRequest,
    ) -> pb::ImportPublishedCheckpointResponse {
        let mut response = pb::ImportPublishedCheckpointResponse {
            status: Some(status_ok()),
            session: None,
        };

        let published = match self.published.fetch(&req.remote_checkpoint_id) {
            Ok(published) => published,
            Err(err) => {
                response.status = Some(err.status());
                return response;
            }
        };

        let snapshot = match published.snapshot {
            Some(snapshot) => snapshot,
            None => {
                response.status = Some(
                    SessionError::InvalidRequest("published checkpoint missing snapshot".into())
                        .status(),
                );
                return response;
            }
        };

        if !req.target_session_id.is_empty() {
            match self.import_into_existing(&req, &snapshot) {
                Ok(meta) => response.session = Some(meta),
                Err(err) => response.status = Some(err.status()),
            }
        } else {
            match self.import_into_new(&req, snapshot) {
                Ok(meta) => response.session = Some(meta),
                Err(err) => response.status = Some(err.status()),
            }
        }

        response
    }

    fn import_into_existing(
        &self,
        req: &pb::ImportPublishedCheckpointRequest,
        snapshot: &pb::SessionSnapshot,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let options = decode_existing_import_options(req)?;

        let current = self
            .store
            .get_snapshot(&req.target_session_id, req.password.as_ref())?;

        if snapshots_equivalent(&current, snapshot) {
            return current.metadata.ok_or_else(|| {
                SessionError::InvalidRequest("target snapshot missing metadata".into())
            });
        }

        self.store.import_snapshot_into_existing(
            &req.target_session_id,
            snapshot,
            req.password.as_ref(),
            options.mode,
            options.conflict_policy,
            options.expected_overlay_version,
        )
    }

    fn import_into_new(
        &self,
        req: &pb::ImportPublishedCheckpointRequest,
        snapshot: pb::SessionSnapshot,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let session_id = imported_session_id(&req.remote_checkpoint_id);
        let display_name = if req.new_display_name.trim().is_empty() {
            snapshot
                .metadata
                .as_ref()
                .map(|m| m.display_name.clone())
                .unwrap_or_else(|| "Imported session".into())
        } else {
            req.new_display_name.clone()
        };

        match self.store.create_session_from_snapshot_with_id(
            &session_id,
            snapshot.clone(),
            Some(display_name),
            req.password.clone(),
        ) {
            Ok(metadata) => Ok(metadata),
            Err(SessionError::Conflict(_)) => {
                let existing_snapshot = self
                    .store
                    .get_snapshot(&session_id, req.password.as_ref())?;
                if snapshots_equivalent(&existing_snapshot, &snapshot) {
                    existing_snapshot.metadata.ok_or_else(|| {
                        SessionError::InvalidRequest("snapshot missing metadata".into())
                    })
                } else {
                    Err(SessionError::Conflict(format!(
                        "remote checkpoint {} already imported into {}",
                        req.remote_checkpoint_id, session_id
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    fn reply<M: prost::Message>(&self, msg: &nats::Message, payload: M) -> anyhow::Result<()> {
        if let (Some(nats), Some(reply)) = (&self.nats, &msg.reply) {
            let bytes = encode_session_message(&payload)?;
            nats.publish(reply, bytes)?;
        }
        Ok(())
    }
}

impl SessionMetricsHandle {
    pub fn snapshot(&self) -> SessionMetricsSnapshot {
        self.metrics.snapshot()
    }
}

fn status_ok() -> pb::OperationStatus {
    pb::OperationStatus {
        ok: true,
        message: String::new(),
    }
}

fn decode_request<T: Default + prost::Message>(bytes: &[u8]) -> Result<T, SessionError> {
    decode_session_message(bytes).map_err(SessionError::from)
}

fn decode_existing_import_options(
    req: &pb::ImportPublishedCheckpointRequest,
) -> Result<ExistingImportOptions, SessionError> {
    let mode = pb::ImportMode::try_from(req.mode).map_err(|_| {
        SessionError::InvalidRequest(format!("unknown import mode value {}", req.mode))
    })?;
    let conflict_policy = pb::ConflictPolicy::try_from(req.conflict_policy).map_err(|_| {
        SessionError::InvalidRequest(format!(
            "unknown conflict policy value {}",
            req.conflict_policy
        ))
    })?;
    let expected_overlay_version = if req.expected_overlay_version < 0 {
        None
    } else {
        Some(req.expected_overlay_version)
    };

    Ok(ExistingImportOptions {
        mode,
        conflict_policy,
        expected_overlay_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::published_store::{PublishedCheckpointStore, PublishedError, PublishedOutcome};

    #[derive(Clone, Default)]
    struct FakePublishedStore {
        entries: Arc<Mutex<HashMap<String, pb::PublishedCheckpoint>>>,
    }

    impl PublishedCheckpointStore for FakePublishedStore {
        fn publish(
            &self,
            remote_id: &str,
            checkpoint: &pb::PublishedCheckpoint,
        ) -> Result<PublishedOutcome, PublishedError> {
            let mut entries = self.entries.lock().expect("published store lock");
            match entries.get(remote_id) {
                Some(existing) if existing == checkpoint => Ok(PublishedOutcome::Unchanged),
                Some(_) => Err(PublishedError::IdempotencyConflict {
                    remote_id: remote_id.to_string(),
                }),
                None => {
                    entries.insert(remote_id.to_string(), checkpoint.clone());
                    Ok(PublishedOutcome::Stored)
                }
            }
        }

        fn list(&self) -> Result<Vec<pb::PublishedCheckpoint>, PublishedError> {
            Ok(self
                .entries
                .lock()
                .expect("published store lock")
                .values()
                .cloned()
                .collect())
        }

        fn fetch(&self, remote_id: &str) -> Result<pb::PublishedCheckpoint, PublishedError> {
            self.entries
                .lock()
                .expect("published store lock")
                .get(remote_id)
                .cloned()
                .ok_or_else(|| PublishedError::NotFound(remote_id.to_string()))
        }
    }

    fn test_service() -> (TempDir, SessionService<FakePublishedStore>) {
        let tmp = TempDir::new().expect("tempdir");
        let store = SessionStore::load(tmp.path().to_path_buf()).expect("session store");
        let service = SessionService::new_for_tests(store, FakePublishedStore::default());
        (tmp, service)
    }

    fn create_request(cow_root: &std::path::Path) -> pb::CreateSessionRequest {
        pb::CreateSessionRequest {
            display_name: "session".into(),
            workspace_name: "workspace".into(),
            cow_root: cow_root.to_string_lossy().to_string(),
            password: None,
        }
    }

    #[test]
    fn session_service_handlers_cover_session_lifecycle() {
        let (_tmp, service) = test_service();

        let created = service.handle_create_request(create_request(service.store.cow_root()));
        assert!(created.status.as_ref().is_some_and(|status| status.ok));
        let metadata = created.metadata.expect("session metadata");

        let listed = service.handle_list_sessions();
        assert!(listed.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(listed.sessions.len(), 1);

        let init = service.handle_init_session(pb::InitSessionRequest {
            session_id: metadata.session_id.clone(),
            password: None,
        });
        assert!(init.status.as_ref().is_some_and(|status| status.ok));

        let update = service.handle_update_overlay(pb::UpdateOverlayRequest {
            session_id: metadata.session_id.clone(),
            password: None,
            add_aliases: vec![pb::Alias {
                logical_path: "/alias".into(),
                target_path: "/target".into(),
                created_at_unix_nanos: 0,
                origin: None,
            }],
            add_tombstones: Vec::new(),
            remove_alias_paths: Vec::new(),
            remove_tombstone_paths: Vec::new(),
        });
        assert!(update.status.as_ref().is_some_and(|status| status.ok));

        let overlay = service.handle_list_overlay(pb::ListOverlayEntriesRequest {
            session_id: metadata.session_id.clone(),
            password: None,
            directory_prefix: "/".into(),
        });
        assert!(overlay.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(overlay.entries.len(), 1);

        let snapshot = service.handle_get_session(pb::GetSessionRequest {
            session_id: metadata.session_id.clone(),
            password: None,
        });
        assert!(snapshot.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(snapshot.snapshot.expect("snapshot").entries.len(), 1);

        let checkpoint = service.handle_checkpoint(pb::CheckpointSessionRequest {
            session_id: metadata.session_id.clone(),
            password: None,
            label: "checkpoint".into(),
        });
        assert!(checkpoint.status.as_ref().is_some_and(|status| status.ok));
        let checkpoint = checkpoint.checkpoint.expect("checkpoint metadata");

        let checkpoints = service.handle_list_checkpoints(pb::ListCheckpointsRequest {
            session_id: metadata.session_id.clone(),
            password: None,
        });
        assert!(checkpoints.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(checkpoints.checkpoints.len(), 1);
        assert_eq!(
            checkpoints.checkpoints[0].checkpoint_id,
            checkpoint.checkpoint_id
        );

        let deleted = service.handle_delete_session(pb::DeleteSessionRequest {
            session_id: metadata.session_id,
            password: None,
        });
        assert!(deleted.status.as_ref().is_some_and(|status| status.ok));
    }

    #[test]
    fn session_service_publish_and_import_handlers_round_trip_with_fake_store() {
        let (_tmp, service) = test_service();

        let created = service.handle_create_request(create_request(service.store.cow_root()));
        let metadata = created.metadata.expect("session metadata");

        service.handle_update_overlay(pb::UpdateOverlayRequest {
            session_id: metadata.session_id.clone(),
            password: None,
            add_aliases: vec![pb::Alias {
                logical_path: "/alias".into(),
                target_path: "/target".into(),
                created_at_unix_nanos: 0,
                origin: None,
            }],
            add_tombstones: Vec::new(),
            remove_alias_paths: Vec::new(),
            remove_tombstone_paths: Vec::new(),
        });

        let checkpoint = service
            .handle_checkpoint(pb::CheckpointSessionRequest {
                session_id: metadata.session_id.clone(),
                password: None,
                label: "publishable".into(),
            })
            .checkpoint
            .expect("checkpoint metadata");

        let published = service.handle_publish_request(pb::PublishCheckpointRequest {
            session_id: metadata.session_id.clone(),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            password: None,
            remote_checkpoint_id: "remote-1".into(),
        });
        assert!(published.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(published.remote_checkpoint_id, "remote-1");

        let listed = service.handle_list_published_request();
        assert!(listed.status.as_ref().is_some_and(|status| status.ok));
        assert_eq!(listed.checkpoints.len(), 1);

        let imported = service.handle_import_request(pb::ImportPublishedCheckpointRequest {
            remote_checkpoint_id: "remote-1".into(),
            target_session_id: String::new(),
            new_display_name: "imported".into(),
            password: None,
            mode: pb::ImportMode::Replace as i32,
            conflict_policy: pb::ConflictPolicy::Error as i32,
            expected_overlay_version: -1,
        });
        assert!(imported.status.as_ref().is_some_and(|status| status.ok));
        let imported_meta = imported.session.expect("imported session");

        let imported_again = service.handle_import_request(pb::ImportPublishedCheckpointRequest {
            remote_checkpoint_id: "remote-1".into(),
            target_session_id: String::new(),
            new_display_name: "imported".into(),
            password: None,
            mode: pb::ImportMode::Replace as i32,
            conflict_policy: pb::ConflictPolicy::Error as i32,
            expected_overlay_version: -1,
        });
        assert!(imported_again
            .status
            .as_ref()
            .is_some_and(|status| status.ok));
        assert_eq!(
            imported_again.session.expect("stable import").session_id,
            imported_meta.session_id
        );
    }

    #[test]
    fn import_idempotency_survives_service_restart() {
        let tmp = TempDir::new().expect("tempdir");
        let published = FakePublishedStore::default();

        let first_store = SessionStore::load(tmp.path().to_path_buf()).expect("session store");
        let first_service = SessionService::new_for_tests(first_store, published.clone());
        let created =
            first_service.handle_create_request(create_request(first_service.store.cow_root()));
        let metadata = created.metadata.expect("session metadata");

        first_service.handle_update_overlay(pb::UpdateOverlayRequest {
            session_id: metadata.session_id.clone(),
            password: None,
            add_aliases: vec![pb::Alias {
                logical_path: "/alias".into(),
                target_path: "/target".into(),
                created_at_unix_nanos: 0,
                origin: None,
            }],
            add_tombstones: Vec::new(),
            remove_alias_paths: Vec::new(),
            remove_tombstone_paths: Vec::new(),
        });
        let checkpoint = first_service
            .handle_checkpoint(pb::CheckpointSessionRequest {
                session_id: metadata.session_id.clone(),
                password: None,
                label: "publishable".into(),
            })
            .checkpoint
            .expect("checkpoint metadata");
        let published_response =
            first_service.handle_publish_request(pb::PublishCheckpointRequest {
                session_id: metadata.session_id,
                checkpoint_id: checkpoint.checkpoint_id,
                password: None,
                remote_checkpoint_id: "remote-restart".into(),
            });
        assert!(published_response
            .status
            .as_ref()
            .is_some_and(|status| status.ok));

        let imported = first_service.handle_import_request(pb::ImportPublishedCheckpointRequest {
            remote_checkpoint_id: "remote-restart".into(),
            target_session_id: String::new(),
            new_display_name: "imported".into(),
            password: None,
            mode: pb::ImportMode::Replace as i32,
            conflict_policy: pb::ConflictPolicy::Error as i32,
            expected_overlay_version: -1,
        });
        assert!(imported.status.as_ref().is_some_and(|status| status.ok));
        let first_import_id = imported.session.expect("imported session").session_id;

        let reloaded_store = SessionStore::load(tmp.path().to_path_buf()).expect("reloaded store");
        let reloaded_service = SessionService::new_for_tests(reloaded_store, published);
        let imported_again =
            reloaded_service.handle_import_request(pb::ImportPublishedCheckpointRequest {
                remote_checkpoint_id: "remote-restart".into(),
                target_session_id: String::new(),
                new_display_name: "different name ignored on idempotent retry".into(),
                password: None,
                mode: pb::ImportMode::Replace as i32,
                conflict_policy: pb::ConflictPolicy::Error as i32,
                expected_overlay_version: -1,
            });
        assert!(imported_again
            .status
            .as_ref()
            .is_some_and(|status| status.ok));
        assert_eq!(
            imported_again
                .session
                .expect("stable imported session")
                .session_id,
            first_import_id
        );
        assert_eq!(
            reloaded_service
                .store
                .list_sessions()
                .expect("list sessions")
                .len(),
            2,
            "restart retry must not create a duplicate imported session"
        );
    }

    #[test]
    fn import_into_existing_rejects_unknown_enum_values_without_mutation() {
        let (_tmp, service) = test_service();

        let source = service.handle_create_request(create_request(service.store.cow_root()));
        let source_meta = source.metadata.expect("source metadata");
        let source_overlay = service.handle_update_overlay(pb::UpdateOverlayRequest {
            session_id: source_meta.session_id.clone(),
            password: None,
            add_aliases: vec![pb::Alias {
                logical_path: "/remote".into(),
                target_path: "/target".into(),
                created_at_unix_nanos: 0,
                origin: None,
            }],
            add_tombstones: Vec::new(),
            remove_alias_paths: Vec::new(),
            remove_tombstone_paths: Vec::new(),
        });
        assert!(source_overlay
            .status
            .as_ref()
            .is_some_and(|status| status.ok));

        let checkpoint = service
            .handle_checkpoint(pb::CheckpointSessionRequest {
                session_id: source_meta.session_id.clone(),
                password: None,
                label: "publishable".into(),
            })
            .checkpoint
            .expect("checkpoint metadata");
        let published = service.handle_publish_request(pb::PublishCheckpointRequest {
            session_id: source_meta.session_id,
            checkpoint_id: checkpoint.checkpoint_id,
            password: None,
            remote_checkpoint_id: "remote-invalid-enum".into(),
        });
        assert!(published.status.as_ref().is_some_and(|status| status.ok));

        let target = service.handle_create_request(create_request(service.store.cow_root()));
        let target_meta = target.metadata.expect("target metadata");
        let target_overlay = service.handle_update_overlay(pb::UpdateOverlayRequest {
            session_id: target_meta.session_id.clone(),
            password: None,
            add_aliases: vec![pb::Alias {
                logical_path: "/local".into(),
                target_path: "/preserved".into(),
                created_at_unix_nanos: 0,
                origin: None,
            }],
            add_tombstones: Vec::new(),
            remove_alias_paths: Vec::new(),
            remove_tombstone_paths: Vec::new(),
        });
        assert!(target_overlay
            .status
            .as_ref()
            .is_some_and(|status| status.ok));

        let baseline_snapshot = service
            .store
            .get_snapshot(&target_meta.session_id, None)
            .expect("baseline snapshot");

        let invalid_mode = service.handle_import_request(pb::ImportPublishedCheckpointRequest {
            remote_checkpoint_id: "remote-invalid-enum".into(),
            target_session_id: target_meta.session_id.clone(),
            new_display_name: String::new(),
            password: None,
            mode: 99,
            conflict_policy: pb::ConflictPolicy::Error as i32,
            expected_overlay_version: -1,
        });
        let invalid_mode_status = invalid_mode.status.expect("invalid mode status");
        assert!(!invalid_mode_status.ok);
        assert!(invalid_mode_status
            .message
            .contains("unknown import mode value 99"));
        let after_invalid_mode = service
            .store
            .get_snapshot(&target_meta.session_id, None)
            .expect("snapshot after invalid mode");
        assert_eq!(after_invalid_mode, baseline_snapshot);

        let invalid_conflict =
            service.handle_import_request(pb::ImportPublishedCheckpointRequest {
                remote_checkpoint_id: "remote-invalid-enum".into(),
                target_session_id: target_meta.session_id.clone(),
                new_display_name: String::new(),
                password: None,
                mode: pb::ImportMode::Replace as i32,
                conflict_policy: 77,
                expected_overlay_version: -1,
            });
        let invalid_conflict_status = invalid_conflict.status.expect("invalid conflict status");
        assert!(!invalid_conflict_status.ok);
        assert!(invalid_conflict_status
            .message
            .contains("unknown conflict policy value 77"));
        let after_invalid_conflict = service
            .store
            .get_snapshot(&target_meta.session_id, None)
            .expect("snapshot after invalid conflict");
        assert_eq!(after_invalid_conflict, baseline_snapshot);
    }

    #[test]
    fn session_metrics_snapshot_tracks_failures_and_unknown_subjects() {
        let metrics = SessionMetrics::default();
        metrics.record(true, Instant::now());
        metrics.record(false, Instant::now());
        metrics
            .unknown_subject_total
            .fetch_add(1, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.handled_total, 2);
        assert_eq!(snapshot.failed_total, 1);
        assert_eq!(snapshot.unknown_subject_total, 1);
        assert_eq!(snapshot.request_latency_micros.total, 2);
    }
}
