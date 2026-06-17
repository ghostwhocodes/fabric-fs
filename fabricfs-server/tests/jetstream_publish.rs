#![cfg(feature = "jetstream-tests")]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fabricfs_server::published_store::{
    PublishedError, PublishedOutcome, PublishedStore, PublishedStoreConfig,
};
use fabricfs_server::session_service::SessionService;
use fabricfs_server::session_storage::SessionStore;
use fabricfs_session_protocol::session_proto as pb;
use nats::jetstream;
use nats::Connection;
use tempfile::TempDir;

static TEST_BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn publishes_lists_and_imports_checkpoint_over_jetstream() {
    let (_broker, nats, store) = match jetstream_store() {
        Ok(parts) => parts,
        Err(err) => {
            eprintln!("skipping js integration test; {err}");
            return;
        }
    };

    let tmp = TempDir::new().expect("tempdir");
    let session_store = SessionStore::load(tmp.path().to_path_buf()).expect("session store");

    let session_meta = session_store
        .create_session(&pb::CreateSessionRequest {
            display_name: "integration".into(),
            workspace_name: "ws".into(),
            cow_root: tmp.path().to_string_lossy().to_string(),
            password: None,
        })
        .expect("create session");

    let checkpoint = session_store
        .checkpoint(&pb::CheckpointSessionRequest {
            session_id: session_meta.session_id.clone(),
            password: None,
            label: "first".into(),
        })
        .expect("checkpoint");

    let service = SessionService::new(nats, session_store, store);

    let publish_req = pb::PublishCheckpointRequest {
        session_id: session_meta.session_id.clone(),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        remote_checkpoint_id: String::new(),
        password: None,
    };

    let publish_resp = service.handle_publish_request(publish_req.clone());
    assert!(publish_resp.status.as_ref().expect("status").ok);
    let remote_id = publish_resp.remote_checkpoint_id.clone();

    let publish_again = service.handle_publish_request(pb::PublishCheckpointRequest {
        remote_checkpoint_id: remote_id.clone(),
        ..publish_req
    });
    assert!(publish_again.status.as_ref().expect("status").ok);

    let list_resp = service.handle_list_published_request();
    assert!(list_resp.status.as_ref().expect("status").ok);
    assert!(list_resp.checkpoints.iter().any(|published| {
        published
            .checkpoint
            .as_ref()
            .is_some_and(|meta| meta.checkpoint_id == checkpoint.checkpoint_id)
    }));

    let import_req = pb::ImportPublishedCheckpointRequest {
        remote_checkpoint_id: remote_id.clone(),
        target_session_id: String::new(),
        new_display_name: "restored".into(),
        password: None,
        mode: pb::ImportMode::Replace as i32,
        conflict_policy: pb::ConflictPolicy::Error as i32,
        expected_overlay_version: -1,
    };

    let import_resp = service.handle_import_request(import_req.clone());
    assert!(import_resp.status.as_ref().expect("status").ok);
    let imported = import_resp
        .session
        .as_ref()
        .expect("session")
        .session_id
        .clone();

    let import_again = service.handle_import_request(import_req);
    assert!(import_again.status.as_ref().expect("status").ok);
    assert_eq!(
        import_again.session.as_ref().expect("session").session_id,
        imported
    );
}

#[test]
fn concurrent_publish_keeps_one_winner_for_each_remote_id() {
    let (_broker, _nats, store) = match jetstream_store() {
        Ok(parts) => parts,
        Err(err) => {
            eprintln!("skipping js integration test; {err}");
            return;
        }
    };

    let remote_id = "shared-remote";
    let first_checkpoint = checkpoint("checkpoint-a", "session-a", 1);
    let second_checkpoint = checkpoint("checkpoint-b", "session-b", 2);
    let barrier = Arc::new(Barrier::new(3));

    let first_store = store.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_store.publish(remote_id, &first_checkpoint)
    });

    let second_store = store.clone();
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        second_store.publish(remote_id, &second_checkpoint)
    });

    barrier.wait();

    let first_result = first.join().expect("first publish thread");
    let second_result = second.join().expect("second publish thread");
    let outcomes = [&first_result, &second_result];

    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Ok(PublishedOutcome::Stored)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(PublishedError::IdempotencyConflict { remote_id })
                        if remote_id == "shared-remote"
                )
            })
            .count(),
        1
    );

    let fetched = store.fetch(remote_id).expect("fetch winner");
    let expected = if matches!(first_result, Ok(PublishedOutcome::Stored)) {
        checkpoint("checkpoint-a", "session-a", 1)
    } else {
        checkpoint("checkpoint-b", "session-b", 2)
    };
    assert_eq!(fetched, expected);
}

#[test]
fn default_store_uses_canonical_fabricfs_bucket() {
    let (_broker, _nats, js) = match jetstream_context() {
        Ok(parts) => parts,
        Err(err) => {
            eprintln!("skipping js integration test; {err}");
            return;
        }
    };

    reset_default_bucket_streams(&js);
    let store = PublishedStore::new(js).expect("resolve default store");
    assert_eq!(store.bucket_name(), "fabricfs_sessions_published");
}

fn checkpoint(
    checkpoint_id: &str,
    session_id: &str,
    overlay_version: i64,
) -> pb::PublishedCheckpoint {
    pb::PublishedCheckpoint {
        checkpoint: Some(pb::CheckpointMetadata {
            checkpoint_id: checkpoint_id.into(),
            session_id: session_id.into(),
            label: checkpoint_id.into(),
            created_at_unix_nanos: overlay_version,
        }),
        snapshot: Some(pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: session_id.into(),
                display_name: format!("display-{session_id}"),
                workspace_name: "workspace".into(),
                cow_root: "/tmp/cow".into(),
                password: None,
                created_at_unix_nanos: overlay_version,
                updated_at_unix_nanos: overlay_version,
                overlay_version,
            }),
            entries: Vec::new(),
            overlay_version,
        }),
    }
}

fn jetstream_store() -> Result<(NatsBroker, Connection, PublishedStore), String> {
    let (broker, nats, js) = jetstream_context()?;
    let store = PublishedStore::with_config(
        js,
        PublishedStoreConfig {
            bucket: unique_bucket_name(),
            ..PublishedStoreConfig::default()
        },
    )
    .map_err(|err| format!("cannot init store: {err}"))?;
    Ok((broker, nats, store))
}

fn jetstream_context() -> Result<(NatsBroker, Connection, jetstream::JetStream), String> {
    let broker = NatsBroker::start()
        .ok_or_else(|| "nats-server is not available and no broker URL was provided".to_string())?;
    let nats = nats::connect(&broker.url)
        .map_err(|err| format!("failed to connect to {}: {err}", broker.url))?;
    let js = nats::jetstream::new(nats.clone());
    Ok((broker, nats, js))
}

fn reset_default_bucket_streams(js: &jetstream::JetStream) {
    let _ = js.delete_stream("KV_fabricfs_sessions_published");
}

fn unique_bucket_name() -> String {
    let counter = TEST_BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "fabricfs_sessions_published_{}_{}",
        std::process::id(),
        counter
    )
}

struct NatsBroker {
    url: String,
    child: Option<Child>,
}

impl NatsBroker {
    fn start() -> Option<Self> {
        if let Ok(url) = std::env::var("NATS_TEST_URL").or_else(|_| std::env::var("NATS_URL")) {
            return wait_for_broker(url).map(|url| Self { url, child: None });
        }

        let port = free_tcp_port()?;
        let url = format!("nats://127.0.0.1:{port}");
        let mut child = Command::new("nats-server")
            .arg("-a")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        if let Some(url) = wait_for_broker(url.clone()) {
            Some(Self {
                url,
                child: Some(child),
            })
        } else {
            let _ = child.kill();
            let _ = child.wait();
            None
        }
    }
}

impl Drop for NatsBroker {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn free_tcp_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|addr| addr.port())
}

fn wait_for_broker(url: String) -> Option<String> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        if let Ok(connection) = nats::connect(&url) {
            let js = nats::jetstream::new(connection);
            if js.account_info().is_ok() {
                return Some(url);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}
