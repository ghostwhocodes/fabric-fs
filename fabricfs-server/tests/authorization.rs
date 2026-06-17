use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use fabricfs_server::auth::FabricFsAuthorizer;
use fabricfs_server::passthrough::PassthroughFs;
use fabricfs_server::server::FsOptions;
use fabricfs_server::service::FabricFsFileSystemService;
use fabricfs_transport::{command_subject, FileSystemServer, TransportAuth};
use fs_core::{now_unix_nanos, Dispatcher, RpcMetadata};
use fs_protocol::{
    decode_response, encode_request, path, pb, Errno, Operation, RequestEnvelope, RequestPayload,
    ResponsePayload,
};

fn passthrough_fs(root: std::path::PathBuf) -> Arc<PassthroughFs> {
    Arc::new(PassthroughFs::new(root, FsOptions::default()).expect("passthrough fs init"))
}

fn metadata_for(request: &RequestEnvelope) -> RpcMetadata {
    let mut metadata = RpcMetadata::for_request(request, 0);
    metadata.namespace = "auth-ns".into();
    metadata.peer_identity = Some(transport_auth().peer_identity());
    metadata.caller = Some(pb::CallerContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: std::process::id(),
    });
    metadata
}

fn request(request_id: &str, payload: RequestPayload) -> RequestEnvelope {
    RequestEnvelope::new(
        request_id,
        "auth-ns",
        now_unix_nanos().saturating_add(Duration::from_secs(5).as_nanos() as u64),
        pb::TraceContext::default(),
        payload,
    )
    .expect("request is valid")
    .with_caller(pb::CallerContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: std::process::id(),
    })
}

fn transport_auth() -> TransportAuth {
    TransportAuth::shared_secret("shared-secret").expect("transport auth")
}

#[test]
fn denied_mutation_does_not_touch_storage_or_invalidation_sequence() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"seed").expect("fixture file");
    let fs = passthrough_fs(root.path().to_path_buf());
    let dispatcher = Dispatcher::with_authorizer(
        FabricFsFileSystemService::new(fs.clone()),
        FabricFsAuthorizer::for_namespace("auth-ns"),
    );

    let denied_request = request(
        "denied-write",
        RequestPayload::Write(pb::WriteRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 0,
            offset: 0,
            data: b"x".to_vec(),
        }),
    );
    let mut denied_metadata = metadata_for(&denied_request);
    denied_metadata.peer_identity = None;
    let denied = dispatcher.dispatch(denied_request, denied_metadata);
    assert_eq!(denied.errno, Some(Errno::PermissionDenied));
    assert!(denied.invalidations.is_empty());
    assert_eq!(
        std::fs::read(root.path().join("file.txt")).expect("read fixture"),
        b"seed"
    );

    let create_request = request(
        "allowed-create",
        RequestPayload::Create(pb::CreateRequest {
            path: Some(path("/created.txt").expect("valid path")),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
        }),
    );
    let create = dispatcher.dispatch(create_request.clone(), metadata_for(&create_request));
    assert!(create.ok, "authorized create failed: {create:?}");
    assert_eq!(create.invalidations.len(), 1);
    assert_eq!(create.invalidations[0].sequence, 1);
}

#[test]
fn mismatched_namespace_is_denied_before_dispatch() {
    let root = tempfile::tempdir().expect("tempdir");
    let fs = passthrough_fs(root.path().to_path_buf());
    let dispatcher = Dispatcher::with_authorizer(
        FabricFsFileSystemService::new(fs),
        FabricFsAuthorizer::for_namespace("auth-ns"),
    );
    let request = RequestEnvelope::new(
        "lookup-wrong-ns",
        "other-ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Lookup(pb::LookupRequest {
            path: Some(path("/").expect("valid path")),
        }),
    )
    .expect("request is valid");
    let mut metadata = RpcMetadata::for_request(&request, 0);
    metadata.peer_identity = Some(transport_auth().peer_identity());
    metadata.caller = Some(pb::CallerContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: std::process::id(),
    });
    let response = dispatcher.dispatch(request, metadata);
    assert_eq!(response.errno, Some(Errno::PermissionDenied));
    assert!(response.invalidations.is_empty());
    assert!(response.payload.is_none());
}

#[test]
fn transport_metadata_peer_identity_allows_reads() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"seed").expect("fixture file");
    let fs = passthrough_fs(root.path().to_path_buf());
    let dispatcher = Dispatcher::with_authorizer(
        FabricFsFileSystemService::new(fs),
        FabricFsAuthorizer::for_namespace("auth-ns"),
    );
    let request = request(
        "lookup-file",
        RequestPayload::Lookup(pb::LookupRequest {
            path: Some(path("/file.txt").expect("valid path")),
        }),
    );
    let response = dispatcher.dispatch(request.clone(), metadata_for(&request));
    assert!(response.ok, "authorized lookup failed: {response:?}");
    match response.payload.expect("lookup payload") {
        ResponsePayload::Lookup(value) => {
            assert_ne!(value.attr.expect("lookup attr").inode, 0);
        }
        other => panic!("unexpected lookup payload: {other:?}"),
    }
}

#[test]
fn forged_transport_caller_is_rejected_on_real_nats_path() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live authorization transport test");
        return;
    };

    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"seed").expect("fixture file");
    let fs = passthrough_fs(root.path().to_path_buf());
    let dispatcher = Dispatcher::with_authorizer(
        FabricFsFileSystemService::new(fs.clone()),
        FabricFsAuthorizer::for_namespace("auth-ns"),
    );
    let server = FileSystemServer::new(Arc::new(dispatcher))
        .with_invalidation_mount("auth-ns")
        .with_expected_namespace("auth-ns")
        .with_transport_auth(transport_auth());
    let connection = nats::connect(&broker.url).expect("connect to broker");
    let reply = connection.new_inbox();
    let replies = connection.subscribe(&reply).expect("subscribe to replies");
    connection.flush().expect("reply subscription flush");

    let denied_request = request(
        "forged-write",
        RequestPayload::Write(pb::WriteRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 0,
            offset: 0,
            data: b"x".to_vec(),
        }),
    )
    .with_caller(pb::CallerContext {
        uid: 12345,
        gid: 12345,
        pid: 12345,
    });
    let denied_subject = command_subject("auth-ns", Operation::Write.as_str());
    let denied_bytes = encode_request(&denied_request).expect("request encodes");
    server
        .handle_message(
            &connection,
            nats::Message::new(&denied_subject, Some(&reply), &denied_bytes, None),
        )
        .expect("server responds to unauthenticated request");
    let denied_response = decode_response(
        &replies
            .next_timeout(Duration::from_secs(2))
            .expect("permission response arrives")
            .data,
    )
    .expect("permission response decodes");
    assert_eq!(denied_response.errno, Some(Errno::PermissionDenied));
    assert!(denied_response.invalidations.is_empty());
    assert_eq!(
        std::fs::read(root.path().join("file.txt")).expect("read fixture"),
        b"seed"
    );

    let create_request = request(
        "authorized-create",
        RequestPayload::Create(pb::CreateRequest {
            path: Some(path("/created.txt").expect("valid path")),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
        }),
    );
    let create_subject = command_subject("auth-ns", Operation::Create.as_str());
    let create_bytes = encode_request(&create_request).expect("request encodes");
    let headers = transport_auth().headers_for(&create_subject, &reply, &create_bytes);
    server
        .handle_message(
            &connection,
            nats::Message::new(&create_subject, Some(&reply), &create_bytes, Some(headers)),
        )
        .expect("server responds to authenticated request");
    let create_response = decode_response(
        &replies
            .next_timeout(Duration::from_secs(2))
            .expect("create response arrives")
            .data,
    )
    .expect("create response decodes");
    assert!(
        create_response.ok,
        "authenticated create failed: {create_response:?}"
    );
    assert_eq!(create_response.invalidations.len(), 1);
    assert_eq!(create_response.invalidations[0].sequence, 1);
}

struct NatsBroker {
    url: String,
    child: Option<Child>,
}

impl NatsBroker {
    fn start() -> Option<Self> {
        if let Ok(url) = std::env::var("NATS_TEST_URL") {
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
        if nats::connect(&url).is_ok() {
            return Some(url);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}
