use fs_core::Dispatcher;
use fs_fuse::FuseAdapter;
use fs_testkit::{
    assert_direct_transport_conformance, assert_serialized_transport_conformance,
    fuse_create_request_frame_len, RecordingFs,
};
use fs_transport_local::{LocalClient, LocalMode};
use std::sync::Arc;

#[test]
fn conformance_local_direct_mode_passes_shared_suite() {
    assert_direct_transport_conformance(
        "local-direct",
        || local_client(LocalMode::Direct),
        |service| local_client_with_service(LocalMode::Direct, service),
        LocalClient::disconnect,
    );
}

#[test]
fn conformance_local_serialized_mode_passes_shared_suite() {
    assert_serialized_transport_conformance(
        "local-serialized",
        || local_client(LocalMode::Serialized),
        |service| local_client_with_service(LocalMode::Serialized, service),
        || local_client_with_frame_limit(LocalMode::Serialized, 8).0,
        LocalClient::call_bytes,
        LocalClient::disconnect,
    );
}

#[test]
fn local_serialized_response_failure_after_dispatch_poisons_fuse_cache() {
    let (client, service) = local_client_with_frame_limit(
        LocalMode::Serialized,
        fuse_create_request_frame_len("fuse-ns", "created.txt"),
    );
    let adapter = FuseAdapter::new(client, "fuse-ns");

    let error = adapter
        .create(1, "created.txt", 0, 0o644)
        .expect_err("serialized response frame failure is an uncertain mutation outcome");

    assert_eq!(error.errno(), fs_protocol::Errno::MessageTooLarge);
    assert_eq!(service.calls().len(), 1);
    assert!(adapter.cache_poisoned());
    assert_eq!(
        adapter
            .lookup(1, "file.txt")
            .expect_err("poisoned cache blocks later cache-backed work")
            .errno(),
        fs_protocol::Errno::Stale
    );
}

fn local_client(mode: LocalMode) -> (LocalClient, RecordingFs) {
    local_client_with_frame_limit(mode, 4 * 1024 * 1024)
}

fn local_client_with_frame_limit(
    mode: LocalMode,
    max_frame_bytes: usize,
) -> (LocalClient, RecordingFs) {
    let service = RecordingFs::default();
    let dispatcher = Dispatcher::new(service.clone());
    let client = LocalClient::new(Arc::new(dispatcher), mode).with_max_frame_bytes(max_frame_bytes);
    (client, service)
}

fn local_client_with_service<S>(mode: LocalMode, service: S) -> LocalClient
where
    S: fs_core::FileSystemService + 'static,
{
    let dispatcher = Dispatcher::new(service);
    LocalClient::new(Arc::new(dispatcher), mode)
}
