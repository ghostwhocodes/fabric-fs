use fabricfs_transport::{
    command_subject, invalidation_subject, publish_full_resync, subscribe_requests,
    FileSystemClient, FileSystemClientConfig, FileSystemServer, TransportAuth,
};
use fs_core::{
    now_unix_nanos, Dispatcher, FileSystemService, FsError, FsResult, RpcClient, RpcError,
    RpcMetadata,
};
use fs_protocol::{
    decode_request, decode_response, encode_message, encode_request, encode_response, pb, Errno,
    Operation, RequestPayload, SEEK_CUR,
};
use fs_testkit::{
    assert_serialized_transport_conformance, request_for_operation,
    request_for_operation_in_namespace, RecordingFs,
};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn common_server_dispatches_common_envelopes_and_maps_errno_responses() {
    let service = RecordingFs::default();
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())));

    let request = request_for_operation(Operation::Lookup, "nats-server-success");
    let response = dispatch(&server, &request);
    assert!(response.ok, "expected success response: {response:?}");
    assert_eq!(response.operation, Operation::Lookup);
    assert_eq!(service.calls(), vec![Operation::Lookup]);

    service.fail_next(
        Operation::Lookup,
        FsError::new(Errno::NotFound, "not present"),
    );
    let request = request_for_operation(Operation::Lookup, "nats-server-errno");
    let response = dispatch(&server, &request);
    assert!(!response.ok, "expected errno response: {response:?}");
    assert_eq!(response.errno, Some(Errno::NotFound));
}

#[test]
fn common_server_rejects_malformed_and_oversized_frames() {
    let service = RecordingFs::default();
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service))).with_max_frame_bytes(8);

    assert!(matches!(
        server.handle_bytes(&[0xff, 0xff, 0xff]),
        Err(RpcError::Malformed(_))
    ));

    let request = request_for_operation(Operation::Lookup, "nats-server-frame");
    let bytes = encode_request(&request).expect("request encodes");
    assert_eq!(server.handle_bytes(&bytes), Err(RpcError::FrameTooLarge));
}

#[test]
fn common_server_rejects_subject_envelope_operation_mismatches() {
    let service = RecordingFs::default();
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())));
    let request = request_for_operation(Operation::Write, "subject-envelope-mismatch");
    let bytes = encode_request(&request).expect("request encodes");

    let error = server
        .handle_subject_bytes(&command_subject("demo", Operation::Lookup.as_str()), &bytes)
        .expect_err("mismatched NATS operation subject must be rejected");

    assert!(matches!(error, RpcError::Malformed(message) if message.contains("does not match")));
    assert!(
        service.calls().is_empty(),
        "mismatched subject/envelope operations must not dispatch to storage"
    );
    let response = server
        .handle_subject_bytes(&command_subject("demo", Operation::Write.as_str()), &bytes)
        .expect("matching operation subject dispatches");
    let response = decode_response(&response).expect("response decodes");
    assert_eq!(response.operation, Operation::Write);
    assert_eq!(service.calls(), vec![Operation::Write]);
}

#[test]
fn common_server_rejects_subject_envelope_namespace_mismatches() {
    let service = RecordingFs::default();
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
        .with_expected_namespace("demo");
    let request = request_for_operation_in_namespace(
        Operation::Write,
        "subject-namespace-mismatch",
        "other-namespace",
    );
    let bytes = encode_request(&request).expect("request encodes");

    let error = server
        .handle_subject_bytes(&command_subject("demo", Operation::Write.as_str()), &bytes)
        .expect_err("mismatched NATS namespace must be rejected");

    assert!(matches!(error, RpcError::Malformed(message) if message.contains("namespace")));
    assert!(
        service.calls().is_empty(),
        "mismatched subject/envelope namespaces must not dispatch to storage"
    );
}

#[test]
fn common_server_serializes_same_namespace_mutation_invalidations() {
    let (service, first_started, release_first, second_started) = BlockingWriteFs::new();
    let server = Arc::new(FileSystemServer::new(Arc::new(Dispatcher::new(service))));
    let subject = command_subject("demo", Operation::Write.as_str());
    let first = encode_request(&request_for_operation_in_namespace(
        Operation::Write,
        "ordered-first",
        "ordered-namespace",
    ))
    .expect("first request encodes");
    let second = encode_request(&request_for_operation_in_namespace(
        Operation::Write,
        "ordered-second",
        "ordered-namespace",
    ))
    .expect("second request encodes");

    let first_call = {
        let server = server.clone();
        let subject = subject.clone();
        thread::spawn(move || server.handle_subject_bytes(&subject, &first))
    };
    first_started
        .recv_timeout(Duration::from_secs(2))
        .expect("first write reaches storage");

    let second_call = {
        let server = server.clone();
        let subject = subject.clone();
        thread::spawn(move || server.handle_subject_bytes(&subject, &second))
    };
    assert!(
        second_started
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "second same-namespace mutation must wait until the first can publish sequence 1"
    );

    release_first.send(()).expect("first write release sends");
    let first_response = decode_response(
        &first_call
            .join()
            .expect("first thread joins")
            .expect("first response succeeds"),
    )
    .expect("first response decodes");
    second_started
        .recv_timeout(Duration::from_secs(2))
        .expect("second write reaches storage after first release");
    let second_response = decode_response(
        &second_call
            .join()
            .expect("second thread joins")
            .expect("second response succeeds"),
    )
    .expect("second response decodes");

    assert_eq!(first_response.invalidations[0].sequence, 1);
    assert_eq!(first_response.invalidations[0].request_id, "ordered-first");
    assert_eq!(second_response.invalidations[0].sequence, 2);
    assert_eq!(
        second_response.invalidations[0].request_id,
        "ordered-second"
    );
}

#[test]
fn common_server_releases_open_handle_when_response_deadline_expires_after_dispatch() {
    let service = SlowOpenFs::new(Duration::from_millis(100));
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())));
    let mut request = request_for_operation(Operation::Open, "expired-open-cleanup");
    request.deadline_unix_nanos = now_unix_nanos().saturating_add(50_000_000);
    let bytes = encode_request(&request).expect("request encodes");

    let error = server
        .handle_subject_bytes(&command_subject("demo", Operation::Open.as_str()), &bytes)
        .expect_err("expired post-dispatch open must not return a lost handle");

    assert_eq!(error, RpcError::Timeout);
    assert_eq!(service.open_calls(), 1);
    assert_eq!(service.release_calls(), 1);
}

#[test]
fn common_server_releases_create_handle_when_response_deadline_expires_after_dispatch() {
    let service = SlowCreateFs::new(Duration::from_millis(100));
    let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())));
    let mut request = request_for_operation(Operation::Create, "expired-create-cleanup");
    request.deadline_unix_nanos = now_unix_nanos().saturating_add(50_000_000);
    let bytes = encode_request(&request).expect("request encodes");

    let error = server
        .handle_subject_bytes(&command_subject("demo", Operation::Create.as_str()), &bytes)
        .expect_err("expired post-dispatch create must release its returned handle");

    assert_eq!(error, RpcError::Timeout);
    assert_eq!(service.create_calls(), 1);
    assert_eq!(service.release_calls(), 1);
}

#[test]
fn nats_common_stack_transport_passes_shared_conformance_when_broker_available() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS common-stack conformance");
        return;
    };
    let counter = AtomicU64::new(1);

    assert_serialized_transport_conformance(
        "nats-common",
        || {
            let service = RecordingFs::default();
            let client = managed_client(
                &broker.url,
                next_mount(&counter),
                service.clone(),
                FileSystemClientConfig::default(),
                4 * 1024 * 1024,
            );
            (client, service)
        },
        |service| {
            managed_client(
                &broker.url,
                next_mount(&counter),
                service,
                FileSystemClientConfig::default(),
                4 * 1024 * 1024,
            )
        },
        || {
            managed_client(
                &broker.url,
                next_mount(&counter),
                RecordingFs::default(),
                FileSystemClientConfig {
                    max_frame_bytes: 8,
                    ..FileSystemClientConfig::default()
                },
                4 * 1024 * 1024,
            )
        },
        ManagedNatsClient::call_bytes,
        ManagedNatsClient::disconnect,
    );
}

#[test]
fn nats_transport_delivers_remote_invalidations_without_replaying_own_responses() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS invalidation fanout test");
        return;
    };
    let mount = unique_mount("fanout");
    let service = RecordingFs::default();
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service)))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let client_a = nats_client(&broker.url, &mount);
    let client_b = nats_client(&broker.url, &mount);
    let response = client_a
        .call(request_for_operation(
            Operation::Write,
            "nats-invalidation-fanout",
        ))
        .expect("write succeeds");
    assert_eq!(response.request_id, "nats-invalidation-fanout");
    assert_eq!(response.invalidations.len(), 1);

    assert!(
        client_a
            .drain_invalidations("test-namespace")
            .expect("own drain succeeds")
            .is_empty(),
        "the client that received response invalidations must filter its own out-of-band copy"
    );

    let remote = wait_for_invalidations(&client_b, "test-namespace");
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].sequence, 1);
}

#[test]
fn nats_transport_rejects_replay_of_signed_command_after_original_caller_is_gone() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS authenticated replay test");
        return;
    };
    let mount = unique_mount("signed-replay");
    let service = RecordingFs::default();
    let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone()),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("publisher connects");
    let reply = connection.new_inbox();
    let subject = command_subject(&mount, Operation::Write.as_str());
    let mut request = request_for_operation(Operation::Write, "signed-replay");
    request.deadline_unix_nanos = now_unix_nanos().saturating_add(1_000_000_000);
    let request_bytes = encode_request(&request).expect("request encodes");
    let headers = auth.headers_for(&subject, &reply, &request_bytes);

    connection
        .publish_with_reply_or_headers(&subject, Some(&reply), Some(&headers), &request_bytes)
        .expect("first signed request publishes");
    connection.flush().expect("first signed request flushes");
    let started = Instant::now();
    while service.calls().is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for the first signed request to execute"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(service.calls(), vec![Operation::Write]);

    connection
        .publish_with_reply_or_headers(&subject, Some(&reply), Some(&headers), &request_bytes)
        .expect("captured signed request replays");
    connection.flush().expect("replay publish flushes");
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(200) {
        assert_eq!(
            service.calls(),
            vec![Operation::Write],
            "the same signed command must not execute twice even after the original reply inbox is gone"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn nats_transport_filters_own_invalidations_while_mutation_reply_is_in_flight() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS in-flight replay test");
        return;
    };
    let mount = unique_mount("in-flight-own-replay");
    let (ready_tx, ready_rx) = mpsc::channel();
    let (published_tx, published_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();
    let server = thread::spawn({
        let url = broker.url.clone();
        let mount = mount.clone();
        move || {
            let connection = nats::connect(&url).expect("server connects");
            let subscription = connection
                .subscribe(&command_subject(&mount, Operation::Create.as_str()))
                .expect("server subscribes to create");
            connection.flush().expect("server subscription flushes");
            ready_tx.send(()).expect("server readiness sends");

            let message = subscription
                .next_timeout(Duration::from_secs(2))
                .expect("server receives create");
            let request = decode_request(&message.data).expect("request decodes");
            let metadata = RpcMetadata::for_request(&request, message.data.len() as u64);
            let dispatcher = Dispatcher::new(RecordingFs::default());
            let response = dispatcher.dispatch(request, metadata);
            for invalidation in &response.invalidations {
                let bytes = encode_message(invalidation).expect("invalidation encodes");
                connection
                    .publish(&invalidation_subject(&mount), bytes)
                    .expect("own invalidation publishes");
            }
            connection.flush().expect("own invalidation flushes");
            published_tx
                .send(())
                .expect("invalidation publication signal sends");
            reply_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("reply release received");
            let reply = message.reply.as_deref().expect("request has reply inbox");
            connection
                .publish(reply, encode_response(&response).expect("response encodes"))
                .expect("delayed response publishes");
        }
    });
    ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("server becomes ready");

    let client = nats_client(&broker.url, &mount);
    let observer_connection = nats::connect(&broker.url).expect("observer connects");
    let observer = observer_connection
        .subscribe(&invalidation_subject(&mount))
        .expect("observer subscribes to invalidations");
    observer_connection
        .flush()
        .expect("observer subscription flushes");
    let call_client = client.clone();
    let call = thread::spawn(move || {
        call_client.call(request_for_operation(
            Operation::Create,
            "in-flight-own-replay",
        ))
    });

    published_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("server publishes out-of-band invalidation before reply");
    observer
        .next_timeout(Duration::from_secs(2))
        .expect("observer receives the out-of-band invalidation");
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(250) {
        let invalidations = client
            .drain_invalidations("test-namespace")
            .expect("in-flight drain succeeds");
        assert!(
            invalidations.is_empty(),
            "client must filter its own broadcast before the direct response arrives: {invalidations:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }

    reply_tx.send(()).expect("reply release sends");
    let response = call
        .join()
        .expect("call thread joins")
        .expect("create call succeeds");
    assert_eq!(response.request_id, "in-flight-own-replay");
    assert_eq!(response.invalidations.len(), 1);
    server.join().expect("server thread joins");
}

#[test]
fn nats_transport_delivers_concurrent_mutation_invalidations_in_sequence_order() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS invalidation ordering test");
        return;
    };
    let mount = unique_mount("concurrent-order");
    let (service, first_started, release_first, second_started) = BlockingWriteFs::new();
    let server = Arc::new(with_test_transport_auth(
        FileSystemServer::new(Arc::new(Dispatcher::new(service)))
            .with_invalidation_mount(mount.clone()),
    ));
    let _task = ConcurrentServerTask::spawn(broker.url.clone(), mount.clone(), server)
        .expect("concurrent server task starts");
    let observer = nats_client(&broker.url, &mount);

    let first_client = nats_client(&broker.url, &mount);
    let first_call = thread::spawn(move || {
        first_client.call(request_for_operation(
            Operation::Write,
            "concurrent-ordered-first",
        ))
    });
    first_started
        .recv_timeout(Duration::from_secs(2))
        .expect("first write reaches storage");

    let second_client = nats_client(&broker.url, &mount);
    let second_call = thread::spawn(move || {
        second_client.call(request_for_operation(
            Operation::Write,
            "concurrent-ordered-second",
        ))
    });
    assert!(
        second_started
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "second same-namespace write must not publish ahead of the first"
    );

    release_first.send(()).expect("first write release sends");
    let first_response = first_call
        .join()
        .expect("first call thread joins")
        .expect("first write succeeds");
    assert_eq!(first_response.invalidations[0].sequence, 1);
    second_started
        .recv_timeout(Duration::from_secs(2))
        .expect("second write reaches storage after first release");
    let second_response = second_call
        .join()
        .expect("second call thread joins")
        .expect("second write succeeds");
    assert_eq!(second_response.invalidations[0].sequence, 2);

    let remote = wait_for_invalidation_count(&observer, "test-namespace", 2);
    assert_eq!(remote[0].sequence, 1);
    assert_eq!(remote[1].sequence, 2);
}

#[test]
fn nats_transport_does_not_filter_remote_invalidations_with_colliding_local_request_ids() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS collision test");
        return;
    };
    let mount = unique_mount("request-id-collision");
    let service = RecordingFs::default();
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service)))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let client_a = nats_client(&broker.url, &mount);
    let client_b = nats_client(&broker.url, &mount);

    let response_b = client_b
        .call(request_for_operation(Operation::Write, "fuse-1"))
        .expect("client B write succeeds");
    assert_eq!(response_b.request_id, "fuse-1");
    assert!(
        client_b
            .drain_invalidations("test-namespace")
            .expect("client B own drain succeeds")
            .is_empty(),
        "client B should filter only its own out-of-band response replay"
    );

    let response_a = client_a
        .call(request_for_operation(Operation::Write, "fuse-1"))
        .expect("client A write with colliding local id succeeds");
    assert_eq!(response_a.request_id, "fuse-1");

    let remote = wait_for_invalidations(&client_b, "test-namespace");
    assert_eq!(remote.len(), 1);
    assert_eq!(
        remote[0].kind,
        fs_protocol::InvalidationKind::Modify.wire_value()
    );
}

#[test]
fn nats_transport_does_not_retry_mutations_after_publish_timeout() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS mutation timeout test");
        return;
    };
    let mount = unique_mount("mutation-timeout");
    let service = SlowWriteFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("client connects");
    let client = FileSystemClient::with_config(
        mount,
        connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 2,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("client config is valid");

    let error = client
        .call(request_for_operation(Operation::Write, "fuse-1"))
        .expect_err("published mutation timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);

    wait_for_write_calls(&service, 1);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        service.write_calls(),
        1,
        "a mutation whose request was already published must not be retried without server-side dedupe"
    );
}

#[test]
fn nats_transport_retries_release_after_publish_timeout() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS release timeout test");
        return;
    };
    let mount = unique_mount("release-timeout");
    let service = SlowReleaseFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("client connects");
    let client = FileSystemClient::with_config(
        mount,
        connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 1,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("client config is valid");

    let error = client
        .call(request_for_operation(Operation::Release, "release-timeout"))
        .expect_err("timed-out release exhausts its retry budget");
    assert_eq!(error, RpcError::Timeout);

    wait_for_release_calls(&service, 2);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        service.release_calls(),
        2,
        "release cleanup should retry exactly within the configured retry budget"
    );
}

#[test]
fn nats_transport_delivers_own_timed_out_mutation_invalidation_for_recovery() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS timeout recovery test");
        return;
    };
    let mount = unique_mount("timeout-recovery");
    let service = SlowWriteFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let timed_out_connection = nats::connect(&broker.url).expect("timed-out client connects");
    let timed_out_client = FileSystemClient::with_config(
        mount.clone(),
        timed_out_connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 0,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("timed-out client config is valid");
    let remote_client = nats_client(&broker.url, &mount);

    let error = timed_out_client
        .call(request_for_operation(Operation::Write, "timed-out-write"))
        .expect_err("published mutation timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);
    wait_for_write_calls(&service, 1);

    let recovered = wait_for_invalidations(&timed_out_client, "test-namespace");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, 1);
    assert_eq!(
        recovered[0].kind,
        fs_protocol::InvalidationKind::Modify.wire_value()
    );

    remote_client
        .call(request_for_operation(
            Operation::Write,
            "remote-after-timeout",
        ))
        .expect("subsequent remote write succeeds");
    let remote = wait_for_invalidations(&timed_out_client, "test-namespace");
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].sequence, 2);
}

#[test]
fn nats_transport_does_not_retry_open_after_publish_timeout() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS open timeout test");
        return;
    };
    let mount = unique_mount("open-timeout");
    let service = SlowOpenFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("client connects");
    let client = FileSystemClient::with_config(
        mount,
        connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 2,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("client config is valid");

    let error = client
        .call(request_for_operation(Operation::Open, "open-timeout"))
        .expect_err("published open timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);

    wait_for_open_calls(&service, 1);
    wait_for_release_calls(&service, 1);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        service.open_calls(),
        1,
        "an open whose request was already published must not be retried without server-side handle dedupe"
    );
    assert_eq!(
        service.release_calls(),
        1,
        "an open reply that misses the client timeout must release the backend handle"
    );
}

#[test]
fn nats_transport_does_not_retry_seek_cur_after_publish_timeout() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS lseek timeout test");
        return;
    };
    let mount = unique_mount("lseek-timeout");
    let service = SlowLseekFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("client connects");
    let client = FileSystemClient::with_config(
        mount,
        connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 2,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("client config is valid");

    let mut request = request_for_operation(Operation::Lseek, "seek-cur-timeout");
    match &mut request.payload {
        RequestPayload::Lseek(value) => {
            value.whence = SEEK_CUR;
            value.offset = 1;
        }
        other => panic!("unexpected request payload: {other:?}"),
    }

    let error = client
        .call(request)
        .expect_err("published SEEK_CUR timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);

    wait_for_lseek_calls(&service, 1);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        service.lseek_calls(),
        1,
        "SEEK_CUR mutates handle cursor state and must not be retried without server-side dedupe"
    );
}

#[test]
fn nats_transport_releases_create_handle_after_publish_timeout() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS create timeout cleanup test");
        return;
    };
    let mount = unique_mount("create-timeout");
    let service = SlowCreateFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let connection = nats::connect(&broker.url).expect("client connects");
    let client = FileSystemClient::with_config(
        mount,
        connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 2,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("client config is valid");

    let error = client
        .call(request_for_operation(Operation::Create, "create-timeout"))
        .expect_err("published create timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);

    wait_for_create_calls(&service, 1);
    wait_for_release_calls(&service, 1);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        service.create_calls(),
        1,
        "a create whose request was already published must not be retried without server-side dedupe"
    );
    assert_eq!(
        service.release_calls(),
        1,
        "a create reply that misses the client timeout must release the backend handle"
    );
}

#[test]
fn nats_transport_delivers_timed_out_create_invalidations_for_recovery_and_remote_observers() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS create timeout recovery test");
        return;
    };
    let mount = unique_mount("create-timeout-recovery");
    let service = SlowCreateFs::new(Duration::from_millis(120));
    let _task = ServerTask::spawn(
        broker.url.clone(),
        mount.clone(),
        with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount(mount.clone()),
        ),
    )
    .expect("server task starts");

    let timed_out_connection = nats::connect(&broker.url).expect("timed-out client connects");
    let timed_out_client = FileSystemClient::with_config(
        mount.clone(),
        timed_out_connection,
        with_test_transport_auth_config(FileSystemClientConfig {
            timeout: Duration::from_millis(20),
            max_retries: 0,
            retry_backoff: Duration::ZERO,
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }),
    )
    .expect("timed-out client config is valid");
    let remote_client = nats_client(&broker.url, &mount);

    let error = timed_out_client
        .call(request_for_operation(Operation::Create, "timed-out-create"))
        .expect_err("published create timeout is returned as uncertain");
    assert_eq!(error, RpcError::Timeout);
    wait_for_create_calls(&service, 1);
    wait_for_release_calls(&service, 1);

    let recovered = wait_for_invalidations(&timed_out_client, "test-namespace");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, 1);
    assert_eq!(
        recovered[0].kind,
        fs_protocol::InvalidationKind::Create.wire_value()
    );

    let remote = wait_for_invalidations(&remote_client, "test-namespace");
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].sequence, 1);
    assert_eq!(
        remote[0].kind,
        fs_protocol::InvalidationKind::Create.wire_value()
    );
}

#[test]
fn nats_transport_delivers_full_resync_to_existing_clients() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS full-resync test");
        return;
    };
    let mount = unique_mount("full-resync");
    let client = nats_client(&broker.url, &mount);
    let publisher = nats::connect(&broker.url).expect("publisher connects");

    publish_full_resync(&publisher, &mount, "test-namespace").expect("full resync publishes");

    let invalidations = wait_for_invalidations(&client, "test-namespace");
    assert_eq!(invalidations.len(), 1);
    assert_eq!(
        invalidations[0].kind,
        fs_protocol::InvalidationKind::FullResync.wire_value()
    );
    assert_eq!(invalidations[0].sequence, 0);
}

#[test]
fn nats_transport_reports_malformed_invalidation_frames() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS malformed invalidation test");
        return;
    };
    let mount = unique_mount("bad-invalidation");
    let client = nats_client(&broker.url, &mount);
    let publisher = nats::connect(&broker.url).expect("publisher connects");
    publisher
        .publish(&invalidation_subject(&mount), vec![0xff, 0xff, 0xff])
        .expect("malformed invalidation publishes");
    publisher.flush().expect("malformed invalidation flushes");

    let error = wait_for_invalidation_error(&client, "test-namespace");
    assert!(matches!(error, RpcError::Malformed(_)));
    assert!(
        client
            .drain_invalidations("test-namespace")
            .expect("second drain succeeds")
            .is_empty(),
        "the malformed frame has been consumed, so the FUSE adapter must fail closed on the first error"
    );
}

#[test]
fn nats_request_subscription_is_flushed_before_readiness_when_broker_available() {
    let Some(broker) = NatsBroker::start() else {
        eprintln!("nats-server is not available; skipping live NATS readiness flush test");
        return;
    };
    let mount = unique_mount("readiness-flush");
    let server_connection = nats::connect(&broker.url).expect("server connects");
    let subscription =
        subscribe_requests(&server_connection, &mount).expect("request subscription flushes");
    let client_connection = nats::connect(&broker.url).expect("client connects");
    client_connection
        .publish(
            &command_subject(&mount, Operation::Lookup.as_str()),
            b"readiness-probe",
        )
        .expect("readiness probe publishes");
    client_connection
        .flush()
        .expect("readiness probe publish flushes");

    let message = subscription
        .next_timeout(Duration::from_secs(2))
        .expect("flushed subscription receives immediately after readiness");
    assert_eq!(
        message.subject,
        command_subject(&mount, Operation::Lookup.as_str())
    );
    assert_eq!(message.data, b"readiness-probe");
}

#[derive(Clone)]
struct SlowWriteFs {
    delay: Duration,
    writes: Arc<AtomicU64>,
}

impl SlowWriteFs {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            writes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn write_calls(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }
}

impl FileSystemService for SlowWriteFs {
    fn write(
        &self,
        request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        thread::sleep(self.delay);
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }
}

#[derive(Clone)]
struct SlowOpenFs {
    delay: Duration,
    opens: Arc<AtomicU64>,
    releases: Arc<AtomicU64>,
}

impl SlowOpenFs {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            opens: Arc::new(AtomicU64::new(0)),
            releases: Arc::new(AtomicU64::new(0)),
        }
    }

    fn open_calls(&self) -> u64 {
        self.opens.load(Ordering::SeqCst)
    }

    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

impl FileSystemService for SlowOpenFs {
    fn open(
        &self,
        _request: &pb::OpenRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::OpenResponse> {
        let call = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
        thread::sleep(self.delay);
        Ok(pb::OpenResponse {
            handle: call,
            flags: 0,
        })
    }

    fn release(
        &self,
        _request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        Ok(pb::EmptyResponse {})
    }
}

#[derive(Clone)]
struct SlowLseekFs {
    delay: Duration,
    lseeks: Arc<AtomicU64>,
}

impl SlowLseekFs {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            lseeks: Arc::new(AtomicU64::new(0)),
        }
    }

    fn lseek_calls(&self) -> u64 {
        self.lseeks.load(Ordering::SeqCst)
    }
}

impl FileSystemService for SlowLseekFs {
    fn lseek(
        &self,
        request: &pb::LseekRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LseekResponse> {
        self.lseeks.fetch_add(1, Ordering::SeqCst);
        thread::sleep(self.delay);
        Ok(pb::LseekResponse {
            offset: request.offset.max(0),
        })
    }
}

#[derive(Clone)]
struct SlowReleaseFs {
    delay: Duration,
    releases: Arc<AtomicU64>,
}

impl SlowReleaseFs {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            releases: Arc::new(AtomicU64::new(0)),
        }
    }

    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

impl FileSystemService for SlowReleaseFs {
    fn release(
        &self,
        _request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        thread::sleep(self.delay);
        Ok(pb::EmptyResponse {})
    }
}

#[derive(Clone)]
struct SlowCreateFs {
    delay: Duration,
    creates: Arc<AtomicU64>,
    releases: Arc<AtomicU64>,
}

impl SlowCreateFs {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            creates: Arc::new(AtomicU64::new(0)),
            releases: Arc::new(AtomicU64::new(0)),
        }
    }

    fn create_calls(&self) -> u64 {
        self.creates.load(Ordering::SeqCst)
    }

    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

impl FileSystemService for SlowCreateFs {
    fn create(
        &self,
        _request: &pb::CreateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        let call = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
        thread::sleep(self.delay);
        Ok(pb::CreateResponse {
            attr: Some(fs_testkit::file_attr(100 + call)),
            handle: call,
        })
    }

    fn release(
        &self,
        _request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        Ok(pb::EmptyResponse {})
    }
}

#[derive(Clone)]
struct BlockingWriteFs {
    state: Arc<BlockingWriteState>,
}

struct BlockingWriteState {
    calls: AtomicU64,
    first_started: Mutex<Option<mpsc::Sender<()>>>,
    second_started: Mutex<Option<mpsc::Sender<()>>>,
    release_first: Mutex<mpsc::Receiver<()>>,
}

impl BlockingWriteFs {
    fn new() -> (
        Self,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
        mpsc::Receiver<()>,
    ) {
        let (first_started_tx, first_started_rx) = mpsc::channel();
        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        (
            Self {
                state: Arc::new(BlockingWriteState {
                    calls: AtomicU64::new(0),
                    first_started: Mutex::new(Some(first_started_tx)),
                    second_started: Mutex::new(Some(second_started_tx)),
                    release_first: Mutex::new(release_first_rx),
                }),
            },
            first_started_rx,
            release_first_tx,
            second_started_rx,
        )
    }
}

impl FileSystemService for BlockingWriteFs {
    fn write(
        &self,
        request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        let call = self.state.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 1 {
            if let Some(sender) = self
                .state
                .first_started
                .lock()
                .expect("first sender lock")
                .take()
            {
                sender.send(()).expect("first started signal sends");
            }
            self.state
                .release_first
                .lock()
                .expect("release receiver lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("first write release received");
        } else if let Some(sender) = self
            .state
            .second_started
            .lock()
            .expect("second sender lock")
            .take()
        {
            sender.send(()).expect("second started signal sends");
        }
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }
}

fn wait_for_write_calls(service: &SlowWriteFs, expected: u64) {
    let started = Instant::now();
    while service.write_calls() < expected {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for write calls"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_open_calls(service: &SlowOpenFs, expected: u64) {
    let started = Instant::now();
    while service.open_calls() < expected {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for open calls"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_lseek_calls(service: &SlowLseekFs, expected: u64) {
    let started = Instant::now();
    while service.lseek_calls() < expected {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for lseek calls"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_create_calls(service: &SlowCreateFs, expected: u64) {
    let started = Instant::now();
    while service.create_calls() < expected {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for create calls"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

trait ReleaseCallCounter {
    fn release_calls(&self) -> u64;
}

impl ReleaseCallCounter for SlowOpenFs {
    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

impl ReleaseCallCounter for SlowCreateFs {
    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

impl ReleaseCallCounter for SlowReleaseFs {
    fn release_calls(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }
}

fn wait_for_release_calls(service: &dyn ReleaseCallCounter, expected: u64) {
    let started = Instant::now();
    while service.release_calls() < expected {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timed out waiting for release calls"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn dispatch(
    server: &FileSystemServer,
    request: &fs_protocol::RequestEnvelope,
) -> fs_protocol::ResponseEnvelope {
    let bytes = encode_request(request).expect("request encodes");
    let response = server.handle_bytes(&bytes).expect("server dispatches");
    decode_response(&response).expect("response decodes")
}

fn next_mount(counter: &AtomicU64) -> String {
    format!("test-mount-{}", counter.fetch_add(1, Ordering::SeqCst))
}

fn unique_mount(label: &str) -> String {
    format!("test-{label}-mount-{}", std::process::id())
}

fn test_transport_auth() -> TransportAuth {
    TransportAuth::shared_secret("shared-secret").expect("test transport auth")
}

fn with_test_transport_auth(server: FileSystemServer) -> FileSystemServer {
    server.with_transport_auth(test_transport_auth())
}

fn with_test_transport_auth_config(mut config: FileSystemClientConfig) -> FileSystemClientConfig {
    config.transport_auth = Some(test_transport_auth());
    config
}

fn nats_client(url: &str, mount: &str) -> FileSystemClient {
    let connection = nats::connect(url).expect("client connects to test broker");
    FileSystemClient::with_config(
        mount.to_string(),
        connection,
        with_test_transport_auth_config(FileSystemClientConfig::default()),
    )
    .expect("client config is valid")
}

fn wait_for_invalidations(
    client: &FileSystemClient,
    namespace: &str,
) -> Vec<fs_protocol::pb::Invalidation> {
    let started = Instant::now();
    loop {
        let invalidations = client
            .drain_invalidations(namespace)
            .expect("drain invalidations");
        if !invalidations.is_empty() {
            return invalidations;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "timed out waiting for invalidations"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_invalidation_count(
    client: &FileSystemClient,
    namespace: &str,
    expected: usize,
) -> Vec<fs_protocol::pb::Invalidation> {
    let started = Instant::now();
    let mut received = Vec::new();
    loop {
        received.extend(
            client
                .drain_invalidations(namespace)
                .expect("drain invalidations"),
        );
        if received.len() >= expected {
            return received;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "timed out waiting for {expected} invalidations; received {received:?}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_invalidation_error(client: &FileSystemClient, namespace: &str) -> RpcError {
    let started = Instant::now();
    loop {
        match client.drain_invalidations(namespace) {
            Ok(invalidations) if invalidations.is_empty() => {}
            Ok(invalidations) => {
                panic!("expected malformed invalidation error, got {invalidations:?}")
            }
            Err(error) => return error,
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "timed out waiting for malformed invalidation error"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn managed_client<S>(
    url: &str,
    mount: String,
    service: S,
    config: FileSystemClientConfig,
    server_frame_limit: usize,
) -> ManagedNatsClient
where
    S: fs_core::FileSystemService + 'static,
{
    let dispatcher = Dispatcher::new(service);
    let server = with_test_transport_auth(
        FileSystemServer::new(Arc::new(dispatcher))
            .with_max_frame_bytes(server_frame_limit)
            .with_invalidation_mount(mount.clone()),
    );
    let task =
        ServerTask::spawn(url.to_string(), mount.clone(), server).expect("server task starts");
    let connection = nats::connect(url).expect("client connects to test broker");
    let client =
        FileSystemClient::with_config(mount, connection, with_test_transport_auth_config(config))
            .expect("client config is valid");
    ManagedNatsClient {
        client,
        _task: task,
    }
}

struct ManagedNatsClient {
    client: FileSystemClient,
    _task: ServerTask,
}

impl ManagedNatsClient {
    fn call_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, RpcError> {
        self.client.call_bytes(bytes)
    }

    fn disconnect(&self) {
        self.client.disconnect();
    }
}

impl RpcClient for ManagedNatsClient {
    fn call(
        &self,
        request: fs_protocol::RequestEnvelope,
    ) -> Result<fs_protocol::ResponseEnvelope, RpcError> {
        self.client.call(request)
    }

    fn drain_invalidations(
        &self,
        namespace: &str,
    ) -> Result<Vec<fs_protocol::pb::Invalidation>, RpcError> {
        self.client.drain_invalidations(namespace)
    }
}

struct ServerTask {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ServerTask {
    fn spawn(url: String, mount: String, server: FileSystemServer) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let (ready_tx, ready_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let connection = match nats::connect(&url) {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = ready_tx.send(Err(format!("server connect failed: {error}")));
                    return;
                }
            };
            let subscription = match subscribe_requests(&connection, &mount) {
                Ok(subscription) => subscription,
                Err(error) => {
                    let _ = ready_tx.send(Err(format!("server subscribe failed: {error}")));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));

            while !thread_stop.load(Ordering::SeqCst) {
                match subscription.next_timeout(Duration::from_millis(25)) {
                    Ok(message) => {
                        let _ = server.handle_message(&connection, message);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => break,
                }
            }
        });

        match ready_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(())) => Ok(Self {
                stop,
                thread: Some(handle),
            }),
            Ok(Err(error)) => {
                stop.store(true, Ordering::SeqCst);
                let _ = handle.join();
                Err(error)
            }
            Err(error) => {
                stop.store(true, Ordering::SeqCst);
                let _ = handle.join();
                Err(format!("server task did not become ready: {error}"))
            }
        }
    }
}

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

struct ConcurrentServerTask {
    stop: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
    workers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
}

impl ConcurrentServerTask {
    fn spawn(url: String, mount: String, server: Arc<FileSystemServer>) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let workers = Arc::new(Mutex::new(Vec::new()));
        let thread_workers = workers.clone();
        let (ready_tx, ready_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let connection = match nats::connect(&url) {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = ready_tx.send(Err(format!("server connect failed: {error}")));
                    return;
                }
            };
            let subscription = match subscribe_requests(&connection, &mount) {
                Ok(subscription) => subscription,
                Err(error) => {
                    let _ = ready_tx.send(Err(format!("server subscribe failed: {error}")));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));

            while !thread_stop.load(Ordering::SeqCst) {
                match subscription.next_timeout(Duration::from_millis(25)) {
                    Ok(message) => {
                        let server = server.clone();
                        let connection = connection.clone();
                        let handle = thread::spawn(move || {
                            let _ = server.handle_message(&connection, message);
                        });
                        thread_workers
                            .lock()
                            .expect("worker handle lock")
                            .push(handle);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => break,
                }
            }
        });

        match ready_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(())) => Ok(Self {
                stop,
                accept_thread: Some(handle),
                workers,
            }),
            Ok(Err(error)) => {
                stop.store(true, Ordering::SeqCst);
                let _ = handle.join();
                Err(error)
            }
            Err(error) => {
                stop.store(true, Ordering::SeqCst);
                let _ = handle.join();
                Err(format!(
                    "concurrent server task did not become ready: {error}"
                ))
            }
        }
    }
}

impl Drop for ConcurrentServerTask {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        for handle in self.workers.lock().expect("worker handle lock").drain(..) {
            let _ = handle.join();
        }
    }
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
