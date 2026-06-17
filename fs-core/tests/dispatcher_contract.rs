use fs_core::{
    Authorizer, Dispatch, Dispatcher, FileSystemService, FsError, FsResult, ResourceLimits,
    RpcMetadata,
};
use fs_protocol::{
    file_attr, path, pb, Errno, InvalidationKind, Operation, OperationEffect, RequestEnvelope,
    RequestPayload, ResponseHandle, LOCK_EXCLUSIVE, OPEN_FLAG_TRUNCATE, PROTOCOL_VERSION, SEEK_SET,
};
use std::sync::{Arc, Mutex};

#[test]
fn dispatcher_routes_every_supported_operation_to_typed_handler() {
    let service = RecordingService::default();
    let calls = service.calls.clone();
    let dispatcher = Dispatcher::new(service);

    for operation in Operation::ALL {
        let request = request_for_operation(operation, &format!("dispatcher-{operation:?}"));
        let metadata = RpcMetadata::for_request(&request, 0);
        let response = dispatcher.dispatch(request, metadata);
        assert!(response.ok, "operation {operation:?} failed: {response:?}");
        assert_eq!(response.operation, operation);
    }

    assert_eq!(*calls.lock().expect("calls lock"), Operation::ALL);
}

#[test]
fn dispatcher_aborts_handle_returning_responses_by_releasing_backend_handle() {
    for operation in Operation::ALL
        .into_iter()
        .filter(|operation| operation.spec().response_handle == ResponseHandle::OpenedObject)
    {
        let service = RecordingService::default();
        let calls = service.calls.clone();
        let dispatcher = Dispatcher::new(service);
        let request = request_for_operation(operation, &format!("abort-{operation:?}"));
        let metadata = RpcMetadata::for_request(&request, 0);
        let response = dispatcher.dispatch(request.clone(), metadata.clone());
        assert!(response.ok, "fixture operation failed: {response:?}");

        dispatcher
            .abort_response_handles(&request, &metadata, &response)
            .expect("handle cleanup succeeds");

        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![operation, Operation::Release],
            "handle-returning {operation:?} must release its backend handle when the transport cannot deliver the reply"
        );
    }
}

#[test]
fn dispatcher_abort_ignores_responses_without_backend_handles() {
    let service = RecordingService::default();
    let calls = service.calls.clone();
    let dispatcher = Dispatcher::new(service);
    let request = request_for_operation(Operation::Lookup, "abort-lookup");
    let metadata = RpcMetadata::for_request(&request, 0);
    let response = dispatcher.dispatch(request.clone(), metadata.clone());

    dispatcher
        .abort_response_handles(&request, &metadata, &response)
        .expect("non-handle cleanup is a no-op");

    assert_eq!(*calls.lock().expect("calls lock"), vec![Operation::Lookup]);
}

#[test]
fn dispatcher_rejects_deadline_auth_metadata_and_limits_before_handlers() {
    let service = RecordingService::default();
    let calls = service.calls.clone();
    let expired = request_with_deadline(Operation::Write, "dispatcher-expired", 1);
    let dispatcher = Dispatcher::new(service.clone());
    let response = dispatcher.dispatch(expired.clone(), RpcMetadata::for_request(&expired, 0));
    assert_eq!(response.errno, Some(Errno::TimedOut));
    assert!(calls.lock().expect("calls lock").is_empty());

    let denied_dispatcher = Dispatcher::with_authorizer(service.clone(), DenyAll);
    let request = request_for_operation(Operation::Write, "dispatcher-denied");
    let response =
        denied_dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0));
    assert_eq!(response.errno, Some(Errno::PermissionDenied));
    assert!(calls.lock().expect("calls lock").is_empty());

    let dispatcher = Dispatcher::new(service.clone());
    let mut metadata = RpcMetadata::for_request(&request, 0);
    metadata.request_id = "wrong".into();
    let response = dispatcher.dispatch(request.clone(), metadata);
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(calls.lock().expect("calls lock").is_empty());

    let limited_dispatcher = Dispatcher::new(service).with_limits(ResourceLimits {
        max_envelope_bytes: 8,
        ..ResourceLimits::default()
    });
    let response =
        limited_dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 9));
    assert_eq!(response.errno, Some(Errno::MessageTooLarge));
    assert!(calls.lock().expect("calls lock").is_empty());
}

#[test]
fn dispatcher_keeps_wire_relevant_limits_without_capping_server_side_copy_or_preallocation() {
    let service = RecordingService::default();
    let calls = service.calls.clone();
    let dispatcher = Dispatcher::new(service).with_limits(ResourceLimits {
        max_symlink_target_bytes: 3,
        ..ResourceLimits::default()
    });

    let symlink = request_for_operation(Operation::Symlink, "dispatcher-symlink-limit");
    let response = dispatcher.dispatch(symlink.clone(), RpcMetadata::for_request(&symlink, 0));
    assert_eq!(response.errno, Some(Errno::MessageTooLarge));

    let mut copy = request_for_operation(Operation::CopyFileRange, "dispatcher-copy-large");
    let RequestPayload::CopyFileRange(copy_request) = &mut copy.payload else {
        panic!("expected copy_file_range payload");
    };
    copy_request.length = u64::MAX;
    let response = dispatcher.dispatch(copy.clone(), RpcMetadata::for_request(&copy, 0));
    assert!(
        response.ok,
        "copy_file_range should not be capped by payload-style byte limits"
    );
    assert_eq!(response.operation, Operation::CopyFileRange);

    let mut fallocate = request_for_operation(Operation::Fallocate, "dispatcher-fallocate-large");
    let RequestPayload::Fallocate(fallocate_request) = &mut fallocate.payload else {
        panic!("expected fallocate payload");
    };
    fallocate_request.length = i64::MAX;
    let response = dispatcher.dispatch(fallocate.clone(), RpcMetadata::for_request(&fallocate, 0));
    assert!(
        response.ok,
        "fallocate should not be capped by payload-style byte limits"
    );
    assert_eq!(response.operation, Operation::Fallocate);

    assert_eq!(
        *calls.lock().expect("calls lock"),
        vec![Operation::CopyFileRange, Operation::Fallocate],
    );
}

#[test]
fn dispatcher_rejects_invalid_direct_envelopes_before_handlers() {
    let service = RecordingService::default();
    let calls = service.calls.clone();
    let dispatcher = Dispatcher::new(service);

    let mut mismatched = request_for_operation(Operation::Write, "dispatcher-payload-mismatch");
    mismatched.operation = Operation::Lookup;
    let response =
        dispatcher.dispatch(mismatched.clone(), RpcMetadata::for_request(&mismatched, 0));
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    assert!(calls.lock().expect("calls lock").is_empty());

    let mut invalid_path = request_for_operation(Operation::Write, "dispatcher-invalid-path");
    match &mut invalid_path.payload {
        RequestPayload::Write(value) => {
            value.path = Some(pb::PathDto {
                path: "relative/path".into(),
            });
        }
        other => panic!("expected write payload, got {other:?}"),
    }
    let response = dispatcher.dispatch(
        invalid_path.clone(),
        RpcMetadata::for_request(&invalid_path, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    assert!(calls.lock().expect("calls lock").is_empty());

    let mut root_unlink = request_for_operation(Operation::Unlink, "dispatcher-root-unlink");
    match &mut root_unlink.payload {
        RequestPayload::Unlink(value) => {
            value.path = Some(path("/").expect("root path DTO is syntactically valid"));
        }
        other => panic!("expected unlink payload, got {other:?}"),
    }
    let response = dispatcher.dispatch(
        root_unlink.clone(),
        RpcMetadata::for_request(&root_unlink, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    assert!(calls.lock().expect("calls lock").is_empty());

    for (old_path, new_path) in [("/dir", "/dir/sub"), ("/dir/sub", "/dir")] {
        let mut overlapping_rename =
            request_for_operation(Operation::Rename, "dispatcher-overlapping-rename");
        match &mut overlapping_rename.payload {
            RequestPayload::Rename(value) => {
                value.old_path = Some(path(old_path).expect("old path DTO is valid"));
                value.new_path = Some(path(new_path).expect("new path DTO is valid"));
            }
            other => panic!("expected rename payload, got {other:?}"),
        }
        let response = dispatcher.dispatch(
            overlapping_rename.clone(),
            RpcMetadata::for_request(&overlapping_rename, 0),
        );
        assert_eq!(response.errno, Some(Errno::InvalidArgument));
        assert!(response.invalidations.is_empty());
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    let mut unsupported_version =
        request_for_operation(Operation::Write, "dispatcher-unsupported-version");
    unsupported_version.protocol_version = PROTOCOL_VERSION + 1;
    let response = dispatcher.dispatch(
        unsupported_version.clone(),
        RpcMetadata::for_request(&unsupported_version, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    response
        .validate()
        .expect("unsupported-version rejection remains protocol-valid");
    assert_eq!(response.protocol_version, PROTOCOL_VERSION);
    assert!(calls.lock().expect("calls lock").is_empty());

    let mut empty_request_id =
        request_for_operation(Operation::Write, "dispatcher-empty-request-id");
    empty_request_id.request_id.clear();
    let response = dispatcher.dispatch(
        empty_request_id.clone(),
        RpcMetadata::for_request(&empty_request_id, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    response
        .validate()
        .expect("empty-request-id rejection remains protocol-valid");
    assert!(!response.request_id.is_empty());
    assert!(calls.lock().expect("calls lock").is_empty());

    let mut empty_namespace = request_for_operation(Operation::Write, "dispatcher-empty-namespace");
    empty_namespace.namespace.clear();
    let response = dispatcher.dispatch(
        empty_namespace.clone(),
        RpcMetadata::for_request(&empty_namespace, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
    response
        .validate()
        .expect("empty-namespace rejection remains protocol-valid");
    assert!(!response.namespace.is_empty());
    assert!(calls.lock().expect("calls lock").is_empty());
}

#[test]
fn dispatcher_maps_handler_errors_without_parsing_strings() {
    let service = RecordingService::default();
    service.fail_next(
        Operation::Lookup,
        FsError::new(Errno::NotFound, "lookup miss"),
    );
    let dispatcher = Dispatcher::new(service);
    let request = request_for_operation(Operation::Lookup, "dispatcher-handler-error");
    let response = dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0));

    assert!(!response.ok);
    assert_eq!(response.errno, Some(Errno::NotFound));
    assert_eq!(response.error_message, "lookup miss");
    assert!(response.invalidations.is_empty());
}

#[test]
fn dispatcher_emits_invalidations_only_for_mutations() {
    let dispatcher = Dispatcher::new(RecordingService::default());

    for operation in Operation::ALL {
        let request =
            request_for_operation(operation, &format!("dispatcher-invalidation-{operation:?}"));
        let response = dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0));
        let expected_kind = expected_invalidation_kind(operation);
        assert_eq!(
            response.invalidations.len(),
            usize::from(expected_kind.is_some()),
            "unexpected invalidation count for {operation:?}"
        );
        if let Some(kind) = expected_kind {
            let invalidation = &response.invalidations[0];
            assert_eq!(invalidation.kind, kind.wire_value());
            assert_eq!(invalidation.request_id, request.request_id);
            assert_eq!(invalidation.namespace, request.namespace);
        }
    }
}

#[test]
fn dispatcher_emits_modify_invalidation_for_truncating_open_only() {
    let dispatcher = Dispatcher::new(RecordingService::default());

    let ordinary_open = request_for_operation(Operation::Open, "dispatcher-open-read-only");
    let ordinary_response = dispatcher.dispatch(
        ordinary_open.clone(),
        RpcMetadata::for_request(&ordinary_open, 0),
    );
    assert!(
        ordinary_response.ok,
        "open response failed: {ordinary_response:?}"
    );
    assert!(ordinary_response.invalidations.is_empty());

    let mut truncating_open = request_for_operation(Operation::Open, "dispatcher-open-truncate");
    match &mut truncating_open.payload {
        RequestPayload::Open(value) => value.flags = OPEN_FLAG_TRUNCATE,
        other => panic!("expected open payload, got {other:?}"),
    }
    let truncating_response = dispatcher.dispatch(
        truncating_open.clone(),
        RpcMetadata::for_request(&truncating_open, 0),
    );
    assert!(
        truncating_response.ok,
        "truncating open response failed: {truncating_response:?}"
    );
    assert_eq!(truncating_response.invalidations.len(), 1);
    let invalidation = &truncating_response.invalidations[0];
    assert_eq!(invalidation.kind, InvalidationKind::Modify.wire_value());
    assert_eq!(invalidation.path, "/file.txt");
    assert_eq!(invalidation.request_id, truncating_open.request_id);
    assert_eq!(invalidation.namespace, truncating_open.namespace);
    assert_eq!(invalidation.sequence, 1);
}

#[test]
fn dispatcher_scopes_invalidation_sequences_by_namespace() {
    let dispatcher = Dispatcher::new(RecordingService::default());

    let ns_a_first = request_for_operation_in_namespace(
        Operation::Write,
        "dispatcher-ns-a-first",
        "namespace-a",
    );
    let ns_b_first = request_for_operation_in_namespace(
        Operation::Write,
        "dispatcher-ns-b-first",
        "namespace-b",
    );
    let ns_a_second = request_for_operation_in_namespace(
        Operation::Create,
        "dispatcher-ns-a-second",
        "namespace-a",
    );

    let ns_a_first_response =
        dispatcher.dispatch(ns_a_first.clone(), RpcMetadata::for_request(&ns_a_first, 0));
    let ns_b_first_response =
        dispatcher.dispatch(ns_b_first.clone(), RpcMetadata::for_request(&ns_b_first, 0));
    let ns_a_second_response = dispatcher.dispatch(
        ns_a_second.clone(),
        RpcMetadata::for_request(&ns_a_second, 0),
    );

    assert_eq!(
        ns_a_first_response.invalidations[0].namespace,
        "namespace-a"
    );
    assert_eq!(ns_a_first_response.invalidations[0].sequence, 1);
    assert_eq!(
        ns_b_first_response.invalidations[0].namespace,
        "namespace-b"
    );
    assert_eq!(ns_b_first_response.invalidations[0].sequence, 1);
    assert_eq!(
        ns_a_second_response.invalidations[0].namespace,
        "namespace-a"
    );
    assert_eq!(ns_a_second_response.invalidations[0].sequence, 2);
}

#[test]
fn dispatcher_includes_created_inode_in_create_and_mkdir_invalidations() {
    let dispatcher = Dispatcher::new(RecordingService::default());

    let create = request_for_operation(Operation::Create, "dispatcher-create-inode");
    let create_response = dispatcher.dispatch(create.clone(), RpcMetadata::for_request(&create, 0));
    assert!(
        create_response.ok,
        "create response failed: {create_response:?}"
    );
    assert_eq!(create_response.invalidations.len(), 1);
    assert_eq!(
        create_response.invalidations[0].kind,
        InvalidationKind::Create.wire_value()
    );
    assert_eq!(create_response.invalidations[0].path, "/new.txt");
    assert_eq!(create_response.invalidations[0].inode, 3);

    let mkdir = request_for_operation(Operation::Mkdir, "dispatcher-mkdir-inode");
    let mkdir_response = dispatcher.dispatch(mkdir.clone(), RpcMetadata::for_request(&mkdir, 0));
    assert!(
        mkdir_response.ok,
        "mkdir response failed: {mkdir_response:?}"
    );
    assert_eq!(mkdir_response.invalidations.len(), 1);
    assert_eq!(
        mkdir_response.invalidations[0].kind,
        InvalidationKind::Create.wire_value()
    );
    assert_eq!(mkdir_response.invalidations[0].path, "/dir");
    assert_eq!(mkdir_response.invalidations[0].inode, 4);

    let symlink = request_for_operation(Operation::Symlink, "dispatcher-symlink-inode");
    let symlink_response =
        dispatcher.dispatch(symlink.clone(), RpcMetadata::for_request(&symlink, 0));
    assert!(
        symlink_response.ok,
        "symlink response failed: {symlink_response:?}"
    );
    assert_eq!(
        symlink_response.invalidations[0].kind,
        InvalidationKind::Create.wire_value()
    );
    assert_eq!(symlink_response.invalidations[0].path, "/link.txt");
    assert_eq!(symlink_response.invalidations[0].inode, 5);

    let hardlink = request_for_operation(Operation::Hardlink, "dispatcher-hardlink-inode");
    let hardlink_response =
        dispatcher.dispatch(hardlink.clone(), RpcMetadata::for_request(&hardlink, 0));
    assert!(
        hardlink_response.ok,
        "hardlink response failed: {hardlink_response:?}"
    );
    assert_eq!(
        hardlink_response.invalidations[0].kind,
        InvalidationKind::Create.wire_value()
    );
    assert_eq!(hardlink_response.invalidations[0].path, "/hard.txt");
    assert_eq!(hardlink_response.invalidations[0].inode, 6);
}

#[test]
fn dispatcher_uses_operation_specific_invalidation_paths_for_new_mutations() {
    let dispatcher = Dispatcher::new(RecordingService::default());

    let setattr = request_for_operation(Operation::Setattr, "dispatcher-setattr-invalidation");
    let setattr_response =
        dispatcher.dispatch(setattr.clone(), RpcMetadata::for_request(&setattr, 0));
    assert_eq!(
        setattr_response.invalidations[0].kind,
        InvalidationKind::Metadata.wire_value()
    );
    assert_eq!(setattr_response.invalidations[0].path, "/file.txt");

    let copy = request_for_operation(Operation::CopyFileRange, "dispatcher-copy-invalidation");
    let copy_response = dispatcher.dispatch(copy.clone(), RpcMetadata::for_request(&copy, 0));
    assert_eq!(
        copy_response.invalidations[0].kind,
        InvalidationKind::Modify.wire_value()
    );
    assert_eq!(copy_response.invalidations[0].path, "/copy.txt");

    let fallocate =
        request_for_operation(Operation::Fallocate, "dispatcher-fallocate-invalidation");
    let fallocate_response =
        dispatcher.dispatch(fallocate.clone(), RpcMetadata::for_request(&fallocate, 0));
    assert_eq!(
        fallocate_response.invalidations[0].kind,
        InvalidationKind::Modify.wire_value()
    );
    assert_eq!(fallocate_response.invalidations[0].path, "/file.txt");
}

#[test]
fn dispatcher_does_not_advance_sequence_for_invalid_success_payloads() {
    let dispatcher = Dispatcher::new(InvalidCreateService);

    let invalid_create = request_for_operation(Operation::Create, "dispatcher-invalid-create");
    let response = dispatcher.dispatch(
        invalid_create.clone(),
        RpcMetadata::for_request(&invalid_create, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());

    let write = request_for_operation(Operation::Write, "dispatcher-write-after-invalid-create");
    let response = dispatcher.dispatch(write.clone(), RpcMetadata::for_request(&write, 0));
    assert!(response.ok, "write response failed: {response:?}");
    assert_eq!(response.invalidations.len(), 1);
    assert_eq!(response.invalidations[0].sequence, 1);
}

#[test]
fn dispatcher_rejects_handler_responses_that_contradict_requests() {
    let dispatcher = Dispatcher::new(ContradictoryResponseService::default());

    let read = request_for_operation(Operation::Read, "dispatcher-oversized-read");
    let response = dispatcher.dispatch(read.clone(), RpcMetadata::for_request(&read, 0));
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());

    let mut readdir = request_for_operation(Operation::Readdir, "dispatcher-too-many-entries");
    match &mut readdir.payload {
        RequestPayload::Readdir(value) => value.max_entries = 1,
        other => panic!("expected readdir payload, got {other:?}"),
    }
    let response = dispatcher.dispatch(readdir.clone(), RpcMetadata::for_request(&readdir, 0));
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());

    let invalid_write = request_for_operation(Operation::Write, "dispatcher-impossible-write");
    let response = dispatcher.dispatch(
        invalid_write.clone(),
        RpcMetadata::for_request(&invalid_write, 0),
    );
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());

    let valid_write = request_for_operation(Operation::Write, "dispatcher-valid-write-after");
    let response = dispatcher.dispatch(
        valid_write.clone(),
        RpcMetadata::for_request(&valid_write, 0),
    );
    assert!(response.ok, "write response failed: {response:?}");
    assert_eq!(response.invalidations.len(), 1);
    assert_eq!(response.invalidations[0].sequence, 1);
}

#[test]
fn dispatcher_rejects_create_and_mkdir_attr_kind_mismatches() {
    let dispatcher = Dispatcher::new(WrongKindService);

    let create = request_for_operation(Operation::Create, "dispatcher-create-wrong-kind");
    let response = dispatcher.dispatch(create.clone(), RpcMetadata::for_request(&create, 0));
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());

    let mkdir = request_for_operation(Operation::Mkdir, "dispatcher-mkdir-wrong-kind");
    let response = dispatcher.dispatch(mkdir.clone(), RpcMetadata::for_request(&mkdir, 0));
    assert_eq!(response.errno, Some(Errno::InvalidArgument));
    assert!(response.invalidations.is_empty());
}

#[derive(Clone, Default)]
struct RecordingService {
    calls: Arc<Mutex<Vec<Operation>>>,
    fail_next: Arc<Mutex<Option<(Operation, FsError)>>>,
}

impl RecordingService {
    fn record(&self, operation: Operation) -> FsResult<()> {
        self.calls.lock().expect("calls lock").push(operation);
        let mut fail_next = self.fail_next.lock().expect("fail lock");
        if fail_next
            .as_ref()
            .is_some_and(|(failed_operation, _)| *failed_operation == operation)
        {
            let (_, error) = fail_next.take().expect("checked above");
            return Err(error);
        }
        Ok(())
    }

    fn fail_next(&self, operation: Operation, error: FsError) {
        *self.fail_next.lock().expect("fail lock") = Some((operation, error));
    }
}

impl FileSystemService for RecordingService {
    fn lookup(
        &self,
        _request: &pb::LookupRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        self.record(Operation::Lookup)?;
        Ok(pb::LookupResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        })
    }

    fn getattr(
        &self,
        _request: &pb::GetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetattrResponse> {
        self.record(Operation::Getattr)?;
        Ok(pb::GetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        })
    }

    fn readdir(
        &self,
        _request: &pb::ReaddirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReaddirResponse> {
        self.record(Operation::Readdir)?;
        Ok(pb::ReaddirResponse {
            entries: vec![pb::DirectoryEntry {
                inode: 2,
                name: "file.txt".into(),
                kind: pb::FileKind::File as i32,
            }],
            end: true,
        })
    }

    fn open(
        &self,
        _request: &pb::OpenRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::OpenResponse> {
        self.record(Operation::Open)?;
        Ok(pb::OpenResponse {
            handle: 7,
            flags: 0,
        })
    }

    fn read(
        &self,
        _request: &pb::ReadRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadResponse> {
        self.record(Operation::Read)?;
        Ok(pb::ReadResponse {
            data: b"hello".to_vec(),
        })
    }

    fn write(
        &self,
        request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        self.record(Operation::Write)?;
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }

    fn create(
        &self,
        _request: &pb::CreateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        self.record(Operation::Create)?;
        Ok(pb::CreateResponse {
            attr: Some(file_attr(3, pb::FileKind::File, 0)),
            handle: 8,
        })
    }

    fn rename(
        &self,
        _request: &pb::RenameRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Rename)?;
        Ok(pb::EmptyResponse {})
    }

    fn unlink(
        &self,
        _request: &pb::UnlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Unlink)?;
        Ok(pb::EmptyResponse {})
    }

    fn mkdir(
        &self,
        _request: &pb::MkdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        self.record(Operation::Mkdir)?;
        Ok(pb::LookupResponse {
            attr: Some(file_attr(4, pb::FileKind::Directory, 0)),
        })
    }

    fn rmdir(
        &self,
        _request: &pb::RmdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Rmdir)?;
        Ok(pb::EmptyResponse {})
    }

    fn statfs(
        &self,
        _request: &pb::StatfsRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::StatfsResponse> {
        self.record(Operation::Statfs)?;
        Ok(pb::StatfsResponse {
            stat: Some(pb::StatFs {
                blocks: 1,
                blocks_free: 1,
                files: 1,
                files_free: 1,
                block_size: 4096,
                name_max: 255,
                blocks_available: 1,
                fragment_size: 4096,
            }),
        })
    }

    fn getxattr(
        &self,
        _request: &pb::GetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetxattrResponse> {
        self.record(Operation::Getxattr)?;
        Ok(pb::GetxattrResponse {
            value: b"value".to_vec(),
        })
    }

    fn setxattr(
        &self,
        _request: &pb::SetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Setxattr)?;
        Ok(pb::EmptyResponse {})
    }

    fn listxattr(
        &self,
        _request: &pb::ListxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ListxattrResponse> {
        self.record(Operation::Listxattr)?;
        Ok(pb::ListxattrResponse {
            names: vec!["user.key".into()],
        })
    }

    fn removexattr(
        &self,
        _request: &pb::RemovexattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Removexattr)?;
        Ok(pb::EmptyResponse {})
    }

    fn release(
        &self,
        _request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Release)?;
        Ok(pb::EmptyResponse {})
    }

    fn readlink(
        &self,
        _request: &pb::ReadlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadlinkResponse> {
        self.record(Operation::Readlink)?;
        Ok(pb::ReadlinkResponse {
            target: b"file.txt".to_vec(),
        })
    }

    fn symlink(
        &self,
        _request: &pb::SymlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SymlinkResponse> {
        self.record(Operation::Symlink)?;
        Ok(pb::SymlinkResponse {
            attr: Some(file_attr(5, pb::FileKind::Symlink, 8)),
        })
    }

    fn hardlink(
        &self,
        _request: &pb::HardlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::HardlinkResponse> {
        self.record(Operation::Hardlink)?;
        Ok(pb::HardlinkResponse {
            attr: Some(file_attr(6, pb::FileKind::File, 128)),
        })
    }

    fn setattr(
        &self,
        _request: &pb::SetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SetattrResponse> {
        self.record(Operation::Setattr)?;
        Ok(pb::SetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 16)),
        })
    }

    fn flush(
        &self,
        _request: &pb::FlushRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Flush)?;
        Ok(pb::EmptyResponse {})
    }

    fn fsync(
        &self,
        _request: &pb::FsyncRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Fsync)?;
        Ok(pb::EmptyResponse {})
    }

    fn fsyncdir(
        &self,
        _request: &pb::FsyncdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Fsyncdir)?;
        Ok(pb::EmptyResponse {})
    }

    fn getlk(
        &self,
        _request: &pb::GetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetlkResponse> {
        self.record(Operation::Getlk)?;
        Ok(pb::GetlkResponse { lock: None })
    }

    fn setlk(
        &self,
        _request: &pb::SetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Setlk)?;
        Ok(pb::EmptyResponse {})
    }

    fn flock(
        &self,
        _request: &pb::FlockRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Flock)?;
        Ok(pb::EmptyResponse {})
    }

    fn copy_file_range(
        &self,
        _request: &pb::CopyFileRangeRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CopyFileRangeResponse> {
        self.record(Operation::CopyFileRange)?;
        Ok(pb::CopyFileRangeResponse { bytes_copied: 5 })
    }

    fn fallocate(
        &self,
        _request: &pb::FallocateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.record(Operation::Fallocate)?;
        Ok(pb::EmptyResponse {})
    }

    fn lseek(
        &self,
        _request: &pb::LseekRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LseekResponse> {
        self.record(Operation::Lseek)?;
        Ok(pb::LseekResponse { offset: 5 })
    }
}

struct DenyAll;

impl Authorizer for DenyAll {
    fn authorize(&self, _metadata: &RpcMetadata, _request: &RequestEnvelope) -> FsResult<()> {
        Err(FsError::new(Errno::PermissionDenied, "denied by test"))
    }
}

struct InvalidCreateService;

impl FileSystemService for InvalidCreateService {
    fn create(
        &self,
        _request: &pb::CreateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        Ok(pb::CreateResponse {
            attr: None,
            handle: 8,
        })
    }

    fn write(
        &self,
        request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }
}

struct WrongKindService;

impl FileSystemService for WrongKindService {
    fn create(
        &self,
        _request: &pb::CreateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        Ok(pb::CreateResponse {
            attr: Some(file_attr(3, pb::FileKind::Directory, 0)),
            handle: 8,
        })
    }

    fn mkdir(
        &self,
        _request: &pb::MkdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        Ok(pb::LookupResponse {
            attr: Some(file_attr(4, pb::FileKind::File, 0)),
        })
    }
}

#[derive(Default)]
struct ContradictoryResponseService {
    invalid_write_remaining: Mutex<bool>,
}

impl FileSystemService for ContradictoryResponseService {
    fn readdir(
        &self,
        _request: &pb::ReaddirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReaddirResponse> {
        Ok(pb::ReaddirResponse {
            entries: vec![
                pb::DirectoryEntry {
                    inode: 2,
                    name: "a.txt".into(),
                    kind: pb::FileKind::File as i32,
                },
                pb::DirectoryEntry {
                    inode: 3,
                    name: "b.txt".into(),
                    kind: pb::FileKind::File as i32,
                },
            ],
            end: false,
        })
    }

    fn read(
        &self,
        _request: &pb::ReadRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadResponse> {
        Ok(pb::ReadResponse {
            data: b"too many bytes".to_vec(),
        })
    }

    fn write(
        &self,
        request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        let mut invalid_write_remaining = self
            .invalid_write_remaining
            .lock()
            .expect("invalid write lock");
        if !*invalid_write_remaining {
            *invalid_write_remaining = true;
            return Ok(pb::WriteResponse {
                bytes_written: request.data.len() as u32 + 1,
            });
        }
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }
}

fn request_for_operation(operation: Operation, request_id: &str) -> RequestEnvelope {
    request_with_deadline(operation, request_id, 4_102_444_800_000_000_000)
}

fn request_for_operation_in_namespace(
    operation: Operation,
    request_id: &str,
    namespace: &str,
) -> RequestEnvelope {
    request_with_deadline_in_namespace(operation, request_id, namespace, 4_102_444_800_000_000_000)
}

fn request_with_deadline(operation: Operation, request_id: &str, deadline: u64) -> RequestEnvelope {
    request_with_deadline_in_namespace(operation, request_id, "dispatcher-ns", deadline)
}

fn request_with_deadline_in_namespace(
    operation: Operation,
    request_id: &str,
    namespace: &str,
    deadline: u64,
) -> RequestEnvelope {
    let payload = match operation {
        Operation::Lookup => RequestPayload::Lookup(pb::LookupRequest {
            path: Some(path("/file.txt").expect("valid path")),
        }),
        Operation::Getattr => RequestPayload::Getattr(pb::GetattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
        }),
        Operation::Readdir => RequestPayload::Readdir(pb::ReaddirRequest {
            path: Some(path("/").expect("valid path")),
            offset: 0,
            max_entries: 16,
        }),
        Operation::Open => RequestPayload::Open(pb::OpenRequest {
            path: Some(path("/file.txt").expect("valid path")),
            flags: 0,
            kind: pb::OpenKind::File as i32,
        }),
        Operation::Read => RequestPayload::Read(pb::ReadRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            offset: 0,
            size: 5,
        }),
        Operation::Write => RequestPayload::Write(pb::WriteRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            offset: 0,
            data: b"hello".to_vec(),
        }),
        Operation::Create => RequestPayload::Create(pb::CreateRequest {
            path: Some(path("/new.txt").expect("valid path")),
            flags: 0,
            mode: 0o644,
        }),
        Operation::Rename => RequestPayload::Rename(pb::RenameRequest {
            old_path: Some(path("/old.txt").expect("valid path")),
            new_path: Some(path("/new.txt").expect("valid path")),
        }),
        Operation::Unlink => RequestPayload::Unlink(pb::UnlinkRequest {
            path: Some(path("/file.txt").expect("valid path")),
        }),
        Operation::Mkdir => RequestPayload::Mkdir(pb::MkdirRequest {
            path: Some(path("/dir").expect("valid path")),
            mode: 0o755,
        }),
        Operation::Rmdir => RequestPayload::Rmdir(pb::RmdirRequest {
            path: Some(path("/dir").expect("valid path")),
        }),
        Operation::Statfs => RequestPayload::Statfs(pb::StatfsRequest {
            path: Some(path("/").expect("valid path")),
        }),
        Operation::Getxattr => RequestPayload::Getxattr(pb::GetxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            name: "user.key".into(),
            size: 64,
        }),
        Operation::Setxattr => RequestPayload::Setxattr(pb::SetxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            name: "user.key".into(),
            value: b"value".to_vec(),
            flags: 0,
        }),
        Operation::Listxattr => RequestPayload::Listxattr(pb::ListxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            size: 128,
        }),
        Operation::Removexattr => RequestPayload::Removexattr(pb::RemovexattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            name: "user.key".into(),
        }),
        Operation::Release => RequestPayload::Release(pb::ReleaseRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            flags: 0,
        }),
        Operation::Readlink => RequestPayload::Readlink(pb::ReadlinkRequest {
            path: Some(path("/link.txt").expect("valid path")),
        }),
        Operation::Symlink => RequestPayload::Symlink(pb::SymlinkRequest {
            path: Some(path("/link.txt").expect("valid path")),
            target: b"file.txt".to_vec(),
        }),
        Operation::Hardlink => RequestPayload::Hardlink(pb::HardlinkRequest {
            existing_path: Some(path("/file.txt").expect("valid path")),
            new_path: Some(path("/hard.txt").expect("valid path")),
        }),
        Operation::Setattr => RequestPayload::Setattr(pb::SetattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: Some(16),
            handle: None,
        }),
        Operation::Flush => RequestPayload::Flush(pb::FlushRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            lock_owner: 99,
        }),
        Operation::Fsync => RequestPayload::Fsync(pb::FsyncRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            datasync: true,
        }),
        Operation::Fsyncdir => RequestPayload::Fsyncdir(pb::FsyncdirRequest {
            path: Some(path("/").expect("valid path")),
            handle: 9,
            datasync: false,
        }),
        Operation::Getlk => RequestPayload::Getlk(pb::GetlkRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            owner: 123,
            start: 0,
            end: u64::MAX,
            typ: 1,
            pid: 42,
        }),
        Operation::Setlk => RequestPayload::Setlk(pb::SetlkRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            owner: 123,
            start: 0,
            end: u64::MAX,
            typ: 1,
            pid: 42,
            wait: false,
        }),
        Operation::Flock => RequestPayload::Flock(pb::FlockRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            owner: 123,
            operation: LOCK_EXCLUSIVE,
        }),
        Operation::CopyFileRange => RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
            input_path: Some(path("/file.txt").expect("valid path")),
            input_handle: 7,
            input_offset: 0,
            output_path: Some(path("/copy.txt").expect("valid path")),
            output_handle: 8,
            output_offset: 0,
            length: 5,
            flags: 0,
        }),
        Operation::Fallocate => RequestPayload::Fallocate(pb::FallocateRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            offset: 0,
            length: 4096,
            mode: 0,
        }),
        Operation::Lseek => RequestPayload::Lseek(pb::LseekRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            offset: 0,
            whence: SEEK_SET,
        }),
    };
    RequestEnvelope::new(
        request_id,
        namespace,
        deadline,
        pb::TraceContext::default(),
        payload,
    )
    .expect("valid request")
}

fn expected_invalidation_kind(operation: Operation) -> Option<InvalidationKind> {
    match operation.spec().effect {
        OperationEffect::ContentMutation => Some(InvalidationKind::Modify),
        OperationEffect::CreateNode => Some(InvalidationKind::Create),
        OperationEffect::RenameNode => Some(InvalidationKind::Rename),
        OperationEffect::DeleteNode => Some(InvalidationKind::Delete),
        OperationEffect::MetadataMutation => Some(InvalidationKind::Metadata),
        OperationEffect::XattrMutation => Some(InvalidationKind::Xattr),
        _ => None,
    }
}
