use fs_protocol::{
    decode_request, decode_response, encode_message, encode_request, encode_response, file_attr,
    path, pb, validate_invalidation, Errno, InvalidationKind, Operation, OperationEffect, PathRole,
    PathRootPolicy, ProtocolError, RequestEnvelope, RequestPayload, ResponseEnvelope,
    ResponseHandle, ResponseLimit, ResponsePayload, LOCK_EXCLUSIVE, LOCK_SHARED, PROTOCOL_VERSION,
    SEEK_DATA, SEEK_SET,
};
use prost::Message;

#[test]
fn protocol_envelope_round_trip_preserves_request_fields() {
    let request = read_request("protocol-read-round-trip");
    let decoded = decode_request(&encode_request(&request).expect("request encodes"))
        .expect("request decodes");

    assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
    assert_eq!(decoded.request_id, request.request_id);
    assert_eq!(decoded.operation, Operation::Read);
    assert_eq!(decoded.namespace, request.namespace);
    assert_eq!(decoded.deadline_unix_nanos, request.deadline_unix_nanos);
    assert_eq!(decoded.trace.trace_id, "trace-1");
    assert_eq!(decoded.observations[0].key, "source");
    match decoded.payload {
        RequestPayload::Read(value) => {
            assert_eq!(value.path.expect("path").path, "/file.txt");
            assert_eq!(value.handle, 7);
            assert_eq!(value.size, 5);
        }
        other => panic!("unexpected payload: {other:?}"),
    }
}

#[test]
fn protocol_response_round_trip_preserves_errno_observations_and_invalidations() {
    let request = write_request("protocol-write-response");
    let mut response = ResponseEnvelope::success_for(
        &request,
        ResponsePayload::Write(pb::WriteResponse { bytes_written: 5 }),
        vec![sample_invalidation(1)],
    )
    .expect("response is valid");
    response.observations.push(pb::Observation {
        key: "limit".into(),
        value: "ok".into(),
    });

    let decoded = decode_response(&encode_response(&response).expect("response encodes"))
        .expect("response decodes");

    assert!(decoded.ok);
    assert_eq!(decoded.errno, None);
    assert_eq!(decoded.invalidations.len(), 1);
    assert_eq!(
        decoded.invalidations[0].kind,
        InvalidationKind::Modify.wire_value()
    );
    assert_eq!(decoded.observations[0].key, "limit");

    let error = ResponseEnvelope::failure_for(&request, Errno::PermissionDenied, "denied");
    let decoded_error =
        decode_response(&encode_response(&error).expect("error encodes")).expect("error decodes");
    assert!(!decoded_error.ok);
    assert_eq!(decoded_error.errno, Some(Errno::PermissionDenied));
    assert_eq!(decoded_error.error_message, "denied");
}

#[test]
fn protocol_preserves_common_filesystem_errno_values() {
    let request = read_request("protocol-common-errno");

    for (errno, raw) in [
        (Errno::BadFileDescriptor, 9),
        (Errno::from_raw(30).expect("EROFS decodes"), 30),
        (Errno::Range, 34),
        (Errno::NotEmpty, 39),
        (Errno::from_raw(40).expect("ELOOP decodes"), 40),
        (Errno::NoData, 61),
    ] {
        assert_eq!(errno.wire_value(), raw);
        assert_eq!(Errno::try_from(raw).expect("errno decodes"), errno);

        let response = ResponseEnvelope::failure_for(&request, errno, "filesystem errno");
        let decoded = decode_response(&encode_response(&response).expect("errno response encodes"))
            .expect("errno response decodes");

        assert_eq!(decoded.errno, Some(errno));
    }
}

#[test]
fn protocol_failure_response_sanitizes_malformed_direct_request_identity() {
    let mut unsupported_version = write_request("protocol-invalid-version");
    unsupported_version.protocol_version = PROTOCOL_VERSION + 1;
    let response =
        ResponseEnvelope::failure_for(&unsupported_version, Errno::InvalidArgument, "bad version");
    response
        .validate()
        .expect("failure response uses current protocol version");
    assert_eq!(response.protocol_version, PROTOCOL_VERSION);
    assert_eq!(response.request_id, "protocol-invalid-version");
    assert_eq!(response.namespace, "ns");

    let mut empty_request_id = write_request("protocol-empty-request-id");
    empty_request_id.request_id.clear();
    let response =
        ResponseEnvelope::failure_for(&empty_request_id, Errno::InvalidArgument, "bad request id");
    response
        .validate()
        .expect("failure response has a usable fallback request id");
    assert!(!response.request_id.is_empty());
    assert_eq!(response.namespace, "ns");

    let mut empty_namespace = write_request("protocol-empty-namespace");
    empty_namespace.namespace.clear();
    let response =
        ResponseEnvelope::failure_for(&empty_namespace, Errno::InvalidArgument, "bad namespace");
    response
        .validate()
        .expect("failure response has a usable fallback namespace");
    assert_eq!(response.request_id, "protocol-empty-namespace");
    assert!(!response.namespace.is_empty());
}

#[test]
fn protocol_operation_payload_registry_covers_every_supported_operation() {
    for operation in Operation::ALL {
        let request = request_for_operation(operation, "protocol-registry");
        let decoded = decode_request(&encode_request(&request).expect("request encodes"))
            .expect("request decodes");
        assert_eq!(decoded.operation, operation);
        assert_eq!(decoded.payload.operation(), operation);

        let response =
            ResponseEnvelope::success_for(&decoded, response_for_operation(operation), Vec::new())
                .expect("response maps to request operation");
        let decoded_response =
            decode_response(&encode_response(&response).expect("response encodes"))
                .expect("response decodes");
        assert_eq!(decoded_response.operation, operation);
        assert_eq!(
            decoded_response.payload.expect("payload").operation(),
            operation
        );
    }
}

#[test]
fn protocol_operation_spec_covers_every_supported_operation() {
    assert_eq!(fs_protocol::OPERATION_SPECS.len(), Operation::ALL.len());

    for operation in Operation::ALL {
        let spec = operation.spec();
        assert_eq!(spec.operation, operation);
        assert_eq!(Operation::try_from(spec.wire_value), Ok(operation));
        assert_eq!(
            Operation::from_subject_token(spec.subject_token),
            Some(operation)
        );
        assert_eq!(operation.wire_value(), spec.wire_value);
        assert_eq!(operation.as_str(), spec.subject_token);
        assert_eq!(operation.is_mutation(), spec.effect.is_mutation());
    }
}

#[test]
fn protocol_operation_spec_describes_path_roles() {
    let hardlink = request_for_operation(Operation::Hardlink, "protocol-hardlink-roles");
    assert_eq!(hardlink.payload.primary_path(), Some("/hard.txt"));
    assert_eq!(
        hardlink.payload.path_for_role(PathRole::Target),
        Some("/hard.txt")
    );
    assert_eq!(
        hardlink.payload.path_for_role(PathRole::Source),
        Some("/file.txt")
    );
    assert_eq!(
        Operation::Hardlink.spec().path_roles[0].root,
        PathRootPolicy::NonRoot
    );

    let copy = request_for_operation(Operation::CopyFileRange, "protocol-copy-roles");
    assert_eq!(copy.payload.primary_path(), Some("/copy.txt"));
    assert_eq!(
        copy.payload.path_for_role(PathRole::Target),
        Some("/copy.txt")
    );
    assert_eq!(
        copy.payload.path_for_role(PathRole::Source),
        Some("/file.txt")
    );

    let rename = request_for_operation(Operation::Rename, "protocol-rename-roles");
    assert_eq!(rename.payload.primary_path(), Some("/old.txt"));
    assert_eq!(
        rename.payload.path_for_role(PathRole::Source),
        Some("/old.txt")
    );
    assert_eq!(
        rename.payload.path_for_role(PathRole::Target),
        Some("/new.txt")
    );
}

#[test]
fn protocol_operation_spec_describes_response_limits_and_handles() {
    assert_eq!(
        Operation::Read.spec().response_limit,
        ResponseLimit::RequestedReadBytes
    );
    assert_eq!(
        Operation::Readdir.spec().response_limit,
        ResponseLimit::RequestedDirectoryEntries
    );
    assert_eq!(
        Operation::CopyFileRange.spec().response_limit,
        ResponseLimit::RequestedCopyLength
    );

    for operation in Operation::ALL {
        let response = response_for_operation(operation);
        assert_eq!(
            response.opened_handle().is_some(),
            operation.spec().response_handle == ResponseHandle::OpenedObject,
            "{operation:?} response handle fact must match payload shape"
        );
    }
}

#[test]
fn protocol_operation_spec_effects_remain_neutral() {
    assert_eq!(Operation::Lookup.spec().effect, OperationEffect::ReadOnly);
    assert_eq!(
        Operation::Write.spec().effect,
        OperationEffect::ContentMutation
    );
    assert_eq!(Operation::Create.spec().effect, OperationEffect::CreateNode);
    assert_eq!(Operation::Rename.spec().effect, OperationEffect::RenameNode);
    assert_eq!(
        Operation::Setxattr.spec().effect,
        OperationEffect::XattrMutation
    );
    assert_eq!(Operation::Fsync.spec().effect, OperationEffect::Durability);
    assert_eq!(Operation::Setlk.spec().effect, OperationEffect::LockState);
    assert_eq!(Operation::Lseek.spec().effect, OperationEffect::SeekState);
}

#[test]
fn protocol_decode_fails_closed_for_invalid_envelopes() {
    assert!(matches!(
        decode_request(&[0xff, 0xff, 0xff]),
        Err(ProtocolError::Decode(_))
    ));

    let mut raw = raw_request(read_request("protocol-version"));
    raw.protocol_version = PROTOCOL_VERSION + 1;
    assert!(matches!(
        decode_request(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::UnsupportedVersion(_))
    ));

    let mut raw = raw_request(read_request("protocol-unknown-operation"));
    raw.operation = 999;
    raw.payload_operation = 999;
    assert!(matches!(
        decode_request(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::UnknownOperation(999))
    ));

    let mut raw = raw_request(read_request("protocol-mismatch"));
    raw.payload_operation = Operation::Write.wire_value();
    assert!(matches!(
        decode_request(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::PayloadMismatch { .. })
    ));

    let payload = encode_message(&pb::LookupRequest {
        path: Some(pb::PathDto {
            path: "../bad".into(),
        }),
    })
    .expect("payload encodes");
    let raw = pb::RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: "protocol-invalid-path".into(),
        operation: Operation::Lookup.wire_value(),
        namespace: "ns".into(),
        deadline_unix_nanos: 0,
        trace: Some(pb::TraceContext::default()),
        payload,
        payload_operation: Operation::Lookup.wire_value(),
        observations: Vec::new(),
        caller: None,
    };
    assert!(matches!(
        decode_request(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::InvalidPath(_))
    ));

    let root_create = RequestEnvelope::new(
        "protocol-root-create",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Create(pb::CreateRequest {
            path: Some(path("/").expect("root path DTO is syntactically valid")),
            flags: 0,
            mode: 0o644,
        }),
    );
    assert!(matches!(
        root_create,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let root_rename = RequestEnvelope::new(
        "protocol-root-rename",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Rename(pb::RenameRequest {
            old_path: Some(path("/").expect("root path DTO is syntactically valid")),
            new_path: Some(path("/renamed").expect("valid path")),
        }),
    );
    assert!(matches!(
        root_rename,
        Err(ProtocolError::InvalidEnvelope(_))
    ));
}

#[test]
fn protocol_rejects_overlapping_rename_requests_and_invalidations() {
    for (old_path, new_path) in [("/dir", "/dir/sub"), ("/dir/sub", "/dir"), ("/dir", "/dir")] {
        let request = RequestEnvelope::new(
            format!("protocol-overlapping-rename-{old_path}-{new_path}"),
            "ns",
            0,
            pb::TraceContext::default(),
            RequestPayload::Rename(pb::RenameRequest {
                old_path: Some(path(old_path).expect("old path DTO is valid")),
                new_path: Some(path(new_path).expect("new path DTO is valid")),
            }),
        );
        assert!(
            matches!(request, Err(ProtocolError::InvalidEnvelope(_))),
            "rename request {old_path} -> {new_path} should reject overlapping subtrees"
        );

        let mut invalidation = sample_invalidation(1);
        invalidation.kind = InvalidationKind::Rename.wire_value();
        invalidation.path.clear();
        invalidation.old_path = old_path.into();
        invalidation.new_path = new_path.into();
        assert!(
            matches!(
                validate_invalidation(&invalidation),
                Err(ProtocolError::InvalidEnvelope(_))
            ),
            "rename invalidation {old_path} -> {new_path} should reject overlapping subtrees"
        );
    }

    let valid = RequestEnvelope::new(
        "protocol-disjoint-rename",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Rename(pb::RenameRequest {
            old_path: Some(path("/dir").expect("old path DTO is valid")),
            new_path: Some(path("/dir2").expect("new path DTO is valid")),
        }),
    );
    assert!(
        valid.is_ok(),
        "path boundary matching must not reject /dir -> /dir2"
    );
}

#[test]
fn protocol_response_decode_rejects_impossible_states() {
    let request = read_request("protocol-impossible-response");
    let mut raw = raw_response(
        ResponseEnvelope::success_for(
            &request,
            ResponsePayload::Read(pb::ReadResponse {
                data: b"hello".to_vec(),
            }),
            Vec::new(),
        )
        .expect("response is valid"),
    );
    raw.errno = Errno::NotFound.wire_value();
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    raw.ok = false;
    raw.errno = Errno::Success.wire_value();
    raw.payload.clear();
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    raw.errno = -1;
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::InvalidErrno(-1))
    ));
}

#[test]
fn protocol_response_correlation_must_match_request_identity() {
    let request = read_request("protocol-correlation-request");
    let mut response = ResponseEnvelope::success_for(
        &request,
        response_for_operation(Operation::Read),
        Vec::new(),
    )
    .expect("response is valid");

    response
        .validate_for_request(&request)
        .expect("matching response validates");

    response.request_id = "other-request".into();
    assert!(matches!(
        response.validate_for_request(&request),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mut response = ResponseEnvelope::success_for(
        &request,
        response_for_operation(Operation::Read),
        Vec::new(),
    )
    .expect("response is valid");
    response.namespace = "other-namespace".into();
    assert!(matches!(
        response.validate_for_request(&request),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mut response = ResponseEnvelope::success_for(
        &request,
        response_for_operation(Operation::Read),
        Vec::new(),
    )
    .expect("response is valid");
    response.operation = Operation::Write;
    response.payload = Some(response_for_operation(Operation::Write));
    assert!(matches!(
        response.validate_for_request(&request),
        Err(ProtocolError::InvalidEnvelope(_))
    ));
}

#[test]
fn protocol_response_validation_is_request_aware() {
    let read = read_request("protocol-read-too-large");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &read,
            ResponsePayload::Read(pb::ReadResponse {
                data: b"too many bytes".to_vec(),
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    let write = write_request("protocol-write-too-large");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &write,
            ResponsePayload::Write(pb::WriteResponse { bytes_written: 6 }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    let readdir = RequestEnvelope::new(
        "protocol-readdir-too-many",
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext::default(),
        RequestPayload::Readdir(pb::ReaddirRequest {
            path: Some(path("/").expect("valid path")),
            offset: 0,
            max_entries: 1,
        }),
    )
    .expect("request is valid");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &readdir,
            ResponsePayload::Readdir(pb::ReaddirResponse {
                entries: vec![directory_entry("a.txt", 2), directory_entry("b.txt", 3)],
                end: false,
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    let getxattr = RequestEnvelope::new(
        "protocol-getxattr-too-large",
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext::default(),
        RequestPayload::Getxattr(pb::GetxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            name: "user.key".into(),
            size: 2,
        }),
    )
    .expect("request is valid");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &getxattr,
            ResponsePayload::Getxattr(pb::GetxattrResponse {
                value: b"value".to_vec(),
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    let getxattr_probe = RequestEnvelope::new(
        "protocol-getxattr-size-probe",
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext::default(),
        RequestPayload::Getxattr(pb::GetxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            name: "user.key".into(),
            size: 0,
        }),
    )
    .expect("request is valid");
    ResponseEnvelope::success_for(
        &getxattr_probe,
        ResponsePayload::Getxattr(pb::GetxattrResponse {
            value: b"value".to_vec(),
        }),
        Vec::new(),
    )
    .expect("size probe may carry required value length");

    let listxattr = RequestEnvelope::new(
        "protocol-listxattr-too-large",
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext::default(),
        RequestPayload::Listxattr(pb::ListxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            size: 4,
        }),
    )
    .expect("request is valid");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &listxattr,
            ResponsePayload::Listxattr(pb::ListxattrResponse {
                names: vec!["user.key".into()],
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));

    let listxattr_probe = RequestEnvelope::new(
        "protocol-listxattr-size-probe",
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext::default(),
        RequestPayload::Listxattr(pb::ListxattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            size: 0,
        }),
    )
    .expect("request is valid");
    ResponseEnvelope::success_for(
        &listxattr_probe,
        ResponsePayload::Listxattr(pb::ListxattrResponse {
            names: vec!["user.key".into()],
        }),
        Vec::new(),
    )
    .expect("size probe may carry required list length");

    let copy = request_for_operation(Operation::CopyFileRange, "protocol-copy-too-large");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &copy,
            ResponsePayload::CopyFileRange(pb::CopyFileRangeResponse { bytes_copied: 6 }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidResponseState(_))
    ));
}

#[test]
fn protocol_validates_mounted_posix_extension_requests() {
    let root_symlink = RequestEnvelope::new(
        "protocol-root-symlink",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Symlink(pb::SymlinkRequest {
            path: Some(path("/").expect("root path DTO is syntactically valid")),
            target: b"target".to_vec(),
        }),
    );
    assert!(matches!(
        root_symlink,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let empty_setattr = RequestEnvelope::new(
        "protocol-empty-setattr",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Setattr(pb::SetattrRequest {
            path: Some(path("/file.txt").expect("valid path")),
            mode: None,
            uid: None,
            gid: None,
            size: None,
            handle: None,
        }),
    );
    assert!(matches!(
        empty_setattr,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let missing_open_kind = RequestEnvelope::new(
        "protocol-missing-open-kind",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Open(pb::OpenRequest {
            path: Some(path("/file.txt").expect("valid path")),
            flags: 0,
            kind: pb::OpenKind::Unspecified as i32,
        }),
    );
    assert!(matches!(
        missing_open_kind,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let unknown_open_kind = RequestEnvelope::new(
        "protocol-unknown-open-kind",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Open(pb::OpenRequest {
            path: Some(path("/file.txt").expect("valid path")),
            flags: 0,
            kind: 999,
        }),
    );
    assert_eq!(unknown_open_kind, Err(ProtocolError::UnknownOpenKind(999)));

    let invalid_lock = RequestEnvelope::new(
        "protocol-invalid-lock-range",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Setlk(pb::SetlkRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            owner: 123,
            start: 9,
            end: 8,
            typ: 1,
            pid: 42,
            wait: false,
        }),
    );
    assert!(matches!(
        invalid_lock,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let invalid_flock = RequestEnvelope::new(
        "protocol-invalid-flock",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Flock(pb::FlockRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            owner: 123,
            operation: LOCK_SHARED | LOCK_EXCLUSIVE,
        }),
    );
    assert!(matches!(
        invalid_flock,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let negative_copy_offset = RequestEnvelope::new(
        "protocol-negative-copy-offset",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
            input_path: Some(path("/file.txt").expect("valid path")),
            input_handle: 7,
            input_offset: -1,
            output_path: Some(path("/copy.txt").expect("valid path")),
            output_handle: 8,
            output_offset: 0,
            length: 5,
            flags: 0,
        }),
    );
    assert!(matches!(
        negative_copy_offset,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let unsupported_copy_flags = RequestEnvelope::new(
        "protocol-copy-flags",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
            input_path: Some(path("/file.txt").expect("valid path")),
            input_handle: 7,
            input_offset: 0,
            output_path: Some(path("/copy.txt").expect("valid path")),
            output_handle: 8,
            output_offset: 0,
            length: 5,
            flags: 1,
        }),
    );
    assert!(matches!(
        unsupported_copy_flags,
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let invalid_lseek = RequestEnvelope::new(
        "protocol-negative-seek-data",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Lseek(pb::LseekRequest {
            path: Some(path("/file.txt").expect("valid path")),
            handle: 7,
            offset: -1,
            whence: SEEK_DATA,
        }),
    );
    assert!(matches!(
        invalid_lseek,
        Err(ProtocolError::InvalidEnvelope(_))
    ));
}

#[test]
fn protocol_round_trips_non_utf8_symlink_target_bytes() {
    let target = b"target-\xff".to_vec();
    let request = RequestEnvelope::new(
        "protocol-non-utf8-symlink",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Symlink(pb::SymlinkRequest {
            path: Some(path("/link.txt").expect("valid path")),
            target: target.clone(),
        }),
    )
    .expect("request is valid");
    let decoded = decode_request(&encode_request(&request).expect("request encodes"))
        .expect("request decodes");
    match decoded.payload {
        RequestPayload::Symlink(value) => assert_eq!(value.target, target),
        other => panic!("unexpected payload: {other:?}"),
    }

    let readlink_request = RequestEnvelope::new(
        "protocol-non-utf8-readlink",
        "ns",
        0,
        pb::TraceContext::default(),
        RequestPayload::Readlink(pb::ReadlinkRequest {
            path: Some(path("/link.txt").expect("valid path")),
        }),
    )
    .expect("request is valid");
    let response = ResponseEnvelope::success_for(
        &readlink_request,
        ResponsePayload::Readlink(pb::ReadlinkResponse {
            target: b"target-\xff".to_vec(),
        }),
        Vec::new(),
    )
    .expect("response is valid");
    let decoded = decode_response(&encode_response(&response).expect("response encodes"))
        .expect("response decodes");
    match decoded.payload.expect("payload") {
        ResponsePayload::Readlink(value) => assert_eq!(value.target, b"target-\xff"),
        other => panic!("unexpected payload: {other:?}"),
    }
}

#[test]
fn protocol_rejects_kind_specific_malformed_invalidations() {
    let mut create = sample_invalidation(1);
    create.kind = InvalidationKind::Create.wire_value();
    create.path = "/".into();
    assert!(matches!(
        validate_invalidation(&create),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mut delete = sample_invalidation(1);
    delete.kind = InvalidationKind::Delete.wire_value();
    delete.path.clear();
    assert!(matches!(
        validate_invalidation(&delete),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    delete.path = "/".into();
    assert!(matches!(
        validate_invalidation(&delete),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mut rename = sample_invalidation(2);
    rename.kind = InvalidationKind::Rename.wire_value();
    rename.path.clear();
    rename.old_path = "/old.txt".into();
    rename.new_path.clear();
    assert!(matches!(
        validate_invalidation(&rename),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    rename.old_path = "/".into();
    rename.new_path = "/new.txt".into();
    assert!(matches!(
        validate_invalidation(&rename),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mut modify = sample_invalidation(3);
    modify.kind = InvalidationKind::Modify.wire_value();
    modify.path.clear();
    assert!(matches!(
        validate_invalidation(&modify),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let request = write_request("protocol-invalid-response-invalidation");
    let mut invalid_response_invalidation = sample_invalidation(4);
    invalid_response_invalidation.kind = InvalidationKind::Delete.wire_value();
    invalid_response_invalidation.path = "/".into();
    assert!(matches!(
        ResponseEnvelope::success_for(
            &request,
            ResponsePayload::Write(pb::WriteResponse { bytes_written: 5 }),
            vec![invalid_response_invalidation],
        ),
        Err(ProtocolError::InvalidEnvelope(_))
    ));
}

#[test]
fn protocol_response_decode_rejects_unknown_file_kinds() {
    let lookup_payload = pb::LookupResponse {
        attr: Some(pb::FileAttr {
            inode: 2,
            size: 0,
            kind: 999,
            perm: 0o644,
            mtime_unix_nanos: 0,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_unix_nanos: 0,
            ctime_unix_nanos: 0,
            crtime_unix_nanos: 0,
        }),
    };
    let raw = raw_success_response(
        &request_for_operation(Operation::Lookup, "protocol-unknown-attr-kind"),
        Operation::Lookup,
        encode_message(&lookup_payload).expect("lookup payload encodes"),
    );
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::UnknownFileKind(999))
    ));

    let lookup_payload = pb::LookupResponse {
        attr: Some(pb::FileAttr {
            inode: 2,
            size: 0,
            kind: pb::FileKind::Unknown as i32,
            perm: 0o644,
            mtime_unix_nanos: 0,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_unix_nanos: 0,
            ctime_unix_nanos: 0,
            crtime_unix_nanos: 0,
        }),
    };
    let raw = raw_success_response(
        &request_for_operation(Operation::Lookup, "protocol-zero-attr-kind"),
        Operation::Lookup,
        encode_message(&lookup_payload).expect("lookup payload encodes"),
    );
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let readdir_payload = pb::ReaddirResponse {
        entries: vec![pb::DirectoryEntry {
            inode: 2,
            name: "file.txt".into(),
            kind: 999,
        }],
        end: true,
    };
    let raw = raw_success_response(
        &request_for_operation(Operation::Readdir, "protocol-unknown-entry-kind"),
        Operation::Readdir,
        encode_message(&readdir_payload).expect("readdir payload encodes"),
    );
    assert!(matches!(
        decode_response(&encode_message(&raw).expect("raw encodes")),
        Err(ProtocolError::UnknownFileKind(999))
    ));
}

#[test]
fn protocol_rejects_operation_specific_attr_kind_mismatches() {
    let create = request_for_operation(Operation::Create, "protocol-create-wrong-kind");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &create,
            ResponsePayload::Create(pb::CreateResponse {
                attr: Some(file_attr(3, pb::FileKind::Directory, 0)),
                handle: 8,
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidEnvelope(_))
    ));

    let mkdir = request_for_operation(Operation::Mkdir, "protocol-mkdir-wrong-kind");
    assert!(matches!(
        ResponseEnvelope::success_for(
            &mkdir,
            ResponsePayload::Mkdir(pb::LookupResponse {
                attr: Some(file_attr(4, pb::FileKind::File, 0)),
            }),
            Vec::new(),
        ),
        Err(ProtocolError::InvalidEnvelope(_))
    ));
}

#[test]
fn protocol_golden_fixtures_are_stable() {
    let read = encode_request(&read_request("fixture-read")).expect("read fixture encodes");
    let write = encode_request(&write_request("fixture-write")).expect("write fixture encodes");
    let error = encode_response(&ResponseEnvelope::failure_for(
        &read_request("fixture-error"),
        Errno::NotFound,
        "not found",
    ))
    .expect("error fixture encodes");
    let directory = encode_response(
        &ResponseEnvelope::success_for(
            &request_for_operation(Operation::Readdir, "fixture-directory"),
            response_for_operation(Operation::Readdir),
            Vec::new(),
        )
        .expect("directory fixture response is valid"),
    )
    .expect("directory fixture encodes");
    let xattr = encode_response(
        &ResponseEnvelope::success_for(
            &request_for_operation(Operation::Getxattr, "fixture-xattr"),
            response_for_operation(Operation::Getxattr),
            Vec::new(),
        )
        .expect("xattr fixture response is valid"),
    )
    .expect("xattr fixture encodes");
    let symlink = encode_request(&request_for_operation(
        Operation::Symlink,
        "fixture-symlink",
    ))
    .expect("symlink fixture encodes");
    let setattr = encode_response(
        &ResponseEnvelope::success_for(
            &request_for_operation(Operation::Setattr, "fixture-setattr"),
            response_for_operation(Operation::Setattr),
            Vec::new(),
        )
        .expect("setattr fixture response is valid"),
    )
    .expect("setattr fixture encodes");
    let copy_file_range = encode_response(
        &ResponseEnvelope::success_for(
            &request_for_operation(Operation::CopyFileRange, "fixture-copy-file-range"),
            response_for_operation(Operation::CopyFileRange),
            Vec::new(),
        )
        .expect("copy_file_range fixture response is valid"),
    )
    .expect("copy_file_range fixture encodes");
    let unsupported = encode_response(&ResponseEnvelope::failure_for(
        &request_for_operation(Operation::Lseek, "fixture-lseek-unsupported"),
        Errno::NotSupported,
        "operation is not supported by this backend",
    ))
    .expect("unsupported fixture encodes");
    let invalidation = encode_message(&sample_invalidation(42)).expect("invalidation encodes");

    assert_fixture(
        "read",
        include_str!("../fixtures/read_request.pb.hex"),
        &read,
    );
    assert_fixture(
        "write",
        include_str!("../fixtures/write_request.pb.hex"),
        &write,
    );
    assert_fixture(
        "error",
        include_str!("../fixtures/error_response.pb.hex"),
        &error,
    );
    assert_fixture(
        "directory",
        include_str!("../fixtures/directory_response.pb.hex"),
        &directory,
    );
    assert_fixture(
        "xattr",
        include_str!("../fixtures/xattr_response.pb.hex"),
        &xattr,
    );
    assert_fixture(
        "symlink",
        include_str!("../fixtures/symlink_request.pb.hex"),
        &symlink,
    );
    assert_fixture(
        "setattr",
        include_str!("../fixtures/setattr_response.pb.hex"),
        &setattr,
    );
    assert_fixture(
        "copy_file_range",
        include_str!("../fixtures/copy_file_range_response.pb.hex"),
        &copy_file_range,
    );
    assert_fixture(
        "unsupported",
        include_str!("../fixtures/unsupported_response.pb.hex"),
        &unsupported,
    );
    assert_fixture(
        "invalidation",
        include_str!("../fixtures/invalidation.pb.hex"),
        &invalidation,
    );
}

#[test]
#[ignore = "prints current fixture bytes for intentional fixture updates"]
fn protocol_print_golden_fixture_hex() {
    let fixtures = [
        (
            "read_request.pb.hex",
            encode_request(&read_request("fixture-read")).expect("read encodes"),
        ),
        (
            "write_request.pb.hex",
            encode_request(&write_request("fixture-write")).expect("write encodes"),
        ),
        (
            "error_response.pb.hex",
            encode_response(&ResponseEnvelope::failure_for(
                &read_request("fixture-error"),
                Errno::NotFound,
                "not found",
            ))
            .expect("error encodes"),
        ),
        (
            "directory_response.pb.hex",
            encode_response(
                &ResponseEnvelope::success_for(
                    &request_for_operation(Operation::Readdir, "fixture-directory"),
                    response_for_operation(Operation::Readdir),
                    Vec::new(),
                )
                .expect("directory response"),
            )
            .expect("directory encodes"),
        ),
        (
            "xattr_response.pb.hex",
            encode_response(
                &ResponseEnvelope::success_for(
                    &request_for_operation(Operation::Getxattr, "fixture-xattr"),
                    response_for_operation(Operation::Getxattr),
                    Vec::new(),
                )
                .expect("xattr response"),
            )
            .expect("xattr encodes"),
        ),
        (
            "symlink_request.pb.hex",
            encode_request(&request_for_operation(
                Operation::Symlink,
                "fixture-symlink",
            ))
            .expect("symlink encodes"),
        ),
        (
            "setattr_response.pb.hex",
            encode_response(
                &ResponseEnvelope::success_for(
                    &request_for_operation(Operation::Setattr, "fixture-setattr"),
                    response_for_operation(Operation::Setattr),
                    Vec::new(),
                )
                .expect("setattr response"),
            )
            .expect("setattr encodes"),
        ),
        (
            "copy_file_range_response.pb.hex",
            encode_response(
                &ResponseEnvelope::success_for(
                    &request_for_operation(Operation::CopyFileRange, "fixture-copy-file-range"),
                    response_for_operation(Operation::CopyFileRange),
                    Vec::new(),
                )
                .expect("copy_file_range response"),
            )
            .expect("copy_file_range encodes"),
        ),
        (
            "unsupported_response.pb.hex",
            encode_response(&ResponseEnvelope::failure_for(
                &request_for_operation(Operation::Lseek, "fixture-lseek-unsupported"),
                Errno::NotSupported,
                "operation is not supported by this backend",
            ))
            .expect("unsupported encodes"),
        ),
        (
            "invalidation.pb.hex",
            encode_message(&sample_invalidation(42)).expect("invalidation encodes"),
        ),
    ];

    for (name, bytes) in fixtures {
        println!("{name}: {}", to_hex(&bytes));
    }
}

fn request_for_operation(operation: Operation, request_id: &str) -> RequestEnvelope {
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
            path: Some(path("/created.txt").expect("valid path")),
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
    let mut request = RequestEnvelope::new(
        request_id,
        "ns",
        4_102_444_800_000_000_000,
        pb::TraceContext {
            trace_id: "trace-1".into(),
            parent_id: "parent-1".into(),
            entries: vec![pb::TraceEntry {
                key: "span".into(),
                value: "root".into(),
            }],
        },
        payload,
    )
    .expect("request is valid");
    request.observations.push(pb::Observation {
        key: "source".into(),
        value: "protocol-test".into(),
    });
    request
}

fn response_for_operation(operation: Operation) -> ResponsePayload {
    match operation {
        Operation::Lookup => ResponsePayload::Lookup(pb::LookupResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        }),
        Operation::Getattr => ResponsePayload::Getattr(pb::GetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        }),
        Operation::Readdir => ResponsePayload::Readdir(pb::ReaddirResponse {
            entries: vec![pb::DirectoryEntry {
                inode: 2,
                name: "file.txt".into(),
                kind: pb::FileKind::File as i32,
            }],
            end: true,
        }),
        Operation::Open => ResponsePayload::Open(pb::OpenResponse {
            handle: 7,
            flags: 0,
        }),
        Operation::Read => ResponsePayload::Read(pb::ReadResponse {
            data: b"hello".to_vec(),
        }),
        Operation::Write => ResponsePayload::Write(pb::WriteResponse { bytes_written: 5 }),
        Operation::Create => ResponsePayload::Create(pb::CreateResponse {
            attr: Some(file_attr(3, pb::FileKind::File, 0)),
            handle: 8,
        }),
        Operation::Rename => ResponsePayload::Rename(pb::EmptyResponse {}),
        Operation::Unlink => ResponsePayload::Unlink(pb::EmptyResponse {}),
        Operation::Mkdir => ResponsePayload::Mkdir(pb::LookupResponse {
            attr: Some(file_attr(4, pb::FileKind::Directory, 0)),
        }),
        Operation::Rmdir => ResponsePayload::Rmdir(pb::EmptyResponse {}),
        Operation::Statfs => ResponsePayload::Statfs(pb::StatfsResponse {
            stat: Some(pb::StatFs {
                blocks: 100,
                blocks_free: 50,
                files: 10,
                files_free: 5,
                block_size: 4096,
                name_max: 255,
                blocks_available: 40,
                fragment_size: 2048,
            }),
        }),
        Operation::Getxattr => ResponsePayload::Getxattr(pb::GetxattrResponse {
            value: b"value".to_vec(),
        }),
        Operation::Setxattr => ResponsePayload::Setxattr(pb::EmptyResponse {}),
        Operation::Listxattr => ResponsePayload::Listxattr(pb::ListxattrResponse {
            names: vec!["user.key".into()],
        }),
        Operation::Removexattr => ResponsePayload::Removexattr(pb::EmptyResponse {}),
        Operation::Release => ResponsePayload::Release(pb::EmptyResponse {}),
        Operation::Readlink => ResponsePayload::Readlink(pb::ReadlinkResponse {
            target: b"file.txt".to_vec(),
        }),
        Operation::Symlink => ResponsePayload::Symlink(pb::SymlinkResponse {
            attr: Some(file_attr(5, pb::FileKind::Symlink, 8)),
        }),
        Operation::Hardlink => ResponsePayload::Hardlink(pb::HardlinkResponse {
            attr: Some(file_attr(6, pb::FileKind::File, 128)),
        }),
        Operation::Setattr => ResponsePayload::Setattr(pb::SetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 16)),
        }),
        Operation::Flush => ResponsePayload::Flush(pb::EmptyResponse {}),
        Operation::Fsync => ResponsePayload::Fsync(pb::EmptyResponse {}),
        Operation::Fsyncdir => ResponsePayload::Fsyncdir(pb::EmptyResponse {}),
        Operation::Getlk => ResponsePayload::Getlk(pb::GetlkResponse {
            lock: Some(pb::FileLock {
                start: 0,
                end: 7,
                typ: 1,
                pid: 43,
            }),
        }),
        Operation::Setlk => ResponsePayload::Setlk(pb::EmptyResponse {}),
        Operation::Flock => ResponsePayload::Flock(pb::EmptyResponse {}),
        Operation::CopyFileRange => {
            ResponsePayload::CopyFileRange(pb::CopyFileRangeResponse { bytes_copied: 5 })
        }
        Operation::Fallocate => ResponsePayload::Fallocate(pb::EmptyResponse {}),
        Operation::Lseek => ResponsePayload::Lseek(pb::LseekResponse { offset: 5 }),
    }
}

fn read_request(request_id: &str) -> RequestEnvelope {
    request_for_operation(Operation::Read, request_id)
}

fn write_request(request_id: &str) -> RequestEnvelope {
    request_for_operation(Operation::Write, request_id)
}

fn sample_invalidation(sequence: u64) -> pb::Invalidation {
    pb::Invalidation {
        namespace: "ns".into(),
        sequence,
        kind: InvalidationKind::Modify.wire_value(),
        path: "/file.txt".into(),
        old_path: String::new(),
        new_path: String::new(),
        inode: 2,
        handle: 7,
        request_id: "fixture-write".into(),
    }
}

fn directory_entry(name: &str, inode: u64) -> pb::DirectoryEntry {
    pb::DirectoryEntry {
        inode,
        name: name.into(),
        kind: pb::FileKind::File as i32,
    }
}

fn raw_request(request: RequestEnvelope) -> pb::RequestEnvelope {
    pb::RequestEnvelope::decode(
        encode_request(&request)
            .expect("request encodes")
            .as_slice(),
    )
    .expect("raw request decodes")
}

fn raw_response(response: ResponseEnvelope) -> pb::ResponseEnvelope {
    pb::ResponseEnvelope::decode(
        encode_response(&response)
            .expect("response encodes")
            .as_slice(),
    )
    .expect("raw response decodes")
}

fn raw_success_response(
    request: &RequestEnvelope,
    operation: Operation,
    payload: Vec<u8>,
) -> pb::ResponseEnvelope {
    pb::ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        operation: operation.wire_value(),
        namespace: request.namespace.clone(),
        deadline_unix_nanos: request.deadline_unix_nanos,
        trace: Some(request.trace.clone()),
        payload,
        ok: true,
        errno: Errno::Success.wire_value(),
        error_message: String::new(),
        observations: Vec::new(),
        invalidations: Vec::new(),
        payload_operation: operation.wire_value(),
    }
}

fn assert_fixture(name: &str, expected_hex: &str, bytes: &[u8]) {
    let expected = parse_hex(expected_hex);
    assert_eq!(
        expected,
        bytes,
        "fixture {name} changed; current hex: {}",
        to_hex(bytes)
    );
}

fn parse_hex(value: &str) -> Vec<u8> {
    let compact: String = value.chars().filter(|ch| !ch.is_whitespace()).collect();
    assert!(
        compact.len().is_multiple_of(2),
        "hex fixture must have an even number of digits"
    );
    compact
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("fixture hex is utf8");
            u8::from_str_radix(text, 16).expect("fixture hex byte parses")
        })
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
