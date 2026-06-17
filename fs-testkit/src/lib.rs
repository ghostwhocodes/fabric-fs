use fs_core::{
    now_unix_nanos, FileSystemService, FsError, FsResult, RpcClient, RpcError, RpcMetadata,
};
use fs_protocol::{
    encode_request, path, pb, Errno, Operation, RequestEnvelope, RequestPayload, ResponseEnvelope,
    LOCK_EXCLUSIVE, SEEK_SET,
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct RecordingFs {
    inner: Arc<Mutex<RecordingState>>,
}

#[derive(Default)]
struct RecordingState {
    calls: Vec<Operation>,
    fail_next: Option<(Operation, FsError)>,
}

#[derive(Clone, Default)]
pub struct ContradictoryResponseFs {
    inner: Arc<Mutex<ContradictoryResponseState>>,
}

#[derive(Default)]
struct ContradictoryResponseState {
    returned_invalid_write: bool,
}

impl RecordingFs {
    pub fn calls(&self) -> Vec<Operation> {
        self.inner
            .lock()
            .expect("recording state poisoned")
            .calls
            .clone()
    }

    pub fn fail_next(&self, operation: Operation, error: FsError) {
        self.inner
            .lock()
            .expect("recording state poisoned")
            .fail_next = Some((operation, error));
    }

    fn record(&self, operation: Operation) -> FsResult<()> {
        let mut state = self.inner.lock().expect("recording state poisoned");
        state.calls.push(operation);
        if state
            .fail_next
            .as_ref()
            .is_some_and(|(failed_operation, _)| *failed_operation == operation)
        {
            let (_, error) = state.fail_next.take().expect("checked above");
            return Err(error);
        }
        Ok(())
    }
}

impl FileSystemService for RecordingFs {
    fn lookup(
        &self,
        _request: &pb::LookupRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        self.record(Operation::Lookup)?;
        Ok(pb::LookupResponse {
            attr: Some(file_attr(2)),
        })
    }

    fn getattr(
        &self,
        _request: &pb::GetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetattrResponse> {
        self.record(Operation::Getattr)?;
        Ok(pb::GetattrResponse {
            attr: Some(file_attr(2)),
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
            attr: Some(file_attr(3)),
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
            attr: Some(directory_attr(4)),
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
                blocks: 100,
                blocks_free: 50,
                files: 10,
                files_free: 5,
                block_size: 4096,
                name_max: 255,
                blocks_available: 40,
                fragment_size: 2048,
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
            target: "file.txt".into(),
        })
    }

    fn symlink(
        &self,
        _request: &pb::SymlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SymlinkResponse> {
        self.record(Operation::Symlink)?;
        Ok(pb::SymlinkResponse {
            attr: Some(fs_protocol::file_attr(5, pb::FileKind::Symlink, 8)),
        })
    }

    fn hardlink(
        &self,
        _request: &pb::HardlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::HardlinkResponse> {
        self.record(Operation::Hardlink)?;
        Ok(pb::HardlinkResponse {
            attr: Some(file_attr(6)),
        })
    }

    fn setattr(
        &self,
        _request: &pb::SetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SetattrResponse> {
        self.record(Operation::Setattr)?;
        Ok(pb::SetattrResponse {
            attr: Some(fs_protocol::file_attr(2, pb::FileKind::File, 16)),
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

impl FileSystemService for ContradictoryResponseFs {
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
        let mut state = self
            .inner
            .lock()
            .expect("contradictory response state poisoned");
        if !state.returned_invalid_write {
            state.returned_invalid_write = true;
            return Ok(pb::WriteResponse {
                bytes_written: request.data.len() as u32 + 1,
            });
        }
        Ok(pb::WriteResponse {
            bytes_written: request.data.len() as u32,
        })
    }

    fn copy_file_range(
        &self,
        request: &pb::CopyFileRangeRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CopyFileRangeResponse> {
        Ok(pb::CopyFileRangeResponse {
            bytes_copied: request.length + 1,
        })
    }
}

pub fn request_for_operation(operation: Operation, request_id: &str) -> RequestEnvelope {
    request_for_operation_in_namespace(operation, request_id, "test-namespace")
}

pub fn request_for_operation_in_namespace(
    operation: Operation,
    request_id: &str,
    namespace: &str,
) -> RequestEnvelope {
    let payload = match operation {
        Operation::Lookup => RequestPayload::Lookup(pb::LookupRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
        }),
        Operation::Getattr => RequestPayload::Getattr(pb::GetattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
        }),
        Operation::Readdir => RequestPayload::Readdir(pb::ReaddirRequest {
            path: Some(path("/").expect("fixture path is valid")),
            offset: 0,
            max_entries: 32,
        }),
        Operation::Open => RequestPayload::Open(pb::OpenRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            flags: 0,
            kind: pb::OpenKind::File as i32,
        }),
        Operation::Read => RequestPayload::Read(pb::ReadRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            offset: 0,
            size: 5,
        }),
        Operation::Write => RequestPayload::Write(pb::WriteRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            offset: 0,
            data: b"hello".to_vec(),
        }),
        Operation::Create => RequestPayload::Create(pb::CreateRequest {
            path: Some(path("/new.txt").expect("fixture path is valid")),
            flags: 0,
            mode: 0o644,
        }),
        Operation::Rename => RequestPayload::Rename(pb::RenameRequest {
            old_path: Some(path("/old.txt").expect("fixture path is valid")),
            new_path: Some(path("/new.txt").expect("fixture path is valid")),
        }),
        Operation::Unlink => RequestPayload::Unlink(pb::UnlinkRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
        }),
        Operation::Mkdir => RequestPayload::Mkdir(pb::MkdirRequest {
            path: Some(path("/dir").expect("fixture path is valid")),
            mode: 0o755,
        }),
        Operation::Rmdir => RequestPayload::Rmdir(pb::RmdirRequest {
            path: Some(path("/dir").expect("fixture path is valid")),
        }),
        Operation::Statfs => RequestPayload::Statfs(pb::StatfsRequest {
            path: Some(path("/").expect("fixture path is valid")),
        }),
        Operation::Getxattr => RequestPayload::Getxattr(pb::GetxattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            name: "user.key".into(),
            size: 64,
        }),
        Operation::Setxattr => RequestPayload::Setxattr(pb::SetxattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            name: "user.key".into(),
            value: b"value".to_vec(),
            flags: 0,
        }),
        Operation::Listxattr => RequestPayload::Listxattr(pb::ListxattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            size: 128,
        }),
        Operation::Removexattr => RequestPayload::Removexattr(pb::RemovexattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            name: "user.key".into(),
        }),
        Operation::Release => RequestPayload::Release(pb::ReleaseRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            flags: 0,
        }),
        Operation::Readlink => RequestPayload::Readlink(pb::ReadlinkRequest {
            path: Some(path("/link.txt").expect("fixture path is valid")),
        }),
        Operation::Symlink => RequestPayload::Symlink(pb::SymlinkRequest {
            path: Some(path("/link.txt").expect("fixture path is valid")),
            target: "file.txt".into(),
        }),
        Operation::Hardlink => RequestPayload::Hardlink(pb::HardlinkRequest {
            existing_path: Some(path("/file.txt").expect("fixture path is valid")),
            new_path: Some(path("/hard.txt").expect("fixture path is valid")),
        }),
        Operation::Setattr => RequestPayload::Setattr(pb::SetattrRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: Some(16),
            handle: None,
        }),
        Operation::Flush => RequestPayload::Flush(pb::FlushRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            lock_owner: 99,
        }),
        Operation::Fsync => RequestPayload::Fsync(pb::FsyncRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            datasync: true,
        }),
        Operation::Fsyncdir => RequestPayload::Fsyncdir(pb::FsyncdirRequest {
            path: Some(path("/").expect("fixture path is valid")),
            handle: 9,
            datasync: false,
        }),
        Operation::Getlk => RequestPayload::Getlk(pb::GetlkRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            owner: 123,
            start: 0,
            end: u64::MAX,
            typ: 1,
            pid: 42,
        }),
        Operation::Setlk => RequestPayload::Setlk(pb::SetlkRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            owner: 123,
            start: 0,
            end: u64::MAX,
            typ: 1,
            pid: 42,
            wait: false,
        }),
        Operation::Flock => RequestPayload::Flock(pb::FlockRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            owner: 123,
            operation: LOCK_EXCLUSIVE,
        }),
        Operation::CopyFileRange => RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
            input_path: Some(path("/file.txt").expect("fixture path is valid")),
            input_handle: 7,
            input_offset: 0,
            output_path: Some(path("/copy.txt").expect("fixture path is valid")),
            output_handle: 8,
            output_offset: 0,
            length: 5,
            flags: 0,
        }),
        Operation::Fallocate => RequestPayload::Fallocate(pb::FallocateRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            offset: 0,
            length: 4096,
            mode: 0,
        }),
        Operation::Lseek => RequestPayload::Lseek(pb::LseekRequest {
            path: Some(path("/file.txt").expect("fixture path is valid")),
            handle: 7,
            offset: 0,
            whence: SEEK_SET,
        }),
    };

    let mut request = RequestEnvelope::new(
        request_id,
        namespace,
        future_deadline(),
        pb::TraceContext {
            trace_id: "trace-1".into(),
            parent_id: "parent-1".into(),
            entries: vec![pb::TraceEntry {
                key: "component".into(),
                value: "testkit".into(),
            }],
        },
        payload,
    )
    .expect("fixture request is valid");
    request.observations.push(pb::Observation {
        key: "fixture".into(),
        value: operation.as_str().into(),
    });
    request
}

pub fn unsupported_version_request(request_id: &str) -> RequestEnvelope {
    let mut request = request_for_operation(Operation::Write, request_id);
    request.protocol_version += 1;
    request
}

pub fn empty_request_id_request() -> RequestEnvelope {
    let mut request = request_for_operation(Operation::Write, "empty-request-id");
    request.request_id.clear();
    request
}

pub fn empty_namespace_request(request_id: &str) -> RequestEnvelope {
    let mut request = request_for_operation(Operation::Write, request_id);
    request.namespace.clear();
    request
}

pub fn mismatched_operation_request(request_id: &str) -> RequestEnvelope {
    let mut request = request_for_operation(Operation::Write, request_id);
    request.operation = Operation::Lookup;
    request
}

pub fn invalid_path_request(request_id: &str) -> RequestEnvelope {
    let mut request = request_for_operation(Operation::Write, request_id);
    match &mut request.payload {
        RequestPayload::Write(value) => {
            value.path = Some(pb::PathDto {
                path: "relative/path".into(),
            });
        }
        other => panic!("expected write payload, got {other:?}"),
    }
    request
}

pub fn readdir_request_with_max_entries(request_id: &str, max_entries: u32) -> RequestEnvelope {
    RequestEnvelope::new(
        request_id,
        "test-namespace",
        future_deadline(),
        pb::TraceContext {
            trace_id: "trace-1".into(),
            parent_id: "parent-1".into(),
            entries: vec![pb::TraceEntry {
                key: "component".into(),
                value: "testkit".into(),
            }],
        },
        RequestPayload::Readdir(pb::ReaddirRequest {
            path: Some(path("/").expect("fixture path is valid")),
            offset: 0,
            max_entries,
        }),
    )
    .expect("fixture request is valid")
}

pub fn fuse_create_request_frame_len(namespace: &str, name: &str) -> usize {
    let request = RequestEnvelope::new(
        "fuse-1",
        namespace,
        0,
        pb::TraceContext::default(),
        RequestPayload::Create(pb::CreateRequest {
            path: Some(path(format!("/{name}")).expect("fixture path is valid")),
            flags: 0,
            mode: 0o644,
        }),
    )
    .expect("fixture request is valid");
    encode_request(&request)
        .expect("fixture request encodes")
        .len()
}

pub fn future_deadline() -> u64 {
    now_unix_nanos() + 60_000_000_000
}

pub fn file_attr(inode: u64) -> pb::FileAttr {
    fs_protocol::file_attr(inode, pb::FileKind::File, 128)
}

pub fn directory_attr(inode: u64) -> pb::FileAttr {
    fs_protocol::directory_attr(inode)
}

pub fn assert_success(response: &ResponseEnvelope, operation: Operation) {
    assert!(response.ok, "expected successful response: {response:?}");
    assert_eq!(response.operation, operation);
    assert!(response.errno.is_none());
    assert!(response.payload.is_some());
}

pub fn assert_errno(response: &ResponseEnvelope, errno: Errno) {
    assert!(!response.ok, "expected errno response: {response:?}");
    assert_eq!(response.errno, Some(errno));
    assert!(response.payload.is_none());
}

pub fn assert_basic_transport_conformance(client: &dyn RpcClient) {
    let lookup = request_for_operation(Operation::Lookup, "conformance-lookup");
    let response = client.call(lookup.clone()).expect("lookup call succeeds");
    assert_success(&response, Operation::Lookup);
    assert_eq!(response.request_id, lookup.request_id);
    assert_eq!(response.trace.trace_id, lookup.trace.trace_id);

    let write = request_for_operation(Operation::Write, "conformance-write");
    let response = client.call(write).expect("write call succeeds");
    assert_success(&response, Operation::Write);
    assert_eq!(response.invalidations.len(), 1);
    assert_eq!(response.invalidations[0].sequence, 1);

    let read = request_for_operation(Operation::Read, "conformance-read");
    let response = client.call(read).expect("read call succeeds");
    assert_success(&response, Operation::Read);
    assert!(response.invalidations.is_empty());

    for operation in Operation::ALL {
        let request =
            request_for_operation(operation, &format!("conformance-{}", operation.as_str()));
        let response = client
            .call(request.clone())
            .unwrap_or_else(|error| panic!("operation {operation:?} transport failed: {error:?}"));
        assert_success(&response, operation);
        response
            .validate_for_request(&request)
            .unwrap_or_else(|error| panic!("operation {operation:?} response invalid: {error}"));
    }
}

pub fn assert_response_invalidations_are_not_replayed(client: &dyn RpcClient, request_id: &str) {
    let response = client
        .call(request_for_operation(Operation::Write, request_id))
        .expect("write call succeeds");
    assert_success(&response, Operation::Write);
    assert_eq!(response.invalidations.len(), 1);
    let namespace = response.invalidations[0].namespace.clone();
    assert!(
        client
            .drain_invalidations(&namespace)
            .expect("drain succeeds")
            .is_empty(),
        "response invalidations must not be replayed through the out-of-band drain"
    );
}

pub fn assert_direct_transport_conformance<C, MakeRecording, MakeContradictory, Disconnect>(
    label: &str,
    make_recording_client: MakeRecording,
    make_contradictory_client: MakeContradictory,
    disconnect: Disconnect,
) where
    C: RpcClient,
    MakeRecording: Fn() -> (C, RecordingFs),
    MakeContradictory: Fn(ContradictoryResponseFs) -> C,
    Disconnect: Fn(&C),
{
    let (client, _) = make_recording_client();
    assert_basic_transport_conformance(&client);

    let (client, service) = make_recording_client();
    assert_transport_maps_filesystem_errno(&client, &service, label);

    let (client, service) = make_recording_client();
    assert_transport_rejects_deadline_before_dispatch(&client, &service, label);

    let (client, service) = make_recording_client();
    assert_transport_rejects_invalid_typed_envelopes_before_dispatch(&client, &service, label);

    assert_transport_rejects_request_contradictory_handler_successes(
        label,
        make_contradictory_client,
    );

    let (client, _) = make_recording_client();
    assert_response_invalidations_are_not_replayed(
        &client,
        &format!("{label}-no-invalidation-replay"),
    );

    let (client, _) = make_recording_client();
    assert_transport_returns_ordered_response_invalidations_without_replay(&client, label);

    let (client, _) = make_recording_client();
    assert_transport_interleaves_namespaces_without_sequence_gaps(&client, label);

    let (client, _) = make_recording_client();
    assert_transport_connection_loss_maps_to_transport_error(&client, disconnect, label);
}

pub fn assert_serialized_transport_conformance<
    C,
    MakeRecording,
    MakeContradictory,
    MakeLimited,
    CallBytes,
    Disconnect,
>(
    label: &str,
    make_recording_client: MakeRecording,
    make_contradictory_client: MakeContradictory,
    make_limited_client: MakeLimited,
    call_bytes: CallBytes,
    disconnect: Disconnect,
) where
    C: RpcClient,
    MakeRecording: Fn() -> (C, RecordingFs),
    MakeContradictory: Fn(ContradictoryResponseFs) -> C,
    MakeLimited: Fn() -> C,
    CallBytes: Fn(&C, &[u8]) -> Result<Vec<u8>, RpcError>,
    Disconnect: Fn(&C),
{
    let (client, _) = make_recording_client();
    assert_basic_transport_conformance(&client);

    let (client, _) = make_recording_client();
    assert_serialized_transport_uses_wire_decode_and_rejects_malformed_bytes(
        &client,
        &call_bytes,
        label,
    );

    let (client, service) = make_recording_client();
    assert_transport_maps_filesystem_errno(&client, &service, label);

    let (client, service) = make_recording_client();
    assert_transport_rejects_deadline_before_dispatch(&client, &service, label);

    assert_transport_rejects_request_contradictory_handler_successes(
        label,
        make_contradictory_client,
    );

    let (client, _) = make_recording_client();
    assert_response_invalidations_are_not_replayed(
        &client,
        &format!("{label}-no-invalidation-replay"),
    );

    let client = make_limited_client();
    assert_transport_rejects_oversized_frames(&client, label);

    let (client, _) = make_recording_client();
    assert_transport_returns_ordered_response_invalidations_without_replay(&client, label);

    let (client, _) = make_recording_client();
    assert_transport_interleaves_namespaces_without_sequence_gaps(&client, label);

    let (client, _) = make_recording_client();
    assert_transport_connection_loss_maps_to_transport_error(&client, disconnect, label);
}

pub fn assert_serialized_transport_uses_wire_decode_and_rejects_malformed_bytes<C, CallBytes>(
    client: &C,
    call_bytes: CallBytes,
    label: &str,
) where
    C: RpcClient,
    CallBytes: Fn(&C, &[u8]) -> Result<Vec<u8>, RpcError>,
{
    let request = request_for_operation(Operation::Lookup, &format!("{label}-wire"));
    let response = client.call(request).expect("serialized call succeeds");
    assert_success(&response, Operation::Lookup);

    let error =
        call_bytes(client, &[0xff, 0xff, 0xff]).expect_err("malformed protobuf is rejected");
    assert!(matches!(error, RpcError::Malformed(_)));
}

pub fn assert_transport_maps_filesystem_errno(
    client: &dyn RpcClient,
    service: &RecordingFs,
    label: &str,
) {
    service.fail_next(
        Operation::Lookup,
        FsError::new(Errno::NotFound, "not present"),
    );

    let response = client
        .call(request_for_operation(
            Operation::Lookup,
            &format!("{label}-errno"),
        ))
        .expect("filesystem errno is a response, not transport failure");
    assert_errno(&response, Errno::NotFound);
}

pub fn assert_transport_rejects_deadline_before_dispatch(
    client: &dyn RpcClient,
    service: &RecordingFs,
    label: &str,
) {
    let mut request = request_for_operation(Operation::Write, &format!("{label}-deadline"));
    request.deadline_unix_nanos = 1;

    let response = client
        .call(request)
        .expect("deadline rejection is a response");
    assert_errno(&response, Errno::TimedOut);
    assert!(service.calls().is_empty());
}

pub fn assert_transport_rejects_invalid_typed_envelopes_before_dispatch(
    client: &dyn RpcClient,
    service: &RecordingFs,
    label: &str,
) {
    let response = client
        .call(mismatched_operation_request(&format!(
            "{label}-direct-mismatch"
        )))
        .expect("invalid direct envelope is a response");
    assert_errno(&response, Errno::InvalidArgument);
    assert!(service.calls().is_empty());

    let response = client
        .call(invalid_path_request(&format!("{label}-direct-path")))
        .expect("invalid direct path is a response");
    assert_errno(&response, Errno::InvalidArgument);
    response
        .validate()
        .expect("invalid path rejection remains protocol-valid");
    assert!(service.calls().is_empty());

    let response = client
        .call(unsupported_version_request(&format!(
            "{label}-unsupported-version"
        )))
        .expect("unsupported version is a response");
    assert_errno(&response, Errno::InvalidArgument);
    response
        .validate()
        .expect("unsupported-version rejection remains protocol-valid");
    assert!(service.calls().is_empty());

    let response = client
        .call(empty_request_id_request())
        .expect("empty request_id is a response");
    assert_errno(&response, Errno::InvalidArgument);
    response
        .validate()
        .expect("empty-request-id rejection remains protocol-valid");
    assert!(!response.request_id.is_empty());
    assert!(service.calls().is_empty());

    let response = client
        .call(empty_namespace_request(&format!("{label}-empty-namespace")))
        .expect("empty namespace is a response");
    assert_errno(&response, Errno::InvalidArgument);
    response
        .validate()
        .expect("empty-namespace rejection remains protocol-valid");
    assert!(!response.namespace.is_empty());
    assert!(service.calls().is_empty());
}

pub fn assert_transport_rejects_request_contradictory_handler_successes<C, MakeClient>(
    label: &str,
    make_client: MakeClient,
) where
    C: RpcClient,
    MakeClient: Fn(ContradictoryResponseFs) -> C,
{
    let client = make_client(ContradictoryResponseFs::default());

    let response = client
        .call(request_for_operation(
            Operation::Read,
            &format!("{label}-oversized-read"),
        ))
        .expect("oversized read rejection is a response");
    assert_errno(&response, Errno::InvalidArgument);

    let response = client
        .call(readdir_request_with_max_entries(
            &format!("{label}-too-many-entries"),
            1,
        ))
        .expect("oversized readdir rejection is a response");
    assert_errno(&response, Errno::InvalidArgument);

    let response = client
        .call(request_for_operation(
            Operation::Write,
            &format!("{label}-impossible-write"),
        ))
        .expect("impossible write rejection is a response");
    assert_errno(&response, Errno::InvalidArgument);
    assert!(client
        .drain_invalidations("test-namespace")
        .expect("drain succeeds")
        .is_empty());

    let response = client
        .call(request_for_operation(
            Operation::CopyFileRange,
            &format!("{label}-impossible-copy-file-range"),
        ))
        .expect("impossible copy_file_range rejection is a response");
    assert_errno(&response, Errno::InvalidArgument);

    let response = client
        .call(request_for_operation(
            Operation::Write,
            &format!("{label}-valid-write-after-invalid"),
        ))
        .expect("valid write succeeds after rejected handler response");
    assert_success(&response, Operation::Write);
    assert_eq!(response.invalidations.len(), 1);
    assert_eq!(response.invalidations[0].sequence, 1);
}

pub fn assert_transport_rejects_oversized_frames(client: &dyn RpcClient, label: &str) {
    let error = client
        .call(request_for_operation(
            Operation::Lookup,
            &format!("{label}-frame-limit"),
        ))
        .expect_err("oversized frame is rejected");
    assert_eq!(error, RpcError::FrameTooLarge);
}

pub fn assert_transport_returns_ordered_response_invalidations_without_replay(
    client: &dyn RpcClient,
    label: &str,
) {
    let write = client
        .call(request_for_operation(
            Operation::Write,
            &format!("{label}-write-one"),
        ))
        .expect("first write succeeds");
    let create = client
        .call(request_for_operation(
            Operation::Create,
            &format!("{label}-create-two"),
        ))
        .expect("create succeeds");
    assert_eq!(write.invalidations[0].sequence, 1);
    assert_eq!(create.invalidations[0].sequence, 2);
    assert!(client
        .drain_invalidations("test-namespace")
        .expect("response invalidations are not replayed")
        .is_empty());
}

pub fn assert_transport_interleaves_namespaces_without_sequence_gaps(
    client: &dyn RpcClient,
    label: &str,
) {
    let ns_a_first = client
        .call(request_for_operation_in_namespace(
            Operation::Write,
            &format!("{label}-ns-a-first"),
            "namespace-a",
        ))
        .expect("namespace-a first write succeeds");
    let ns_b_first = client
        .call(request_for_operation_in_namespace(
            Operation::Write,
            &format!("{label}-ns-b-first"),
            "namespace-b",
        ))
        .expect("namespace-b write succeeds");
    let ns_a_second = client
        .call(request_for_operation_in_namespace(
            Operation::Create,
            &format!("{label}-ns-a-second"),
            "namespace-a",
        ))
        .expect("namespace-a create succeeds");

    assert_eq!(ns_a_first.invalidations[0].namespace, "namespace-a");
    assert_eq!(ns_a_first.invalidations[0].sequence, 1);
    assert_eq!(ns_b_first.invalidations[0].namespace, "namespace-b");
    assert_eq!(ns_b_first.invalidations[0].sequence, 1);
    assert_eq!(ns_a_second.invalidations[0].namespace, "namespace-a");
    assert_eq!(ns_a_second.invalidations[0].sequence, 2);
    assert!(client
        .drain_invalidations("namespace-a")
        .expect("namespace-a drain succeeds")
        .is_empty());
    assert!(client
        .drain_invalidations("namespace-b")
        .expect("namespace-b drain succeeds")
        .is_empty());
}

pub fn assert_transport_connection_loss_maps_to_transport_error<C, Disconnect>(
    client: &C,
    disconnect: Disconnect,
    label: &str,
) where
    C: RpcClient,
    Disconnect: Fn(&C),
{
    disconnect(client);
    let error = client
        .call(request_for_operation(
            Operation::Lookup,
            &format!("{label}-disconnect"),
        ))
        .expect_err("disconnected client fails at transport layer");
    assert_eq!(error, RpcError::ConnectionClosed);
    assert_eq!(error.errno(), Errno::ConnectionReset);
}
