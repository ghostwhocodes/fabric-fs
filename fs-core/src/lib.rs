use fs_protocol::{
    pb, Errno, InvalidationKind, Operation, OperationEffect, PathRole, ProtocolError,
    RequestEnvelope, RequestPayload, ResponseEnvelope, ResponseHandle, ResponsePayload,
    OPEN_FLAG_TRUNCATE,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct RpcMetadata {
    pub request_id: String,
    pub namespace: String,
    pub caller: Option<pb::CallerContext>,
    pub peer_identity: Option<String>,
    pub trace_id: Option<String>,
    pub payload_len: u64,
    pub received_unix_nanos: u64,
}

impl RpcMetadata {
    pub fn for_request(request: &RequestEnvelope, payload_len: u64) -> Self {
        Self {
            request_id: request.request_id.clone(),
            namespace: request.namespace.clone(),
            caller: request.caller.clone(),
            peer_identity: None,
            trace_id: (!request.trace.trace_id.is_empty()).then(|| request.trace.trace_id.clone()),
            payload_len,
            received_unix_nanos: now_unix_nanos(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_envelope_bytes: u64,
    pub max_read_bytes: u32,
    pub max_write_bytes: usize,
    pub max_directory_entries: u32,
    pub max_xattr_bytes: u32,
    pub max_symlink_target_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_envelope_bytes: 4 * 1024 * 1024,
            max_read_bytes: 1024 * 1024,
            max_write_bytes: 1024 * 1024,
            max_directory_entries: 4096,
            max_xattr_bytes: 64 * 1024,
            max_symlink_target_bytes: 4096,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{errno:?}: {message}")]
pub struct FsError {
    pub errno: Errno,
    pub message: String,
}

impl FsError {
    pub fn new(errno: Errno, message: impl Into<String>) -> Self {
        let errno = if errno == Errno::Success {
            Errno::InvalidArgument
        } else {
            errno
        };
        Self {
            errno,
            message: message.into(),
        }
    }
}

pub type FsResult<T> = Result<T, FsError>;

pub trait FileSystemService: Send + Sync {
    fn lookup(
        &self,
        _request: &pb::LookupRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        unsupported(Operation::Lookup)
    }

    fn getattr(
        &self,
        _request: &pb::GetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetattrResponse> {
        unsupported(Operation::Getattr)
    }

    fn readdir(
        &self,
        _request: &pb::ReaddirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReaddirResponse> {
        unsupported(Operation::Readdir)
    }

    fn open(
        &self,
        _request: &pb::OpenRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::OpenResponse> {
        unsupported(Operation::Open)
    }

    fn read(
        &self,
        _request: &pb::ReadRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadResponse> {
        unsupported(Operation::Read)
    }

    fn write(
        &self,
        _request: &pb::WriteRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        unsupported(Operation::Write)
    }

    fn create(
        &self,
        _request: &pb::CreateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        unsupported(Operation::Create)
    }

    fn rename(
        &self,
        _request: &pb::RenameRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Rename)
    }

    fn unlink(
        &self,
        _request: &pb::UnlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Unlink)
    }

    fn mkdir(
        &self,
        _request: &pb::MkdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        unsupported(Operation::Mkdir)
    }

    fn rmdir(
        &self,
        _request: &pb::RmdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Rmdir)
    }

    fn statfs(
        &self,
        _request: &pb::StatfsRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::StatfsResponse> {
        unsupported(Operation::Statfs)
    }

    fn getxattr(
        &self,
        _request: &pb::GetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetxattrResponse> {
        unsupported(Operation::Getxattr)
    }

    fn setxattr(
        &self,
        _request: &pb::SetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Setxattr)
    }

    fn listxattr(
        &self,
        _request: &pb::ListxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ListxattrResponse> {
        unsupported(Operation::Listxattr)
    }

    fn removexattr(
        &self,
        _request: &pb::RemovexattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Removexattr)
    }

    fn release(
        &self,
        _request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Release)
    }

    fn readlink(
        &self,
        _request: &pb::ReadlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadlinkResponse> {
        unsupported(Operation::Readlink)
    }

    fn symlink(
        &self,
        _request: &pb::SymlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SymlinkResponse> {
        unsupported(Operation::Symlink)
    }

    fn hardlink(
        &self,
        _request: &pb::HardlinkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::HardlinkResponse> {
        unsupported(Operation::Hardlink)
    }

    fn setattr(
        &self,
        _request: &pb::SetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::SetattrResponse> {
        unsupported(Operation::Setattr)
    }

    fn flush(
        &self,
        _request: &pb::FlushRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Flush)
    }

    fn fsync(
        &self,
        _request: &pb::FsyncRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Fsync)
    }

    fn fsyncdir(
        &self,
        _request: &pb::FsyncdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Fsyncdir)
    }

    fn getlk(
        &self,
        _request: &pb::GetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetlkResponse> {
        unsupported(Operation::Getlk)
    }

    fn setlk(
        &self,
        _request: &pb::SetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Setlk)
    }

    fn flock(
        &self,
        _request: &pb::FlockRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Flock)
    }

    fn copy_file_range(
        &self,
        _request: &pb::CopyFileRangeRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::CopyFileRangeResponse> {
        unsupported(Operation::CopyFileRange)
    }

    fn fallocate(
        &self,
        _request: &pb::FallocateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        unsupported(Operation::Fallocate)
    }

    fn lseek(
        &self,
        _request: &pb::LseekRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LseekResponse> {
        unsupported(Operation::Lseek)
    }
}

pub trait Authorizer: Send + Sync {
    fn authorize(&self, metadata: &RpcMetadata, request: &RequestEnvelope) -> FsResult<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn authorize(&self, _metadata: &RpcMetadata, _request: &RequestEnvelope) -> FsResult<()> {
        Ok(())
    }
}

pub trait Dispatch: Send + Sync {
    fn dispatch_request(&self, request: RequestEnvelope, metadata: RpcMetadata)
        -> ResponseEnvelope;

    fn abort_response_handles(
        &self,
        _request: &RequestEnvelope,
        _metadata: &RpcMetadata,
        _response: &ResponseEnvelope,
    ) -> FsResult<()> {
        Ok(())
    }
}

pub struct Dispatcher<S, A = AllowAll> {
    service: S,
    authorizer: A,
    limits: ResourceLimits,
    next_invalidation_sequence_by_namespace: Mutex<HashMap<String, u64>>,
}

impl<S> Dispatcher<S, AllowAll>
where
    S: FileSystemService,
{
    pub fn new(service: S) -> Self {
        Self::with_authorizer(service, AllowAll)
    }
}

impl<S, A> Dispatcher<S, A>
where
    S: FileSystemService,
    A: Authorizer,
{
    pub fn with_authorizer(service: S, authorizer: A) -> Self {
        Self {
            service,
            authorizer,
            limits: ResourceLimits::default(),
            next_invalidation_sequence_by_namespace: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn full_resync_invalidation(
        &self,
        namespace: &str,
        request_id: impl Into<String>,
    ) -> pb::Invalidation {
        let sequence = self.next_invalidation_sequence(namespace).unwrap_or(0);
        full_resync_invalidation(namespace, request_id, sequence)
    }

    fn full_resync_invalidation_at(
        &self,
        namespace: &str,
        request_id: impl Into<String>,
        sequence: u64,
    ) -> pb::Invalidation {
        full_resync_invalidation(namespace, request_id, sequence)
    }

    pub fn dispatch(&self, request: RequestEnvelope, metadata: RpcMetadata) -> ResponseEnvelope {
        if let Err(error) = self.validate_before_handler(&request, &metadata) {
            return ResponseEnvelope::failure_for(&request, error.errno, error.message);
        }

        let result = self.call_handler(&request, &metadata);
        match result {
            Ok(payload) => {
                if let Err(error) = payload.validate_for_request(&request.payload) {
                    return ResponseEnvelope::failure_for(
                        &request,
                        Errno::InvalidArgument,
                        error.to_string(),
                    );
                }
                let invalidations = self.invalidations_for_success(&request, &payload);
                ResponseEnvelope::success_for(&request, payload, invalidations).unwrap_or_else(
                    |error| {
                        ResponseEnvelope::failure_for(
                            &request,
                            Errno::InvalidArgument,
                            error.to_string(),
                        )
                    },
                )
            }
            Err(error) => ResponseEnvelope::failure_for(&request, error.errno, error.message),
        }
    }

    fn validate_before_handler(
        &self,
        request: &RequestEnvelope,
        metadata: &RpcMetadata,
    ) -> FsResult<()> {
        request.validate().map_err(protocol_error_to_fs_error)?;
        if metadata.request_id != request.request_id {
            return Err(FsError::new(
                Errno::InvalidArgument,
                "metadata request_id does not match envelope",
            ));
        }
        if metadata.namespace != request.namespace {
            return Err(FsError::new(
                Errno::InvalidArgument,
                "metadata namespace does not match envelope",
            ));
        }
        if request.deadline_unix_nanos != 0 && request.deadline_unix_nanos <= now_unix_nanos() {
            return Err(FsError::new(
                Errno::TimedOut,
                "request deadline expired before dispatch",
            ));
        }
        self.check_limits(request, metadata)?;
        self.authorizer.authorize(metadata, request)
    }

    fn check_limits(&self, request: &RequestEnvelope, metadata: &RpcMetadata) -> FsResult<()> {
        if metadata.payload_len > self.limits.max_envelope_bytes {
            return Err(FsError::new(
                Errno::MessageTooLarge,
                "request envelope exceeds max bytes",
            ));
        }

        match &request.payload {
            RequestPayload::Read(value) if value.size > self.limits.max_read_bytes => Err(
                FsError::new(Errno::NoBufferSpace, "read size exceeds limit"),
            ),
            RequestPayload::Write(value) if value.data.len() > self.limits.max_write_bytes => Err(
                FsError::new(Errno::MessageTooLarge, "write size exceeds limit"),
            ),
            RequestPayload::Readdir(value)
                if value.max_entries > self.limits.max_directory_entries =>
            {
                Err(FsError::new(
                    Errno::NoBufferSpace,
                    "directory entry request exceeds limit",
                ))
            }
            RequestPayload::Getxattr(value) if value.size > self.limits.max_xattr_bytes => Err(
                FsError::new(Errno::NoBufferSpace, "xattr size exceeds limit"),
            ),
            RequestPayload::Listxattr(value) if value.size > self.limits.max_xattr_bytes => Err(
                FsError::new(Errno::NoBufferSpace, "xattr list size exceeds limit"),
            ),
            RequestPayload::Setxattr(value)
                if value.value.len() > self.limits.max_xattr_bytes as usize =>
            {
                Err(FsError::new(
                    Errno::MessageTooLarge,
                    "xattr value exceeds limit",
                ))
            }
            RequestPayload::Symlink(value)
                if value.target.len() > self.limits.max_symlink_target_bytes =>
            {
                Err(FsError::new(
                    Errno::MessageTooLarge,
                    "symlink target exceeds limit",
                ))
            }
            _ => Ok(()),
        }
    }

    fn call_handler(
        &self,
        request: &RequestEnvelope,
        metadata: &RpcMetadata,
    ) -> FsResult<ResponsePayload> {
        match &request.payload {
            RequestPayload::Lookup(value) => self
                .service
                .lookup(value, metadata)
                .map(ResponsePayload::Lookup),
            RequestPayload::Getattr(value) => self
                .service
                .getattr(value, metadata)
                .map(ResponsePayload::Getattr),
            RequestPayload::Readdir(value) => self
                .service
                .readdir(value, metadata)
                .map(ResponsePayload::Readdir),
            RequestPayload::Open(value) => self
                .service
                .open(value, metadata)
                .map(ResponsePayload::Open),
            RequestPayload::Read(value) => self
                .service
                .read(value, metadata)
                .map(ResponsePayload::Read),
            RequestPayload::Write(value) => self
                .service
                .write(value, metadata)
                .map(ResponsePayload::Write),
            RequestPayload::Create(value) => self
                .service
                .create(value, metadata)
                .map(ResponsePayload::Create),
            RequestPayload::Rename(value) => self
                .service
                .rename(value, metadata)
                .map(ResponsePayload::Rename),
            RequestPayload::Unlink(value) => self
                .service
                .unlink(value, metadata)
                .map(ResponsePayload::Unlink),
            RequestPayload::Mkdir(value) => self
                .service
                .mkdir(value, metadata)
                .map(ResponsePayload::Mkdir),
            RequestPayload::Rmdir(value) => self
                .service
                .rmdir(value, metadata)
                .map(ResponsePayload::Rmdir),
            RequestPayload::Statfs(value) => self
                .service
                .statfs(value, metadata)
                .map(ResponsePayload::Statfs),
            RequestPayload::Getxattr(value) => self
                .service
                .getxattr(value, metadata)
                .map(ResponsePayload::Getxattr),
            RequestPayload::Setxattr(value) => self
                .service
                .setxattr(value, metadata)
                .map(ResponsePayload::Setxattr),
            RequestPayload::Listxattr(value) => self
                .service
                .listxattr(value, metadata)
                .map(ResponsePayload::Listxattr),
            RequestPayload::Removexattr(value) => self
                .service
                .removexattr(value, metadata)
                .map(ResponsePayload::Removexattr),
            RequestPayload::Release(value) => self
                .service
                .release(value, metadata)
                .map(ResponsePayload::Release),
            RequestPayload::Readlink(value) => self
                .service
                .readlink(value, metadata)
                .map(ResponsePayload::Readlink),
            RequestPayload::Symlink(value) => self
                .service
                .symlink(value, metadata)
                .map(ResponsePayload::Symlink),
            RequestPayload::Hardlink(value) => self
                .service
                .hardlink(value, metadata)
                .map(ResponsePayload::Hardlink),
            RequestPayload::Setattr(value) => self
                .service
                .setattr(value, metadata)
                .map(ResponsePayload::Setattr),
            RequestPayload::Flush(value) => self
                .service
                .flush(value, metadata)
                .map(ResponsePayload::Flush),
            RequestPayload::Fsync(value) => self
                .service
                .fsync(value, metadata)
                .map(ResponsePayload::Fsync),
            RequestPayload::Fsyncdir(value) => self
                .service
                .fsyncdir(value, metadata)
                .map(ResponsePayload::Fsyncdir),
            RequestPayload::Getlk(value) => self
                .service
                .getlk(value, metadata)
                .map(ResponsePayload::Getlk),
            RequestPayload::Setlk(value) => self
                .service
                .setlk(value, metadata)
                .map(ResponsePayload::Setlk),
            RequestPayload::Flock(value) => self
                .service
                .flock(value, metadata)
                .map(ResponsePayload::Flock),
            RequestPayload::CopyFileRange(value) => self
                .service
                .copy_file_range(value, metadata)
                .map(ResponsePayload::CopyFileRange),
            RequestPayload::Fallocate(value) => self
                .service
                .fallocate(value, metadata)
                .map(ResponsePayload::Fallocate),
            RequestPayload::Lseek(value) => self
                .service
                .lseek(value, metadata)
                .map(ResponsePayload::Lseek),
        }
    }

    fn invalidations_for_success(
        &self,
        request: &RequestEnvelope,
        payload: &ResponsePayload,
    ) -> Vec<pb::Invalidation> {
        let Some(kind) = invalidation_kind_for_request(&request.payload) else {
            return Vec::new();
        };
        let sequence = match self.next_invalidation_sequence(&request.namespace) {
            Ok(sequence) => sequence,
            Err(()) => {
                return vec![self.full_resync_invalidation_at(
                    &request.namespace,
                    &request.request_id,
                    0,
                )]
            }
        };
        let mut invalidation = pb::Invalidation {
            namespace: request.namespace.clone(),
            sequence,
            kind: kind.wire_value(),
            path: request
                .payload
                .primary_path()
                .unwrap_or_default()
                .to_owned(),
            old_path: String::new(),
            new_path: String::new(),
            inode: payload.created_inode().unwrap_or(0),
            handle: 0,
            request_id: request.request_id.clone(),
        };
        if let RequestPayload::Rename(value) = &request.payload {
            invalidation.path.clear();
            invalidation.old_path = value
                .old_path
                .as_ref()
                .map(|path| path.path.clone())
                .unwrap_or_default();
            invalidation.new_path = value
                .new_path
                .as_ref()
                .map(|path| path.path.clone())
                .unwrap_or_default();
        }
        vec![invalidation]
    }

    fn next_invalidation_sequence(&self, namespace: &str) -> Result<u64, ()> {
        let mut sequences = match self.next_invalidation_sequence_by_namespace.lock() {
            Ok(sequences) => sequences,
            Err(poisoned) => {
                let mut sequences = poisoned.into_inner();
                sequences.clear();
                drop(sequences);
                self.next_invalidation_sequence_by_namespace.clear_poison();
                return Err(());
            }
        };
        let sequence = sequences.entry(namespace.to_owned()).or_insert(1);
        let current = *sequence;
        *sequence += 1;
        Ok(current)
    }
}

impl<S, A> Dispatch for Dispatcher<S, A>
where
    S: FileSystemService,
    A: Authorizer,
{
    fn dispatch_request(
        &self,
        request: RequestEnvelope,
        metadata: RpcMetadata,
    ) -> ResponseEnvelope {
        self.dispatch(request, metadata)
    }

    fn abort_response_handles(
        &self,
        request: &RequestEnvelope,
        metadata: &RpcMetadata,
        response: &ResponseEnvelope,
    ) -> FsResult<()> {
        let Some(release) = release_for_handle_response(request, response) else {
            return Ok(());
        };
        self.service.release(&release, metadata).map(|_| ())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RpcError {
    #[error("transport closed")]
    ConnectionClosed,
    #[error("frame exceeded transport limit")]
    FrameTooLarge,
    #[error("request timed out")]
    Timeout,
    #[error("malformed message: {0}")]
    Malformed(String),
    #[error("transport failed: {0}")]
    Transport(String),
}

impl RpcError {
    pub fn errno(&self) -> Errno {
        match self {
            RpcError::ConnectionClosed => Errno::ConnectionReset,
            RpcError::FrameTooLarge => Errno::MessageTooLarge,
            RpcError::Timeout => Errno::TimedOut,
            RpcError::Malformed(_) => Errno::InvalidArgument,
            RpcError::Transport(_) => Errno::Io,
        }
    }
}

pub trait RpcClient: Send + Sync {
    fn call(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError>;

    fn drain_invalidations(&self, _namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
        Ok(Vec::new())
    }
}

pub fn now_unix_nanos() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn unsupported<T>(operation: Operation) -> FsResult<T> {
    Err(FsError::new(
        Errno::NotSupported,
        format!("operation {} is not implemented", operation.as_str()),
    ))
}

fn invalidation_kind_for_request(payload: &RequestPayload) -> Option<InvalidationKind> {
    match payload {
        RequestPayload::Open(value) if value.flags & OPEN_FLAG_TRUNCATE != 0 => {
            Some(InvalidationKind::Modify)
        }
        payload => invalidation_kind_for_effect(payload.operation().spec().effect),
    }
}

fn invalidation_kind_for_effect(effect: OperationEffect) -> Option<InvalidationKind> {
    match effect {
        OperationEffect::ContentMutation => Some(InvalidationKind::Modify),
        OperationEffect::CreateNode => Some(InvalidationKind::Create),
        OperationEffect::RenameNode => Some(InvalidationKind::Rename),
        OperationEffect::DeleteNode => Some(InvalidationKind::Delete),
        OperationEffect::MetadataMutation => Some(InvalidationKind::Metadata),
        OperationEffect::XattrMutation => Some(InvalidationKind::Xattr),
        _ => None,
    }
}

fn full_resync_invalidation(
    namespace: &str,
    request_id: impl Into<String>,
    sequence: u64,
) -> pb::Invalidation {
    pb::Invalidation {
        namespace: namespace.to_owned(),
        sequence,
        kind: InvalidationKind::FullResync.wire_value(),
        path: String::new(),
        old_path: String::new(),
        new_path: String::new(),
        inode: 0,
        handle: 0,
        request_id: request_id.into(),
    }
}

fn release_for_handle_response(
    request: &RequestEnvelope,
    response: &ResponseEnvelope,
) -> Option<pb::ReleaseRequest> {
    if !response.ok {
        return None;
    }
    if request.operation.spec().response_handle != ResponseHandle::OpenedObject {
        return None;
    }
    Some(pb::ReleaseRequest {
        path: request.payload.path_dto_for_role(PathRole::Target).cloned(),
        handle: response.payload.as_ref()?.opened_handle()?,
        flags: 0,
    })
}

fn protocol_error_to_fs_error(error: ProtocolError) -> FsError {
    FsError::new(
        Errno::InvalidArgument,
        format!("invalid protocol request: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_protocol::{path, pb};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn dispatcher_recovers_poisoned_invalidation_sequence_with_full_resync() {
        let dispatcher = Dispatcher::new(WriteService);

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = dispatcher
                .next_invalidation_sequence_by_namespace
                .lock()
                .expect("test lock starts healthy");
            panic!("poison invalidation state for regression coverage");
        }));

        let request = RequestEnvelope::new(
            "poisoned-invalidation-write",
            "poisoned-ns",
            0,
            pb::TraceContext::default(),
            RequestPayload::Write(pb::WriteRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: 7,
                offset: 0,
                data: b"data".to_vec(),
            }),
        )
        .expect("request is valid");
        let response = dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0));

        assert!(
            response.ok,
            "poison recovery should not panic or fail: {response:?}"
        );
        assert_eq!(response.invalidations.len(), 1);
        assert_eq!(
            response.invalidations[0].kind,
            InvalidationKind::FullResync.wire_value()
        );
        assert_eq!(response.invalidations[0].sequence, 0);
        assert_eq!(response.invalidations[0].request_id, request.request_id);
    }

    #[test]
    fn dispatcher_allocates_external_full_resync_in_sequence_order() {
        let dispatcher = Dispatcher::new(WriteService);
        let first = write_request("sequenced-write-1", "sequenced-ns");
        let first_response =
            dispatcher.dispatch(first.clone(), RpcMetadata::for_request(&first, 0));
        assert_eq!(first_response.invalidations[0].sequence, 1);

        let full_resync = dispatcher.full_resync_invalidation("sequenced-ns", "storage-watch-1");
        assert_eq!(full_resync.kind, InvalidationKind::FullResync.wire_value());
        assert_eq!(full_resync.sequence, 2);
        assert_eq!(full_resync.request_id, "storage-watch-1");

        let second = write_request("sequenced-write-2", "sequenced-ns");
        let second_response =
            dispatcher.dispatch(second.clone(), RpcMetadata::for_request(&second, 0));
        assert_eq!(second_response.invalidations[0].sequence, 3);
    }

    struct WriteService;

    impl FileSystemService for WriteService {
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

    fn write_request(request_id: &str, namespace: &str) -> RequestEnvelope {
        RequestEnvelope::new(
            request_id,
            namespace,
            0,
            pb::TraceContext::default(),
            RequestPayload::Write(pb::WriteRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: 7,
                offset: 0,
                data: b"data".to_vec(),
            }),
        )
        .expect("request is valid")
    }
}
