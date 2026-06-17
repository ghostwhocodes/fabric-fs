mod cache;

use cache::CacheKernel;
use fabricfs_observability::{AtomicHistogram, HistogramSnapshot, LATENCY_BUCKETS_MICROS};
use fs_core::{RpcClient, RpcError};
use fs_protocol::{
    path, pb, Errno, Operation, RequestEnvelope, RequestPayload, ResponseEnvelope, ResponsePayload,
};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

thread_local! {
    static CALLER_CONTEXT: RefCell<Option<pb::CallerContext>> = const { RefCell::new(None) };
}

const MAX_REPLY_WRITE_BYTES: u64 = u32::MAX as u64;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FuseError {
    #[error("{errno:?}: {message}")]
    Response { errno: Errno, message: String },
    #[error("transport {errno:?}: {message}")]
    Transport { errno: Errno, message: String },
    #[error("cache is poisoned by an invalidation gap")]
    StaleCache,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("unexpected response payload for {0:?}")]
    UnexpectedPayload(Operation),
}

impl FuseError {
    pub fn errno(&self) -> Errno {
        match self {
            FuseError::Response { errno, .. } => *errno,
            FuseError::Transport { errno, .. } => *errno,
            FuseError::StaleCache => Errno::Stale,
            FuseError::Protocol(_) | FuseError::UnexpectedPayload(_) => Errno::InvalidArgument,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseEntry {
    pub attr: pb::FileAttr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseHandle {
    pub handle: u64,
    pub flags: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseWrite {
    pub bytes_written: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseSetlk {
    pub handle: u64,
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub typ: i32,
    pub pid: u32,
    pub wait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseStatFs {
    pub stat: pb::StatFs,
}

pub struct CallerContextGuard {
    previous: Option<pb::CallerContext>,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for CallerContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        CALLER_CONTEXT.with(|caller| {
            *caller.borrow_mut() = previous;
        });
    }
}

pub struct FuseAdapter<C> {
    client: C,
    namespace: String,
    cache: CacheKernel,
    next_request_id: AtomicU64,
    metrics: Arc<AdapterMetrics>,
}

struct AdapterMetrics {
    calls_total: AtomicU64,
    call_failures: AtomicU64,
    stale_cache_total: AtomicU64,
    invalidation_drains_total: AtomicU64,
    invalidation_errors_total: AtomicU64,
    cache_poison_total: AtomicU64,
    call_latency_micros: AtomicHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterMetricsSnapshot {
    pub calls_total: u64,
    pub call_failures: u64,
    pub stale_cache_total: u64,
    pub invalidation_drains_total: u64,
    pub invalidation_errors_total: u64,
    pub cache_poison_total: u64,
    pub call_latency_micros: HistogramSnapshot,
}

#[derive(Clone)]
pub struct AdapterMetricsHandle {
    metrics: Arc<AdapterMetrics>,
}

impl Default for AdapterMetrics {
    fn default() -> Self {
        Self {
            calls_total: AtomicU64::new(0),
            call_failures: AtomicU64::new(0),
            stale_cache_total: AtomicU64::new(0),
            invalidation_drains_total: AtomicU64::new(0),
            invalidation_errors_total: AtomicU64::new(0),
            cache_poison_total: AtomicU64::new(0),
            call_latency_micros: AtomicHistogram::new(LATENCY_BUCKETS_MICROS),
        }
    }
}

impl AdapterMetrics {
    fn snapshot(&self) -> AdapterMetricsSnapshot {
        AdapterMetricsSnapshot {
            calls_total: self.calls_total.load(Ordering::Relaxed),
            call_failures: self.call_failures.load(Ordering::Relaxed),
            stale_cache_total: self.stale_cache_total.load(Ordering::Relaxed),
            invalidation_drains_total: self.invalidation_drains_total.load(Ordering::Relaxed),
            invalidation_errors_total: self.invalidation_errors_total.load(Ordering::Relaxed),
            cache_poison_total: self.cache_poison_total.load(Ordering::Relaxed),
            call_latency_micros: self.call_latency_micros.snapshot(),
        }
    }
}

impl<C> FuseAdapter<C>
where
    C: RpcClient,
{
    pub fn new(client: C, namespace: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            cache: CacheKernel::new(),
            next_request_id: AtomicU64::new(1),
            metrics: Arc::new(AdapterMetrics::default()),
        }
    }

    pub fn metrics(&self) -> AdapterMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn metrics_handle(&self) -> AdapterMetricsHandle {
        AdapterMetricsHandle {
            metrics: Arc::clone(&self.metrics),
        }
    }

    pub fn bind_caller_context(&self, caller: pb::CallerContext) -> CallerContextGuard {
        CALLER_CONTEXT.with(|current| {
            let previous = current.borrow_mut().replace(caller);
            CallerContextGuard {
                previous,
                _not_send: PhantomData,
            }
        })
    }

    pub fn with_caller_context<R>(
        &self,
        caller: pb::CallerContext,
        operation: impl FnOnce() -> R,
    ) -> R {
        let _caller = self.bind_caller_context(caller);
        operation()
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Result<FuseEntry, FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Lookup(pb::LookupRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Lookup(value) => {
                let attr = required_attr(value.attr, Operation::Lookup)?;
                self.cache.insert_lookup(path, attr.inode, 1);
                Ok(FuseEntry { attr })
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Lookup)),
        }
    }

    pub fn getattr(&self, inode: u64) -> Result<FuseEntry, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Getattr(pb::GetattrRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Getattr(value) => Ok(FuseEntry {
                attr: required_attr(value.attr, Operation::Getattr)?,
            }),
            _ => Err(FuseError::UnexpectedPayload(Operation::Getattr)),
        }
    }

    pub fn readdir(
        &self,
        inode: u64,
        offset: u64,
        max_entries: u32,
    ) -> Result<Vec<pb::DirectoryEntry>, FuseError> {
        self.drain_remote_invalidations()?;
        let path = self.path_for_inode_after_drain(inode)?;
        self.readdir_path(path, offset, max_entries)
    }

    pub fn readdir_with_parent(
        &self,
        inode: u64,
        offset: u64,
        max_entries: u32,
    ) -> Result<(u64, Vec<pb::DirectoryEntry>), FuseError> {
        self.drain_remote_invalidations()?;
        let (parent, path) = self.parent_and_path_for_inode_after_drain(inode)?;
        let entries = self.readdir_path(path, offset, max_entries)?;
        Ok((parent, entries))
    }

    fn readdir_path(
        &self,
        path: String,
        offset: u64,
        max_entries: u32,
    ) -> Result<Vec<pb::DirectoryEntry>, FuseError> {
        let response = self.call(RequestPayload::Readdir(pb::ReaddirRequest {
            path: Some(path_dto(&path)?),
            offset,
            max_entries,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Readdir(value) => Ok(value.entries),
            _ => Err(FuseError::UnexpectedPayload(Operation::Readdir)),
        }
    }

    pub fn open(&self, inode: u64, flags: u32) -> Result<FuseHandle, FuseError> {
        self.open_with_kind(inode, flags, pb::OpenKind::File)
    }

    pub fn opendir(&self, inode: u64, flags: u32) -> Result<FuseHandle, FuseError> {
        self.open_with_kind(inode, flags, pb::OpenKind::Directory)
    }

    fn open_with_kind(
        &self,
        inode: u64,
        flags: u32,
        kind: pb::OpenKind,
    ) -> Result<FuseHandle, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Open(pb::OpenRequest {
            path: Some(path_dto(&path)?),
            flags,
            kind: kind as i32,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Open(value) => {
                self.remember_handle_path(inode, value.handle, path);
                Ok(FuseHandle {
                    handle: value.handle,
                    flags: value.flags,
                })
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Open)),
        }
    }

    pub fn read(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Read(pb::ReadRequest {
            path: Some(path_dto(&path)?),
            handle,
            offset,
            size,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Read(value) => Ok(value.data),
            _ => Err(FuseError::UnexpectedPayload(Operation::Read)),
        }
    }

    pub fn write(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<FuseWrite, FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Write(pb::WriteRequest {
            path: Some(path_dto(&path)?),
            handle,
            offset,
            data,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Write(value) => Ok(FuseWrite {
                bytes_written: value.bytes_written,
            }),
            _ => Err(FuseError::UnexpectedPayload(Operation::Write)),
        }
    }

    pub fn create(
        &self,
        parent: u64,
        name: &str,
        flags: u32,
        mode: u32,
    ) -> Result<(FuseEntry, FuseHandle), FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Create(pb::CreateRequest {
            path: Some(path_dto(&path)?),
            flags,
            mode,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Create(value) => {
                let attr = required_attr(value.attr, Operation::Create)?;
                self.cache.insert_lookup(path.clone(), attr.inode, 1);
                self.remember_handle_path(attr.inode, value.handle, path);
                Ok((
                    FuseEntry { attr },
                    FuseHandle {
                        handle: value.handle,
                        flags: 0,
                    },
                ))
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Create)),
        }
    }

    pub fn rename(
        &self,
        old_parent: u64,
        old_name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(), FuseError> {
        self.drain_remote_invalidations()?;
        let (old_path, new_path) =
            self.rename_paths_after_drain(old_parent, old_name, new_parent, new_name)?;
        let response = self.call(RequestPayload::Rename(pb::RenameRequest {
            old_path: Some(path_dto(&old_path)?),
            new_path: Some(path_dto(&new_path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Rename(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Rename)),
        }
    }

    pub fn unlink(&self, parent: u64, name: &str) -> Result<(), FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Unlink(pb::UnlinkRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Unlink(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Unlink)),
        }
    }

    pub fn mkdir(&self, parent: u64, name: &str, mode: u32) -> Result<FuseEntry, FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Mkdir(pb::MkdirRequest {
            path: Some(path_dto(&path)?),
            mode,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Mkdir(value) => {
                let attr = required_attr(value.attr, Operation::Mkdir)?;
                self.cache.insert_lookup(path, attr.inode, 1);
                Ok(FuseEntry { attr })
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Mkdir)),
        }
    }

    pub fn rmdir(&self, parent: u64, name: &str) -> Result<(), FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Rmdir(pb::RmdirRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Rmdir(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Rmdir)),
        }
    }

    pub fn statfs(&self, inode: u64) -> Result<FuseStatFs, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Statfs(pb::StatfsRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Statfs(value) => Ok(FuseStatFs {
                stat: value
                    .stat
                    .ok_or_else(|| FuseError::Protocol("missing statfs payload".into()))?,
            }),
            _ => Err(FuseError::UnexpectedPayload(Operation::Statfs)),
        }
    }

    pub fn getxattr(&self, inode: u64, name: &str, size: u32) -> Result<Vec<u8>, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Getxattr(pb::GetxattrRequest {
            path: Some(path_dto(&path)?),
            name: name.into(),
            size,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Getxattr(value) => Ok(value.value),
            _ => Err(FuseError::UnexpectedPayload(Operation::Getxattr)),
        }
    }

    pub fn setxattr(
        &self,
        inode: u64,
        name: &str,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Setxattr(pb::SetxattrRequest {
            path: Some(path_dto(&path)?),
            name: name.into(),
            value,
            flags,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Setxattr(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Setxattr)),
        }
    }

    pub fn listxattr(&self, inode: u64, size: u32) -> Result<Vec<String>, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Listxattr(pb::ListxattrRequest {
            path: Some(path_dto(&path)?),
            size,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Listxattr(value) => Ok(value.names),
            _ => Err(FuseError::UnexpectedPayload(Operation::Listxattr)),
        }
    }

    pub fn removexattr(&self, inode: u64, name: &str) -> Result<(), FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Removexattr(pb::RemovexattrRequest {
            path: Some(path_dto(&path)?),
            name: name.into(),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Removexattr(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Removexattr)),
        }
    }

    pub fn release(&self, inode: u64, handle: u64, flags: u32) -> Result<(), FuseError> {
        let path = self.path_for_release(inode, handle)?;
        let response = self.call_release(RequestPayload::Release(pb::ReleaseRequest {
            path: Some(path_dto(&path)?),
            handle,
            flags,
        }));
        self.forget_handle_path(inode, handle);
        let response = response?;
        match take_payload(response)? {
            ResponsePayload::Release(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Release)),
        }
    }

    pub fn readlink(&self, inode: u64) -> Result<Vec<u8>, FuseError> {
        let path = self.path_for_inode(inode)?;
        let response = self.call(RequestPayload::Readlink(pb::ReadlinkRequest {
            path: Some(path_dto(&path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Readlink(value) => Ok(value.target),
            _ => Err(FuseError::UnexpectedPayload(Operation::Readlink)),
        }
    }

    pub fn symlink(&self, parent: u64, name: &str, target: &[u8]) -> Result<FuseEntry, FuseError> {
        let path = self.child_path(parent, name)?;
        let response = self.call(RequestPayload::Symlink(pb::SymlinkRequest {
            path: Some(path_dto(&path)?),
            target: target.to_vec(),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Symlink(value) => {
                let attr = required_attr(value.attr, Operation::Symlink)?;
                self.cache.insert_lookup(path, attr.inode, 1);
                Ok(FuseEntry { attr })
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Symlink)),
        }
    }

    pub fn hardlink(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
    ) -> Result<FuseEntry, FuseError> {
        self.drain_remote_invalidations()?;
        let (existing_path, new_path) =
            self.hardlink_paths_after_drain(inode, new_parent, new_name)?;
        let response = self.call(RequestPayload::Hardlink(pb::HardlinkRequest {
            existing_path: Some(path_dto(&existing_path)?),
            new_path: Some(path_dto(&new_path)?),
        }))?;
        match take_payload(response)? {
            ResponsePayload::Hardlink(value) => {
                let attr = required_attr(value.attr, Operation::Hardlink)?;
                self.cache.insert_lookup(new_path, attr.inode, 1);
                Ok(FuseEntry { attr })
            }
            _ => Err(FuseError::UnexpectedPayload(Operation::Hardlink)),
        }
    }

    pub fn setattr(
        &self,
        inode: u64,
        handle: Option<u64>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
    ) -> Result<FuseEntry, FuseError> {
        let path = match handle {
            Some(handle) => self.path_for_handle_bound_call(inode, handle)?,
            None => self.path_for_inode(inode)?,
        };
        let payload = RequestPayload::Setattr(pb::SetattrRequest {
            path: Some(path_dto(&path)?),
            mode,
            uid,
            gid,
            size,
            handle,
        });
        let response = if handle.is_some() {
            self.call_handle_bound(payload)
        } else {
            self.call(payload)
        }?;
        match take_payload(response)? {
            ResponsePayload::Setattr(value) => Ok(FuseEntry {
                attr: required_attr(value.attr, Operation::Setattr)?,
            }),
            _ => Err(FuseError::UnexpectedPayload(Operation::Setattr)),
        }
    }

    pub fn flush(&self, inode: u64, handle: u64, lock_owner: u64) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Flush(pb::FlushRequest {
            path: Some(path_dto(&path)?),
            handle,
            lock_owner,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Flush(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Flush)),
        }
    }

    pub fn fsync(&self, inode: u64, handle: u64, datasync: bool) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Fsync(pb::FsyncRequest {
            path: Some(path_dto(&path)?),
            handle,
            datasync,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Fsync(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Fsync)),
        }
    }

    pub fn fsyncdir(&self, inode: u64, handle: u64, datasync: bool) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Fsyncdir(pb::FsyncdirRequest {
            path: Some(path_dto(&path)?),
            handle,
            datasync,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Fsyncdir(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Fsyncdir)),
        }
    }

    pub fn getlk(
        &self,
        inode: u64,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<Option<pb::FileLock>, FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Getlk(pb::GetlkRequest {
            path: Some(path_dto(&path)?),
            handle,
            owner,
            start,
            end,
            typ,
            pid,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Getlk(value) => Ok(value.lock),
            _ => Err(FuseError::UnexpectedPayload(Operation::Getlk)),
        }
    }

    pub fn setlk(&self, inode: u64, lock: FuseSetlk) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, lock.handle)?;
        let response = self.call_handle_bound(RequestPayload::Setlk(pb::SetlkRequest {
            path: Some(path_dto(&path)?),
            handle: lock.handle,
            owner: lock.owner,
            start: lock.start,
            end: lock.end,
            typ: lock.typ,
            pid: lock.pid,
            wait: lock.wait,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Setlk(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Setlk)),
        }
    }

    pub fn flock(
        &self,
        inode: u64,
        handle: u64,
        owner: u64,
        operation: i32,
    ) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Flock(pb::FlockRequest {
            path: Some(path_dto(&path)?),
            handle,
            owner,
            operation,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Flock(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Flock)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn copy_file_range(
        &self,
        input_inode: u64,
        input_handle: u64,
        input_offset: i64,
        output_inode: u64,
        output_handle: u64,
        output_offset: i64,
        length: u64,
        flags: u32,
    ) -> Result<u64, FuseError> {
        if flags != 0 {
            return Err(FuseError::Response {
                errno: Errno::NotSupported,
                message: "copy_file_range flags are not supported".into(),
            });
        }
        self.drain_remote_invalidations()?;
        let (input_path, output_path) = self.copy_file_range_paths_after_drain(
            input_inode,
            input_handle,
            output_inode,
            output_handle,
        )?;
        let response =
            self.call_handle_bound(RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
                input_path: Some(path_dto(&input_path)?),
                input_handle,
                input_offset,
                output_path: Some(path_dto(&output_path)?),
                output_handle,
                output_offset,
                length,
                flags,
            }))?;
        match take_payload(response)? {
            ResponsePayload::CopyFileRange(value) => Ok(value.bytes_copied),
            _ => Err(FuseError::UnexpectedPayload(Operation::CopyFileRange)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn copy_file_range_for_reply_write(
        &self,
        input_inode: u64,
        input_handle: u64,
        input_offset: i64,
        output_inode: u64,
        output_handle: u64,
        output_offset: i64,
        length: u64,
        flags: u32,
    ) -> Result<FuseWrite, FuseError> {
        let bytes_copied = self.copy_file_range(
            input_inode,
            input_handle,
            input_offset,
            output_inode,
            output_handle,
            output_offset,
            length.min(MAX_REPLY_WRITE_BYTES),
            flags,
        )?;
        Ok(FuseWrite {
            bytes_written: u32::try_from(bytes_copied).map_err(|_| {
                FuseError::Protocol("copy_file_range response exceeded ReplyWrite capacity".into())
            })?,
        })
    }

    pub fn fallocate(
        &self,
        inode: u64,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Fallocate(pb::FallocateRequest {
            path: Some(path_dto(&path)?),
            handle,
            offset,
            length,
            mode,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Fallocate(_) => Ok(()),
            _ => Err(FuseError::UnexpectedPayload(Operation::Fallocate)),
        }
    }

    pub fn lseek(
        &self,
        inode: u64,
        handle: u64,
        offset: i64,
        whence: i32,
    ) -> Result<i64, FuseError> {
        let path = self.path_for_handle_bound_call(inode, handle)?;
        let response = self.call_handle_bound(RequestPayload::Lseek(pb::LseekRequest {
            path: Some(path_dto(&path)?),
            handle,
            offset,
            whence,
        }))?;
        match take_payload(response)? {
            ResponsePayload::Lseek(value) => Ok(value.offset),
            _ => Err(FuseError::UnexpectedPayload(Operation::Lseek)),
        }
    }

    pub fn forget(&self, inode: u64, nlookup: u64) {
        self.cache.forget(inode, nlookup);
    }

    pub fn apply_invalidation(&self, invalidation: &pb::Invalidation) -> Result<(), FuseError> {
        self.cache
            .apply_invalidation(&self.namespace, invalidation, true)
            .map(|_| ())
    }

    pub fn cached_path(&self, inode: u64) -> Option<String> {
        self.cache.cached_path(inode)
    }

    pub fn cache_poisoned(&self) -> bool {
        self.cache.poisoned()
    }

    pub fn parent_inode(&self, inode: u64) -> Result<u64, FuseError> {
        self.drain_remote_invalidations()?;
        self.parent_inode_after_drain(inode)
    }

    fn parent_inode_after_drain(&self, inode: u64) -> Result<u64, FuseError> {
        self.cache.parent_inode(inode)
    }

    fn call(&self, payload: RequestPayload) -> Result<ResponseEnvelope, FuseError> {
        self.call_with_cache_policy(payload, true, false)
    }

    fn call_handle_bound(&self, payload: RequestPayload) -> Result<ResponseEnvelope, FuseError> {
        self.call_with_cache_policy(payload, false, false)
    }

    fn call_release(&self, payload: RequestPayload) -> Result<ResponseEnvelope, FuseError> {
        self.call_with_cache_policy(payload, false, false)
    }

    fn call_with_cache_policy(
        &self,
        payload: RequestPayload,
        require_healthy_cache: bool,
        drain_before_call: bool,
    ) -> Result<ResponseEnvelope, FuseError> {
        self.metrics.calls_total.fetch_add(1, Ordering::Relaxed);
        let operation = payload.operation();
        let started = std::time::Instant::now();
        let _span = tracing::debug_span!(
            "fuse_adapter_call",
            namespace = %self.namespace,
            operation = operation.as_str(),
            require_healthy_cache,
            drain_before_call
        )
        .entered();
        if drain_before_call {
            self.drain_remote_invalidations()?;
        }
        if require_healthy_cache && self.cache.poisoned() {
            self.metrics
                .stale_cache_total
                .fetch_add(1, Ordering::Relaxed);
            let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            self.metrics.call_latency_micros.record(latency);
            return Err(FuseError::StaleCache);
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let mut request = RequestEnvelope::new(
            format!("fuse-{request_id}"),
            self.namespace.clone(),
            0,
            pb::TraceContext::default(),
            payload,
        )
        .map_err(|error| FuseError::Protocol(error.to_string()))?;
        request.caller = active_caller_context();
        let response = match self.client.call(request.clone()) {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(
                    namespace = %self.namespace,
                    operation = request.operation.as_str(),
                    request_id = %request.request_id,
                    error = ?error,
                    "filesystem RPC failed"
                );
                self.metrics.call_failures.fetch_add(1, Ordering::Relaxed);
                let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                self.metrics.call_latency_micros.record(latency);
                self.poison_cache_after_uncertain_path_mutation(&request);
                return Err(map_transport_error(error));
            }
        };
        if response.ok {
            if let Err(error) = response.validate_identity_and_payload_for_request(&request) {
                self.metrics.call_failures.fetch_add(1, Ordering::Relaxed);
                let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                self.metrics.call_latency_micros.record(latency);
                self.poison_cache_after_uncertain_path_mutation(&request);
                return Err(FuseError::Protocol(error.to_string()));
            }
            for invalidation in &response.invalidations {
                self.apply_success_response_invalidation(invalidation)?;
            }
            self.require_covering_path_invalidation(&request, &response)?;
            let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            self.metrics.call_latency_micros.record(latency);
            Ok(response)
        } else {
            if let Err(error) = response.validate_for_request(&request) {
                self.metrics.call_failures.fetch_add(1, Ordering::Relaxed);
                let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                self.metrics.call_latency_micros.record(latency);
                self.poison_cache_after_uncertain_path_mutation(&request);
                return Err(FuseError::Protocol(error.to_string()));
            }
            self.metrics.call_failures.fetch_add(1, Ordering::Relaxed);
            let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            self.metrics.call_latency_micros.record(latency);
            Err(FuseError::Response {
                errno: response.errno.unwrap_or(Errno::Io),
                message: response.error_message,
            })
        }
    }

    fn apply_success_response_invalidation(
        &self,
        invalidation: &pb::Invalidation,
    ) -> Result<(), FuseError> {
        self.cache
            .apply_success_response_invalidation(&self.namespace, invalidation)
    }

    fn path_for_inode(&self, inode: u64) -> Result<String, FuseError> {
        self.drain_remote_invalidations()?;
        self.path_for_inode_after_drain(inode)
    }

    fn path_for_inode_after_drain(&self, inode: u64) -> Result<String, FuseError> {
        self.cache.path_for_inode(inode)
    }

    fn path_for_handle_bound_call(&self, inode: u64, handle: u64) -> Result<String, FuseError> {
        self.drain_remote_invalidations()?;
        self.path_for_handle_bound_call_after_drain(inode, handle)
    }

    fn path_for_handle_bound_call_after_drain(
        &self,
        inode: u64,
        handle: u64,
    ) -> Result<String, FuseError> {
        self.cache.path_for_handle_bound_call(inode, handle)
    }

    fn path_for_release(&self, inode: u64, handle: u64) -> Result<String, FuseError> {
        self.cache.path_for_release(inode, handle)
    }

    fn remember_handle_path(&self, inode: u64, handle: u64, path: String) {
        self.cache.remember_handle_path(inode, handle, path);
    }

    fn forget_handle_path(&self, inode: u64, handle: u64) {
        self.cache.forget_handle_path(inode, handle);
    }

    fn child_path(&self, parent: u64, name: &str) -> Result<String, FuseError> {
        self.drain_remote_invalidations()?;
        self.child_path_after_drain(parent, name)
    }

    fn child_path_after_drain(&self, parent: u64, name: &str) -> Result<String, FuseError> {
        self.cache.child_path(parent, name)
    }

    fn parent_and_path_for_inode_after_drain(
        &self,
        inode: u64,
    ) -> Result<(u64, String), FuseError> {
        self.cache.parent_and_path_for_inode(inode)
    }

    fn rename_paths_after_drain(
        &self,
        old_parent: u64,
        old_name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(String, String), FuseError> {
        self.cache
            .rename_paths(old_parent, old_name, new_parent, new_name)
    }

    fn hardlink_paths_after_drain(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(String, String), FuseError> {
        self.cache.hardlink_paths(inode, new_parent, new_name)
    }

    fn copy_file_range_paths_after_drain(
        &self,
        input_inode: u64,
        input_handle: u64,
        output_inode: u64,
        output_handle: u64,
    ) -> Result<(String, String), FuseError> {
        self.cache
            .copy_file_range_paths(input_inode, input_handle, output_inode, output_handle)
    }

    fn require_covering_path_invalidation(
        &self,
        request: &RequestEnvelope,
        response: &ResponseEnvelope,
    ) -> Result<(), FuseError> {
        let result = self
            .cache
            .require_covering_path_invalidation(request, response);
        if matches!(result, Err(FuseError::StaleCache)) {
            self.metrics
                .cache_poison_total
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn poison_cache_after_uncertain_path_mutation(&self, request: &RequestEnvelope) {
        if self.cache.poison_after_uncertain_path_mutation(request) {
            self.metrics
                .cache_poison_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn drain_remote_invalidations(&self) -> Result<(), FuseError> {
        self.metrics
            .invalidation_drains_total
            .fetch_add(1, Ordering::Relaxed);
        let invalidations = match self.client.drain_invalidations(&self.namespace) {
            Ok(invalidations) => invalidations,
            Err(error) => {
                tracing::warn!(
                    namespace = %self.namespace,
                    error = ?error,
                    "remote invalidation drain failed"
                );
                self.metrics
                    .invalidation_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                self.poison_cache();
                return Err(map_transport_error(error));
            }
        };
        tracing::trace!(
            namespace = %self.namespace,
            count = invalidations.len(),
            "drained remote invalidations"
        );
        let mut batch_error = None;
        for invalidation in invalidations {
            match self
                .cache
                .apply_invalidation(&self.namespace, &invalidation, true)
            {
                Ok(outcome) => {
                    if outcome.full_resync {
                        batch_error = None;
                    }
                }
                Err(error) => {
                    if batch_error.is_none() {
                        batch_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = batch_error {
            self.metrics
                .invalidation_errors_total
                .fetch_add(1, Ordering::Relaxed);
            Err(error)
        } else {
            Ok(())
        }
    }

    fn poison_cache(&self) {
        self.metrics
            .cache_poison_total
            .fetch_add(1, Ordering::Relaxed);
        self.cache.poison();
    }
}

impl AdapterMetricsHandle {
    pub fn snapshot(&self) -> AdapterMetricsSnapshot {
        self.metrics.snapshot()
    }
}

fn active_caller_context() -> Option<pb::CallerContext> {
    CALLER_CONTEXT.with(|caller| caller.borrow().clone())
}

fn path_dto(value: &str) -> Result<pb::PathDto, FuseError> {
    path(value).map_err(|error| FuseError::Protocol(error.to_string()))
}

fn take_payload(response: ResponseEnvelope) -> Result<ResponsePayload, FuseError> {
    response
        .payload
        .ok_or_else(|| FuseError::Protocol("successful response missing payload".into()))
}

fn required_attr(
    value: Option<pb::FileAttr>,
    operation: Operation,
) -> Result<pb::FileAttr, FuseError> {
    value.ok_or(FuseError::UnexpectedPayload(operation))
}

fn map_transport_error(error: RpcError) -> FuseError {
    FuseError::Transport {
        errno: error.errno(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_protocol::{directory_attr, InvalidationKind};
    use std::sync::{Arc, Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn rename_path_snapshot_blocks_concurrent_drain_between_cached_reads() {
        let client = BlockingDrainClient::new(vec![test_invalidation(
            1,
            InvalidationKind::Rename,
            "",
            "/dir",
            "/renamed",
            0,
        )]);
        let adapter = Arc::new(FuseAdapter::new(client.clone(), "fuse-ns"));
        adapter.cache.insert_lookup_for_test("/dir".into(), 10, 1);

        let adapter_for_drain = Arc::clone(&adapter);
        let client_for_wait = client.clone();
        let mut drain_thread = None;
        let (old_path, new_path) = adapter
            .cache
            .with_cache_snapshot_for_test(|cache| {
                let old_path = cache.child_path(10, "file.txt")?;
                drain_thread = Some(thread::spawn(move || {
                    adapter_for_drain
                        .getattr(1)
                        .expect("concurrent getattr drains invalidation");
                }));
                client_for_wait.wait_for_drain_started();
                let new_path = cache.child_path(10, "target.txt")?;
                Ok((old_path, new_path))
            })
            .expect("snapshot path derivation");

        drain_thread
            .expect("drain thread started")
            .join()
            .expect("drain thread completed");
        assert_eq!(old_path, "/dir/file.txt");
        assert_eq!(new_path, "/dir/target.txt");
        assert_eq!(adapter.cached_path(10), Some("/renamed".into()));
    }

    #[test]
    fn readdir_snapshot_blocks_concurrent_drain_between_parent_and_path_reads() {
        let client = BlockingDrainClient::new(vec![test_invalidation(
            1,
            InvalidationKind::Rename,
            "",
            "/old-parent/dir",
            "/new-parent/dir",
            0,
        )]);
        let adapter = Arc::new(FuseAdapter::new(client.clone(), "fuse-ns"));
        adapter
            .cache
            .insert_lookup_for_test("/old-parent".into(), 20, 1);
        adapter
            .cache
            .insert_lookup_for_test("/new-parent".into(), 30, 1);
        adapter
            .cache
            .insert_lookup_for_test("/old-parent/dir".into(), 10, 1);

        let adapter_for_drain = Arc::clone(&adapter);
        let client_for_wait = client.clone();
        let mut drain_thread = None;
        let (parent, path) = adapter
            .cache
            .with_cache_snapshot_for_test(|cache| {
                let parent = cache.parent_inode(10)?;
                drain_thread = Some(thread::spawn(move || {
                    adapter_for_drain
                        .getattr(1)
                        .expect("concurrent getattr drains invalidation");
                }));
                client_for_wait.wait_for_drain_started();
                let path = cache.path_for_inode(10)?;
                Ok((parent, path))
            })
            .expect("snapshot parent/path derivation");

        drain_thread
            .expect("drain thread started")
            .join()
            .expect("drain thread completed");
        assert_eq!(parent, 20);
        assert_eq!(path, "/old-parent/dir");
        assert_eq!(adapter.cached_path(10), Some("/new-parent/dir".into()));
        assert_eq!(adapter.parent_inode(10).expect("renamed parent"), 30);
    }

    #[derive(Clone)]
    struct BlockingDrainClient {
        state: Arc<BlockingDrainState>,
    }

    struct BlockingDrainState {
        invalidations: StdMutex<Vec<pb::Invalidation>>,
        drain_started: StdMutex<bool>,
        drain_started_cv: Condvar,
    }

    impl BlockingDrainClient {
        fn new(invalidations: Vec<pb::Invalidation>) -> Self {
            Self {
                state: Arc::new(BlockingDrainState {
                    invalidations: StdMutex::new(invalidations),
                    drain_started: StdMutex::new(false),
                    drain_started_cv: Condvar::new(),
                }),
            }
        }

        fn wait_for_drain_started(&self) {
            let mut started = self.state.drain_started.lock().expect("drain started lock");
            while !*started {
                let (next, timeout) = self
                    .state
                    .drain_started_cv
                    .wait_timeout(started, Duration::from_secs(5))
                    .expect("drain started condvar");
                started = next;
                assert!(
                    !timeout.timed_out(),
                    "concurrent drain did not reach the invalidation boundary"
                );
            }
        }
    }

    impl RpcClient for BlockingDrainClient {
        fn call(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError> {
            ResponseEnvelope::success_for(
                &request,
                ResponsePayload::Getattr(pb::GetattrResponse {
                    attr: Some(directory_attr(1)),
                }),
                Vec::new(),
            )
            .map_err(|error| RpcError::Malformed(error.to_string()))
        }

        fn drain_invalidations(&self, namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
            let mut invalidations = self.state.invalidations.lock().expect("invalidations lock");
            let mut drained = Vec::new();
            let mut retained = Vec::new();
            for invalidation in std::mem::take(&mut *invalidations) {
                if invalidation.namespace == namespace {
                    drained.push(invalidation);
                } else {
                    retained.push(invalidation);
                }
            }
            *invalidations = retained;
            if !drained.is_empty() {
                let mut started = self.state.drain_started.lock().expect("drain started lock");
                *started = true;
                self.state.drain_started_cv.notify_all();
            }
            Ok(drained)
        }
    }

    fn test_invalidation(
        sequence: u64,
        kind: InvalidationKind,
        path: &str,
        old_path: &str,
        new_path: &str,
        inode: u64,
    ) -> pb::Invalidation {
        pb::Invalidation {
            namespace: "fuse-ns".into(),
            sequence,
            kind: kind.wire_value(),
            path: path.into(),
            old_path: old_path.into(),
            new_path: new_path.into(),
            inode,
            handle: 0,
            request_id: "remote".into(),
        }
    }
}
