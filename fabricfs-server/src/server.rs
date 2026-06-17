mod advanced_io;
mod byte_io;
mod errors;
mod locks;
mod metadata;
mod paths;
mod permissions;
mod xattrs;

pub use advanced_io::{
    apply_mode_zero_fallocate, checked_fallocate_end, ensure_fallocate_mode_supported, seek_offset,
};
pub use byte_io::{copy_file_range_at, read_file_at, write_file_at};
pub use errors::{errno, io_errno};
pub use locks::{lock_key_for_file, lock_required_access, FlockTable, LockTable};
pub use metadata::metadata_to_stat;
pub use paths::{
    append_rel, descendant_suffix, dir_is_empty, ensure_parent_search_allowed, normalize_path,
    parent_rel, path_to_cstring, require_context, strip_root,
};
pub use permissions::{
    apply_handle_setattr, apply_ownership, current_process_umask, ensure_access_bits,
    ensure_dir_creation_allowed, ensure_file_creation_allowed, ensure_not_symlink,
    ensure_open_flags_allowed, ensure_owner_or_root, ensure_read_allowed,
    ensure_read_write_allowed, ensure_regular_file, ensure_removal_allowed, ensure_root,
    ensure_search_allowed, ensure_setattr_allowed, ensure_stat_access_bits, ensure_sticky_allowed,
    ensure_write_allowed, open_access_bits,
};
pub use xattrs::{
    copy_acl_xattrs, is_acl_name, list_xattr_names, read_all_xattrs, read_xattr, write_xattr,
};

use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FuseContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stat {
    pub dev: u64,
    pub ino: u64,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatFs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u64,
    pub namelen: u64,
    pub frsize: u64,
}

/// Options for configuring filesystem behavior
#[derive(Clone, Debug)]
pub struct FsOptions {
    pub limits: FsLimits,
    pub umask: u32,
    pub propagate_acls: bool,
    pub allow_backing_permission_updates: bool,
    pub allow_xattr_updates: bool,
    pub allow_direct_backing_updates: bool,
}

pub const DEFAULT_FS_UMASK: u32 = 0o022;

impl FsOptions {
    pub fn with_limits(limits: FsLimits) -> Self {
        Self {
            limits: limits.validated(),
            umask: DEFAULT_FS_UMASK,
            propagate_acls: false,
            allow_backing_permission_updates: false,
            allow_xattr_updates: false,
            allow_direct_backing_updates: false,
        }
        .normalized()
    }

    pub fn normalized(mut self) -> Self {
        self.umask &= 0o777;
        self
    }
}

impl Default for FsOptions {
    fn default() -> Self {
        Self::with_limits(FsLimits::default())
    }
}

/// Limits for I/O operations
#[derive(Clone, Debug)]
pub struct FsLimits {
    pub io_chunk_bytes: usize,
    pub max_read_bytes: usize,
}

impl FsLimits {
    pub fn new(io_chunk_bytes: usize, max_read_bytes: usize) -> Self {
        Self {
            io_chunk_bytes,
            max_read_bytes,
        }
        .validated()
    }

    pub fn validated(self) -> Self {
        let chunk = self.io_chunk_bytes.max(1);
        let max_read = self.max_read_bytes.max(1);
        Self {
            io_chunk_bytes: chunk,
            max_read_bytes: max_read,
        }
    }
}

impl Default for FsLimits {
    fn default() -> Self {
        Self {
            io_chunk_bytes: 1024 * 1024,     // 1 MiB
            max_read_bytes: 4 * 1024 * 1024, // 4 MiB
        }
    }
}

/// Handle kind for open file/directory handles
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleKind {
    File,
    Dir,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyFileRangeHandles {
    pub fh_in: u64,
    pub fh_out: u64,
}

/// File lock information
#[derive(Clone, Copy, Debug)]
pub struct FileLock {
    pub start: u64,
    pub end: u64,
    pub typ: i32,
    pub pid: u32,
}

/// Dynamic server storage type used by the RPC service adapter.
pub type DynStorage = Arc<dyn ServerStorage + 'static>;

/// Server-domain storage seam exposed to the filesystem service adapter.
pub trait ServerStorage:
    NamespaceStorage + MetadataStorage + DirectoryStorage + OpenedObjectStorage + Send + Sync
{
    fn namespace(&self) -> &dyn NamespaceStorage;
    fn metadata(&self) -> &dyn MetadataStorage;
    fn directories(&self) -> &dyn DirectoryStorage;
    fn runtime(&self) -> &dyn OpenedObjectStorage;
}

impl<T> ServerStorage for T
where
    T: NamespaceStorage + MetadataStorage + DirectoryStorage + OpenedObjectStorage + Send + Sync,
{
    fn namespace(&self) -> &dyn NamespaceStorage {
        self
    }

    fn metadata(&self) -> &dyn MetadataStorage {
        self
    }

    fn directories(&self) -> &dyn DirectoryStorage {
        self
    }

    fn runtime(&self) -> &dyn OpenedObjectStorage {
        self
    }
}

/// Namespace mutation and existence behavior owned by storage path adapters.
pub trait NamespaceStorage: Send + Sync {
    fn mkdir(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<(), i32>;
    fn unlink(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32>;
    fn rmdir(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32>;
    fn rename(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32>;
    fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32>;
    fn mknod(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<Stat, i32>;
    fn exists(&self, path: &str) -> Result<(), i32>;
    fn link(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32>;
    fn symlink(&self, path: &str, target: Vec<u8>, ctx: Option<FuseContext>) -> Result<(), i32>;
}

/// Metadata, stat, and extended-attribute behavior for resolved storage paths.
pub trait MetadataStorage: Send + Sync {
    fn readlink(&self, path: &str) -> Result<Vec<u8>, i32>;
    fn stat(&self, path: &str) -> Result<Stat, i32>;
    fn statfs(&self) -> Result<StatFs, i32>;
    fn setattr(
        &self,
        path: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32>;
    fn setxattr(&self, path: &str, name: &str, value: Vec<u8>, flags: i32) -> Result<(), i32>;
    fn getxattr(&self, path: &str, name: &str, size: u32) -> Result<Vec<u8>, i32>;
    fn listxattr(&self, path: &str) -> Result<Vec<String>, i32>;
    fn removexattr(&self, path: &str, name: &str) -> Result<(), i32>;
}

/// Directory view behavior after storage path adapters resolve visibility.
pub trait DirectoryStorage: Send + Sync {
    fn list_dir(&self, path: &str) -> Result<Vec<(String, Stat)>, i32>;
}

/// Runtime behavior for opened objects, handles, locks, durability, and byte IO.
pub trait OpenedObjectStorage: Send + Sync {
    fn read(
        &self,
        path: &str,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32>;
    fn read_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32>;
    fn write(
        &self,
        path: &str,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32>;
    fn write_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32>;
    fn setattr_fh(
        &self,
        path: &str,
        handle: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32>;
    fn open(
        &self,
        path: &str,
        kind: HandleKind,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32>;
    fn release_fh(&self, fh: u64);
    fn sync_dir_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32>;
    fn sync_file(&self, path: &str, datasync: bool) -> Result<(), i32>;
    fn sync_file_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32>;
    fn check_handle(&self, path: &str, fh: Option<u64>, expected: HandleKind) -> Result<(), i32>;
    fn release_posix_locks(&self, path: &str, handle: u64, owner: u64) -> Result<(), i32>;
    fn getlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32>;
    fn setlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32>;
    fn flock(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        operation: i32,
        _pid: u32,
    ) -> Result<(), i32>;
    fn copy_file_range(
        &self,
        from: &str,
        to: &str,
        handles: CopyFileRangeHandles,
        offset_in: i64,
        offset_out: i64,
        len: u64,
        ctx: Option<FuseContext>,
    ) -> Result<u64, i32>;
    fn fallocate(
        &self,
        path: &str,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32>;
    fn lseek(&self, path: &str, handle: u64, offset: i64, whence: i32) -> Result<i64, i32>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_options_default_uses_stable_release_umask() {
        assert_eq!(FsOptions::default().umask, DEFAULT_FS_UMASK);
    }
}
