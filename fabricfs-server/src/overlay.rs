mod copy_up;
mod directory_ops;
mod fs_ops;
mod layout;
mod metadata_ops;
mod namespace_ops;
mod objects;
mod runtime_ops;
mod tombstones;
mod visibility;
mod xattrs;

use std::path::PathBuf;
use std::sync::Arc;

use crate::root::StorageInitError;
use crate::server::{
    CopyFileRangeHandles, DirectoryStorage, FileLock, FsLimits, FuseContext, HandleKind,
    MetadataStorage, NamespaceStorage, OpenedObjectStorage, Stat, StatFs,
};
use crate::storage_runtime::StorageRuntime;
use crate::watch::InternalMetadataNotifier;

use layout::Layout;
use xattrs::XattrStore;

/// OverlayFs implements an overlay filesystem with:
/// - Copy-on-write (COW) overlay
/// - Tombstones for deleted files
/// - Alias paths for visibility control
/// - Extended attribute persistence
///
/// The resolution order for reads is:
/// 1. Check tombstones → ENOENT if tombstoned
/// 2. Check COW overlay → use if exists
/// 3. Check backing root → use if exists
/// 4. Return ENOENT
///
/// For writes, files are copied from backing to COW before mutation (copy-up).
#[derive(Clone)]
pub struct OverlayFs {
    state: Arc<OverlayState>,
}

struct OverlayState {
    layout: Layout,
    limits: FsLimits,
    umask: u32,
    propagate_acls: bool,
    allow_backing_permission_updates: bool,
    allow_xattr_updates: bool,
    allow_direct_backing_updates: bool,
    enable_reflinks: bool,
    preserve_sparse_files: bool,
    runtime: StorageRuntime,
    internal_metadata_notifier: Option<Arc<dyn InternalMetadataNotifier>>,
    xattrs: XattrStore,
}

impl OverlayFs {
    /// Create a new OverlayFs.
    ///
    /// - `backing_root`: Optional read-only backing directory
    /// - `alias_root`: Optional alias directory (enables mutations)
    /// - `cow_root`: Optional copy-on-write overlay directory
    /// - `limits`: I/O limits
    /// - `umask`: Umask for new files
    /// - `propagate_acls`: Copy ACL xattrs during copy-up
    /// - `allow_backing_permission_updates`: Allow chmod/chown on backing files
    /// - `allow_xattr_updates`: Allow xattr changes on backing files
    /// - `allow_direct_backing_updates`: Allow writes directly to backing tree (no COW)
    /// - `enable_reflinks`: Enable reflink-based copy optimizations (default: true)
    /// - `preserve_sparse_files`: Preserve holes in sparse files during copy (default: true)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backing_root: Option<PathBuf>,
        alias_root: Option<PathBuf>,
        cow_root: Option<PathBuf>,
        limits: FsLimits,
        umask: u32,
        propagate_acls: bool,
        allow_backing_permission_updates: bool,
        allow_xattr_updates: bool,
        allow_direct_backing_updates: bool,
        enable_reflinks: bool,
        preserve_sparse_files: bool,
    ) -> Result<Self, StorageInitError> {
        Self::new_with_internal_metadata_notifier(
            backing_root,
            alias_root,
            cow_root,
            limits,
            umask,
            propagate_acls,
            allow_backing_permission_updates,
            allow_xattr_updates,
            allow_direct_backing_updates,
            enable_reflinks,
            preserve_sparse_files,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_internal_metadata_notifier(
        backing_root: Option<PathBuf>,
        alias_root: Option<PathBuf>,
        cow_root: Option<PathBuf>,
        limits: FsLimits,
        umask: u32,
        propagate_acls: bool,
        allow_backing_permission_updates: bool,
        allow_xattr_updates: bool,
        allow_direct_backing_updates: bool,
        enable_reflinks: bool,
        preserve_sparse_files: bool,
        internal_metadata_notifier: Option<Arc<dyn InternalMetadataNotifier>>,
    ) -> Result<Self, StorageInitError> {
        let layout = Layout::new(backing_root, alias_root, cow_root)?;
        let xattrs = XattrStore::new(layout.xattr_root(), internal_metadata_notifier.clone())?;

        let state = OverlayState {
            layout,
            limits: limits.clone(),
            umask,
            propagate_acls,
            allow_backing_permission_updates,
            allow_xattr_updates,
            allow_direct_backing_updates,
            enable_reflinks,
            preserve_sparse_files,
            runtime: StorageRuntime::new(limits),
            internal_metadata_notifier,
            xattrs,
        };

        Ok(Self {
            state: Arc::new(state),
        })
    }
}

impl MetadataStorage for OverlayFs {
    fn readlink(&self, path: &str) -> Result<Vec<u8>, i32> {
        OverlayFs::readlink(self, path)
    }

    fn stat(&self, path: &str) -> Result<Stat, i32> {
        OverlayFs::stat(self, path)
    }

    fn statfs(&self) -> Result<StatFs, i32> {
        OverlayFs::statfs(self)
    }

    fn setattr(
        &self,
        path: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        OverlayFs::setattr(self, path, mode, uid, gid, size, ctx)
    }

    fn setxattr(&self, path: &str, name: &str, value: Vec<u8>, flags: i32) -> Result<(), i32> {
        OverlayFs::setxattr(self, path, name, value, flags)
    }

    fn getxattr(&self, path: &str, name: &str, size: u32) -> Result<Vec<u8>, i32> {
        OverlayFs::getxattr(self, path, name, size)
    }

    fn listxattr(&self, path: &str) -> Result<Vec<String>, i32> {
        OverlayFs::listxattr(self, path)
    }

    fn removexattr(&self, path: &str, name: &str) -> Result<(), i32> {
        OverlayFs::removexattr(self, path, name)
    }
}

impl DirectoryStorage for OverlayFs {
    fn list_dir(&self, path: &str) -> Result<Vec<(String, Stat)>, i32> {
        OverlayFs::list_dir(self, path)
    }
}

impl NamespaceStorage for OverlayFs {
    fn mkdir(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::mkdir(self, path, mode, ctx)
    }

    fn unlink(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::unlink(self, path, ctx)
    }

    fn rmdir(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::rmdir(self, path, ctx)
    }

    fn rename(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::rename(self, from, to, ctx)
    }

    fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        OverlayFs::create_file(self, path, mode, flags, ctx)
    }

    fn mknod(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<Stat, i32> {
        OverlayFs::mknod(self, path, mode, ctx)
    }

    fn exists(&self, path: &str) -> Result<(), i32> {
        OverlayFs::exists(self, path)
    }

    fn link(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::link(self, from, to, ctx)
    }

    fn symlink(&self, path: &str, target: Vec<u8>, ctx: Option<FuseContext>) -> Result<(), i32> {
        OverlayFs::symlink(self, path, target, ctx)
    }
}

impl OpenedObjectStorage for OverlayFs {
    fn read(
        &self,
        path: &str,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        OverlayFs::read(self, path, offset, size, ctx)
    }

    fn read_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        OverlayFs::read_fh(self, path, fh, offset, size, ctx)
    }

    fn write(
        &self,
        path: &str,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        OverlayFs::write(self, path, offset, data, ctx)
    }

    fn write_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        OverlayFs::write_fh(self, path, fh, offset, data, ctx)
    }

    fn setattr_fh(
        &self,
        path: &str,
        handle: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        OverlayFs::setattr_fh(self, path, handle, mode, uid, gid, size, ctx)
    }

    fn open(
        &self,
        path: &str,
        kind: HandleKind,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        OverlayFs::open(self, path, kind, flags, ctx)
    }

    fn release_fh(&self, fh: u64) {
        OverlayFs::release_fh(self, fh);
    }

    fn sync_dir_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        OverlayFs::sync_dir_fh(self, path, fh, datasync)
    }

    fn sync_file(&self, path: &str, datasync: bool) -> Result<(), i32> {
        OverlayFs::sync_file(self, path, datasync)
    }

    fn sync_file_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        OverlayFs::sync_file_fh(self, path, fh, datasync)
    }

    fn check_handle(&self, path: &str, fh: Option<u64>, expected: HandleKind) -> Result<(), i32> {
        OverlayFs::check_handle(self, path, fh, expected)
    }

    fn release_posix_locks(&self, path: &str, handle: u64, owner: u64) -> Result<(), i32> {
        OverlayFs::release_posix_locks(self, path, handle, owner)
    }

    fn getlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32> {
        OverlayFs::getlk(self, path, handle, owner, start, end, typ)
    }

    fn setlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32> {
        OverlayFs::setlk(self, path, handle, owner, start, end, typ, pid)
    }

    fn flock(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        operation: i32,
        pid: u32,
    ) -> Result<(), i32> {
        OverlayFs::flock(self, path, handle, owner, operation, pid)
    }

    fn copy_file_range(
        &self,
        from: &str,
        to: &str,
        handles: CopyFileRangeHandles,
        offset_in: i64,
        offset_out: i64,
        len: u64,
        ctx: Option<FuseContext>,
    ) -> Result<u64, i32> {
        OverlayFs::copy_file_range(self, from, to, handles, offset_in, offset_out, len, ctx)
    }

    fn fallocate(
        &self,
        path: &str,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32> {
        OverlayFs::fallocate(self, path, handle, offset, length, mode)
    }

    fn lseek(&self, path: &str, handle: u64, offset: i64, whence: i32) -> Result<i64, i32> {
        OverlayFs::lseek(self, path, handle, offset, whence)
    }
}
