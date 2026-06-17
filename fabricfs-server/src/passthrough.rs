use std::ffi::CString;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::root::{require_existing_dir, StorageInitError};
use crate::server::{
    apply_ownership, dir_is_empty, ensure_dir_creation_allowed, ensure_file_creation_allowed,
    ensure_open_flags_allowed, ensure_parent_search_allowed, ensure_regular_file,
    ensure_removal_allowed, ensure_search_allowed, ensure_setattr_allowed, ensure_sticky_allowed,
    errno, io_errno, metadata_to_stat, normalize_path, open_access_bits, parent_rel,
    path_to_cstring, require_context, strip_root, CopyFileRangeHandles, DirectoryStorage, FileLock,
    FsOptions, FuseContext, HandleKind, MetadataStorage, NamespaceStorage, OpenedObjectStorage,
    Stat, StatFs,
};
use crate::storage_runtime::{ResolvedStorageObject, StorageRuntime};

/// PassthroughFs is a simple filesystem backend that delegates all operations
/// to a single root directory on the local filesystem.
///
/// This is the base implementation with no overlay, tombstone, or COW logic.
/// It provides:
/// - Basic POSIX filesystem operations
/// - Handle management for open files/directories
/// - File locking
/// - Extended attribute support (delegated to underlying FS)
#[derive(Clone)]
pub struct PassthroughFs {
    state: Arc<PassthroughState>,
}

struct PassthroughState {
    root: PathBuf,
    umask: u32,
    runtime: StorageRuntime,
}

impl PassthroughFs {
    pub fn new(root: PathBuf, options: FsOptions) -> Result<Self, StorageInitError> {
        let options = options.normalized();
        let limits = options.limits.validated();
        let root = require_existing_dir(root, "passthrough_root")?;

        let state = PassthroughState {
            root,
            umask: options.umask,
            runtime: StorageRuntime::new(limits),
        };

        Ok(Self {
            state: Arc::new(state),
        })
    }

    fn resolve_path(&self, rel: &str) -> PathBuf {
        self.state.root.join(strip_root(rel))
    }

    fn resolved_object(&self, rel: &str) -> ResolvedStorageObject {
        ResolvedStorageObject::new(rel, self.resolve_path(rel))
    }

    fn apply_umask(&self, mode: u32) -> u32 {
        mode & !self.state.umask
    }

    pub fn read(
        &self,
        path: &str,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .read_path(&self.resolved_object(&rel), offset, size, ctx)
    }

    pub fn read_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        let _ = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .read_fh(&self.resolved_object(&rel), fh, offset, size)
    }

    pub fn write(
        &self,
        path: &str,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .write_path(&self.resolved_object(&rel), offset, data, ctx)
    }

    pub fn write_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        let _ = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .write_fh(&self.resolved_object(&rel), fh, offset, data)
    }

    pub fn sync_file_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .sync_file_fh(&self.resolved_object(&rel), fh, datasync)
    }

    pub fn mkdir(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        ensure_dir_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;

        fs::create_dir(&target).map_err(io_errno)?;
        let masked = self.apply_umask(mode);
        let perm = PermissionsExt::from_mode(libc::S_IFDIR | masked);
        fs::set_permissions(&target, perm).map_err(io_errno)?;
        apply_ownership(&target, ctx.uid, ctx.gid, true)?;
        Ok(())
    }

    pub fn unlink(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        let meta = fs::symlink_metadata(&target).map_err(io_errno)?;
        if meta.is_dir() {
            return Err(libc::EISDIR);
        }

        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        ensure_removal_allowed(&parent_meta, &meta, ctx.uid, ctx.gid)?;
        ensure_sticky_allowed(&parent_meta, &meta, ctx.uid)?;

        fs::remove_file(&target).map_err(io_errno)?;
        Ok(())
    }

    pub fn rmdir(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        if !dir_is_empty(&target)? {
            return Err(libc::ENOTEMPTY);
        }

        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        let meta = fs::symlink_metadata(&target).map_err(io_errno)?;
        ensure_removal_allowed(&parent_meta, &meta, ctx.uid, ctx.gid)?;
        ensure_sticky_allowed(&parent_meta, &meta, ctx.uid)?;

        fs::remove_dir(&target).map_err(io_errno)?;
        Ok(())
    }

    pub fn rename(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let from_rel = normalize_path(from)?;
        let to_rel = normalize_path(to)?;

        let src = self.resolve_path(&from_rel);
        let src_meta = fs::symlink_metadata(&src).map_err(io_errno)?;

        let from_parent_rel = parent_rel(&from_rel);
        let from_parent = self.resolve_path(&from_parent_rel);
        let from_parent_meta = fs::symlink_metadata(&from_parent).map_err(io_errno)?;
        ensure_removal_allowed(&from_parent_meta, &src_meta, ctx.uid, ctx.gid)?;

        let to_parent_rel = parent_rel(&to_rel);
        let to_parent = self.resolve_path(&to_parent_rel);
        let to_parent_meta = fs::symlink_metadata(&to_parent).map_err(io_errno)?;
        ensure_dir_creation_allowed(&to_parent_meta, ctx.uid, ctx.gid)?;

        let dst = self.resolve_path(&to_rel);
        let dest_meta = fs::symlink_metadata(&dst).ok();
        let dest_meta = dest_meta.as_ref().unwrap_or(&src_meta);
        ensure_sticky_allowed(&to_parent_meta, dest_meta, ctx.uid)?;

        if let Ok(meta) = fs::symlink_metadata(&dst) {
            if meta.is_dir() && !dir_is_empty(&dst)? {
                return Err(libc::ENOTEMPTY);
            }
            if meta.is_dir() {
                fs::remove_dir(&dst).map_err(io_errno)?;
            } else {
                fs::remove_file(&dst).map_err(io_errno)?;
            }
        }

        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        fs::rename(&src, &dst).map_err(io_errno)?;
        self.state.runtime.rewrite_paths(&from_rel, &to_rel)?;
        Ok(())
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let access_bits = open_access_bits(flags)?;

        let existing_meta = match fs::symlink_metadata(&target) {
            Ok(meta) if meta.file_type().is_symlink() => {
                Some(fs::metadata(&target).map_err(io_errno)?)
            }
            Ok(meta) => Some(meta),
            Err(_) => None,
        };
        let existed = existing_meta.is_some();
        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        if existed {
            ensure_search_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        } else {
            ensure_dir_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        }
        if flags & libc::O_EXCL != 0 && existed {
            return Err(libc::EEXIST);
        }
        if let Some(meta) = existing_meta.as_ref() {
            ensure_regular_file(meta)?;
            ensure_open_flags_allowed(meta, ctx.uid, ctx.gid, flags)?;
        }

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        let truncate = flags & libc::O_TRUNC != 0;
        let mut opts = OpenOptions::new();
        let masked = self.apply_umask(mode);
        opts.create(!existed)
            .read(access_bits & 0o4 != 0)
            .write(access_bits & 0o2 != 0 || !existed)
            .truncate(false)
            .mode(masked & 0o7777);
        let file = opts.open(&target).map_err(io_errno)?;
        if !existed {
            let perm = PermissionsExt::from_mode(libc::S_IFREG | masked);
            fs::set_permissions(&target, perm).map_err(io_errno)?;
            apply_ownership(&target, ctx.uid, ctx.gid, true)?;
        }

        if existed && truncate {
            file.set_len(0).map_err(io_errno)?;
        }
        let (fh, stat) = self.state.runtime.register_file_handle(
            &self.resolved_object(&rel),
            access_bits,
            file,
        )?;
        Ok((fh, stat))
    }

    pub fn mknod(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<Stat, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;

        let cstr = path_to_cstring(&target)?;
        let masked = self.apply_umask(mode);
        let rc = unsafe { libc::mknod(cstr.as_ptr(), masked, 0) };
        if rc != 0 {
            return Err(errno());
        }
        let perm = PermissionsExt::from_mode(masked & 0o7777);
        fs::set_permissions(&target, perm).map_err(io_errno)?;
        apply_ownership(&target, ctx.uid, ctx.gid, true)?;

        let meta = fs::metadata(&target).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
    }

    pub fn setattr(
        &self,
        path: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        if let Some(sz) = size {
            let f = OpenOptions::new()
                .write(true)
                .open(&target)
                .map_err(io_errno)?;
            let meta = f.metadata().map_err(io_errno)?;
            ensure_setattr_allowed(&meta, ctx.uid, ctx.gid, mode, uid, gid, size)?;
            f.set_len(sz).map_err(io_errno)?;
        } else {
            let meta = fs::metadata(&target).map_err(io_errno)?;
            ensure_setattr_allowed(&meta, ctx.uid, ctx.gid, mode, uid, gid, size)?;
        }

        if let Some(m) = mode {
            let perm = PermissionsExt::from_mode(m);
            fs::set_permissions(&target, perm).map_err(io_errno)?;
        }

        if uid.is_some() || gid.is_some() {
            let uidv = uid.unwrap_or(u32::MAX);
            let gidv = gid.unwrap_or(u32::MAX);
            let cstr = path_to_cstring(&target)?;
            let rc = unsafe { libc::chown(cstr.as_ptr(), uidv, gidv) };
            if rc != 0 {
                return Err(errno());
            }
        }

        let meta = fs::metadata(&target).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
    }

    pub fn setattr_fh(
        &self,
        path: &str,
        fh: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .setattr_fh(&self.resolved_object(&rel), fh, mode, uid, gid, size, ctx)
    }

    pub fn open(
        &self,
        path: &str,
        kind: HandleKind,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .open_existing(&self.resolved_object(&rel), kind, flags, ctx)
    }

    pub fn check_handle(
        &self,
        path: &str,
        fh: Option<u64>,
        expected: HandleKind,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .check_handle(&self.resolved_object(&rel), fh, expected)
    }

    pub fn list_dir(&self, path: &str) -> Result<Vec<(String, Stat)>, i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);

        let mut entries = Vec::new();
        for entry in fs::read_dir(&target).map_err(io_errno)? {
            let entry = entry.map_err(io_errno)?;
            let name = entry.file_name().to_string_lossy().to_string();
            let meta = fs::symlink_metadata(entry.path()).map_err(io_errno)?;
            entries.push((name, metadata_to_stat(&meta)));
        }

        Ok(entries)
    }

    pub fn link(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let from_rel = normalize_path(from)?;
        let to_rel = normalize_path(to)?;
        let src = self.resolve_path(&from_rel);
        let dst = self.resolve_path(&to_rel);

        ensure_parent_search_allowed(&from_rel, |rel| {
            let meta = fs::metadata(self.resolve_path(rel)).map_err(io_errno)?;
            ensure_search_allowed(&meta, ctx.uid, ctx.gid)
        })?;
        fs::symlink_metadata(&src).map_err(io_errno)?;

        let parent_rel = parent_rel(&to_rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;

        fs::hard_link(&src, &dst).map_err(io_errno)?;
        Ok(())
    }

    pub fn symlink(
        &self,
        path: &str,
        target: Vec<u8>,
        ctx: Option<FuseContext>,
    ) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let link_path = self.resolve_path(&rel);

        let parent_rel = parent_rel(&rel);
        let parent = self.resolve_path(&parent_rel);
        let parent_meta = fs::symlink_metadata(&parent).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;

        if let Some(parent) = link_path.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        let target = Path::new(OsStr::from_bytes(&target));
        std::os::unix::fs::symlink(target, &link_path).map_err(io_errno)?;
        apply_ownership(&link_path, ctx.uid, ctx.gid, false)?;
        Ok(())
    }

    pub fn release_posix_locks(&self, path: &str, fh: u64, owner: u64) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .release_posix_locks(&self.resolved_object(&rel), fh, owner)
    }

    pub fn getlk(
        &self,
        path: &str,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .getlk(&self.resolved_object(&rel), fh, owner, start, end, typ)
    }

    pub fn setlk(
        &self,
        path: &str,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .setlk(&self.resolved_object(&rel), fh, owner, start, end, typ, pid)
    }

    pub fn flock(
        &self,
        path: &str,
        fh: u64,
        owner: u64,
        operation: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .flock(&self.resolved_object(&rel), fh, owner, operation, pid)
    }

    pub fn copy_file_range(
        &self,
        from: &str,
        to: &str,
        file_handles: CopyFileRangeHandles,
        offset_in: i64,
        offset_out: i64,
        len: u64,
        ctx: Option<FuseContext>,
    ) -> Result<u64, i32> {
        let _ = require_context(ctx)?;
        let from_rel = normalize_path(from)?;
        let to_rel = normalize_path(to)?;
        self.state.runtime.copy_file_range(
            &self.resolved_object(&from_rel),
            &self.resolved_object(&to_rel),
            file_handles,
            offset_in,
            offset_out,
            len,
        )
    }

    pub fn fallocate(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .fallocate(&self.resolved_object(&rel), fh, offset, length, mode)
    }

    pub fn lseek(&self, path: &str, fh: u64, offset: i64, whence: i32) -> Result<i64, i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .lseek(&self.resolved_object(&rel), fh, offset, whence)
    }
}

impl MetadataStorage for PassthroughFs {
    fn readlink(&self, path: &str) -> Result<Vec<u8>, i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let link = fs::read_link(&target).map_err(io_errno)?;
        Ok(link.into_os_string().into_vec())
    }

    fn stat(&self, path: &str) -> Result<Stat, i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let meta = fs::symlink_metadata(&target).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
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
        PassthroughFs::setattr(self, path, mode, uid, gid, size, ctx)
    }

    fn statfs(&self) -> Result<StatFs, i32> {
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let cstr = path_to_cstring(&self.state.root)?;
        let rc = unsafe { libc::statvfs(cstr.as_ptr(), &mut stat) };
        if rc != 0 {
            return Err(errno());
        }
        Ok(StatFs {
            blocks: stat.f_blocks,
            bfree: stat.f_bfree,
            bavail: stat.f_bavail,
            files: stat.f_files,
            ffree: stat.f_ffree,
            bsize: stat.f_bsize as u64,
            namelen: stat.f_namemax as u64,
            frsize: stat.f_frsize as u64,
        })
    }

    fn setxattr(&self, path: &str, name: &str, value: Vec<u8>, flags: i32) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let cpath = path_to_cstring(&target)?;
        let cname = CString::new(name).map_err(|_| libc::EINVAL)?;

        let rc = unsafe {
            libc::setxattr(
                cpath.as_ptr(),
                cname.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags,
            )
        };

        if rc != 0 {
            return Err(errno());
        }
        Ok(())
    }

    fn getxattr(&self, path: &str, name: &str, size: u32) -> Result<Vec<u8>, i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let cpath = path_to_cstring(&target)?;
        let cname = CString::new(name).map_err(|_| libc::EINVAL)?;

        if size == 0 {
            let rc =
                unsafe { libc::getxattr(cpath.as_ptr(), cname.as_ptr(), std::ptr::null_mut(), 0) };
            if rc < 0 {
                return Err(errno());
            }
            return Ok(vec![0; rc as usize]);
        }

        let mut buf = vec![0u8; size as usize];
        let rc = unsafe {
            libc::getxattr(
                cpath.as_ptr(),
                cname.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                size as usize,
            )
        };

        if rc < 0 {
            return Err(errno());
        }

        buf.truncate(rc as usize);
        Ok(buf)
    }

    fn listxattr(&self, path: &str) -> Result<Vec<String>, i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let cpath = path_to_cstring(&target)?;

        let rc = unsafe { libc::listxattr(cpath.as_ptr(), std::ptr::null_mut(), 0) };

        if rc < 0 {
            return Err(errno());
        }

        if rc == 0 {
            return Ok(vec![]);
        }

        let mut buf = vec![0u8; rc as usize];
        let rc2 = unsafe {
            libc::listxattr(
                cpath.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        if rc2 < 0 {
            return Err(errno());
        }

        buf.truncate(rc2 as usize);
        let names = buf
            .split(|&b| b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).to_string())
            .collect();

        Ok(names)
    }

    fn removexattr(&self, path: &str, name: &str) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let cpath = path_to_cstring(&target)?;
        let cname = CString::new(name).map_err(|_| libc::EINVAL)?;

        let rc = unsafe { libc::removexattr(cpath.as_ptr(), cname.as_ptr()) };

        if rc != 0 {
            return Err(errno());
        }
        Ok(())
    }
}

impl DirectoryStorage for PassthroughFs {
    fn list_dir(&self, path: &str) -> Result<Vec<(String, Stat)>, i32> {
        PassthroughFs::list_dir(self, path)
    }
}

impl NamespaceStorage for PassthroughFs {
    fn mkdir(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::mkdir(self, path, mode, ctx)
    }

    fn unlink(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::unlink(self, path, ctx)
    }

    fn rmdir(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::rmdir(self, path, ctx)
    }

    fn rename(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::rename(self, from, to, ctx)
    }

    fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        PassthroughFs::create_file(self, path, mode, flags, ctx)
    }

    fn mknod(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<Stat, i32> {
        PassthroughFs::mknod(self, path, mode, ctx)
    }

    fn exists(&self, path: &str) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        if fs::symlink_metadata(&target).is_ok() {
            Ok(())
        } else {
            Err(libc::ENOENT)
        }
    }

    fn link(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::link(self, from, to, ctx)
    }

    fn symlink(&self, path: &str, target: Vec<u8>, ctx: Option<FuseContext>) -> Result<(), i32> {
        PassthroughFs::symlink(self, path, target, ctx)
    }
}

impl OpenedObjectStorage for PassthroughFs {
    fn read(
        &self,
        path: &str,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        PassthroughFs::read(self, path, offset, size, ctx)
    }

    fn read_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        PassthroughFs::read_fh(self, path, fh, offset, size, ctx)
    }

    fn write(
        &self,
        path: &str,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        PassthroughFs::write(self, path, offset, data, ctx)
    }

    fn write_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        PassthroughFs::write_fh(self, path, fh, offset, data, ctx)
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
        PassthroughFs::setattr_fh(self, path, handle, mode, uid, gid, size, ctx)
    }

    fn open(
        &self,
        path: &str,
        kind: HandleKind,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        PassthroughFs::open(self, path, kind, flags, ctx)
    }

    fn release_fh(&self, fh: u64) {
        self.state.runtime.release_handle(fh);
    }

    fn sync_dir_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.state
            .runtime
            .sync_dir_fh(&self.resolved_object(&rel), fh, datasync)
    }

    fn sync_file(&self, path: &str, _datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.resolve_path(&rel);
        let file = File::open(&target).map_err(io_errno)?;
        file.sync_all().map_err(io_errno)?;
        Ok(())
    }

    fn sync_file_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        PassthroughFs::sync_file_fh(self, path, fh, datasync)
    }

    fn check_handle(&self, path: &str, fh: Option<u64>, expected: HandleKind) -> Result<(), i32> {
        PassthroughFs::check_handle(self, path, fh, expected)
    }

    fn release_posix_locks(&self, path: &str, handle: u64, owner: u64) -> Result<(), i32> {
        PassthroughFs::release_posix_locks(self, path, handle, owner)
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
        PassthroughFs::getlk(self, path, handle, owner, start, end, typ)
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
        PassthroughFs::setlk(self, path, handle, owner, start, end, typ, pid)
    }

    fn flock(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        operation: i32,
        pid: u32,
    ) -> Result<(), i32> {
        PassthroughFs::flock(self, path, handle, owner, operation, pid)
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
        PassthroughFs::copy_file_range(self, from, to, handles, offset_in, offset_out, len, ctx)
    }

    fn fallocate(
        &self,
        path: &str,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32> {
        PassthroughFs::fallocate(self, path, handle, offset, length, mode)
    }

    fn lseek(&self, path: &str, handle: u64, offset: i64, whence: i32) -> Result<i64, i32> {
        PassthroughFs::lseek(self, path, handle, offset, whence)
    }
}
