use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::runtime_state::lock_or_errno;
use crate::server::{
    apply_handle_setattr, apply_mode_zero_fallocate, copy_file_range_at, descendant_suffix,
    ensure_fallocate_mode_supported, ensure_open_flags_allowed, ensure_regular_file,
    ensure_search_allowed, io_errno, lock_key_for_file, lock_required_access, metadata_to_stat,
    read_file_at, seek_offset, write_file_at, CopyFileRangeHandles, FileLock, FlockTable, FsLimits,
    FuseContext, HandleKind, LockTable, Stat,
};

const MIN_HANDLE_ID: u64 = 1;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedStorageObject {
    virtual_path: String,
    host_path: PathBuf,
    permission_updates_blocked: bool,
}

impl ResolvedStorageObject {
    pub(crate) fn new(virtual_path: impl Into<String>, host_path: impl Into<PathBuf>) -> Self {
        Self {
            virtual_path: virtual_path.into(),
            host_path: host_path.into(),
            permission_updates_blocked: false,
        }
    }

    pub(crate) fn with_permission_updates_blocked(mut self, blocked: bool) -> Self {
        self.permission_updates_blocked = blocked;
        self
    }

    pub(crate) fn virtual_path(&self) -> &str {
        &self.virtual_path
    }

    pub(crate) fn host_path(&self) -> &Path {
        &self.host_path
    }
}

#[derive(Debug)]
pub(crate) struct StorageRuntime {
    limits: FsLimits,
    handles: Mutex<HandleState>,
    locks: Mutex<LockTable>,
    flocks: Mutex<FlockTable>,
}

#[derive(Debug)]
struct HandleEntry {
    path: String,
    kind: HandleKind,
    access_bits: u32,
    offset: u64,
    permission_updates_blocked: bool,
    file: Option<File>,
}

#[derive(Default, Debug)]
struct HandleState {
    next: u64,
    entries: HashMap<u64, HandleEntry>,
}

impl StorageRuntime {
    pub(crate) fn new(limits: FsLimits) -> Self {
        Self {
            limits: limits.validated(),
            handles: Mutex::new(HandleState::default()),
            locks: Mutex::new(LockTable::default()),
            flocks: Mutex::new(FlockTable::default()),
        }
    }

    pub(crate) fn read_path(
        &self,
        object: &ResolvedStorageObject,
        offset: i64,
        size: i64,
        ctx: FuseContext,
    ) -> Result<Vec<u8>, i32> {
        let mut file = File::open(object.host_path()).map_err(io_errno)?;
        let meta = file.metadata().map_err(io_errno)?;
        crate::server::ensure_read_allowed(&meta, ctx.uid, ctx.gid)?;
        read_file_at(&mut file, offset, size, &self.limits)
    }

    pub(crate) fn write_path(
        &self,
        object: &ResolvedStorageObject,
        offset: i64,
        data: &[u8],
        ctx: FuseContext,
    ) -> Result<usize, i32> {
        let mut file = OpenOptions::new()
            .write(true)
            .open(object.host_path())
            .map_err(io_errno)?;
        let meta = file.metadata().map_err(io_errno)?;
        crate::server::ensure_write_allowed(&meta, ctx.uid, ctx.gid)?;
        write_file_at(&mut file, offset, data)
    }

    pub(crate) fn open_existing(
        &self,
        object: &ResolvedStorageObject,
        kind: HandleKind,
        flags: i32,
        ctx: FuseContext,
    ) -> Result<(u64, Stat), i32> {
        let link_meta = fs::symlink_metadata(object.host_path()).map_err(io_errno)?;
        let meta = if link_meta.file_type().is_symlink() {
            fs::metadata(object.host_path()).map_err(io_errno)?
        } else {
            link_meta
        };

        match (kind, meta.is_dir()) {
            (HandleKind::Dir, false) => return Err(libc::ENOTDIR),
            (HandleKind::File, true) => return Err(libc::EISDIR),
            _ => {}
        }

        match kind {
            HandleKind::File => {
                ensure_regular_file(&meta)?;
                let access_bits = ensure_open_flags_allowed(&meta, ctx.uid, ctx.gid, flags)?;
                let mut opts = OpenOptions::new();
                opts.read(access_bits & 0o4 != 0)
                    .write(access_bits & 0o2 != 0);
                let file = opts.open(object.host_path()).map_err(io_errno)?;
                if flags & libc::O_TRUNC != 0 {
                    file.set_len(0).map_err(io_errno)?;
                }
                let stat = metadata_to_stat(&file.metadata().map_err(io_errno)?);
                let fh = self.allocate_handle(
                    object.virtual_path(),
                    kind,
                    access_bits,
                    object.permission_updates_blocked,
                    Some(file),
                )?;
                Ok((fh, stat))
            }
            HandleKind::Dir => {
                ensure_search_allowed(&meta, ctx.uid, ctx.gid)?;
                let file = File::open(object.host_path()).map_err(io_errno)?;
                let stat = metadata_to_stat(&meta);
                let fh = self.allocate_handle(
                    object.virtual_path(),
                    kind,
                    0,
                    object.permission_updates_blocked,
                    Some(file),
                )?;
                Ok((fh, stat))
            }
        }
    }

    pub(crate) fn register_file_handle(
        &self,
        object: &ResolvedStorageObject,
        access_bits: u32,
        file: File,
    ) -> Result<(u64, Stat), i32> {
        let stat = metadata_to_stat(&file.metadata().map_err(io_errno)?);
        let fh = self.allocate_handle(
            object.virtual_path(),
            HandleKind::File,
            access_bits,
            object.permission_updates_blocked,
            Some(file),
        )?;
        Ok((fh, stat))
    }

    pub(crate) fn read_fh(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        offset: i64,
        size: i64,
    ) -> Result<Vec<u8>, i32> {
        let mut handles = self.lock_handles()?;
        let handle = handles.entries.get_mut(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, object.virtual_path(), HandleKind::File)?;
        if handle.access_bits & 0o4 == 0 {
            return Err(libc::EBADF);
        }
        let file = handle.file.as_mut().ok_or(libc::EBADF)?;
        read_file_at(file, offset, size, &self.limits)
    }

    pub(crate) fn write_fh(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<usize, i32> {
        let mut handles = self.lock_handles()?;
        let handle = handles.entries.get_mut(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, object.virtual_path(), HandleKind::File)?;
        if handle.access_bits & 0o2 == 0 {
            return Err(libc::EBADF);
        }
        let file = handle.file.as_mut().ok_or(libc::EBADF)?;
        write_file_at(file, offset, data)
    }

    pub(crate) fn setattr_fh(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: FuseContext,
    ) -> Result<Stat, i32> {
        let required_access = if size.is_some() { 0o2 } else { 0 };
        let (file, access_bits, permission_updates_blocked) =
            self.clone_file_handle_with_policy(object, handle, required_access)?;
        if (mode.is_some() || uid.is_some() || gid.is_some()) && permission_updates_blocked {
            return Err(libc::EACCES);
        }
        apply_handle_setattr(&file, access_bits, mode, uid, gid, size, ctx)
    }

    pub(crate) fn sync_file_fh(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        datasync: bool,
    ) -> Result<(), i32> {
        let handles = self.lock_handles()?;
        Self::sync_handle_from_state(
            &handles,
            object.virtual_path(),
            handle,
            HandleKind::File,
            datasync,
        )
    }

    pub(crate) fn sync_dir_fh(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        datasync: bool,
    ) -> Result<(), i32> {
        let handles = self.lock_handles()?;
        Self::sync_handle_from_state(
            &handles,
            object.virtual_path(),
            handle,
            HandleKind::Dir,
            datasync,
        )
    }

    pub(crate) fn check_handle(
        &self,
        object: &ResolvedStorageObject,
        handle: Option<u64>,
        expected: HandleKind,
    ) -> Result<(), i32> {
        let handle = handle.ok_or(libc::EINVAL)?;
        let handles = self.lock_handles()?;
        let handle = handles.entries.get(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, object.virtual_path(), expected)
    }

    pub(crate) fn release_handle(&self, handle: u64) {
        if let Ok(mut handles) = self.handles.lock() {
            handles.entries.remove(&handle);
        }
        if let Ok(mut locks) = self.locks.lock() {
            locks.release_handle(handle);
        }
        if let Ok(mut flocks) = self.flocks.lock() {
            flocks.release_handle(handle);
        }
    }

    pub(crate) fn rewrite_paths(&self, old_path: &str, new_path: &str) -> Result<(), i32> {
        {
            let mut handles = self.lock_handles()?;
            for entry in handles.entries.values_mut() {
                if entry.path == old_path {
                    entry.path = new_path.to_string();
                } else if let Some(suffix) = descendant_suffix(&entry.path, old_path) {
                    entry.path = format!("{new_path}{suffix}");
                }
            }
        }
        self.lock_locks()?.rename_path(old_path, new_path);
        self.lock_flocks()?.rename_path(old_path, new_path);
        Ok(())
    }

    pub(crate) fn release_posix_locks(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        owner: u64,
    ) -> Result<(), i32> {
        let file = self.clone_file_handle(object, handle, 0)?;
        let lock_key = lock_key_for_file(&file)?;
        self.lock_locks()?.release_owner(&lock_key, owner);
        Ok(())
    }

    pub(crate) fn getlk(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32> {
        let file = self.clone_file_handle(object, handle, lock_required_access(typ)?)?;
        let lock_key = lock_key_for_file(&file)?;
        self.lock_locks()?.getlk(&lock_key, owner, start, end, typ)
    }

    pub(crate) fn setlk(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let file = self.clone_file_handle(object, handle, lock_required_access(typ)?)?;
        let lock_key = lock_key_for_file(&file)?;
        self.lock_locks()?
            .setlk(lock_key, handle, owner, start, end, typ, pid)
    }

    pub(crate) fn flock(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        owner: u64,
        operation: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let file = self.clone_file_handle(object, handle, 0)?;
        let lock_key = lock_key_for_file(&file)?;
        self.lock_flocks()?
            .flock(lock_key, handle, owner, operation, pid)
    }

    pub(crate) fn copy_file_range(
        &self,
        input: &ResolvedStorageObject,
        output: &ResolvedStorageObject,
        handles: CopyFileRangeHandles,
        offset_in: i64,
        offset_out: i64,
        len: u64,
    ) -> Result<u64, i32> {
        if offset_in < 0 || offset_out < 0 {
            return Err(libc::EINVAL);
        }
        let handle_state = self.lock_handles()?;
        let src_file = Self::clone_file_handle_from_state(
            &handle_state,
            input.virtual_path(),
            handles.fh_in,
            0o4,
        )?;
        let dst_file = Self::clone_file_handle_from_state(
            &handle_state,
            output.virtual_path(),
            handles.fh_out,
            0o2,
        )?;
        drop(handle_state);
        copy_file_range_at(
            &src_file,
            &dst_file,
            offset_in,
            offset_out,
            len,
            &self.limits,
        )
    }

    pub(crate) fn fallocate(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32> {
        ensure_fallocate_mode_supported(mode)?;
        let file = self.clone_file_handle(object, handle, 0o2)?;
        apply_mode_zero_fallocate(&file, offset, length)
    }

    pub(crate) fn lseek(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        offset: i64,
        whence: i32,
    ) -> Result<i64, i32> {
        let mut handles = self.lock_handles()?;
        let handle = handles.entries.get_mut(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, object.virtual_path(), HandleKind::File)?;
        let file = handle.file.as_ref().ok_or(libc::EBADF)?;
        let file_len = file.metadata().map_err(io_errno)?.len();
        let new_offset = seek_offset(handle.offset, file_len, offset, whence)?;
        handle.offset = new_offset;
        Ok(new_offset as i64)
    }

    fn allocate_handle(
        &self,
        path: &str,
        kind: HandleKind,
        access_bits: u32,
        permission_updates_blocked: bool,
        file: Option<File>,
    ) -> Result<u64, i32> {
        let mut handles = self.lock_handles()?;
        let fh = handles.next.max(MIN_HANDLE_ID);
        handles.next = fh.saturating_add(1);
        handles.entries.insert(
            fh,
            HandleEntry {
                path: path.to_string(),
                kind,
                access_bits,
                offset: 0,
                permission_updates_blocked,
                file,
            },
        );
        Ok(fh)
    }

    fn clone_file_handle(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        required_access: u32,
    ) -> Result<File, i32> {
        self.clone_file_handle_with_policy(object, handle, required_access)
            .map(|value| value.0)
    }

    fn clone_file_handle_with_policy(
        &self,
        object: &ResolvedStorageObject,
        handle: u64,
        required_access: u32,
    ) -> Result<(File, u32, bool), i32> {
        let handles = self.lock_handles()?;
        Self::clone_file_handle_with_policy_from_state(
            &handles,
            object.virtual_path(),
            handle,
            required_access,
        )
    }

    fn clone_file_handle_from_state(
        handles: &HandleState,
        path: &str,
        handle: u64,
        required_access: u32,
    ) -> Result<File, i32> {
        Self::clone_file_handle_with_policy_from_state(handles, path, handle, required_access)
            .map(|value| value.0)
    }

    fn clone_file_handle_with_policy_from_state(
        handles: &HandleState,
        path: &str,
        handle: u64,
        required_access: u32,
    ) -> Result<(File, u32, bool), i32> {
        let handle = handles.entries.get(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, path, HandleKind::File)?;
        if handle.access_bits & required_access != required_access {
            return Err(libc::EBADF);
        }
        handle
            .file
            .as_ref()
            .ok_or(libc::EBADF)?
            .try_clone()
            .map(|file| (file, handle.access_bits, handle.permission_updates_blocked))
            .map_err(io_errno)
    }

    fn sync_handle_from_state(
        handles: &HandleState,
        path: &str,
        handle: u64,
        expected: HandleKind,
        datasync: bool,
    ) -> Result<(), i32> {
        let handle = handles.entries.get(&handle).ok_or(libc::EBADF)?;
        Self::ensure_handle_matches(handle, path, expected)?;
        let file = handle.file.as_ref().ok_or(libc::EBADF)?;
        if datasync {
            file.sync_data().map_err(io_errno)
        } else {
            file.sync_all().map_err(io_errno)
        }
    }

    fn ensure_handle_matches(
        handle: &HandleEntry,
        path: &str,
        expected: HandleKind,
    ) -> Result<(), i32> {
        if handle.path != path || handle.kind != expected {
            Err(libc::EBADF)
        } else {
            Ok(())
        }
    }

    fn lock_handles(&self) -> Result<MutexGuard<'_, HandleState>, i32> {
        lock_or_errno(&self.handles, "storage_runtime.handles", |handles| {
            handles.entries.clear();
            handles.next = MIN_HANDLE_ID;
        })
    }

    fn lock_locks(&self) -> Result<MutexGuard<'_, LockTable>, i32> {
        lock_or_errno(&self.locks, "storage_runtime.locks", |locks| {
            *locks = LockTable::default();
        })
    }

    fn lock_flocks(&self) -> Result<MutexGuard<'_, FlockTable>, i32> {
        lock_or_errno(&self.flocks, "storage_runtime.flocks", |flocks| {
            *flocks = FlockTable::default();
        })
    }
}
