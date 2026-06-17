use std::fs;
use std::fs::File;
use std::path::PathBuf;

use crate::server::{
    ensure_not_symlink, ensure_open_flags_allowed, ensure_regular_file, ensure_search_allowed,
    io_errno, normalize_path, require_context, CopyFileRangeHandles, FileLock, FuseContext,
    HandleKind, Stat,
};

use super::OverlayFs;

impl OverlayFs {
    pub(super) fn read(
        &self,
        path: &str,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        self.state
            .runtime
            .read_path(&self.resolved_object(&rel, resolved), offset, size, ctx)
    }

    pub(super) fn read_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        size: i64,
        ctx: Option<FuseContext>,
    ) -> Result<Vec<u8>, i32> {
        let _ = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .read_fh(&self.resolved_object(&rel, resolved), fh, offset, size)
    }

    pub(super) fn write(
        &self,
        path: &str,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        ensure_not_symlink(&resolved)?;

        let target = self.prepare_file_for_write(path)?;

        self.state
            .runtime
            .write_path(&self.resolved_object(&rel, target), offset, data, ctx)
    }

    pub(super) fn write_fh(
        &self,
        path: &str,
        fh: u64,
        offset: i64,
        data: &[u8],
        ctx: Option<FuseContext>,
    ) -> Result<usize, i32> {
        let _ = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .write_fh(&self.resolved_object(&rel, resolved), fh, offset, data)
    }

    pub(super) fn sync_file_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .sync_file_fh(&self.resolved_object(&rel, resolved), fh, datasync)
    }

    pub(super) fn setattr_fh(
        &self,
        path: &str,
        handle: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;

        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.setattr_fh(
            &self.resolved_object(&rel, resolved),
            handle,
            mode,
            uid,
            gid,
            size,
            ctx,
        )
    }

    pub(super) fn open(
        &self,
        path: &str,
        kind: HandleKind,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        let link_meta = fs::symlink_metadata(&resolved).map_err(io_errno)?;
        let meta = if link_meta.file_type().is_symlink() {
            fs::metadata(&resolved).map_err(io_errno)?
        } else {
            link_meta
        };

        match (kind, meta.is_dir()) {
            (HandleKind::Dir, false) => return Err(libc::ENOTDIR),
            (HandleKind::File, true) => return Err(libc::EISDIR),
            _ => {}
        }

        let target = match kind {
            HandleKind::File => {
                ensure_regular_file(&meta)?;
                let access_bits = ensure_open_flags_allowed(&meta, ctx.uid, ctx.gid, flags)?;
                if access_bits & 0o2 != 0 {
                    ensure_not_symlink(&resolved)?;
                    if flags & libc::O_TRUNC != 0 && self.state.layout.has_cow() {
                        self.copy_up_empty_file_for_truncate(&rel)?;
                        self.clear_tombstone(&rel).map_err(io_errno)?;
                        self.overlay_path(&rel).ok_or(libc::EIO)?
                    } else {
                        self.prepare_file_for_write(path)?
                    }
                } else {
                    resolved
                }
            }
            HandleKind::Dir => {
                ensure_search_allowed(&meta, ctx.uid, ctx.gid)?;
                resolved
            }
        };

        self.state.runtime.open_existing(
            &self.runtime_object_for_target(&rel, target),
            kind,
            flags,
            ctx,
        )
    }

    pub(super) fn release_fh(&self, fh: u64) {
        self.state.runtime.release_handle(fh);
    }

    pub(super) fn sync_dir_fh(&self, path: &str, fh: u64, datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .sync_dir_fh(&self.resolved_object(&rel, resolved), fh, datasync)
    }

    pub(super) fn sync_file(&self, path: &str, _datasync: bool) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        let file = File::open(&resolved).map_err(io_errno)?;
        file.sync_all().map_err(io_errno)?;
        Ok(())
    }

    pub(super) fn check_handle(
        &self,
        path: &str,
        fh: Option<u64>,
        expected: HandleKind,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .check_handle(&self.resolved_object(&rel, resolved), fh, expected)
    }

    pub(super) fn release_posix_locks(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state
            .runtime
            .release_posix_locks(&self.resolved_object(&rel, resolved), handle, owner)
    }

    pub(super) fn getlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.getlk(
            &self.resolved_object(&rel, resolved),
            handle,
            owner,
            start,
            end,
            typ,
        )
    }

    pub(super) fn setlk(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.setlk(
            &self.resolved_object(&rel, resolved),
            handle,
            owner,
            start,
            end,
            typ,
            pid,
        )
    }

    pub(super) fn flock(
        &self,
        path: &str,
        handle: u64,
        owner: u64,
        operation: i32,
        pid: u32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.flock(
            &self.resolved_object(&rel, resolved),
            handle,
            owner,
            operation,
            pid,
        )
    }

    pub(super) fn copy_file_range(
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
        let from_resolved = self
            .resolve_existing(&from_rel)
            .unwrap_or_else(|_| PathBuf::new());
        let to_resolved = self
            .resolve_existing(&to_rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.copy_file_range(
            &self.resolved_object(&from_rel, from_resolved),
            &self.resolved_object(&to_rel, to_resolved),
            file_handles,
            offset_in,
            offset_out,
            len,
        )
    }

    pub(super) fn fallocate(
        &self,
        path: &str,
        handle: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.fallocate(
            &self.resolved_object(&rel, resolved),
            handle,
            offset,
            length,
            mode,
        )
    }

    pub(super) fn lseek(
        &self,
        path: &str,
        handle_id: u64,
        offset: i64,
        whence: i32,
    ) -> Result<i64, i32> {
        let rel = normalize_path(path)?;
        let resolved = self
            .resolve_existing(&rel)
            .unwrap_or_else(|_| PathBuf::new());
        self.state.runtime.lseek(
            &self.resolved_object(&rel, resolved),
            handle_id,
            offset,
            whence,
        )
    }
}
