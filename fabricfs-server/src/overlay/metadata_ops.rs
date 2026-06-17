use std::fs;
use std::fs::OpenOptions;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;

use crate::server::{
    ensure_not_symlink, ensure_setattr_allowed, errno, io_errno, metadata_to_stat, normalize_path,
    path_to_cstring, require_context, FuseContext, Stat, StatFs,
};

use super::OverlayFs;

impl OverlayFs {
    pub(super) fn readlink(&self, path: &str) -> Result<Vec<u8>, i32> {
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        let link = fs::read_link(&resolved).map_err(io_errno)?;
        Ok(link.into_os_string().into_vec())
    }

    pub(super) fn stat(&self, path: &str) -> Result<Stat, i32> {
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;
        let meta = fs::symlink_metadata(&resolved).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
    }

    pub(super) fn setattr(
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
        let current = self.resolve_existing(&rel)?;
        ensure_not_symlink(&current)?;
        let current_meta = fs::metadata(&current).map_err(io_errno)?;
        ensure_setattr_allowed(&current_meta, ctx.uid, ctx.gid, mode, uid, gid, size)?;

        let target = self.materialize_for_write(&rel)?;

        if (mode.is_some() || uid.is_some() || gid.is_some())
            && self.permission_updates_blocked(&target)
        {
            return Err(libc::EACCES);
        }

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

        let meta = fs::symlink_metadata(&target).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
    }

    pub(super) fn statfs(&self) -> Result<StatFs, i32> {
        // Use COW root if available, otherwise backing root
        let root = self
            .state
            .layout
            .cow
            .as_ref()
            .or(self.state.layout.backing.as_ref())
            .ok_or(libc::ENOENT)?;

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let cstr = path_to_cstring(root)?;
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
}
