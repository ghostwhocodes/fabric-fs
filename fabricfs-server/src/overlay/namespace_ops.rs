use std::ffi::OsStr;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use crate::server::{
    apply_ownership, dir_is_empty, ensure_dir_creation_allowed, ensure_file_creation_allowed,
    ensure_not_symlink, ensure_open_flags_allowed, ensure_parent_search_allowed,
    ensure_regular_file, ensure_removal_allowed, ensure_search_allowed, ensure_sticky_allowed,
    errno, io_errno, metadata_to_stat, normalize_path, open_access_bits, parent_rel,
    path_to_cstring, require_context, strip_root, FuseContext, Stat,
};

use super::fs_ops::node_exists;
use super::OverlayFs;

impl OverlayFs {
    pub(super) fn mkdir(&self, path: &str, mode: u32, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let parent_rel = parent_rel(&rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
        ensure_dir_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        self.ensure_union_entry_absent(&rel)?;

        let target = self.target_file_for_write(&rel)?;
        self.clear_tombstone(&rel).map_err(io_errno)?;

        fs::create_dir(&target).map_err(io_errno)?;
        let masked = self.apply_umask(mode);
        let perm = PermissionsExt::from_mode(libc::S_IFDIR | masked);
        fs::set_permissions(&target, perm).map_err(io_errno)?;
        apply_ownership(&target, ctx.uid, ctx.gid, true)?;
        Ok(())
    }

    pub(super) fn unlink(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let resolved = self.resolve_existing(&rel)?;
        let meta = fs::symlink_metadata(&resolved).map_err(io_errno)?;
        if meta.is_dir() {
            return Err(libc::EISDIR);
        }

        let parent_rel = parent_rel(&rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
        ensure_removal_allowed(&parent_meta, &meta, ctx.uid, ctx.gid)?;
        ensure_sticky_allowed(&parent_meta, &meta, ctx.uid)?;

        if self.state.layout.has_cow() {
            if let Some(overlay_path) = self.overlay_path(&rel) {
                if node_exists(&overlay_path) {
                    fs::remove_file(&overlay_path).map_err(io_errno)?;
                    self.state.xattrs.purge_if_last_link(&meta)?;
                }
            }
            self.mark_tombstone(&rel).map_err(io_errno)?;
        } else {
            fs::remove_file(&resolved).map_err(io_errno)?;
            self.state.xattrs.purge_if_last_link(&meta)?;
        }

        Ok(())
    }

    pub(super) fn rmdir(&self, path: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        if !self.dir_is_empty(&rel)? {
            return Err(libc::ENOTEMPTY);
        }

        let parent_rel = parent_rel(&rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;

        let resolved = self.resolve_existing(&rel)?;
        let meta = fs::symlink_metadata(&resolved).map_err(io_errno)?;
        ensure_removal_allowed(&parent_meta, &meta, ctx.uid, ctx.gid)?;
        ensure_sticky_allowed(&parent_meta, &meta, ctx.uid)?;

        if self.state.layout.has_cow() {
            if let Some(overlay_path) = self.overlay_path(&rel) {
                if node_exists(&overlay_path) {
                    fs::remove_dir(&overlay_path).map_err(io_errno)?;
                }
            }
            self.mark_tombstone(&rel).map_err(io_errno)?;
        } else {
            fs::remove_dir(&resolved).map_err(io_errno)?;
        }

        Ok(())
    }

    pub(super) fn rename(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let from_rel = normalize_path(from)?;
        let to_rel = normalize_path(to)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let src = self.resolve_existing(&from_rel)?;
        let src_meta = fs::symlink_metadata(&src).map_err(io_errno)?;

        let from_parent_rel = parent_rel(&from_rel);
        let from_parent_view = self.resolve_dir(&from_parent_rel)?;
        let from_parent_path = from_parent_view
            .overlay
            .as_ref()
            .or(from_parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let from_parent_meta = fs::symlink_metadata(from_parent_path).map_err(io_errno)?;
        ensure_removal_allowed(&from_parent_meta, &src_meta, ctx.uid, ctx.gid)?;

        let to_parent_rel = parent_rel(&to_rel);
        let to_parent_view = self.resolve_dir(&to_parent_rel)?;
        let to_parent_path = to_parent_view
            .overlay
            .as_ref()
            .or(to_parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let to_parent_meta = fs::symlink_metadata(to_parent_path).map_err(io_errno)?;
        ensure_dir_creation_allowed(&to_parent_meta, ctx.uid, ctx.gid)?;

        let dest_meta = self
            .resolve_existing(&to_rel)
            .ok()
            .and_then(|p| fs::symlink_metadata(p).ok());
        let dest_meta = dest_meta.as_ref().unwrap_or(&src_meta);
        ensure_sticky_allowed(&to_parent_meta, dest_meta, ctx.uid)?;

        if self.state.layout.has_cow() {
            self.copy_up_tree(&from_rel)?;
            let src = self.overlay_path(&from_rel).ok_or(libc::EIO)?;
            if !node_exists(&src) {
                return Err(libc::ENOENT);
            }
            let dst = self.overlay_path(&to_rel).ok_or(libc::EIO)?;
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(io_errno)?;
            }
            if node_exists(&dst) {
                let meta = fs::symlink_metadata(&dst).map_err(io_errno)?;
                if meta.is_dir() && !dir_is_empty(&dst)? {
                    return Err(libc::ENOTEMPTY);
                }
                if meta.is_dir() {
                    fs::remove_dir(&dst).map_err(io_errno)?;
                } else {
                    fs::remove_file(&dst).map_err(io_errno)?;
                }
                self.state.xattrs.purge_if_last_link(&meta)?;
            }
            self.clear_tombstone(&to_rel).map_err(io_errno)?;
            fs::rename(&src, &dst).map_err(io_errno)?;
            self.mark_tombstone(&from_rel).map_err(io_errno)?;
        } else {
            let backing = self.state.layout.backing.clone().ok_or(libc::EROFS)?;
            let src = backing.join(strip_root(&from_rel));
            if !node_exists(&src) {
                return Err(libc::ENOENT);
            }
            let dst = backing.join(strip_root(&to_rel));
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(io_errno)?;
            }
            if node_exists(&dst) {
                let meta = fs::symlink_metadata(&dst).map_err(io_errno)?;
                if meta.is_dir() && !dir_is_empty(&dst)? {
                    return Err(libc::ENOTEMPTY);
                }
                if meta.is_dir() {
                    fs::remove_dir(&dst).map_err(io_errno)?;
                } else {
                    fs::remove_file(&dst).map_err(io_errno)?;
                }
                self.state.xattrs.purge_if_last_link(&meta)?;
            }
            fs::rename(&src, &dst).map_err(io_errno)?;
        }

        self.state.runtime.rewrite_paths(&from_rel, &to_rel)?;
        Ok(())
    }

    pub(super) fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
        ctx: Option<FuseContext>,
    ) -> Result<(u64, Stat), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let truncate = flags & libc::O_TRUNC != 0;
        let access_bits = open_access_bits(flags)?;
        let existing = self.resolve_existing(&rel).ok();
        let mut checked_existing = false;
        if let Some(path) = existing.as_ref() {
            let parent = path.parent().ok_or(libc::ENOENT)?;
            let parent_meta = fs::metadata(parent).map_err(io_errno)?;
            ensure_search_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        } else {
            let parent_rel = parent_rel(&rel);
            let parent_view = self.resolve_dir(&parent_rel)?;
            let parent_path = parent_view
                .overlay
                .as_ref()
                .or(parent_view.backing.as_ref())
                .ok_or(libc::ENOENT)?;
            let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
            ensure_dir_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        }
        if flags & libc::O_EXCL != 0 && existing.is_some() {
            return Err(libc::EEXIST);
        }
        if let Some(path) = existing.as_ref() {
            ensure_not_symlink(path)?;
            let meta = fs::metadata(path).map_err(io_errno)?;
            ensure_regular_file(&meta)?;
            ensure_open_flags_allowed(&meta, ctx.uid, ctx.gid, flags)?;
            checked_existing = true;
        }

        if existing.is_some() && self.state.layout.has_cow() {
            if truncate {
                self.copy_up_empty_file_for_truncate(&rel)?;
            } else {
                self.copy_up_file(&rel)?;
            }
        }

        let target = self.target_file_for_write(&rel)?;
        let target_meta = match fs::symlink_metadata(&target) {
            Ok(meta) => Some(meta),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(io_errno(err)),
        };
        if let Some(meta) = target_meta.as_ref() {
            if meta.file_type().is_symlink() {
                return Err(libc::ELOOP);
            }
            if !checked_existing {
                ensure_regular_file(meta)?;
                ensure_open_flags_allowed(meta, ctx.uid, ctx.gid, flags)?;
                checked_existing = true;
            }
        }
        self.clear_tombstone(&rel).map_err(io_errno)?;

        let existed = existing.is_some() || target_meta.is_some();
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

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

        let meta = file.metadata().map_err(io_errno)?;
        if existed {
            if !checked_existing {
                ensure_regular_file(&meta)?;
                ensure_open_flags_allowed(&meta, ctx.uid, ctx.gid, flags)?;
            }
            if truncate {
                file.set_len(0).map_err(io_errno)?;
            }
        }
        let permission_updates_blocked = self.permission_updates_blocked(&target);
        let (fh, stat) = self.state.runtime.register_file_handle(
            &self
                .resolved_object(&rel, target)
                .with_permission_updates_blocked(permission_updates_blocked),
            access_bits,
            file,
        )?;
        Ok((fh, stat))
    }

    pub(super) fn mknod(
        &self,
        path: &str,
        mode: u32,
        ctx: Option<FuseContext>,
    ) -> Result<Stat, i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let parent_rel = parent_rel(&rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        self.ensure_union_entry_absent(&rel)?;

        let target = self.target_file_for_write(&rel)?;
        self.clear_tombstone(&rel).map_err(io_errno)?;

        let cstr = path_to_cstring(&target)?;
        let masked = self.apply_umask(mode);
        let rc = unsafe { libc::mknod(cstr.as_ptr(), masked, 0) };
        if rc != 0 {
            return Err(errno());
        }
        let perm = PermissionsExt::from_mode(masked & 0o7777);
        fs::set_permissions(&target, perm).map_err(io_errno)?;
        apply_ownership(&target, ctx.uid, ctx.gid, true)?;

        let meta = fs::symlink_metadata(&target).map_err(io_errno)?;
        Ok(metadata_to_stat(&meta))
    }

    pub(super) fn exists(&self, path: &str) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        self.resolve_existing(&rel)?;
        Ok(())
    }

    pub(super) fn link(&self, from: &str, to: &str, ctx: Option<FuseContext>) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let from_rel = normalize_path(from)?;
        let to_rel = normalize_path(to)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let src_existing = self.resolve_existing(&from_rel)?;
        ensure_parent_search_allowed(&from_rel, |rel| {
            let view = self.resolve_dir(rel)?;
            if let Some(dir) = view.overlay.as_ref() {
                let meta = fs::metadata(dir).map_err(io_errno)?;
                ensure_search_allowed(&meta, ctx.uid, ctx.gid)?;
            }
            if let Some(dir) = view.backing.as_ref() {
                let meta = fs::metadata(dir).map_err(io_errno)?;
                ensure_search_allowed(&meta, ctx.uid, ctx.gid)?;
            }
            Ok(())
        })?;
        fs::symlink_metadata(&src_existing).map_err(io_errno)?;

        let parent_rel = parent_rel(&to_rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        self.ensure_union_entry_absent(&to_rel)?;

        if self.state.layout.has_cow() {
            self.copy_up_file(&from_rel)?;
            let src = self.overlay_path(&from_rel).ok_or(libc::EIO)?;
            let dst = self.overlay_path(&to_rel).ok_or(libc::EIO)?;
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(io_errno)?;
            }
            self.clear_tombstone(&to_rel).map_err(io_errno)?;
            fs::hard_link(&src, &dst).map_err(io_errno)?;
        } else {
            let src = self.resolve_existing(&from_rel)?;
            let dst = self.backing_path(&to_rel).ok_or(libc::EROFS)?;
            fs::hard_link(&src, &dst).map_err(io_errno)?;
        }
        Ok(())
    }

    pub(super) fn symlink(
        &self,
        path: &str,
        target: Vec<u8>,
        ctx: Option<FuseContext>,
    ) -> Result<(), i32> {
        let ctx = require_context(ctx)?;
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        let parent_rel = parent_rel(&rel);
        let parent_view = self.resolve_dir(&parent_rel)?;
        let parent_path = parent_view
            .overlay
            .as_ref()
            .or(parent_view.backing.as_ref())
            .ok_or(libc::ENOENT)?;
        let parent_meta = fs::symlink_metadata(parent_path).map_err(io_errno)?;
        ensure_file_creation_allowed(&parent_meta, ctx.uid, ctx.gid)?;
        self.ensure_union_entry_absent(&rel)?;

        let link_path = self.target_file_for_write(&rel)?;
        self.clear_tombstone(&rel).map_err(io_errno)?;

        if let Some(parent) = link_path.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        let target = Path::new(OsStr::from_bytes(&target));
        std::os::unix::fs::symlink(target, &link_path).map_err(io_errno)?;
        apply_ownership(&link_path, ctx.uid, ctx.gid, false)?;
        Ok(())
    }
}
