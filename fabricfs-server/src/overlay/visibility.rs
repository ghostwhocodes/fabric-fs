use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use crate::server::{append_rel, io_errno};

use super::fs_ops::node_exists;
use super::OverlayFs;

pub(super) struct DirView {
    pub(super) overlay: Option<PathBuf>,
    pub(super) backing: Option<PathBuf>,
}

impl OverlayFs {
    pub(super) fn dir_view_path(path: PathBuf) -> Result<(PathBuf, bool), i32> {
        let link_meta = fs::symlink_metadata(&path).map_err(io_errno)?;
        let is_symlink = link_meta.file_type().is_symlink();
        let meta = if is_symlink {
            fs::metadata(&path).map_err(io_errno)?
        } else {
            link_meta
        };
        if !meta.is_dir() {
            return Err(libc::ENOTDIR);
        }
        if is_symlink {
            Ok((fs::canonicalize(&path).map_err(io_errno)?, true))
        } else {
            Ok((path, false))
        }
    }

    pub(super) fn resolve_existing(&self, rel: &str) -> Result<PathBuf, i32> {
        if self.is_tombstoned(rel) {
            return Err(libc::ENOENT);
        }
        if let Some(path) = self.overlay_path(rel) {
            if node_exists(&path) {
                return Ok(path);
            }
        }
        if let Some(path) = self.backing_path(rel) {
            if node_exists(&path) {
                return Ok(path);
            }
        }
        Err(libc::ENOENT)
    }

    pub(super) fn ensure_union_entry_absent(&self, rel: &str) -> Result<(), i32> {
        match self.resolve_existing(rel) {
            Ok(_) => Err(libc::EEXIST),
            Err(libc::ENOENT) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub(super) fn resolve_dir(&self, rel: &str) -> Result<DirView, i32> {
        if self.is_tombstoned(rel) {
            return Err(libc::ENOENT);
        }

        let mut overlay = None;
        if let Some(p) = self.overlay_path(rel) {
            if node_exists(&p) {
                let (path, is_symlink) = Self::dir_view_path(p)?;
                if is_symlink {
                    return Ok(DirView {
                        overlay: Some(path),
                        backing: None,
                    });
                }
                overlay = Some(path);
            }
        }

        let mut backing = None;
        if let Some(p) = self.backing_path(rel) {
            if node_exists(&p) {
                let (path, _) = Self::dir_view_path(p)?;
                backing = Some(path);
            }
        }

        if overlay.is_none() && backing.is_none() {
            return Err(libc::ENOENT);
        }

        Ok(DirView { overlay, backing })
    }

    pub(super) fn dir_is_empty(&self, rel: &str) -> Result<bool, i32> {
        let view = self.resolve_dir(rel)?;
        let mut seen = HashSet::new();

        if let Some(dir) = view.overlay {
            for entry in fs::read_dir(&dir).map_err(io_errno)? {
                let entry = entry.map_err(io_errno)?;
                let name = entry.file_name().to_string_lossy().to_string();
                let child = append_rel(rel, &name);
                if self.is_tombstoned(&child) {
                    continue;
                }
                seen.insert(name);
                if !seen.is_empty() {
                    return Ok(false);
                }
            }
        }

        if let Some(dir) = view.backing {
            for entry in fs::read_dir(&dir).map_err(io_errno)? {
                let entry = entry.map_err(io_errno)?;
                let name = entry.file_name().to_string_lossy().to_string();
                if seen.contains(&name) {
                    continue;
                }
                let child = append_rel(rel, &name);
                if self.is_tombstoned(&child) {
                    continue;
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub(super) fn overlay_path(&self, rel: &str) -> Option<PathBuf> {
        self.state.layout.overlay_path(rel)
    }

    pub(super) fn backing_path(&self, rel: &str) -> Option<PathBuf> {
        self.state.layout.backing_path(rel)
    }

    pub(super) fn tombstone_path(&self, rel: &str) -> Option<PathBuf> {
        self.state.layout.tombstone_path(rel)
    }
}
