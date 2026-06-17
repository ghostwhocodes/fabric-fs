use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::server::{
    append_rel, copy_file_range_at, ensure_regular_file, errno, io_errno, normalize_path,
};

use super::fs_ops::{node_exists, preserve_ownership};
use super::OverlayFs;

impl OverlayFs {
    pub(super) fn target_file_for_write(&self, rel: &str) -> Result<PathBuf, i32> {
        if !self.state.layout.is_writable() {
            return Err(libc::EROFS);
        }
        self.ensure_backing_mutable()?;
        if let Some(cow) = self.overlay_path(rel) {
            return Ok(cow);
        }
        self.backing_path(rel).ok_or(libc::EROFS)
    }

    pub(super) fn prepare_file_for_write(&self, path: &str) -> Result<PathBuf, i32> {
        let rel = normalize_path(path)?;
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        if self.state.layout.has_cow() {
            self.copy_up_file(&rel)?;
            let overlay = self.overlay_path(&rel).ok_or(libc::EIO)?;
            if !node_exists(&overlay) {
                return Err(libc::ENOENT);
            }
            self.clear_tombstone(&rel).map_err(io_errno)?;
            return Ok(overlay);
        }

        self.backing_path(&rel).ok_or(libc::EROFS)
    }

    pub(super) fn materialize_for_write(&self, rel: &str) -> Result<PathBuf, i32> {
        self.state.layout.ensure_writable()?;
        self.ensure_backing_mutable()?;

        if self.state.layout.has_cow() {
            self.copy_up_tree(rel)?;
            let path = self.overlay_path(rel).ok_or(libc::EIO)?;
            self.clear_tombstone(rel).map_err(io_errno)?;
            return Ok(path);
        }

        self.backing_path(rel).ok_or(libc::EROFS)
    }

    pub(super) fn materialize_for_xattr(&self, rel: &str) -> Result<PathBuf, i32> {
        if self.state.layout.has_cow() {
            self.copy_up_node(rel)?;
            let path = self.overlay_path(rel).ok_or(libc::EIO)?;
            if !node_exists(&path) {
                return Err(libc::ENOENT);
            }
            return Ok(path);
        }
        self.resolve_existing(rel)
    }

    pub(super) fn copy_up_node(&self, rel: &str) -> Result<(), i32> {
        let Some(overlay) = self.overlay_path(rel) else {
            return Err(libc::EROFS);
        };
        if node_exists(&overlay) {
            return Ok(());
        }
        let Some(backing) = self.backing_path(rel) else {
            return Ok(());
        };
        if !node_exists(&backing) {
            return Ok(());
        }

        let meta = fs::symlink_metadata(&backing).map_err(io_errno)?;
        if let Some(parent) = overlay.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        if meta.file_type().is_symlink() {
            let target = fs::read_link(&backing).map_err(io_errno)?;
            std::os::unix::fs::symlink(target, &overlay).map_err(io_errno)?;
            preserve_ownership(&overlay, &meta)?;
            self.copy_acls(&backing, &overlay, false, false)?;
        } else if meta.is_dir() {
            fs::create_dir_all(&overlay).map_err(io_errno)?;
            preserve_ownership(&overlay, &meta)?;
            fs::set_permissions(&overlay, meta.permissions()).map_err(io_errno)?;
            self.copy_acls(&backing, &overlay, true, true)?;
        } else {
            // Use optimized copy with reflink and sparse file support
            self.copy_with_reflink_fallback(&backing, &overlay)?;
            preserve_ownership(&overlay, &meta)?;
            fs::set_permissions(&overlay, meta.permissions()).map_err(io_errno)?;
            self.copy_acls(&backing, &overlay, true, true)?;
        }

        let dest_meta = fs::symlink_metadata(&overlay).map_err(io_errno)?;
        self.state
            .xattrs
            .seed_from_host(&dest_meta, &backing, !meta.file_type().is_symlink())?;
        Ok(())
    }

    pub(super) fn copy_up_file(&self, rel: &str) -> Result<(), i32> {
        let Some(overlay) = self.overlay_path(rel) else {
            return Err(libc::EROFS);
        };
        match fs::symlink_metadata(&overlay) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(libc::ELOOP);
                }
                return Ok(());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_errno(err)),
        }
        let Some(backing) = self.backing_path(rel) else {
            return Ok(());
        };
        if !node_exists(&backing) {
            return Ok(());
        }

        let meta = fs::symlink_metadata(&backing).map_err(io_errno)?;
        if meta.is_dir() {
            return Err(libc::EISDIR);
        }

        if let Some(parent) = overlay.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        if meta.file_type().is_symlink() {
            let target = fs::read_link(&backing).map_err(io_errno)?;
            std::os::unix::fs::symlink(target, &overlay).map_err(io_errno)?;
            preserve_ownership(&overlay, &meta)?;
            self.copy_acls(&backing, &overlay, false, false)?;
        } else {
            // Use optimized copy with reflink and sparse file support
            self.copy_with_reflink_fallback(&backing, &overlay)?;
            preserve_ownership(&overlay, &meta)?;
            fs::set_permissions(&overlay, meta.permissions()).map_err(io_errno)?;
            self.copy_acls(&backing, &overlay, true, true)?;
        }

        let dest_meta = fs::symlink_metadata(&overlay).map_err(io_errno)?;
        self.state
            .xattrs
            .seed_from_host(&dest_meta, &backing, !meta.file_type().is_symlink())?;
        Ok(())
    }

    pub(super) fn copy_up_empty_file_for_truncate(&self, rel: &str) -> Result<(), i32> {
        let Some(overlay) = self.overlay_path(rel) else {
            return Err(libc::EROFS);
        };
        match fs::symlink_metadata(&overlay) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(libc::ELOOP);
                }
                ensure_regular_file(&meta)?;
                return Ok(());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_errno(err)),
        }

        let Some(backing) = self.backing_path(rel) else {
            return Ok(());
        };
        if !node_exists(&backing) {
            return Ok(());
        }

        let meta = fs::symlink_metadata(&backing).map_err(io_errno)?;
        if meta.file_type().is_symlink() {
            return Err(libc::ELOOP);
        }
        ensure_regular_file(&meta)?;

        if let Some(parent) = overlay.parent() {
            fs::create_dir_all(parent).map_err(io_errno)?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(meta.mode() & 0o7777)
            .open(&overlay)
            .map_err(io_errno)?;
        drop(file);
        preserve_ownership(&overlay, &meta)?;
        fs::set_permissions(&overlay, meta.permissions()).map_err(io_errno)?;
        self.copy_acls(&backing, &overlay, true, true)?;

        let dest_meta = fs::symlink_metadata(&overlay).map_err(io_errno)?;
        self.state
            .xattrs
            .seed_from_host(&dest_meta, &backing, true)?;
        Ok(())
    }

    pub(super) fn copy_up_tree(&self, rel: &str) -> Result<(), i32> {
        let Some(overlay_root) = self.overlay_path(rel) else {
            return Err(libc::EROFS);
        };
        if node_exists(&overlay_root) {
            return Ok(());
        }
        let Some(backing_root) = self.backing_path(rel) else {
            return Err(libc::ENOENT);
        };
        if !node_exists(&backing_root) {
            return Err(libc::ENOENT);
        }

        let meta = fs::symlink_metadata(&backing_root).map_err(io_errno)?;
        if meta.is_dir() {
            fs::create_dir_all(&overlay_root).map_err(io_errno)?;
            preserve_ownership(&overlay_root, &meta)?;
            fs::set_permissions(&overlay_root, meta.permissions()).map_err(io_errno)?;
            self.copy_acls(&backing_root, &overlay_root, true, true)?;

            let dest_meta = fs::symlink_metadata(&overlay_root).map_err(io_errno)?;
            self.state
                .xattrs
                .seed_from_host(&dest_meta, &backing_root, true)?;

            for entry in fs::read_dir(&backing_root).map_err(io_errno)? {
                let entry = entry.map_err(io_errno)?;
                let name = entry.file_name();
                let child_rel = append_rel(rel, &name.to_string_lossy());
                self.copy_up_tree(&child_rel)?;
            }
        } else if meta.file_type().is_symlink() {
            if let Some(parent) = overlay_root.parent() {
                fs::create_dir_all(parent).map_err(io_errno)?;
            }
            let target = fs::read_link(&backing_root).map_err(io_errno)?;
            std::os::unix::fs::symlink(target, &overlay_root).map_err(io_errno)?;
            preserve_ownership(&overlay_root, &meta)?;
            self.copy_acls(&backing_root, &overlay_root, false, false)?;

            let dest_meta = fs::symlink_metadata(&overlay_root).map_err(io_errno)?;
            self.state
                .xattrs
                .seed_from_host(&dest_meta, &backing_root, false)?;
        } else {
            if let Some(parent) = overlay_root.parent() {
                fs::create_dir_all(parent).map_err(io_errno)?;
            }
            // Use optimized copy with reflink and sparse file support
            self.copy_with_reflink_fallback(&backing_root, &overlay_root)?;
            preserve_ownership(&overlay_root, &meta)?;
            fs::set_permissions(&overlay_root, meta.permissions()).map_err(io_errno)?;
            self.copy_acls(&backing_root, &overlay_root, true, true)?;

            let dest_meta = fs::symlink_metadata(&overlay_root).map_err(io_errno)?;
            self.state
                .xattrs
                .seed_from_host(&dest_meta, &backing_root, true)?;
        }
        Ok(())
    }
    pub(super) fn is_sparse(&self, path: &Path) -> Result<bool, i32> {
        let meta = fs::metadata(path).map_err(io_errno)?;
        // File is sparse if allocated blocks * block_size < file_size
        // blocks is in 512-byte units
        let apparent_size = meta.size();
        let actual_size = meta.blocks() * 512;
        Ok(actual_size < apparent_size)
    }

    /// Copy file with reflink optimization, falling back to regular copy
    pub(super) fn copy_with_reflink_fallback(&self, src: &Path, dst: &Path) -> Result<(), i32> {
        if !self.state.enable_reflinks {
            return self.copy_file_fallback(src, dst);
        }

        // Try reflink copy using FICLONE ioctl
        match self.try_reflink_copy(src, dst) {
            Ok(()) => Ok(()),
            Err(_) => self.copy_file_fallback(src, dst),
        }
    }

    /// Attempt reflink copy using FICLONE ioctl
    pub(super) fn try_reflink_copy(&self, src: &Path, dst: &Path) -> Result<(), i32> {
        use std::os::unix::io::AsRawFd;

        let src_file = File::open(src).map_err(io_errno)?;
        let dst_file = File::create(dst).map_err(io_errno)?;

        // FICLONE ioctl constant (from linux/fs.h)
        const FICLONE: libc::c_ulong = 0x40049409;

        let result = unsafe {
            libc::ioctl(
                dst_file.as_raw_fd(),
                FICLONE as libc::c_ulong,
                src_file.as_raw_fd(),
            )
        };

        if result == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }

    /// Fallback copy implementation with sparse file preservation
    pub(super) fn copy_file_fallback(&self, src: &Path, dst: &Path) -> Result<(), i32> {
        // Check if we should preserve sparse files
        if self.state.preserve_sparse_files && self.is_sparse(src).unwrap_or(false) {
            return self.copy_sparse_file(src, dst);
        }

        // Regular copy using fs::copy (simple and reliable)
        fs::copy(src, dst).map_err(io_errno)?;
        Ok(())
    }

    /// Copy a sparse file while preserving holes
    pub(super) fn copy_sparse_file(&self, src: &Path, dst: &Path) -> Result<(), i32> {
        use std::os::unix::io::AsRawFd;

        let src_file = File::open(src).map_err(io_errno)?;
        let dst_file = File::create(dst).map_err(io_errno)?;

        let src_meta = src_file.metadata().map_err(io_errno)?;
        let file_size = src_meta.size();

        // Set destination file size
        dst_file.set_len(file_size).map_err(io_errno)?;

        let src_fd = src_file.as_raw_fd();

        // SEEK_DATA and SEEK_HOLE constants
        const SEEK_DATA: i32 = 3;
        const SEEK_HOLE: i32 = 4;

        let mut offset: i64 = 0;
        let file_size_i64 = file_size as i64;

        // Iterate through data regions
        while offset < file_size_i64 {
            // Find next data region
            let data_offset = unsafe { libc::lseek(src_fd, offset, SEEK_DATA) };

            if data_offset < 0 {
                // No more data regions
                break;
            }

            if data_offset >= file_size_i64 {
                break;
            }

            // Find end of data region (start of next hole)
            let hole_offset = unsafe { libc::lseek(src_fd, data_offset, SEEK_HOLE) };
            let end_offset = if hole_offset < 0 || hole_offset > file_size_i64 {
                file_size_i64
            } else {
                hole_offset
            };

            // Copy this data region
            let region_size = (end_offset - data_offset) as usize;
            self.copy_file_data_range(&src_file, &dst_file, data_offset as u64, region_size)?;

            offset = end_offset;
        }

        Ok(())
    }

    /// Copy a sparse data range without taking ownership of caller-owned files.
    pub(super) fn copy_file_data_range(
        &self,
        src_file: &File,
        dst_file: &File,
        offset: u64,
        length: usize,
    ) -> Result<(), i32> {
        let offset = i64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let length = length as u64;
        copy_file_range_at(
            src_file,
            dst_file,
            offset,
            offset,
            length,
            &self.state.limits,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::FsLimits;
    use std::os::unix::fs::FileExt;
    use std::os::unix::io::AsRawFd;
    use tempfile::tempdir;

    fn assert_fd_open(label: &str, file: &File) {
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        assert!(
            result >= 0,
            "{label} fd should remain open: {}",
            std::io::Error::last_os_error()
        );
    }

    #[test]
    fn sparse_data_copy_error_keeps_caller_owned_files_open() {
        let temp = tempdir().expect("tempdir");
        let src_path = temp.path().join("src.bin");
        let dst_path = temp.path().join("dst.bin");
        fs::write(&src_path, b"abcdefgh").expect("seed source");
        fs::write(&dst_path, b"........").expect("seed destination");
        let src_file = File::open(&src_path).expect("open source");
        let dst_file = File::open(&dst_path).expect("open destination read-only");
        let overlay = OverlayFs::new(
            None,
            None,
            None,
            FsLimits::new(4, 4),
            0,
            false,
            false,
            false,
            false,
            false,
            true,
        )
        .expect("overlay initializes");

        let err = overlay
            .copy_file_data_range(&src_file, &dst_file, 0, 8)
            .expect_err("read-only destination rejects sparse copy data write");

        assert_eq!(err, libc::EBADF);
        assert_fd_open("source", &src_file);
        assert_fd_open("destination", &dst_file);
        let mut buf = [0; 4];
        assert_eq!(
            src_file
                .read_at(&mut buf, 0)
                .expect("source remains readable"),
            4
        );
        assert_eq!(&buf, b"abcd");
        dst_file
            .metadata()
            .expect("destination file descriptor remains usable");
    }
}
