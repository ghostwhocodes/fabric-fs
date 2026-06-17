use std::fs;
use std::fs::File;

use crate::watch::InternalMetadataWrite;

use super::fs_ops::create_dir_all_with_recorded_dirs;
use super::OverlayFs;

pub(super) const TOMBSTONE_DIR: &str = ".fabricfs_tombstones";
pub(super) const TOMBSTONE_MARKER: &str = ".fabricfs_tombstone";

impl OverlayFs {
    pub(super) fn is_tombstoned(&self, rel: &str) -> bool {
        self.tombstone_path(rel)
            .map(|path| match fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_dir() => path.join(TOMBSTONE_MARKER).is_file(),
                Ok(_) => true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
                Err(_) => true,
            })
            .unwrap_or(false)
    }

    pub(super) fn clear_tombstone(&self, rel: &str) -> std::io::Result<()> {
        if let Some(path) = self.tombstone_path(rel) {
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_dir() => {
                    let marker = path.join(TOMBSTONE_MARKER);
                    if marker.exists() {
                        fs::remove_file(&marker)?;
                        self.record_completed_internal_metadata_write(
                            InternalMetadataWrite::remove_file(marker),
                        );
                    }
                }
                Ok(_) => {
                    fs::remove_file(&path)?;
                    self.record_completed_internal_metadata_write(
                        InternalMetadataWrite::remove_file(path),
                    );
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub(super) fn mark_tombstone(&self, rel: &str) -> std::io::Result<()> {
        if let Some(path) = self.tombstone_path(rel) {
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_dir() => {
                    let marker = path.join(TOMBSTONE_MARKER);
                    let _ = File::create(&marker)?;
                    self.record_completed_internal_metadata_write(
                        InternalMetadataWrite::write_file(marker, Vec::new()),
                    );
                }
                Ok(_) => {
                    let _ = File::create(&path)?;
                    self.record_completed_internal_metadata_write(
                        InternalMetadataWrite::write_file(path, Vec::new()),
                    );
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    let mut created_dirs = Vec::new();
                    if let Some(parent) = path.parent() {
                        created_dirs = create_dir_all_with_recorded_dirs(parent)?;
                    }
                    let _ = File::create(&path)?;
                    self.record_completed_internal_metadata_write(
                        InternalMetadataWrite::write_file(path, Vec::new())
                            .with_created_dirs(created_dirs),
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }
}
