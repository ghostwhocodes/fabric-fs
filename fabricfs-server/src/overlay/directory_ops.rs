use std::collections::HashSet;
use std::fs;

use crate::server::{append_rel, io_errno, metadata_to_stat, normalize_path, Stat};

use super::OverlayFs;

impl OverlayFs {
    pub(super) fn list_dir(&self, path: &str) -> Result<Vec<(String, Stat)>, i32> {
        let rel = normalize_path(path)?;
        let view = self.resolve_dir(&rel)?;
        let mut entries = Vec::new();
        let mut seen = HashSet::new();

        if let Some(overlay_dir) = view.overlay {
            for entry in fs::read_dir(&overlay_dir).map_err(io_errno)? {
                let entry = entry.map_err(io_errno)?;
                let name = entry.file_name().to_string_lossy().to_string();
                let child = append_rel(&rel, &name);
                if self.is_tombstoned(&child) {
                    continue;
                }
                let meta = fs::symlink_metadata(entry.path()).map_err(io_errno)?;
                entries.push((name.clone(), metadata_to_stat(&meta)));
                seen.insert(name);
            }
        }

        if let Some(backing_dir) = view.backing {
            for entry in fs::read_dir(&backing_dir).map_err(io_errno)? {
                let entry = entry.map_err(io_errno)?;
                let name = entry.file_name().to_string_lossy().to_string();
                if seen.contains(&name) {
                    continue;
                }
                let child = append_rel(&rel, &name);
                if self.is_tombstoned(&child) {
                    continue;
                }
                let meta = fs::symlink_metadata(entry.path()).map_err(io_errno)?;
                entries.push((name, metadata_to_stat(&meta)));
            }
        }

        Ok(entries)
    }
}
