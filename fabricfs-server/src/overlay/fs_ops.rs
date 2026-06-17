use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::server::apply_ownership;

pub(super) fn preserve_ownership(path: &Path, source_meta: &fs::Metadata) -> Result<(), i32> {
    let follow_symlink = !source_meta.file_type().is_symlink();
    match apply_ownership(path, source_meta.uid(), source_meta.gid(), follow_symlink) {
        Ok(()) => Ok(()),
        Err(libc::EPERM) if unsafe { libc::geteuid() } != 0 => {
            match apply_ownership(path, u32::MAX, source_meta.gid(), follow_symlink) {
                Ok(()) | Err(libc::EPERM) => Ok(()),
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

pub(super) fn node_exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

pub(super) fn create_dir_all_with_recorded_dirs(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    if path.as_os_str().is_empty() {
        return Ok(Vec::new());
    }
    if path.is_dir() {
        return Ok(Vec::new());
    }

    let mut missing = Vec::new();
    let mut current = path;
    loop {
        if current.exists() {
            break;
        }
        missing.push(current.to_path_buf());
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }

    if missing.is_empty() {
        fs::create_dir(path)?;
        return Ok(vec![path.to_path_buf()]);
    }

    missing.reverse();
    let mut created = Vec::with_capacity(missing.len());
    for dir in missing {
        match fs::create_dir(&dir) {
            Ok(()) => created.push(dir),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && dir.is_dir() => {}
            Err(err) => return Err(err),
        }
    }
    Ok(created)
}
