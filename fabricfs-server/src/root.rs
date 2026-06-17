use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageInitError {
    #[error("{label} must exist and be a directory: {path}")]
    MissingDirectory { label: &'static str, path: PathBuf },
    #[error("create {label} directory {path}: {source}")]
    CreateDirectory {
        label: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn clean_root(path: PathBuf) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(..) => {}
            std::path::Component::RootDir => cleaned.push("/"),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                cleaned.pop();
            }
            std::path::Component::Normal(segment) => cleaned.push(segment),
        }
    }
    if cleaned.as_os_str().is_empty() {
        cleaned.push("/");
    }
    cleaned
}

pub fn require_existing_dir(
    path: impl AsRef<Path>,
    label: &'static str,
) -> Result<PathBuf, StorageInitError> {
    let cleaned = clean_root(path.as_ref().to_path_buf());
    if cleaned.is_dir() {
        Ok(cleaned)
    } else {
        Err(StorageInitError::MissingDirectory {
            label,
            path: cleaned,
        })
    }
}

pub fn ensure_dir(path: impl AsRef<Path>, label: &'static str) -> Result<(), StorageInitError> {
    let path = path.as_ref();
    std::fs::create_dir_all(path).map_err(|source| StorageInitError::CreateDirectory {
        label,
        path: path.to_path_buf(),
        source,
    })
}
