use std::path::PathBuf;

use crate::root::{require_existing_dir, StorageInitError};
use crate::server::strip_root;

use super::tombstones::TOMBSTONE_DIR;
use super::xattrs::XATTR_DIR;

pub(super) struct Layout {
    pub(super) backing: Option<PathBuf>,
    pub(super) alias: Option<PathBuf>,
    pub(super) cow: Option<PathBuf>,
}

impl Layout {
    pub(super) fn new(
        backing: Option<PathBuf>,
        alias: Option<PathBuf>,
        cow: Option<PathBuf>,
    ) -> Result<Self, StorageInitError> {
        Ok(Self {
            backing: backing
                .map(|path| require_existing_dir(path, "backing_root"))
                .transpose()?,
            alias: alias
                .map(|path| require_existing_dir(path, "alias_path"))
                .transpose()?,
            cow: cow
                .map(|path| require_existing_dir(path, "cow_path"))
                .transpose()?,
        })
    }

    pub(super) fn has_cow(&self) -> bool {
        self.cow.is_some()
    }

    pub(super) fn is_writable(&self) -> bool {
        self.alias.is_some() && (self.cow.is_some() || self.backing.is_some())
    }

    pub(super) fn ensure_writable(&self) -> Result<(), i32> {
        if self.is_writable() {
            Ok(())
        } else {
            Err(libc::EROFS)
        }
    }

    pub(super) fn overlay_path(&self, rel: &str) -> Option<PathBuf> {
        self.cow.as_ref().map(|root| root.join(strip_root(rel)))
    }

    pub(super) fn backing_path(&self, rel: &str) -> Option<PathBuf> {
        self.backing.as_ref().map(|root| root.join(strip_root(rel)))
    }

    pub(super) fn tombstone_path(&self, rel: &str) -> Option<PathBuf> {
        let stripped = strip_root(rel);
        if stripped.is_empty() {
            return None;
        }
        self.alias
            .as_ref()
            .map(|root| root.join(TOMBSTONE_DIR).join(stripped))
    }

    pub(super) fn xattr_root(&self) -> Option<PathBuf> {
        self.cow.as_ref().map(|root| root.join(XATTR_DIR))
    }
}
