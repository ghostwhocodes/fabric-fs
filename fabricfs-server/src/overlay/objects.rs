use std::path::{Path, PathBuf};

use crate::server::copy_acl_xattrs;
use crate::storage_runtime::ResolvedStorageObject;
use crate::watch::InternalMetadataWrite;

use super::OverlayFs;

impl OverlayFs {
    pub(super) fn record_completed_internal_metadata_write(&self, write: InternalMetadataWrite) {
        let Some(notifier) = self.state.internal_metadata_notifier.as_ref() else {
            return;
        };
        notifier.record_completed_internal_metadata_write(write);
    }

    pub(super) fn apply_umask(&self, mode: u32) -> u32 {
        mode & !self.state.umask
    }

    pub(super) fn resolved_object(
        &self,
        rel: &str,
        host_path: impl Into<PathBuf>,
    ) -> ResolvedStorageObject {
        ResolvedStorageObject::new(rel, host_path)
    }

    pub(super) fn runtime_object_for_target(
        &self,
        rel: &str,
        host_path: impl Into<PathBuf>,
    ) -> ResolvedStorageObject {
        let host_path = host_path.into();
        ResolvedStorageObject::new(rel, host_path.clone())
            .with_permission_updates_blocked(self.permission_updates_blocked(&host_path))
    }

    pub(super) fn ensure_backing_mutable(&self) -> Result<(), i32> {
        if self.state.layout.has_cow() {
            return Ok(());
        }
        if self.state.allow_direct_backing_updates && self.state.layout.backing.is_some() {
            return Ok(());
        }
        Err(libc::EACCES)
    }
    pub(super) fn permission_updates_blocked(&self, path: &Path) -> bool {
        !self.state.allow_backing_permission_updates && self.is_backing_path(path)
    }

    pub(super) fn xattr_updates_blocked(&self, path: &Path) -> bool {
        !self.state.allow_xattr_updates && self.is_backing_path(path)
    }

    pub(super) fn is_backing_path(&self, path: &Path) -> bool {
        self.state
            .layout
            .backing
            .as_ref()
            .map(|root| path.starts_with(root))
            .unwrap_or(false)
    }

    pub(super) fn copy_acls(
        &self,
        src: &Path,
        dst: &Path,
        follow_src: bool,
        follow_dst: bool,
    ) -> Result<(), i32> {
        if !self.state.propagate_acls {
            return Ok(());
        }
        copy_acl_xattrs(src, dst, follow_src, follow_dst)
    }
}
