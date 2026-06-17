use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::root::{ensure_dir, StorageInitError};
use crate::server::{
    errno, io_errno, is_acl_name, list_xattr_names, normalize_path, path_to_cstring,
    read_all_xattrs,
};
use crate::watch::{InternalMetadataNotifier, InternalMetadataWrite};

use super::fs_ops::create_dir_all_with_recorded_dirs;
use super::OverlayFs;

pub(super) const XATTR_DIR: &str = ".fabricfs_xattrs";
const XATTR_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct XattrKey {
    dev: u64,
    ino: u64,
}

type XattrTable = HashMap<String, Vec<u8>>;
type XattrCache = Arc<Mutex<HashMap<XattrKey, XattrTable>>>;

#[derive(Clone)]
pub(super) struct XattrStore {
    root: Option<PathBuf>,
    cache: XattrCache,
    internal_metadata_notifier: Option<Arc<dyn InternalMetadataNotifier>>,
}

#[derive(Serialize, Deserialize, Default)]
struct XattrManifest {
    version: u32,
    entries: HashMap<String, Vec<u8>>,
}

impl XattrStore {
    pub(super) fn new(
        root: Option<PathBuf>,
        internal_metadata_notifier: Option<Arc<dyn InternalMetadataNotifier>>,
    ) -> Result<Self, StorageInitError> {
        if let Some(dir) = root.as_ref() {
            ensure_dir(dir, "xattr_store")?;
        }
        Ok(Self {
            root,
            cache: Arc::new(Mutex::new(HashMap::new())),
            internal_metadata_notifier,
        })
    }

    fn record_completed_internal_metadata_write(&self, write: InternalMetadataWrite) {
        let Some(notifier) = self.internal_metadata_notifier.as_ref() else {
            return;
        };
        notifier.record_completed_internal_metadata_write(write);
    }

    fn key_for_path(&self, path: &Path) -> Result<XattrKey, i32> {
        let meta = fs::symlink_metadata(path).map_err(io_errno)?;
        Ok(XattrKey {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }

    fn load(&self, key: XattrKey) -> Result<XattrTable, i32> {
        if let Ok(cache) = self.cache.lock() {
            if let Some(existing) = cache.get(&key) {
                return Ok(existing.clone());
            }
        }

        let mut out = HashMap::new();
        if let Some(path) = self.manifest_path(&key) {
            if path.exists() {
                let data = fs::read(&path).map_err(io_errno)?;
                if let Ok(manifest) = serde_json::from_slice::<XattrManifest>(&data) {
                    if manifest.version == XATTR_VERSION {
                        out = manifest.entries;
                    }
                }
            }
        }

        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(key, out.clone());
        }
        Ok(out)
    }

    fn update<F>(&self, key: XattrKey, f: F) -> Result<(), i32>
    where
        F: FnOnce(XattrTable) -> Result<XattrTable, i32>,
    {
        let current = self.load(key)?;
        let updated = f(current)?;
        self.persist(key, updated)
    }

    fn persist(&self, key: XattrKey, entries: XattrTable) -> Result<(), i32> {
        if let Some(path) = self.manifest_path(&key) {
            let mut created_dirs = Vec::new();
            if let Some(parent) = path.parent() {
                created_dirs = create_dir_all_with_recorded_dirs(parent).map_err(io_errno)?;
            }
            let manifest = XattrManifest {
                version: XATTR_VERSION,
                entries: entries.clone(),
            };
            let data = serde_json::to_vec(&manifest).map_err(|_| libc::EIO)?;
            let tmp = path.with_extension("tmp");
            fs::write(&tmp, &data).map_err(io_errno)?;
            if let Ok(file) = File::open(&tmp) {
                let _ = file.sync_all();
            }
            fs::rename(&tmp, &path).map_err(io_errno)?;
            if let Ok(mut cache) = self.cache.lock() {
                cache.insert(key, entries);
            }
            self.record_completed_internal_metadata_write(
                InternalMetadataWrite::atomic_replace(tmp, path, data)
                    .with_created_dirs(created_dirs),
            );
            return Ok(());
        }
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(key, entries);
        }
        Ok(())
    }

    pub(super) fn seed_from_host(
        &self,
        dest_meta: &fs::Metadata,
        source: &Path,
        follow_src: bool,
    ) -> Result<(), i32> {
        let key = XattrKey {
            dev: dest_meta.dev(),
            ino: dest_meta.ino(),
        };
        if self.has_persisted(&key)? {
            return Ok(());
        }
        let values = read_all_xattrs(source, follow_src)?;
        if values.is_empty() {
            return Ok(());
        }
        self.persist(key, values)
    }

    fn has_persisted(&self, key: &XattrKey) -> Result<bool, i32> {
        if let Ok(cache) = self.cache.lock() {
            if cache.contains_key(key) {
                return Ok(true);
            }
        }
        if let Some(path) = self.manifest_path(key) {
            return Ok(path.exists());
        }
        Ok(false)
    }

    pub(super) fn purge_if_last_link(&self, meta: &fs::Metadata) -> Result<(), i32> {
        let is_dir = meta.file_type().is_dir();
        if meta.nlink() > 1 && !is_dir {
            return Ok(());
        }
        let key = XattrKey {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        let manifest = self.manifest_path(&key);
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(&key);
        }
        if let Some(path) = manifest {
            if path.exists() && fs::remove_file(&path).is_ok() {
                self.record_completed_internal_metadata_write(InternalMetadataWrite::remove_file(
                    path,
                ));
            }
        }
        Ok(())
    }

    fn manifest_path(&self, key: &XattrKey) -> Option<PathBuf> {
        self.root.as_ref().map(|root| {
            let dev_dir = root.join(format!("{:016x}", key.dev));
            dev_dir.join(format!("{:016x}.json", key.ino))
        })
    }
}

impl OverlayFs {
    pub(super) fn setxattr(
        &self,
        path: &str,
        name: &str,
        value: Vec<u8>,
        flags: i32,
    ) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.materialize_for_xattr(&rel)?;

        if self.xattr_updates_blocked(&target) {
            return Err(libc::EACCES);
        }

        if is_acl_name(name) {
            let cpath = path_to_cstring(&target)?;
            let cname = CString::new(name).map_err(|_| libc::EINVAL)?;
            let rc = unsafe {
                libc::setxattr(
                    cpath.as_ptr(),
                    cname.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    flags,
                )
            };
            if rc != 0 {
                return Err(errno());
            }
            return Ok(());
        }

        let key = self.state.xattrs.key_for_path(&target)?;
        self.state.xattrs.update(key, |mut table| {
            if flags & libc::XATTR_CREATE != 0 && table.contains_key(name) {
                return Err(libc::EEXIST);
            }
            if flags & libc::XATTR_REPLACE != 0 && !table.contains_key(name) {
                return Err(libc::ENODATA);
            }
            table.insert(name.to_string(), value);
            Ok(table)
        })
    }

    pub(super) fn getxattr(&self, path: &str, name: &str, size: u32) -> Result<Vec<u8>, i32> {
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;

        if is_acl_name(name) {
            let cpath = path_to_cstring(&resolved)?;
            let cname = CString::new(name).map_err(|_| libc::EINVAL)?;

            if size == 0 {
                let rc = unsafe {
                    libc::getxattr(cpath.as_ptr(), cname.as_ptr(), std::ptr::null_mut(), 0)
                };
                if rc < 0 {
                    return Err(errno());
                }
                return Ok(vec![0; rc as usize]);
            }

            let mut buf = vec![0u8; size as usize];
            let rc = unsafe {
                libc::getxattr(
                    cpath.as_ptr(),
                    cname.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    size as usize,
                )
            };
            if rc < 0 {
                return Err(errno());
            }
            buf.truncate(rc as usize);
            return Ok(buf);
        }

        let key = self.state.xattrs.key_for_path(&resolved)?;
        let table = self.state.xattrs.load(key)?;

        match table.get(name) {
            Some(val) if size == 0 => Ok(vec![0; val.len()]),
            Some(val) if size >= val.len() as u32 => Ok(val.clone()),
            Some(_) => Err(libc::ERANGE),
            None => Err(libc::ENODATA),
        }
    }

    pub(super) fn listxattr(&self, path: &str) -> Result<Vec<String>, i32> {
        let rel = normalize_path(path)?;
        let resolved = self.resolve_existing(&rel)?;

        let mut names = Vec::new();
        for name in list_xattr_names(&resolved, true)? {
            if is_acl_name(&name) {
                names.push(name);
            }
        }

        let key = self.state.xattrs.key_for_path(&resolved)?;
        let table = self.state.xattrs.load(key)?;
        for name in table.keys() {
            names.push(name.clone());
        }

        Ok(names)
    }

    pub(super) fn removexattr(&self, path: &str, name: &str) -> Result<(), i32> {
        let rel = normalize_path(path)?;
        let target = self.materialize_for_xattr(&rel)?;

        if self.xattr_updates_blocked(&target) {
            return Err(libc::EACCES);
        }

        if is_acl_name(name) {
            let cpath = path_to_cstring(&target)?;
            let cname = CString::new(name).map_err(|_| libc::EINVAL)?;
            let rc = unsafe { libc::removexattr(cpath.as_ptr(), cname.as_ptr()) };
            if rc != 0 {
                return Err(errno());
            }
            return Ok(());
        }

        let key = self.state.xattrs.key_for_path(&target)?;
        self.state.xattrs.update(key, |mut table| {
            if table.remove(name).is_none() {
                return Err(libc::ENODATA);
            }
            Ok(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watch::StorageInvalidationGate;
    use tempfile::tempdir;

    fn sample_xattr_key() -> XattrKey {
        XattrKey {
            dev: 0x1c,
            ino: 0x620f36,
        }
    }

    fn sample_xattr_entries() -> XattrTable {
        HashMap::from([("user.fabricfs.test".to_string(), b"value".to_vec())])
    }

    #[test]
    fn xattr_persist_failure_does_not_register_self_notify_paths() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join(XATTR_DIR);
        let gate = StorageInvalidationGate::new();
        let store = XattrStore::new(Some(root), Some(Arc::new(gate.clone())))
            .expect("xattr store initializes");
        let key = sample_xattr_key();
        let manifest_path = store.manifest_path(&key).expect("manifest path");
        let tmp_path = manifest_path.with_extension("tmp");
        if let Some(parent) = tmp_path.parent() {
            fs::create_dir_all(parent).expect("tmp parent exists");
        }
        fs::create_dir(&tmp_path).expect("tmp path is blocked by directory");

        let err = store
            .persist(key, sample_xattr_entries())
            .expect_err("persist fails");

        assert_eq!(err, libc::EISDIR);
        assert!(!gate.suppress_self_notify_event(
            &notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            &[tmp_path],
        ));
        assert!(!gate.suppress_self_notify_event(
            &notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            &[manifest_path],
        ));
    }

    #[test]
    fn xattr_persist_rename_failure_does_not_register_self_notify_paths_or_cache() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join(XATTR_DIR);
        let gate = StorageInvalidationGate::new();
        let store = XattrStore::new(Some(root), Some(Arc::new(gate.clone())))
            .expect("xattr store initializes");
        let key = sample_xattr_key();
        let manifest_path = store.manifest_path(&key).expect("manifest path");
        let tmp_path = manifest_path.with_extension("tmp");
        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent).expect("manifest parent exists");
        }
        fs::create_dir(&manifest_path).expect("manifest path blocks rename");

        let err = store
            .persist(key, sample_xattr_entries())
            .expect_err("persist fails after tmp write");

        assert_eq!(err, libc::EISDIR);
        assert!(store
            .cache
            .lock()
            .expect("xattr cache lock")
            .get(&key)
            .is_none());
        assert!(!gate.suppress_self_notify_event(
            &notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            &[tmp_path],
        ));
        assert!(!gate.suppress_self_notify_event(
            &notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            &[manifest_path],
        ));
    }
}
