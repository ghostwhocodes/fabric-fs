use std::collections::HashMap;

use crate::server::Stat;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(super) struct FileIdentity {
    dev: u64,
    ino: u64,
}

impl FileIdentity {
    pub(super) fn from_stat(stat: &Stat) -> Self {
        Self {
            dev: stat.dev,
            ino: stat.ino,
        }
    }
}

pub(super) struct InodeMap {
    by_path: HashMap<String, u64>,
    identity_by_path: HashMap<String, FileIdentity>,
    by_identity: HashMap<FileIdentity, u64>,
    next_inode: u64,
}

impl InodeMap {
    pub(super) fn new() -> Self {
        let mut by_path = HashMap::new();
        by_path.insert("/".into(), 1);
        Self {
            by_path,
            identity_by_path: HashMap::new(),
            by_identity: HashMap::new(),
            next_inode: 2,
        }
    }

    pub(super) fn inode_for(&mut self, path: &str, identity: Option<FileIdentity>) -> u64 {
        let path = normalize_path(path);
        if path == "/" {
            if let Some(identity) = identity {
                self.set_path_identity(path, 1, identity);
            }
            return 1;
        }

        if let Some(identity) = identity {
            if let Some(inode) = self.by_path.get(&path).copied() {
                self.set_path_identity(path, inode, identity);
                return inode;
            }
            if let Some(inode) = self.by_identity.get(&identity).copied() {
                self.by_path.insert(path.clone(), inode);
                self.set_path_identity(path, inode, identity);
                return inode;
            }
            let inode = self.allocate_inode();
            self.by_path.insert(path.clone(), inode);
            self.set_path_identity(path, inode, identity);
            return inode;
        }

        if let Some(inode) = self.by_path.get(&path).copied() {
            return inode;
        }
        let inode = self.allocate_inode();
        self.by_path.insert(path, inode);
        inode
    }

    pub(super) fn rename_path(&mut self, old_path: &str, new_path: &str) {
        let old_path = normalize_path(old_path);
        let new_path = normalize_path(new_path);
        self.remove_path(&new_path);
        let moved = self
            .by_path
            .iter()
            .filter_map(|(path, inode)| {
                path.strip_prefix(&old_path).and_then(|suffix| {
                    (suffix.is_empty() || suffix.starts_with('/'))
                        .then(|| (path.clone(), *inode, format!("{new_path}{suffix}")))
                })
            })
            .collect::<Vec<_>>();
        for (old, inode, new) in moved {
            let identity = self.identity_by_path.remove(&old);
            self.by_path.remove(&old);
            self.by_path.insert(new.clone(), inode);
            if let Some(identity) = identity {
                self.identity_by_path.insert(new, identity);
            }
        }
    }

    pub(super) fn remove_path(&mut self, path: &str) {
        let path = normalize_path(path);
        let removed = self
            .by_path
            .keys()
            .filter(|cached| {
                *cached == &path
                    || cached
                        .strip_prefix(&path)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .cloned()
            .collect::<Vec<_>>();
        for cached in removed {
            self.by_path.remove(&cached);
            self.identity_by_path.remove(&cached);
        }
        self.retain_live_identities();
        self.by_path.insert("/".into(), 1);
    }

    fn allocate_inode(&mut self) -> u64 {
        let inode = self.next_inode;
        self.next_inode = self.next_inode.saturating_add(1).max(2);
        inode
    }

    fn set_path_identity(&mut self, path: String, inode: u64, identity: FileIdentity) {
        let previous = self.identity_by_path.insert(path, identity);
        if let Some(previous) = previous.filter(|previous| *previous != identity) {
            let identity_still_live = self
                .identity_by_path
                .values()
                .any(|cached| *cached == previous);
            if !identity_still_live {
                self.by_identity.remove(&previous);
            }
        }
        self.by_identity.insert(identity, inode);
    }

    fn retain_live_identities(&mut self) {
        let live = self
            .identity_by_path
            .values()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        self.by_identity
            .retain(|identity, _| live.contains(identity));
    }
}

fn normalize_path(path: &str) -> String {
    if path == "/" {
        "/".into()
    } else {
        format!("/{}", path.trim_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_map_preserves_cached_path_inode_when_identity_changes() {
        let mut inodes = InodeMap::new();
        let lower_identity = FileIdentity { dev: 1, ino: 10 };
        let cow_identity = FileIdentity { dev: 2, ino: 20 };

        let inode = inodes.inode_for("/file", Some(lower_identity));

        assert_eq!(inodes.inode_for("/file", Some(cow_identity)), inode);
        assert_eq!(inodes.inode_for("/hardlink", Some(cow_identity)), inode);
        assert_ne!(inodes.inode_for("/new-lower", Some(lower_identity)), inode);
    }

    #[test]
    fn inode_map_seeds_copy_up_identity_for_new_hardlink_paths() {
        let mut inodes = InodeMap::new();
        let lower_identity = FileIdentity { dev: 1, ino: 10 };
        let cow_identity = FileIdentity { dev: 2, ino: 20 };

        let source_inode = inodes.inode_for("/source", Some(lower_identity));
        assert_eq!(
            inodes.inode_for("/source", Some(cow_identity)),
            source_inode
        );

        assert_eq!(
            inodes.inode_for("/linked", Some(cow_identity)),
            source_inode
        );
    }
}
