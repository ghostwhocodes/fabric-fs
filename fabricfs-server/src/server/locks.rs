use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::MetadataExt;

use super::errors::io_errno;
use super::paths::descendant_suffix;
use super::FileLock;

#[derive(Default, Debug)]
pub struct LockTable {
    by_key: HashMap<String, Vec<BackendLock>>,
}

#[derive(Default, Debug)]
pub struct FlockTable {
    by_key: HashMap<String, Vec<BackendFlock>>,
}

#[derive(Clone, Debug)]
struct BackendLock {
    handle: u64,
    owner: u64,
    start: u64,
    end: u64,
    typ: i32,
    pid: u32,
}

#[derive(Clone, Debug)]
struct BackendFlock {
    handle: u64,
    owner: u64,
    typ: i32,
}

pub fn lock_required_access(typ: i32) -> Result<u32, i32> {
    match typ {
        value if value == libc::F_RDLCK => Ok(0o4),
        value if value == libc::F_WRLCK => Ok(0o2),
        value if value == libc::F_UNLCK => Ok(0),
        _ => Err(libc::EINVAL),
    }
}

pub fn lock_key_for_file(file: &File) -> Result<String, i32> {
    let meta = file.metadata().map_err(io_errno)?;
    Ok(format!("{}:{}", meta.dev(), meta.ino()))
}

impl LockTable {
    pub fn getlk(
        &self,
        key: &str,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
    ) -> Result<Option<FileLock>, i32> {
        validate_lock_request(start, end, typ)?;
        if typ == libc::F_UNLCK {
            return Ok(None);
        }

        Ok(self.by_key.get(key).and_then(|locks| {
            locks
                .iter()
                .find(|lock| lock.conflicts(owner, start, end, typ))
                .map(|lock| FileLock {
                    start: lock.start,
                    end: lock.end,
                    typ: lock.typ,
                    pid: lock.pid,
                })
        }))
    }

    pub fn setlk(
        &mut self,
        key: String,
        handle: u64,
        owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<(), i32> {
        validate_lock_request(start, end, typ)?;
        if typ == libc::F_UNLCK {
            self.unlock_owned_range(&key, owner, start, end);
            return Ok(());
        }

        if self.by_key.get(&key).is_some_and(|locks| {
            locks
                .iter()
                .any(|lock| lock.conflicts(owner, start, end, typ))
        }) {
            return Err(libc::EAGAIN);
        }

        self.unlock_owned_range(&key, owner, start, end);
        let locks = self.by_key.entry(key).or_default();
        locks.push(BackendLock {
            handle,
            owner,
            start,
            end,
            typ,
            pid,
        });
        Self::merge_path_locks(locks);
        Ok(())
    }

    pub fn release_handle(&mut self, handle: u64) {
        self.by_key.retain(|_, locks| {
            locks.retain(|lock| lock.handle != handle);
            !locks.is_empty()
        });
    }

    pub fn release_owner(&mut self, key: &str, owner: u64) {
        let Some(locks) = self.by_key.get_mut(key) else {
            return;
        };
        locks.retain(|lock| lock.owner != owner);
        if locks.is_empty() {
            self.by_key.remove(key);
        }
    }

    pub fn rename_path(&mut self, old_path: &str, new_path: &str) {
        let keys = self.by_key.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(suffix) = descendant_suffix(&key, old_path) else {
                continue;
            };
            let new_key = format!("{new_path}{suffix}");
            if let Some(mut moved) = self.by_key.remove(&key) {
                self.by_key.entry(new_key).or_default().append(&mut moved);
            }
        }
        for locks in self.by_key.values_mut() {
            Self::merge_path_locks(locks);
        }
    }

    fn unlock_owned_range(&mut self, key: &str, owner: u64, start: u64, end: u64) {
        let Some(locks) = self.by_key.get_mut(key) else {
            return;
        };
        let mut retained = Vec::with_capacity(locks.len());
        for lock in locks.drain(..) {
            if lock.owner != owner || !ranges_overlap(lock.start, lock.end, start, end) {
                retained.push(lock);
                continue;
            }
            if lock.start < start {
                let mut left = lock.clone();
                left.end = start - 1;
                retained.push(left);
            }
            if lock.end > end {
                let mut right = lock;
                right.start = end + 1;
                retained.push(right);
            }
        }
        *locks = retained;
        Self::merge_path_locks(locks);
        if locks.is_empty() {
            self.by_key.remove(key);
        }
    }

    fn merge_path_locks(locks: &mut Vec<BackendLock>) {
        locks.sort_by_key(|lock| (lock.owner, lock.typ, lock.pid, lock.handle, lock.start));
        let mut merged: Vec<BackendLock> = Vec::with_capacity(locks.len());
        for lock in locks.drain(..) {
            let Some(last) = merged.last_mut() else {
                merged.push(lock);
                continue;
            };
            let adjacent = last.end == u64::MAX || last.end.saturating_add(1) >= lock.start;
            if last.owner == lock.owner
                && last.typ == lock.typ
                && last.pid == lock.pid
                && last.handle == lock.handle
                && adjacent
            {
                last.end = last.end.max(lock.end);
            } else {
                merged.push(lock);
            }
        }
        *locks = merged;
    }
}

impl FlockTable {
    pub fn flock(
        &mut self,
        key: String,
        handle: u64,
        owner: u64,
        operation: i32,
        _pid: u32,
    ) -> Result<(), i32> {
        let typ = validate_flock_operation(operation)?;
        let nonblocking = operation & libc::LOCK_NB != 0;
        if typ == libc::LOCK_UN {
            self.unlock_owned(&key, owner);
            return Ok(());
        }

        if self
            .by_key
            .get(&key)
            .is_some_and(|locks| locks.iter().any(|lock| lock.conflicts(owner, typ)))
        {
            return Err(if nonblocking {
                libc::EAGAIN
            } else {
                libc::EOPNOTSUPP
            });
        }

        self.unlock_owned(&key, owner);
        self.by_key
            .entry(key)
            .or_default()
            .push(BackendFlock { handle, owner, typ });
        Ok(())
    }

    pub fn release_handle(&mut self, handle: u64) {
        self.by_key.retain(|_, locks| {
            locks.retain(|lock| lock.handle != handle);
            !locks.is_empty()
        });
    }

    pub fn rename_path(&mut self, old_path: &str, new_path: &str) {
        let keys = self.by_key.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(suffix) = descendant_suffix(&key, old_path) else {
                continue;
            };
            let new_key = format!("{new_path}{suffix}");
            if let Some(mut moved) = self.by_key.remove(&key) {
                self.by_key.entry(new_key).or_default().append(&mut moved);
            }
        }
    }

    fn unlock_owned(&mut self, key: &str, owner: u64) {
        let Some(locks) = self.by_key.get_mut(key) else {
            return;
        };
        locks.retain(|lock| lock.owner != owner);
        if locks.is_empty() {
            self.by_key.remove(key);
        }
    }
}

impl BackendLock {
    fn conflicts(&self, requester_owner: u64, start: u64, end: u64, typ: i32) -> bool {
        self.owner != requester_owner
            && ranges_overlap(self.start, self.end, start, end)
            && (self.typ == libc::F_WRLCK || typ == libc::F_WRLCK)
    }
}

impl BackendFlock {
    fn conflicts(&self, requester_owner: u64, typ: i32) -> bool {
        self.owner != requester_owner && (self.typ == libc::LOCK_EX || typ == libc::LOCK_EX)
    }
}

fn validate_lock_request(start: u64, end: u64, typ: i32) -> Result<(), i32> {
    if start > end {
        return Err(libc::EINVAL);
    }
    let _ = lock_required_access(typ)?;
    Ok(())
}

fn validate_flock_operation(operation: i32) -> Result<i32, i32> {
    match operation & !libc::LOCK_NB {
        value if value == libc::LOCK_SH => Ok(libc::LOCK_SH),
        value if value == libc::LOCK_EX => Ok(libc::LOCK_EX),
        value if value == libc::LOCK_UN => Ok(libc::LOCK_UN),
        _ => Err(libc::EINVAL),
    }
}

fn ranges_overlap(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    left_start <= right_end && right_start <= left_end
}
