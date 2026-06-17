use fs_protocol::{
    pb, validate_invalidation, validate_rename_paths, InvalidationKind, OperationEffect, PathRole,
    RequestEnvelope, RequestPayload, ResponseEnvelope, ResponsePayload, OPEN_FLAG_TRUNCATE,
};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

use crate::FuseError;

#[derive(Debug)]
pub(crate) struct CacheKernel {
    cache: Mutex<PathCache>,
    open_handles: Mutex<HashMap<OpenHandleKey, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidationApplyOutcome {
    pub full_resync: bool,
}

impl CacheKernel {
    pub(crate) fn new() -> Self {
        Self {
            cache: Mutex::new(PathCache::new()),
            open_handles: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn insert_lookup(&self, path: String, inode: u64, count: u64) {
        self.lock_cache().insert_lookup(path, inode, count);
    }

    pub(crate) fn forget(&self, inode: u64, nlookup: u64) {
        self.lock_cache().forget(inode, nlookup);
    }

    pub(crate) fn cached_path(&self, inode: u64) -> Option<String> {
        self.lock_cache().representative_path(inode)
    }

    pub(crate) fn poisoned(&self) -> bool {
        self.lock_cache().poisoned
    }

    pub(crate) fn parent_inode(&self, inode: u64) -> Result<u64, FuseError> {
        self.with_cache_snapshot(|cache| cache.parent_inode(inode))
    }

    pub(crate) fn path_for_inode(&self, inode: u64) -> Result<String, FuseError> {
        self.with_cache_snapshot(|cache| cache.path_for_inode(inode))
    }

    pub(crate) fn path_for_handle_bound_call(
        &self,
        inode: u64,
        handle: u64,
    ) -> Result<String, FuseError> {
        self.with_cache_and_handles_snapshot(|cache, handles| {
            path_for_handle_bound_call(cache, handles, inode, handle)
        })
    }

    pub(crate) fn path_for_release(&self, inode: u64, handle: u64) -> Result<String, FuseError> {
        self.path_for_handle_bound_call(inode, handle)
    }

    pub(crate) fn remember_handle_path(&self, inode: u64, handle: u64, path: String) {
        self.lock_handles()
            .insert(OpenHandleKey { inode, handle }, path);
    }

    pub(crate) fn forget_handle_path(&self, inode: u64, handle: u64) {
        self.lock_handles().remove(&OpenHandleKey { inode, handle });
    }

    pub(crate) fn child_path(&self, parent: u64, name: &str) -> Result<String, FuseError> {
        self.with_cache_snapshot(|cache| cache.child_path(parent, name))
    }

    pub(crate) fn parent_and_path_for_inode(&self, inode: u64) -> Result<(u64, String), FuseError> {
        self.with_cache_snapshot(|cache| {
            let path = cache.path_for_inode(inode)?;
            let parent = cache.parent_inode_for_path(&path)?;
            Ok((parent, path))
        })
    }

    pub(crate) fn rename_paths(
        &self,
        old_parent: u64,
        old_name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(String, String), FuseError> {
        self.with_cache_snapshot(|cache| {
            Ok((
                cache.child_path(old_parent, old_name)?,
                cache.child_path(new_parent, new_name)?,
            ))
        })
    }

    pub(crate) fn hardlink_paths(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(String, String), FuseError> {
        self.with_cache_snapshot(|cache| {
            Ok((
                cache.path_for_inode(inode)?,
                cache.child_path(new_parent, new_name)?,
            ))
        })
    }

    pub(crate) fn copy_file_range_paths(
        &self,
        input_inode: u64,
        input_handle: u64,
        output_inode: u64,
        output_handle: u64,
    ) -> Result<(String, String), FuseError> {
        self.with_cache_and_handles_snapshot(|cache, handles| {
            Ok((
                path_for_handle_bound_call(cache, handles, input_inode, input_handle)?,
                path_for_handle_bound_call(cache, handles, output_inode, output_handle)?,
            ))
        })
    }

    pub(crate) fn apply_invalidation(
        &self,
        namespace: &str,
        invalidation: &pb::Invalidation,
        allow_empty_sequence_baseline: bool,
    ) -> Result<InvalidationApplyOutcome, FuseError> {
        if invalidation.namespace.is_empty() {
            self.poison();
            return Err(FuseError::Protocol(
                "invalidation namespace must not be empty".into(),
            ));
        }
        if invalidation.namespace != namespace {
            return Ok(InvalidationApplyOutcome { full_resync: false });
        }
        if let Err(error) = validate_invalidation(invalidation) {
            self.poison();
            return Err(FuseError::Protocol(error.to_string()));
        }
        let kind = InvalidationKind::try_from(invalidation.kind)
            .map_err(|error| FuseError::Protocol(error.to_string()))?;
        let mut cache = self.lock_cache();
        if kind == InvalidationKind::FullResync {
            cache.reset_with_sequence(invalidation.sequence);
            return Ok(InvalidationApplyOutcome { full_resync: true });
        }
        if invalidation.sequence != cache.last_sequence + 1 {
            let mut handles = self.lock_handles();
            if kind == InvalidationKind::Rename {
                rename_handle_paths(&mut handles, &invalidation.old_path, &invalidation.new_path);
            }
            let handles_empty = handles.is_empty();
            if invalidation.sequence == 0
                || !allow_empty_sequence_baseline
                || !handles_empty
                || !cache.can_accept_noncontiguous_sequence()
            {
                cache.poison();
                return Err(FuseError::StaleCache);
            }
            cache.last_sequence = invalidation.sequence - 1;
        }
        match kind {
            InvalidationKind::Create => {
                if invalidation.inode == 0 {
                    cache.remove_path(&invalidation.path);
                } else {
                    cache.insert(invalidation.path.clone(), invalidation.inode);
                }
            }
            InvalidationKind::Modify | InvalidationKind::Metadata | InvalidationKind::Xattr => {}
            InvalidationKind::Delete => cache.remove_path(&invalidation.path),
            InvalidationKind::Rename => {
                if let Err(error) = cache.rename(&invalidation.old_path, &invalidation.new_path) {
                    cache.poison();
                    return Err(FuseError::Protocol(error));
                }
                let mut handles = self.lock_handles();
                rename_handle_paths(&mut handles, &invalidation.old_path, &invalidation.new_path);
                cache.last_sequence = invalidation.sequence;
                return Ok(InvalidationApplyOutcome { full_resync: false });
            }
            InvalidationKind::FullResync => {}
        }
        cache.last_sequence = invalidation.sequence;
        Ok(InvalidationApplyOutcome { full_resync: false })
    }

    pub(crate) fn apply_success_response_invalidation(
        &self,
        namespace: &str,
        invalidation: &pb::Invalidation,
    ) -> Result<(), FuseError> {
        match self.apply_invalidation(namespace, invalidation, false) {
            Ok(_) | Err(FuseError::StaleCache) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn require_covering_path_invalidation(
        &self,
        request: &RequestEnvelope,
        response: &ResponseEnvelope,
    ) -> Result<(), FuseError> {
        let Some(payload) = response.payload.as_ref() else {
            return Ok(());
        };
        let Some(required) =
            RequiredPathInvalidation::for_request_and_response(&request.payload, payload)
        else {
            return Ok(());
        };
        if response
            .invalidations
            .iter()
            .any(|invalidation| required.matches(request, invalidation))
        {
            return Ok(());
        }
        self.poison();
        Err(FuseError::StaleCache)
    }

    pub(crate) fn poison_after_uncertain_path_mutation(&self, request: &RequestEnvelope) -> bool {
        if request_requires_path_invalidation(&request.payload) {
            self.poison();
            true
        } else {
            false
        }
    }

    pub(crate) fn poison(&self) {
        self.lock_cache().poison();
    }

    fn with_cache_snapshot<R>(
        &self,
        read: impl FnOnce(&PathCache) -> Result<R, FuseError>,
    ) -> Result<R, FuseError> {
        let cache = self.lock_cache();
        read(&cache)
    }

    fn with_cache_and_handles_snapshot<R>(
        &self,
        read: impl FnOnce(&PathCache, &HashMap<OpenHandleKey, String>) -> Result<R, FuseError>,
    ) -> Result<R, FuseError> {
        let cache = self.lock_cache();
        let handles = self.lock_handles();
        read(&cache, &handles)
    }

    fn lock_cache(&self) -> MutexGuard<'_, PathCache> {
        match self.cache.lock() {
            Ok(cache) => cache,
            Err(poisoned) => {
                let mut cache = poisoned.into_inner();
                cache.poison();
                cache
            }
        }
    }

    fn lock_handles(&self) -> MutexGuard<'_, HashMap<OpenHandleKey, String>> {
        match self.open_handles.lock() {
            Ok(handles) => handles,
            Err(poisoned) => {
                let mut handles = poisoned.into_inner();
                handles.clear();
                handles
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_lookup_for_test(&self, path: String, inode: u64, count: u64) {
        self.insert_lookup(path, inode, count);
    }

    #[cfg(test)]
    pub(crate) fn with_cache_snapshot_for_test<R>(
        &self,
        read: impl FnOnce(&PathCache) -> Result<R, FuseError>,
    ) -> Result<R, FuseError> {
        self.with_cache_snapshot(read)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OpenHandleKey {
    inode: u64,
    handle: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RequiredPathInvalidation {
    Create {
        path: String,
        inode: u64,
    },
    Delete {
        path: String,
    },
    Rename {
        old_path: String,
        new_path: String,
    },
    Update {
        path: String,
        kind: InvalidationKind,
    },
}

impl RequiredPathInvalidation {
    fn for_request_and_response(
        request: &RequestPayload,
        response: &ResponsePayload,
    ) -> Option<Self> {
        if let RequestPayload::Open(value) = request {
            if value.flags & OPEN_FLAG_TRUNCATE != 0 {
                Some(Self::Update {
                    path: request.primary_path()?.to_owned(),
                    kind: InvalidationKind::Modify,
                })
            } else {
                None
            }
        } else {
            match request.operation().spec().effect {
                OperationEffect::CreateNode => Some(Self::Create {
                    path: request.primary_path()?.to_owned(),
                    inode: response.created_inode()?,
                }),
                OperationEffect::DeleteNode => Some(Self::Delete {
                    path: request.primary_path()?.to_owned(),
                }),
                OperationEffect::RenameNode => Some(Self::Rename {
                    old_path: request.path_for_role(PathRole::Source)?.to_owned(),
                    new_path: request.path_for_role(PathRole::Target)?.to_owned(),
                }),
                effect => {
                    let kind = invalidation_kind_for_effect(effect)?;
                    Some(Self::Update {
                        path: request.primary_path()?.to_owned(),
                        kind,
                    })
                }
            }
        }
    }

    fn matches(&self, request: &RequestEnvelope, invalidation: &pb::Invalidation) -> bool {
        if invalidation.namespace != request.namespace
            || invalidation.request_id != request.request_id
        {
            return false;
        }
        match (self, InvalidationKind::try_from(invalidation.kind)) {
            (_, Ok(InvalidationKind::FullResync)) => true,
            (Self::Create { path, inode }, Ok(InvalidationKind::Create)) => {
                invalidation.path == *path && invalidation.inode == *inode
            }
            (Self::Delete { path }, Ok(InvalidationKind::Delete)) => invalidation.path == *path,
            (Self::Rename { old_path, new_path }, Ok(InvalidationKind::Rename)) => {
                invalidation.old_path == *old_path && invalidation.new_path == *new_path
            }
            (Self::Update { path, kind }, Ok(actual)) => {
                actual == *kind && invalidation.path == *path
            }
            _ => false,
        }
    }
}

fn request_requires_path_invalidation(payload: &RequestPayload) -> bool {
    match payload {
        RequestPayload::Open(value) if value.flags & OPEN_FLAG_TRUNCATE != 0 => true,
        payload => payload
            .operation()
            .spec()
            .effect
            .requires_path_invalidation(),
    }
}

fn invalidation_kind_for_effect(effect: OperationEffect) -> Option<InvalidationKind> {
    match effect {
        OperationEffect::ContentMutation => Some(InvalidationKind::Modify),
        OperationEffect::CreateNode => Some(InvalidationKind::Create),
        OperationEffect::RenameNode => Some(InvalidationKind::Rename),
        OperationEffect::DeleteNode => Some(InvalidationKind::Delete),
        OperationEffect::MetadataMutation => Some(InvalidationKind::Metadata),
        OperationEffect::XattrMutation => Some(InvalidationKind::Xattr),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PathCache {
    path_to_inode: HashMap<String, u64>,
    inode_to_paths: HashMap<u64, BTreeSet<String>>,
    lookup_counts: HashMap<u64, u64>,
    poisoned: bool,
    last_sequence: u64,
}

impl PathCache {
    fn new() -> Self {
        let mut cache = Self {
            path_to_inode: HashMap::new(),
            inode_to_paths: HashMap::new(),
            lookup_counts: HashMap::new(),
            poisoned: false,
            last_sequence: 0,
        };
        cache.insert("/".into(), 1);
        cache
    }

    fn insert(&mut self, path: String, inode: u64) {
        if inode == 0 {
            return;
        }
        if let Some(old_inode) = self
            .path_to_inode
            .get(&path)
            .copied()
            .filter(|old| *old != inode)
        {
            self.remove_inode_path(old_inode, &path);
        }
        self.path_to_inode.insert(path.clone(), inode);
        self.inode_to_paths.entry(inode).or_default().insert(path);
    }

    fn insert_lookup(&mut self, path: String, inode: u64, count: u64) {
        self.insert(path, inode);
        self.add_lookup(inode, count);
    }

    fn add_lookup(&mut self, inode: u64, count: u64) {
        if inode == 0 || inode == 1 || count == 0 || !self.inode_to_paths.contains_key(&inode) {
            return;
        }
        let current = self.lookup_counts.entry(inode).or_insert(0);
        *current = current.saturating_add(count);
    }

    fn forget(&mut self, inode: u64, nlookup: u64) {
        if inode == 1 || nlookup == 0 {
            return;
        }
        let Some(count) = self.lookup_counts.get_mut(&inode) else {
            self.remove_inode(inode);
            return;
        };
        if *count > nlookup {
            *count -= nlookup;
            return;
        }
        self.lookup_counts.remove(&inode);
        self.remove_inode(inode);
    }

    fn rename(&mut self, old_path: &str, new_path: &str) -> Result<(), String> {
        validate_rename_paths(old_path, new_path).map_err(|error| error.to_string())?;
        let moved = self.paths_in_subtree(old_path);
        self.remove_subtree_except(new_path, &moved);
        for (inode, path) in moved {
            self.path_to_inode.remove(&path);
            self.detach_inode_path_for_rename(inode, &path);
            let suffix = subtree_suffix(&path, old_path);
            let renamed = format!("{new_path}{suffix}");
            self.path_to_inode.insert(renamed.clone(), inode);
            self.inode_to_paths
                .entry(inode)
                .or_default()
                .insert(renamed);
        }
        Ok(())
    }

    fn remove_path(&mut self, path: &str) {
        if path.is_empty() || path == "/" {
            return;
        }
        for (inode, cached_path) in self.paths_in_subtree(path) {
            self.path_to_inode.remove(&cached_path);
            self.remove_inode_path(inode, &cached_path);
        }
    }

    fn remove_inode(&mut self, inode: u64) {
        if inode == 1 {
            return;
        }
        if let Some(paths) = self.inode_to_paths.remove(&inode) {
            for path in paths {
                self.path_to_inode.remove(&path);
            }
        }
        self.lookup_counts.remove(&inode);
    }

    fn poison(&mut self) {
        self.poisoned = true;
        self.path_to_inode.clear();
        self.inode_to_paths.clear();
        self.lookup_counts.clear();
        self.insert("/".into(), 1);
    }

    fn reset_with_sequence(&mut self, sequence: u64) {
        self.poisoned = false;
        self.path_to_inode.clear();
        self.inode_to_paths.clear();
        self.lookup_counts.clear();
        self.last_sequence = sequence;
        self.insert("/".into(), 1);
    }

    fn can_accept_noncontiguous_sequence(&self) -> bool {
        !self.poisoned
            && self.path_to_inode.len() == 1
            && self.path_to_inode.get("/") == Some(&1)
            && self.inode_to_paths.len() == 1
            && self
                .inode_to_paths
                .get(&1)
                .is_some_and(|paths| paths.len() == 1 && paths.contains("/"))
            && self.lookup_counts.is_empty()
    }

    pub(crate) fn path_for_inode(&self, inode: u64) -> Result<String, FuseError> {
        if self.poisoned {
            return Err(FuseError::StaleCache);
        }
        self.representative_path(inode).ok_or(FuseError::StaleCache)
    }

    pub(crate) fn child_path(&self, parent: u64, name: &str) -> Result<String, FuseError> {
        validate_child_name(name)?;
        let parent_path = self.path_for_inode(parent)?;
        Ok(join_child_path(&parent_path, name))
    }

    pub(crate) fn parent_inode(&self, inode: u64) -> Result<u64, FuseError> {
        let path = self.path_for_inode(inode)?;
        self.parent_inode_for_path(&path)
    }

    fn parent_inode_for_path(&self, path: &str) -> Result<u64, FuseError> {
        if self.poisoned {
            return Err(FuseError::StaleCache);
        }
        if path == "/" {
            return Ok(1);
        }
        let parent_path = parent_path(path);
        self.path_to_inode
            .get(parent_path)
            .copied()
            .ok_or(FuseError::StaleCache)
    }

    fn paths_in_subtree(&self, path: &str) -> Vec<(u64, String)> {
        self.path_to_inode
            .iter()
            .filter(|(cached_path, _)| path_is_in_subtree(cached_path, path))
            .map(|(cached_path, inode)| (*inode, cached_path.clone()))
            .collect()
    }

    fn remove_subtree_except(&mut self, path: &str, except: &[(u64, String)]) {
        let except_paths = except
            .iter()
            .map(|(_, path)| path.as_str())
            .collect::<HashSet<_>>();
        for (inode, cached_path) in self.paths_in_subtree(path) {
            if except_paths.contains(cached_path.as_str()) {
                continue;
            }
            self.path_to_inode.remove(&cached_path);
            self.remove_inode_path(inode, &cached_path);
        }
    }

    fn representative_path(&self, inode: u64) -> Option<String> {
        self.inode_to_paths
            .get(&inode)
            .and_then(|paths| paths.iter().next().cloned())
    }

    fn remove_inode_path(&mut self, inode: u64, path: &str) {
        let Some(paths) = self.inode_to_paths.get_mut(&inode) else {
            self.lookup_counts.remove(&inode);
            return;
        };
        paths.remove(path);
        if paths.is_empty() {
            self.inode_to_paths.remove(&inode);
            self.lookup_counts.remove(&inode);
        }
    }

    fn detach_inode_path_for_rename(&mut self, inode: u64, path: &str) {
        let Some(paths) = self.inode_to_paths.get_mut(&inode) else {
            return;
        };
        paths.remove(path);
        if paths.is_empty() {
            self.inode_to_paths.remove(&inode);
        }
    }
}

fn path_is_in_subtree(path: &str, root: &str) -> bool {
    if root.is_empty() {
        return false;
    }
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn subtree_suffix<'a>(path: &'a str, root: &str) -> &'a str {
    path.strip_prefix(root).unwrap_or_default()
}

fn parent_path(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) | None => "/",
        Some((parent, _)) => parent,
    }
}

fn validate_child_name(name: &str) -> Result<(), FuseError> {
    if name.is_empty() || name.contains('/') || name.as_bytes().contains(&0) {
        return Err(FuseError::Protocol("invalid child name".into()));
    }
    Ok(())
}

fn join_child_path(parent_path: &str, name: &str) -> String {
    if parent_path == "/" {
        format!("/{name}")
    } else {
        format!("{parent_path}/{name}")
    }
}

fn path_for_handle_bound_call(
    cache: &PathCache,
    handles: &HashMap<OpenHandleKey, String>,
    inode: u64,
    handle: u64,
) -> Result<String, FuseError> {
    if let Some(path) = handles.get(&OpenHandleKey { inode, handle }).cloned() {
        return Ok(path);
    }
    if !cache.poisoned {
        if let Some(path) = cache.representative_path(inode) {
            return Ok(path);
        }
    }
    Err(FuseError::StaleCache)
}

fn rename_handle_paths(
    handles: &mut HashMap<OpenHandleKey, String>,
    old_path: &str,
    new_path: &str,
) {
    for path in handles.values_mut() {
        if path_is_in_subtree(path, old_path) {
            let suffix = subtree_suffix(path, old_path).to_owned();
            *path = format!("{new_path}{suffix}");
        }
    }
}
