use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::root::clean_root;

#[derive(Clone, Debug)]
pub enum InternalMetadataWrite {
    AtomicReplace {
        temp_path: PathBuf,
        final_path: PathBuf,
        final_bytes: Vec<u8>,
        created_dirs: Vec<PathBuf>,
    },
    WriteFile {
        path: PathBuf,
        bytes: Vec<u8>,
        created_dirs: Vec<PathBuf>,
    },
    RemoveFile {
        path: PathBuf,
    },
}

impl InternalMetadataWrite {
    pub fn atomic_replace(temp_path: PathBuf, final_path: PathBuf, final_bytes: Vec<u8>) -> Self {
        Self::AtomicReplace {
            temp_path,
            final_path,
            final_bytes,
            created_dirs: Vec::new(),
        }
    }

    pub fn write_file(path: PathBuf, bytes: Vec<u8>) -> Self {
        Self::WriteFile {
            path,
            bytes,
            created_dirs: Vec::new(),
        }
    }

    pub fn remove_file(path: PathBuf) -> Self {
        Self::RemoveFile { path }
    }

    pub fn with_created_dirs<I>(mut self, created_dirs: I) -> Self
    where
        I: IntoIterator<Item = PathBuf>,
    {
        let created_dirs: Vec<PathBuf> = created_dirs.into_iter().collect();
        match &mut self {
            Self::AtomicReplace {
                created_dirs: existing,
                ..
            }
            | Self::WriteFile {
                created_dirs: existing,
                ..
            } => existing.extend(created_dirs),
            Self::RemoveFile { .. } => {}
        }
        self
    }

    pub(super) fn tracked_paths(self) -> Vec<(PathBuf, TrackedSelfNotifyPath)> {
        let (mut tracked, created_dirs, created_dir_budget) = match self {
            Self::AtomicReplace {
                temp_path,
                final_path,
                final_bytes,
                created_dirs,
            } => (
                vec![
                    (
                        temp_path,
                        TrackedSelfNotifyPath::new(
                            SelfNotifyEventBudget::atomic_replace_temp(),
                            ExpectedPathState::Missing,
                        ),
                    ),
                    (
                        final_path,
                        TrackedSelfNotifyPath::new(
                            SelfNotifyEventBudget::atomic_replace_final(),
                            ExpectedPathState::file(final_bytes),
                        ),
                    ),
                ],
                created_dirs,
                SelfNotifyEventBudget::created_dir_for_atomic_replace(),
            ),
            Self::WriteFile {
                path,
                bytes,
                created_dirs,
            } => (
                vec![(
                    path,
                    TrackedSelfNotifyPath::new(
                        SelfNotifyEventBudget::write_file(),
                        ExpectedPathState::file(bytes),
                    ),
                )],
                created_dirs,
                SelfNotifyEventBudget::created_dir_for_write_file(),
            ),
            Self::RemoveFile { path } => (
                vec![(
                    path,
                    TrackedSelfNotifyPath::new(
                        SelfNotifyEventBudget::remove_file(),
                        ExpectedPathState::Missing,
                    ),
                )],
                Vec::new(),
                SelfNotifyEventBudget::default(),
            ),
        };
        let created_dir_entries = expected_created_dir_entries(&created_dirs, &tracked);
        tracked.extend(created_dirs.into_iter().map(|path| {
            let normalized = clean_root(path.clone());
            let entries = created_dir_entries
                .get(&normalized)
                .cloned()
                .unwrap_or_default();
            (
                path,
                TrackedSelfNotifyPath::new(
                    created_dir_budget,
                    ExpectedPathState::directory(entries),
                ),
            )
        }));
        tracked
    }
}

#[derive(Clone)]
pub(super) struct TrackedSelfNotifyWrite {
    pub(super) expires_at: Instant,
    pub(super) paths: HashMap<PathBuf, TrackedSelfNotifyPath>,
}

#[derive(Clone)]
pub(super) struct TrackedSelfNotifyPath {
    budget: SelfNotifyEventBudget,
    expected_state: ExpectedPathState,
}

#[derive(Clone)]
pub(super) enum ExpectedPathState {
    Missing,
    File(Vec<u8>),
    Directory(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SelfNotifyEventClass {
    Any,
    Create,
    Modify,
    Remove,
    CloseWrite,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SelfNotifyEventBudget {
    any: u8,
    create: u8,
    modify: u8,
    remove: u8,
    close_write: u8,
}

impl SelfNotifyEventBudget {
    const fn new(any: u8, create: u8, modify: u8, remove: u8, close_write: u8) -> Self {
        Self {
            any,
            create,
            modify,
            remove,
            close_write,
        }
    }

    const fn atomic_replace_temp() -> Self {
        Self::new(1, 1, 1, 1, 1)
    }

    const fn atomic_replace_final() -> Self {
        Self::new(1, 1, 1, 0, 0)
    }

    const fn write_file() -> Self {
        Self::new(1, 1, 1, 0, 1)
    }

    const fn created_dir_for_atomic_replace() -> Self {
        Self::new(1, 1, 2, 0, 0)
    }

    const fn created_dir_for_write_file() -> Self {
        Self::new(1, 1, 1, 0, 0)
    }

    const fn remove_file() -> Self {
        Self::new(1, 0, 0, 1, 0)
    }

    pub(super) fn can_consume(self, event_class: SelfNotifyEventClass) -> bool {
        self.remaining(event_class) > 0
    }

    pub(super) fn consume(&mut self, event_class: SelfNotifyEventClass) {
        let remaining = match event_class {
            SelfNotifyEventClass::Any => &mut self.any,
            SelfNotifyEventClass::Create => &mut self.create,
            SelfNotifyEventClass::Modify => &mut self.modify,
            SelfNotifyEventClass::Remove => &mut self.remove,
            SelfNotifyEventClass::CloseWrite => &mut self.close_write,
        };
        *remaining = remaining.saturating_sub(1);
    }

    pub(super) fn is_exhausted(self) -> bool {
        self.any == 0
            && self.create == 0
            && self.modify == 0
            && self.remove == 0
            && self.close_write == 0
    }

    pub(super) fn remaining(self, event_class: SelfNotifyEventClass) -> u8 {
        match event_class {
            SelfNotifyEventClass::Any => self.any,
            SelfNotifyEventClass::Create => self.create,
            SelfNotifyEventClass::Modify => self.modify,
            SelfNotifyEventClass::Remove => self.remove,
            SelfNotifyEventClass::CloseWrite => self.close_write,
        }
    }
}

impl TrackedSelfNotifyWrite {
    pub(super) fn new(write: InternalMetadataWrite, expires_at: Instant) -> Self {
        let mut paths = HashMap::new();
        for (path, tracked_path) in write.tracked_paths() {
            paths.insert(clean_root(path), tracked_path);
        }
        Self { expires_at, paths }
    }

    pub(super) fn overlaps_with(&self, other_paths: &HashSet<PathBuf>) -> bool {
        self.paths.keys().any(|path| other_paths.contains(path))
    }

    pub(super) fn can_consume_paths(
        &self,
        event_class: SelfNotifyEventClass,
        paths: &[PathBuf],
    ) -> bool {
        paths.iter().all(|path| {
            self.paths
                .get(path)
                .is_some_and(|tracked| tracked.budget.can_consume(event_class))
        })
    }

    pub(super) fn matches_expected_state(&self, paths: &[PathBuf]) -> bool {
        paths.iter().all(|path| {
            self.paths
                .get(path)
                .is_some_and(|tracked| tracked.expected_state.matches_path(path))
        })
    }

    pub(super) fn consume(&mut self, event_class: SelfNotifyEventClass, paths: &[PathBuf]) {
        for path in paths {
            let should_remove = match self.paths.get_mut(path) {
                Some(tracked) => {
                    tracked.budget.consume(event_class);
                    tracked.budget.is_exhausted()
                }
                None => false,
            };
            if should_remove {
                self.paths.remove(path);
            }
        }
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.paths.is_empty()
            || self
                .paths
                .values()
                .all(|tracked| tracked.budget.is_exhausted())
    }
}

impl TrackedSelfNotifyPath {
    pub(super) fn new(budget: SelfNotifyEventBudget, expected_state: ExpectedPathState) -> Self {
        Self {
            budget,
            expected_state,
        }
    }
}

impl ExpectedPathState {
    pub(super) fn file(bytes: Vec<u8>) -> Self {
        Self::File(bytes)
    }

    pub(super) fn directory(entries: Vec<String>) -> Self {
        Self::Directory(entries)
    }

    pub(super) fn matches_path(&self, path: &Path) -> bool {
        match self {
            Self::Missing => matches!(
                fs::symlink_metadata(path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            ),
            Self::File(expected_bytes) => fs::read(path)
                .map(|actual_bytes| actual_bytes == *expected_bytes)
                .unwrap_or(false),
            Self::Directory(expected_entries) => read_directory_entries(path)
                .map(|actual_entries| actual_entries == *expected_entries)
                .unwrap_or(false),
        }
    }
}

fn expected_created_dir_entries(
    created_dirs: &[PathBuf],
    tracked_paths: &[(PathBuf, TrackedSelfNotifyPath)],
) -> HashMap<PathBuf, Vec<String>> {
    let normalized_created_dirs: Vec<PathBuf> =
        created_dirs.iter().cloned().map(clean_root).collect();
    let present_paths: Vec<PathBuf> = tracked_paths
        .iter()
        .filter_map(|(path, tracked)| match &tracked.expected_state {
            ExpectedPathState::Missing => None,
            ExpectedPathState::File(_) | ExpectedPathState::Directory(_) => {
                Some(clean_root(path.clone()))
            }
        })
        .collect();
    normalized_created_dirs
        .iter()
        .map(|created_dir| {
            let mut entries: Vec<String> = normalized_created_dirs
                .iter()
                .chain(present_paths.iter())
                .filter(|path| *path != created_dir)
                .filter_map(|path| {
                    (path.parent() == Some(created_dir.as_path()))
                        .then(|| path.file_name())
                        .flatten()
                        .map(|name| name.to_string_lossy().into_owned())
                })
                .collect();
            entries.sort();
            entries.dedup();
            (created_dir.clone(), entries)
        })
        .collect()
}

fn read_directory_entries(path: &Path) -> std::io::Result<Vec<String>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        entries.push(entry.file_name().to_string_lossy().into_owned());
    }
    entries.sort();
    Ok(entries)
}

pub(super) fn normalize_paths(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut normalized = Vec::with_capacity(paths.len());
    for path in paths {
        let path = clean_root(path.clone());
        if !normalized.contains(&path) {
            normalized.push(path);
        }
    }
    normalized
}
