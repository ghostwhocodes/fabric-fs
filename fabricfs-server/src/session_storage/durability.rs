use fabricfs_session_protocol::session::{decode_session_message, encode_session_message};
use fabricfs_session_protocol::session_proto as pb;
use prost::{Enumeration, Message};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

use super::{
    build_snapshot, entries_to_map, is_delete_quarantine_session_dir_name,
    is_staging_session_dir_name, SessionError, SessionPaths, SessionState, SessionStore,
    RECOVERY_DIR_NAME,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingDirectorySync {
    SessionDir,
    SessionsRoot,
}

impl PendingDirectorySync {
    pub(super) fn label(self) -> &'static str {
        match self {
            PendingDirectorySync::SessionDir => "session_dir",
            PendingDirectorySync::SessionsRoot => "sessions_root",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingDeleteStage {
    LiveRename,
    DirectoryRemoval,
    SessionsRootSync,
}

impl PendingDeleteStage {
    pub(super) fn label(self) -> &'static str {
        match self {
            PendingDeleteStage::LiveRename => "live_rename",
            PendingDeleteStage::DirectoryRemoval => "directory_removal",
            PendingDeleteStage::SessionsRootSync => "sessions_root",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionDurability {
    Clean,
    PendingSync(PendingDirectorySync),
    PendingDelete(PendingDeleteStage),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
enum RecoveryDurability {
    SessionDirSync = 0,
    SessionsRootSync = 1,
    DeleteDirectoryRemoval = 2,
    DeleteSessionsRootSync = 3,
    DeleteLiveRename = 4,
}

impl RecoveryDurability {
    pub(super) fn from_session_durability(durability: SessionDurability) -> Option<Self> {
        match durability {
            SessionDurability::Clean => None,
            SessionDurability::PendingSync(PendingDirectorySync::SessionDir) => {
                Some(RecoveryDurability::SessionDirSync)
            }
            SessionDurability::PendingSync(PendingDirectorySync::SessionsRoot) => {
                Some(RecoveryDurability::SessionsRootSync)
            }
            SessionDurability::PendingDelete(PendingDeleteStage::LiveRename) => {
                Some(RecoveryDurability::DeleteLiveRename)
            }
            SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval) => {
                Some(RecoveryDurability::DeleteDirectoryRemoval)
            }
            SessionDurability::PendingDelete(PendingDeleteStage::SessionsRootSync) => {
                Some(RecoveryDurability::DeleteSessionsRootSync)
            }
        }
    }

    pub(super) fn into_session_durability(self) -> SessionDurability {
        match self {
            RecoveryDurability::SessionDirSync => {
                SessionDurability::PendingSync(PendingDirectorySync::SessionDir)
            }
            RecoveryDurability::SessionsRootSync => {
                SessionDurability::PendingSync(PendingDirectorySync::SessionsRoot)
            }
            RecoveryDurability::DeleteLiveRename => {
                SessionDurability::PendingDelete(PendingDeleteStage::LiveRename)
            }
            RecoveryDurability::DeleteDirectoryRemoval => {
                SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval)
            }
            RecoveryDurability::DeleteSessionsRootSync => {
                SessionDurability::PendingDelete(PendingDeleteStage::SessionsRootSync)
            }
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SessionRecoveryRecord {
    #[prost(message, optional, tag = "1")]
    metadata: Option<pb::SessionMetadata>,
    #[prost(message, optional, tag = "2")]
    password: Option<pb::PasswordRecord>,
    #[prost(message, repeated, tag = "3")]
    entries: Vec<pb::OverlayEntry>,
    #[prost(enumeration = "RecoveryDurability", tag = "4")]
    durability: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionAccessOutcome {
    Accessible,
    Deleted,
}

#[derive(Debug)]
pub(super) struct LoadedSessionState {
    state: SessionState,
    password_file_missing: bool,
    password_status_mismatch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeleteAction {
    AlreadyDeleted,
    RenameLiveDirectory,
    RemoveQuarantineDirectory,
    SyncRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeleteBoundaryState {
    PreLive,
    Quarantined,
    Removed,
    Ambiguous,
}

#[derive(Debug)]
pub(super) enum PersistenceOutcome {
    Durable,
    RenamedNeedsDirectorySync(io::Error),
}

#[derive(Debug)]
pub(super) enum PreparedWriteCommitOutcome {
    Applied(Result<(), SessionError>),
    Aborted(SessionError),
}

#[derive(Debug)]
pub(super) struct PreparedAtomicWrite {
    pub(super) staged_path: PathBuf,
    pub(super) destination_path: PathBuf,
}

pub(super) trait SessionDisk: Send + Sync + std::fmt::Debug {
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
    fn path_exists(&self, path: &Path) -> bool;
    fn stage_atomic_write(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<PreparedAtomicWrite, SessionError>;
    fn commit_atomic_write(
        &self,
        prepared: PreparedAtomicWrite,
    ) -> Result<PersistenceOutcome, SessionError>;
    fn atomic_write(&self, path: &Path, data: &[u8]) -> Result<PersistenceOutcome, SessionError> {
        let prepared = self.stage_atomic_write(path, data)?;
        self.commit_atomic_write(prepared)
    }
}

#[derive(Debug)]
pub(super) struct OsSessionDisk;

impl SessionDisk for OsSessionDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        sync_directory(path)
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn stage_atomic_write(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<PreparedAtomicWrite, SessionError> {
        stage_atomic_write(path, data)
    }

    fn commit_atomic_write(
        &self,
        prepared: PreparedAtomicWrite,
    ) -> Result<PersistenceOutcome, SessionError> {
        commit_atomic_write(prepared)
    }
}

impl SessionStore {
    pub(super) fn pending_sync_path<'a>(
        &'a self,
        paths: &'a SessionPaths,
        pending: PendingDirectorySync,
    ) -> &'a Path {
        match pending {
            PendingDirectorySync::SessionDir => &paths.session_dir,
            PendingDirectorySync::SessionsRoot => &self.layout.sessions_root,
        }
    }

    pub(super) fn delete_boundary_state(&self, session_id: &str) -> DeleteBoundaryState {
        let live_paths = self.layout.paths(session_id);
        let deleting_paths = self.layout.deleting_paths(session_id);
        match (
            self.disk.path_exists(&live_paths.session_dir),
            self.disk.path_exists(&deleting_paths.session_dir),
        ) {
            (true, false) => DeleteBoundaryState::PreLive,
            (false, true) => DeleteBoundaryState::Quarantined,
            (false, false) => DeleteBoundaryState::Removed,
            (true, true) => DeleteBoundaryState::Ambiguous,
        }
    }

    pub(super) fn arm_pre_live_recovery_state(
        &self,
        state: &mut SessionState,
        next: SessionDurability,
    ) -> Result<(), SessionError> {
        let previous = state.durability;
        state.durability = next;
        if let Err(err) = self.persist_recovery_state(state) {
            state.durability = previous;
            let _ = self.clear_recovery_state(&state.metadata.session_id);
            return Err(err);
        }
        Ok(())
    }

    pub(super) fn arm_pending_sync_recovery(
        &self,
        state: &mut SessionState,
        pending: PendingDirectorySync,
    ) -> Result<(), SessionError> {
        self.arm_pre_live_recovery_state(state, SessionDurability::PendingSync(pending))
    }

    pub(super) fn arm_pending_delete_recovery(
        &self,
        state: &mut SessionState,
        pending: PendingDeleteStage,
    ) -> Result<(), SessionError> {
        self.arm_pre_live_recovery_state(state, SessionDurability::PendingDelete(pending))
    }

    pub(super) fn prepare_live_file_with_recovery(
        &self,
        state: &mut SessionState,
        path: &Path,
        encoded: &[u8],
        pending: PendingDirectorySync,
    ) -> Result<PreparedAtomicWrite, SessionError> {
        let prepared = self.disk.stage_atomic_write(path, encoded)?;
        if let Err(err) = self.arm_pending_sync_recovery(state, pending) {
            if !self
                .layout
                .recovery_paths(&state.metadata.session_id)
                .record_file
                .exists()
            {
                let _ = self.disk.remove_file(&prepared.staged_path);
            }
            return Err(err);
        }
        Ok(prepared)
    }

    pub(super) fn commit_prepared_live_file(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
        pending: PendingDirectorySync,
        operation: &'static str,
        prepared: PreparedAtomicWrite,
    ) -> PreparedWriteCommitOutcome {
        match self.disk.commit_atomic_write(prepared) {
            Ok(outcome) => PreparedWriteCommitOutcome::Applied(
                self.settle_live_persistence(state, paths, pending, operation, outcome),
            ),
            Err(err) => {
                tracing::warn!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    error = ?err,
                    "session persistence failed before the live boundary"
                );
                PreparedWriteCommitOutcome::Aborted(
                    self.abort_pre_live_pending_sync(state, pending, operation, err),
                )
            }
        }
    }

    pub(super) fn abort_pre_live_pending_sync(
        &self,
        state: &mut SessionState,
        pending: PendingDirectorySync,
        operation: &'static str,
        error: SessionError,
    ) -> SessionError {
        match self.discard_pre_live_recovery_artifacts(
            &state.metadata.session_id,
            SessionDurability::PendingSync(pending),
        ) {
            Ok(()) => {
                state.durability = SessionDurability::Clean;
                tracing::info!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    "discarded pre-live recovery state after an uncommitted write failure"
                );
                error
            }
            Err(cleanup_err) => {
                tracing::warn!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    original_error = ?error,
                    cleanup_error = ?cleanup_err,
                    "failed to discard pre-live recovery state after an uncommitted write failure"
                );
                cleanup_err
            }
        }
    }

    pub(super) fn abort_pre_live_pending_delete(
        &self,
        state: &mut SessionState,
        pending: PendingDeleteStage,
        operation: &'static str,
        error: SessionError,
    ) -> SessionError {
        match self.discard_pre_live_recovery_artifacts(
            &state.metadata.session_id,
            SessionDurability::PendingDelete(pending),
        ) {
            Ok(()) => {
                state.durability = SessionDurability::Clean;
                tracing::info!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    "discarded pre-live delete recovery state after the live rename never committed"
                );
                error
            }
            Err(cleanup_err) => {
                tracing::warn!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    original_error = ?error,
                    cleanup_error = ?cleanup_err,
                    "failed to discard pre-live delete recovery state after the live rename never committed"
                );
                cleanup_err
            }
        }
    }

    pub(super) fn rename_session_into_delete_quarantine(
        &self,
        state: &mut SessionState,
    ) -> Result<(), SessionError> {
        let live_paths = self.layout.paths(&state.metadata.session_id);
        let deleting_paths = self.layout.deleting_paths(&state.metadata.session_id);
        if deleting_paths.session_dir.exists() {
            return Err(SessionError::InvalidRequest(format!(
                "session {} delete quarantine already exists",
                state.metadata.session_id
            )));
        }
        match self
            .disk
            .rename(&live_paths.session_dir, &deleting_paths.session_dir)
        {
            Ok(()) => {
                state.durability =
                    SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval);
                Ok(())
            }
            Err(err) => Err(self.abort_pre_live_pending_delete(
                state,
                PendingDeleteStage::LiveRename,
                "delete_session",
                SessionError::Io(err),
            )),
        }
    }

    pub(super) fn persist_session_state_with_recovery(
        &self,
        paths: &SessionPaths,
        state: &mut SessionState,
        pending: PendingDirectorySync,
        operation: &'static str,
    ) -> Result<(), SessionError> {
        let snapshot = build_snapshot(state);
        let encoded = encode_session_message(&snapshot)?;
        let prepared =
            self.prepare_live_file_with_recovery(state, &paths.overlay_file, &encoded, pending)?;
        match self.commit_prepared_live_file(state, paths, pending, operation, prepared) {
            PreparedWriteCommitOutcome::Applied(result) => result,
            PreparedWriteCommitOutcome::Aborted(err) => Err(err),
        }
    }

    pub(super) fn persist_password_with_recovery(
        &self,
        paths: &SessionPaths,
        state: &mut SessionState,
        pending: PendingDirectorySync,
        operation: &'static str,
    ) -> Result<(), SessionError> {
        let encoded = encode_session_message(&state.password)?;
        let prepared =
            self.prepare_live_file_with_recovery(state, &paths.password_file, &encoded, pending)?;
        match self.commit_prepared_live_file(state, paths, pending, operation, prepared) {
            PreparedWriteCommitOutcome::Applied(result) => result,
            PreparedWriteCommitOutcome::Aborted(err) => Err(err),
        }
    }

    pub(super) fn commit_live_overlay_update(
        &self,
        current_state: &mut SessionState,
        paths: &SessionPaths,
        mut next_state: SessionState,
        operation: &'static str,
    ) -> Result<(), SessionError> {
        let snapshot = build_snapshot(&next_state);
        let encoded = encode_session_message(&snapshot)?;
        let prepared = self.prepare_live_file_with_recovery(
            &mut next_state,
            &paths.overlay_file,
            &encoded,
            PendingDirectorySync::SessionDir,
        )?;
        match self.commit_prepared_live_file(
            &mut next_state,
            paths,
            PendingDirectorySync::SessionDir,
            operation,
            prepared,
        ) {
            PreparedWriteCommitOutcome::Applied(result) => {
                *current_state = next_state;
                result
            }
            PreparedWriteCommitOutcome::Aborted(err) => Err(err),
        }
    }

    pub(super) fn persist_recovery_state(&self, state: &SessionState) -> Result<(), SessionError> {
        let Some(durability) = RecoveryDurability::from_session_durability(state.durability) else {
            return self.clear_recovery_state(&state.metadata.session_id);
        };
        let record = SessionRecoveryRecord {
            metadata: Some(state.metadata.clone()),
            password: Some(state.password.clone()),
            entries: state.overlay.values().cloned().collect(),
            durability: durability as i32,
        };
        let recovery_paths = self.layout.recovery_paths(&state.metadata.session_id);
        require_durable_persistence(
            self.disk
                .atomic_write(&recovery_paths.record_file, &record.encode_to_vec())?,
        )
    }

    pub(super) fn clear_recovery_state(&self, session_id: &str) -> Result<(), SessionError> {
        let recovery_paths = self.layout.recovery_paths(session_id);
        match self.disk.remove_file(&recovery_paths.record_file) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(SessionError::Io(err)),
        }
        self.disk
            .sync_dir(&self.layout.recovery_root)
            .map_err(SessionError::Io)
    }

    pub(super) fn recovery_record_precedes_live_boundary(
        &self,
        session_id: &str,
        durability: SessionDurability,
    ) -> bool {
        match durability {
            SessionDurability::PendingSync(PendingDirectorySync::SessionDir) => {
                let paths = self.layout.paths(session_id);
                staged_atomic_write_paths(&paths.overlay_file)
                    .staged_path
                    .exists()
                    || staged_atomic_write_paths(&paths.password_file)
                        .staged_path
                        .exists()
            }
            SessionDurability::PendingSync(PendingDirectorySync::SessionsRoot) => {
                self.layout.staging_paths(session_id).session_dir.exists()
            }
            SessionDurability::PendingDelete(PendingDeleteStage::LiveRename)
            | SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval) => {
                matches!(
                    self.delete_boundary_state(session_id),
                    DeleteBoundaryState::PreLive
                )
            }
            SessionDurability::Clean
            | SessionDurability::PendingDelete(PendingDeleteStage::SessionsRootSync) => false,
        }
    }

    pub(super) fn discard_pre_live_recovery_artifacts(
        &self,
        session_id: &str,
        durability: SessionDurability,
    ) -> Result<(), SessionError> {
        match durability {
            SessionDurability::PendingSync(PendingDirectorySync::SessionDir) => {
                let paths = self.layout.paths(session_id);
                for staged_path in [
                    staged_atomic_write_paths(&paths.overlay_file).staged_path,
                    staged_atomic_write_paths(&paths.password_file).staged_path,
                ] {
                    match self.disk.remove_file(&staged_path) {
                        Ok(()) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(SessionError::Io(err)),
                    }
                }
            }
            SessionDurability::PendingSync(PendingDirectorySync::SessionsRoot) => {
                let staging_paths = self.layout.staging_paths(session_id);
                match self.disk.remove_dir_all(&staging_paths.session_dir) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(SessionError::Io(err)),
                }
            }
            SessionDurability::Clean | SessionDurability::PendingDelete(_) => {}
        }

        self.clear_recovery_state(session_id)
    }

    pub(super) fn load_recovery_sessions(
        &self,
    ) -> Result<HashMap<String, SessionState>, SessionError> {
        let mut sessions = HashMap::new();
        for entry in fs::read_dir(&self.layout.recovery_root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some("pb") {
                continue;
            }
            let Some(session_id) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            self.validate_session_id(session_id)?;
            let state = self.load_recovery_state(&path, session_id)?;
            if self.recovery_record_precedes_live_boundary(session_id, state.durability) {
                tracing::warn!(
                    session_id,
                    pending = ?state.durability,
                    "discarding a recovery journal that never crossed the live boundary"
                );
                self.discard_pre_live_recovery_artifacts(session_id, state.durability)?;
                continue;
            }
            sessions.insert(session_id.to_string(), state);
        }
        Ok(sessions)
    }

    pub(super) fn load_recovery_state(
        &self,
        path: &Path,
        session_id: &str,
    ) -> Result<SessionState, SessionError> {
        let bytes = fs::read(path)?;
        let record = SessionRecoveryRecord::decode(bytes.as_slice()).map_err(|error| {
            SessionError::InvalidRequest(format!(
                "failed to decode session recovery record for {session_id}: {error}"
            ))
        })?;
        let mut metadata = record.metadata.ok_or_else(|| {
            SessionError::InvalidRequest(format!(
                "session recovery record missing metadata for {session_id}"
            ))
        })?;
        self.validate_loaded_session_id(session_id, &metadata)?;
        let password = match record.password {
            Some(password) => password,
            None if metadata
                .password
                .as_ref()
                .is_some_and(|status| status.is_protected) =>
            {
                return Err(SessionError::InvalidRequest(format!(
                    "password record missing for protected session recovery {session_id}"
                )))
            }
            None => self.passwords.unprotected_record(),
        };
        metadata.password = Some(self.passwords.status(&password));
        let durability = RecoveryDurability::try_from(record.durability).map_err(|_| {
            SessionError::InvalidRequest(format!(
                "session recovery record has unknown durability value {} for {session_id}",
                record.durability
            ))
        })?;
        Ok(SessionState {
            metadata,
            password,
            overlay: entries_to_map(&record.entries),
            durability: durability.into_session_durability(),
        })
    }

    pub(super) fn load_live_session_state(
        &self,
        paths: &SessionPaths,
        session_id: &str,
    ) -> Result<LoadedSessionState, SessionError> {
        let (mut metadata, overlay) = self.load_persisted_overlay_state(paths, session_id)?;
        self.validate_loaded_session_id(session_id, &metadata)?;
        let password_file_missing = !paths.password_file.exists();
        let password = if !password_file_missing {
            let bytes = fs::read(&paths.password_file)?;
            decode_session_message(&bytes)?
        } else {
            if metadata
                .password
                .as_ref()
                .map(|status| status.is_protected)
                .unwrap_or(false)
            {
                return Err(SessionError::InvalidRequest(format!(
                    "password record missing for protected session {session_id}"
                )));
            }
            self.passwords.unprotected_record()
        };
        let status = self.passwords.status(&password);
        let password_status_mismatch = metadata.password.as_ref() != Some(&status);
        metadata.password = Some(status);
        Ok(LoadedSessionState {
            state: SessionState {
                metadata,
                password,
                overlay,
                durability: SessionDurability::Clean,
            },
            password_file_missing,
            password_status_mismatch,
        })
    }

    pub(super) fn recover_pending_sync(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
        pending: PendingDirectorySync,
    ) -> Result<(), SessionError> {
        let recover = match pending {
            PendingDirectorySync::SessionDir => {
                require_durable_persistence(self.persist_password(paths, &state.password)?)?;
                let outcome = self.persist_session_state(paths, state)?;
                self.settle_live_persistence(
                    state,
                    paths,
                    pending,
                    "recover_pending_session_sync",
                    outcome,
                )
            }
            PendingDirectorySync::SessionsRoot => {
                match self.disk.remove_dir_all(&paths.session_dir) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(SessionError::Io(err)),
                }
                let staging_paths = self.layout.staging_paths(&state.metadata.session_id);
                let outcome = self.persist_new_session(paths, &staging_paths, state)?;
                self.settle_live_persistence(
                    state,
                    paths,
                    pending,
                    "recover_pending_session_sync",
                    outcome,
                )
            }
        };

        match recover {
            Ok(()) => {
                if let Err(err) = self.clear_recovery_state(&state.metadata.session_id) {
                    state.durability = SessionDurability::PendingSync(pending);
                    tracing::warn!(
                        session_id = %state.metadata.session_id,
                        pending = pending.label(),
                        error = ?err,
                        "session durability recovered on disk but recovery journal cleanup failed"
                    );
                    return Err(err);
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub(super) fn discard_pre_live_pending_sync_state(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
        pending: PendingDirectorySync,
    ) -> Result<SessionAccessOutcome, SessionError> {
        tracing::warn!(
            session_id = %state.metadata.session_id,
            pending = pending.label(),
            "discarding in-process pending sync state that never crossed the live boundary"
        );
        self.discard_pre_live_recovery_artifacts(
            &state.metadata.session_id,
            SessionDurability::PendingSync(pending),
        )?;
        match pending {
            PendingDirectorySync::SessionDir => {
                let loaded = self.load_live_session_state(paths, &state.metadata.session_id)?;
                *state = loaded.state;
                Ok(SessionAccessOutcome::Accessible)
            }
            PendingDirectorySync::SessionsRoot => Ok(SessionAccessOutcome::Deleted),
        }
    }

    pub(super) fn discard_pre_live_pending_delete_state(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
        pending: PendingDeleteStage,
    ) -> Result<SessionAccessOutcome, SessionError> {
        tracing::warn!(
            session_id = %state.metadata.session_id,
            pending = pending.label(),
            "discarding pending delete state that never moved the live session into delete quarantine"
        );
        self.discard_pre_live_recovery_artifacts(
            &state.metadata.session_id,
            SessionDurability::PendingDelete(pending),
        )?;
        let loaded = self.load_live_session_state(paths, &state.metadata.session_id)?;
        *state = loaded.state;
        Ok(SessionAccessOutcome::Accessible)
    }

    pub(super) fn reconcile_session_durability(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
    ) -> Result<SessionAccessOutcome, SessionError> {
        match state.durability {
            SessionDurability::Clean => Ok(SessionAccessOutcome::Accessible),
            SessionDurability::PendingSync(pending) => {
                if self.recovery_record_precedes_live_boundary(
                    &state.metadata.session_id,
                    state.durability,
                ) {
                    return self.discard_pre_live_pending_sync_state(state, paths, pending);
                }
                match self.recover_pending_sync(state, paths, pending) {
                    Ok(()) => {
                        tracing::info!(
                            session_id = %state.metadata.session_id,
                            pending = pending.label(),
                            "recovered pending session durability state from the recovery journal"
                        );
                        Ok(SessionAccessOutcome::Accessible)
                    }
                    Err(err) => {
                        tracing::warn!(
                            session_id = %state.metadata.session_id,
                            pending = pending.label(),
                            error = ?err,
                            "session durability recovery sync failed"
                        );
                        Err(err)
                    }
                }
            }
            SessionDurability::PendingDelete(pending) => match pending {
                PendingDeleteStage::LiveRename | PendingDeleteStage::DirectoryRemoval => {
                    // Observe the mutable delete boundary exactly once for this
                    // reconciliation pass so out-of-band filesystem changes
                    // cannot turn a retry path into a panic.
                    match self.delete_boundary_state(&state.metadata.session_id) {
                        DeleteBoundaryState::PreLive => {
                            self.discard_pre_live_pending_delete_state(state, paths, pending)
                        }
                        DeleteBoundaryState::Quarantined => {
                            tracing::warn!(
                                session_id = %state.metadata.session_id,
                                pending = pending.label(),
                                "session delete remains quarantined until quarantine-directory removal retry succeeds"
                            );
                            Err(SessionError::Io(io::Error::other(format!(
                                "session {} delete remains pending retry after the live path moved into delete quarantine",
                                state.metadata.session_id
                            ))))
                        }
                        DeleteBoundaryState::Removed => {
                            tracing::warn!(
                                session_id = %state.metadata.session_id,
                                pending = pending.label(),
                                "session delete finished removing the quarantine directory but still needs the final parent sync"
                            );
                            Err(SessionError::Io(io::Error::other(format!(
                                "session {} delete remains pending final root sync after the quarantine directory was removed",
                                state.metadata.session_id
                            ))))
                        }
                        DeleteBoundaryState::Ambiguous => {
                            Err(SessionError::InvalidRequest(format!(
                                "session {} has both live and delete-quarantine directories present",
                                state.metadata.session_id
                            )))
                        }
                    }
                }
                PendingDeleteStage::SessionsRootSync => {
                    match self.disk.sync_dir(&self.layout.sessions_root) {
                        Ok(()) => {
                            if let Err(err) = self.clear_recovery_state(&state.metadata.session_id)
                            {
                                tracing::warn!(
                                    session_id = %state.metadata.session_id,
                                    pending = pending.label(),
                                    error = ?err,
                                    "session delete root-sync recovered but recovery journal cleanup failed"
                                );
                                return Err(err);
                            }
                            tracing::info!(
                                session_id = %state.metadata.session_id,
                                pending = pending.label(),
                                "recovered pending session delete sync"
                            );
                            Ok(SessionAccessOutcome::Deleted)
                        }
                        Err(err) => {
                            tracing::warn!(
                                session_id = %state.metadata.session_id,
                                pending = pending.label(),
                                error = ?err,
                                "session delete recovery sync failed"
                            );
                            Err(SessionError::Io(err))
                        }
                    }
                }
            },
        }
    }

    pub(super) fn reconcile_all_session_durability(&self) -> Result<(), SessionError> {
        let mut sessions = self.write_sessions()?;
        let session_ids = sessions.keys().cloned().collect::<Vec<_>>();
        for session_id in session_ids {
            let paths = self.layout.paths(&session_id);
            let outcome = {
                let Some(state) = sessions.get_mut(&session_id) else {
                    continue;
                };
                self.reconcile_session_durability(state, &paths)?
            };
            if matches!(outcome, SessionAccessOutcome::Deleted) {
                sessions.remove(&session_id);
            }
        }
        Ok(())
    }

    pub(super) fn reconcile_session_entry(
        &self,
        sessions: &mut HashMap<String, SessionState>,
        session_id: &str,
    ) -> Result<SessionAccessOutcome, SessionError> {
        let paths = self.layout.paths(session_id);
        let outcome = {
            let state = sessions
                .get_mut(session_id)
                .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
            self.reconcile_session_durability(state, &paths)?
        };
        if matches!(outcome, SessionAccessOutcome::Deleted) {
            sessions.remove(session_id);
        }
        Ok(outcome)
    }

    pub(super) fn authorize_session_access<'a>(
        &self,
        sessions: &'a mut HashMap<String, SessionState>,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<(&'a mut SessionState, SessionPaths), SessionError> {
        let paths = self.layout.paths(session_id);
        if matches!(
            self.reconcile_session_entry(sessions, session_id)?,
            SessionAccessOutcome::Deleted
        ) {
            return Err(SessionError::NotFound(session_id.to_string()));
        }
        let state = sessions
            .get_mut(session_id)
            .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
        self.enforce_password(state, password, &paths)?;
        Ok((state, paths))
    }

    pub(super) fn enforce_password(
        &self,
        state: &mut SessionState,
        provided: Option<&pb::SessionPassword>,
        paths: &SessionPaths,
    ) -> Result<(), SessionError> {
        if state.metadata.password.is_none() {
            state.metadata.password = Some(self.passwords.status(&state.password));
            self.persist_session_state_with_recovery(
                paths,
                state,
                PendingDirectorySync::SessionDir,
                "enforce_password",
            )?;
        }
        self.passwords
            .verify(&state.metadata.session_id, provided, &state.password)?;
        Ok(())
    }

    pub(super) fn authorize_session_delete(
        &self,
        sessions: &mut HashMap<String, SessionState>,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<DeleteAction, SessionError> {
        let paths = self.layout.paths(session_id);
        let durability = sessions
            .get(session_id)
            .map(|state| state.durability)
            .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
        match durability {
            SessionDurability::PendingDelete(PendingDeleteStage::LiveRename)
            | SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval) => {
                let state = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
                match self.delete_boundary_state(session_id) {
                    DeleteBoundaryState::PreLive => {
                        self.enforce_password(state, password, &paths)?;
                        Ok(DeleteAction::RenameLiveDirectory)
                    }
                    DeleteBoundaryState::Quarantined => {
                        // Once the live directory has already moved into delete
                        // quarantine, finishing the retry is recovery of an
                        // already-authorized delete rather than a fresh read of
                        // protected session data.
                        state.durability = SessionDurability::PendingDelete(
                            PendingDeleteStage::DirectoryRemoval,
                        );
                        Ok(DeleteAction::RemoveQuarantineDirectory)
                    }
                    DeleteBoundaryState::Removed => Ok(DeleteAction::SyncRoot),
                    DeleteBoundaryState::Ambiguous => Err(SessionError::InvalidRequest(format!(
                        "session {session_id} has both live and delete-quarantine directories present"
                    ))),
                }
            }
            SessionDurability::PendingDelete(PendingDeleteStage::SessionsRootSync) => {
                Ok(DeleteAction::SyncRoot)
            }
            _ => {
                if matches!(
                    self.reconcile_session_entry(sessions, session_id)?,
                    SessionAccessOutcome::Deleted
                ) {
                    return Ok(DeleteAction::AlreadyDeleted);
                }
                let state = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
                self.enforce_password(state, password, &paths)?;
                self.arm_pending_delete_recovery(state, PendingDeleteStage::LiveRename)?;
                Ok(DeleteAction::RenameLiveDirectory)
            }
        }
    }

    pub(super) fn delete_session_durably(
        &self,
        sessions: &mut HashMap<String, SessionState>,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<(), SessionError> {
        let action = self.authorize_session_delete(sessions, session_id, password)?;
        if matches!(action, DeleteAction::AlreadyDeleted) {
            return Ok(());
        }
        let action = if matches!(action, DeleteAction::RenameLiveDirectory) {
            let state = sessions
                .get_mut(session_id)
                .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
            self.rename_session_into_delete_quarantine(state)?;
            DeleteAction::RemoveQuarantineDirectory
        } else {
            action
        };
        if matches!(action, DeleteAction::RemoveQuarantineDirectory) {
            let deleting_paths = self.layout.deleting_paths(session_id);
            match self.disk.remove_dir_all(&deleting_paths.session_dir) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    let state = sessions
                        .get_mut(session_id)
                        .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
                    state.durability =
                        SessionDurability::PendingDelete(PendingDeleteStage::DirectoryRemoval);
                    tracing::warn!(
                        session_id,
                        pending = PendingDeleteStage::DirectoryRemoval.label(),
                        error = ?err,
                        "session delete remains quarantined until the delete-quarantine directory can be removed"
                    );
                    return Err(SessionError::Io(err));
                }
            }
        }
        self.finalize_deleted_session(sessions, session_id)
    }

    pub(super) fn finalize_deleted_session(
        &self,
        sessions: &mut HashMap<String, SessionState>,
        session_id: &str,
    ) -> Result<(), SessionError> {
        match self.disk.sync_dir(&self.layout.sessions_root) {
            Ok(()) => {
                self.clear_recovery_state(session_id)?;
                sessions.remove(session_id);
                Ok(())
            }
            Err(err) => {
                let state = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| SessionError::NotFound(session_id.to_string()))?;
                state.durability =
                    SessionDurability::PendingDelete(PendingDeleteStage::SessionsRootSync);
                self.persist_recovery_state(state)?;
                tracing::warn!(
                    session_id,
                    pending = PendingDeleteStage::SessionsRootSync.label(),
                    error = ?err,
                    "session delete reached the live path before the final directory sync"
                );
                Err(SessionError::Io(err))
            }
        }
    }

    pub(super) fn settle_live_persistence(
        &self,
        state: &mut SessionState,
        paths: &SessionPaths,
        pending: PendingDirectorySync,
        operation: &'static str,
        outcome: PersistenceOutcome,
    ) -> Result<(), SessionError> {
        let complete_success = |state: &mut SessionState| -> Result<(), SessionError> {
            if matches!(state.durability, SessionDurability::PendingSync(_)) {
                self.clear_recovery_state(&state.metadata.session_id)?;
            }
            state.durability = SessionDurability::Clean;
            Ok(())
        };

        match outcome {
            PersistenceOutcome::Durable => complete_success(state),
            PersistenceOutcome::RenamedNeedsDirectorySync(err) => {
                tracing::warn!(
                    session_id = %state.metadata.session_id,
                    pending = pending.label(),
                    operation,
                    error = ?err,
                    "session persistence reached the live path before the final directory sync"
                );
                let sync_path = self.pending_sync_path(paths, pending);
                match self.disk.sync_dir(sync_path) {
                    Ok(()) => {
                        tracing::info!(
                            session_id = %state.metadata.session_id,
                            pending = pending.label(),
                            operation,
                            "session durability recovered after immediate sync retry"
                        );
                        complete_success(state)
                    }
                    Err(retry_err) => {
                        if !matches!(
                            state.durability,
                            SessionDurability::PendingSync(existing) if existing == pending
                        ) {
                            state.durability = SessionDurability::PendingSync(pending);
                            self.persist_recovery_state(state)?;
                        }
                        tracing::warn!(
                            session_id = %state.metadata.session_id,
                            pending = pending.label(),
                            operation,
                            error = ?retry_err,
                            "session durability remains pending sync recovery"
                        );
                        Err(SessionError::Io(retry_err))
                    }
                }
            }
        }
    }
}

impl SessionStore {
    pub(super) fn load_existing_sessions(
        &self,
    ) -> Result<HashMap<String, SessionState>, SessionError> {
        let mut sessions = self.load_recovery_sessions()?;
        for entry in fs::read_dir(&self.layout.sessions_root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let session_id = match path.file_name().and_then(|n| n.to_str()) {
                Some(RECOVERY_DIR_NAME) => continue,
                Some(id)
                    if is_staging_session_dir_name(id)
                        || is_delete_quarantine_session_dir_name(id) =>
                {
                    continue;
                }
                Some(id) => id,
                None => continue,
            };
            self.validate_session_id(session_id)?;
            let session_id = session_id.to_string();
            if sessions.contains_key(&session_id) {
                continue;
            }
            let paths = self.layout.paths(&session_id);
            if !paths.overlay_file.exists() && !paths.metadata_file.exists() {
                continue;
            }
            let mut loaded = self.load_live_session_state(&paths, &session_id)?;
            if loaded.password_file_missing {
                let _ = self.persist_password_with_recovery(
                    &paths,
                    &mut loaded.state,
                    PendingDirectorySync::SessionDir,
                    "load_missing_password_record",
                );
            }
            if loaded.password_status_mismatch {
                let _ = self.persist_session_state_with_recovery(
                    &paths,
                    &mut loaded.state,
                    PendingDirectorySync::SessionDir,
                    "load_password_status",
                );
            }
            sessions.insert(session_id, loaded.state);
        }
        Ok(sessions)
    }
}

impl SessionStore {
    pub(super) fn insert_new_session(
        &self,
        state: SessionState,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let session_id = state.metadata.session_id.clone();
        self.validate_session_id(&session_id)?;
        let mut state = state;
        let metadata = state.metadata.clone();
        let paths = self.layout.paths(&session_id);
        let staging_paths = self.layout.staging_paths(&session_id);
        let mut sessions = self.write_sessions()?;
        if sessions.contains_key(&session_id)
            || paths.overlay_file.exists()
            || paths.metadata_file.exists()
            || self.layout.deleting_paths(&session_id).session_dir.exists()
            || self.layout.recovery_paths(&session_id).record_file.exists()
        {
            return Err(SessionError::Conflict(format!(
                "session {session_id} already exists"
            )));
        }
        if paths.session_dir.exists() {
            self.disk.remove_dir_all(&paths.session_dir)?;
        }
        self.stage_new_session(&staging_paths, &state)?;
        if let Err(err) =
            self.arm_pending_sync_recovery(&mut state, PendingDirectorySync::SessionsRoot)
        {
            if !self
                .layout
                .recovery_paths(&metadata.session_id)
                .record_file
                .exists()
            {
                let _ = self.disk.remove_dir_all(&staging_paths.session_dir);
            }
            return Err(err);
        }
        let outcome = match self.commit_new_session(&paths, &staging_paths) {
            Ok(outcome) => outcome,
            Err(err) => {
                return Err(self.abort_pre_live_pending_sync(
                    &mut state,
                    PendingDirectorySync::SessionsRoot,
                    "insert_new_session",
                    err,
                ));
            }
        };
        let settle = self.settle_live_persistence(
            &mut state,
            &paths,
            PendingDirectorySync::SessionsRoot,
            "insert_new_session",
            outcome,
        );
        sessions.insert(metadata.session_id.clone(), state);
        settle?;
        Ok(metadata)
    }
}

impl SessionStore {
    pub(super) fn persist_new_session(
        &self,
        paths: &SessionPaths,
        staging_paths: &SessionPaths,
        state: &SessionState,
    ) -> Result<PersistenceOutcome, SessionError> {
        self.stage_new_session(staging_paths, state)?;
        self.commit_new_session(paths, staging_paths)
    }

    pub(super) fn stage_new_session(
        &self,
        staging_paths: &SessionPaths,
        state: &SessionState,
    ) -> Result<(), SessionError> {
        if staging_paths.session_dir.exists() {
            self.disk.remove_dir_all(&staging_paths.session_dir)?;
        }
        self.disk.create_dir_all(&staging_paths.checkpoints_dir)?;
        let result = (|| {
            require_durable_persistence(self.persist_password(staging_paths, &state.password)?)?;
            require_durable_persistence(self.persist_session_state(staging_paths, state)?)?;
            Ok(())
        })();

        if let Err(err) = result {
            let _ = self.disk.remove_dir_all(&staging_paths.session_dir);
            return Err(err);
        }
        Ok(())
    }

    pub(super) fn commit_new_session(
        &self,
        paths: &SessionPaths,
        staging_paths: &SessionPaths,
    ) -> Result<PersistenceOutcome, SessionError> {
        self.disk
            .rename(&staging_paths.session_dir, &paths.session_dir)
            .map_err(SessionError::Io)?;
        match self.disk.sync_dir(&self.layout.sessions_root) {
            Ok(()) => Ok(PersistenceOutcome::Durable),
            Err(err) => Ok(PersistenceOutcome::RenamedNeedsDirectorySync(err)),
        }
    }

    pub(super) fn persist_session_state(
        &self,
        paths: &SessionPaths,
        state: &SessionState,
    ) -> Result<PersistenceOutcome, SessionError> {
        let snapshot = build_snapshot(state);
        let encoded = encode_session_message(&snapshot)?;
        self.disk.atomic_write(&paths.overlay_file, &encoded)
    }

    pub(super) fn persist_password(
        &self,
        paths: &SessionPaths,
        record: &pb::PasswordRecord,
    ) -> Result<PersistenceOutcome, SessionError> {
        let encoded = encode_session_message(record)?;
        self.disk.atomic_write(&paths.password_file, &encoded)
    }
}

pub(super) trait AtomicWriteOps {
    type FileHandle;
    type DirHandle;

    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn create_file(&self, path: &Path) -> io::Result<Self::FileHandle>;
    fn write_all(&self, file: &mut Self::FileHandle, data: &[u8]) -> io::Result<()>;
    fn sync_file(&self, file: &mut Self::FileHandle) -> io::Result<()>;
    fn open_dir(&self, path: &Path) -> io::Result<Self::DirHandle>;
    fn sync_dir(&self, dir: &Self::DirHandle) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
}

struct StdAtomicWriteOps;

impl AtomicWriteOps for StdAtomicWriteOps {
    type FileHandle = File;
    type DirHandle = File;

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn create_file(&self, path: &Path) -> io::Result<Self::FileHandle> {
        File::create(path)
    }

    fn write_all(&self, file: &mut Self::FileHandle, data: &[u8]) -> io::Result<()> {
        file.write_all(data)
    }

    fn sync_file(&self, file: &mut Self::FileHandle) -> io::Result<()> {
        file.sync_all()
    }

    fn open_dir(&self, path: &Path) -> io::Result<Self::DirHandle> {
        File::open(path)
    }

    fn sync_dir(&self, dir: &Self::DirHandle) -> io::Result<()> {
        dir.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }
}

pub(super) fn require_durable_persistence(outcome: PersistenceOutcome) -> Result<(), SessionError> {
    match outcome {
        PersistenceOutcome::Durable => Ok(()),
        PersistenceOutcome::RenamedNeedsDirectorySync(err) => Err(SessionError::Io(err)),
    }
}

pub(super) fn staged_atomic_write_paths(path: &Path) -> PreparedAtomicWrite {
    let tmp_name = format!(
        ".{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("tmp")
    );
    let staged_path = path
        .parent()
        .map(|p| p.join(&tmp_name))
        .unwrap_or_else(|| PathBuf::from(&tmp_name));
    PreparedAtomicWrite {
        staged_path,
        destination_path: path.to_path_buf(),
    }
}

pub(super) fn stage_atomic_write(
    path: &Path,
    data: &[u8],
) -> Result<PreparedAtomicWrite, SessionError> {
    stage_atomic_write_with(&StdAtomicWriteOps, path, data)
}

pub(super) fn stage_atomic_write_with<O: AtomicWriteOps>(
    ops: &O,
    path: &Path,
    data: &[u8],
) -> Result<PreparedAtomicWrite, SessionError> {
    if let Some(parent) = path.parent() {
        ops.create_dir_all(parent)?;
    }
    let prepared = staged_atomic_write_paths(path);

    let mut file = ops.create_file(&prepared.staged_path)?;
    ops.write_all(&mut file, data)?;
    ops.sync_file(&mut file)?;
    drop(file);

    if let Some(parent) = path.parent() {
        let dir = ops.open_dir(parent)?;
        ops.sync_dir(&dir)?;
    }

    Ok(prepared)
}

pub(super) fn commit_atomic_write(
    prepared: PreparedAtomicWrite,
) -> Result<PersistenceOutcome, SessionError> {
    commit_atomic_write_with(&StdAtomicWriteOps, prepared)
}

pub(super) fn commit_atomic_write_with<O: AtomicWriteOps>(
    ops: &O,
    prepared: PreparedAtomicWrite,
) -> Result<PersistenceOutcome, SessionError> {
    ops.rename(&prepared.staged_path, &prepared.destination_path)?;
    if let Some(parent) = prepared.destination_path.parent() {
        let dir = ops.open_dir(parent)?;
        match ops.sync_dir(&dir) {
            Ok(()) => {}
            Err(err) => return Ok(PersistenceOutcome::RenamedNeedsDirectorySync(err)),
        }
    }
    Ok(PersistenceOutcome::Durable)
}

#[cfg(test)]
pub(super) fn atomic_write_with<O: AtomicWriteOps>(
    ops: &O,
    path: &Path,
    data: &[u8],
) -> Result<PersistenceOutcome, SessionError> {
    let prepared = stage_atomic_write_with(ops, path, data)?;
    commit_atomic_write_with(ops, prepared)
}

pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()
}
