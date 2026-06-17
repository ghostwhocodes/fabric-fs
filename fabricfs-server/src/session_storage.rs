use argon2::{Algorithm, Argon2, Params, Version};
use fabricfs_session_protocol::session::{decode_session_message, encode_session_message};
use fabricfs_session_protocol::session_proto as pb;
use rand::{rngs::OsRng, RngCore};
use std::collections::HashMap;
use std::fs;
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

const CURRENT_ALGORITHM: &str = "argon2id";
const SALT_LENGTH: usize = 16;
const HASH_LENGTH: usize = 32;
const ARGON2_MEMORY_KIB: u32 = 19456;
const ARGON2_ITERATIONS: u32 = 3;
const ARGON2_PARALLELISM: u32 = 1;
const MAX_PASSWORD_LEN: usize = 1024;
const RECOVERY_DIR_NAME: &str = ".recovery";
const DELETE_QUARANTINE_SUFFIX: &str = ".deleting";

mod durability;

#[cfg(test)]
use durability::{
    atomic_write_with, commit_atomic_write, stage_atomic_write, sync_directory, AtomicWriteOps,
    DeleteAction, PersistenceOutcome, PreparedAtomicWrite,
};
use durability::{require_durable_persistence, OsSessionDisk, SessionDisk, SessionDurability};

#[derive(Debug, Clone)]
struct SessionState {
    metadata: pb::SessionMetadata,
    password: pb::PasswordRecord,
    overlay: HashMap<String, pb::OverlayEntry>,
    durability: SessionDurability,
}

#[derive(Debug)]
struct SessionLayout {
    cow_root: PathBuf,
    sessions_root: PathBuf,
    recovery_root: PathBuf,
}

#[derive(Debug)]
pub struct SessionStore {
    disk: Arc<dyn SessionDisk>,
    layout: Arc<SessionLayout>,
    sessions: Arc<RwLock<HashMap<String, SessionState>>>,
    passwords: PasswordManager,
}

impl Clone for SessionStore {
    fn clone(&self) -> Self {
        SessionStore {
            disk: Arc::clone(&self.disk),
            layout: Arc::clone(&self.layout),
            sessions: Arc::clone(&self.sessions),
            passwords: self.passwords.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("unauthorized for session {0}")]
    Unauthorized(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("overlay version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: i64, found: i64 },
    #[error("password hashing failed: {0}")]
    PasswordHash(String),
    #[error("session state lock poisoned: {0}")]
    StatePoisoned(&'static str),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode error: {0}")]
    Decode(#[from] fabricfs_session_protocol::session::SessionCodecError),
}

impl SessionStore {
    pub fn load(cow_root: PathBuf) -> Result<Self, SessionError> {
        Self::load_with_disk_impl(cow_root, Arc::new(OsSessionDisk))
    }

    fn load_with_disk_impl(
        cow_root: PathBuf,
        disk: Arc<dyn SessionDisk>,
    ) -> Result<Self, SessionError> {
        let layout = SessionLayout::new(cow_root)?;
        let passwords = PasswordManager::new();
        let store = SessionStore {
            disk,
            layout: Arc::new(layout),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            passwords,
        };
        let existing = store.load_existing_sessions()?;
        store.write_sessions()?.extend(existing);
        Ok(store)
    }

    #[cfg(test)]
    fn load_with_disk(cow_root: PathBuf, disk: Arc<dyn SessionDisk>) -> Result<Self, SessionError> {
        Self::load_with_disk_impl(cow_root, disk)
    }

    fn read_sessions(
        &self,
    ) -> Result<RwLockReadGuard<'_, HashMap<String, SessionState>>, SessionError> {
        self.sessions
            .read()
            .map_err(|_| SessionError::StatePoisoned("sessions"))
    }

    fn write_sessions(
        &self,
    ) -> Result<RwLockWriteGuard<'_, HashMap<String, SessionState>>, SessionError> {
        self.sessions
            .write()
            .map_err(|_| SessionError::StatePoisoned("sessions"))
    }

    pub fn cow_root(&self) -> &Path {
        &self.layout.cow_root
    }

    pub fn create_session(
        &self,
        req: &pb::CreateSessionRequest,
    ) -> Result<pb::SessionMetadata, SessionError> {
        self.create_session_with_id(Uuid::new_v4().to_string(), req)
    }

    pub fn list_sessions(&self) -> Result<Vec<pb::SessionMetadata>, SessionError> {
        self.reconcile_all_session_durability()?;
        Ok(self
            .read_sessions()?
            .values()
            .map(|state| state.metadata.clone())
            .collect())
    }

    pub fn init_session(
        &self,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let mut sessions = self.write_sessions()?;
        let (state, _) = self.authorize_session_access(&mut sessions, session_id, password)?;
        Ok(state.metadata.clone())
    }

    pub fn get_snapshot(
        &self,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<pb::SessionSnapshot, SessionError> {
        let mut sessions = self.write_sessions()?;
        let (state, _) = self.authorize_session_access(&mut sessions, session_id, password)?;
        Ok(build_snapshot(state))
    }

    pub fn delete_session(
        &self,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<(), SessionError> {
        let mut sessions = self.write_sessions()?;
        self.delete_session_durably(&mut sessions, session_id, password)
    }

    pub fn update_overlay(&self, req: &pb::UpdateOverlayRequest) -> Result<(), SessionError> {
        let mut sessions = self.write_sessions()?;
        let (state, paths) =
            self.authorize_session_access(&mut sessions, &req.session_id, req.password.as_ref())?;
        let mut next_state = state.clone();

        for path in &req.remove_alias_paths {
            next_state.overlay.remove(path);
        }
        for path in &req.remove_tombstone_paths {
            next_state.overlay.remove(path);
        }

        let now = now_nanos();
        for alias in &req.add_aliases {
            let mut alias = alias.clone();
            if alias.created_at_unix_nanos == 0 {
                alias.created_at_unix_nanos = now;
            }
            next_state.overlay.insert(
                alias.logical_path.clone(),
                pb::OverlayEntry {
                    logical_path: alias.logical_path.clone(),
                    kind: Some(pb::overlay_entry::Kind::Alias(alias)),
                },
            );
        }

        for tombstone in &req.add_tombstones {
            let mut tombstone = tombstone.clone();
            if tombstone.created_at_unix_nanos == 0 {
                tombstone.created_at_unix_nanos = now;
            }
            next_state.overlay.insert(
                tombstone.logical_path.clone(),
                pb::OverlayEntry {
                    logical_path: tombstone.logical_path.clone(),
                    kind: Some(pb::overlay_entry::Kind::Tombstone(tombstone)),
                },
            );
        }

        next_state.metadata.overlay_version = next_state.metadata.overlay_version.saturating_add(1);
        next_state.metadata.updated_at_unix_nanos = now;
        self.commit_live_overlay_update(state, &paths, next_state, "update_overlay")
    }

    pub fn list_overlay(
        &self,
        req: &pb::ListOverlayEntriesRequest,
    ) -> Result<Vec<pb::OverlayEntry>, SessionError> {
        let mut sessions = self.write_sessions()?;
        let (state, _) =
            self.authorize_session_access(&mut sessions, &req.session_id, req.password.as_ref())?;
        let prefix = req.directory_prefix.clone();
        Ok(state
            .overlay
            .values()
            .filter(|entry| prefix.is_empty() || entry.logical_path.starts_with(&prefix))
            .cloned()
            .collect())
    }

    pub fn checkpoint(
        &self,
        req: &pb::CheckpointSessionRequest,
    ) -> Result<pb::CheckpointMetadata, SessionError> {
        let snapshot = {
            let mut sessions = self.write_sessions()?;
            let (state, _) = self.authorize_session_access(
                &mut sessions,
                &req.session_id,
                req.password.as_ref(),
            )?;
            build_snapshot(state)
        };

        let checkpoint_id = Uuid::new_v4().to_string();
        let created_at = now_nanos();
        let metadata = pb::CheckpointMetadata {
            checkpoint_id: checkpoint_id.clone(),
            session_id: req.session_id.clone(),
            label: req.label.clone(),
            created_at_unix_nanos: created_at,
        };

        let paths = self.layout.paths(&req.session_id);
        fs::create_dir_all(&paths.checkpoints_dir)?;
        self.persist_checkpoint(&paths, &checkpoint_id, &snapshot)?;
        self.persist_checkpoint_meta(&paths, &metadata)?;
        Ok(metadata)
    }

    pub fn read_checkpoint(
        &self,
        session_id: &str,
        checkpoint_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<(pb::CheckpointMetadata, pb::SessionSnapshot), SessionError> {
        let paths = {
            let mut sessions = self.write_sessions()?;
            let (_, paths) = self.authorize_session_access(&mut sessions, session_id, password)?;
            paths
        };
        let snapshot_bytes = fs::read(paths.checkpoint_file(checkpoint_id))?;
        let meta_bytes = fs::read(paths.checkpoint_meta_file(checkpoint_id))?;
        let snapshot: pb::SessionSnapshot = decode_session_message(&snapshot_bytes)?;
        let meta: pb::CheckpointMetadata = decode_session_message(&meta_bytes)?;
        Ok((meta, snapshot))
    }

    pub fn create_session_from_snapshot(
        &self,
        snapshot: pb::SessionSnapshot,
        display_name: Option<String>,
        password: Option<pb::SessionPassword>,
    ) -> Result<pb::SessionMetadata, SessionError> {
        self.create_session_from_snapshot_with_id(
            &Uuid::new_v4().to_string(),
            snapshot,
            display_name,
            password,
        )
    }

    pub fn create_session_from_snapshot_with_id(
        &self,
        session_id: &str,
        snapshot: pb::SessionSnapshot,
        display_name: Option<String>,
        password: Option<pb::SessionPassword>,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let meta = snapshot
            .metadata
            .as_ref()
            .ok_or_else(|| SessionError::InvalidRequest("snapshot missing metadata".into()))?;
        self.validate_cow_root(&meta.cow_root, "snapshot cow_root invalid")?;

        let chosen_password = match (password, meta.password.as_ref()) {
            (Some(pw), _) => Some(pw),
            (None, Some(info)) if info.is_protected => {
                return Err(SessionError::InvalidRequest(
                    "protected snapshot import requires password".into(),
                ))
            }
            _ => None,
        };

        let create_req = pb::CreateSessionRequest {
            display_name: display_name.unwrap_or_else(|| meta.display_name.clone()),
            workspace_name: meta.workspace_name.clone(),
            cow_root: self.layout.cow_root.to_string_lossy().to_string(),
            password: chosen_password,
        };
        let mut state = self.build_session_state(session_id.to_string(), &create_req)?;
        state.overlay = entries_to_map(&snapshot.entries);
        state.metadata.overlay_version = snapshot.overlay_version;
        state.metadata.updated_at_unix_nanos = now_nanos();
        self.insert_new_session(state)
    }

    pub fn import_snapshot_into_existing(
        &self,
        target_session_id: &str,
        snapshot: &pb::SessionSnapshot,
        password: Option<&pb::SessionPassword>,
        mode: pb::ImportMode,
        conflict_policy: pb::ConflictPolicy,
        expected_overlay_version: Option<i64>,
    ) -> Result<pb::SessionMetadata, SessionError> {
        let mut sessions = self.write_sessions()?;
        let (state, paths) =
            self.authorize_session_access(&mut sessions, target_session_id, password)?;

        let meta = snapshot
            .metadata
            .as_ref()
            .ok_or_else(|| SessionError::InvalidRequest("snapshot missing metadata".into()))?;
        let cow_root = PathBuf::from(&meta.cow_root)
            .canonicalize()
            .map_err(|_| SessionError::InvalidRequest("snapshot cow_root invalid".into()))?;
        if cow_root != self.layout.cow_root {
            return Err(SessionError::InvalidRequest(
                "snapshot cow_root does not match server".into(),
            ));
        }

        if let Some(expected) = expected_overlay_version {
            if expected >= 0 && expected != state.metadata.overlay_version {
                return Err(SessionError::VersionMismatch {
                    expected,
                    found: state.metadata.overlay_version,
                });
            }
        }

        let mut next_state = state.clone();
        match mode {
            pb::ImportMode::Replace => {
                next_state.overlay = entries_to_map(&snapshot.entries);
                next_state.metadata.overlay_version = std::cmp::max(
                    next_state.metadata.overlay_version + 1,
                    snapshot.overlay_version,
                );
            }
            pb::ImportMode::Merge => {
                let incoming = entries_to_map(&snapshot.entries);
                for (path, entry) in incoming {
                    match next_state.overlay.get(&path) {
                        None => {
                            next_state.overlay.insert(path, entry);
                        }
                        Some(existing) => {
                            if entries_equal(existing, &entry) {
                                continue;
                            }
                            match conflict_policy {
                                pb::ConflictPolicy::Error => {
                                    return Err(SessionError::Conflict(format!(
                                        "overlay entry conflict at {}",
                                        path
                                    )));
                                }
                                pb::ConflictPolicy::KeepLocal => continue,
                                pb::ConflictPolicy::OverwriteRemote => {
                                    next_state.overlay.insert(path, entry);
                                }
                            }
                        }
                    }
                }
                next_state.metadata.overlay_version = std::cmp::max(
                    next_state.metadata.overlay_version,
                    snapshot.overlay_version,
                ) + 1;
            }
        }

        next_state.metadata.updated_at_unix_nanos = now_nanos();
        self.commit_live_overlay_update(
            state,
            &paths,
            next_state,
            "import_snapshot_into_existing",
        )?;
        Ok(state.metadata.clone())
    }

    pub fn list_checkpoints(
        &self,
        session_id: &str,
        password: Option<&pb::SessionPassword>,
    ) -> Result<Vec<pb::CheckpointMetadata>, SessionError> {
        let paths = {
            let mut sessions = self.write_sessions()?;
            let (_, paths) = self.authorize_session_access(&mut sessions, session_id, password)?;
            paths
        };
        if !paths.checkpoints_dir.exists() {
            return Ok(Vec::new());
        }

        let mut metas = Vec::new();
        for entry in fs::read_dir(&paths.checkpoints_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".meta.pb") {
                    let bytes = fs::read(&path)?;
                    let meta: pb::CheckpointMetadata = decode_session_message(&bytes)?;
                    metas.push(meta);
                }
            }
        }
        metas.sort_by_key(|m| m.created_at_unix_nanos);
        Ok(metas)
    }

    fn validate_cow_root(&self, cow_root: &str, invalid_message: &str) -> Result<(), SessionError> {
        if cow_root.is_empty() {
            return Err(SessionError::InvalidRequest("cow_root is required".into()));
        }
        let requested_root = PathBuf::from(cow_root)
            .canonicalize()
            .map_err(|_| SessionError::InvalidRequest(invalid_message.into()))?;
        if requested_root != self.layout.cow_root {
            return Err(SessionError::InvalidRequest(format!(
                "cow_root {} does not match server root {}",
                requested_root.display(),
                self.layout.cow_root.display()
            )));
        }
        Ok(())
    }

    fn validate_session_id(&self, session_id: &str) -> Result<(), SessionError> {
        if is_valid_session_dir_name(session_id) {
            return Ok(());
        }
        Err(SessionError::InvalidRequest(format!(
            "invalid session identifier: {session_id}"
        )))
    }

    fn validate_loaded_session_id(
        &self,
        directory_session_id: &str,
        metadata: &pb::SessionMetadata,
    ) -> Result<(), SessionError> {
        self.validate_session_id(&metadata.session_id)?;
        if metadata.session_id == directory_session_id {
            return Ok(());
        }
        Err(SessionError::InvalidRequest(format!(
            "session directory {directory_session_id} metadata session_id {} mismatch",
            metadata.session_id
        )))
    }

    fn build_session_state(
        &self,
        session_id: String,
        req: &pb::CreateSessionRequest,
    ) -> Result<SessionState, SessionError> {
        let now = now_nanos();
        let password_record = self.passwords.build_record(req.password.as_ref())?;
        let password_status = self.passwords.status(&password_record);
        let metadata = pb::SessionMetadata {
            session_id: session_id.clone(),
            display_name: req.display_name.clone(),
            workspace_name: req.workspace_name.clone(),
            cow_root: self.layout.cow_root.to_string_lossy().to_string(),
            password: Some(password_status),
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            overlay_version: 0,
        };
        Ok(SessionState {
            metadata,
            password: password_record,
            overlay: HashMap::new(),
            durability: SessionDurability::Clean,
        })
    }

    fn create_session_with_id(
        &self,
        session_id: String,
        req: &pb::CreateSessionRequest,
    ) -> Result<pb::SessionMetadata, SessionError> {
        self.validate_session_id(&session_id)?;
        self.validate_cow_root(&req.cow_root, "cow_root must exist")?;
        let state = self.build_session_state(session_id, req)?;
        self.insert_new_session(state)
    }

    fn persist_checkpoint(
        &self,
        paths: &SessionPaths,
        checkpoint_id: &str,
        snapshot: &pb::SessionSnapshot,
    ) -> Result<(), SessionError> {
        let encoded = encode_session_message(snapshot)?;
        let file = paths.checkpoint_file(checkpoint_id);
        require_durable_persistence(self.disk.atomic_write(&file, &encoded)?)?;
        Ok(())
    }

    fn persist_checkpoint_meta(
        &self,
        paths: &SessionPaths,
        metadata: &pb::CheckpointMetadata,
    ) -> Result<(), SessionError> {
        let encoded = encode_session_message(metadata)?;
        let file = paths.checkpoint_meta_file(&metadata.checkpoint_id);
        require_durable_persistence(self.disk.atomic_write(&file, &encoded)?)?;
        Ok(())
    }

    fn load_persisted_overlay_state(
        &self,
        paths: &SessionPaths,
        session_id: &str,
    ) -> Result<(pb::SessionMetadata, HashMap<String, pb::OverlayEntry>), SessionError> {
        if paths.overlay_file.exists() {
            let overlay_bytes = fs::read(&paths.overlay_file)?;
            let snapshot: pb::SessionSnapshot = decode_session_message(&overlay_bytes)?;
            if let Some(metadata) = snapshot.metadata {
                if snapshot.overlay_version != metadata.overlay_version {
                    return Err(SessionError::InvalidRequest(format!(
                        "authoritative overlay snapshot version mismatch for session {session_id}"
                    )));
                }
                return Ok((metadata, entries_to_map(&snapshot.entries)));
            }
            let metadata = self.load_legacy_metadata(paths, session_id)?;
            return Ok((metadata, entries_to_map(&snapshot.entries)));
        }

        let metadata = self.load_legacy_metadata(paths, session_id)?;
        Ok((metadata, HashMap::new()))
    }

    fn load_legacy_metadata(
        &self,
        paths: &SessionPaths,
        session_id: &str,
    ) -> Result<pb::SessionMetadata, SessionError> {
        if !paths.metadata_file.exists() {
            return Err(SessionError::InvalidRequest(format!(
                "legacy session metadata missing for session {session_id}"
            )));
        }
        let metadata_bytes = fs::read(&paths.metadata_file)?;
        decode_session_message(&metadata_bytes).map_err(SessionError::from)
    }
}

impl SessionError {
    pub fn status(&self) -> pb::OperationStatus {
        pb::OperationStatus {
            ok: false,
            message: self.to_string(),
        }
    }
}

impl SessionLayout {
    fn new(cow_root: PathBuf) -> Result<Self, SessionError> {
        let canonical_root = cow_root
            .canonicalize()
            .map_err(|_| SessionError::InvalidRequest("cow_root must exist".into()))?;
        let sessions_root = canonical_root.join(".fabricfs").join("sessions");
        let recovery_root = sessions_root.join(RECOVERY_DIR_NAME);
        fs::create_dir_all(&sessions_root)?;
        fs::create_dir_all(&recovery_root)?;
        Ok(SessionLayout {
            cow_root: canonical_root,
            sessions_root,
            recovery_root,
        })
    }

    fn paths(&self, session_id: &str) -> SessionPaths {
        SessionPaths::new(&self.sessions_root, session_id)
    }

    fn staging_paths(&self, session_id: &str) -> SessionPaths {
        SessionPaths::staging(&self.sessions_root, session_id)
    }

    fn deleting_paths(&self, session_id: &str) -> SessionPaths {
        SessionPaths::deleting(&self.sessions_root, session_id)
    }

    fn recovery_paths(&self, session_id: &str) -> SessionRecoveryPaths {
        SessionRecoveryPaths::new(&self.recovery_root, session_id)
    }
}

#[derive(Debug, Clone)]
struct PasswordManager {
    argon2: Argon2<'static>,
}

impl PasswordManager {
    fn new() -> Self {
        let params = Params::new(
            ARGON2_MEMORY_KIB,
            ARGON2_ITERATIONS,
            ARGON2_PARALLELISM,
            Some(HASH_LENGTH),
        )
        .expect("argon2 params are valid");
        PasswordManager {
            argon2: Argon2::new(Algorithm::Argon2id, Version::V0x13, params),
        }
    }

    fn build_record(
        &self,
        password: Option<&pb::SessionPassword>,
    ) -> Result<pb::PasswordRecord, SessionError> {
        match password {
            None => Ok(self.unprotected_record()),
            Some(pw) if pw.value.is_empty() => Ok(self.unprotected_record()),
            Some(pw) => {
                if pw.value.len() > MAX_PASSWORD_LEN {
                    return Err(SessionError::InvalidRequest("password too long".into()));
                }
                let salt = self.random_salt();
                let hash = self.argon2_hash(&pw.value, &salt)?;
                Ok(pb::PasswordRecord {
                    is_protected: true,
                    algorithm: CURRENT_ALGORITHM.into(),
                    salt,
                    hash,
                })
            }
        }
    }

    fn verify(
        &self,
        session_id: &str,
        provided: Option<&pb::SessionPassword>,
        record: &pb::PasswordRecord,
    ) -> Result<(), SessionError> {
        if !record.is_protected {
            return Ok(());
        }
        let provided =
            provided.ok_or_else(|| SessionError::Unauthorized(session_id.to_string()))?;
        if record.algorithm != CURRENT_ALGORITHM {
            return Err(SessionError::InvalidRequest(format!(
                "unsupported password algorithm: {}",
                record.algorithm
            )));
        }
        self.assert_material(record)?;
        let candidate = self.argon2_hash(&provided.value, &record.salt)?;
        if constant_time_eq(&candidate, &record.hash) {
            Ok(())
        } else {
            Err(SessionError::Unauthorized(session_id.to_string()))
        }
    }

    fn assert_material(&self, record: &pb::PasswordRecord) -> Result<(), SessionError> {
        if record.salt.len() < 8 || record.hash.is_empty() {
            return Err(SessionError::InvalidRequest(
                "password metadata incomplete".into(),
            ));
        }
        Ok(())
    }

    fn argon2_hash(&self, password: &str, salt: &[u8]) -> Result<Vec<u8>, SessionError> {
        let mut output = vec![0u8; HASH_LENGTH];
        self.argon2
            .hash_password_into(password.as_bytes(), salt, &mut output)
            .map_err(|err| SessionError::PasswordHash(err.to_string()))?;
        Ok(output)
    }

    fn random_salt(&self) -> Vec<u8> {
        let mut salt = vec![0u8; SALT_LENGTH];
        OsRng.fill_bytes(&mut salt);
        salt
    }

    fn unprotected_record(&self) -> pb::PasswordRecord {
        pb::PasswordRecord {
            is_protected: false,
            algorithm: String::new(),
            salt: Vec::new(),
            hash: Vec::new(),
        }
    }

    fn status(&self, record: &pb::PasswordRecord) -> pb::PasswordStatus {
        pb::PasswordStatus {
            is_protected: record.is_protected,
            algorithm: if record.is_protected {
                record.algorithm.clone()
            } else {
                String::new()
            },
        }
    }
}

struct SessionPaths {
    session_dir: PathBuf,
    metadata_file: PathBuf,
    password_file: PathBuf,
    overlay_file: PathBuf,
    checkpoints_dir: PathBuf,
}

impl SessionPaths {
    fn new(root: &Path, session_id: &str) -> Self {
        Self::from_session_dir(root.join(session_id))
    }

    fn staging(root: &Path, session_id: &str) -> Self {
        Self::from_session_dir(root.join(format!(".{session_id}.tmp")))
    }

    fn deleting(root: &Path, session_id: &str) -> Self {
        Self::from_session_dir(root.join(delete_quarantine_session_dir_name(session_id)))
    }

    fn from_session_dir(session_dir: PathBuf) -> Self {
        let metadata_file = session_dir.join("session.meta.pb");
        let password_file = session_dir.join("password.pb");
        let overlay_file = session_dir.join("overlay.pb");
        let checkpoints_dir = session_dir.join("checkpoints");
        SessionPaths {
            session_dir,
            metadata_file,
            password_file,
            overlay_file,
            checkpoints_dir,
        }
    }

    fn checkpoint_file(&self, checkpoint_id: &str) -> PathBuf {
        self.checkpoints_dir.join(format!("{checkpoint_id}.pb"))
    }

    fn checkpoint_meta_file(&self, checkpoint_id: &str) -> PathBuf {
        self.checkpoints_dir
            .join(format!("{checkpoint_id}.meta.pb"))
    }
}

struct SessionRecoveryPaths {
    record_file: PathBuf,
}

impl SessionRecoveryPaths {
    fn new(recovery_root: &Path, session_id: &str) -> Self {
        SessionRecoveryPaths {
            record_file: recovery_root.join(format!("{session_id}.pb")),
        }
    }
}

fn build_snapshot(state: &SessionState) -> pb::SessionSnapshot {
    pb::SessionSnapshot {
        metadata: Some(state.metadata.clone()),
        entries: state.overlay.values().cloned().collect(),
        overlay_version: state.metadata.overlay_version,
    }
}

fn entries_to_map(entries: &[pb::OverlayEntry]) -> HashMap<String, pb::OverlayEntry> {
    entries
        .iter()
        .cloned()
        .map(|e| (e.logical_path.clone(), e))
        .collect()
}

fn entries_equal(a: &pb::OverlayEntry, b: &pb::OverlayEntry) -> bool {
    if a.logical_path != b.logical_path {
        return false;
    }
    match (&a.kind, &b.kind) {
        (Some(pb::overlay_entry::Kind::Alias(ax)), Some(pb::overlay_entry::Kind::Alias(bx))) => {
            ax.target_path == bx.target_path
        }
        (
            Some(pb::overlay_entry::Kind::Tombstone(_)),
            Some(pb::overlay_entry::Kind::Tombstone(_)),
        ) => true,
        _ => false,
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn is_valid_session_dir_name(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id != "."
        && session_id != ".."
        && session_id != RECOVERY_DIR_NAME
        && !session_id.chars().any(std::path::is_separator)
        && !is_staging_session_dir_name(session_id)
        && !is_delete_quarantine_session_dir_name(session_id)
}

fn is_staging_session_dir_name(session_id: &str) -> bool {
    session_id
        .strip_prefix('.')
        .and_then(|value| value.strip_suffix(".tmp"))
        .is_some_and(|value| !value.is_empty())
}

fn delete_quarantine_session_dir_name(session_id: &str) -> String {
    format!(".{session_id}{DELETE_QUARANTINE_SUFFIX}")
}

fn is_delete_quarantine_session_dir_name(session_id: &str) -> bool {
    session_id
        .strip_prefix('.')
        .and_then(|value| value.strip_suffix(DELETE_QUARANTINE_SUFFIX))
        .is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabricfs_session_protocol::session_proto as pb;
    use std::collections::VecDeque;
    use tempfile::TempDir;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum DeleteFailureMode {
        BeforeRemoval,
        AfterOverlayAndPasswordRemoval,
    }

    #[derive(Debug, Default)]
    struct FaultySessionDisk {
        delete_failures: std::sync::Mutex<VecDeque<DeleteFailureMode>>,
        path_exists_overrides: std::sync::Mutex<HashMap<PathBuf, VecDeque<bool>>>,
        sync_failures: std::sync::Mutex<usize>,
        write_failures: std::sync::Mutex<Vec<String>>,
        commit_failures: std::sync::Mutex<Vec<String>>,
        rename_failures: std::sync::Mutex<Vec<String>>,
        post_rename_sync_failures: std::sync::Mutex<Vec<String>>,
    }

    impl FaultySessionDisk {
        fn fail_next_delete(&self) {
            self.delete_failures
                .lock()
                .unwrap()
                .push_back(DeleteFailureMode::BeforeRemoval);
        }

        fn fail_next_delete_after_overlay_and_password_removal(&self) {
            self.delete_failures
                .lock()
                .unwrap()
                .push_back(DeleteFailureMode::AfterOverlayAndPasswordRemoval);
        }

        fn fail_next_sync(&self) {
            *self.sync_failures.lock().unwrap() += 1;
        }

        fn queue_path_exists(&self, path: &Path, exists: bool) {
            self.path_exists_overrides
                .lock()
                .unwrap()
                .entry(path.to_path_buf())
                .or_default()
                .push_back(exists);
        }

        fn fail_next_write(&self, file_name: &str) {
            self.write_failures
                .lock()
                .unwrap()
                .push(file_name.to_string());
        }

        fn fail_next_commit_before_live_boundary(&self, file_name: &str) {
            self.commit_failures
                .lock()
                .unwrap()
                .push(file_name.to_string());
        }

        fn fail_next_rename_before_live_boundary(&self, entry_name: &str) {
            self.rename_failures
                .lock()
                .unwrap()
                .push(entry_name.to_string());
        }

        fn fail_next_post_rename_sync(&self, file_name: &str) {
            self.post_rename_sync_failures
                .lock()
                .unwrap()
                .push(file_name.to_string());
        }

        fn fail_next_recovery_record_sync(&self, session_id: &str) {
            self.fail_next_post_rename_sync(&format!("{session_id}.pb"));
        }

        fn consume_write_failure(&self, path: &Path) -> bool {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            let mut failures = self.write_failures.lock().unwrap();
            if let Some(index) = failures.iter().position(|failure| failure == name) {
                failures.remove(index);
                true
            } else {
                false
            }
        }

        fn consume_commit_failure(&self, path: &Path) -> bool {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            let mut failures = self.commit_failures.lock().unwrap();
            if let Some(index) = failures.iter().position(|failure| failure == name) {
                failures.remove(index);
                true
            } else {
                false
            }
        }

        fn consume_rename_failure(&self, path: &Path) -> bool {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            let mut failures = self.rename_failures.lock().unwrap();
            if let Some(index) = failures.iter().position(|failure| failure == name) {
                failures.remove(index);
                true
            } else {
                false
            }
        }

        fn consume_post_rename_sync_failure(&self, path: &Path) -> bool {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            let mut failures = self.post_rename_sync_failures.lock().unwrap();
            if let Some(index) = failures.iter().position(|failure| failure == name) {
                failures.remove(index);
                true
            } else {
                false
            }
        }
    }

    impl SessionDisk for FaultySessionDisk {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            fs::create_dir_all(path)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            fs::remove_file(path)
        }

        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            if let Some(failure) = self.delete_failures.lock().unwrap().pop_front() {
                return match failure {
                    DeleteFailureMode::BeforeRemoval => Err(io::Error::new(
                        ErrorKind::PermissionDenied,
                        format!("injected delete failure for {}", path.display()),
                    )),
                    DeleteFailureMode::AfterOverlayAndPasswordRemoval => {
                        let overlay_file = path.join("overlay.pb");
                        if overlay_file.exists() {
                            fs::remove_file(&overlay_file)?;
                        }
                        let password_file = path.join("password.pb");
                        if password_file.exists() {
                            fs::remove_file(&password_file)?;
                        }
                        Err(io::Error::new(
                            ErrorKind::PermissionDenied,
                            format!(
                                "injected partial delete failure after removing {} and {}",
                                overlay_file.display(),
                                password_file.display()
                            ),
                        ))
                    }
                };
            }
            fs::remove_dir_all(path)
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            if self.consume_rename_failure(to) {
                return Err(io::Error::other(format!(
                    "injected pre-live rename failure for {} -> {}",
                    from.display(),
                    to.display()
                )));
            }
            fs::rename(from, to)
        }

        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            let mut failures = self.sync_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(io::Error::other(format!(
                    "injected sync failure for {}",
                    path.display()
                )));
            }
            sync_directory(path)
        }

        fn path_exists(&self, path: &Path) -> bool {
            let mut overrides = self.path_exists_overrides.lock().unwrap();
            if let Some(results) = overrides.get_mut(path) {
                if let Some(exists) = results.pop_front() {
                    if results.is_empty() {
                        overrides.remove(path);
                    }
                    return exists;
                }
            }
            path.exists()
        }

        fn stage_atomic_write(
            &self,
            path: &Path,
            data: &[u8],
        ) -> Result<PreparedAtomicWrite, SessionError> {
            if self.consume_write_failure(path) {
                return Err(SessionError::Io(io::Error::other(format!(
                    "injected write failure for {}",
                    path.display()
                ))));
            }
            stage_atomic_write(path, data)
        }

        fn commit_atomic_write(
            &self,
            prepared: PreparedAtomicWrite,
        ) -> Result<PersistenceOutcome, SessionError> {
            if self.consume_commit_failure(&prepared.destination_path) {
                return Err(SessionError::Io(io::Error::other(format!(
                    "injected pre-live commit failure for {}",
                    prepared.destination_path.display()
                ))));
            }
            if self.consume_post_rename_sync_failure(&prepared.destination_path) {
                fs::rename(&prepared.staged_path, &prepared.destination_path)?;
                return Ok(PersistenceOutcome::RenamedNeedsDirectorySync(
                    io::Error::other("injected post-rename directory sync failure"),
                ));
            }
            commit_atomic_write(prepared)
        }
    }

    fn make_alias(logical: &str, target: &str) -> pb::Alias {
        pb::Alias {
            logical_path: logical.to_string(),
            target_path: target.to_string(),
            created_at_unix_nanos: 0,
            origin: None,
        }
    }

    fn make_snapshot(cow_root: &Path, overlay_version: i64) -> pb::SessionSnapshot {
        pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: "source".into(),
                display_name: "Imported".into(),
                workspace_name: "workspace".into(),
                cow_root: cow_root.to_string_lossy().to_string(),
                password: None,
                created_at_unix_nanos: 1,
                updated_at_unix_nanos: 1,
                overlay_version,
            }),
            entries: vec![pb::OverlayEntry {
                logical_path: "/alias".into(),
                kind: Some(pb::overlay_entry::Kind::Alias(make_alias(
                    "/alias", "/target",
                ))),
            }],
            overlay_version,
        }
    }

    fn write_legacy_session(
        root: &Path,
        session_id: &str,
        overlay_version: i64,
        entries: Vec<pb::OverlayEntry>,
    ) {
        let paths = SessionPaths::new(&root.join(".fabricfs").join("sessions"), session_id);
        fs::create_dir_all(&paths.checkpoints_dir).unwrap();
        let metadata = pb::SessionMetadata {
            session_id: session_id.to_string(),
            display_name: "legacy".into(),
            workspace_name: "ws".into(),
            cow_root: root.to_string_lossy().to_string(),
            password: Some(pb::PasswordStatus {
                is_protected: false,
                algorithm: String::new(),
            }),
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
            overlay_version,
        };
        fs::write(
            &paths.metadata_file,
            encode_session_message(&metadata).unwrap(),
        )
        .unwrap();
        fs::write(
            &paths.password_file,
            encode_session_message(&pb::PasswordRecord {
                is_protected: false,
                algorithm: String::new(),
                salt: Vec::new(),
                hash: Vec::new(),
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &paths.overlay_file,
            encode_session_message(&pb::SessionSnapshot {
                metadata: None,
                entries,
                overlay_version,
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn write_authoritative_session(
        root: &Path,
        directory_session_id: &str,
        metadata_session_id: &str,
        overlay_version: i64,
        entries: Vec<pb::OverlayEntry>,
    ) {
        let paths = SessionPaths::new(
            &root.join(".fabricfs").join("sessions"),
            directory_session_id,
        );
        fs::create_dir_all(&paths.checkpoints_dir).unwrap();
        let metadata = pb::SessionMetadata {
            session_id: metadata_session_id.to_string(),
            display_name: "authoritative".into(),
            workspace_name: "ws".into(),
            cow_root: root.to_string_lossy().to_string(),
            password: Some(pb::PasswordStatus {
                is_protected: false,
                algorithm: String::new(),
            }),
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
            overlay_version,
        };
        fs::write(
            &paths.password_file,
            encode_session_message(&pb::PasswordRecord {
                is_protected: false,
                algorithm: String::new(),
                salt: Vec::new(),
                hash: Vec::new(),
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &paths.overlay_file,
            encode_session_message(&pb::SessionSnapshot {
                metadata: Some(metadata),
                entries,
                overlay_version,
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn recovery_record_file(root: &Path, session_id: &str) -> PathBuf {
        root.join(".fabricfs")
            .join("sessions")
            .join(RECOVERY_DIR_NAME)
            .join(format!("{session_id}.pb"))
    }

    #[test]
    fn creates_session_and_metadata() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let req = pb::CreateSessionRequest {
            display_name: "demo".into(),
            workspace_name: "ws".into(),
            cow_root: tmp.path().to_string_lossy().to_string(),
            password: None,
        };
        let meta = store.create_session(&req).unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        assert!(!paths.metadata_file.exists());
        assert!(paths.overlay_file.exists());
        assert!(paths.password_file.exists());
        assert!(paths.checkpoints_dir.exists());
        let decoded: pb::SessionSnapshot =
            decode_session_message(&fs::read(paths.overlay_file).unwrap()).unwrap();
        let decoded = decoded.metadata.unwrap();
        assert_eq!(decoded.session_id, meta.session_id);
    }

    #[test]
    fn create_session_returns_error_and_quarantines_live_state_after_parent_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .create_session_with_id(
                "recoverable-session".into(),
                &pb::CreateSessionRequest {
                    display_name: "recoverable".into(),
                    workspace_name: "ws".into(),
                    cow_root: tmp.path().to_string_lossy().to_string(),
                    password: None,
                },
            )
            .expect_err("session create must fail until the parent directory sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "recoverable-session",
        );
        let recovery_file = recovery_record_file(tmp.path(), "recoverable-session");
        assert!(paths.session_dir.exists());
        assert!(recovery_file.exists());

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .get_snapshot("recoverable-session", None)
            .expect_err("pending durability recovery should fail closed while sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        let snapshot = store.get_snapshot("recoverable-session", None).unwrap();
        assert_eq!(snapshot.overlay_version, 0);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn create_session_pre_live_rename_failure_does_not_leave_pending_state_or_block_retry() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let session_id = "pre-live-create";
        let req = pb::CreateSessionRequest {
            display_name: "recoverable".into(),
            workspace_name: "ws".into(),
            cow_root: tmp.path().to_string_lossy().to_string(),
            password: None,
        };

        disk.fail_next_rename_before_live_boundary(session_id);
        let err = store
            .create_session_with_id(session_id.into(), &req)
            .expect_err("pre-live create rename failure must surface");
        assert!(matches!(err, SessionError::Io(_)));

        let sessions_root = tmp.path().join(".fabricfs").join("sessions");
        let live_paths = SessionPaths::new(&sessions_root, session_id);
        let staging_paths = SessionPaths::staging(&sessions_root, session_id);
        assert!(!live_paths.session_dir.exists());
        assert!(!staging_paths.session_dir.exists());
        assert!(!recovery_record_file(tmp.path(), session_id).exists());
        assert!(store.list_sessions().unwrap().is_empty());
        assert!(matches!(
            store.get_snapshot(session_id, None),
            Err(SessionError::NotFound(_))
        ));

        let created = store
            .create_session_with_id(session_id.into(), &req)
            .expect("retry should succeed after pre-live cleanup");
        assert_eq!(created.session_id, session_id);
    }

    #[test]
    fn create_session_does_not_cross_live_boundary_before_recovery_journal_is_durable() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let session_id = "journal-first-session";

        disk.fail_next_post_rename_sync("journal-first-session.pb");
        let err = store
            .create_session_with_id(
                session_id.into(),
                &pb::CreateSessionRequest {
                    display_name: "recoverable".into(),
                    workspace_name: "ws".into(),
                    cow_root: tmp.path().to_string_lossy().to_string(),
                    password: None,
                },
            )
            .expect_err("session create must abort before the live rename if the recovery journal is not durable");
        assert!(matches!(err, SessionError::Io(_)));

        let paths = SessionPaths::new(&tmp.path().join(".fabricfs").join("sessions"), session_id);
        assert!(
            !paths.session_dir.exists(),
            "the live session directory must not appear before the recovery journal is durable"
        );

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        assert!(
            restarted.list_sessions().unwrap().is_empty(),
            "restart must not replay a session whose write-ahead journal failed before the live rename"
        );
    }

    #[test]
    fn password_material_is_persisted_but_metadata_is_redacted() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "secure".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(pb::SessionPassword {
                    value: "secret".into(),
                }),
            })
            .unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let decoded: pb::SessionSnapshot =
            decode_session_message(&fs::read(paths.overlay_file).unwrap()).unwrap();
        let decoded = decoded.metadata.unwrap();
        let status = decoded.password.unwrap();
        assert!(status.is_protected);
        assert_eq!(status.algorithm, CURRENT_ALGORITHM);

        let record: pb::PasswordRecord =
            decode_session_message(&fs::read(paths.password_file).unwrap()).unwrap();
        assert!(record.is_protected);
        assert_eq!(record.algorithm, CURRENT_ALGORITHM);
        assert_eq!(record.salt.len(), SALT_LENGTH);
        assert_eq!(record.hash.len(), HASH_LENGTH);
    }

    #[test]
    fn password_protection_and_rejection() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "secure".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(pb::SessionPassword {
                    value: "secret".into(),
                }),
            })
            .unwrap();

        let err = store
            .get_snapshot(&meta.session_id, None)
            .expect_err("missing password rejected");
        matches!(err, SessionError::Unauthorized(_));

        let err = store
            .get_snapshot(
                &meta.session_id,
                Some(&pb::SessionPassword {
                    value: "wrong".into(),
                }),
            )
            .expect_err("bad password rejected");
        matches!(err, SessionError::Unauthorized(_));

        let snapshot = store
            .get_snapshot(
                &meta.session_id,
                Some(&pb::SessionPassword {
                    value: "secret".into(),
                }),
            )
            .expect("correct password accepted");
        let metadata = snapshot.metadata.unwrap();
        let status = metadata.password.unwrap();
        assert!(status.is_protected);
        assert_eq!(status.algorithm, CURRENT_ALGORITHM);
        assert_eq!(metadata.session_id, meta.session_id);
    }

    #[test]
    fn rejects_overlong_passwords() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let long_pw = "x".repeat(MAX_PASSWORD_LEN + 1);
        let err = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "too-long".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(pb::SessionPassword { value: long_pw }),
            })
            .expect_err("overlong password rejected");
        matches!(err, SessionError::InvalidRequest(_));
    }

    #[test]
    fn imported_existing_session_retries_after_overlay_persist_failure_without_dirtying_state() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "base".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let snapshot = make_snapshot(tmp.path(), 7);

        disk.fail_next_write("overlay.pb");
        let err = store
            .import_snapshot_into_existing(
                &meta.session_id,
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                None,
            )
            .expect_err("overlay persistence failure must surface");
        assert!(matches!(err, SessionError::Io(_)));

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 0);
        assert!(current.entries.is_empty());

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 0);
        assert!(restarted_snapshot.entries.is_empty());

        store
            .import_snapshot_into_existing(
                &meta.session_id,
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                None,
            )
            .unwrap();
        let retried = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(retried.overlay_version, 7);
        assert_eq!(retried.entries.len(), 1);

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 7);
        assert_eq!(restarted_snapshot.entries.len(), 1);
    }

    #[test]
    fn import_existing_pre_live_commit_failure_discards_pending_state_and_allows_retry() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "base".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let snapshot = make_snapshot(tmp.path(), 7);

        disk.fail_next_commit_before_live_boundary("overlay.pb");
        let err = store
            .import_snapshot_into_existing(
                &meta.session_id,
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                None,
            )
            .expect_err("pre-live import commit failure must surface");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(!recovery_record_file(tmp.path(), &meta.session_id).exists());

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 0);
        assert!(current.entries.is_empty());

        store
            .import_snapshot_into_existing(
                &meta.session_id,
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                None,
            )
            .expect("retry should succeed after pre-live cleanup");
        let retried = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(retried.overlay_version, 7);
        assert_eq!(retried.entries.len(), 1);
    }

    #[test]
    fn update_overlay_pre_live_commit_failure_discards_pending_state_and_allows_retry() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "overlay".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();

        disk.fail_next_commit_before_live_boundary("overlay.pb");
        let err = store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/alias", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .expect_err("pre-live overlay commit failure must surface");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(!recovery_record_file(tmp.path(), &meta.session_id).exists());

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 0);
        assert!(current.entries.is_empty());

        store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/alias", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .expect("retry should succeed after pre-live cleanup");
        let retried = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(retried.overlay_version, 1);
        assert_eq!(retried.entries.len(), 1);
    }

    #[test]
    fn update_overlay_does_not_replace_live_state_before_recovery_journal_is_durable() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "overlay".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();

        disk.fail_next_post_rename_sync(&format!("{}.pb", meta.session_id));
        let err = store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/alias", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .expect_err("overlay update must stop before the live rename if the recovery journal is not durable");
        assert!(matches!(err, SessionError::Io(_)));

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 0);
        assert!(
            current.entries.is_empty(),
            "the in-process session state must remain unchanged when the write-ahead journal fails first"
        );

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 0);
        assert!(restarted_snapshot.entries.is_empty());
    }

    #[test]
    fn update_overlay_returns_error_and_quarantines_live_state_after_post_rename_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "overlay".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();

        disk.fail_next_post_rename_sync("overlay.pb");
        disk.fail_next_sync();
        let err = store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/alias", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .expect_err("overlay update must fail until the final directory sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);
        assert!(recovery_file.exists());

        disk.fail_next_sync();
        let err = store
            .get_snapshot(&meta.session_id, None)
            .expect_err("pending overlay recovery should fail closed while sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 1);
        assert_eq!(current.entries.len(), 1);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn imported_existing_session_returns_error_and_quarantines_live_state_after_post_rename_sync_failure(
    ) {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "base".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let snapshot = make_snapshot(tmp.path(), 7);

        disk.fail_next_post_rename_sync("overlay.pb");
        disk.fail_next_sync();
        let err = store
            .import_snapshot_into_existing(
                &meta.session_id,
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                None,
            )
            .expect_err("import must fail until the final directory sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);
        assert!(recovery_file.exists());

        disk.fail_next_sync();
        let err = store
            .get_snapshot(&meta.session_id, None)
            .expect_err("pending import recovery should fail closed while sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        let current = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(current.overlay_version, 7);
        assert_eq!(current.entries.len(), 1);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn authoritative_overlay_snapshot_ignores_stale_legacy_metadata_after_restart() {
        let tmp = TempDir::new().unwrap();
        write_legacy_session(tmp.path(), "legacy-session", 0, Vec::new());
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let snapshot = make_snapshot(tmp.path(), 7);

        let metadata = store
            .import_snapshot_into_existing(
                "legacy-session",
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                Some(0),
            )
            .unwrap();
        assert_eq!(metadata.overlay_version, 7);

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "legacy-session",
        );
        let stale_metadata: pb::SessionMetadata =
            decode_session_message(&fs::read(paths.metadata_file).unwrap()).unwrap();
        assert_eq!(
            stale_metadata.overlay_version, 0,
            "the legacy sidecar remains stale and must not override the authoritative snapshot"
        );

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot("legacy-session", None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 7);
        assert_eq!(restarted_snapshot.entries.len(), 1);

        let retried = restarted
            .import_snapshot_into_existing(
                "legacy-session",
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                Some(7),
            )
            .unwrap();
        assert_eq!(retried.overlay_version, 8);
    }

    #[test]
    fn imported_new_session_cleans_staging_after_overlay_persist_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let snapshot = make_snapshot(tmp.path(), 5);
        let sessions_root = tmp.path().join(".fabricfs").join("sessions");

        disk.fail_next_write("overlay.pb");
        let err = store
            .create_session_from_snapshot_with_id(
                "imported-session",
                snapshot.clone(),
                Some("Imported".into()),
                None,
            )
            .expect_err("overlay persistence failure must surface");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(store.list_sessions().unwrap().is_empty());
        assert!(!SessionPaths::new(&sessions_root, "imported-session")
            .session_dir
            .exists());
        assert!(!SessionPaths::staging(&sessions_root, "imported-session")
            .session_dir
            .exists());

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        assert!(restarted.list_sessions().unwrap().is_empty());

        let metadata = store
            .create_session_from_snapshot_with_id(
                "imported-session",
                snapshot,
                Some("Imported".into()),
                None,
            )
            .unwrap();
        assert_eq!(metadata.session_id, "imported-session");

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot("imported-session", None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 5);
        assert_eq!(restarted_snapshot.entries.len(), 1);
    }

    #[test]
    fn imported_new_session_returns_error_and_quarantines_live_state_after_parent_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let snapshot = make_snapshot(tmp.path(), 5);

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .create_session_from_snapshot_with_id(
                "imported-session",
                snapshot,
                Some("Imported".into()),
                None,
            )
            .expect_err("imported session create must fail until the parent sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));
        let recovery_file = recovery_record_file(tmp.path(), "imported-session");
        assert!(recovery_file.exists());

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .get_snapshot("imported-session", None)
            .expect_err("pending import recovery should fail closed while sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        let current = store.get_snapshot("imported-session", None).unwrap();
        assert_eq!(current.overlay_version, 5);
        assert_eq!(current.entries.len(), 1);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn create_session_restart_recovery_replays_external_journal_after_parent_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .create_session_with_id(
                "restart-recoverable".into(),
                &pb::CreateSessionRequest {
                    display_name: "recoverable".into(),
                    workspace_name: "ws".into(),
                    cow_root: tmp.path().to_string_lossy().to_string(),
                    password: None,
                },
            )
            .expect_err("session create must fail until the parent directory sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "restart-recoverable",
        );
        let recovery_file = recovery_record_file(tmp.path(), "restart-recoverable");
        assert!(recovery_file.exists());
        fs::remove_dir_all(&paths.session_dir).unwrap();

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let snapshot = restarted.get_snapshot("restart-recoverable", None).unwrap();
        assert_eq!(snapshot.overlay_version, 0);
        assert!(snapshot.entries.is_empty());
        assert!(!recovery_file.exists());
    }

    #[test]
    fn update_overlay_restart_recovery_replays_external_journal_after_post_rename_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "overlay".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();

        disk.fail_next_post_rename_sync("overlay.pb");
        disk.fail_next_sync();
        let err = store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/alias", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .expect_err("overlay update must fail until the final directory sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);
        assert!(recovery_file.exists());
        fs::remove_file(&paths.overlay_file).unwrap();

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(snapshot.overlay_version, 1);
        assert_eq!(snapshot.entries.len(), 1);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn imported_new_session_restart_recovery_replays_external_journal_after_parent_sync_failure() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let snapshot = make_snapshot(tmp.path(), 5);

        disk.fail_next_sync();
        disk.fail_next_sync();
        let err = store
            .create_session_from_snapshot_with_id(
                "restart-imported-session",
                snapshot,
                Some("Imported".into()),
                None,
            )
            .expect_err("imported session create must fail until the parent sync succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "restart-imported-session",
        );
        let recovery_file = recovery_record_file(tmp.path(), "restart-imported-session");
        assert!(recovery_file.exists());
        fs::remove_dir_all(&paths.session_dir).unwrap();

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted
            .get_snapshot("restart-imported-session", None)
            .unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 5);
        assert_eq!(restarted_snapshot.entries.len(), 1);
        assert!(!recovery_file.exists());
    }

    #[test]
    fn load_skips_crash_leftover_staging_directories() {
        let tmp = TempDir::new().unwrap();
        write_authoritative_session(
            tmp.path(),
            ".staged-session.tmp",
            "staged-session",
            4,
            make_snapshot(tmp.path(), 4).entries,
        );

        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        assert!(store.list_sessions().unwrap().is_empty());

        let metadata = store
            .create_session_from_snapshot_with_id(
                "staged-session",
                make_snapshot(tmp.path(), 7),
                Some("Imported".into()),
                None,
            )
            .unwrap();
        assert_eq!(metadata.session_id, "staged-session");

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_snapshot = restarted.get_snapshot("staged-session", None).unwrap();
        assert_eq!(restarted_snapshot.overlay_version, 7);
        assert_eq!(restarted_snapshot.entries.len(), 1);
    }

    #[test]
    fn load_rejects_session_directory_metadata_mismatch() {
        let tmp = TempDir::new().unwrap();
        write_authoritative_session(tmp.path(), "dir-session", "other-session", 0, Vec::new());

        let err = SessionStore::load(tmp.path().to_path_buf())
            .expect_err("directory and metadata session ids must match");
        assert!(matches!(
            err,
            SessionError::InvalidRequest(message)
                if message.contains("dir-session")
                    && message.contains("other-session")
        ));
    }

    #[test]
    fn rejects_session_ids_that_overlap_the_staging_namespace() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();

        let err = store
            .create_session_from_snapshot_with_id(
                ".staged.tmp",
                make_snapshot(tmp.path(), 1),
                Some("Imported".into()),
                None,
            )
            .expect_err("staging-style session ids must be rejected");
        assert!(matches!(
            err,
            SessionError::InvalidRequest(message)
                if message.contains("invalid session identifier")
        ));

        let err = store
            .create_session_from_snapshot_with_id(
                ".delete.deleting",
                make_snapshot(tmp.path(), 2),
                Some("Imported".into()),
                None,
            )
            .expect_err("delete-quarantine-style session ids must be rejected");
        assert!(matches!(
            err,
            SessionError::InvalidRequest(message)
                if message.contains("invalid session identifier")
        ));
    }

    #[test]
    fn delete_session_quarantines_live_state_when_directory_removal_fails() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let deleting_paths = SessionPaths::deleting(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_delete();
        let err = store
            .delete_session(&meta.session_id, None)
            .expect_err("delete must fail closed on disk error");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(
            !paths.session_dir.exists(),
            "delete must first move the live session out of the mounted namespace before retrying destructive work"
        );
        assert!(deleting_paths.session_dir.exists());
        assert!(
            recovery_file.exists(),
            "delete quarantine must leave a durable external recovery record before destructive retry work resumes"
        );

        let err = store.list_sessions().expect_err(
            "delete retry quarantine must block live reads after directory removal starts",
        );
        assert!(matches!(err, SessionError::Io(_)));

        let err = store.get_snapshot(&meta.session_id, None).expect_err(
            "delete retry quarantine must block snapshots after directory removal starts",
        );
        assert!(matches!(err, SessionError::Io(_)));

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let err = restarted.list_sessions().expect_err(
            "restart must preserve delete quarantine rather than silently re-exposing the session",
        );
        assert!(matches!(err, SessionError::Io(_)));
        let err = restarted
            .get_snapshot(&meta.session_id, None)
            .expect_err("restart must keep the pending delete unreadable until retry succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        store.delete_session(&meta.session_id, None).unwrap();
        assert!(store.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        assert!(restarted.list_sessions().unwrap().is_empty());
    }

    #[test]
    fn delete_session_recovery_arm_failure_keeps_session_accessible_until_delete_starts() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete-journal".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let deleting_paths = SessionPaths::deleting(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_recovery_record_sync(&meta.session_id);
        let err = store.delete_session(&meta.session_id, None).expect_err(
            "delete must fail before quarantine if the recovery journal is not durable",
        );
        assert!(matches!(err, SessionError::Io(_)));
        assert!(
            paths.session_dir.exists(),
            "the live session directory must remain untouched when delete never crosses its first live boundary"
        );
        assert!(
            !deleting_paths.session_dir.exists(),
            "failed delete arming must not create a hidden delete-quarantine directory"
        );
        assert!(
            !recovery_file.exists(),
            "failed delete arming must roll back any tentative recovery journal"
        );

        let listed = store.list_sessions().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, meta.session_id);

        let initialized = store.init_session(&meta.session_id, None).unwrap();
        assert_eq!(initialized.session_id, meta.session_id);

        let snapshot = store.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(snapshot.metadata.unwrap().session_id, meta.session_id);

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let restarted_listed = restarted.list_sessions().unwrap();
        assert_eq!(restarted_listed.len(), 1);
        assert_eq!(restarted_listed[0].session_id, meta.session_id);
        let restarted_snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(
            restarted_snapshot.metadata.unwrap().session_id,
            meta.session_id
        );

        store.delete_session(&meta.session_id, None).unwrap();
        assert!(store.list_sessions().unwrap().is_empty());
    }

    #[test]
    fn delete_session_restart_after_journal_arm_before_live_rename_keeps_session_accessible() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete-restart-window".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let sessions_root = tmp.path().join(".fabricfs").join("sessions");
        let paths = SessionPaths::new(&sessions_root, &meta.session_id);
        let deleting_paths = SessionPaths::deleting(&sessions_root, &meta.session_id);
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        {
            let mut sessions = store.write_sessions().unwrap();
            let action = store
                .authorize_session_delete(&mut sessions, &meta.session_id, None)
                .unwrap();
            assert!(matches!(action, DeleteAction::RenameLiveDirectory));
        }

        assert!(paths.session_dir.exists());
        assert!(!deleting_paths.session_dir.exists());
        assert!(recovery_file.exists());

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let listed = restarted.list_sessions().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, meta.session_id);
        let snapshot = restarted.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(snapshot.metadata.unwrap().session_id, meta.session_id);
        assert!(
            !recovery_file.exists(),
            "restart must discard a delete journal that never moved the live session into quarantine"
        );
        assert!(paths.session_dir.exists());
        assert!(!deleting_paths.session_dir.exists());
    }

    #[test]
    fn pending_delete_boundary_drift_fails_closed_without_panicking_and_recovers_on_retry() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete-boundary-drift".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let sessions_root = tmp.path().join(".fabricfs").join("sessions");
        let live_paths = SessionPaths::new(&sessions_root, &meta.session_id);
        let deleting_paths = SessionPaths::deleting(&sessions_root, &meta.session_id);
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        {
            let mut sessions = store.write_sessions().unwrap();
            let action = store
                .authorize_session_delete(&mut sessions, &meta.session_id, None)
                .unwrap();
            assert!(matches!(action, DeleteAction::RenameLiveDirectory));
        }

        assert!(live_paths.session_dir.exists());
        assert!(!deleting_paths.session_dir.exists());
        assert!(recovery_file.exists());

        disk.queue_path_exists(&live_paths.session_dir, false);
        disk.queue_path_exists(&deleting_paths.session_dir, true);

        let err = store
            .get_snapshot(&meta.session_id, None)
            .expect_err("a drifting delete-boundary read must fail closed instead of panicking");
        assert!(matches!(err, SessionError::Io(_)));

        let snapshot = store
            .get_snapshot(&meta.session_id, None)
            .expect("retry must discard the still-pre-live delete journal once the live boundary reads consistently");
        assert_eq!(snapshot.metadata.unwrap().session_id, meta.session_id);
        assert!(
            !recovery_file.exists(),
            "pre-live delete recovery must be discarded once the live boundary proves the delete never started"
        );
        assert!(live_paths.session_dir.exists());
        assert!(!deleting_paths.session_dir.exists());
    }

    #[test]
    fn delete_session_quarantines_partially_removed_directory_until_retry_completes() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete-partial".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let deleting_paths = SessionPaths::deleting(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_delete_after_overlay_and_password_removal();
        let err = store
            .delete_session(&meta.session_id, None)
            .expect_err("partial delete failures must quarantine the live session");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(
            !paths.session_dir.exists(),
            "partial delete failures must leave the live namespace hidden behind the delete quarantine directory"
        );
        assert!(deleting_paths.session_dir.exists());
        assert!(
            recovery_file.exists(),
            "partial deletes must leave a durable external recovery record behind"
        );
        assert!(
            !deleting_paths.overlay_file.exists(),
            "the fault injector must prove durable state can be partially removed inside delete quarantine before the error returns"
        );
        assert!(
            !deleting_paths.password_file.exists(),
            "restart recovery must not depend on password material surviving inside the delete quarantine directory"
        );

        let err = store
            .list_sessions()
            .expect_err("partially removed sessions must fail closed until delete retry succeeds");
        assert!(matches!(err, SessionError::Io(_)));

        let err = store.get_snapshot(&meta.session_id, None).expect_err(
            "partially removed sessions must remain unreadable until delete retry succeeds",
        );
        assert!(matches!(err, SessionError::Io(_)));

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let err = restarted.list_sessions().expect_err(
            "restart must keep the partially deleted session quarantined until retry succeeds",
        );
        assert!(matches!(err, SessionError::Io(_)));
        let err = restarted
            .get_snapshot(&meta.session_id, None)
            .expect_err("restart must not expose a partially deleted session snapshot");
        assert!(matches!(err, SessionError::Io(_)));
        restarted.delete_session(&meta.session_id, None).unwrap();
        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());

        store.delete_session(&meta.session_id, None).unwrap();
        assert!(store.list_sessions().unwrap().is_empty());
    }

    #[test]
    fn delete_session_quarantines_migrated_legacy_session_before_partial_delete_can_reanimate_it() {
        let tmp = TempDir::new().unwrap();
        write_legacy_session(tmp.path(), "legacy-session", 0, Vec::new());
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let snapshot = make_snapshot(tmp.path(), 7);
        store
            .import_snapshot_into_existing(
                "legacy-session",
                &snapshot,
                None,
                pb::ImportMode::Replace,
                pb::ConflictPolicy::Error,
                Some(0),
            )
            .unwrap();

        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "legacy-session",
        );
        let deleting_paths = SessionPaths::deleting(
            &tmp.path().join(".fabricfs").join("sessions"),
            "legacy-session",
        );
        let recovery_file = recovery_record_file(tmp.path(), "legacy-session");
        assert!(paths.metadata_file.exists());
        assert!(paths.overlay_file.exists());

        disk.fail_next_delete_after_overlay_and_password_removal();
        let err = store
            .delete_session("legacy-session", None)
            .expect_err("partial delete failures must quarantine migrated legacy sessions");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(
            recovery_file.exists(),
            "the external recovery record must survive partial legacy cleanup"
        );
        assert!(
            !paths.session_dir.exists(),
            "migrated legacy sessions must leave the live namespace before destructive cleanup begins"
        );
        assert!(deleting_paths.session_dir.exists());
        assert!(
            !deleting_paths.overlay_file.exists(),
            "the fault injector must still remove the authoritative overlay snapshot before returning the delete error"
        );
        assert!(
            !deleting_paths.password_file.exists(),
            "restart recovery for migrated legacy deletes must not rely on password.pb remaining in the partially removed quarantine directory"
        );

        let err = store
            .get_snapshot("legacy-session", None)
            .expect_err("partially deleted migrated sessions must remain unreadable in-process");
        assert!(matches!(err, SessionError::Io(_)));

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let err = restarted.list_sessions().expect_err(
            "restart must preserve migrated legacy delete quarantine rather than silently dropping it",
        );
        assert!(matches!(err, SessionError::Io(_)));
        let err = restarted.get_snapshot("legacy-session", None).expect_err(
            "restart must keep the migrated legacy session unreadable until delete retry succeeds",
        );
        assert!(matches!(err, SessionError::Io(_)));
        restarted.delete_session("legacy-session", None).unwrap();
        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());

        store.delete_session("legacy-session", None).unwrap();
        assert!(store.list_sessions().unwrap().is_empty());
    }

    #[test]
    fn legacy_only_delete_failure_stays_quarantined_across_restart() {
        let tmp = TempDir::new().unwrap();
        write_legacy_session(tmp.path(), "legacy-only", 0, Vec::new());
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            "legacy-only",
        );
        let deleting_paths = SessionPaths::deleting(
            &tmp.path().join(".fabricfs").join("sessions"),
            "legacy-only",
        );
        let recovery_file = recovery_record_file(tmp.path(), "legacy-only");

        disk.fail_next_delete();
        let err = store
            .delete_session("legacy-only", None)
            .expect_err("legacy-only delete failures must stay durable across restart");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(!paths.session_dir.exists());
        assert!(deleting_paths.session_dir.exists());
        assert!(
            deleting_paths.metadata_file.exists(),
            "legacy-only delete retry must be able to finish from the hidden quarantine directory without mutating the live namespace in place"
        );
        assert!(
            recovery_file.exists(),
            "legacy-only delete failures must persist a durable external recovery record"
        );

        let restarted = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let err = restarted.list_sessions().expect_err(
            "restart must quarantine failed legacy-only deletes instead of silently dropping them",
        );
        assert!(matches!(err, SessionError::Io(_)));
        let err = restarted.get_snapshot("legacy-only", None).expect_err(
            "restart must keep failed legacy-only deletes unreadable until retry succeeds",
        );
        assert!(matches!(err, SessionError::Io(_)));

        restarted.delete_session("legacy-only", None).unwrap();
        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());
    }

    #[test]
    fn delete_session_quarantines_failed_delete_until_parent_sync_recovers() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "delete-sync".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        let paths = SessionPaths::new(
            &tmp.path().join(".fabricfs").join("sessions"),
            &meta.session_id,
        );
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_sync();
        let err = store
            .delete_session(&meta.session_id, None)
            .expect_err("delete must fail if the parent directory sync fails");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(!paths.session_dir.exists());
        assert!(recovery_file.exists());

        disk.fail_next_sync();
        let err = store
            .list_sessions()
            .expect_err("failed deletes must fail closed while the root sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        disk.fail_next_sync();
        let err = store
            .get_snapshot(&meta.session_id, None)
            .expect_err("deleted sessions must remain quarantined while the root sync still fails");
        assert!(matches!(err, SessionError::Io(_)));

        let restarted =
            SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        disk.fail_next_sync();
        let err = restarted.list_sessions().expect_err(
            "restart must preserve the pending root sync rather than dropping the delete",
        );
        assert!(matches!(err, SessionError::Io(_)));

        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());
    }

    #[test]
    fn protected_delete_quarantine_retry_does_not_require_password_after_live_boundary() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let password = pb::SessionPassword {
            value: "secret".into(),
        };
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "protected-delete-partial".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(password.clone()),
            })
            .unwrap();
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_delete_after_overlay_and_password_removal();
        let err = store
            .delete_session(&meta.session_id, Some(&password))
            .expect_err("partial delete failures must still fail closed for protected sessions");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(recovery_file.exists());

        let restarted =
            SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let err = restarted.list_sessions().expect_err(
            "restart must keep the protected session quarantined until delete retry succeeds",
        );
        assert!(matches!(err, SessionError::Io(_)));

        restarted
            .delete_session(&meta.session_id, None)
            .expect("quarantine cleanup must not require the original end-user password");
        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());
    }

    #[test]
    fn protected_delete_root_sync_retry_does_not_require_password_after_live_boundary() {
        let tmp = TempDir::new().unwrap();
        let disk = Arc::new(FaultySessionDisk::default());
        let store = SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        let password = pb::SessionPassword {
            value: "secret".into(),
        };
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "protected-delete-sync".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(password.clone()),
            })
            .unwrap();
        let recovery_file = recovery_record_file(tmp.path(), &meta.session_id);

        disk.fail_next_sync();
        let err = store
            .delete_session(&meta.session_id, Some(&password))
            .expect_err("delete must fail if the final root sync fails for a protected session");
        assert!(matches!(err, SessionError::Io(_)));
        assert!(recovery_file.exists());

        let restarted =
            SessionStore::load_with_disk(tmp.path().to_path_buf(), disk.clone()).unwrap();
        disk.fail_next_sync();
        let err = restarted.list_sessions().expect_err(
            "restart must keep the protected session pending while the root sync still fails",
        );
        assert!(matches!(err, SessionError::Io(_)));

        restarted
            .delete_session(&meta.session_id, None)
            .expect("post-live root-sync completion must not require the original password");
        assert!(restarted.list_sessions().unwrap().is_empty());
        assert!(!recovery_file.exists());
    }

    #[test]
    fn persists_overlay_and_checkpoints() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "persist".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: None,
            })
            .unwrap();
        store
            .update_overlay(&pb::UpdateOverlayRequest {
                session_id: meta.session_id.clone(),
                password: None,
                add_aliases: vec![make_alias("/persist", "/target")],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            })
            .unwrap();
        let checkpoint = store
            .checkpoint(&pb::CheckpointSessionRequest {
                session_id: meta.session_id.clone(),
                password: None,
                label: "first".into(),
            })
            .unwrap();
        drop(store);

        let reloaded = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let snapshot = reloaded.get_snapshot(&meta.session_id, None).unwrap();
        assert_eq!(snapshot.overlay_version, 1);
        assert_eq!(snapshot.entries.len(), 1);

        let (_, saved_snapshot) = reloaded
            .read_checkpoint(&meta.session_id, &checkpoint.checkpoint_id, None)
            .unwrap();
        assert_eq!(saved_snapshot.overlay_version, 1);
    }

    #[test]
    fn list_checkpoints_enforces_password() {
        let tmp = TempDir::new().unwrap();
        let store = SessionStore::load(tmp.path().to_path_buf()).unwrap();
        let meta = store
            .create_session(&pb::CreateSessionRequest {
                display_name: "protected".into(),
                workspace_name: "ws".into(),
                cow_root: tmp.path().to_string_lossy().to_string(),
                password: Some(pb::SessionPassword {
                    value: "secret".into(),
                }),
            })
            .unwrap();

        store
            .checkpoint(&pb::CheckpointSessionRequest {
                session_id: meta.session_id.clone(),
                password: Some(pb::SessionPassword {
                    value: "secret".into(),
                }),
                label: "first".into(),
            })
            .unwrap();

        let err = store
            .list_checkpoints(&meta.session_id, None)
            .expect_err("missing password rejected");
        matches!(err, SessionError::Unauthorized(_));

        let err = store
            .list_checkpoints(
                &meta.session_id,
                Some(&pb::SessionPassword {
                    value: "wrong".into(),
                }),
            )
            .expect_err("bad password rejected");
        matches!(err, SessionError::Unauthorized(_));

        let checkpoints = store
            .list_checkpoints(
                &meta.session_id,
                Some(&pb::SessionPassword {
                    value: "secret".into(),
                }),
            )
            .expect("correct password accepted");
        assert_eq!(checkpoints.len(), 1);
    }

    #[test]
    fn atomic_write_fsyncs_parent_directory_after_rename() {
        #[derive(Default)]
        struct RecordingOps {
            steps: std::sync::Mutex<Vec<&'static str>>,
        }

        impl AtomicWriteOps for RecordingOps {
            type FileHandle = ();
            type DirHandle = ();

            fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
                self.steps.lock().unwrap().push("create_dir_all");
                Ok(())
            }

            fn create_file(&self, _path: &Path) -> io::Result<Self::FileHandle> {
                self.steps.lock().unwrap().push("create_file");
                Ok(())
            }

            fn write_all(&self, _file: &mut Self::FileHandle, _data: &[u8]) -> io::Result<()> {
                self.steps.lock().unwrap().push("write_all");
                Ok(())
            }

            fn sync_file(&self, _file: &mut Self::FileHandle) -> io::Result<()> {
                self.steps.lock().unwrap().push("sync_file");
                Ok(())
            }

            fn open_dir(&self, _path: &Path) -> io::Result<Self::DirHandle> {
                self.steps.lock().unwrap().push("open_dir");
                Ok(())
            }

            fn sync_dir(&self, _dir: &Self::DirHandle) -> io::Result<()> {
                self.steps.lock().unwrap().push("sync_dir");
                Ok(())
            }

            fn rename(&self, _from: &Path, _to: &Path) -> io::Result<()> {
                self.steps.lock().unwrap().push("rename");
                Ok(())
            }
        }

        let ops = RecordingOps::default();
        let outcome =
            atomic_write_with(&ops, Path::new("/tmp/session.meta.pb"), b"payload").unwrap();
        assert!(matches!(outcome, PersistenceOutcome::Durable));
        assert_eq!(
            *ops.steps.lock().unwrap(),
            vec![
                "create_dir_all",
                "create_file",
                "write_all",
                "sync_file",
                "open_dir",
                "sync_dir",
                "rename",
                "open_dir",
                "sync_dir",
            ]
        );
    }
}
