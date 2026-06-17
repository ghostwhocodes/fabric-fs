use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use fabricfs_session_protocol::session::{
    decode_session_message, encode_session_message, session_subject, SessionCodecError, SessionOp,
};
use fabricfs_session_protocol::session_proto as pb;
use fabricfs_transport::connect_nats;
use nats::Connection;
use serde::Serialize;
use tabled::{settings::Style, Table, Tabled};
use thiserror::Error;

#[derive(Parser, Debug)]
#[command(name = "fabricfsctl", about = "SessionControl CLI over NATS")]
struct Cli {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// Path to NATS credentials file (overrides embedded credentials in URL).
    /// Can also be set via NATS_CREDS_FILE environment variable.
    #[arg(long, env = "NATS_CREDS_FILE")]
    nats_creds_file: Option<PathBuf>,

    #[arg(long, default_value_t = 5, help = "request timeout seconds")]
    timeout_secs: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Sessions {
        #[command(subcommand)]
        action: SessionsCommand,
    },
    Overlay {
        #[command(subcommand)]
        action: OverlayCommand,
    },
    Checkpoints {
        #[command(subcommand)]
        action: CheckpointCommand,
    },
    Published {
        #[command(subcommand)]
        action: PublishedCommand,
    },
}

#[derive(Subcommand, Debug)]
enum SessionsCommand {
    List {
        #[arg(long, help = "render the session list as json")]
        json: bool,
    },
    Create {
        display_name: String,
        cow_root: String,
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    Delete {
        session_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    Show {
        session_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
        #[arg(long, help = "render the session snapshot as json")]
        json: bool,
    },
    Attach {
        session_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
}

#[derive(Subcommand, Debug)]
enum OverlayCommand {
    List {
        session_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
        #[arg(long, default_value = "", help = "filter by directory prefix")]
        dir: String,
        #[arg(long, help = "render overlay entries as json")]
        json: bool,
    },
    AliasAdd {
        session_id: String,
        logical_path: String,
        target_path: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    AliasRm {
        session_id: String,
        logical_path: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    TombAdd {
        session_id: String,
        logical_path: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    TombRm {
        session_id: String,
        logical_path: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
}

#[derive(Subcommand, Debug)]
enum CheckpointCommand {
    Commit {
        session_id: String,
        #[arg(long, default_value = "", help = "label for the checkpoint")]
        label: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    List {
        session_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
        #[arg(long, help = "render checkpoints as json")]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum PublishedCommand {
    List {
        #[arg(long, help = "render published checkpoints as json")]
        json: bool,
    },
    Push {
        session_id: String,
        checkpoint_id: String,
        #[arg(
            long,
            default_value = "",
            help = "remote checkpoint id (defaults to checkpoint_id)"
        )]
        remote_id: String,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
    Pull {
        remote_id: String,
        #[arg(long, default_value = "", help = "display name for a new session")]
        new_session_name: String,
        #[arg(long, default_value = "", help = "import into an existing session")]
        into: String,
        #[arg(long, value_enum, default_value_t = ImportModeFlag::Replace)]
        mode: ImportModeFlag,
        #[arg(long, value_enum, default_value_t = ConflictPolicyFlag::Error)]
        conflict_policy: ConflictPolicyFlag,
        #[arg(
            long,
            default_value_t = -1,
            help = "expected overlay version; -1 disables optimistic checking"
        )]
        expect_overlay_version: i64,
        #[arg(long, value_enum, default_value_t = PasswordInput::None)]
        password: PasswordInput,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum PasswordInput {
    None,
    Prompt,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ImportModeFlag {
    Replace,
    Merge,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ConflictPolicyFlag {
    Error,
    KeepLocal,
    OverwriteRemote,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("failed to connect to nats: {0}")]
    Connect(#[source] io::Error),

    #[error("request to {:?} failed: {source}", op)]
    Request {
        op: SessionOp,
        #[source]
        source: io::Error,
    },

    #[error("encode {:?} request failed: {source}", op)]
    Encode {
        op: SessionOp,
        #[source]
        source: SessionCodecError,
    },

    #[error("decode {:?} response failed: {source}", op)]
    Decode {
        op: SessionOp,
        #[source]
        source: SessionCodecError,
    },

    #[error("{:?} returned an error: {message}", op)]
    RemoteFailure { op: SessionOp, message: String },

    #[error("response missing {0}")]
    Missing(&'static str),

    #[error("password prompt failed: {0}")]
    PasswordIo(#[from] io::Error),

    #[error("failed to render json: {0}")]
    Json(#[from] serde_json::Error),
}

impl CliError {
    fn exit_code(&self) -> ExitCode {
        match self {
            CliError::RemoteFailure { .. } => ExitCode::from(10),
            CliError::Missing(_) => ExitCode::from(11),
            CliError::Connect(_) => ExitCode::from(12),
            CliError::Request { .. } => ExitCode::from(13),
            CliError::Encode { .. } | CliError::Decode { .. } => ExitCode::from(14),
            CliError::PasswordIo(_) => ExitCode::from(15),
            CliError::Json(_) => ExitCode::from(16),
        }
    }

    fn print(&self) {
        eprintln!("error: {self}");
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            err.print();
            err.exit_code()
        }
    }
}

fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let connection = connect_nats(
        &cli.nats_url,
        cli.nats_creds_file
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .as_deref(),
    )
    .map_err(|e| CliError::Connect(io::Error::other(e)))?;
    let timeout = Duration::from_secs(cli.timeout_secs);

    match cli.command {
        Command::Sessions { action } => handle_sessions(&connection, timeout, action),
        Command::Overlay { action } => handle_overlay(&connection, timeout, action),
        Command::Checkpoints { action } => handle_checkpoints(&connection, timeout, action),
        Command::Published { action } => handle_published(&connection, timeout, action),
    }
}

fn handle_sessions(
    connection: &Connection,
    timeout: Duration,
    action: SessionsCommand,
) -> Result<(), CliError> {
    match action {
        SessionsCommand::List { json } => {
            let res: pb::ListSessionsResponse = rpc_request(
                connection,
                timeout,
                SessionOp::ListSessions,
                pb::ListSessionsRequest {},
            )?;
            require_ok(SessionOp::ListSessions, res.status)?;
            let sessions: Vec<SessionView> = res.sessions.iter().map(SessionView::from).collect();
            if json {
                render_json(&sessions)?;
            } else {
                let rows: Vec<SessionTableRow> =
                    sessions.iter().map(SessionTableRow::from).collect();
                render_table(rows);
            }
        }
        SessionsCommand::Create {
            display_name,
            cow_root,
            workspace,
            password,
        } => {
            let req = pb::CreateSessionRequest {
                display_name,
                workspace_name: workspace.unwrap_or_default(),
                cow_root,
                password: password_input(password)?,
            };
            let res: pb::CreateSessionResponse =
                rpc_request(connection, timeout, SessionOp::CreateSession, req)?;
            require_ok(SessionOp::CreateSession, res.status)?;
            let meta = res.metadata.ok_or(CliError::Missing("session metadata"))?;
            println!("{}", meta.session_id);
        }
        SessionsCommand::Delete {
            session_id,
            password,
        } => {
            let req = pb::DeleteSessionRequest {
                session_id,
                password: password_input(password)?,
            };
            let res: pb::DeleteSessionResponse =
                rpc_request(connection, timeout, SessionOp::DeleteSession, req)?;
            require_ok(SessionOp::DeleteSession, res.status)?;
            println!("deleted");
        }
        SessionsCommand::Show {
            session_id,
            password,
            json,
        } => {
            let req = pb::GetSessionRequest {
                session_id,
                password: password_input(password)?,
            };
            let res: pb::GetSessionResponse =
                rpc_request(connection, timeout, SessionOp::GetSession, req)?;
            require_ok(SessionOp::GetSession, res.status)?;
            let snapshot = res
                .snapshot
                .as_ref()
                .ok_or(CliError::Missing("session snapshot"))?;
            let view = SnapshotView::try_from(snapshot)?;
            render_snapshot(&view, json)?;
        }
        SessionsCommand::Attach {
            session_id,
            password,
        } => {
            let req = pb::InitSessionRequest {
                session_id,
                password: password_input(password)?,
            };
            let res: pb::InitSessionResponse =
                rpc_request(connection, timeout, SessionOp::InitSession, req)?;
            require_ok(SessionOp::InitSession, res.status)?;
            let meta = res.metadata.ok_or(CliError::Missing("session metadata"))?;
            println!(
                "{}\t{}\t{}\t{}",
                meta.session_id, meta.display_name, meta.workspace_name, meta.cow_root
            );
        }
    }
    Ok(())
}

fn handle_overlay(
    connection: &Connection,
    timeout: Duration,
    action: OverlayCommand,
) -> Result<(), CliError> {
    match action {
        OverlayCommand::List {
            session_id,
            password,
            dir,
            json,
        } => {
            let req = pb::ListOverlayEntriesRequest {
                session_id,
                password: password_input(password)?,
                directory_prefix: dir,
            };
            let res: pb::ListOverlayEntriesResponse =
                rpc_request(connection, timeout, SessionOp::ListOverlayEntries, req)?;
            require_ok(SessionOp::ListOverlayEntries, res.status)?;
            let entries: Vec<OverlayEntryView> =
                res.entries.iter().map(OverlayEntryView::from).collect();
            if json {
                render_json(&entries)?;
            } else {
                let rows: Vec<OverlayTableRow> =
                    entries.iter().map(OverlayTableRow::from).collect();
                render_table(rows);
            }
        }
        OverlayCommand::AliasAdd {
            session_id,
            logical_path,
            target_path,
            password,
        } => {
            let req = pb::UpdateOverlayRequest {
                session_id,
                password: password_input(password)?,
                add_aliases: vec![pb::Alias {
                    logical_path,
                    target_path,
                    created_at_unix_nanos: 0,
                    origin: None,
                }],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            };
            let res: pb::UpdateOverlayResponse =
                rpc_request(connection, timeout, SessionOp::UpdateOverlay, req)?;
            require_ok(SessionOp::UpdateOverlay, res.status)?;
            println!("ok");
        }
        OverlayCommand::AliasRm {
            session_id,
            logical_path,
            password,
        } => {
            let req = pb::UpdateOverlayRequest {
                session_id,
                password: password_input(password)?,
                add_aliases: vec![],
                add_tombstones: vec![],
                remove_alias_paths: vec![logical_path],
                remove_tombstone_paths: vec![],
            };
            let res: pb::UpdateOverlayResponse =
                rpc_request(connection, timeout, SessionOp::UpdateOverlay, req)?;
            require_ok(SessionOp::UpdateOverlay, res.status)?;
            println!("ok");
        }
        OverlayCommand::TombAdd {
            session_id,
            logical_path,
            password,
        } => {
            let req = pb::UpdateOverlayRequest {
                session_id,
                password: password_input(password)?,
                add_aliases: vec![],
                add_tombstones: vec![pb::Tombstone {
                    logical_path,
                    created_at_unix_nanos: 0,
                    origin: None,
                }],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![],
            };
            let res: pb::UpdateOverlayResponse =
                rpc_request(connection, timeout, SessionOp::UpdateOverlay, req)?;
            require_ok(SessionOp::UpdateOverlay, res.status)?;
            println!("ok");
        }
        OverlayCommand::TombRm {
            session_id,
            logical_path,
            password,
        } => {
            let req = pb::UpdateOverlayRequest {
                session_id,
                password: password_input(password)?,
                add_aliases: vec![],
                add_tombstones: vec![],
                remove_alias_paths: vec![],
                remove_tombstone_paths: vec![logical_path],
            };
            let res: pb::UpdateOverlayResponse =
                rpc_request(connection, timeout, SessionOp::UpdateOverlay, req)?;
            require_ok(SessionOp::UpdateOverlay, res.status)?;
            println!("ok");
        }
    }
    Ok(())
}

fn handle_checkpoints(
    connection: &Connection,
    timeout: Duration,
    action: CheckpointCommand,
) -> Result<(), CliError> {
    match action {
        CheckpointCommand::Commit {
            session_id,
            label,
            password,
        } => {
            let req = pb::CheckpointSessionRequest {
                session_id,
                password: password_input(password)?,
                label,
            };
            let res: pb::CheckpointSessionResponse =
                rpc_request(connection, timeout, SessionOp::CheckpointSession, req)?;
            require_ok(SessionOp::CheckpointSession, res.status)?;
            let meta = res
                .checkpoint
                .ok_or(CliError::Missing("checkpoint metadata"))?;
            println!("{}", meta.checkpoint_id);
        }
        CheckpointCommand::List {
            session_id,
            password,
            json,
        } => {
            let req = pb::ListCheckpointsRequest {
                session_id,
                password: password_input(password)?,
            };
            let res: pb::ListCheckpointsResponse =
                rpc_request(connection, timeout, SessionOp::ListCheckpoints, req)?;
            require_ok(SessionOp::ListCheckpoints, res.status)?;
            let checkpoints: Vec<CheckpointView> =
                res.checkpoints.iter().map(CheckpointView::from).collect();
            if json {
                render_json(&checkpoints)?;
            } else {
                let rows: Vec<CheckpointTableRow> =
                    checkpoints.iter().map(CheckpointTableRow::from).collect();
                render_table(rows);
            }
        }
    }
    Ok(())
}

fn handle_published(
    connection: &Connection,
    timeout: Duration,
    action: PublishedCommand,
) -> Result<(), CliError> {
    match action {
        PublishedCommand::List { json } => {
            let res: pb::ListPublishedCheckpointsResponse = rpc_request(
                connection,
                timeout,
                SessionOp::ListPublishedCheckpoints,
                pb::ListPublishedCheckpointsRequest {},
            )?;
            require_ok(SessionOp::ListPublishedCheckpoints, res.status)?;
            let published: Vec<PublishedView> = res
                .checkpoints
                .iter()
                .map(PublishedView::try_from)
                .collect::<Result<_, _>>()?;
            if json {
                render_json(&published)?;
            } else {
                let rows: Vec<PublishedTableRow> =
                    published.iter().map(PublishedTableRow::from).collect();
                render_table(rows);
            }
        }
        PublishedCommand::Push {
            session_id,
            checkpoint_id,
            remote_id,
            password,
        } => {
            let req = pb::PublishCheckpointRequest {
                session_id,
                checkpoint_id,
                remote_checkpoint_id: remote_id,
                password: password_input(password)?,
            };
            let res: pb::PublishCheckpointResponse =
                rpc_request(connection, timeout, SessionOp::PublishCheckpoint, req)?;
            require_ok(SessionOp::PublishCheckpoint, res.status)?;
            println!("{}", res.remote_checkpoint_id);
        }
        PublishedCommand::Pull {
            remote_id,
            new_session_name,
            into,
            mode,
            conflict_policy,
            expect_overlay_version,
            password,
        } => {
            let req = pb::ImportPublishedCheckpointRequest {
                remote_checkpoint_id: remote_id,
                target_session_id: into,
                new_display_name: new_session_name,
                password: password_input(password)?,
                mode: map_import_mode(mode) as i32,
                conflict_policy: map_conflict_policy(conflict_policy) as i32,
                expected_overlay_version: expect_overlay_version,
            };
            let res: pb::ImportPublishedCheckpointResponse = rpc_request(
                connection,
                timeout,
                SessionOp::ImportPublishedCheckpoint,
                req,
            )?;
            require_ok(SessionOp::ImportPublishedCheckpoint, res.status)?;
            let meta = res.session.ok_or(CliError::Missing("session metadata"))?;
            println!("{}", meta.session_id);
        }
    }
    Ok(())
}

fn rpc_request<Req, Res>(
    connection: &Connection,
    timeout: Duration,
    op: SessionOp,
    req: Req,
) -> Result<Res, CliError>
where
    Req: prost::Message,
    Res: Default + prost::Message,
{
    let encoded = encode_session_message(&req).map_err(|source| CliError::Encode { op, source })?;
    let message = connection
        .request_timeout(session_subject(op), &encoded, timeout)
        .map_err(|source| CliError::Request { op, source })?;
    let response =
        decode_session_message(&message.data).map_err(|source| CliError::Decode { op, source })?;
    Ok(response)
}

fn require_ok(op: SessionOp, status: Option<pb::OperationStatus>) -> Result<(), CliError> {
    match status {
        Some(st) if st.ok => Ok(()),
        Some(st) => Err(CliError::RemoteFailure {
            op,
            message: st.message,
        }),
        None => Err(CliError::Missing("operation status")),
    }
}

fn password_input(mode: PasswordInput) -> Result<Option<pb::SessionPassword>, CliError> {
    match mode {
        PasswordInput::None => Ok(None),
        PasswordInput::Prompt => {
            eprint!("Password: ");
            io::stderr().flush()?;
            let mut buf = String::new();
            io::stdin().read_line(&mut buf)?;
            Ok(Some(pb::SessionPassword {
                value: buf.trim_end().to_owned(),
            }))
        }
    }
}

fn map_import_mode(mode: ImportModeFlag) -> pb::ImportMode {
    match mode {
        ImportModeFlag::Replace => pb::ImportMode::Replace,
        ImportModeFlag::Merge => pb::ImportMode::Merge,
    }
}

fn map_conflict_policy(policy: ConflictPolicyFlag) -> pb::ConflictPolicy {
    match policy {
        ConflictPolicyFlag::Error => pb::ConflictPolicy::Error,
        ConflictPolicyFlag::KeepLocal => pb::ConflictPolicy::KeepLocal,
        ConflictPolicyFlag::OverwriteRemote => pb::ConflictPolicy::OverwriteRemote,
    }
}

fn render_table<T: Tabled>(rows: Vec<T>) {
    if rows.is_empty() {
        println!("(empty)");
    } else {
        println!("{}", Table::new(rows).with(Style::rounded()));
    }
}

fn render_json<T: Serialize>(value: &T) -> Result<(), CliError> {
    let text = serde_json::to_string_pretty(value)?;
    println!("{text}");
    Ok(())
}

fn render_snapshot(snapshot: &SnapshotView, json: bool) -> Result<(), CliError> {
    if json {
        return render_json(snapshot);
    }

    println!("session: {}", snapshot.metadata.id);
    println!("name: {}", snapshot.metadata.display_name);
    println!("workspace: {}", snapshot.metadata.workspace_name);
    println!("cow_root: {}", snapshot.metadata.cow_root);
    println!("overlay_version: {}", snapshot.overlay_version);
    println!("protected: {}", snapshot.metadata.protected);
    println!(
        "created_at_unix_nanos: {}",
        snapshot.metadata.created_at_unix_nanos
    );
    println!(
        "updated_at_unix_nanos: {}",
        snapshot.metadata.updated_at_unix_nanos
    );

    if snapshot.overlay_entries.is_empty() {
        println!("\noverlay: (empty)");
    } else {
        println!("\noverlay:");
        let rows: Vec<OverlayTableRow> = snapshot
            .overlay_entries
            .iter()
            .map(OverlayTableRow::from)
            .collect();
        render_table(rows);
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct SessionView {
    id: String,
    display_name: String,
    workspace_name: String,
    cow_root: String,
    protected: bool,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
    overlay_version: i64,
}

impl From<&pb::SessionMetadata> for SessionView {
    fn from(meta: &pb::SessionMetadata) -> Self {
        SessionView {
            id: meta.session_id.clone(),
            display_name: meta.display_name.clone(),
            workspace_name: meta.workspace_name.clone(),
            cow_root: meta.cow_root.clone(),
            protected: meta
                .password
                .as_ref()
                .map(|p| p.is_protected)
                .unwrap_or(false),
            created_at_unix_nanos: meta.created_at_unix_nanos,
            updated_at_unix_nanos: meta.updated_at_unix_nanos,
            overlay_version: meta.overlay_version,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct OverlayOriginView {
    session_id: String,
    checkpoint_id: String,
}

#[derive(Debug, Clone, Serialize)]
enum OverlayKind {
    Alias,
    Tombstone,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
struct OverlayEntryView {
    logical_path: String,
    kind: OverlayKind,
    target_path: Option<String>,
    created_at_unix_nanos: i64,
    origin: Option<OverlayOriginView>,
}

impl From<&pb::OverlayEntry> for OverlayEntryView {
    fn from(entry: &pb::OverlayEntry) -> Self {
        match &entry.kind {
            Some(pb::overlay_entry::Kind::Alias(alias)) => OverlayEntryView {
                logical_path: entry.logical_path.clone(),
                kind: OverlayKind::Alias,
                target_path: Some(alias.target_path.clone()),
                created_at_unix_nanos: alias.created_at_unix_nanos,
                origin: alias.origin.as_ref().map(OverlayOriginView::from),
            },
            Some(pb::overlay_entry::Kind::Tombstone(tomb)) => OverlayEntryView {
                logical_path: entry.logical_path.clone(),
                kind: OverlayKind::Tombstone,
                target_path: None,
                created_at_unix_nanos: tomb.created_at_unix_nanos,
                origin: tomb.origin.as_ref().map(OverlayOriginView::from),
            },
            None => OverlayEntryView {
                logical_path: entry.logical_path.clone(),
                kind: OverlayKind::Unknown,
                target_path: None,
                created_at_unix_nanos: 0,
                origin: None,
            },
        }
    }
}

impl From<&pb::OverlayOrigin> for OverlayOriginView {
    fn from(origin: &pb::OverlayOrigin) -> Self {
        OverlayOriginView {
            session_id: origin.session_id.clone(),
            checkpoint_id: origin.checkpoint_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CheckpointView {
    checkpoint_id: String,
    session_id: String,
    label: String,
    created_at_unix_nanos: i64,
}

impl From<&pb::CheckpointMetadata> for CheckpointView {
    fn from(meta: &pb::CheckpointMetadata) -> Self {
        CheckpointView {
            checkpoint_id: meta.checkpoint_id.clone(),
            session_id: meta.session_id.clone(),
            label: meta.label.clone(),
            created_at_unix_nanos: meta.created_at_unix_nanos,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PublishedView {
    remote_checkpoint_id: String,
    session_id: String,
    checkpoint_id: String,
    label: String,
    created_at_unix_nanos: i64,
}

impl TryFrom<&pb::PublishedCheckpoint> for PublishedView {
    type Error = CliError;

    fn try_from(item: &pb::PublishedCheckpoint) -> Result<Self, Self::Error> {
        let checkpoint = item
            .checkpoint
            .as_ref()
            .ok_or(CliError::Missing("published checkpoint metadata"))?;
        Ok(PublishedView {
            remote_checkpoint_id: checkpoint.checkpoint_id.clone(),
            session_id: checkpoint.session_id.clone(),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            label: checkpoint.label.clone(),
            created_at_unix_nanos: checkpoint.created_at_unix_nanos,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotView {
    metadata: SessionView,
    overlay_entries: Vec<OverlayEntryView>,
    overlay_version: i64,
}

impl TryFrom<&pb::SessionSnapshot> for SnapshotView {
    type Error = CliError;

    fn try_from(snapshot: &pb::SessionSnapshot) -> Result<Self, Self::Error> {
        let metadata = snapshot
            .metadata
            .as_ref()
            .ok_or(CliError::Missing("session metadata"))?;
        Ok(SnapshotView {
            metadata: SessionView::from(metadata),
            overlay_entries: snapshot
                .entries
                .iter()
                .map(OverlayEntryView::from)
                .collect(),
            overlay_version: snapshot.overlay_version,
        })
    }
}

#[derive(Debug, Tabled)]
struct SessionTableRow {
    id: String,
    name: String,
    workspace: String,
    cow_root: String,
    protected: bool,
    overlay_version: i64,
}

impl From<&SessionView> for SessionTableRow {
    fn from(view: &SessionView) -> Self {
        SessionTableRow {
            id: view.id.clone(),
            name: view.display_name.clone(),
            workspace: view.workspace_name.clone(),
            cow_root: view.cow_root.clone(),
            protected: view.protected,
            overlay_version: view.overlay_version,
        }
    }
}

#[derive(Debug, Tabled)]
struct OverlayTableRow {
    path: String,
    kind: String,
    target: String,
    origin: String,
    created_at: i64,
}

impl From<&OverlayEntryView> for OverlayTableRow {
    fn from(view: &OverlayEntryView) -> Self {
        let target = view.target_path.clone().unwrap_or_default();
        let kind = match view.kind {
            OverlayKind::Alias => "alias",
            OverlayKind::Tombstone => "tombstone",
            OverlayKind::Unknown => "unknown",
        }
        .to_string();

        let origin = view
            .origin
            .as_ref()
            .map(|o| format!("{}:{}", o.session_id, o.checkpoint_id))
            .unwrap_or_else(|| "-".to_string());

        OverlayTableRow {
            path: view.logical_path.clone(),
            kind,
            target,
            origin,
            created_at: view.created_at_unix_nanos,
        }
    }
}

#[derive(Debug, Tabled)]
struct CheckpointTableRow {
    id: String,
    label: String,
    created_at: i64,
}

impl From<&CheckpointView> for CheckpointTableRow {
    fn from(view: &CheckpointView) -> Self {
        CheckpointTableRow {
            id: view.checkpoint_id.clone(),
            label: view.label.clone(),
            created_at: view.created_at_unix_nanos,
        }
    }
}

#[derive(Debug, Tabled)]
struct PublishedTableRow {
    remote_id: String,
    session_id: String,
    checkpoint_id: String,
    label: String,
    created_at: i64,
}

impl From<&PublishedView> for PublishedTableRow {
    fn from(view: &PublishedView) -> Self {
        PublishedTableRow {
            remote_id: view.remote_checkpoint_id.clone(),
            session_id: view.session_id.clone(),
            checkpoint_id: view.checkpoint_id.clone(),
            label: view.label.clone(),
            created_at: view.created_at_unix_nanos,
        }
    }
}
