use prost::Message;
use thiserror::Error;

pub const SESSION_SUBJECT_PREFIX: &str = "fabricfs.session.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionOp {
    CreateSession,
    ListSessions,
    GetSession,
    DeleteSession,
    InitSession,
    UpdateOverlay,
    ListOverlayEntries,
    CheckpointSession,
    ListCheckpoints,
    PublishCheckpoint,
    ListPublishedCheckpoints,
    ImportPublishedCheckpoint,
}

impl SessionOp {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionOp::CreateSession => "CreateSession",
            SessionOp::ListSessions => "ListSessions",
            SessionOp::GetSession => "GetSession",
            SessionOp::DeleteSession => "DeleteSession",
            SessionOp::InitSession => "InitSession",
            SessionOp::UpdateOverlay => "UpdateOverlay",
            SessionOp::ListOverlayEntries => "ListOverlayEntries",
            SessionOp::CheckpointSession => "CheckpointSession",
            SessionOp::ListCheckpoints => "ListCheckpoints",
            SessionOp::PublishCheckpoint => "PublishCheckpoint",
            SessionOp::ListPublishedCheckpoints => "ListPublishedCheckpoints",
            SessionOp::ImportPublishedCheckpoint => "ImportPublishedCheckpoint",
        }
    }

    pub fn subject(self) -> &'static str {
        match self {
            SessionOp::CreateSession => "fabricfs.session.v1.CreateSession",
            SessionOp::ListSessions => "fabricfs.session.v1.ListSessions",
            SessionOp::GetSession => "fabricfs.session.v1.GetSession",
            SessionOp::DeleteSession => "fabricfs.session.v1.DeleteSession",
            SessionOp::InitSession => "fabricfs.session.v1.InitSession",
            SessionOp::UpdateOverlay => "fabricfs.session.v1.UpdateOverlay",
            SessionOp::ListOverlayEntries => "fabricfs.session.v1.ListOverlayEntries",
            SessionOp::CheckpointSession => "fabricfs.session.v1.CheckpointSession",
            SessionOp::ListCheckpoints => "fabricfs.session.v1.ListCheckpoints",
            SessionOp::PublishCheckpoint => "fabricfs.session.v1.PublishCheckpoint",
            SessionOp::ListPublishedCheckpoints => "fabricfs.session.v1.ListPublishedCheckpoints",
            SessionOp::ImportPublishedCheckpoint => "fabricfs.session.v1.ImportPublishedCheckpoint",
        }
    }

    pub fn from_subject(subject: &str) -> Option<Self> {
        use SessionOp::*;
        match subject {
            "fabricfs.session.v1.CreateSession" => Some(CreateSession),
            "fabricfs.session.v1.ListSessions" => Some(ListSessions),
            "fabricfs.session.v1.GetSession" => Some(GetSession),
            "fabricfs.session.v1.DeleteSession" => Some(DeleteSession),
            "fabricfs.session.v1.InitSession" => Some(InitSession),
            "fabricfs.session.v1.UpdateOverlay" => Some(UpdateOverlay),
            "fabricfs.session.v1.ListOverlayEntries" => Some(ListOverlayEntries),
            "fabricfs.session.v1.CheckpointSession" => Some(CheckpointSession),
            "fabricfs.session.v1.ListCheckpoints" => Some(ListCheckpoints),
            "fabricfs.session.v1.PublishCheckpoint" => Some(PublishCheckpoint),
            "fabricfs.session.v1.ListPublishedCheckpoints" => Some(ListPublishedCheckpoints),
            "fabricfs.session.v1.ImportPublishedCheckpoint" => Some(ImportPublishedCheckpoint),
            _ => None,
        }
    }
}

pub fn session_subject(op: SessionOp) -> &'static str {
    op.subject()
}

#[derive(Debug, Error)]
pub enum SessionCodecError {
    #[error("failed to encode session message: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("failed to decode session message: {0}")]
    Decode(#[from] prost::DecodeError),
}

pub fn encode_session_message<M: Message>(message: &M) -> Result<Vec<u8>, SessionCodecError> {
    let mut buf = Vec::new();
    message.encode(&mut buf)?;
    Ok(buf)
}

pub fn decode_session_message<M: Default + Message>(bytes: &[u8]) -> Result<M, SessionCodecError> {
    Ok(M::decode(bytes)?)
}

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/fabricfs.session.v1.rs"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::pb::{
        Alias, CheckpointSessionRequest, OperationStatus, SessionMetadata, SessionPassword,
        SessionSnapshot,
    };

    #[test]
    fn subjects_match_spec_and_round_trip() {
        let expected = [
            (
                SessionOp::CreateSession,
                "fabricfs.session.v1.CreateSession",
            ),
            (SessionOp::ListSessions, "fabricfs.session.v1.ListSessions"),
            (SessionOp::GetSession, "fabricfs.session.v1.GetSession"),
            (
                SessionOp::DeleteSession,
                "fabricfs.session.v1.DeleteSession",
            ),
            (SessionOp::InitSession, "fabricfs.session.v1.InitSession"),
            (
                SessionOp::UpdateOverlay,
                "fabricfs.session.v1.UpdateOverlay",
            ),
            (
                SessionOp::ListOverlayEntries,
                "fabricfs.session.v1.ListOverlayEntries",
            ),
            (
                SessionOp::CheckpointSession,
                "fabricfs.session.v1.CheckpointSession",
            ),
            (
                SessionOp::ListCheckpoints,
                "fabricfs.session.v1.ListCheckpoints",
            ),
            (
                SessionOp::PublishCheckpoint,
                "fabricfs.session.v1.PublishCheckpoint",
            ),
            (
                SessionOp::ListPublishedCheckpoints,
                "fabricfs.session.v1.ListPublishedCheckpoints",
            ),
            (
                SessionOp::ImportPublishedCheckpoint,
                "fabricfs.session.v1.ImportPublishedCheckpoint",
            ),
        ];

        for (op, subject) in expected {
            assert_eq!(op.subject(), subject);
            assert_eq!(session_subject(op), subject);
            assert_eq!(SessionOp::from_subject(subject), Some(op));
        }
        assert_eq!(SessionOp::from_subject("unknown"), None);
    }

    #[test]
    fn encode_decode_round_trip() {
        let req = CheckpointSessionRequest {
            session_id: "s-123".to_string(),
            password: Some(SessionPassword {
                value: "secret".to_string(),
            }),
            label: "pre-merge".to_string(),
        };

        let encoded = encode_session_message(&req).expect("encode ok");
        let decoded: CheckpointSessionRequest =
            decode_session_message(&encoded).expect("decode ok");
        assert_eq!(decoded.session_id, "s-123");
        assert_eq!(decoded.label, "pre-merge");
        assert_eq!(decoded.password.unwrap().value, "secret");
    }

    #[test]
    fn snapshot_encode_decode() {
        let snapshot = SessionSnapshot {
            metadata: Some(SessionMetadata {
                session_id: "s-123".into(),
                display_name: "demo".into(),
                workspace_name: "ws".into(),
                cow_root: "/tmp/demo".into(),
                password: None,
                created_at_unix_nanos: 1,
                updated_at_unix_nanos: 2,
                overlay_version: 7,
            }),
            entries: vec![crate::session::pb::OverlayEntry {
                logical_path: "/foo".into(),
                kind: Some(crate::session::pb::overlay_entry::Kind::Alias(Alias {
                    logical_path: "/foo".into(),
                    target_path: "/bar".into(),
                    created_at_unix_nanos: 3,
                    origin: None,
                })),
            }],
            overlay_version: 7,
        };

        let encoded = encode_session_message(&snapshot).expect("encode ok");
        let decoded: SessionSnapshot = decode_session_message(&encoded).expect("decode ok");
        assert_eq!(decoded.metadata.unwrap().session_id, "s-123");
        assert_eq!(decoded.overlay_version, 7);
        let entry = decoded.entries.first().expect("entry present");
        assert_eq!(entry.logical_path, "/foo");
    }

    #[test]
    fn operation_status_round_trip() {
        let status = OperationStatus {
            ok: false,
            message: "failed".into(),
        };

        let encoded = encode_session_message(&status).expect("encode ok");
        let decoded: OperationStatus = decode_session_message(&encoded).expect("decode ok");
        assert!(!decoded.ok);
        assert_eq!(decoded.message, "failed");
    }
}
