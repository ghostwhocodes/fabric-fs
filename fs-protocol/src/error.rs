use thiserror::Error;

use crate::Operation;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("protobuf decode failed: {0}")]
    Decode(String),
    #[error("protobuf encode failed: {0}")]
    Encode(String),
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u32),
    #[error("unknown operation value {0}")]
    UnknownOperation(i32),
    #[error("invalid errno value {0}")]
    InvalidErrno(i32),
    #[error("unknown invalidation kind value {0}")]
    UnknownInvalidationKind(i32),
    #[error("unknown file kind value {0}")]
    UnknownFileKind(i32),
    #[error("unknown open kind value {0}")]
    UnknownOpenKind(i32),
    #[error("payload operation {payload:?} does not match envelope operation {envelope:?}")]
    PayloadMismatch {
        envelope: Operation,
        payload: Operation,
    },
    #[error("invalid path DTO `{0}`")]
    InvalidPath(String),
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(String),
    #[error("invalid response state: {0}")]
    InvalidResponseState(String),
}
