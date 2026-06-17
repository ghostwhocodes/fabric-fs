pub const PROTOCOL_VERSION: u32 = 2;
pub const OPEN_FLAG_TRUNCATE: u32 = 0o1000;
pub const LOCK_SHARED: i32 = 1;
pub const LOCK_EXCLUSIVE: i32 = 2;
pub const LOCK_NONBLOCK: i32 = 4;
pub const LOCK_UNLOCK: i32 = 8;
pub const SEEK_SET: i32 = 0;
pub const SEEK_CUR: i32 = 1;
pub const SEEK_END: i32 = 2;
pub const SEEK_DATA: i32 = 3;
pub const SEEK_HOLE: i32 = 4;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/fs.protocol.v1.rs"));
}

mod attributes;
mod codec;
mod envelope;
mod errno;
mod error;
mod invalidation;
mod operation;
pub mod operation_spec;
mod path;
mod payload;
mod validation;

pub use attributes::{directory_attr, file_attr};
pub use codec::{
    decode_message, decode_request, decode_response, encode_message, encode_request,
    encode_response,
};
pub use envelope::{RequestEnvelope, ResponseEnvelope};
pub use errno::Errno;
pub use error::ProtocolError;
pub use invalidation::{validate_invalidation, InvalidationKind};
pub use operation::Operation;
pub use operation_spec::{
    MessageShape, OperationEffect, OperationSpec, PathRole, PathRoleSpec, PathRootPolicy,
    ResponseHandle, ResponseLimit, OPERATION_SPECS,
};
pub use path::{path, validate_rename_paths};
pub use payload::{RequestPayload, ResponsePayload};

pub type Observation = pb::Observation;
pub type CallerContext = pb::CallerContext;
pub type TraceContext = pb::TraceContext;
pub type TraceEntry = pb::TraceEntry;
