use std::convert::TryFrom;

use crate::path::{
    validate_path_value, validate_rename_paths, validate_required_invalidation_path,
    validate_required_non_root_invalidation_path,
};
use crate::{pb, ProtocolError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum InvalidationKind {
    Create = 1,
    Modify = 2,
    Delete = 3,
    Rename = 4,
    Metadata = 5,
    Xattr = 6,
    FullResync = 7,
}

impl InvalidationKind {
    pub fn wire_value(self) -> i32 {
        self as i32
    }
}

impl TryFrom<i32> for InvalidationKind {
    type Error = ProtocolError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(InvalidationKind::Create),
            2 => Ok(InvalidationKind::Modify),
            3 => Ok(InvalidationKind::Delete),
            4 => Ok(InvalidationKind::Rename),
            5 => Ok(InvalidationKind::Metadata),
            6 => Ok(InvalidationKind::Xattr),
            7 => Ok(InvalidationKind::FullResync),
            other => Err(ProtocolError::UnknownInvalidationKind(other)),
        }
    }
}

pub fn validate_invalidation(value: &pb::Invalidation) -> Result<(), ProtocolError> {
    if value.namespace.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(
            "invalidation namespace must not be empty".into(),
        ));
    }
    let kind = InvalidationKind::try_from(value.kind)?;
    match kind {
        InvalidationKind::Create | InvalidationKind::Delete => {
            validate_required_non_root_invalidation_path("path", &value.path)?;
        }
        InvalidationKind::Modify | InvalidationKind::Metadata | InvalidationKind::Xattr => {
            validate_required_invalidation_path("path", &value.path)?;
        }
        InvalidationKind::Rename => {
            validate_required_non_root_invalidation_path("old_path", &value.old_path)?;
            validate_required_non_root_invalidation_path("new_path", &value.new_path)?;
            validate_rename_paths(&value.old_path, &value.new_path)?;
        }
        InvalidationKind::FullResync => {}
    }
    if !value.path.is_empty() {
        validate_path_value(&value.path)?;
    }
    if !value.old_path.is_empty() {
        validate_path_value(&value.old_path)?;
    }
    if !value.new_path.is_empty() {
        validate_path_value(&value.new_path)?;
    }
    Ok(())
}
