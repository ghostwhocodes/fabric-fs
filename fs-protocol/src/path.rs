use crate::{pb, ProtocolError};

pub fn path(value: impl Into<String>) -> Result<pb::PathDto, ProtocolError> {
    let value = value.into();
    validate_path_value(&value)?;
    Ok(pb::PathDto { path: value })
}

pub fn validate_rename_paths(old_path: &str, new_path: &str) -> Result<(), ProtocolError> {
    validate_non_root_path_value(old_path)?;
    validate_non_root_path_value(new_path)?;
    if path_is_in_subtree(old_path, new_path) || path_is_in_subtree(new_path, old_path) {
        return Err(ProtocolError::InvalidEnvelope(
            "rename paths must be in disjoint subtrees".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_path_field(value: Option<&pb::PathDto>) -> Result<(), ProtocolError> {
    let value = value.ok_or_else(|| ProtocolError::InvalidEnvelope("missing path DTO".into()))?;
    validate_path_value(&value.path)
}

pub(super) fn validate_non_root_path_field(
    value: Option<&pb::PathDto>,
) -> Result<(), ProtocolError> {
    let value = required_path_value(value)?;
    validate_non_root_path_value(&value.path)
}

pub(super) fn required_path_value(
    value: Option<&pb::PathDto>,
) -> Result<&pb::PathDto, ProtocolError> {
    value.ok_or_else(|| ProtocolError::InvalidEnvelope("missing path DTO".into()))
}

pub(super) fn validate_path_value(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || !value.starts_with('/') || value.as_bytes().contains(&0) {
        return Err(ProtocolError::InvalidPath(value.into()));
    }
    if value == "/" {
        return Ok(());
    }
    for component in value.split('/').skip(1) {
        if component.is_empty() || component == "." || component == ".." {
            return Err(ProtocolError::InvalidPath(value.into()));
        }
    }
    Ok(())
}

pub(super) fn validate_non_root_path_value(value: &str) -> Result<(), ProtocolError> {
    validate_path_value(value)?;
    if value == "/" {
        return Err(ProtocolError::InvalidEnvelope(
            "operation path must not be root".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_required_invalidation_path(
    field: &str,
    value: &str,
) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(format!(
            "{field} is required for invalidation kind"
        )));
    }
    validate_path_value(value)
}

pub(super) fn validate_required_non_root_invalidation_path(
    field: &str,
    value: &str,
) -> Result<(), ProtocolError> {
    validate_required_invalidation_path(field, value)?;
    if value == "/" {
        return Err(ProtocolError::InvalidEnvelope(format!(
            "{field} must not be root for invalidation kind"
        )));
    }
    Ok(())
}

fn path_is_in_subtree(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
