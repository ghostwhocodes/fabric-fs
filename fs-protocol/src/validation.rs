use crate::path::{required_path_value, validate_non_root_path_value, validate_path_field};
use crate::{
    pb, ProtocolError, LOCK_EXCLUSIVE, LOCK_NONBLOCK, LOCK_SHARED, LOCK_UNLOCK, SEEK_CUR,
    SEEK_DATA, SEEK_END, SEEK_HOLE, SEEK_SET,
};

pub(super) fn validate_xattr_name(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(ProtocolError::InvalidEnvelope("invalid xattr name".into()));
    }
    Ok(())
}

pub(super) fn validate_symlink_target(value: &[u8]) -> Result<(), ProtocolError> {
    if value.is_empty() || value.contains(&0) {
        return Err(ProtocolError::InvalidEnvelope(
            "invalid symlink target".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_open_request(value: &pb::OpenRequest) -> Result<(), ProtocolError> {
    validate_path_field(value.path.as_ref())?;
    parse_open_kind(value.kind).map(|_| ())
}

pub(super) fn validate_hardlink_request(value: &pb::HardlinkRequest) -> Result<(), ProtocolError> {
    let existing_path = required_path_value(value.existing_path.as_ref())?;
    let new_path = required_path_value(value.new_path.as_ref())?;
    validate_non_root_path_value(&existing_path.path)?;
    validate_non_root_path_value(&new_path.path)?;
    if existing_path.path == new_path.path {
        return Err(ProtocolError::InvalidEnvelope(
            "hardlink paths must be distinct".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_setattr_request(value: &pb::SetattrRequest) -> Result<(), ProtocolError> {
    validate_path_field(value.path.as_ref())?;
    if value.mode.is_none() && value.uid.is_none() && value.gid.is_none() && value.size.is_none() {
        return Err(ProtocolError::InvalidEnvelope(
            "setattr must request at least one attribute change".into(),
        ));
    }
    if value.mode.is_some_and(|mode| mode > 0o7777) {
        return Err(ProtocolError::InvalidEnvelope(
            "setattr mode exceeds permission bits".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_lock_range(start: u64, end: u64) -> Result<(), ProtocolError> {
    if start > end {
        return Err(ProtocolError::InvalidEnvelope(
            "lock start must be less than or equal to end".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_lock_type(value: i32, allow_unlock: bool) -> Result<(), ProtocolError> {
    let valid = value == libc_f_rdlck()
        || value == libc_f_wrlck()
        || (allow_unlock && value == libc_f_unlck());
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::InvalidEnvelope("invalid lock type".into()))
    }
}

pub(super) fn validate_flock_operation(value: i32) -> Result<(), ProtocolError> {
    let command = value & !LOCK_NONBLOCK;
    let valid = command == LOCK_SHARED || command == LOCK_EXCLUSIVE || command == LOCK_UNLOCK;
    if valid && value & !(LOCK_SHARED | LOCK_EXCLUSIVE | LOCK_NONBLOCK | LOCK_UNLOCK) == 0 {
        Ok(())
    } else {
        Err(ProtocolError::InvalidEnvelope(
            "invalid flock operation".into(),
        ))
    }
}

pub(super) fn validate_file_lock(value: &pb::FileLock) -> Result<(), ProtocolError> {
    validate_lock_range(value.start, value.end)?;
    validate_lock_type(value.typ, false)
}

pub(super) fn validate_copy_file_range_request(
    value: &pb::CopyFileRangeRequest,
) -> Result<(), ProtocolError> {
    validate_path_field(value.input_path.as_ref())?;
    validate_path_field(value.output_path.as_ref())?;
    if value.input_offset < 0 || value.output_offset < 0 {
        return Err(ProtocolError::InvalidEnvelope(
            "copy_file_range offsets must be non-negative".into(),
        ));
    }
    if value.flags != 0 {
        return Err(ProtocolError::InvalidEnvelope(
            "copy_file_range flags must be zero".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_fallocate_request(
    value: &pb::FallocateRequest,
) -> Result<(), ProtocolError> {
    validate_path_field(value.path.as_ref())?;
    if value.offset < 0 || value.length <= 0 || value.mode < 0 {
        return Err(ProtocolError::InvalidEnvelope(
            "fallocate offset, length, and mode are invalid".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_lseek_request(value: &pb::LseekRequest) -> Result<(), ProtocolError> {
    validate_path_field(value.path.as_ref())?;
    match value.whence {
        SEEK_SET | SEEK_DATA | SEEK_HOLE if value.offset < 0 => Err(
            ProtocolError::InvalidEnvelope("lseek offset must be non-negative".into()),
        ),
        SEEK_SET | SEEK_CUR | SEEK_END | SEEK_DATA | SEEK_HOLE => Ok(()),
        _ => Err(ProtocolError::InvalidEnvelope(
            "invalid lseek whence".into(),
        )),
    }
}

pub(super) fn validate_attr_field(value: Option<&pb::FileAttr>) -> Result<(), ProtocolError> {
    validate_attr_field_kind(value, None)
}

pub(super) fn validate_non_directory_attr(
    value: Option<&pb::FileAttr>,
) -> Result<(), ProtocolError> {
    let value = value.ok_or_else(|| ProtocolError::InvalidEnvelope("missing file attr".into()))?;
    if value.inode == 0 {
        return Err(ProtocolError::InvalidEnvelope(
            "inode must be nonzero".into(),
        ));
    }
    let kind = parse_file_kind(value.kind)?;
    if kind == pb::FileKind::Directory {
        return Err(ProtocolError::InvalidEnvelope(
            "hardlink attr kind must not be directory".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_attr_field_kind(
    value: Option<&pb::FileAttr>,
    expected: impl Into<Option<pb::FileKind>>,
) -> Result<(), ProtocolError> {
    let value = value.ok_or_else(|| ProtocolError::InvalidEnvelope("missing file attr".into()))?;
    if value.inode == 0 {
        return Err(ProtocolError::InvalidEnvelope(
            "inode must be nonzero".into(),
        ));
    }
    let kind = parse_file_kind(value.kind)?;
    if let Some(expected) = expected.into() {
        if kind != expected {
            return Err(ProtocolError::InvalidEnvelope(
                "file attr kind does not match operation".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_directory_entry(value: &pb::DirectoryEntry) -> Result<(), ProtocolError> {
    if value.inode == 0
        || value.name.is_empty()
        || value.name == "."
        || value.name == ".."
        || value.name.contains('/')
        || value.name.as_bytes().contains(&0)
    {
        return Err(ProtocolError::InvalidEnvelope(
            "invalid directory entry".into(),
        ));
    }
    validate_file_kind(value.kind)?;
    Ok(())
}

pub(super) fn validate_file_kind(value: i32) -> Result<(), ProtocolError> {
    parse_file_kind(value).map(|_| ())
}

fn parse_file_kind(value: i32) -> Result<pb::FileKind, ProtocolError> {
    match pb::FileKind::try_from(value) {
        Ok(pb::FileKind::Unknown) => Err(ProtocolError::InvalidEnvelope(
            "file kind must not be unknown".into(),
        )),
        Ok(kind) => Ok(kind),
        Err(_) => Err(ProtocolError::UnknownFileKind(value)),
    }
}

fn parse_open_kind(value: i32) -> Result<pb::OpenKind, ProtocolError> {
    match pb::OpenKind::try_from(value) {
        Ok(pb::OpenKind::Unspecified) => Err(ProtocolError::InvalidEnvelope(
            "open kind must not be unspecified".into(),
        )),
        Ok(kind) => Ok(kind),
        Err(_) => Err(ProtocolError::UnknownOpenKind(value)),
    }
}

pub(super) fn listxattr_response_size(names: &[String]) -> usize {
    names.iter().map(|name| name.len() + 1).sum()
}

fn libc_f_rdlck() -> i32 {
    0
}

fn libc_f_wrlck() -> i32 {
    1
}

fn libc_f_unlck() -> i32 {
    2
}
