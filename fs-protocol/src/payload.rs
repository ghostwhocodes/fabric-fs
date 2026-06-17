use crate::codec::{decode_message, encode_message};
use crate::path::{
    required_path_value, validate_non_root_path_field, validate_path_field, validate_rename_paths,
};
use crate::validation::{
    listxattr_response_size, validate_attr_field, validate_attr_field_kind,
    validate_copy_file_range_request, validate_directory_entry, validate_fallocate_request,
    validate_file_lock, validate_flock_operation, validate_hardlink_request, validate_lock_range,
    validate_lock_type, validate_lseek_request, validate_non_directory_attr, validate_open_request,
    validate_setattr_request, validate_symlink_target, validate_xattr_name,
};
use crate::{pb, Operation, PathRole, ProtocolError, ResponseLimit};

#[derive(Debug, Clone, PartialEq)]
pub enum RequestPayload {
    Lookup(pb::LookupRequest),
    Getattr(pb::GetattrRequest),
    Readdir(pb::ReaddirRequest),
    Open(pb::OpenRequest),
    Read(pb::ReadRequest),
    Write(pb::WriteRequest),
    Create(pb::CreateRequest),
    Rename(pb::RenameRequest),
    Unlink(pb::UnlinkRequest),
    Mkdir(pb::MkdirRequest),
    Rmdir(pb::RmdirRequest),
    Statfs(pb::StatfsRequest),
    Getxattr(pb::GetxattrRequest),
    Setxattr(pb::SetxattrRequest),
    Listxattr(pb::ListxattrRequest),
    Removexattr(pb::RemovexattrRequest),
    Release(pb::ReleaseRequest),
    Readlink(pb::ReadlinkRequest),
    Symlink(pb::SymlinkRequest),
    Hardlink(pb::HardlinkRequest),
    Setattr(pb::SetattrRequest),
    Flush(pb::FlushRequest),
    Fsync(pb::FsyncRequest),
    Fsyncdir(pb::FsyncdirRequest),
    Getlk(pb::GetlkRequest),
    Setlk(pb::SetlkRequest),
    Flock(pb::FlockRequest),
    CopyFileRange(pb::CopyFileRangeRequest),
    Fallocate(pb::FallocateRequest),
    Lseek(pb::LseekRequest),
}

impl RequestPayload {
    pub fn operation(&self) -> Operation {
        match self {
            RequestPayload::Lookup(_) => Operation::Lookup,
            RequestPayload::Getattr(_) => Operation::Getattr,
            RequestPayload::Readdir(_) => Operation::Readdir,
            RequestPayload::Open(_) => Operation::Open,
            RequestPayload::Read(_) => Operation::Read,
            RequestPayload::Write(_) => Operation::Write,
            RequestPayload::Create(_) => Operation::Create,
            RequestPayload::Rename(_) => Operation::Rename,
            RequestPayload::Unlink(_) => Operation::Unlink,
            RequestPayload::Mkdir(_) => Operation::Mkdir,
            RequestPayload::Rmdir(_) => Operation::Rmdir,
            RequestPayload::Statfs(_) => Operation::Statfs,
            RequestPayload::Getxattr(_) => Operation::Getxattr,
            RequestPayload::Setxattr(_) => Operation::Setxattr,
            RequestPayload::Listxattr(_) => Operation::Listxattr,
            RequestPayload::Removexattr(_) => Operation::Removexattr,
            RequestPayload::Release(_) => Operation::Release,
            RequestPayload::Readlink(_) => Operation::Readlink,
            RequestPayload::Symlink(_) => Operation::Symlink,
            RequestPayload::Hardlink(_) => Operation::Hardlink,
            RequestPayload::Setattr(_) => Operation::Setattr,
            RequestPayload::Flush(_) => Operation::Flush,
            RequestPayload::Fsync(_) => Operation::Fsync,
            RequestPayload::Fsyncdir(_) => Operation::Fsyncdir,
            RequestPayload::Getlk(_) => Operation::Getlk,
            RequestPayload::Setlk(_) => Operation::Setlk,
            RequestPayload::Flock(_) => Operation::Flock,
            RequestPayload::CopyFileRange(_) => Operation::CopyFileRange,
            RequestPayload::Fallocate(_) => Operation::Fallocate,
            RequestPayload::Lseek(_) => Operation::Lseek,
        }
    }

    pub fn path_dto_for_role(&self, role: PathRole) -> Option<&pb::PathDto> {
        match (self, role) {
            (RequestPayload::Lookup(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Getattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Readdir(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Open(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Read(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Write(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Create(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Rename(value), PathRole::Source) => value.old_path.as_ref(),
            (RequestPayload::Rename(value), PathRole::Target) => value.new_path.as_ref(),
            (RequestPayload::Unlink(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Mkdir(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Rmdir(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Statfs(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Getxattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Setxattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Listxattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Removexattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Release(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Readlink(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Symlink(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Hardlink(value), PathRole::Source) => value.existing_path.as_ref(),
            (RequestPayload::Hardlink(value), PathRole::Target) => value.new_path.as_ref(),
            (RequestPayload::Setattr(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Flush(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Fsync(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Fsyncdir(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Getlk(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Setlk(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Flock(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::CopyFileRange(value), PathRole::Source) => value.input_path.as_ref(),
            (RequestPayload::CopyFileRange(value), PathRole::Target) => value.output_path.as_ref(),
            (RequestPayload::Fallocate(value), PathRole::Target) => value.path.as_ref(),
            (RequestPayload::Lseek(value), PathRole::Target) => value.path.as_ref(),
            _ => None,
        }
    }

    pub fn path_for_role(&self, role: PathRole) -> Option<&str> {
        self.path_dto_for_role(role).map(|path| path.path.as_str())
    }

    pub fn primary_path(&self) -> Option<&str> {
        self.operation()
            .spec()
            .primary_path_role()
            .and_then(|role| self.path_for_role(role))
    }

    pub(super) fn encode_payload(&self) -> Result<Vec<u8>, ProtocolError> {
        match self {
            RequestPayload::Lookup(value) => encode_message(value),
            RequestPayload::Getattr(value) => encode_message(value),
            RequestPayload::Readdir(value) => encode_message(value),
            RequestPayload::Open(value) => encode_message(value),
            RequestPayload::Read(value) => encode_message(value),
            RequestPayload::Write(value) => encode_message(value),
            RequestPayload::Create(value) => encode_message(value),
            RequestPayload::Rename(value) => encode_message(value),
            RequestPayload::Unlink(value) => encode_message(value),
            RequestPayload::Mkdir(value) => encode_message(value),
            RequestPayload::Rmdir(value) => encode_message(value),
            RequestPayload::Statfs(value) => encode_message(value),
            RequestPayload::Getxattr(value) => encode_message(value),
            RequestPayload::Setxattr(value) => encode_message(value),
            RequestPayload::Listxattr(value) => encode_message(value),
            RequestPayload::Removexattr(value) => encode_message(value),
            RequestPayload::Release(value) => encode_message(value),
            RequestPayload::Readlink(value) => encode_message(value),
            RequestPayload::Symlink(value) => encode_message(value),
            RequestPayload::Hardlink(value) => encode_message(value),
            RequestPayload::Setattr(value) => encode_message(value),
            RequestPayload::Flush(value) => encode_message(value),
            RequestPayload::Fsync(value) => encode_message(value),
            RequestPayload::Fsyncdir(value) => encode_message(value),
            RequestPayload::Getlk(value) => encode_message(value),
            RequestPayload::Setlk(value) => encode_message(value),
            RequestPayload::Flock(value) => encode_message(value),
            RequestPayload::CopyFileRange(value) => encode_message(value),
            RequestPayload::Fallocate(value) => encode_message(value),
            RequestPayload::Lseek(value) => encode_message(value),
        }
    }

    pub(super) fn decode(operation: Operation, bytes: &[u8]) -> Result<Self, ProtocolError> {
        let payload = match operation {
            Operation::Lookup => RequestPayload::Lookup(decode_message(bytes)?),
            Operation::Getattr => RequestPayload::Getattr(decode_message(bytes)?),
            Operation::Readdir => RequestPayload::Readdir(decode_message(bytes)?),
            Operation::Open => RequestPayload::Open(decode_message(bytes)?),
            Operation::Read => RequestPayload::Read(decode_message(bytes)?),
            Operation::Write => RequestPayload::Write(decode_message(bytes)?),
            Operation::Create => RequestPayload::Create(decode_message(bytes)?),
            Operation::Rename => RequestPayload::Rename(decode_message(bytes)?),
            Operation::Unlink => RequestPayload::Unlink(decode_message(bytes)?),
            Operation::Mkdir => RequestPayload::Mkdir(decode_message(bytes)?),
            Operation::Rmdir => RequestPayload::Rmdir(decode_message(bytes)?),
            Operation::Statfs => RequestPayload::Statfs(decode_message(bytes)?),
            Operation::Getxattr => RequestPayload::Getxattr(decode_message(bytes)?),
            Operation::Setxattr => RequestPayload::Setxattr(decode_message(bytes)?),
            Operation::Listxattr => RequestPayload::Listxattr(decode_message(bytes)?),
            Operation::Removexattr => RequestPayload::Removexattr(decode_message(bytes)?),
            Operation::Release => RequestPayload::Release(decode_message(bytes)?),
            Operation::Readlink => RequestPayload::Readlink(decode_message(bytes)?),
            Operation::Symlink => RequestPayload::Symlink(decode_message(bytes)?),
            Operation::Hardlink => RequestPayload::Hardlink(decode_message(bytes)?),
            Operation::Setattr => RequestPayload::Setattr(decode_message(bytes)?),
            Operation::Flush => RequestPayload::Flush(decode_message(bytes)?),
            Operation::Fsync => RequestPayload::Fsync(decode_message(bytes)?),
            Operation::Fsyncdir => RequestPayload::Fsyncdir(decode_message(bytes)?),
            Operation::Getlk => RequestPayload::Getlk(decode_message(bytes)?),
            Operation::Setlk => RequestPayload::Setlk(decode_message(bytes)?),
            Operation::Flock => RequestPayload::Flock(decode_message(bytes)?),
            Operation::CopyFileRange => RequestPayload::CopyFileRange(decode_message(bytes)?),
            Operation::Fallocate => RequestPayload::Fallocate(decode_message(bytes)?),
            Operation::Lseek => RequestPayload::Lseek(decode_message(bytes)?),
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            RequestPayload::Lookup(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Getattr(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Readdir(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Open(value) => validate_open_request(value),
            RequestPayload::Read(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Write(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Create(value) => validate_non_root_path_field(value.path.as_ref()),
            RequestPayload::Rename(value) => {
                let old_path = required_path_value(value.old_path.as_ref())?;
                let new_path = required_path_value(value.new_path.as_ref())?;
                validate_rename_paths(&old_path.path, &new_path.path)
            }
            RequestPayload::Unlink(value) => validate_non_root_path_field(value.path.as_ref()),
            RequestPayload::Mkdir(value) => validate_non_root_path_field(value.path.as_ref()),
            RequestPayload::Rmdir(value) => validate_non_root_path_field(value.path.as_ref()),
            RequestPayload::Statfs(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Getxattr(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_xattr_name(&value.name)
            }
            RequestPayload::Setxattr(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_xattr_name(&value.name)
            }
            RequestPayload::Listxattr(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Removexattr(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_xattr_name(&value.name)
            }
            RequestPayload::Release(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Readlink(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Symlink(value) => {
                validate_non_root_path_field(value.path.as_ref())?;
                validate_symlink_target(&value.target)
            }
            RequestPayload::Hardlink(value) => validate_hardlink_request(value),
            RequestPayload::Setattr(value) => validate_setattr_request(value),
            RequestPayload::Flush(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Fsync(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Fsyncdir(value) => validate_path_field(value.path.as_ref()),
            RequestPayload::Getlk(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_lock_range(value.start, value.end)?;
                validate_lock_type(value.typ, false)
            }
            RequestPayload::Setlk(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_lock_range(value.start, value.end)?;
                validate_lock_type(value.typ, true)
            }
            RequestPayload::Flock(value) => {
                validate_path_field(value.path.as_ref())?;
                validate_flock_operation(value.operation)
            }
            RequestPayload::CopyFileRange(value) => validate_copy_file_range_request(value),
            RequestPayload::Fallocate(value) => validate_fallocate_request(value),
            RequestPayload::Lseek(value) => validate_lseek_request(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponsePayload {
    Lookup(pb::LookupResponse),
    Getattr(pb::GetattrResponse),
    Readdir(pb::ReaddirResponse),
    Open(pb::OpenResponse),
    Read(pb::ReadResponse),
    Write(pb::WriteResponse),
    Create(pb::CreateResponse),
    Rename(pb::EmptyResponse),
    Unlink(pb::EmptyResponse),
    Mkdir(pb::LookupResponse),
    Rmdir(pb::EmptyResponse),
    Statfs(pb::StatfsResponse),
    Getxattr(pb::GetxattrResponse),
    Setxattr(pb::EmptyResponse),
    Listxattr(pb::ListxattrResponse),
    Removexattr(pb::EmptyResponse),
    Release(pb::EmptyResponse),
    Readlink(pb::ReadlinkResponse),
    Symlink(pb::SymlinkResponse),
    Hardlink(pb::HardlinkResponse),
    Setattr(pb::SetattrResponse),
    Flush(pb::EmptyResponse),
    Fsync(pb::EmptyResponse),
    Fsyncdir(pb::EmptyResponse),
    Getlk(pb::GetlkResponse),
    Setlk(pb::EmptyResponse),
    Flock(pb::EmptyResponse),
    CopyFileRange(pb::CopyFileRangeResponse),
    Fallocate(pb::EmptyResponse),
    Lseek(pb::LseekResponse),
}

impl ResponsePayload {
    pub fn operation(&self) -> Operation {
        match self {
            ResponsePayload::Lookup(_) => Operation::Lookup,
            ResponsePayload::Getattr(_) => Operation::Getattr,
            ResponsePayload::Readdir(_) => Operation::Readdir,
            ResponsePayload::Open(_) => Operation::Open,
            ResponsePayload::Read(_) => Operation::Read,
            ResponsePayload::Write(_) => Operation::Write,
            ResponsePayload::Create(_) => Operation::Create,
            ResponsePayload::Rename(_) => Operation::Rename,
            ResponsePayload::Unlink(_) => Operation::Unlink,
            ResponsePayload::Mkdir(_) => Operation::Mkdir,
            ResponsePayload::Rmdir(_) => Operation::Rmdir,
            ResponsePayload::Statfs(_) => Operation::Statfs,
            ResponsePayload::Getxattr(_) => Operation::Getxattr,
            ResponsePayload::Setxattr(_) => Operation::Setxattr,
            ResponsePayload::Listxattr(_) => Operation::Listxattr,
            ResponsePayload::Removexattr(_) => Operation::Removexattr,
            ResponsePayload::Release(_) => Operation::Release,
            ResponsePayload::Readlink(_) => Operation::Readlink,
            ResponsePayload::Symlink(_) => Operation::Symlink,
            ResponsePayload::Hardlink(_) => Operation::Hardlink,
            ResponsePayload::Setattr(_) => Operation::Setattr,
            ResponsePayload::Flush(_) => Operation::Flush,
            ResponsePayload::Fsync(_) => Operation::Fsync,
            ResponsePayload::Fsyncdir(_) => Operation::Fsyncdir,
            ResponsePayload::Getlk(_) => Operation::Getlk,
            ResponsePayload::Setlk(_) => Operation::Setlk,
            ResponsePayload::Flock(_) => Operation::Flock,
            ResponsePayload::CopyFileRange(_) => Operation::CopyFileRange,
            ResponsePayload::Fallocate(_) => Operation::Fallocate,
            ResponsePayload::Lseek(_) => Operation::Lseek,
        }
    }

    pub fn created_inode(&self) -> Option<u64> {
        match self {
            ResponsePayload::Create(value) => value.attr.as_ref().map(|attr| attr.inode),
            ResponsePayload::Mkdir(value) => value.attr.as_ref().map(|attr| attr.inode),
            ResponsePayload::Symlink(value) => value.attr.as_ref().map(|attr| attr.inode),
            ResponsePayload::Hardlink(value) => value.attr.as_ref().map(|attr| attr.inode),
            _ => None,
        }
    }

    pub fn opened_handle(&self) -> Option<u64> {
        match self {
            ResponsePayload::Open(value) => Some(value.handle),
            ResponsePayload::Create(value) => Some(value.handle),
            _ => None,
        }
    }

    pub(super) fn encode_payload(&self) -> Result<Vec<u8>, ProtocolError> {
        match self {
            ResponsePayload::Lookup(value) => encode_message(value),
            ResponsePayload::Getattr(value) => encode_message(value),
            ResponsePayload::Readdir(value) => encode_message(value),
            ResponsePayload::Open(value) => encode_message(value),
            ResponsePayload::Read(value) => encode_message(value),
            ResponsePayload::Write(value) => encode_message(value),
            ResponsePayload::Create(value) => encode_message(value),
            ResponsePayload::Rename(value) => encode_message(value),
            ResponsePayload::Unlink(value) => encode_message(value),
            ResponsePayload::Mkdir(value) => encode_message(value),
            ResponsePayload::Rmdir(value) => encode_message(value),
            ResponsePayload::Statfs(value) => encode_message(value),
            ResponsePayload::Getxattr(value) => encode_message(value),
            ResponsePayload::Setxattr(value) => encode_message(value),
            ResponsePayload::Listxattr(value) => encode_message(value),
            ResponsePayload::Removexattr(value) => encode_message(value),
            ResponsePayload::Release(value) => encode_message(value),
            ResponsePayload::Readlink(value) => encode_message(value),
            ResponsePayload::Symlink(value) => encode_message(value),
            ResponsePayload::Hardlink(value) => encode_message(value),
            ResponsePayload::Setattr(value) => encode_message(value),
            ResponsePayload::Flush(value) => encode_message(value),
            ResponsePayload::Fsync(value) => encode_message(value),
            ResponsePayload::Fsyncdir(value) => encode_message(value),
            ResponsePayload::Getlk(value) => encode_message(value),
            ResponsePayload::Setlk(value) => encode_message(value),
            ResponsePayload::Flock(value) => encode_message(value),
            ResponsePayload::CopyFileRange(value) => encode_message(value),
            ResponsePayload::Fallocate(value) => encode_message(value),
            ResponsePayload::Lseek(value) => encode_message(value),
        }
    }

    pub(super) fn decode(operation: Operation, bytes: &[u8]) -> Result<Self, ProtocolError> {
        let payload = match operation {
            Operation::Lookup => ResponsePayload::Lookup(decode_message(bytes)?),
            Operation::Getattr => ResponsePayload::Getattr(decode_message(bytes)?),
            Operation::Readdir => ResponsePayload::Readdir(decode_message(bytes)?),
            Operation::Open => ResponsePayload::Open(decode_message(bytes)?),
            Operation::Read => ResponsePayload::Read(decode_message(bytes)?),
            Operation::Write => ResponsePayload::Write(decode_message(bytes)?),
            Operation::Create => ResponsePayload::Create(decode_message(bytes)?),
            Operation::Rename => ResponsePayload::Rename(decode_message(bytes)?),
            Operation::Unlink => ResponsePayload::Unlink(decode_message(bytes)?),
            Operation::Mkdir => ResponsePayload::Mkdir(decode_message(bytes)?),
            Operation::Rmdir => ResponsePayload::Rmdir(decode_message(bytes)?),
            Operation::Statfs => ResponsePayload::Statfs(decode_message(bytes)?),
            Operation::Getxattr => ResponsePayload::Getxattr(decode_message(bytes)?),
            Operation::Setxattr => ResponsePayload::Setxattr(decode_message(bytes)?),
            Operation::Listxattr => ResponsePayload::Listxattr(decode_message(bytes)?),
            Operation::Removexattr => ResponsePayload::Removexattr(decode_message(bytes)?),
            Operation::Release => ResponsePayload::Release(decode_message(bytes)?),
            Operation::Readlink => ResponsePayload::Readlink(decode_message(bytes)?),
            Operation::Symlink => ResponsePayload::Symlink(decode_message(bytes)?),
            Operation::Hardlink => ResponsePayload::Hardlink(decode_message(bytes)?),
            Operation::Setattr => ResponsePayload::Setattr(decode_message(bytes)?),
            Operation::Flush => ResponsePayload::Flush(decode_message(bytes)?),
            Operation::Fsync => ResponsePayload::Fsync(decode_message(bytes)?),
            Operation::Fsyncdir => ResponsePayload::Fsyncdir(decode_message(bytes)?),
            Operation::Getlk => ResponsePayload::Getlk(decode_message(bytes)?),
            Operation::Setlk => ResponsePayload::Setlk(decode_message(bytes)?),
            Operation::Flock => ResponsePayload::Flock(decode_message(bytes)?),
            Operation::CopyFileRange => ResponsePayload::CopyFileRange(decode_message(bytes)?),
            Operation::Fallocate => ResponsePayload::Fallocate(decode_message(bytes)?),
            Operation::Lseek => ResponsePayload::Lseek(decode_message(bytes)?),
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            ResponsePayload::Lookup(value) => validate_attr_field(value.attr.as_ref()),
            ResponsePayload::Getattr(value) => validate_attr_field(value.attr.as_ref()),
            ResponsePayload::Readdir(value) => {
                for entry in &value.entries {
                    validate_directory_entry(entry)?;
                }
                Ok(())
            }
            ResponsePayload::Open(_) => Ok(()),
            ResponsePayload::Read(_) => Ok(()),
            ResponsePayload::Write(_) => Ok(()),
            ResponsePayload::Create(value) => {
                validate_attr_field_kind(value.attr.as_ref(), pb::FileKind::File)
            }
            ResponsePayload::Rename(_) => Ok(()),
            ResponsePayload::Unlink(_) => Ok(()),
            ResponsePayload::Mkdir(value) => {
                validate_attr_field_kind(value.attr.as_ref(), pb::FileKind::Directory)
            }
            ResponsePayload::Rmdir(_) => Ok(()),
            ResponsePayload::Statfs(value) => {
                if value.stat.is_some() {
                    Ok(())
                } else {
                    Err(ProtocolError::InvalidEnvelope(
                        "missing statfs payload".into(),
                    ))
                }
            }
            ResponsePayload::Getxattr(_) => Ok(()),
            ResponsePayload::Setxattr(_) => Ok(()),
            ResponsePayload::Listxattr(value) => {
                for name in &value.names {
                    validate_xattr_name(name)?;
                }
                Ok(())
            }
            ResponsePayload::Removexattr(_) => Ok(()),
            ResponsePayload::Release(_) => Ok(()),
            ResponsePayload::Readlink(value) => validate_symlink_target(&value.target),
            ResponsePayload::Symlink(value) => {
                validate_attr_field_kind(value.attr.as_ref(), pb::FileKind::Symlink)
            }
            ResponsePayload::Hardlink(value) => validate_non_directory_attr(value.attr.as_ref()),
            ResponsePayload::Setattr(value) => validate_attr_field(value.attr.as_ref()),
            ResponsePayload::Flush(_) => Ok(()),
            ResponsePayload::Fsync(_) => Ok(()),
            ResponsePayload::Fsyncdir(_) => Ok(()),
            ResponsePayload::Getlk(value) => {
                if let Some(lock) = value.lock.as_ref() {
                    validate_file_lock(lock)?;
                }
                Ok(())
            }
            ResponsePayload::Setlk(_) => Ok(()),
            ResponsePayload::Flock(_) => Ok(()),
            ResponsePayload::CopyFileRange(_) => Ok(()),
            ResponsePayload::Fallocate(_) => Ok(()),
            ResponsePayload::Lseek(value) => {
                if value.offset < 0 {
                    Err(ProtocolError::InvalidResponseState(
                        "lseek response offset must be non-negative".into(),
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    pub fn validate_for_request(&self, request: &RequestPayload) -> Result<(), ProtocolError> {
        if self.operation() != request.operation() {
            return Err(ProtocolError::PayloadMismatch {
                envelope: request.operation(),
                payload: self.operation(),
            });
        }
        self.validate()?;
        match request.operation().spec().response_limit {
            ResponseLimit::None => Ok(()),
            ResponseLimit::RequestedDirectoryEntries => match (request, self) {
                (RequestPayload::Readdir(request), ResponsePayload::Readdir(response))
                    if response.entries.len() > request.max_entries as usize =>
                {
                    Err(ProtocolError::InvalidResponseState(
                        "readdir response exceeded requested max_entries".into(),
                    ))
                }
                _ => Ok(()),
            },
            ResponseLimit::RequestedReadBytes => match (request, self) {
                (RequestPayload::Read(request), ResponsePayload::Read(response))
                    if response.data.len() > request.size as usize =>
                {
                    Err(ProtocolError::InvalidResponseState(
                        "read response exceeded requested size".into(),
                    ))
                }
                _ => Ok(()),
            },
            ResponseLimit::RequestWriteBytes => match (request, self) {
                (RequestPayload::Write(request), ResponsePayload::Write(response))
                    if response.bytes_written as usize > request.data.len() =>
                {
                    Err(ProtocolError::InvalidResponseState(
                        "write response exceeded request data length".into(),
                    ))
                }
                _ => Ok(()),
            },
            ResponseLimit::RequestedXattrBytes => match (request, self) {
                (RequestPayload::Getxattr(request), ResponsePayload::Getxattr(response))
                    if request.size != 0 && response.value.len() > request.size as usize =>
                {
                    Err(ProtocolError::InvalidResponseState(
                        "getxattr response exceeded requested size".into(),
                    ))
                }
                _ => Ok(()),
            },
            ResponseLimit::RequestedListxattrBytes => match (request, self) {
                (RequestPayload::Listxattr(request), ResponsePayload::Listxattr(response))
                    if request.size != 0
                        && listxattr_response_size(&response.names) > request.size as usize =>
                {
                    Err(ProtocolError::InvalidResponseState(
                        "listxattr response exceeded requested size".into(),
                    ))
                }
                _ => Ok(()),
            },
            ResponseLimit::RequestedCopyLength => match (request, self) {
                (
                    RequestPayload::CopyFileRange(request),
                    ResponsePayload::CopyFileRange(response),
                ) if response.bytes_copied > request.length => {
                    Err(ProtocolError::InvalidResponseState(
                        "copy_file_range response exceeded requested length".into(),
                    ))
                }
                _ => Ok(()),
            },
        }
    }
}
