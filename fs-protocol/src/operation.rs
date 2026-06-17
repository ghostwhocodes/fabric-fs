use std::convert::TryFrom;

use crate::operation_spec;
use crate::{OperationSpec, ProtocolError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum Operation {
    Lookup = 1,
    Getattr = 2,
    Readdir = 3,
    Open = 4,
    Read = 5,
    Write = 6,
    Create = 7,
    Rename = 8,
    Unlink = 9,
    Mkdir = 10,
    Rmdir = 11,
    Statfs = 12,
    Getxattr = 13,
    Setxattr = 14,
    Listxattr = 15,
    Removexattr = 16,
    Release = 17,
    Readlink = 18,
    Symlink = 19,
    Hardlink = 20,
    Setattr = 21,
    Flush = 22,
    Fsync = 23,
    Fsyncdir = 24,
    Getlk = 25,
    Setlk = 26,
    Flock = 27,
    CopyFileRange = 28,
    Fallocate = 29,
    Lseek = 30,
}

impl Operation {
    pub const ALL: [Operation; 30] = [
        Operation::Lookup,
        Operation::Getattr,
        Operation::Readdir,
        Operation::Open,
        Operation::Read,
        Operation::Write,
        Operation::Create,
        Operation::Rename,
        Operation::Unlink,
        Operation::Mkdir,
        Operation::Rmdir,
        Operation::Statfs,
        Operation::Getxattr,
        Operation::Setxattr,
        Operation::Listxattr,
        Operation::Removexattr,
        Operation::Release,
        Operation::Readlink,
        Operation::Symlink,
        Operation::Hardlink,
        Operation::Setattr,
        Operation::Flush,
        Operation::Fsync,
        Operation::Fsyncdir,
        Operation::Getlk,
        Operation::Setlk,
        Operation::Flock,
        Operation::CopyFileRange,
        Operation::Fallocate,
        Operation::Lseek,
    ];

    pub fn wire_value(self) -> i32 {
        self.spec().wire_value
    }

    pub fn is_mutation(self) -> bool {
        self.spec().effect.is_mutation()
    }

    pub fn as_str(self) -> &'static str {
        self.spec().subject_token
    }

    pub fn spec(self) -> &'static OperationSpec {
        operation_spec::spec_for(self)
    }

    pub fn from_subject_token(token: &str) -> Option<Self> {
        operation_spec::operation_for_subject_token(token)
    }
}

impl TryFrom<i32> for Operation {
    type Error = ProtocolError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        operation_spec::operation_for_wire_value(value)
            .ok_or(ProtocolError::UnknownOperation(value))
    }
}
