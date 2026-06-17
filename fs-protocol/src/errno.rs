use std::convert::TryFrom;

use crate::ProtocolError;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Errno(i32);

#[allow(non_upper_case_globals)]
impl Errno {
    pub const Success: Self = Self(0);
    pub const OperationNotPermitted: Self = Self(1);
    pub const NotFound: Self = Self(2);
    pub const Io: Self = Self(5);
    pub const BadFileDescriptor: Self = Self(9);
    pub const WouldBlock: Self = Self(11);
    pub const PermissionDenied: Self = Self(13);
    pub const AlreadyExists: Self = Self(17);
    pub const NotDirectory: Self = Self(20);
    pub const IsDirectory: Self = Self(21);
    pub const InvalidArgument: Self = Self(22);
    pub const NoSpace: Self = Self(28);
    pub const Range: Self = Self(34);
    pub const NotEmpty: Self = Self(39);
    pub const NoData: Self = Self(61);
    pub const MessageTooLarge: Self = Self(90);
    pub const NotSupported: Self = Self(95);
    pub const ConnectionReset: Self = Self(104);
    pub const NoBufferSpace: Self = Self(105);
    pub const TimedOut: Self = Self(110);
    pub const Stale: Self = Self(116);

    pub fn from_raw(value: i32) -> Result<Self, ProtocolError> {
        Self::try_from(value)
    }

    pub fn wire_value(self) -> i32 {
        self.0
    }

    pub fn is_error(self) -> bool {
        self.0 > 0
    }

    fn name(self) -> Option<&'static str> {
        match self.0 {
            0 => Some("Success"),
            1 => Some("OperationNotPermitted"),
            2 => Some("NotFound"),
            5 => Some("Io"),
            9 => Some("BadFileDescriptor"),
            11 => Some("WouldBlock"),
            13 => Some("PermissionDenied"),
            17 => Some("AlreadyExists"),
            20 => Some("NotDirectory"),
            21 => Some("IsDirectory"),
            22 => Some("InvalidArgument"),
            28 => Some("NoSpace"),
            34 => Some("Range"),
            39 => Some("NotEmpty"),
            61 => Some("NoData"),
            90 => Some("MessageTooLarge"),
            95 => Some("NotSupported"),
            104 => Some("ConnectionReset"),
            105 => Some("NoBufferSpace"),
            110 => Some("TimedOut"),
            116 => Some("Stale"),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Errno {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => formatter.write_str(name),
            None => formatter.debug_tuple("Errno").field(&self.0).finish(),
        }
    }
}

impl TryFrom<i32> for Errno {
    type Error = ProtocolError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(ProtocolError::InvalidErrno(value))
        } else {
            Ok(Self(value))
        }
    }
}
