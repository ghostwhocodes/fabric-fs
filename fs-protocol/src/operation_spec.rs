use crate::Operation;

pub const OPERATION_COUNT: usize = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageShape {
    LookupRequest,
    LookupResponse,
    GetattrRequest,
    GetattrResponse,
    ReaddirRequest,
    ReaddirResponse,
    OpenRequest,
    OpenResponse,
    ReadRequest,
    ReadResponse,
    WriteRequest,
    WriteResponse,
    CreateRequest,
    CreateResponse,
    RenameRequest,
    UnlinkRequest,
    MkdirRequest,
    RmdirRequest,
    StatfsRequest,
    StatfsResponse,
    GetxattrRequest,
    GetxattrResponse,
    SetxattrRequest,
    ListxattrRequest,
    ListxattrResponse,
    RemovexattrRequest,
    ReleaseRequest,
    ReadlinkRequest,
    ReadlinkResponse,
    SymlinkRequest,
    SymlinkResponse,
    HardlinkRequest,
    HardlinkResponse,
    SetattrRequest,
    SetattrResponse,
    FlushRequest,
    FsyncRequest,
    FsyncdirRequest,
    GetlkRequest,
    GetlkResponse,
    SetlkRequest,
    FlockRequest,
    CopyFileRangeRequest,
    CopyFileRangeResponse,
    FallocateRequest,
    LseekRequest,
    LseekResponse,
    EmptyResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationEffect {
    ReadOnly,
    HandleLifecycle,
    ContentMutation,
    CreateNode,
    DeleteNode,
    RenameNode,
    MetadataMutation,
    XattrMutation,
    Durability,
    LockState,
    SeekState,
}

impl OperationEffect {
    pub fn is_mutation(self) -> bool {
        matches!(
            self,
            Self::ContentMutation
                | Self::CreateNode
                | Self::DeleteNode
                | Self::RenameNode
                | Self::MetadataMutation
                | Self::XattrMutation
                | Self::Durability
                | Self::LockState
        )
    }

    pub fn requires_path_invalidation(self) -> bool {
        matches!(
            self,
            Self::ContentMutation
                | Self::CreateNode
                | Self::DeleteNode
                | Self::RenameNode
                | Self::MetadataMutation
                | Self::XattrMutation
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathRole {
    Target,
    Source,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathRootPolicy {
    Any,
    NonRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PathRoleSpec {
    pub role: PathRole,
    pub field: &'static str,
    pub root: PathRootPolicy,
}

impl PathRoleSpec {
    const fn new(role: PathRole, field: &'static str, root: PathRootPolicy) -> Self {
        Self { role, field, root }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseLimit {
    None,
    RequestedDirectoryEntries,
    RequestedReadBytes,
    RequestWriteBytes,
    RequestedXattrBytes,
    RequestedListxattrBytes,
    RequestedCopyLength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseHandle {
    None,
    OpenedObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationSpec {
    pub operation: Operation,
    pub wire_value: i32,
    pub subject_token: &'static str,
    pub request_shape: MessageShape,
    pub response_shape: MessageShape,
    pub path_roles: &'static [PathRoleSpec],
    pub effect: OperationEffect,
    pub response_limit: ResponseLimit,
    pub response_handle: ResponseHandle,
}

impl OperationSpec {
    pub fn primary_path_role(self) -> Option<PathRole> {
        self.path_roles.first().map(|path| path.role)
    }
}

static TARGET_ANY: [PathRoleSpec; 1] = [PathRoleSpec::new(
    PathRole::Target,
    "path",
    PathRootPolicy::Any,
)];
static TARGET_NON_ROOT: [PathRoleSpec; 1] = [PathRoleSpec::new(
    PathRole::Target,
    "path",
    PathRootPolicy::NonRoot,
)];
static RENAME_PATHS: [PathRoleSpec; 2] = [
    PathRoleSpec::new(PathRole::Source, "old_path", PathRootPolicy::NonRoot),
    PathRoleSpec::new(PathRole::Target, "new_path", PathRootPolicy::NonRoot),
];
static HARDLINK_PATHS: [PathRoleSpec; 2] = [
    PathRoleSpec::new(PathRole::Target, "new_path", PathRootPolicy::NonRoot),
    PathRoleSpec::new(PathRole::Source, "existing_path", PathRootPolicy::NonRoot),
];
static COPY_FILE_RANGE_PATHS: [PathRoleSpec; 2] = [
    PathRoleSpec::new(PathRole::Target, "output_path", PathRootPolicy::Any),
    PathRoleSpec::new(PathRole::Source, "input_path", PathRootPolicy::Any),
];

pub static OPERATION_SPECS: [OperationSpec; OPERATION_COUNT] = [
    OperationSpec {
        operation: Operation::Lookup,
        wire_value: 1,
        subject_token: "lookup",
        request_shape: MessageShape::LookupRequest,
        response_shape: MessageShape::LookupResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Getattr,
        wire_value: 2,
        subject_token: "getattr",
        request_shape: MessageShape::GetattrRequest,
        response_shape: MessageShape::GetattrResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Readdir,
        wire_value: 3,
        subject_token: "readdir",
        request_shape: MessageShape::ReaddirRequest,
        response_shape: MessageShape::ReaddirResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::RequestedDirectoryEntries,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Open,
        wire_value: 4,
        subject_token: "open",
        request_shape: MessageShape::OpenRequest,
        response_shape: MessageShape::OpenResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::HandleLifecycle,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::OpenedObject,
    },
    OperationSpec {
        operation: Operation::Read,
        wire_value: 5,
        subject_token: "read",
        request_shape: MessageShape::ReadRequest,
        response_shape: MessageShape::ReadResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::RequestedReadBytes,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Write,
        wire_value: 6,
        subject_token: "write",
        request_shape: MessageShape::WriteRequest,
        response_shape: MessageShape::WriteResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ContentMutation,
        response_limit: ResponseLimit::RequestWriteBytes,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Create,
        wire_value: 7,
        subject_token: "create",
        request_shape: MessageShape::CreateRequest,
        response_shape: MessageShape::CreateResponse,
        path_roles: &TARGET_NON_ROOT,
        effect: OperationEffect::CreateNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::OpenedObject,
    },
    OperationSpec {
        operation: Operation::Rename,
        wire_value: 8,
        subject_token: "rename",
        request_shape: MessageShape::RenameRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &RENAME_PATHS,
        effect: OperationEffect::RenameNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Unlink,
        wire_value: 9,
        subject_token: "unlink",
        request_shape: MessageShape::UnlinkRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_NON_ROOT,
        effect: OperationEffect::DeleteNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Mkdir,
        wire_value: 10,
        subject_token: "mkdir",
        request_shape: MessageShape::MkdirRequest,
        response_shape: MessageShape::LookupResponse,
        path_roles: &TARGET_NON_ROOT,
        effect: OperationEffect::CreateNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Rmdir,
        wire_value: 11,
        subject_token: "rmdir",
        request_shape: MessageShape::RmdirRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_NON_ROOT,
        effect: OperationEffect::DeleteNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Statfs,
        wire_value: 12,
        subject_token: "statfs",
        request_shape: MessageShape::StatfsRequest,
        response_shape: MessageShape::StatfsResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Getxattr,
        wire_value: 13,
        subject_token: "getxattr",
        request_shape: MessageShape::GetxattrRequest,
        response_shape: MessageShape::GetxattrResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::RequestedXattrBytes,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Setxattr,
        wire_value: 14,
        subject_token: "setxattr",
        request_shape: MessageShape::SetxattrRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::XattrMutation,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Listxattr,
        wire_value: 15,
        subject_token: "listxattr",
        request_shape: MessageShape::ListxattrRequest,
        response_shape: MessageShape::ListxattrResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::RequestedListxattrBytes,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Removexattr,
        wire_value: 16,
        subject_token: "removexattr",
        request_shape: MessageShape::RemovexattrRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::XattrMutation,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Release,
        wire_value: 17,
        subject_token: "release",
        request_shape: MessageShape::ReleaseRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::HandleLifecycle,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Readlink,
        wire_value: 18,
        subject_token: "readlink",
        request_shape: MessageShape::ReadlinkRequest,
        response_shape: MessageShape::ReadlinkResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Symlink,
        wire_value: 19,
        subject_token: "symlink",
        request_shape: MessageShape::SymlinkRequest,
        response_shape: MessageShape::SymlinkResponse,
        path_roles: &TARGET_NON_ROOT,
        effect: OperationEffect::CreateNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Hardlink,
        wire_value: 20,
        subject_token: "hardlink",
        request_shape: MessageShape::HardlinkRequest,
        response_shape: MessageShape::HardlinkResponse,
        path_roles: &HARDLINK_PATHS,
        effect: OperationEffect::CreateNode,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Setattr,
        wire_value: 21,
        subject_token: "setattr",
        request_shape: MessageShape::SetattrRequest,
        response_shape: MessageShape::SetattrResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::MetadataMutation,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Flush,
        wire_value: 22,
        subject_token: "flush",
        request_shape: MessageShape::FlushRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::Durability,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Fsync,
        wire_value: 23,
        subject_token: "fsync",
        request_shape: MessageShape::FsyncRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::Durability,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Fsyncdir,
        wire_value: 24,
        subject_token: "fsyncdir",
        request_shape: MessageShape::FsyncdirRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::Durability,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Getlk,
        wire_value: 25,
        subject_token: "getlk",
        request_shape: MessageShape::GetlkRequest,
        response_shape: MessageShape::GetlkResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ReadOnly,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Setlk,
        wire_value: 26,
        subject_token: "setlk",
        request_shape: MessageShape::SetlkRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::LockState,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Flock,
        wire_value: 27,
        subject_token: "flock",
        request_shape: MessageShape::FlockRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::LockState,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::CopyFileRange,
        wire_value: 28,
        subject_token: "copy_file_range",
        request_shape: MessageShape::CopyFileRangeRequest,
        response_shape: MessageShape::CopyFileRangeResponse,
        path_roles: &COPY_FILE_RANGE_PATHS,
        effect: OperationEffect::ContentMutation,
        response_limit: ResponseLimit::RequestedCopyLength,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Fallocate,
        wire_value: 29,
        subject_token: "fallocate",
        request_shape: MessageShape::FallocateRequest,
        response_shape: MessageShape::EmptyResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::ContentMutation,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
    OperationSpec {
        operation: Operation::Lseek,
        wire_value: 30,
        subject_token: "lseek",
        request_shape: MessageShape::LseekRequest,
        response_shape: MessageShape::LseekResponse,
        path_roles: &TARGET_ANY,
        effect: OperationEffect::SeekState,
        response_limit: ResponseLimit::None,
        response_handle: ResponseHandle::None,
    },
];

pub fn spec_for(operation: Operation) -> &'static OperationSpec {
    let index = operation as usize - 1;
    &OPERATION_SPECS[index]
}

pub fn operation_for_wire_value(value: i32) -> Option<Operation> {
    OPERATION_SPECS
        .iter()
        .find(|spec| spec.wire_value == value)
        .map(|spec| spec.operation)
}

pub fn operation_for_subject_token(token: &str) -> Option<Operation> {
    OPERATION_SPECS
        .iter()
        .find(|spec| spec.subject_token == token)
        .map(|spec| spec.operation)
}
