use crate::pb;

pub fn file_attr(inode: u64, kind: pb::FileKind, size: u64) -> pb::FileAttr {
    pb::FileAttr {
        inode,
        size,
        kind: kind as i32,
        perm: 0o644,
        mtime_unix_nanos: 0,
        uid: 0,
        gid: 0,
        nlink: 1,
        atime_unix_nanos: 0,
        ctime_unix_nanos: 0,
        crtime_unix_nanos: 0,
    }
}
pub fn directory_attr(inode: u64) -> pb::FileAttr {
    pb::FileAttr {
        inode,
        size: 0,
        kind: pb::FileKind::Directory as i32,
        perm: 0o755,
        mtime_unix_nanos: 0,
        uid: 0,
        gid: 0,
        nlink: 2,
        atime_unix_nanos: 0,
        ctime_unix_nanos: 0,
        crtime_unix_nanos: 0,
    }
}
