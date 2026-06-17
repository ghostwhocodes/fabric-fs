use fs_core::{FsError, FsResult, RpcMetadata};
use fs_protocol::{pb, Errno};

use crate::server::{FileLock, FuseContext, Stat, StatFs};

pub(super) fn request_path(path: Option<&pb::PathDto>) -> FsResult<&str> {
    path.map(|path| path.path.as_str())
        .ok_or_else(|| service_error(libc::EINVAL, "missing path"))
}

pub(super) fn caller(metadata: &RpcMetadata) -> Option<FuseContext> {
    metadata.caller.as_ref().map(|caller| FuseContext {
        uid: caller.uid,
        gid: caller.gid,
        pid: caller.pid,
    })
}

pub(super) fn fs_errno(errno: i32) -> FsError {
    service_error(errno, format!("filesystem errno {errno}"))
}

pub(super) fn encoded_xattr_name_size(names: &[String]) -> usize {
    names.iter().map(|name| name.len() + 1).sum()
}

pub(super) fn service_error(errno: i32, message: impl Into<String>) -> FsError {
    FsError::new(errno_to_common(errno), message)
}

pub(super) fn stat_to_attr(inode: u64, stat: Stat) -> pb::FileAttr {
    pb::FileAttr {
        inode,
        size: stat.size,
        kind: file_kind(stat.mode) as i32,
        perm: stat.mode & 0o7777,
        mtime_unix_nanos: unix_nanos(stat.mtime_ns),
        uid: stat.uid,
        gid: stat.gid,
        nlink: stat.nlink,
        atime_unix_nanos: unix_nanos(stat.atime_ns),
        ctime_unix_nanos: unix_nanos(stat.ctime_ns),
        crtime_unix_nanos: 0,
    }
}

pub(super) fn statfs_to_proto(stat: StatFs) -> pb::StatFs {
    pb::StatFs {
        blocks: stat.blocks,
        blocks_free: stat.bfree,
        files: stat.files,
        files_free: stat.ffree,
        block_size: stat.bsize.min(u32::MAX as u64) as u32,
        name_max: stat.namelen.min(u32::MAX as u64) as u32,
        blocks_available: stat.bavail,
        fragment_size: stat.frsize.min(u32::MAX as u64) as u32,
    }
}

pub(super) fn file_lock_to_proto(lock: FileLock) -> pb::FileLock {
    pb::FileLock {
        start: lock.start,
        end: lock.end,
        typ: lock.typ,
        pid: lock.pid,
    }
}

pub(super) fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), name)
    }
}

fn errno_to_common(errno: i32) -> Errno {
    if errno <= 0 {
        return Errno::Io;
    }
    Errno::from_raw(errno).unwrap_or(Errno::Io)
}

fn unix_nanos(value: i64) -> u64 {
    value.max(0) as u64
}

pub(super) fn file_kind(mode: u32) -> pb::FileKind {
    match mode & libc::S_IFMT {
        value if value == libc::S_IFDIR => pb::FileKind::Directory,
        value if value == libc::S_IFLNK => pb::FileKind::Symlink,
        _ => pb::FileKind::File,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_to_attr_preserves_distinct_access_modify_and_change_times() {
        let attr = stat_to_attr(
            42,
            Stat {
                dev: 1,
                ino: 2,
                mode: libc::S_IFREG | 0o640,
                nlink: 3,
                uid: 1000,
                gid: 1001,
                size: 4096,
                atime_ns: 11,
                mtime_ns: 22,
                ctime_ns: 33,
            },
        );

        assert_eq!(attr.atime_unix_nanos, 11);
        assert_eq!(attr.mtime_unix_nanos, 22);
        assert_eq!(attr.ctime_unix_nanos, 33);
        assert_eq!(attr.crtime_unix_nanos, 0);
        assert_eq!(attr.perm, 0o640);
    }

    #[test]
    fn statfs_mapping_preserves_available_blocks_and_fragment_size() {
        let stat = statfs_to_proto(StatFs {
            blocks: 100,
            bfree: 90,
            bavail: 80,
            files: 70,
            ffree: 60,
            bsize: 4096,
            namelen: 255,
            frsize: 1024,
        });

        assert_eq!(stat.blocks_free, 90);
        assert_eq!(stat.blocks_available, 80);
        assert_eq!(stat.block_size, 4096);
        assert_eq!(stat.fragment_size, 1024);
    }
}
