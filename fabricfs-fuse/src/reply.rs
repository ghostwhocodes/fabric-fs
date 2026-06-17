use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs_fuse::{FuseEntry, FuseError, FuseHandle, FuseStatFs, FuseWrite};
use fs_protocol::pb;
use fuser::{
    FileAttr, FileType, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, TimeOrNow,
};

const TTL: Duration = Duration::from_secs(1);
const READDIR_MAX_ENTRIES: u32 = 1024;
const MAX_REPLY_WRITE_BYTES: u64 = u32::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductFuseReplyPresenter {
    ttl: Duration,
}

impl Default for ProductFuseReplyPresenter {
    fn default() -> Self {
        Self { ttl: TTL }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductSetattrRequest {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub handle: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductReaddirRequest {
    pub kernel_offset: u64,
    pub server_offset: u64,
    pub max_entries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductCopyFileRangeRequest {
    pub input_offset: i64,
    pub output_offset: i64,
    pub length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductFallocateRequest {
    pub offset: i64,
    pub length: i64,
    pub mode: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductLseekRequest {
    pub offset: i64,
    pub whence: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStatfsReply {
    pub blocks: u64,
    pub blocks_free: u64,
    pub blocks_available: u64,
    pub files: u64,
    pub files_free: u64,
    pub block_size: u32,
    pub name_max: u32,
    pub fragment_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductXattrReply {
    Size(u32),
    Data(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductLockReply {
    pub start: u64,
    pub end: u64,
    pub typ: i32,
    pub pid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductDirectoryReplyEntry {
    pub inode: u64,
    pub offset: i64,
    pub kind: FileType,
    pub name: String,
}

impl ProductFuseReplyPresenter {
    pub fn required_name<'a>(&self, value: &'a OsStr) -> Result<&'a str, libc::c_int> {
        value
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or(libc::EINVAL)
    }

    pub fn rename_names<'a>(
        &self,
        name: &'a OsStr,
        new_name: &'a OsStr,
        flags: u32,
    ) -> Result<(&'a str, &'a str), libc::c_int> {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        Ok((self.required_name(name)?, self.required_name(new_name)?))
    }

    pub fn setxattr_name<'a>(
        &self,
        name: &'a OsStr,
        position: u32,
    ) -> Result<&'a str, libc::c_int> {
        if position != 0 {
            return Err(libc::ENOTSUP);
        }
        self.required_name(name)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn setattr_request(
        &self,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        handle: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<ProductSetattrRequest, libc::c_int> {
        if atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some()
            || flags.is_some()
        {
            return Err(libc::ENOTSUP);
        }
        if mode.is_none() && uid.is_none() && gid.is_none() && size.is_none() {
            return Err(libc::EINVAL);
        }
        Ok(ProductSetattrRequest {
            mode: mode.map(|mode| mode & 0o7777),
            uid,
            gid,
            size,
            handle,
        })
    }

    pub fn readdir_request(&self, offset: i64) -> Result<ProductReaddirRequest, libc::c_int> {
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let kernel_offset = offset as u64;
        Ok(ProductReaddirRequest {
            kernel_offset,
            server_offset: kernel_offset.saturating_sub(2),
            max_entries: READDIR_MAX_ENTRIES,
        })
    }

    pub fn non_negative_offset(&self, offset: i64) -> Result<u64, libc::c_int> {
        u64::try_from(offset).map_err(|_| libc::EINVAL)
    }

    pub fn copy_file_range_request(
        &self,
        input_offset: i64,
        output_offset: i64,
        length: u64,
    ) -> Result<ProductCopyFileRangeRequest, libc::c_int> {
        if input_offset < 0 || output_offset < 0 {
            return Err(libc::EINVAL);
        }
        Ok(ProductCopyFileRangeRequest {
            input_offset,
            output_offset,
            length: length.min(MAX_REPLY_WRITE_BYTES),
        })
    }

    pub fn fallocate_request(
        &self,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<ProductFallocateRequest, libc::c_int> {
        if offset < 0 || length <= 0 || mode < 0 {
            return Err(libc::EINVAL);
        }
        Ok(ProductFallocateRequest {
            offset,
            length,
            mode,
        })
    }

    pub fn lseek_request(
        &self,
        offset: i64,
        whence: i32,
    ) -> Result<ProductLseekRequest, libc::c_int> {
        match whence {
            libc::SEEK_SET | libc::SEEK_DATA | libc::SEEK_HOLE if offset < 0 => Err(libc::EINVAL),
            libc::SEEK_SET
            | libc::SEEK_CUR
            | libc::SEEK_END
            | libc::SEEK_DATA
            | libc::SEEK_HOLE => Ok(ProductLseekRequest { offset, whence }),
            _ => Err(libc::EINVAL),
        }
    }

    pub fn errno(&self, error: FuseError) -> libc::c_int {
        error.errno().wire_value()
    }

    pub fn file_attr(&self, attr: pb::FileAttr) -> FileAttr {
        FileAttr {
            ino: attr.inode,
            size: attr.size,
            blocks: attr.size.div_ceil(512),
            atime: timestamp(attr.atime_unix_nanos),
            mtime: timestamp(attr.mtime_unix_nanos),
            ctime: timestamp(attr.ctime_unix_nanos),
            crtime: timestamp(attr.crtime_unix_nanos),
            kind: file_type(attr.kind),
            perm: attr.perm as u16,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    pub fn statfs_fields(&self, stat: pb::StatFs) -> ProductStatfsReply {
        ProductStatfsReply {
            blocks: stat.blocks,
            blocks_free: stat.blocks_free,
            blocks_available: stat.blocks_available,
            files: stat.files,
            files_free: stat.files_free,
            block_size: stat.block_size,
            name_max: stat.name_max,
            fragment_size: stat.fragment_size,
        }
    }

    pub fn xattr_value(
        &self,
        value: Vec<u8>,
        requested_size: u32,
    ) -> Result<ProductXattrReply, libc::c_int> {
        if requested_size == 0 {
            return Ok(ProductXattrReply::Size(value.len() as u32));
        }
        if value.len() > requested_size as usize {
            return Err(libc::ERANGE);
        }
        Ok(ProductXattrReply::Data(value))
    }

    pub fn listxattr_names(
        &self,
        names: Vec<String>,
        requested_size: u32,
    ) -> Result<ProductXattrReply, libc::c_int> {
        self.xattr_value(encode_xattr_names(names), requested_size)
    }

    pub fn lock_reply(&self, lock: Option<pb::FileLock>) -> ProductLockReply {
        match lock {
            Some(lock) => ProductLockReply {
                start: lock.start,
                end: lock.end,
                typ: lock.typ,
                pid: lock.pid,
            },
            None => ProductLockReply {
                start: 0,
                end: 0,
                typ: libc::F_UNLCK,
                pid: 0,
            },
        }
    }

    pub fn copy_file_range_reply_bytes(&self, bytes_copied: u64) -> Result<u32, libc::c_int> {
        u32::try_from(bytes_copied).map_err(|_| libc::EINVAL)
    }

    pub fn directory_entries(
        &self,
        inode: u64,
        parent_inode: u64,
        kernel_offset: u64,
        entries: Vec<pb::DirectoryEntry>,
    ) -> Vec<ProductDirectoryReplyEntry> {
        let mut reply_entries = Vec::new();
        if kernel_offset == 0 {
            reply_entries.push(ProductDirectoryReplyEntry {
                inode,
                offset: 1,
                kind: FileType::Directory,
                name: ".".into(),
            });
        }
        if kernel_offset <= 1 {
            reply_entries.push(ProductDirectoryReplyEntry {
                inode: parent_inode,
                offset: 2,
                kind: FileType::Directory,
                name: "..".into(),
            });
        }

        let server_offset = kernel_offset.saturating_sub(2);
        reply_entries.extend(entries.into_iter().enumerate().map(|(index, entry)| {
            ProductDirectoryReplyEntry {
                inode: entry.inode,
                offset: (server_offset + index as u64 + 3).min(i64::MAX as u64) as i64,
                kind: file_type(entry.kind),
                name: entry.name,
            }
        }));
        reply_entries
    }

    pub fn reply_entry(&self, reply: ReplyEntry, result: Result<FuseEntry, FuseError>) {
        match result {
            Ok(entry) => reply.entry(&self.ttl, &self.file_attr(entry.attr), 0),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_entry_errno(&self, reply: ReplyEntry, result: Result<FuseEntry, libc::c_int>) {
        match result {
            Ok(entry) => reply.entry(&self.ttl, &self.file_attr(entry.attr), 0),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_attr(&self, reply: ReplyAttr, result: Result<FuseEntry, FuseError>) {
        match result {
            Ok(entry) => reply.attr(&self.ttl, &self.file_attr(entry.attr)),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_attr_errno(&self, reply: ReplyAttr, result: Result<FuseEntry, libc::c_int>) {
        match result {
            Ok(entry) => reply.attr(&self.ttl, &self.file_attr(entry.attr)),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_data(&self, reply: ReplyData, result: Result<Vec<u8>, FuseError>) {
        match result {
            Ok(data) => reply.data(&data),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_data_errno(&self, reply: ReplyData, result: Result<Vec<u8>, libc::c_int>) {
        match result {
            Ok(data) => reply.data(&data),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_empty(&self, reply: ReplyEmpty, result: Result<(), FuseError>) {
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_empty_errno(&self, reply: ReplyEmpty, result: Result<(), libc::c_int>) {
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_open(&self, reply: ReplyOpen, result: Result<FuseHandle, FuseError>) {
        match result {
            Ok(handle) => reply.opened(handle.handle, handle.flags),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_write(&self, reply: ReplyWrite, result: Result<FuseWrite, FuseError>) {
        match result {
            Ok(written) => reply.written(written.bytes_written),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_write_bytes(&self, reply: ReplyWrite, result: Result<u32, libc::c_int>) {
        match result {
            Ok(bytes) => reply.written(bytes),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_created(
        &self,
        reply: ReplyCreate,
        result: Result<(FuseEntry, FuseHandle), FuseError>,
    ) {
        match result {
            Ok((entry, handle)) => reply.created(
                &self.ttl,
                &self.file_attr(entry.attr),
                0,
                handle.handle,
                handle.flags,
            ),
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_created_errno(
        &self,
        reply: ReplyCreate,
        result: Result<(FuseEntry, FuseHandle), libc::c_int>,
    ) {
        match result {
            Ok((entry, handle)) => reply.created(
                &self.ttl,
                &self.file_attr(entry.attr),
                0,
                handle.handle,
                handle.flags,
            ),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_statfs(&self, reply: ReplyStatfs, result: Result<FuseStatFs, FuseError>) {
        match result {
            Ok(stat) => {
                let fields = self.statfs_fields(stat.stat);
                reply.statfs(
                    fields.blocks,
                    fields.blocks_free,
                    fields.blocks_available,
                    fields.files,
                    fields.files_free,
                    fields.block_size,
                    fields.name_max,
                    fields.fragment_size,
                )
            }
            Err(error) => reply.error(self.errno(error)),
        }
    }

    pub fn reply_xattr(&self, reply: ReplyXattr, result: Result<ProductXattrReply, libc::c_int>) {
        match result {
            Ok(ProductXattrReply::Size(size)) => reply.size(size),
            Ok(ProductXattrReply::Data(data)) => reply.data(&data),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_lock(&self, reply: ReplyLock, result: Result<Option<pb::FileLock>, libc::c_int>) {
        match result {
            Ok(lock) => {
                let lock = self.lock_reply(lock);
                reply.locked(lock.start, lock.end, lock.typ, lock.pid);
            }
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_lseek(&self, reply: ReplyLseek, result: Result<i64, libc::c_int>) {
        match result {
            Ok(offset) => reply.offset(offset),
            Err(errno) => reply.error(errno),
        }
    }

    pub fn reply_directory(
        &self,
        mut reply: ReplyDirectory,
        inode: u64,
        parent_inode: u64,
        kernel_offset: u64,
        entries: Vec<pb::DirectoryEntry>,
    ) {
        for entry in self.directory_entries(inode, parent_inode, kernel_offset, entries) {
            if reply.add(
                entry.inode,
                entry.offset,
                entry.kind,
                OsStr::new(&entry.name),
            ) {
                break;
            }
        }
        reply.ok();
    }

    pub fn reply_directory_error(&self, reply: ReplyDirectory, errno: libc::c_int) {
        reply.error(errno);
    }
}

fn file_type(kind: i32) -> FileType {
    match pb::FileKind::try_from(kind) {
        Ok(pb::FileKind::Directory) => FileType::Directory,
        Ok(pb::FileKind::Symlink) => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

fn timestamp(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn encode_xattr_names(names: Vec<String>) -> Vec<u8> {
    if names.is_empty() {
        return Vec::new();
    }
    let mut value = names.join("\0").into_bytes();
    value.push(0);
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_protocol::file_attr as pb_file_attr;

    #[test]
    fn presenter_readdir_entries_include_dot_entries_with_stable_offsets() {
        let presenter = ProductFuseReplyPresenter::default();
        let entries = vec![
            pb::DirectoryEntry {
                inode: 10,
                name: "alpha".into(),
                kind: pb::FileKind::File as i32,
            },
            pb::DirectoryEntry {
                inode: 11,
                name: "beta".into(),
                kind: pb::FileKind::Directory as i32,
            },
        ];

        let reply_entries = presenter.directory_entries(2, 1, 0, entries.clone());
        assert_eq!(
            reply_entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.offset))
                .collect::<Vec<_>>(),
            vec![(".", 1), ("..", 2), ("alpha", 3), ("beta", 4)]
        );

        let reply_entries = presenter.directory_entries(2, 1, 1, entries.clone());
        assert_eq!(
            reply_entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.offset))
                .collect::<Vec<_>>(),
            vec![("..", 2), ("alpha", 3), ("beta", 4)]
        );

        let reply_entries = presenter.directory_entries(2, 1, 2, entries);
        assert_eq!(
            reply_entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.offset))
                .collect::<Vec<_>>(),
            vec![("alpha", 3), ("beta", 4)]
        );
    }

    #[test]
    fn presenter_readdir_request_skips_dot_entries_for_server_offsets() {
        let presenter = ProductFuseReplyPresenter::default();

        assert_eq!(presenter.readdir_request(0).unwrap().server_offset, 0);
        assert_eq!(presenter.readdir_request(1).unwrap().server_offset, 0);
        assert_eq!(presenter.readdir_request(2).unwrap().server_offset, 0);
        assert_eq!(presenter.readdir_request(3).unwrap().server_offset, 1);
        assert_eq!(presenter.readdir_request(4).unwrap().server_offset, 2);
        assert_eq!(presenter.readdir_request(-1), Err(libc::EINVAL));
    }

    #[test]
    fn presenter_encodes_xattr_names_as_nul_terminated_list() {
        let presenter = ProductFuseReplyPresenter::default();

        assert_eq!(
            presenter
                .listxattr_names(vec!["user.a".into(), "user.b".into()], 32)
                .unwrap(),
            ProductXattrReply::Data(b"user.a\0user.b\0".to_vec())
        );
        assert_eq!(
            presenter.listxattr_names(Vec::new(), 32).unwrap(),
            ProductXattrReply::Data(Vec::new())
        );
        assert_eq!(
            presenter
                .listxattr_names(vec!["user.a".into(), "user.b".into()], 0)
                .unwrap(),
            ProductXattrReply::Size(14)
        );
    }

    #[test]
    fn presenter_file_attr_preserves_distinct_common_timestamps() {
        let presenter = ProductFuseReplyPresenter::default();
        let attr = presenter.file_attr(pb::FileAttr {
            inode: 9,
            size: 10,
            kind: pb::FileKind::File as i32,
            perm: 0o644,
            mtime_unix_nanos: 22,
            uid: 1000,
            gid: 1001,
            nlink: 1,
            atime_unix_nanos: 11,
            ctime_unix_nanos: 33,
            crtime_unix_nanos: 44,
        });

        assert_eq!(attr.atime, UNIX_EPOCH + Duration::from_nanos(11));
        assert_eq!(attr.mtime, UNIX_EPOCH + Duration::from_nanos(22));
        assert_eq!(attr.ctime, UNIX_EPOCH + Duration::from_nanos(33));
        assert_eq!(attr.crtime, UNIX_EPOCH + Duration::from_nanos(44));
    }

    #[test]
    fn presenter_statfs_reply_preserves_available_blocks_and_fragment_size() {
        let presenter = ProductFuseReplyPresenter::default();
        let fields = presenter.statfs_fields(pb::StatFs {
            blocks: 100,
            blocks_free: 90,
            blocks_available: 80,
            files: 70,
            files_free: 60,
            block_size: 4096,
            name_max: 255,
            fragment_size: 1024,
        });

        assert_eq!(fields.blocks_free, 90);
        assert_eq!(fields.blocks_available, 80);
        assert_eq!(fields.block_size, 4096);
        assert_eq!(fields.fragment_size, 1024);
    }

    #[test]
    fn presenter_rejects_callback_only_arguments_before_rpc() {
        let presenter = ProductFuseReplyPresenter::default();

        assert_eq!(
            presenter.setattr_request(
                Some(0o600),
                None,
                None,
                None,
                Some(TimeOrNow::Now),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            Err(libc::ENOTSUP)
        );
        assert_eq!(
            presenter.setattr_request(
                None, None, None, None, None, None, None, None, None, None, None, None,
            ),
            Err(libc::EINVAL)
        );
        assert_eq!(
            presenter
                .setattr_request(
                    Some(libc::S_IFREG | 0o640),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(7),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap()
                .mode,
            Some(0o640)
        );
        assert_eq!(
            presenter.copy_file_range_request(-1, 0, 1),
            Err(libc::EINVAL)
        );
        assert_eq!(
            presenter
                .copy_file_range_request(0, 0, u64::MAX)
                .unwrap()
                .length,
            MAX_REPLY_WRITE_BYTES
        );
        assert_eq!(presenter.fallocate_request(0, 0, 0), Err(libc::EINVAL));
        assert_eq!(
            presenter.lseek_request(-1, libc::SEEK_SET),
            Err(libc::EINVAL)
        );
        assert_eq!(presenter.lseek_request(0, 999_999), Err(libc::EINVAL));
    }

    #[test]
    fn presenter_maps_lock_absence_to_unlocked_reply() {
        let presenter = ProductFuseReplyPresenter::default();

        assert_eq!(
            presenter.lock_reply(None),
            ProductLockReply {
                start: 0,
                end: 0,
                typ: libc::F_UNLCK,
                pid: 0,
            }
        );
        assert_eq!(
            presenter.lock_reply(Some(pb::FileLock {
                start: 3,
                end: 9,
                typ: libc::F_WRLCK,
                pid: 42,
            })),
            ProductLockReply {
                start: 3,
                end: 9,
                typ: libc::F_WRLCK,
                pid: 42,
            }
        );
    }

    #[test]
    fn presenter_maps_adapter_errors_to_wire_errno() {
        let presenter = ProductFuseReplyPresenter::default();

        assert_eq!(
            presenter.errno(FuseError::StaleCache),
            fs_protocol::Errno::Stale.wire_value()
        );
        assert_eq!(
            presenter
                .file_attr(pb_file_attr(1, pb::FileKind::Directory, 0))
                .kind,
            FileType::Directory
        );
    }
}
