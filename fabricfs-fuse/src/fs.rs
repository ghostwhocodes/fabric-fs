use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::SystemTime;

use crate::reply::ProductFuseReplyPresenter;
use fs_core::RpcClient;
use fs_fuse::{CallerContextGuard, FuseAdapter, FuseEntry, FuseSetlk};
use fs_protocol::pb;
use fuser::{
    consts, fuse_forget_one, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs,
    ReplyWrite, ReplyXattr, Request, TimeOrNow,
};

pub struct FabricFsFuse<C> {
    adapter: FuseAdapter<C>,
    presenter: ProductFuseReplyPresenter,
    debug: bool,
}

impl<C> FabricFsFuse<C>
where
    C: RpcClient,
{
    pub fn new(adapter: FuseAdapter<C>, debug: bool) -> Self {
        Self {
            adapter,
            presenter: ProductFuseReplyPresenter::default(),
            debug,
        }
    }

    pub fn adapter(&self) -> &FuseAdapter<C> {
        &self.adapter
    }

    fn bind_caller(&self, request: &Request<'_>) -> CallerContextGuard {
        self.adapter.bind_caller_context(pb::CallerContext {
            uid: request.uid(),
            gid: request.gid(),
            pid: request.pid(),
        })
    }

    fn log(&self, message: impl AsRef<str>) {
        if self.debug {
            tracing::debug!(message = message.as_ref(), "fabricfs-fuse callback");
        }
    }

    fn readlink_impl(&self, ino: u64) -> Result<Vec<u8>, libc::c_int> {
        self.adapter
            .readlink(ino)
            .map_err(|error| self.presenter.errno(error))
    }

    fn symlink_impl(
        &self,
        parent: u64,
        name: &OsStr,
        target: &Path,
    ) -> Result<FuseEntry, libc::c_int> {
        let name = self.presenter.required_name(name)?;
        self.adapter
            .symlink(parent, name, target.as_os_str().as_bytes())
            .map_err(|error| self.presenter.errno(error))
    }

    fn hardlink_impl(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<FuseEntry, libc::c_int> {
        let new_name = self.presenter.required_name(new_name)?;
        self.adapter
            .hardlink(inode, new_parent, new_name)
            .map_err(|error| self.presenter.errno(error))
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr_impl(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<FuseEntry, libc::c_int> {
        let request = self.presenter.setattr_request(
            mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime, flags,
        )?;
        self.adapter
            .setattr(
                ino,
                request.handle,
                request.mode,
                request.uid,
                request.gid,
                request.size,
            )
            .map_err(|error| self.presenter.errno(error))
    }

    fn flush_impl(&self, ino: u64, fh: u64, lock_owner: u64) -> Result<(), libc::c_int> {
        self.adapter
            .flush(ino, fh, lock_owner)
            .map_err(|error| self.presenter.errno(error))
    }

    fn fsync_impl(&self, ino: u64, fh: u64, datasync: bool) -> Result<(), libc::c_int> {
        self.adapter
            .fsync(ino, fh, datasync)
            .map_err(|error| self.presenter.errno(error))
    }

    fn fsyncdir_impl(&self, ino: u64, fh: u64, datasync: bool) -> Result<(), libc::c_int> {
        self.adapter
            .fsyncdir(ino, fh, datasync)
            .map_err(|error| self.presenter.errno(error))
    }

    fn getlk_impl(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<Option<pb::FileLock>, libc::c_int> {
        self.adapter
            .getlk(ino, fh, lock_owner, start, end, typ, pid)
            .map_err(|error| self.presenter.errno(error))
    }

    #[allow(clippy::too_many_arguments)]
    fn setlk_impl(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
    ) -> Result<(), libc::c_int> {
        self.adapter
            .setlk(
                ino,
                FuseSetlk {
                    handle: fh,
                    owner: lock_owner,
                    start,
                    end,
                    typ,
                    pid,
                    wait: sleep,
                },
            )
            .map_err(|error| self.presenter.errno(error))
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_file_range_impl(
        &self,
        input_inode: u64,
        input_handle: u64,
        input_offset: i64,
        output_inode: u64,
        output_handle: u64,
        output_offset: i64,
        length: u64,
        flags: u32,
    ) -> Result<u32, libc::c_int> {
        let request =
            self.presenter
                .copy_file_range_request(input_offset, output_offset, length)?;
        self.adapter
            .copy_file_range(
                input_inode,
                input_handle,
                request.input_offset,
                output_inode,
                output_handle,
                request.output_offset,
                request.length,
                flags,
            )
            .map_err(|error| self.presenter.errno(error))
            .and_then(|bytes_copied| self.presenter.copy_file_range_reply_bytes(bytes_copied))
    }

    fn fallocate_impl(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<(), libc::c_int> {
        let request = self.presenter.fallocate_request(offset, length, mode)?;
        self.adapter
            .fallocate(ino, fh, request.offset, request.length, request.mode)
            .map_err(|error| self.presenter.errno(error))
    }

    fn lseek_impl(&self, ino: u64, fh: u64, offset: i64, whence: i32) -> Result<i64, libc::c_int> {
        let request = self.presenter.lseek_request(offset, whence)?;
        self.adapter
            .lseek(ino, fh, request.offset, request.whence)
            .map_err(|error| self.presenter.errno(error))
    }
}

impl<C> Filesystem for FabricFsFuse<C>
where
    C: RpcClient,
{
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        config
            .add_capabilities(lock_capabilities())
            .map_err(|_| libc::EINVAL)
    }

    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_entry_errno(reply, Err(errno)),
        };
        self.log(format!("lookup parent={parent} name={name}"));
        self.presenter
            .reply_entry(reply, self.adapter.lookup(parent, name));
    }

    fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
        self.adapter.forget(ino, nlookup);
    }

    fn batch_forget(&mut self, req: &Request<'_>, nodes: &[fuse_forget_one]) {
        for node in nodes {
            self.forget(req, node.nodeid, node.nlookup);
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let _caller = self.bind_caller(req);
        self.log(format!("getattr ino={ino}"));
        self.presenter.reply_attr(reply, self.adapter.getattr(ino));
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!(
            "setattr ino={ino} fh={fh:?} mode={mode:?} size={size:?}"
        ));
        self.presenter.reply_attr_errno(
            reply,
            self.setattr_impl(
                ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime,
                flags,
            ),
        );
    }

    fn readlink(&mut self, req: &Request<'_>, ino: u64, reply: ReplyData) {
        let _caller = self.bind_caller(req);
        self.log(format!("readlink ino={ino}"));
        self.presenter
            .reply_data_errno(reply, self.readlink_impl(ino));
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) {
        let _caller = self.bind_caller(req);
        let readdir = match self.presenter.readdir_request(offset) {
            Ok(readdir) => readdir,
            Err(errno) => {
                return self.presenter.reply_directory_error(reply, errno);
            }
        };
        self.log(format!("readdir ino={ino} offset={offset}"));
        match self
            .adapter
            .readdir_with_parent(ino, readdir.server_offset, readdir.max_entries)
        {
            Ok((parent_ino, entries)) => self.presenter.reply_directory(
                reply,
                ino,
                parent_ino,
                readdir.kernel_offset,
                entries,
            ),
            Err(error) => self
                .presenter
                .reply_directory_error(reply, self.presenter.errno(error)),
        }
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let _caller = self.bind_caller(req);
        self.log(format!("open ino={ino} flags={flags}"));
        self.presenter
            .reply_open(reply, self.adapter.open(ino, flags as u32));
    }

    fn opendir(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let _caller = self.bind_caller(req);
        self.log(format!("opendir ino={ino} flags={flags}"));
        self.presenter
            .reply_open(reply, self.adapter.opendir(ino, flags as u32));
    }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let _caller = self.bind_caller(req);
        let offset = match self.presenter.non_negative_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => return self.presenter.reply_data_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_data(reply, self.adapter.read(ino, fh, offset, size));
    }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let _caller = self.bind_caller(req);
        let offset = match self.presenter.non_negative_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => return self.presenter.reply_write_bytes(reply, Err(errno)),
        };
        self.presenter
            .reply_write(reply, self.adapter.write(ino, fh, offset, data.to_vec()));
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_created_errno(reply, Err(errno)),
        };
        self.log(format!(
            "create parent={parent} name={name} flags={flags} mode={mode:o}"
        ));
        let result = self.adapter.create(parent, name, flags as u32, mode);
        if let Ok((entry, handle)) = &result {
            self.log(format!(
                "create ok inode={} kind={} perm={:o} handle={} response_flags={}",
                entry.attr.inode, entry.attr.kind, entry.attr.perm, handle.handle, handle.flags
            ));
        }
        self.presenter.reply_created(reply, result);
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_entry_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_entry(reply, self.adapter.mkdir(parent, name, mode));
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!("symlink parent={parent} target={target:?}"));
        self.presenter
            .reply_entry_errno(reply, self.symlink_impl(parent, link_name, target));
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_empty_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_empty(reply, self.adapter.unlink(parent, name));
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_empty_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_empty(reply, self.adapter.rmdir(parent, name));
    }

    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        let (name, newname) = match self.presenter.rename_names(name, newname, flags) {
            Ok(names) => names,
            Err(errno) => return self.presenter.reply_empty_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_empty(reply, self.adapter.rename(parent, name, newparent, newname));
    }

    fn link(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!("link inode={ino} newparent={newparent}"));
        self.presenter
            .reply_entry_errno(reply, self.hardlink_impl(ino, newparent, newname));
    }

    fn flush(&mut self, req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        self.log(format!("flush ino={ino} fh={fh}"));
        self.presenter
            .reply_empty_errno(reply, self.flush_impl(ino, fh, lock_owner));
    }

    fn release(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        let _ = lock_owner;
        self.presenter
            .reply_empty(reply, self.adapter.release(ino, fh, flags as u32));
    }

    fn releasedir(&mut self, req: &Request<'_>, ino: u64, fh: u64, flags: i32, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        self.log(format!("releasedir ino={ino} fh={fh} flags={flags}"));
        self.presenter
            .reply_empty(reply, self.adapter.release(ino, fh, flags as u32));
    }

    fn fsync(&mut self, req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        self.log(format!("fsync ino={ino} fh={fh} datasync={datasync}"));
        self.presenter
            .reply_empty_errno(reply, self.fsync_impl(ino, fh, datasync));
    }

    fn fsyncdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!("fsyncdir ino={ino} fh={fh} datasync={datasync}"));
        self.presenter
            .reply_empty_errno(reply, self.fsyncdir_impl(ino, fh, datasync));
    }

    fn statfs(&mut self, req: &Request<'_>, ino: u64, reply: ReplyStatfs) {
        let _caller = self.bind_caller(req);
        self.presenter.reply_statfs(reply, self.adapter.statfs(ino));
    }

    fn getxattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_xattr(reply, Err(errno)),
        };
        let result = self
            .adapter
            .getxattr(ino, name, size)
            .map_err(|error| self.presenter.errno(error))
            .and_then(|value| self.presenter.xattr_value(value, size));
        self.presenter.reply_xattr(reply, result);
    }

    fn setxattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.setxattr_name(name, position) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_empty_errno(reply, Err(errno)),
        };
        self.presenter.reply_empty(
            reply,
            self.adapter
                .setxattr(ino, name, value.to_vec(), flags as u32),
        );
    }

    fn listxattr(&mut self, req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        let _caller = self.bind_caller(req);
        let result = self
            .adapter
            .listxattr(ino, size)
            .map_err(|error| self.presenter.errno(error))
            .and_then(|names| self.presenter.listxattr_names(names, size));
        self.presenter.reply_xattr(reply, result);
    }

    fn removexattr(&mut self, req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let _caller = self.bind_caller(req);
        let name = match self.presenter.required_name(name) {
            Ok(name) => name,
            Err(errno) => return self.presenter.reply_empty_errno(reply, Err(errno)),
        };
        self.presenter
            .reply_empty(reply, self.adapter.removexattr(ino, name));
    }

    fn getlk(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!("getlk ino={ino} fh={fh} start={start} end={end}"));
        self.presenter.reply_lock(
            reply,
            self.getlk_impl(ino, fh, lock_owner, start, end, typ, pid),
        );
    }

    fn setlk(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!("setlk ino={ino} fh={fh} start={start} end={end}"));
        self.presenter.reply_empty_errno(
            reply,
            self.setlk_impl(ino, fh, lock_owner, start, end, typ, pid, sleep),
        );
    }

    fn fallocate(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!(
            "fallocate ino={ino} fh={fh} offset={offset} length={length}"
        ));
        self.presenter
            .reply_empty_errno(reply, self.fallocate_impl(ino, fh, offset, length, mode));
    }

    fn lseek(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!(
            "lseek ino={ino} fh={fh} offset={offset} whence={whence}"
        ));
        self.presenter
            .reply_lseek(reply, self.lseek_impl(ino, fh, offset, whence));
    }

    fn copy_file_range(
        &mut self,
        req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: ReplyWrite,
    ) {
        let _caller = self.bind_caller(req);
        self.log(format!(
            "copy_file_range ino_in={ino_in} fh_in={fh_in} ino_out={ino_out} fh_out={fh_out} len={len}"
        ));
        self.presenter.reply_write_bytes(
            reply,
            self.copy_file_range_impl(
                ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags,
            ),
        );
    }
}

fn lock_capabilities() -> u32 {
    consts::FUSE_POSIX_LOCKS | consts::FUSE_FLOCK_LOCKS
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_core::{RpcClient, RpcError};
    use fs_protocol::{
        file_attr as pb_file_attr, InvalidationKind, Operation, RequestEnvelope, RequestPayload,
        ResponseEnvelope, ResponsePayload,
    };
    use std::os::unix::ffi::OsStrExt;
    use std::sync::{Arc, Mutex};

    #[test]
    fn fuse_init_requests_remote_posix_and_flock_locks() {
        let capabilities = lock_capabilities();

        assert_eq!(
            capabilities & consts::FUSE_POSIX_LOCKS,
            consts::FUSE_POSIX_LOCKS
        );
        assert_eq!(
            capabilities & consts::FUSE_FLOCK_LOCKS,
            consts::FUSE_FLOCK_LOCKS
        );
    }

    #[test]
    fn mounted_posix_extension_helpers_route_through_reusable_adapter() {
        let client = FakeClient::default();
        let filesystem =
            FabricFsFuse::new(FuseAdapter::new(client.clone(), "fabricfs-test"), false);
        let caller = pb::CallerContext {
            uid: 501,
            gid: 502,
            pid: 503,
        };

        filesystem
            .adapter()
            .lookup(1, "file.txt")
            .expect("cache input file path");
        let file_handle = filesystem
            .adapter()
            .open(2, 0)
            .expect("open cached input file")
            .handle;
        let dir_handle = filesystem
            .adapter()
            .opendir(1, 0)
            .expect("open root directory")
            .handle;
        let copy_handle = filesystem
            .adapter()
            .create(1, "copy.txt", 0, 0o644)
            .expect("create copy target")
            .1
            .handle;
        client.take_requests();

        filesystem
            .adapter()
            .with_caller_context(caller.clone(), || {
                assert_eq!(
                    filesystem
                        .symlink_impl(
                            1,
                            OsStr::new("link.txt"),
                            Path::new(OsStr::from_bytes(b"target-\xff")),
                        )
                        .expect("symlink through mounted helper")
                        .attr
                        .inode,
                    5
                );
                assert_eq!(
                    filesystem
                        .readlink_impl(5)
                        .expect("readlink through mounted helper"),
                    b"target-\xff"
                );
                assert_eq!(
                    filesystem
                        .hardlink_impl(2, 1, OsStr::new("hard.txt"))
                        .expect("hardlink through mounted helper")
                        .attr
                        .inode,
                    2
                );
                assert_eq!(
                    filesystem
                        .setattr_impl(
                            2,
                            Some(0o600),
                            None,
                            None,
                            Some(16),
                            None,
                            None,
                            None,
                            Some(file_handle),
                            None,
                            None,
                            None,
                            None,
                        )
                        .expect("setattr through mounted helper")
                        .attr
                        .size,
                    16
                );
                filesystem
                    .flush_impl(2, file_handle, 99)
                    .expect("flush through mounted helper");
                filesystem
                    .fsync_impl(2, file_handle, true)
                    .expect("fsync through mounted helper");
                filesystem
                    .fsyncdir_impl(1, dir_handle, false)
                    .expect("fsyncdir through mounted helper");
                assert_eq!(
                    filesystem
                        .getlk_impl(2, file_handle, 123, 0, u64::MAX, libc::F_WRLCK, 42)
                        .expect("getlk through mounted helper"),
                    None
                );
                filesystem
                    .setlk_impl(2, file_handle, 123, 0, u64::MAX, libc::F_WRLCK, 42, true)
                    .expect("setlk through mounted helper");
                assert_eq!(
                    filesystem
                        .copy_file_range_impl(2, file_handle, 0, 3, copy_handle, 0, 5, 0)
                        .expect("copy_file_range through mounted helper"),
                    5
                );
                filesystem
                    .fallocate_impl(2, file_handle, 0, 8 * 1024 * 1024 * 1024, 0)
                    .expect("large fallocate through mounted helper");
                assert_eq!(
                    filesystem
                        .lseek_impl(2, file_handle, 0, libc::SEEK_SET)
                        .expect("lseek through mounted helper"),
                    5
                );
            });

        let requests = client.take_requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.operation)
                .collect::<Vec<_>>(),
            vec![
                Operation::Symlink,
                Operation::Readlink,
                Operation::Hardlink,
                Operation::Setattr,
                Operation::Flush,
                Operation::Fsync,
                Operation::Fsyncdir,
                Operation::Getlk,
                Operation::Setlk,
                Operation::CopyFileRange,
                Operation::Fallocate,
                Operation::Lseek,
            ]
        );
        assert!(
            requests
                .iter()
                .all(|request| request.caller.as_ref() == Some(&caller)),
            "mounted helpers must preserve the bound caller context on every common request"
        );

        let RequestPayload::Symlink(symlink) = &requests[0].payload else {
            panic!("expected symlink request");
        };
        assert_eq!(symlink.target, b"target-\xff");

        let RequestPayload::Setattr(setattr) = &requests[3].payload else {
            panic!("expected setattr request");
        };
        assert_eq!(setattr.handle, Some(file_handle));
        assert_eq!(setattr.size, Some(16));

        let RequestPayload::Setlk(setlk) = &requests[8].payload else {
            panic!("expected setlk request");
        };
        assert!(setlk.wait);

        let RequestPayload::CopyFileRange(copy) = &requests[9].payload else {
            panic!("expected copy_file_range request");
        };
        assert_eq!(copy.length, 5);
        assert_eq!(copy.output_handle, copy_handle);
    }

    #[test]
    fn mounted_copy_file_range_helper_bounds_reply_write_requests_before_rpc() {
        let client = FakeClient::default();
        let filesystem =
            FabricFsFuse::new(FuseAdapter::new(client.clone(), "fabricfs-test"), false);

        filesystem
            .adapter()
            .lookup(1, "file.txt")
            .expect("cache input file path");
        let file_handle = filesystem
            .adapter()
            .open(2, 0)
            .expect("open cached input file")
            .handle;
        let copy_handle = filesystem
            .adapter()
            .create(1, "copy.txt", 0, 0o644)
            .expect("create copy target")
            .1
            .handle;
        client.take_requests();

        assert_eq!(
            filesystem
                .adapter()
                .with_caller_context(
                    pb::CallerContext {
                        uid: 1000,
                        gid: 1001,
                        pid: 1002,
                    },
                    || {
                        filesystem.copy_file_range_impl(
                            2,
                            file_handle,
                            0,
                            3,
                            copy_handle,
                            0,
                            u64::MAX,
                            0,
                        )
                    },
                )
                .expect("mounted copy_file_range caps oversized reply-write requests"),
            5
        );

        let requests = client.take_requests();
        assert_eq!(requests.len(), 1);
        let RequestPayload::CopyFileRange(copy) = &requests[0].payload else {
            panic!("expected copy_file_range request");
        };
        assert_eq!(copy.length, u64::from(u32::MAX));
    }

    #[test]
    fn mounted_setattr_helper_rejects_unsupported_kernel_only_fields() {
        let client = FakeClient::default();
        let filesystem =
            FabricFsFuse::new(FuseAdapter::new(client.clone(), "fabricfs-test"), false);

        filesystem
            .adapter()
            .lookup(1, "file.txt")
            .expect("cache file path");
        client.take_requests();

        let errno = filesystem.adapter().with_caller_context(
            pb::CallerContext {
                uid: 1000,
                gid: 1000,
                pid: 1000,
            },
            || {
                filesystem
                    .setattr_impl(
                        2,
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
                    )
                    .expect_err("timestamp-only setattr fields are not part of the common contract")
            },
        );

        assert_eq!(errno, libc::ENOTSUP);
        assert!(
            client.take_requests().is_empty(),
            "unsupported kernel-only setattr fields must fail before any RPC is emitted"
        );
    }

    #[test]
    fn mounted_setattr_helper_masks_file_type_bits_before_rpc() {
        let client = FakeClient::default();
        let filesystem =
            FabricFsFuse::new(FuseAdapter::new(client.clone(), "fabricfs-test"), false);

        filesystem
            .adapter()
            .lookup(1, "file.txt")
            .expect("cache file path");
        client.take_requests();

        filesystem.adapter().with_caller_context(
            pb::CallerContext {
                uid: 1000,
                gid: 1000,
                pid: 1000,
            },
            || {
                filesystem
                    .setattr_impl(
                        2,
                        Some(libc::S_IFREG | 0o640),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .expect("chmod-style setattr should mask the file type bits");
            },
        );

        let requests = client.take_requests();
        let RequestPayload::Setattr(setattr) = &requests
            .last()
            .expect("one setattr request should be emitted")
            .payload
        else {
            panic!("expected setattr request");
        };
        assert_eq!(setattr.mode, Some(0o640));
    }

    #[derive(Clone, Default)]
    struct FakeClient {
        inner: Arc<Mutex<FakeState>>,
    }

    #[derive(Default)]
    struct FakeState {
        requests: Vec<RequestEnvelope>,
        next_sequence: u64,
    }

    impl FakeClient {
        fn take_requests(&self) -> Vec<RequestEnvelope> {
            std::mem::take(&mut self.inner.lock().expect("fake state lock").requests)
        }
    }

    impl RpcClient for FakeClient {
        fn call(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError> {
            let mut state = self.inner.lock().expect("fake state lock");
            state.requests.push(request.clone());
            let payload = response_for_request(&request);
            let invalidations =
                default_invalidations_for_success(&request, &payload, &mut state.next_sequence);
            ResponseEnvelope::success_for(&request, payload, invalidations)
                .map_err(|error| RpcError::Malformed(error.to_string()))
        }

        fn drain_invalidations(&self, _namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
            Ok(Vec::new())
        }
    }

    fn response_for_request(request: &RequestEnvelope) -> ResponsePayload {
        match &request.payload {
            RequestPayload::Lookup(value) => {
                let attr = match value.path.as_ref().map(|path| path.path.as_str()) {
                    Some("/file.txt") => pb_file_attr(2, pb::FileKind::File, 128),
                    Some("/copy.txt") => pb_file_attr(3, pb::FileKind::File, 0),
                    _ => pb_file_attr(1, pb::FileKind::Directory, 0),
                };
                ResponsePayload::Lookup(pb::LookupResponse { attr: Some(attr) })
            }
            RequestPayload::Open(value) => {
                let handle = match value.path.as_ref().map(|path| path.path.as_str()) {
                    Some("/") => 9,
                    _ => 7,
                };
                ResponsePayload::Open(pb::OpenResponse { handle, flags: 0 })
            }
            RequestPayload::Create(_) => ResponsePayload::Create(pb::CreateResponse {
                attr: Some(pb_file_attr(3, pb::FileKind::File, 0)),
                handle: 8,
            }),
            RequestPayload::Readlink(_) => ResponsePayload::Readlink(pb::ReadlinkResponse {
                target: b"target-\xff".to_vec(),
            }),
            RequestPayload::Symlink(_) => ResponsePayload::Symlink(pb::SymlinkResponse {
                attr: Some(pb_file_attr(5, pb::FileKind::Symlink, 10)),
            }),
            RequestPayload::Hardlink(_) => ResponsePayload::Hardlink(pb::HardlinkResponse {
                attr: Some(pb_file_attr(2, pb::FileKind::File, 128)),
            }),
            RequestPayload::Setattr(_) => ResponsePayload::Setattr(pb::SetattrResponse {
                attr: Some(pb_file_attr(2, pb::FileKind::File, 16)),
            }),
            RequestPayload::Flush(_) => ResponsePayload::Flush(pb::EmptyResponse {}),
            RequestPayload::Fsync(_) => ResponsePayload::Fsync(pb::EmptyResponse {}),
            RequestPayload::Fsyncdir(_) => ResponsePayload::Fsyncdir(pb::EmptyResponse {}),
            RequestPayload::Getlk(_) => ResponsePayload::Getlk(pb::GetlkResponse { lock: None }),
            RequestPayload::Setlk(_) => ResponsePayload::Setlk(pb::EmptyResponse {}),
            RequestPayload::CopyFileRange(_) => {
                ResponsePayload::CopyFileRange(pb::CopyFileRangeResponse { bytes_copied: 5 })
            }
            RequestPayload::Fallocate(_) => ResponsePayload::Fallocate(pb::EmptyResponse {}),
            RequestPayload::Lseek(_) => ResponsePayload::Lseek(pb::LseekResponse { offset: 5 }),
            other => panic!("unexpected request in fabricfs-fuse test fixture: {other:?}"),
        }
    }

    fn default_invalidations_for_success(
        request: &RequestEnvelope,
        payload: &ResponsePayload,
        next_sequence: &mut u64,
    ) -> Vec<pb::Invalidation> {
        let Some(kind) = invalidation_kind_for_request(&request.payload) else {
            return Vec::new();
        };
        *next_sequence += 1;
        vec![pb::Invalidation {
            namespace: request.namespace.clone(),
            sequence: *next_sequence,
            kind: kind.wire_value(),
            path: request
                .payload
                .primary_path()
                .unwrap_or_default()
                .to_owned(),
            old_path: String::new(),
            new_path: String::new(),
            inode: payload.created_inode().unwrap_or(0),
            handle: 0,
            request_id: request.request_id.clone(),
        }]
    }

    fn invalidation_kind_for_request(payload: &RequestPayload) -> Option<InvalidationKind> {
        match payload.operation().spec().effect {
            fs_protocol::OperationEffect::ContentMutation => Some(InvalidationKind::Modify),
            fs_protocol::OperationEffect::CreateNode => Some(InvalidationKind::Create),
            fs_protocol::OperationEffect::RenameNode => Some(InvalidationKind::Rename),
            fs_protocol::OperationEffect::DeleteNode => Some(InvalidationKind::Delete),
            fs_protocol::OperationEffect::MetadataMutation => Some(InvalidationKind::Metadata),
            fs_protocol::OperationEffect::XattrMutation => Some(InvalidationKind::Xattr),
            _ => None,
        }
    }
}
