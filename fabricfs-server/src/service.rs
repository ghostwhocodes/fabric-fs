mod inode_map;
mod translation;

use fs_core::{FileSystemService, FsError, FsResult, RpcMetadata};
use fs_protocol::{pb, Errno};
use std::sync::Mutex;

use self::inode_map::{FileIdentity, InodeMap};
use self::translation::{
    caller, child_path, encoded_xattr_name_size, file_kind, file_lock_to_proto, fs_errno,
    request_path, service_error, stat_to_attr, statfs_to_proto,
};
use crate::server::{
    ensure_parent_search_allowed, ensure_stat_access_bits, normalize_path, CopyFileRangeHandles,
    DynStorage, HandleKind, ServerStorage, Stat,
};

pub struct FabricFsFileSystemService {
    storage: DynStorage,
    inodes: Mutex<InodeMap>,
}

impl FabricFsFileSystemService {
    pub fn new(storage: DynStorage) -> Self {
        Self {
            storage,
            inodes: Mutex::new(InodeMap::new()),
        }
    }

    fn storage(&self) -> &dyn ServerStorage {
        self.storage.as_ref()
    }

    fn attr_for_stat(&self, path: &str, stat: Stat) -> FsResult<pb::FileAttr> {
        let inode = self.inode_for_stat(path, &stat)?;
        Ok(stat_to_attr(inode, stat))
    }

    fn inode_for_stat(&self, path: &str, stat: &Stat) -> FsResult<u64> {
        let mut inodes = self
            .inodes
            .lock()
            .map_err(|_| service_error(libc::EIO, "inode map lock poisoned"))?;
        Ok(inodes.inode_for(path, Some(FileIdentity::from_stat(stat))))
    }

    fn rename_inode_path(&self, old_path: &str, new_path: &str) -> FsResult<()> {
        self.inodes
            .lock()
            .map_err(|_| service_error(libc::EIO, "inode map lock poisoned"))?
            .rename_path(old_path, new_path);
        Ok(())
    }

    fn remove_inode_path(&self, path: &str) -> FsResult<()> {
        self.inodes
            .lock()
            .map_err(|_| service_error(libc::EIO, "inode map lock poisoned"))?
            .remove_path(path);
        Ok(())
    }

    fn ensure_parent_search_allowed(&self, path: &str, metadata: &RpcMetadata) -> FsResult<()> {
        let ctx = caller(metadata)
            .ok_or_else(|| service_error(libc::EACCES, "missing caller context"))?;
        ensure_parent_search_allowed(path, |parent| {
            let stat = self.storage().metadata().stat(parent)?;
            ensure_stat_access_bits(&stat, ctx.uid, ctx.gid, 0o1)
        })
        .map_err(fs_errno)
    }
}

fn open_handle_kind(kind: i32) -> FsResult<HandleKind> {
    match pb::OpenKind::try_from(kind) {
        Ok(pb::OpenKind::File) => Ok(HandleKind::File),
        Ok(pb::OpenKind::Directory) => Ok(HandleKind::Dir),
        Ok(pb::OpenKind::Unspecified) | Err(_) => {
            Err(service_error(libc::EINVAL, "invalid open kind"))
        }
    }
}

impl FileSystemService for FabricFsFileSystemService {
    fn lookup(
        &self,
        request: &pb::LookupRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        let path = request_path(request.path.as_ref())?;
        let stat = self.storage().metadata().stat(path).map_err(fs_errno)?;
        Ok(pb::LookupResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
        })
    }

    fn getattr(
        &self,
        request: &pb::GetattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetattrResponse> {
        let path = request_path(request.path.as_ref())?;
        let stat = self.storage().metadata().stat(path).map_err(fs_errno)?;
        Ok(pb::GetattrResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
        })
    }

    fn readdir(
        &self,
        request: &pb::ReaddirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ReaddirResponse> {
        let path = request_path(request.path.as_ref())?;
        let entries = self
            .storage()
            .directories()
            .list_dir(path)
            .map_err(fs_errno)?;
        let skip = request.offset as usize;
        let take = request.max_entries as usize;
        let total_entries = entries.len();
        let entries = entries
            .into_iter()
            .skip(skip)
            .take(take)
            .map(|(name, stat)| {
                let child = child_path(path, &name);
                Ok(pb::DirectoryEntry {
                    inode: self.inode_for_stat(&child, &stat)?,
                    name,
                    kind: file_kind(stat.mode) as i32,
                })
            })
            .collect::<FsResult<Vec<_>>>()?;
        let end = skip.saturating_add(entries.len()) >= total_entries;
        Ok(pb::ReaddirResponse { entries, end })
    }

    fn open(
        &self,
        request: &pb::OpenRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::OpenResponse> {
        let path = request_path(request.path.as_ref())?;
        let kind = open_handle_kind(request.kind)?;
        let (handle, _) = self
            .storage()
            .runtime()
            .open(path, kind, request.flags as i32, caller(metadata))
            .map_err(fs_errno)?;
        Ok(pb::OpenResponse { handle, flags: 0 })
    }

    fn read(
        &self,
        request: &pb::ReadRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadResponse> {
        let path = request_path(request.path.as_ref())?;
        let data = self
            .storage()
            .runtime()
            .read_fh(
                path,
                request.handle,
                request.offset as i64,
                request.size as i64,
                caller(metadata),
            )
            .map_err(fs_errno)?;
        Ok(pb::ReadResponse { data })
    }

    fn write(
        &self,
        request: &pb::WriteRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::WriteResponse> {
        let path = request_path(request.path.as_ref())?;
        let bytes_written = self
            .storage()
            .runtime()
            .write_fh(
                path,
                request.handle,
                request.offset as i64,
                &request.data,
                caller(metadata),
            )
            .map_err(fs_errno)?;
        Ok(pb::WriteResponse {
            bytes_written: bytes_written.min(u32::MAX as usize) as u32,
        })
    }

    fn create(
        &self,
        request: &pb::CreateRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::CreateResponse> {
        let path = request_path(request.path.as_ref())?;
        let (handle, stat) = self
            .storage()
            .namespace()
            .create_file(path, request.mode, request.flags as i32, caller(metadata))
            .map_err(fs_errno)?;
        Ok(pb::CreateResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
            handle,
        })
    }

    fn rename(
        &self,
        request: &pb::RenameRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let old_path = request_path(request.old_path.as_ref())?;
        let new_path = request_path(request.new_path.as_ref())?;
        self.storage()
            .namespace()
            .rename(old_path, new_path, caller(metadata))
            .map_err(fs_errno)?;
        self.rename_inode_path(old_path, new_path)?;
        Ok(pb::EmptyResponse {})
    }

    fn unlink(
        &self,
        request: &pb::UnlinkRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .namespace()
            .unlink(path, caller(metadata))
            .map_err(fs_errno)?;
        self.remove_inode_path(path)?;
        Ok(pb::EmptyResponse {})
    }

    fn mkdir(
        &self,
        request: &pb::MkdirRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::LookupResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .namespace()
            .mkdir(path, request.mode, caller(metadata))
            .map_err(fs_errno)?;
        let stat = self.storage().metadata().stat(path).map_err(fs_errno)?;
        Ok(pb::LookupResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
        })
    }

    fn rmdir(
        &self,
        request: &pb::RmdirRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .namespace()
            .rmdir(path, caller(metadata))
            .map_err(fs_errno)?;
        self.remove_inode_path(path)?;
        Ok(pb::EmptyResponse {})
    }

    fn statfs(
        &self,
        _request: &pb::StatfsRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::StatfsResponse> {
        let stat = self.storage().metadata().statfs().map_err(fs_errno)?;
        Ok(pb::StatfsResponse {
            stat: Some(statfs_to_proto(stat)),
        })
    }

    fn getxattr(
        &self,
        request: &pb::GetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetxattrResponse> {
        let path = request_path(request.path.as_ref())?;
        let value = self
            .storage()
            .metadata()
            .getxattr(path, &request.name, request.size)
            .map_err(fs_errno)?;
        Ok(pb::GetxattrResponse { value })
    }

    fn setxattr(
        &self,
        request: &pb::SetxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .metadata()
            .setxattr(
                path,
                &request.name,
                request.value.clone(),
                request.flags as i32,
            )
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn listxattr(
        &self,
        request: &pb::ListxattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::ListxattrResponse> {
        let path = request_path(request.path.as_ref())?;
        let names = self
            .storage()
            .metadata()
            .listxattr(path)
            .map_err(fs_errno)?;
        if request.size != 0 && encoded_xattr_name_size(&names) > request.size as usize {
            return Err(FsError::new(
                Errno::Range,
                "listxattr response exceeds requested buffer size",
            ));
        }
        Ok(pb::ListxattrResponse { names })
    }

    fn removexattr(
        &self,
        request: &pb::RemovexattrRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .metadata()
            .removexattr(path, &request.name)
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn release(
        &self,
        request: &pb::ReleaseRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        self.storage().runtime().release_fh(request.handle);
        Ok(pb::EmptyResponse {})
    }

    fn readlink(
        &self,
        request: &pb::ReadlinkRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::ReadlinkResponse> {
        let path = request_path(request.path.as_ref())?;
        let path = normalize_path(path).map_err(fs_errno)?;
        self.ensure_parent_search_allowed(&path, metadata)?;
        let target = self
            .storage()
            .metadata()
            .readlink(&path)
            .map_err(fs_errno)?;
        Ok(pb::ReadlinkResponse { target })
    }

    fn symlink(
        &self,
        request: &pb::SymlinkRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::SymlinkResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .namespace()
            .symlink(path, request.target.clone(), caller(metadata))
            .map_err(fs_errno)?;
        let stat = self.storage().metadata().stat(path).map_err(fs_errno)?;
        Ok(pb::SymlinkResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
        })
    }

    fn hardlink(
        &self,
        request: &pb::HardlinkRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::HardlinkResponse> {
        let existing_path = request_path(request.existing_path.as_ref())?;
        let new_path = request_path(request.new_path.as_ref())?;
        self.storage()
            .namespace()
            .link(existing_path, new_path, caller(metadata))
            .map_err(fs_errno)?;
        let existing_stat = self
            .storage()
            .metadata()
            .stat(existing_path)
            .map_err(fs_errno)?;
        self.inode_for_stat(existing_path, &existing_stat)?;
        let stat = self.storage().metadata().stat(new_path).map_err(fs_errno)?;
        Ok(pb::HardlinkResponse {
            attr: Some(self.attr_for_stat(new_path, stat)?),
        })
    }

    fn setattr(
        &self,
        request: &pb::SetattrRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::SetattrResponse> {
        let path = request_path(request.path.as_ref())?;
        let stat = match request.handle {
            Some(handle) => self
                .storage()
                .runtime()
                .setattr_fh(
                    path,
                    handle,
                    request.mode,
                    request.uid,
                    request.gid,
                    request.size,
                    caller(metadata),
                )
                .map_err(fs_errno)?,
            None => self
                .storage()
                .metadata()
                .setattr(
                    path,
                    request.mode,
                    request.uid,
                    request.gid,
                    request.size,
                    caller(metadata),
                )
                .map_err(fs_errno)?,
        };
        Ok(pb::SetattrResponse {
            attr: Some(self.attr_for_stat(path, stat)?),
        })
    }

    fn flush(
        &self,
        request: &pb::FlushRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        let runtime = self.storage().runtime();
        let sync_result = runtime.sync_file_fh(path, request.handle, true);
        let release_result = runtime.release_posix_locks(path, request.handle, request.lock_owner);
        sync_result.map_err(fs_errno)?;
        release_result.map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn fsync(
        &self,
        request: &pb::FsyncRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .runtime()
            .sync_file_fh(path, request.handle, request.datasync)
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn fsyncdir(
        &self,
        request: &pb::FsyncdirRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .runtime()
            .sync_dir_fh(path, request.handle, request.datasync)
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn getlk(
        &self,
        request: &pb::GetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::GetlkResponse> {
        let path = request_path(request.path.as_ref())?;
        let lock = self
            .storage()
            .runtime()
            .getlk(
                path,
                request.handle,
                request.owner,
                request.start,
                request.end,
                request.typ,
            )
            .map_err(fs_errno)?
            .map(file_lock_to_proto);
        Ok(pb::GetlkResponse { lock })
    }

    fn setlk(
        &self,
        request: &pb::SetlkRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        if request.wait && request.typ != libc::F_UNLCK {
            return Err(FsError::new(
                Errno::NotSupported,
                "blocking POSIX locks are not supported",
            ));
        }
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .runtime()
            .setlk(
                path,
                request.handle,
                request.owner,
                request.start,
                request.end,
                request.typ,
                request.pid,
            )
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn flock(
        &self,
        request: &pb::FlockRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .runtime()
            .flock(
                path,
                request.handle,
                request.owner,
                request.operation,
                metadata.caller.as_ref().map_or(0, |caller| caller.pid),
            )
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn copy_file_range(
        &self,
        request: &pb::CopyFileRangeRequest,
        metadata: &RpcMetadata,
    ) -> FsResult<pb::CopyFileRangeResponse> {
        if request.flags != 0 {
            return Err(FsError::new(
                Errno::NotSupported,
                "copy_file_range flags are not supported",
            ));
        }
        let input_path = request_path(request.input_path.as_ref())?;
        let output_path = request_path(request.output_path.as_ref())?;
        let bytes_copied = self
            .storage()
            .runtime()
            .copy_file_range(
                input_path,
                output_path,
                CopyFileRangeHandles {
                    fh_in: request.input_handle,
                    fh_out: request.output_handle,
                },
                request.input_offset,
                request.output_offset,
                request.length,
                caller(metadata),
            )
            .map_err(fs_errno)?;
        Ok(pb::CopyFileRangeResponse { bytes_copied })
    }

    fn fallocate(
        &self,
        request: &pb::FallocateRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::EmptyResponse> {
        if request.mode != 0 {
            return Err(FsError::new(
                Errno::NotSupported,
                "fallocate modes other than mode 0 are not supported",
            ));
        }
        let path = request_path(request.path.as_ref())?;
        self.storage()
            .runtime()
            .fallocate(
                path,
                request.handle,
                request.offset,
                request.length,
                request.mode,
            )
            .map_err(fs_errno)?;
        Ok(pb::EmptyResponse {})
    }

    fn lseek(
        &self,
        request: &pb::LseekRequest,
        _metadata: &RpcMetadata,
    ) -> FsResult<pb::LseekResponse> {
        if request.whence == fs_protocol::SEEK_DATA || request.whence == fs_protocol::SEEK_HOLE {
            return Err(FsError::new(
                Errno::NotSupported,
                "SEEK_DATA and SEEK_HOLE are not supported by the mounted backend",
            ));
        }
        let path = request_path(request.path.as_ref())?;
        let offset = self
            .storage()
            .runtime()
            .lseek(path, request.handle, request.offset, request.whence)
            .map_err(fs_errno)?;
        Ok(pb::LseekResponse { offset })
    }
}
