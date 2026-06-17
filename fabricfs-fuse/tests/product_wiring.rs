use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use fabricfs_fuse::fs::FabricFsFuse;
use fabricfs_fuse::reply::{ProductFuseReplyPresenter, ProductLockReply, ProductXattrReply};
use fs_core::{RpcClient, RpcError};
use fs_fuse::FuseAdapter;
use fs_protocol::{pb, RequestEnvelope, ResponseEnvelope};

#[test]
fn product_filesystem_is_wired_around_reusable_fuse_adapter() {
    let client = NoopClient;
    let filesystem = FabricFsFuse::new(FuseAdapter::new(client, "fabricfs-test"), false);

    assert_eq!(filesystem.adapter().cached_path(1).as_deref(), Some("/"));
}

#[test]
fn product_reply_presenter_owns_callback_argument_rejection() {
    let presenter = ProductFuseReplyPresenter::default();

    assert_eq!(presenter.required_name(OsStr::new("")), Err(libc::EINVAL));
    assert_eq!(
        presenter.required_name(OsStr::from_bytes(b"name-\xff")),
        Err(libc::EINVAL)
    );
    assert_eq!(
        presenter.rename_names(OsStr::new("old"), OsStr::new("new"), 1),
        Err(libc::EINVAL)
    );
    assert_eq!(
        presenter.setxattr_name(OsStr::new("user.a"), 1),
        Err(libc::ENOTSUP)
    );
    assert_eq!(presenter.readdir_request(-1), Err(libc::EINVAL));
    assert_eq!(
        presenter.copy_file_range_request(-1, 0, 1),
        Err(libc::EINVAL)
    );
    assert_eq!(presenter.fallocate_request(0, 0, 0), Err(libc::EINVAL));
    assert_eq!(
        presenter.lseek_request(-1, libc::SEEK_SET),
        Err(libc::EINVAL)
    );
}

#[test]
fn product_reply_presenter_shapes_callback_replies() {
    let presenter = ProductFuseReplyPresenter::default();

    let attr = presenter.file_attr(pb::FileAttr {
        inode: 7,
        size: 4097,
        kind: pb::FileKind::Directory as i32,
        perm: 0o755,
        mtime_unix_nanos: 0,
        uid: 1000,
        gid: 1001,
        nlink: 2,
        atime_unix_nanos: 0,
        ctime_unix_nanos: 0,
        crtime_unix_nanos: 0,
    });
    assert_eq!(attr.ino, 7);
    assert_eq!(attr.blocks, 9);
    assert_eq!(attr.perm, 0o755);

    let statfs = presenter.statfs_fields(pb::StatFs {
        blocks: 10,
        blocks_free: 9,
        blocks_available: 8,
        files: 7,
        files_free: 6,
        block_size: 4096,
        name_max: 255,
        fragment_size: 1024,
    });
    assert_eq!(statfs.blocks_available, 8);
    assert_eq!(statfs.fragment_size, 1024);

    assert_eq!(
        presenter
            .listxattr_names(vec!["user.a".into(), "user.b".into()], 32)
            .unwrap(),
        ProductXattrReply::Data(b"user.a\0user.b\0".to_vec())
    );
    assert_eq!(
        presenter.listxattr_names(vec!["user.a".into()], 0).unwrap(),
        ProductXattrReply::Size(7)
    );

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
        presenter
            .copy_file_range_request(0, 0, u64::MAX)
            .unwrap()
            .length,
        u64::from(u32::MAX)
    );
}

struct NoopClient;

impl RpcClient for NoopClient {
    fn call(&self, _request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError> {
        Err(RpcError::ConnectionClosed)
    }

    fn drain_invalidations(&self, _namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
        Ok(Vec::new())
    }
}
