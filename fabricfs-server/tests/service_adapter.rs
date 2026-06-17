use fabricfs_server::overlay::OverlayFs;
use fabricfs_server::passthrough::PassthroughFs;
use fabricfs_server::server::{FsLimits, FsOptions};
use fabricfs_server::service::FabricFsFileSystemService;
use fs_core::{Dispatcher, RpcMetadata};
use fs_protocol::{path, pb, Errno, RequestEnvelope, RequestPayload, ResponsePayload};
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
fn overlay_fs(
    backing: Option<std::path::PathBuf>,
    alias: Option<std::path::PathBuf>,
    cow: Option<std::path::PathBuf>,
    limits: FsLimits,
    umask: u32,
    propagate_acls: bool,
    allow_backing_permission_updates: bool,
    allow_xattr_updates: bool,
    allow_direct_backing_updates: bool,
    enable_reflinks: bool,
    preserve_sparse_files: bool,
) -> OverlayFs {
    OverlayFs::new(
        backing,
        alias,
        cow,
        limits,
        umask,
        propagate_acls,
        allow_backing_permission_updates,
        allow_xattr_updates,
        allow_direct_backing_updates,
        enable_reflinks,
        preserve_sparse_files,
    )
    .expect("overlay fs init")
}

fn passthrough_fs(root: std::path::PathBuf, options: FsOptions) -> PassthroughFs {
    PassthroughFs::new(root, options).expect("passthrough fs init")
}

#[test]
fn service_adapter_routes_common_read_write_through_passthrough_backend() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"hello").expect("fixture file");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let open = dispatch(
        &dispatcher,
        request(
            "open-file",
            RequestPayload::Open(pb::OpenRequest {
                path: Some(path("/file.txt").expect("valid path")),
                flags: libc::O_RDWR as u32,
                kind: pb::OpenKind::File as i32,
            }),
        ),
    );
    let handle = match open.payload.expect("open payload") {
        ResponsePayload::Open(value) => value.handle,
        other => panic!("unexpected open payload: {other:?}"),
    };

    let write = dispatch(
        &dispatcher,
        request(
            "write-file",
            RequestPayload::Write(pb::WriteRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle,
                offset: 5,
                data: b"!".to_vec(),
            }),
        ),
    );
    assert!(write.ok, "write failed: {write:?}");
    assert_eq!(write.invalidations.len(), 1);
    assert_eq!(
        write.invalidations[0].kind,
        fs_protocol::InvalidationKind::Modify as i32
    );

    let read = dispatch(
        &dispatcher,
        request(
            "read-file",
            RequestPayload::Read(pb::ReadRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle,
                offset: 0,
                size: 6,
            }),
        ),
    );
    match read.payload.expect("read payload") {
        ResponsePayload::Read(value) => assert_eq!(value.data, b"hello!"),
        other => panic!("unexpected read payload: {other:?}"),
    }
}

#[test]
fn service_adapter_open_response_does_not_echo_posix_request_flags() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"hello").expect("fixture file");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let open = dispatch(
        &dispatcher,
        request(
            "open-file",
            RequestPayload::Open(pb::OpenRequest {
                path: Some(path("/file.txt").expect("valid path")),
                flags: (libc::O_RDONLY | libc::O_CLOEXEC) as u32,
                kind: pb::OpenKind::File as i32,
            }),
        ),
    );

    match open.payload.expect("open payload") {
        ResponsePayload::Open(value) => {
            assert_ne!(value.flags, (libc::O_RDONLY | libc::O_CLOEXEC) as u32);
            assert_eq!(value.flags, 0);
        }
        other => panic!("unexpected open payload: {other:?}"),
    }
}

#[test]
fn service_adapter_open_uses_explicit_directory_kind_for_directory_symlinks() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(root.path().join("real-dir")).expect("fixture directory");
    std::os::unix::fs::symlink("real-dir", root.path().join("link-dir"))
        .expect("fixture directory symlink");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let open_dir = dispatch(
        &dispatcher,
        request(
            "open-directory-symlink",
            RequestPayload::Open(pb::OpenRequest {
                path: Some(path("/link-dir").expect("valid path")),
                flags: libc::O_RDONLY as u32,
                kind: pb::OpenKind::Directory as i32,
            }),
        ),
    );
    assert!(open_dir.ok, "directory symlink open failed: {open_dir:?}");
    assert!(matches!(
        open_dir.payload.expect("open payload"),
        ResponsePayload::Open(_)
    ));

    let open_file = dispatch(
        &dispatcher,
        request(
            "open-directory-symlink-as-file",
            RequestPayload::Open(pb::OpenRequest {
                path: Some(path("/link-dir").expect("valid path")),
                flags: libc::O_RDONLY as u32,
                kind: pb::OpenKind::File as i32,
            }),
        ),
    );
    assert!(!open_file.ok, "file open should fail: {open_file:?}");
    assert_eq!(open_file.errno, Some(Errno::IsDirectory));
}

#[test]
fn service_adapter_preserves_inode_mapping_across_rename_invalidations() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("old.txt"), b"data").expect("fixture file");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let lookup_old = dispatch(
        &dispatcher,
        request(
            "lookup-old",
            RequestPayload::Lookup(pb::LookupRequest {
                path: Some(path("/old.txt").expect("valid path")),
            }),
        ),
    );
    let old_inode = lookup_inode(lookup_old);

    let rename = dispatch(
        &dispatcher,
        request(
            "rename-file",
            RequestPayload::Rename(pb::RenameRequest {
                old_path: Some(path("/old.txt").expect("valid path")),
                new_path: Some(path("/new.txt").expect("valid path")),
            }),
        ),
    );
    assert!(rename.ok, "rename failed: {rename:?}");
    assert_eq!(rename.invalidations.len(), 1);
    assert_eq!(rename.invalidations[0].old_path, "/old.txt");
    assert_eq!(rename.invalidations[0].new_path, "/new.txt");

    let lookup_new = dispatch(
        &dispatcher,
        request(
            "lookup-new",
            RequestPayload::Lookup(pb::LookupRequest {
                path: Some(path("/new.txt").expect("valid path")),
            }),
        ),
    );
    assert_eq!(lookup_inode(lookup_new), old_inode);
}

#[test]
fn service_adapter_create_response_supplies_created_inode_for_common_invalidation() {
    let root = tempfile::tempdir().expect("tempdir");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let create = dispatch(
        &dispatcher,
        request(
            "create-file",
            RequestPayload::Create(pb::CreateRequest {
                path: Some(path("/created.txt").expect("valid path")),
                flags: libc::O_RDWR as u32,
                mode: 0o644,
            }),
        ),
    );

    let created_inode = match create.payload.as_ref().expect("create payload") {
        ResponsePayload::Create(value) => value.attr.as_ref().expect("attr").inode,
        other => panic!("unexpected create payload: {other:?}"),
    };
    assert!(create.ok, "create failed: {create:?}");
    assert_eq!(create.invalidations.len(), 1);
    assert_eq!(create.invalidations[0].path, "/created.txt");
    assert_eq!(create.invalidations[0].inode, created_inode);
}

#[test]
fn service_adapter_overlay_removes_nested_file_then_directory() {
    let backing = tempfile::tempdir().expect("backing");
    let alias = tempfile::tempdir().expect("alias");
    let cow = tempfile::tempdir().expect("cow");
    let fs = Arc::new(overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsLimits::default(),
        0o022,
        false,
        false,
        false,
        false,
        true,
        true,
    ));
    let dispatcher = Dispatcher::new(FabricFsFileSystemService::new(fs));

    let mkdir = dispatch(
        &dispatcher,
        request(
            "mkdir-dir",
            RequestPayload::Mkdir(pb::MkdirRequest {
                path: Some(path("/dir").expect("valid path")),
                mode: 0o755,
            }),
        ),
    );
    assert!(mkdir.ok, "mkdir failed: {mkdir:?}");

    let create = dispatch(
        &dispatcher,
        request(
            "create-nested",
            RequestPayload::Create(pb::CreateRequest {
                path: Some(path("/dir/nested.txt").expect("valid path")),
                flags: libc::O_RDWR as u32,
                mode: 0o644,
            }),
        ),
    );
    assert!(create.ok, "create failed: {create:?}");

    let unlink = dispatch(
        &dispatcher,
        request(
            "unlink-nested",
            RequestPayload::Unlink(pb::UnlinkRequest {
                path: Some(path("/dir/nested.txt").expect("valid path")),
            }),
        ),
    );
    assert!(unlink.ok, "unlink failed: {unlink:?}");

    let rmdir = dispatch(
        &dispatcher,
        request(
            "rmdir-dir",
            RequestPayload::Rmdir(pb::RmdirRequest {
                path: Some(path("/dir").expect("valid path")),
            }),
        ),
    );
    assert!(rmdir.ok, "rmdir failed: {rmdir:?}");
}

#[test]
fn service_adapter_listxattr_honors_probe_and_small_buffer_contracts() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("file.txt"), b"data").expect("fixture file");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let set = dispatch(
        &dispatcher,
        request(
            "set-xattr",
            RequestPayload::Setxattr(pb::SetxattrRequest {
                path: Some(path("/file.txt").expect("valid path")),
                name: "user.fabricfs_common_stack".into(),
                value: b"value".to_vec(),
                flags: 0,
            }),
        ),
    );
    assert!(set.ok, "setxattr failed: {set:?}");

    let probe = dispatch(
        &dispatcher,
        request(
            "list-xattr-probe",
            RequestPayload::Listxattr(pb::ListxattrRequest {
                path: Some(path("/file.txt").expect("valid path")),
                size: 0,
            }),
        ),
    );
    assert!(probe.ok, "listxattr probe failed: {probe:?}");
    let names = match probe.payload.expect("listxattr payload") {
        ResponsePayload::Listxattr(value) => value.names,
        other => panic!("unexpected listxattr payload: {other:?}"),
    };
    assert!(names
        .iter()
        .any(|name| name == "user.fabricfs_common_stack"));

    let too_small = dispatch(
        &dispatcher,
        request(
            "list-xattr-too-small",
            RequestPayload::Listxattr(pb::ListxattrRequest {
                path: Some(path("/file.txt").expect("valid path")),
                size: 1,
            }),
        ),
    );
    assert!(!too_small.ok, "small listxattr buffer should fail");
    assert_eq!(too_small.errno, Some(Errno::Range));
    assert!(too_small.payload.is_none());
}

#[test]
fn service_adapter_readdir_end_flag_reflects_pagination() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("a.txt"), b"a").expect("fixture file");
    std::fs::write(root.path().join("b.txt"), b"b").expect("fixture file");
    std::fs::write(root.path().join("c.txt"), b"c").expect("fixture file");
    let dispatcher = dispatcher_for(root.path().to_path_buf());

    let first_page = dispatch(
        &dispatcher,
        request(
            "readdir-first-page",
            RequestPayload::Readdir(pb::ReaddirRequest {
                path: Some(path("/").expect("valid path")),
                offset: 0,
                max_entries: 2,
            }),
        ),
    );
    assert!(first_page.ok, "first readdir failed: {first_page:?}");
    let first_page = match first_page.payload.expect("readdir payload") {
        ResponsePayload::Readdir(value) => value,
        other => panic!("unexpected readdir payload: {other:?}"),
    };
    assert_eq!(first_page.entries.len(), 2);
    assert!(
        !first_page.end,
        "first page should not claim end of directory"
    );

    let final_page = dispatch(
        &dispatcher,
        request(
            "readdir-final-page",
            RequestPayload::Readdir(pb::ReaddirRequest {
                path: Some(path("/").expect("valid path")),
                offset: 2,
                max_entries: 2,
            }),
        ),
    );
    assert!(final_page.ok, "final readdir failed: {final_page:?}");
    let final_page = match final_page.payload.expect("readdir payload") {
        ResponsePayload::Readdir(value) => value,
        other => panic!("unexpected readdir payload: {other:?}"),
    };
    assert_eq!(final_page.entries.len(), 1);
    assert!(final_page.end, "final page should claim end of directory");
}

fn dispatcher_for(root: std::path::PathBuf) -> Dispatcher<FabricFsFileSystemService> {
    let fs = Arc::new(passthrough_fs(root, FsOptions::default()));
    Dispatcher::new(FabricFsFileSystemService::new(fs))
}

fn request(id: &str, payload: RequestPayload) -> RequestEnvelope {
    RequestEnvelope::new(id, "fabricfs-test", 0, pb::TraceContext::default(), payload)
        .expect("request is valid")
        .with_caller(pb::CallerContext {
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            pid: std::process::id(),
        })
}

fn dispatch(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    request: RequestEnvelope,
) -> fs_protocol::ResponseEnvelope {
    dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0))
}

fn lookup_inode(response: fs_protocol::ResponseEnvelope) -> u64 {
    assert!(response.ok, "lookup failed: {response:?}");
    match response.payload.expect("lookup payload") {
        ResponsePayload::Lookup(value) => value.attr.expect("attr").inode,
        other => panic!("unexpected lookup payload: {other:?}"),
    }
}
