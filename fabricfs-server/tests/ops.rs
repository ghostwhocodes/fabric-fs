use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use fabricfs_server::overlay::OverlayFs;
use fabricfs_server::passthrough::PassthroughFs;
use fabricfs_server::server::{
    apply_ownership, CopyFileRangeHandles, FsOptions, FuseContext, HandleKind, OpenedObjectStorage,
    ServerStorage,
};
use fabricfs_server::service::FabricFsFileSystemService;
use fs_core::{Dispatcher, FileSystemService, RpcMetadata};
use fs_protocol::{
    path, pb, Errno, InvalidationKind, Operation, RequestEnvelope, RequestPayload,
    ResponseEnvelope, ResponsePayload, LOCK_EXCLUSIVE, LOCK_NONBLOCK, LOCK_SHARED, SEEK_DATA,
    SEEK_SET,
};
use tempfile::TempDir;

fn ctx() -> FuseContext {
    FuseContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: 0,
    }
}

fn blocked_ctx() -> FuseContext {
    fn other_id(id: u32) -> u32 {
        if id == u32::MAX {
            id - 1
        } else {
            id + 1
        }
    }

    let owner = ctx();
    FuseContext {
        uid: other_id(owner.uid),
        gid: other_id(owner.gid),
        pid: 0,
    }
}

fn root_only_mismatched_ctx() -> Option<FuseContext> {
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping uid-mismatch regression test; requires root");
        return None;
    }

    Some(FuseContext {
        uid: 12345,
        gid: 12345,
        pid: 0,
    })
}

fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn make_world_writable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o777)).expect("chmod test directory");
}

fn make_world_searchable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod test directory");
}

fn assert_path_absent(path: &Path) {
    assert!(
        matches!(
            fs::symlink_metadata(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        ),
        "{} should not exist",
        path.display()
    );
}

fn assert_flush_and_fsync_use_open_handle_after_unlink(
    fs: Arc<dyn ServerStorage + Send + Sync>,
    physical_path: &Path,
) {
    let (fh, _) = fs
        .create_file("/tracked", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create tracked file");
    assert!(
        physical_path.exists(),
        "tracked file should exist before unlink"
    );
    fs::remove_file(physical_path).expect("unlink path while handle remains open");
    fs.write_fh("/tracked", fh, 0, b"bytes", Some(ctx()))
        .expect("write through unlinked handle");

    fs.sync_file_fh("/tracked", fh, true)
        .expect("flush open handle");
    fs.sync_file_fh("/tracked", fh, false)
        .expect("fsync open handle");

    fs.release_fh(fh);
}

fn rpc_metadata() -> RpcMetadata {
    RpcMetadata {
        request_id: "direct-service-test".into(),
        namespace: "ops-test".into(),
        caller: Some(pb::CallerContext {
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            pid: std::process::id(),
        }),
        peer_identity: None,
        trace_id: None,
        payload_len: 0,
        received_unix_nanos: 0,
    }
}

#[allow(clippy::too_many_arguments)]
fn overlay_fs(
    backing: Option<std::path::PathBuf>,
    alias: Option<std::path::PathBuf>,
    cow: Option<std::path::PathBuf>,
    limits: fabricfs_server::server::FsLimits,
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

/// Create an OverlayFs with the given paths
fn create_overlay_fs(
    backing: Option<std::path::PathBuf>,
    alias: Option<std::path::PathBuf>,
    cow: Option<std::path::PathBuf>,
    options: FsOptions,
) -> Arc<dyn ServerStorage + Send + Sync> {
    if backing.is_some() || alias.is_some() || cow.is_some() {
        Arc::new(overlay_fs(
            backing,
            alias,
            cow,
            options.limits.clone(),
            options.umask,
            options.propagate_acls,
            options.allow_backing_permission_updates,
            options.allow_xattr_updates,
            options.allow_direct_backing_updates,
            true, // enable_reflinks
            true, // preserve_sparse_files
        ))
    } else {
        let root = "/tmp".into();
        Arc::new(passthrough_fs(root, options))
    }
}

#[test]
fn cow_writes_copy_and_persist_without_touching_backing() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();

    let backing_file = backing.path().join("file.txt");
    fs::write(&backing_file, b"backing").expect("seed backing");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    let before = fs
        .read("/file.txt", 0, 7, Some(ctx()))
        .expect("read backing");
    assert_eq!(before, b"backing");

    fs.write("/file.txt", 0, b"overlay", Some(ctx()))
        .expect("write overlay");

    let backing_contents = fs::read(&backing_file).expect("read backing");
    assert_eq!(
        backing_contents, b"backing",
        "backing layer must remain unchanged"
    );

    drop(fs);

    let fs2 = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let after = fs2
        .read("/file.txt", 0, 32, Some(ctx()))
        .expect("read overlay copy");
    assert_eq!(after, b"overlay");
}

#[test]
fn create_file_existing_backing_copies_up_without_truncating() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();

    let backing_file = backing.path().join("file.txt");
    fs::write(&backing_file, b"backing").expect("seed backing");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    fs.create_file("/file.txt", 0o644, 0, Some(ctx()))
        .expect("open existing through create");

    assert_eq!(
        fs.read("/file.txt", 0, 32, Some(ctx()))
            .expect("read copied-up file"),
        b"backing"
    );
    assert_eq!(
        fs::read(&backing_file).expect("read backing"),
        b"backing",
        "opening an existing file must not mutate backing contents"
    );
}

#[test]
fn create_file_existing_requires_parent_search_permission() {
    let Some(request_ctx) = root_only_mismatched_ctx() else {
        return;
    };

    let passthrough_root = temp_dir();
    let passthrough_parent = passthrough_root.path().join("blocked");
    fs::create_dir(&passthrough_parent).expect("create passthrough parent");
    let passthrough_file = passthrough_parent.join("file.txt");
    fs::write(&passthrough_file, b"data").expect("seed passthrough file");
    apply_ownership(&passthrough_parent, request_ctx.uid, request_ctx.gid, true)
        .expect("own passthrough parent");
    apply_ownership(&passthrough_file, request_ctx.uid, request_ctx.gid, true)
        .expect("own passthrough file");
    fs::set_permissions(&passthrough_parent, fs::Permissions::from_mode(0o600))
        .expect("chmod passthrough parent");
    fs::set_permissions(&passthrough_file, fs::Permissions::from_mode(0o600))
        .expect("chmod passthrough file");

    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .create_file(
                "/blocked/file.txt",
                0o644,
                libc::O_WRONLY | libc::O_CREAT,
                Some(request_ctx),
            )
            .unwrap_err(),
        libc::EACCES
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    make_world_writable(alias.path());
    make_world_writable(cow.path());
    let backing_parent = backing.path().join("blocked");
    fs::create_dir(&backing_parent).expect("create overlay parent");
    let backing_file = backing_parent.join("file.txt");
    fs::write(&backing_file, b"data").expect("seed overlay file");
    apply_ownership(&backing_parent, request_ctx.uid, request_ctx.gid, true)
        .expect("own overlay parent");
    apply_ownership(&backing_file, request_ctx.uid, request_ctx.gid, true)
        .expect("own overlay file");
    fs::set_permissions(&backing_parent, fs::Permissions::from_mode(0o600))
        .expect("chmod overlay parent");
    fs::set_permissions(&backing_file, fs::Permissions::from_mode(0o600))
        .expect("chmod overlay file");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(
        overlay
            .create_file(
                "/blocked/file.txt",
                0o644,
                libc::O_WRONLY | libc::O_CREAT,
                Some(request_ctx),
            )
            .unwrap_err(),
        libc::EACCES
    );
    assert!(
        !cow.path().join("blocked/file.txt").exists(),
        "rejected open must not copy up the backing file"
    );
}

#[test]
fn cow_truncate_existing_backing_creates_empty_copy_without_reading_source() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();

    let backing_file = backing.path().join("write_only.txt");
    fs::write(&backing_file, b"backing data").expect("seed backing file");
    fs::set_permissions(&backing_file, fs::Permissions::from_mode(0o200))
        .expect("chmod backing file");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    let (_, stat) = fs
        .create_file(
            "/write_only.txt",
            0o644,
            libc::O_WRONLY | libc::O_TRUNC,
            Some(ctx()),
        )
        .expect("truncate existing write-only backing file");
    assert_eq!(stat.size, 0);

    let cow_file = cow.path().join("write_only.txt");
    let cow_meta = fs::metadata(&cow_file).expect("metadata for empty cow copy");
    assert_eq!(cow_meta.len(), 0);
    assert_eq!(cow_meta.mode() & 0o777, 0o200);
    assert_eq!(
        fs::metadata(&backing_file)
            .expect("metadata for original backing")
            .len(),
        b"backing data".len() as u64,
        "truncate through COW must not mutate backing contents"
    );
}

#[test]
fn deletions_in_cow_hide_backing_after_restart() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();
    let target = backing.path().join("gone.txt");
    fs::write(&target, b"keep").unwrap();

    {
        let fs = create_overlay_fs(
            Some(backing.path().to_path_buf()),
            Some(alias.path().to_path_buf()),
            Some(cow.path().to_path_buf()),
            FsOptions::default(),
        );
        fs.unlink("/gone.txt", Some(ctx()))
            .expect("unlink through cow");
        assert!(
            fs.unlink("/gone.txt", Some(ctx())).is_err(),
            "second unlink should fail"
        );
    }

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = fs.read("/gone.txt", 0, 4, Some(ctx())).unwrap_err();
    assert_eq!(err, libc::ENOENT);
}

#[test]
fn passthrough_writes_persist_to_backing() {
    let backing = temp_dir();
    let alias = temp_dir();
    let opts = FsOptions {
        allow_direct_backing_updates: true,
        ..FsOptions::default()
    };
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        opts,
    );

    let (_fh, _) = fs
        .create_file("/data.bin", 0o644, 0, Some(ctx()))
        .expect("create");
    fs.write("/data.bin", 0, b"persist", Some(ctx()))
        .expect("write");

    drop(fs);

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        FsOptions::default(),
    );
    let bytes = fs.read("/data.bin", 0, 16, Some(ctx())).expect("read");
    assert_eq!(bytes, b"persist");
}

#[test]
fn rename_in_cow_copies_up_and_hides_source() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();
    fs::write(backing.path().join("old.txt"), b"backing").unwrap();

    {
        let fs = create_overlay_fs(
            Some(backing.path().to_path_buf()),
            Some(alias.path().to_path_buf()),
            Some(cow.path().to_path_buf()),
            FsOptions::default(),
        );
        fs.rename("/old.txt", "/new.txt", Some(ctx()))
            .expect("rename");
        assert_eq!(fs.read("/new.txt", 0, 16, Some(ctx())).unwrap(), b"backing");
        assert_eq!(
            fs.read("/old.txt", 0, 8, Some(ctx())).unwrap_err(),
            libc::ENOENT
        );
    }

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(fs.read("/new.txt", 0, 16, Some(ctx())).unwrap(), b"backing");
    assert_eq!(
        fs.read("/old.txt", 0, 8, Some(ctx())).unwrap_err(),
        libc::ENOENT,
        "tombstone should survive restart"
    );
    let backing_bytes = fs::read(backing.path().join("old.txt")).unwrap();
    assert_eq!(backing_bytes, b"backing", "backing should stay untouched");
}

#[test]
fn rename_without_alias_path_is_read_only() {
    let backing = temp_dir();
    fs::write(backing.path().join("file.txt"), b"backing").unwrap();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        None,
        None,
        FsOptions::default(),
    );
    let err = fs
        .rename("/file.txt", "/other.txt", Some(ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EROFS);
}

#[test]
fn destructive_ops_enforce_fuse_context_permissions() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    let owner_ctx = ctx();
    let blocked_ctx = FuseContext {
        uid: owner_ctx.uid.saturating_add(1),
        gid: owner_ctx.gid.saturating_add(1),
        pid: 0,
    };

    fs.create_file("/owned.txt", 0o644, 0, Some(owner_ctx))
        .expect("create");

    let err = fs.unlink("/owned.txt", Some(blocked_ctx)).unwrap_err();
    assert_eq!(err, libc::EACCES, "unlink must honor fuse_context");

    fs.unlink("/owned.txt", Some(owner_ctx))
        .expect("owner can unlink");

    fs.create_file("/src.txt", 0o644, 0, Some(owner_ctx))
        .expect("create src");
    let err = fs
        .rename("/src.txt", "/dst.txt", Some(blocked_ctx))
        .unwrap_err();
    assert_eq!(err, libc::EACCES, "rename must honor fuse_context");

    fs.rename("/src.txt", "/dst.txt", Some(owner_ctx))
        .expect("owner can rename");
    assert_eq!(fs.read("/dst.txt", 0, 8, Some(owner_ctx)).unwrap(), b"");
}

#[test]
fn copy_file_range_handler_uses_open_handles_after_path_replacement() {
    let root = temp_dir();
    let src = root.path().join("secret.txt");
    let old_src = root.path().join("old-secret.txt");
    let dst = root.path().join("dst.txt");
    fs::write(&src, b"secret").expect("seed source");
    fs::write(&dst, b"empty!").expect("seed destination");
    fs::set_permissions(&dst, fs::Permissions::from_mode(0o666)).expect("chmod destination");

    let passthrough = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    let (fh_in, _) = passthrough
        .open("/secret.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open source handle");
    let (fh_out, _) = passthrough
        .open("/dst.txt", HandleKind::File, libc::O_WRONLY, Some(ctx()))
        .expect("open destination handle");
    let fs: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough);

    fs::rename(&src, &old_src).expect("move open source path aside");
    fs::write(&src, b"public").expect("replace source path");
    fs::set_permissions(&src, fs::Permissions::from_mode(0o666)).expect("chmod replacement source");

    let copied = fs
        .copy_file_range(
            "/secret.txt",
            "/dst.txt",
            CopyFileRangeHandles { fh_in, fh_out },
            0,
            0,
            6,
            Some(blocked_ctx()),
        )
        .expect("copy_file_range through open handles");
    assert_eq!(copied, 6);
    assert_eq!(fs::read(&dst).expect("read destination"), b"secret");
}

#[test]
fn copy_file_range_rejects_handles_without_required_access() {
    let root = temp_dir();
    fs::write(root.path().join("src.txt"), b"src").expect("seed source");
    fs::write(root.path().join("dst.txt"), b"dst").expect("seed destination");

    let passthrough = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    let (write_only_src, _) = passthrough
        .open("/src.txt", HandleKind::File, libc::O_WRONLY, Some(ctx()))
        .expect("open source write-only");
    let (write_dst, _) = passthrough
        .open("/dst.txt", HandleKind::File, libc::O_WRONLY, Some(ctx()))
        .expect("open destination writable");
    let err = passthrough
        .copy_file_range(
            "/src.txt",
            "/dst.txt",
            CopyFileRangeHandles {
                fh_in: write_only_src,
                fh_out: write_dst,
            },
            0,
            0,
            3,
            Some(ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EBADF, "source handle must be readable");

    let (read_src, _) = passthrough
        .open("/src.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open source readable");
    let (read_only_dst, _) = passthrough
        .open("/dst.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open destination read-only");
    let err = passthrough
        .copy_file_range(
            "/src.txt",
            "/dst.txt",
            CopyFileRangeHandles {
                fh_in: read_src,
                fh_out: read_only_dst,
            },
            0,
            0,
            3,
            Some(ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EBADF, "destination handle must be writable");
}

fn assert_copy_file_range_rejects_overlapping_same_file_ranges(
    fs: Arc<dyn ServerStorage + Send + Sync>,
) {
    let (fh, _) = fs
        .create_file("/copy.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create copy target");
    fs.write_fh("/copy.txt", fh, 0, b"abcdef", Some(ctx()))
        .expect("seed copy target");

    let copied = fs
        .copy_file_range(
            "/copy.txt",
            "/copy.txt",
            CopyFileRangeHandles {
                fh_in: fh,
                fh_out: fh,
            },
            0,
            6,
            3,
            Some(ctx()),
        )
        .expect("same-file non-overlapping copy succeeds");
    assert_eq!(copied, 3);
    assert_eq!(
        fs.read_fh("/copy.txt", fh, 0, 16, Some(ctx()))
            .expect("read after non-overlapping copy"),
        b"abcdefabc"
    );

    let err = fs
        .copy_file_range(
            "/copy.txt",
            "/copy.txt",
            CopyFileRangeHandles {
                fh_in: fh,
                fh_out: fh,
            },
            0,
            2,
            4,
            Some(ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EINVAL, "forward overlap must be rejected");

    let err = fs
        .copy_file_range(
            "/copy.txt",
            "/copy.txt",
            CopyFileRangeHandles {
                fh_in: fh,
                fh_out: fh,
            },
            2,
            0,
            4,
            Some(ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EINVAL, "backward overlap must be rejected");

    fs.release_fh(fh);
}

#[test]
fn copy_file_range_rejects_overlapping_same_file_ranges_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_copy_file_range_rejects_overlapping_same_file_ranges(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_copy_file_range_rejects_overlapping_same_file_ranges(overlay);
}

fn assert_lseek_uses_open_handle_after_path_replacement(
    fs: Arc<dyn ServerStorage + Send + Sync>,
    physical_path: &Path,
) {
    let (fh, _) = fs
        .create_file("/seek.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create seek target");
    fs.write_fh("/seek.txt", fh, 0, b"original-data", Some(ctx()))
        .expect("seed through handle");

    let old_physical_path = physical_path.with_file_name("seek-old.txt");
    fs::rename(physical_path, &old_physical_path).expect("replace path after open");
    fs::write(physical_path, b"x").expect("write replacement path");

    assert_eq!(
        fs.lseek("/seek.txt", fh, 0, libc::SEEK_END)
            .expect("lseek through open handle"),
        13
    );
    assert_eq!(
        fs.lseek("/seek.txt", fh + 10_000, 0, libc::SEEK_END)
            .unwrap_err(),
        libc::EBADF
    );

    fs.release_fh(fh);
}

#[test]
fn lseek_uses_open_handle_after_path_replacement_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_lseek_uses_open_handle_after_path_replacement(
        passthrough,
        &passthrough_root.path().join("seek.txt"),
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_lseek_uses_open_handle_after_path_replacement(overlay, &cow.path().join("seek.txt"));
}

fn assert_lseek_seek_cur_uses_logical_open_file_offset(fs: Arc<dyn ServerStorage + Send + Sync>) {
    let (fh, _) = fs
        .create_file("/seek-cur.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create seek target");

    fs.write_fh("/seek-cur.txt", fh, 10, b"abc", Some(ctx()))
        .expect("explicit offset write");
    assert_eq!(
        fs.lseek("/seek-cur.txt", fh, 0, libc::SEEK_CUR)
            .expect("initial seek-cur"),
        0
    );

    assert_eq!(
        fs.lseek("/seek-cur.txt", fh, 5, libc::SEEK_SET)
            .expect("seek-set updates logical position"),
        5
    );
    fs.read_fh("/seek-cur.txt", fh, 0, 1, Some(ctx()))
        .expect("explicit offset read");
    fs.write_fh("/seek-cur.txt", fh, 9, b"z", Some(ctx()))
        .expect("second explicit offset write");
    assert_eq!(
        fs.lseek("/seek-cur.txt", fh, 2, libc::SEEK_CUR)
            .expect("seek-cur ignores explicit offset io cursor changes"),
        7
    );
    assert_eq!(
        fs.lseek("/seek-cur.txt", fh, -3, libc::SEEK_END)
            .expect("seek-end uses current file length"),
        10
    );
    assert_eq!(
        fs.lseek("/seek-cur.txt", fh, -1, libc::SEEK_SET)
            .unwrap_err(),
        libc::EINVAL
    );

    fs.release_fh(fh);
}

#[test]
fn lseek_seek_cur_is_independent_from_explicit_offset_io_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_lseek_seek_cur_uses_logical_open_file_offset(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_lseek_seek_cur_uses_logical_open_file_offset(overlay);
}

fn assert_setattr_uses_open_handle_after_unlink(
    fs: Arc<dyn ServerStorage + Send + Sync>,
    physical_path: &Path,
) {
    let (fh, _) = fs
        .create_file("/truncate.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create truncate target");
    fs.write_fh("/truncate.txt", fh, 0, b"abcdef", Some(ctx()))
        .expect("seed through handle");

    fs::remove_file(physical_path).expect("unlink path while handle remains open");
    let stat = fs
        .setattr_fh("/truncate.txt", fh, None, None, None, Some(3), Some(ctx()))
        .expect("truncate through open unlinked handle");

    assert_eq!(stat.size, 3);
    assert_eq!(
        fs.read_fh("/truncate.txt", fh, 0, 16, Some(ctx()))
            .expect("read truncated open handle"),
        b"abc"
    );

    let owner = ctx();
    let stat = fs
        .setattr_fh(
            "/truncate.txt",
            fh,
            Some(0o600),
            Some(owner.uid),
            Some(owner.gid),
            None,
            Some(owner),
        )
        .expect("chmod/chown through open unlinked handle");
    assert_eq!(stat.mode & 0o7777, 0o600);
    assert_eq!(stat.uid, owner.uid);
    assert_eq!(stat.gid, owner.gid);

    fs.release_fh(fh);
}

fn assert_handle_backed_ftruncate_uses_open_access_after_chmod(
    fs: Arc<dyn ServerStorage + Send + Sync>,
    physical_path: &Path,
) {
    let (fh, _) = fs
        .create_file("/truncate-mode.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create truncate target");
    fs.write_fh("/truncate-mode.txt", fh, 0, b"abcdef", Some(ctx()))
        .expect("seed through writable handle");

    fs::set_permissions(physical_path, fs::Permissions::from_mode(0o444))
        .expect("chmod path after open");
    let stat = fs
        .setattr_fh(
            "/truncate-mode.txt",
            fh,
            None,
            None,
            None,
            Some(3),
            Some(ctx()),
        )
        .expect("ftruncate should use open write access, not current mode bits");
    assert_eq!(stat.size, 3);

    let (read_only, _) = fs
        .open(
            "/truncate-mode.txt",
            HandleKind::File,
            libc::O_RDONLY,
            Some(ctx()),
        )
        .expect("open read-only handle");
    assert_eq!(
        fs.setattr_fh(
            "/truncate-mode.txt",
            read_only,
            None,
            None,
            None,
            Some(1),
            Some(ctx()),
        )
        .expect_err("ftruncate on O_RDONLY handle must fail"),
        libc::EBADF
    );

    fs.release_fh(read_only);
    fs.release_fh(fh);
}

#[test]
fn setattr_uses_open_handle_after_unlink_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_setattr_uses_open_handle_after_unlink(
        passthrough,
        &passthrough_root.path().join("truncate.txt"),
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_setattr_uses_open_handle_after_unlink(overlay, &cow.path().join("truncate.txt"));
}

#[test]
fn handle_backed_ftruncate_uses_open_access_after_chmod_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_handle_backed_ftruncate_uses_open_access_after_chmod(
        passthrough,
        &passthrough_root.path().join("truncate-mode.txt"),
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_handle_backed_ftruncate_uses_open_access_after_chmod(
        overlay,
        &cow.path().join("truncate-mode.txt"),
    );
}

fn assert_fsyncdir_uses_open_dir_handle(fs: Arc<dyn ServerStorage + Send + Sync>) {
    fs.mkdir("/syncdir", 0o755, Some(ctx()))
        .expect("create directory");
    let (fh, _) = fs
        .open("/syncdir", HandleKind::Dir, libc::O_RDONLY, Some(ctx()))
        .expect("open directory handle");

    fs.rename("/syncdir", "/syncdir-renamed", Some(ctx()))
        .expect("rename open directory");
    assert_eq!(
        fs.sync_dir_fh("/syncdir", fh, false)
            .expect_err("stale directory path must not validate handle"),
        libc::EBADF
    );
    fs.sync_dir_fh("/syncdir-renamed", fh, false)
        .expect("fsyncdir should use retained directory handle");
    assert_eq!(
        fs.sync_dir_fh("/syncdir-renamed", fh + 1, false)
            .expect_err("unknown directory handle must fail"),
        libc::EBADF
    );

    fs.release_fh(fh);
    assert_eq!(
        fs.sync_dir_fh("/syncdir-renamed", fh, false)
            .expect_err("released directory handle must fail"),
        libc::EBADF
    );
}

#[test]
fn fsyncdir_uses_open_dir_handle_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_fsyncdir_uses_open_dir_handle(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_fsyncdir_uses_open_dir_handle(overlay);
}

fn assert_fallocate_uses_open_handle_without_shrinking(fs: Arc<dyn ServerStorage + Send + Sync>) {
    let (fh, _) = fs
        .create_file("/alloc.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create fallocate target");
    fs.write_fh("/alloc.txt", fh, 0, b"0123456789", Some(ctx()))
        .expect("seed file through handle");

    fs.fallocate("/alloc.txt", fh, 0, 4, 0)
        .expect("fallocate inside existing range");
    assert_eq!(
        fs.read_fh("/alloc.txt", fh, 0, 32, Some(ctx()))
            .expect("read after no-shrink fallocate"),
        b"0123456789"
    );

    fs.fallocate("/alloc.txt", fh, 20, 5, 0)
        .expect("fallocate extends file");
    assert_eq!(fs.stat("/alloc.txt").expect("stat extended file").size, 25);
    assert_eq!(
        fs.fallocate("/alloc.txt", fh, 0, 1, 1).unwrap_err(),
        libc::EOPNOTSUPP
    );
    assert_eq!(
        fs.fallocate("/alloc.txt", fh + 10_000, 0, 1, 0)
            .unwrap_err(),
        libc::EBADF
    );
    fs.release_fh(fh);
}

#[test]
fn fallocate_is_handle_bound_and_never_shrinks_passthrough_or_overlay() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_fallocate_uses_open_handle_without_shrinking(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_fallocate_uses_open_handle_without_shrinking(overlay);
}

fn assert_lock_table_enforces_conflicts_and_release_cleanup(
    fs: Arc<dyn ServerStorage + Send + Sync>,
) {
    let (first, _) = fs
        .create_file("/locked.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create lock target");
    let (second, _) = fs
        .open("/locked.txt", HandleKind::File, libc::O_RDWR, Some(ctx()))
        .expect("open second lock handle");

    fs.setlk("/locked.txt", first, 10, 0, 20, libc::F_WRLCK, 101)
        .expect("owner 10 acquires exclusive lock");
    assert_eq!(
        fs.setlk("/locked.txt", second, 11, 5, 6, libc::F_WRLCK, 202)
            .unwrap_err(),
        libc::EAGAIN
    );
    let conflict = fs
        .getlk("/locked.txt", second, 11, 5, 6, libc::F_WRLCK)
        .expect("getlk succeeds")
        .expect("conflicting lock is reported");
    assert_eq!(conflict.pid, 101);

    fs.setlk("/locked.txt", first, 10, 5, 6, libc::F_UNLCK, 101)
        .expect("partial unlock succeeds");
    fs.setlk("/locked.txt", second, 11, 5, 6, libc::F_WRLCK, 202)
        .expect("second owner can lock unlocked hole");
    assert_eq!(
        fs.setlk("/locked.txt", second, 11, 0, 4, libc::F_WRLCK, 202)
            .unwrap_err(),
        libc::EAGAIN
    );

    fs.release_fh(first);
    fs.setlk("/locked.txt", second, 11, 0, 4, libc::F_WRLCK, 202)
        .expect("release removes first handle locks");
    fs.release_fh(second);
}

#[test]
fn lock_tables_enforce_conflicts_partial_unlocks_and_release_cleanup() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_lock_table_enforces_conflicts_and_release_cleanup(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_lock_table_enforces_conflicts_and_release_cleanup(overlay);
}

fn assert_hardlink_aliases_share_lock_state(fs: Arc<dyn ServerStorage + Send + Sync>) {
    let (source_fh, _) = fs
        .create_file("/source.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create source");
    fs.link("/source.txt", "/alias.txt", Some(ctx()))
        .expect("create hardlink alias");
    let (alias_fh, _) = fs
        .open("/alias.txt", HandleKind::File, libc::O_RDWR, Some(ctx()))
        .expect("open hardlink alias");

    fs.setlk("/source.txt", source_fh, 21, 0, 10, libc::F_WRLCK, 303)
        .expect("lock source path");
    assert_eq!(
        fs.setlk("/alias.txt", alias_fh, 22, 0, 10, libc::F_WRLCK, 404)
            .unwrap_err(),
        libc::EAGAIN
    );
    let conflict = fs
        .getlk("/alias.txt", alias_fh, 22, 0, 10, libc::F_WRLCK)
        .expect("getlk through alias succeeds")
        .expect("alias sees source conflict");
    assert_eq!(conflict.pid, 303);

    fs.release_fh(source_fh);
    fs.setlk("/alias.txt", alias_fh, 22, 0, 10, libc::F_WRLCK, 404)
        .expect("source handle release clears alias-visible lock");
    fs.release_fh(alias_fh);
}

#[test]
fn hardlink_aliases_share_backend_lock_state() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_hardlink_aliases_share_lock_state(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_hardlink_aliases_share_lock_state(overlay);
}

fn assert_flock_is_independent_from_posix_locks(fs: Arc<dyn ServerStorage + Send + Sync>) {
    let (seed_fh, _) = fs
        .create_file("/flock.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create flock target");
    fs.write_fh("/flock.txt", seed_fh, 0, b"lock", Some(ctx()))
        .expect("seed flock target");
    fs.release_fh(seed_fh);

    let (read_only, _) = fs
        .open("/flock.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open read-only flock handle");
    fs.flock("/flock.txt", read_only, 31, LOCK_EXCLUSIVE, 501)
        .expect("exclusive flock is valid on a read-only descriptor");

    let (posix_handle, _) = fs
        .open("/flock.txt", HandleKind::File, libc::O_RDWR, Some(ctx()))
        .expect("open POSIX lock handle");
    fs.setlk("/flock.txt", posix_handle, 32, 0, 10, libc::F_WRLCK, 502)
        .expect("POSIX locks are independent from flock state");

    let (contender, _) = fs
        .open("/flock.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open flock contender");
    assert_eq!(
        fs.flock("/flock.txt", contender, 33, LOCK_SHARED, 503)
            .unwrap_err(),
        libc::EOPNOTSUPP
    );
    assert_eq!(
        fs.flock(
            "/flock.txt",
            contender,
            33,
            LOCK_SHARED | LOCK_NONBLOCK,
            503
        )
        .unwrap_err(),
        libc::EAGAIN
    );

    fs.release_fh(read_only);
    fs.flock("/flock.txt", contender, 33, LOCK_SHARED, 503)
        .expect("handle release clears flock state");

    fs.release_fh(posix_handle);
    fs.release_fh(contender);
}

#[test]
fn overlay_link_rejects_existing_union_destination_without_mutation() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    fs::write(backing.path().join("source.txt"), b"source").expect("seed source");
    fs::write(backing.path().join("dest.txt"), b"backing-dest").expect("seed backing dest");
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    assert_eq!(
        overlay
            .link("/source.txt", "/dest.txt", Some(ctx()))
            .unwrap_err(),
        libc::EEXIST
    );
    assert_path_absent(&cow.path().join("source.txt"));
    assert_path_absent(&cow.path().join("dest.txt"));
    assert_eq!(
        fs::read(backing.path().join("dest.txt")).expect("read backing dest"),
        b"backing-dest"
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    fs::write(backing.path().join("source.txt"), b"source").expect("seed source");
    fs::write(cow.path().join("dest.txt"), b"overlay-dest").expect("seed overlay dest");
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    assert_eq!(
        overlay
            .link("/source.txt", "/dest.txt", Some(ctx()))
            .unwrap_err(),
        libc::EEXIST
    );
    assert_path_absent(&cow.path().join("source.txt"));
    assert_eq!(
        fs::read(cow.path().join("dest.txt")).expect("read overlay dest"),
        b"overlay-dest"
    );
}

#[test]
fn overlay_symlink_rejects_existing_union_destination_without_mutation() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    fs::write(backing.path().join("dest.txt"), b"backing-dest").expect("seed backing dest");
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    assert_eq!(
        overlay
            .symlink("/dest.txt", b"replacement".to_vec(), Some(ctx()))
            .unwrap_err(),
        libc::EEXIST
    );
    assert_path_absent(&cow.path().join("dest.txt"));
    assert_eq!(
        fs::read(backing.path().join("dest.txt")).expect("read backing dest"),
        b"backing-dest"
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    fs::write(cow.path().join("dest.txt"), b"overlay-dest").expect("seed overlay dest");
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    assert_eq!(
        overlay
            .symlink("/dest.txt", b"replacement".to_vec(), Some(ctx()))
            .unwrap_err(),
        libc::EEXIST
    );
    assert_eq!(
        fs::read(cow.path().join("dest.txt")).expect("read overlay dest"),
        b"overlay-dest"
    );
}

#[test]
fn flock_state_is_separate_from_posix_lock_state() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_flock_is_independent_from_posix_locks(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_flock_is_independent_from_posix_locks(overlay);
}

#[test]
fn create_with_trunc_uses_request_owner_for_new_file_checks() {
    let Some(request_ctx) = root_only_mismatched_ctx() else {
        return;
    };

    let passthrough_root = temp_dir();
    make_world_writable(passthrough_root.path());
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    passthrough
        .create_file("/created.txt", 0o644, libc::O_TRUNC, Some(request_ctx))
        .expect("passthrough create with O_TRUNC");
    let meta = fs::metadata(passthrough_root.path().join("created.txt"))
        .expect("passthrough created metadata");
    assert_eq!(meta.uid(), request_ctx.uid);
    assert_eq!(meta.gid(), request_ctx.gid);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    make_world_writable(backing.path());
    make_world_writable(alias.path());
    make_world_writable(cow.path());
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    overlay
        .create_file("/created.txt", 0o644, libc::O_TRUNC, Some(request_ctx))
        .expect("overlay create with O_TRUNC");
    let meta = fs::metadata(cow.path().join("created.txt")).expect("overlay created metadata");
    assert_eq!(meta.uid(), request_ctx.uid);
    assert_eq!(meta.gid(), request_ctx.gid);
}

#[test]
fn create_file_rejects_existing_directories() {
    let passthrough_root = temp_dir();
    fs::create_dir(passthrough_root.path().join("dir")).expect("seed passthrough directory");
    fs::set_permissions(
        passthrough_root.path().join("dir"),
        fs::Permissions::from_mode(0o777),
    )
    .expect("chmod passthrough directory");

    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .create_file("/dir", 0o644, libc::O_RDONLY | libc::O_CREAT, Some(ctx()),)
            .unwrap_err(),
        libc::EISDIR
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    fs::create_dir(backing.path().join("dir")).expect("seed overlay directory");
    fs::set_permissions(
        backing.path().join("dir"),
        fs::Permissions::from_mode(0o777),
    )
    .expect("chmod overlay directory");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(
        overlay
            .create_file("/dir", 0o644, libc::O_RDONLY | libc::O_CREAT, Some(ctx()),)
            .unwrap_err(),
        libc::EISDIR
    );
    assert!(
        !cow.path().join("dir").exists(),
        "rejected create must not copy up a directory"
    );
}

#[test]
fn create_existing_honors_requested_access_flags() {
    let passthrough_root = temp_dir();
    let passthrough_read = passthrough_root.path().join("read_only.txt");
    let passthrough_write = passthrough_root.path().join("write_only.txt");
    fs::write(&passthrough_read, b"read").expect("seed passthrough read-only file");
    fs::write(&passthrough_write, b"write").expect("seed passthrough write-only file");
    fs::set_permissions(&passthrough_read, fs::Permissions::from_mode(0o400))
        .expect("chmod passthrough read-only file");
    fs::set_permissions(&passthrough_write, fs::Permissions::from_mode(0o200))
        .expect("chmod passthrough write-only file");
    fs::set_permissions(passthrough_root.path(), fs::Permissions::from_mode(0o500))
        .expect("chmod passthrough root");

    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    passthrough
        .create_file(
            "/read_only.txt",
            0o644,
            libc::O_RDONLY | libc::O_CREAT,
            Some(ctx()),
        )
        .expect("passthrough O_RDONLY create/open existing file");
    passthrough
        .create_file(
            "/write_only.txt",
            0o644,
            libc::O_WRONLY | libc::O_CREAT,
            Some(ctx()),
        )
        .expect("passthrough O_WRONLY create/open existing file");
    fs::set_permissions(passthrough_root.path(), fs::Permissions::from_mode(0o700))
        .expect("restore passthrough root permissions");

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let backing_read = backing.path().join("read_only.txt");
    let cow_write = cow.path().join("write_only.txt");
    fs::write(&backing_read, b"read").expect("seed overlay backing read-only file");
    fs::write(&cow_write, b"write").expect("seed overlay cow write-only file");
    fs::set_permissions(&backing_read, fs::Permissions::from_mode(0o400))
        .expect("chmod overlay backing read-only file");
    fs::set_permissions(&cow_write, fs::Permissions::from_mode(0o200))
        .expect("chmod overlay cow write-only file");
    fs::set_permissions(backing.path(), fs::Permissions::from_mode(0o500))
        .expect("chmod overlay backing root");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    overlay
        .create_file(
            "/read_only.txt",
            0o644,
            libc::O_RDONLY | libc::O_CREAT,
            Some(ctx()),
        )
        .expect("overlay O_RDONLY create/open existing backing file");
    overlay
        .create_file(
            "/write_only.txt",
            0o644,
            libc::O_WRONLY | libc::O_CREAT,
            Some(ctx()),
        )
        .expect("overlay O_WRONLY create/open existing cow file");
    fs::set_permissions(backing.path(), fs::Permissions::from_mode(0o700))
        .expect("restore overlay backing root permissions");
}

#[test]
fn passthrough_open_handle_read_survives_chmod() {
    let root = temp_dir();
    let file = root.path().join("secret.txt");
    fs::write(&file, b"secret").expect("seed file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("chmod file");

    let fs = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    let (fh, _) = fs
        .open("/secret.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open readable handle");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).expect("chmod closed path");

    assert_eq!(
        fs.read_fh("/secret.txt", fh, 0, 16, Some(ctx()))
            .expect("read through existing handle"),
        b"secret"
    );
    fs.release_fh(fh);
}

#[test]
fn passthrough_open_handle_write_survives_chmod() {
    let root = temp_dir();
    let file = root.path().join("data.txt");
    fs::write(&file, b"before").expect("seed file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("chmod file");

    let fs = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    let (fh, _) = fs
        .open("/data.txt", HandleKind::File, libc::O_WRONLY, Some(ctx()))
        .expect("open writable handle");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).expect("chmod closed path");

    assert_eq!(
        fs.write_fh("/data.txt", fh, 0, b"after", Some(ctx()))
            .expect("write through existing handle"),
        5
    );
    fs.release_fh(fh);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("restore file mode");
    assert_eq!(fs::read(&file).expect("read file"), b"aftere");
}

#[test]
fn overlay_open_handle_read_survives_chmod() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let file = backing.path().join("secret.txt");
    fs::write(&file, b"secret").expect("seed backing file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("chmod backing file");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let (fh, _) = fs
        .open("/secret.txt", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open readable overlay handle");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).expect("chmod closed path");

    assert_eq!(
        fs.read_fh("/secret.txt", fh, 0, 16, Some(ctx()))
            .expect("read through existing overlay handle"),
        b"secret"
    );
    fs.release_fh(fh);
}

#[test]
fn overlay_open_handle_write_survives_chmod() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let file = backing.path().join("data.txt");
    fs::write(&file, b"before").expect("seed backing file");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("chmod backing file");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let (fh, _) = fs
        .open("/data.txt", HandleKind::File, libc::O_WRONLY, Some(ctx()))
        .expect("open writable overlay handle");
    let cow_file = cow.path().join("data.txt");
    fs::set_permissions(&cow_file, fs::Permissions::from_mode(0o000)).expect("chmod cow path");

    assert_eq!(
        fs.write_fh("/data.txt", fh, 0, b"after", Some(ctx()))
            .expect("write through existing overlay handle"),
        5
    );
    fs.release_fh(fh);
    fs::set_permissions(&cow_file, fs::Permissions::from_mode(0o600)).expect("restore cow mode");
    assert_eq!(fs::read(&cow_file).expect("read cow file"), b"aftere");
}

#[test]
fn open_with_trunc_returns_fresh_size() {
    let passthrough_root = temp_dir();
    let passthrough_file = passthrough_root.path().join("data.txt");
    fs::write(&passthrough_file, b"nonempty").expect("seed passthrough file");

    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    let (fh, stat) = passthrough
        .open(
            "/data.txt",
            HandleKind::File,
            libc::O_WRONLY | libc::O_TRUNC,
            Some(ctx()),
        )
        .expect("open passthrough with O_TRUNC");
    assert_eq!(stat.size, 0);
    passthrough.release_fh(fh);
    assert_eq!(fs::read(&passthrough_file).expect("read passthrough"), b"");

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let backing_file = backing.path().join("data.txt");
    fs::write(&backing_file, b"nonempty").expect("seed backing file");
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    let (fh, stat) = overlay
        .open(
            "/data.txt",
            HandleKind::File,
            libc::O_WRONLY | libc::O_TRUNC,
            Some(ctx()),
        )
        .expect("open overlay with O_TRUNC");
    assert_eq!(stat.size, 0);
    overlay.release_fh(fh);
    assert_eq!(
        fs::read(cow.path().join("data.txt")).expect("read cow"),
        b""
    );
    assert_eq!(
        fs::read(&backing_file).expect("read backing"),
        b"nonempty",
        "overlay truncate must not mutate backing"
    );
}

#[test]
fn cow_create_rechecks_preserved_backing_owner() {
    let Some(request_ctx) = root_only_mismatched_ctx() else {
        return;
    };

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    make_world_writable(cow.path());

    let backing_file = backing.path().join("owned.txt");
    fs::write(&backing_file, b"backing").expect("seed backing");
    apply_ownership(&backing_file, request_ctx.uid, request_ctx.gid, true)
        .expect("own backing file");
    fs::set_permissions(&backing_file, fs::Permissions::from_mode(0o644)).expect("chmod backing");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    overlay
        .create_file("/owned.txt", 0o644, 0, Some(request_ctx))
        .expect("COW create/open existing file");

    let meta = fs::metadata(cow.path().join("owned.txt")).expect("copied metadata");
    assert_eq!(meta.uid(), request_ctx.uid);
    assert_eq!(meta.gid(), request_ctx.gid);
}

#[test]
fn cow_setattr_rechecks_preserved_backing_owner() {
    let Some(request_ctx) = root_only_mismatched_ctx() else {
        return;
    };

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();

    let backing_file = backing.path().join("owned.txt");
    fs::write(&backing_file, b"backing").expect("seed backing");
    apply_ownership(&backing_file, request_ctx.uid, request_ctx.gid, true)
        .expect("own backing file");
    fs::set_permissions(&backing_file, fs::Permissions::from_mode(0o644)).expect("chmod backing");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let stat = overlay
        .setattr("/owned.txt", None, None, None, Some(3), Some(request_ctx))
        .expect("COW truncate backing-owned file");
    assert_eq!(stat.size, 3);

    let copied = cow.path().join("owned.txt");
    let meta = fs::metadata(&copied).expect("copied metadata");
    assert_eq!(meta.uid(), request_ctx.uid);
    assert_eq!(meta.gid(), request_ctx.gid);
    assert_eq!(fs::read(copied).expect("read truncated copy"), b"bac");
}

#[test]
fn reads_enforce_fuse_context_permissions() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let secret = backing.path().join("secret.txt");
    fs::write(&secret, b"secret").expect("seed secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod secret");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = overlay
        .read("/secret.txt", 0, 16, Some(blocked_ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
    assert_eq!(
        overlay.read("/secret.txt", 0, 16, Some(ctx())).unwrap(),
        b"secret"
    );

    let passthrough_root = temp_dir();
    let passthrough_secret = passthrough_root.path().join("secret.txt");
    fs::write(&passthrough_secret, b"secret").expect("seed passthrough secret");
    fs::set_permissions(&passthrough_secret, fs::Permissions::from_mode(0o600))
        .expect("chmod passthrough secret");
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    let err = passthrough
        .read("/secret.txt", 0, 16, Some(blocked_ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
}

#[test]
fn reads_authorize_followed_symlink_targets() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let secret = backing.path().join("secret.txt");
    fs::write(&secret, b"secret").expect("seed secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod secret");
    std::os::unix::fs::symlink("secret.txt", backing.path().join("link.txt"))
        .expect("symlink secret");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(
        overlay
            .read("/link.txt", 0, 16, Some(blocked_ctx()))
            .unwrap_err(),
        libc::EACCES
    );
    assert_eq!(
        overlay.read("/link.txt", 0, 16, Some(ctx())).unwrap(),
        b"secret"
    );

    let passthrough_root = temp_dir();
    let passthrough_secret = passthrough_root.path().join("secret.txt");
    fs::write(&passthrough_secret, b"secret").expect("seed passthrough secret");
    fs::set_permissions(&passthrough_secret, fs::Permissions::from_mode(0o600))
        .expect("chmod passthrough secret");
    std::os::unix::fs::symlink("secret.txt", passthrough_root.path().join("link.txt"))
        .expect("passthrough symlink secret");
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .read("/link.txt", 0, 16, Some(blocked_ctx()))
            .unwrap_err(),
        libc::EACCES
    );
    assert_eq!(
        passthrough.read("/link.txt", 0, 16, Some(ctx())).unwrap(),
        b"secret"
    );
}

#[test]
fn setattr_enforces_fuse_context_permissions() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let target = backing.path().join("owned.txt");
    fs::write(&target, b"owned").expect("seed target");
    fs::set_permissions(&target, fs::Permissions::from_mode(0o644)).expect("chmod target");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    let chmod_err = fs
        .setattr(
            "/owned.txt",
            Some(0o600),
            None,
            None,
            None,
            Some(blocked_ctx()),
        )
        .unwrap_err();
    assert_eq!(chmod_err, libc::EACCES);

    let truncate_err = fs
        .setattr("/owned.txt", None, None, None, Some(0), Some(blocked_ctx()))
        .unwrap_err();
    assert_eq!(truncate_err, libc::EACCES);

    let stat = fs
        .setattr("/owned.txt", None, None, None, Some(2), Some(ctx()))
        .expect("owner truncate");
    assert_eq!(stat.size, 2);
}

#[test]
fn passthrough_setattr_authorizes_followed_symlink_target() {
    let root = temp_dir();
    let secret = root.path().join("secret.txt");
    fs::write(&secret, b"secret").expect("seed secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod secret");
    std::os::unix::fs::symlink("secret.txt", root.path().join("link.txt")).expect("symlink secret");

    let passthrough = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .setattr("/link.txt", None, None, None, Some(0), Some(blocked_ctx()))
            .unwrap_err(),
        libc::EACCES
    );
    assert_eq!(fs::read(&secret).expect("read secret"), b"secret");

    let stat = passthrough
        .setattr("/link.txt", None, None, None, Some(3), Some(ctx()))
        .expect("owner truncates followed target");
    assert_eq!(stat.size, 3);
    assert_eq!(fs::read(&secret).expect("read truncated secret"), b"sec");
}

#[test]
fn overlay_mutations_reject_symlink_paths() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let secret = backing.path().join("secret.txt");
    fs::write(&secret, b"secret").expect("seed secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod secret");
    std::os::unix::fs::symlink("secret.txt", backing.path().join("link.txt"))
        .expect("symlink secret");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    assert_eq!(
        overlay
            .write("/link.txt", 0, b"x", Some(ctx()))
            .unwrap_err(),
        libc::ELOOP
    );
    assert_eq!(
        overlay
            .setattr("/link.txt", None, None, None, Some(0), Some(ctx()))
            .unwrap_err(),
        libc::ELOOP
    );
    assert_eq!(
        overlay
            .create_file("/link.txt", 0o644, libc::O_TRUNC, Some(ctx()))
            .unwrap_err(),
        libc::ELOOP
    );
    assert_eq!(fs::read(&secret).expect("read secret"), b"secret");
}

#[test]
fn passthrough_mutations_authorize_followed_symlink_targets() {
    let root = temp_dir();
    let secret = root.path().join("secret.txt");
    fs::write(&secret, b"secret").expect("seed secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod secret");
    std::os::unix::fs::symlink("secret.txt", root.path().join("link.txt")).expect("symlink secret");

    let passthrough = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .write("/link.txt", 0, b"x", Some(blocked_ctx()))
            .unwrap_err(),
        libc::EACCES
    );
    assert_eq!(fs::read(&secret).expect("read secret"), b"secret");

    assert_eq!(
        passthrough
            .create_file("/link.txt", 0o644, libc::O_TRUNC, Some(blocked_ctx()))
            .unwrap_err(),
        libc::EACCES
    );
    assert_eq!(fs::read(&secret).expect("read secret"), b"secret");
}

#[test]
fn passthrough_create_file_rejects_dangling_symlink_targets() {
    let root = temp_dir();
    let created = root.path().join("created.txt");
    std::os::unix::fs::symlink("created.txt", root.path().join("link.txt"))
        .expect("dangling symlink");

    let passthrough = passthrough_fs(root.path().to_path_buf(), FsOptions::default());
    assert_eq!(
        passthrough
            .create_file("/link.txt", 0o644, 0, Some(ctx()))
            .unwrap_err(),
        libc::ENOENT
    );
    assert!(
        !created.exists(),
        "create_file must not create through a dangling symlink"
    );
}

#[test]
fn overlay_create_file_rejects_dangling_symlink_targets() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let created = cow.path().join("created.txt");
    std::os::unix::fs::symlink("created.txt", cow.path().join("link.txt"))
        .expect("dangling symlink");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(
        overlay
            .create_file("/link.txt", 0o644, 0, Some(ctx()))
            .unwrap_err(),
        libc::ELOOP
    );
    assert!(
        !created.exists(),
        "create_file must not create through a dangling overlay symlink"
    );
}

fn assert_dangling_symlink_is_visible(fs: Arc<dyn ServerStorage + Send + Sync>) {
    fs.symlink("/dangling", b"missing-target".to_vec(), Some(ctx()))
        .expect("create dangling symlink");
    let stat = fs.stat("/dangling").expect("stat dangling symlink");
    assert_eq!(stat.mode & libc::S_IFMT, libc::S_IFLNK);
    assert_eq!(
        fs.readlink("/dangling").expect("readlink dangling symlink"),
        b"missing-target"
    );
    let entries = fs.list_dir("/").expect("list directory");
    let (_, entry_stat) = entries
        .iter()
        .find(|(name, _)| name == "dangling")
        .expect("dangling symlink appears in readdir");
    assert_eq!(entry_stat.mode & libc::S_IFMT, libc::S_IFLNK);
}

#[test]
fn dangling_symlinks_are_visible_in_backend_stat_readdir_and_readlink() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_dangling_symlink_is_visible(passthrough);

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_dangling_symlink_is_visible(overlay);
}

#[test]
fn service_symlink_returns_success_for_dangling_targets() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let dispatcher = Dispatcher::new(FabricFsFileSystemService::new(fs));

    let symlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-dangling-symlink",
            RequestPayload::Symlink(pb::SymlinkRequest {
                path: Some(path("/dangling").expect("valid path")),
                target: b"missing-target".to_vec(),
            }),
        ),
    );
    assert_success_invalidation(&symlink, Operation::Symlink, InvalidationKind::Create);
    match symlink.payload.as_ref().expect("symlink payload") {
        ResponsePayload::Symlink(value) => {
            let attr = value.attr.as_ref().expect("symlink attr");
            assert_eq!(attr.kind, pb::FileKind::Symlink as i32);
        }
        other => panic!("unexpected symlink payload: {other:?}"),
    }

    let readlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-dangling-readlink",
            RequestPayload::Readlink(pb::ReadlinkRequest {
                path: Some(path("/dangling").expect("valid path")),
            }),
        ),
    );
    match readlink.payload.expect("readlink payload") {
        ResponsePayload::Readlink(value) => assert_eq!(value.target, b"missing-target"),
        other => panic!("unexpected readlink payload: {other:?}"),
    }
}

#[test]
fn service_symlink_round_trips_non_utf8_targets() {
    let root = temp_dir();
    let dispatcher = common_dispatcher_for_passthrough(root.path().to_path_buf());

    let symlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-non-utf8-symlink",
            RequestPayload::Symlink(pb::SymlinkRequest {
                path: Some(path("/link.txt").expect("valid path")),
                target: b"target-\xff".to_vec(),
            }),
        ),
    );
    assert_success_invalidation(&symlink, Operation::Symlink, InvalidationKind::Create);

    let readlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-non-utf8-readlink",
            RequestPayload::Readlink(pb::ReadlinkRequest {
                path: Some(path("/link.txt").expect("valid path")),
            }),
        ),
    );
    match readlink.payload.expect("readlink payload") {
        ResponsePayload::Readlink(value) => assert_eq!(value.target, b"target-\xff"),
        other => panic!("unexpected readlink payload: {other:?}"),
    }
}

#[test]
fn hard_link_checks_destination_parent_permissions() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let source = backing.path().join("source.txt");
    fs::write(&source, b"source").expect("seed source");
    fs::set_permissions(&source, fs::Permissions::from_mode(0o644)).expect("chmod source");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = overlay
        .link("/source.txt", "/linked.txt", Some(blocked_ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);

    let passthrough_root = temp_dir();
    let passthrough_source = passthrough_root.path().join("source.txt");
    fs::write(&passthrough_source, b"source").expect("seed passthrough source");
    fs::set_permissions(&passthrough_source, fs::Permissions::from_mode(0o644))
        .expect("chmod passthrough source");
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    let err = passthrough
        .link("/source.txt", "/linked.txt", Some(blocked_ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
}

#[test]
fn hard_link_checks_source_parent_search_permissions() {
    let passthrough_root = temp_dir();
    make_world_searchable(passthrough_root.path());
    let passthrough_private = passthrough_root.path().join("private");
    let passthrough_public = passthrough_root.path().join("public");
    fs::create_dir(&passthrough_private).expect("create passthrough private dir");
    fs::create_dir(&passthrough_public).expect("create passthrough public dir");
    fs::write(passthrough_private.join("source.txt"), b"source").expect("seed passthrough source");
    make_world_writable(&passthrough_public);
    fs::set_permissions(&passthrough_private, fs::Permissions::from_mode(0o700))
        .expect("chmod passthrough private dir");

    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    let err = passthrough
        .link(
            "/private/source.txt",
            "/public/linked.txt",
            Some(blocked_ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
    assert_path_absent(&passthrough_public.join("linked.txt"));

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    make_world_searchable(backing.path());
    make_world_writable(alias.path());
    make_world_writable(cow.path());
    let overlay_private = backing.path().join("private");
    let overlay_public = backing.path().join("public");
    fs::create_dir(&overlay_private).expect("create overlay private dir");
    fs::create_dir(&overlay_public).expect("create overlay public dir");
    fs::write(overlay_private.join("source.txt"), b"source").expect("seed overlay source");
    make_world_writable(&overlay_public);
    fs::set_permissions(&overlay_private, fs::Permissions::from_mode(0o700))
        .expect("chmod overlay private dir");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = overlay
        .link(
            "/private/source.txt",
            "/public/linked.txt",
            Some(blocked_ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
    assert_path_absent(&cow.path().join("public/linked.txt"));
    assert_path_absent(&cow.path().join("private/source.txt"));
}

#[test]
fn hard_link_checks_merged_overlay_source_parent_search_permissions() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    make_world_searchable(backing.path());
    make_world_writable(alias.path());
    make_world_writable(cow.path());

    let backing_private = backing.path().join("private");
    let cow_private = cow.path().join("private");
    let backing_public = backing.path().join("public");
    fs::create_dir(&backing_private).expect("create backing private dir");
    fs::create_dir(&cow_private).expect("create cow private dir");
    fs::create_dir(&backing_public).expect("create backing public dir");
    fs::write(backing_private.join("source.txt"), b"source").expect("seed backing source");
    make_world_searchable(&backing_private);
    make_world_writable(&backing_public);
    fs::set_permissions(&cow_private, fs::Permissions::from_mode(0o700))
        .expect("chmod cow private dir");

    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = overlay
        .link(
            "/private/source.txt",
            "/public/linked.txt",
            Some(blocked_ctx()),
        )
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
    assert_path_absent(&cow.path().join("public/linked.txt"));
}

#[test]
fn hard_link_allows_unreadable_owned_sources() {
    let passthrough_root = temp_dir();
    let passthrough_source = passthrough_root.path().join("source.txt");
    fs::write(&passthrough_source, b"source").expect("seed passthrough source");
    fs::set_permissions(&passthrough_source, fs::Permissions::from_mode(0o000))
        .expect("chmod passthrough source");
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    passthrough
        .link("/source.txt", "/linked.txt", Some(ctx()))
        .expect("link unreadable passthrough source");
    assert_eq!(
        fs::metadata(&passthrough_source)
            .expect("passthrough source metadata")
            .nlink(),
        2
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let source = backing.path().join("source.txt");
    fs::write(&source, b"source").expect("seed source");
    fs::set_permissions(&source, fs::Permissions::from_mode(0o000)).expect("chmod source");
    let options = FsOptions {
        allow_direct_backing_updates: true,
        ..FsOptions::default()
    };
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        options,
    );
    overlay
        .link("/source.txt", "/linked.txt", Some(ctx()))
        .expect("link unreadable overlay source");
    assert_eq!(fs::metadata(&source).expect("source metadata").nlink(), 2);
}

#[test]
fn service_hardlink_rejects_inaccessible_source_parent() {
    let root = temp_dir();
    make_world_searchable(root.path());
    let private = root.path().join("private");
    let public = root.path().join("public");
    fs::create_dir(&private).expect("create private dir");
    fs::create_dir(&public).expect("create public dir");
    fs::write(private.join("source.txt"), b"source").expect("seed source");
    make_world_writable(&public);
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).expect("chmod private dir");

    let blocked = blocked_ctx();
    let request = common_request(
        "svc-hardlink-inaccessible-source-parent",
        RequestPayload::Hardlink(pb::HardlinkRequest {
            existing_path: Some(path("/private/source.txt").expect("valid path")),
            new_path: Some(path("/public/linked.txt").expect("valid path")),
        }),
    )
    .with_caller(pb::CallerContext {
        uid: blocked.uid,
        gid: blocked.gid,
        pid: blocked.pid,
    });
    let dispatcher = common_dispatcher_for_passthrough(root.path().to_path_buf());
    let response = dispatch_common(&dispatcher, request);

    assert_eq!(response.errno, Some(Errno::PermissionDenied));
    assert_path_absent(&public.join("linked.txt"));
}

fn seed_prefix_rename_files(root: &std::path::Path) {
    fs::write(root.join("foo"), b"renamed").expect("seed foo");
    fs::write(root.join("foobar"), b"sibling").expect("seed foobar");
}

fn assert_prefix_rename_preserves_sibling_handle(fs: &dyn ServerStorage) {
    let (fh, _) = fs
        .open("/foobar", HandleKind::File, libc::O_RDONLY, Some(ctx()))
        .expect("open sibling");
    fs.rename("/foo", "/bar", Some(ctx()))
        .expect("rename prefix path");

    fs.check_handle("/foobar", Some(fh), HandleKind::File)
        .expect("sibling handle keeps original path");
    assert_eq!(
        fs.check_handle("/barbar", Some(fh), HandleKind::File)
            .unwrap_err(),
        libc::EBADF
    );
    fs.release_fh(fh);
}

#[test]
fn rename_rewrites_only_exact_paths_and_descendants() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    seed_prefix_rename_files(backing.path());
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_prefix_rename_preserves_sibling_handle(overlay.as_ref());

    let passthrough_root = temp_dir();
    seed_prefix_rename_files(passthrough_root.path());
    let passthrough = passthrough_fs(passthrough_root.path().to_path_buf(), FsOptions::default());
    assert_prefix_rename_preserves_sibling_handle(&passthrough);
}

#[test]
fn hard_link_in_cow_copies_up_source() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();
    fs::write(backing.path().join("orig.txt"), b"shared").unwrap();

    {
        let fs = create_overlay_fs(
            Some(backing.path().to_path_buf()),
            Some(alias.path().to_path_buf()),
            Some(cow.path().to_path_buf()),
            FsOptions::default(),
        );
        fs.link("/orig.txt", "/linked.txt", Some(ctx()))
            .expect("link");
        assert_eq!(fs.read("/orig.txt", 0, 16, Some(ctx())).unwrap(), b"shared");
        assert_eq!(
            fs.read("/linked.txt", 0, 16, Some(ctx())).unwrap(),
            b"shared"
        );
    }

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(fs.read("/orig.txt", 0, 16, Some(ctx())).unwrap(), b"shared");
    assert_eq!(
        fs.read("/linked.txt", 0, 16, Some(ctx())).unwrap(),
        b"shared"
    );
    let backing_bytes = fs::read(backing.path().join("orig.txt")).unwrap();
    assert_eq!(backing_bytes, b"shared");
}

#[test]
fn tombstones_land_in_alias_path() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();

    let target = backing.path().join("gone.txt");
    fs::write(&target, b"backing").unwrap();

    {
        let fs = create_overlay_fs(
            Some(backing.path().to_path_buf()),
            Some(alias.path().to_path_buf()),
            Some(cow.path().to_path_buf()),
            FsOptions::default(),
        );
        fs.unlink("/gone.txt", Some(ctx()))
            .expect("unlink through cow");
        let tombstone = alias.path().join(".fabricfs_tombstones").join("gone.txt");
        assert!(
            tombstone.exists(),
            "tombstone should be persisted under alias_path"
        );
    }

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_eq!(
        fs.read("/gone.txt", 0, 4, Some(ctx())).unwrap_err(),
        libc::ENOENT,
        "tombstone should still hide backing entry"
    );
    assert!(
        target.exists(),
        "backing entry should stay untouched when using COW"
    );
}

#[test]
fn child_tombstone_does_not_hide_parent_directory() {
    let backing = temp_dir();
    let cow = temp_dir();
    let alias = temp_dir();

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );

    fs.mkdir("/dir", 0o755, Some(ctx())).expect("mkdir");
    fs.create_file("/dir/nested.txt", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create nested file");
    fs.unlink("/dir/nested.txt", Some(ctx()))
        .expect("unlink nested file");

    fs.rmdir("/dir", Some(ctx()))
        .expect("rmdir after child tombstone");
}

#[test]
fn cow_layer_is_read_only_without_alias_path() {
    let backing = temp_dir();
    let cow = temp_dir();
    fs::write(backing.path().join("locked.txt"), b"backing").unwrap();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        None,
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let err = fs
        .create_file("/locked.txt", 0o644, 0, Some(ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EROFS);
}

#[test]
fn read_only_without_alias_path_rejects_mutations() {
    let backing = temp_dir();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        None,
        None,
        FsOptions::default(),
    );
    let err = fs.create_file("/file", 0o644, 0, Some(ctx())).unwrap_err();
    assert_eq!(err, libc::EROFS);
}

#[test]
fn passthrough_writes_denied_without_backingtree_flag() {
    let backing = temp_dir();
    let alias = temp_dir();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        FsOptions::default(),
    );

    let err = fs
        .create_file("/data.bin", 0o644, 0, Some(ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
}

#[test]
fn handles_survive_basic_open_and_write_paths() {
    let backing = temp_dir();
    let alias = temp_dir();
    let opts = FsOptions {
        allow_direct_backing_updates: true,
        ..FsOptions::default()
    };
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        opts,
    );
    let (fh, _) = fs
        .create_file("/tracked", 0o644, 0, Some(ctx()))
        .expect("create");
    fs.check_handle("/tracked", Some(fh), HandleKind::File)
        .expect("handle ok");
    fs.write("/tracked", 0, b"bytes", Some(ctx()))
        .expect("write");
    fs.release_fh(fh);
}

#[test]
fn flush_and_fsync_sync_open_file_handles_not_paths() {
    let passthrough_root = temp_dir();
    let passthrough: Arc<dyn ServerStorage + Send + Sync> = Arc::new(passthrough_fs(
        passthrough_root.path().to_path_buf(),
        FsOptions::default(),
    ));
    assert_flush_and_fsync_use_open_handle_after_unlink(
        passthrough,
        &passthrough_root.path().join("tracked"),
    );

    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let overlay = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    assert_flush_and_fsync_use_open_handle_after_unlink(overlay, &cow.path().join("tracked"));
}

#[test]
fn overlay_opendir_and_list_dir_follow_directory_symlink_consistently() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let real_dir = backing.path().join("real");
    fs::create_dir(&real_dir).expect("create backing directory");
    fs::write(real_dir.join("child.txt"), b"child").expect("seed child");
    std::os::unix::fs::symlink("real", backing.path().join("linkdir")).expect("symlink directory");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let (fh, _) = fs
        .open("/linkdir", HandleKind::Dir, libc::O_RDONLY, Some(ctx()))
        .expect("opendir follows directory symlink");
    let entries = fs
        .list_dir("/linkdir")
        .expect("readdir follows same symlink");
    assert!(entries.iter().any(|(name, _)| name == "child.txt"));
    fs.release_fh(fh);
}

#[test]
fn overlay_mutations_authorize_directory_symlink_target() {
    let backing = temp_dir();
    let alias = temp_dir();
    let cow = temp_dir();
    let real_dir = backing.path().join("real");
    fs::create_dir(&real_dir).expect("create backing directory");
    fs::set_permissions(&real_dir, fs::Permissions::from_mode(0o500))
        .expect("chmod backing target");
    std::os::unix::fs::symlink("real", backing.path().join("linkdir")).expect("symlink directory");

    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        Some(cow.path().to_path_buf()),
        FsOptions::default(),
    );
    let request_ctx = if unsafe { libc::geteuid() } == 0 {
        FuseContext {
            uid: 12345,
            gid: 12345,
            pid: 0,
        }
    } else {
        ctx()
    };
    let err = fs
        .create_file("/linkdir/shadow.txt", 0o644, 0, Some(request_ctx))
        .unwrap_err();

    assert_eq!(err, libc::EACCES);
    assert!(!cow.path().join("linkdir").join("shadow.txt").exists());

    fs::set_permissions(&real_dir, fs::Permissions::from_mode(0o700))
        .expect("allow process traversal");
    fs::write(real_dir.join("existing.txt"), b"existing").expect("seed symlink target file");
    fs::set_permissions(
        real_dir.join("existing.txt"),
        fs::Permissions::from_mode(0o666),
    )
    .expect("make file writable without parent search");

    let err = fs
        .create_file(
            "/linkdir/existing.txt",
            0o644,
            libc::O_WRONLY | libc::O_TRUNC,
            Some(blocked_ctx()),
        )
        .unwrap_err();

    assert_eq!(err, libc::EACCES);
    assert!(!cow.path().join("linkdir").join("existing.txt").exists());
    fs::set_permissions(&real_dir, fs::Permissions::from_mode(0o700)).expect("restore permissions");
}

#[test]
fn handles_reject_without_backingtree_flag() {
    let backing = temp_dir();
    let alias = temp_dir();
    let fs = create_overlay_fs(
        Some(backing.path().to_path_buf()),
        Some(alias.path().to_path_buf()),
        None,
        FsOptions::default(),
    );
    let err = fs
        .create_file("/tracked", 0o644, 0, Some(ctx()))
        .unwrap_err();
    assert_eq!(err, libc::EACCES);
}

#[test]
fn service_rejects_unsupported_advanced_io_modes_before_backend_mutation() {
    let root = temp_dir();
    let service = FabricFsFileSystemService::new(Arc::new(passthrough_fs(
        root.path().to_path_buf(),
        FsOptions::default(),
    )));
    let metadata = rpc_metadata();

    let copy_err = service
        .copy_file_range(
            &pb::CopyFileRangeRequest {
                input_path: Some(path("/src.txt").expect("valid path")),
                input_handle: 1,
                input_offset: 0,
                output_path: Some(path("/dst.txt").expect("valid path")),
                output_handle: 2,
                output_offset: 0,
                length: 1,
                flags: 1,
            },
            &metadata,
        )
        .expect_err("nonzero copy_file_range flags are unsupported");
    assert_eq!(copy_err.errno, Errno::NotSupported);

    let fallocate_err = service
        .fallocate(
            &pb::FallocateRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: 1,
                offset: 0,
                length: 1,
                mode: 1,
            },
            &metadata,
        )
        .expect_err("nonzero fallocate modes are unsupported");
    assert_eq!(fallocate_err.errno, Errno::NotSupported);
}

#[test]
fn service_readlink_requires_search_permission_on_parent_directories() {
    let root = temp_dir();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755))
        .expect("make root searchable by non-owner");
    let private = root.path().join("private");
    fs::create_dir(&private).expect("create private dir");
    std::os::unix::fs::symlink("target", private.join("link")).expect("create symlink");
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700))
        .expect("make private dir owner-only");

    let service = FabricFsFileSystemService::new(Arc::new(passthrough_fs(
        root.path().to_path_buf(),
        FsOptions::default(),
    )));
    let request = RequestEnvelope::new(
        "svc-readlink-permission",
        "ops-test",
        0,
        pb::TraceContext::default(),
        RequestPayload::Readlink(pb::ReadlinkRequest {
            path: Some(path("/private/link").expect("valid path")),
        }),
    )
    .expect("request is valid")
    .with_caller(pb::CallerContext {
        uid: blocked_ctx().uid,
        gid: blocked_ctx().gid,
        pid: 0,
    });
    let metadata = RpcMetadata::for_request(&request, 0);

    let error = service
        .readlink(
            match &request.payload {
                RequestPayload::Readlink(value) => value,
                _ => unreachable!("request payload is readlink"),
            },
            &metadata,
        )
        .expect_err("readlink must not reveal targets below unsearchable directories");
    assert_eq!(error.errno, Errno::PermissionDenied);
}

#[test]
fn service_maps_new_posix_operations_to_passthrough_backend() {
    let root = temp_dir();
    fs::write(root.path().join("file.txt"), b"hello").expect("seed source");
    let dispatcher = common_dispatcher_for_passthrough(root.path().to_path_buf());

    let symlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-symlink",
            RequestPayload::Symlink(pb::SymlinkRequest {
                path: Some(path("/link.txt").expect("valid path")),
                target: b"file.txt".to_vec(),
            }),
        ),
    );
    assert_success_invalidation(&symlink, Operation::Symlink, InvalidationKind::Create);

    let readlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-readlink",
            RequestPayload::Readlink(pb::ReadlinkRequest {
                path: Some(path("/link.txt").expect("valid path")),
            }),
        ),
    );
    match readlink.payload.expect("readlink payload") {
        ResponsePayload::Readlink(value) => assert_eq!(value.target, b"file.txt"),
        other => panic!("unexpected readlink payload: {other:?}"),
    }

    let hardlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-hardlink",
            RequestPayload::Hardlink(pb::HardlinkRequest {
                existing_path: Some(path("/file.txt").expect("valid path")),
                new_path: Some(path("/hard.txt").expect("valid path")),
            }),
        ),
    );
    assert_success_invalidation(&hardlink, Operation::Hardlink, InvalidationKind::Create);
    let hardlink_attr = match hardlink.payload.as_ref().expect("hardlink payload") {
        ResponsePayload::Hardlink(value) => value.attr.as_ref().expect("hardlink attr").clone(),
        other => panic!("unexpected hardlink payload: {other:?}"),
    };
    assert_eq!(
        fs::metadata(root.path().join("file.txt"))
            .expect("source metadata")
            .nlink(),
        2
    );
    let source_getattr = dispatch_common(
        &dispatcher,
        common_request(
            "svc-hardlink-source-getattr",
            RequestPayload::Getattr(pb::GetattrRequest {
                path: Some(path("/file.txt").expect("valid path")),
            }),
        ),
    );
    let source_attr = match source_getattr.payload.as_ref().expect("getattr payload") {
        ResponsePayload::Getattr(value) => value.attr.as_ref().expect("source attr"),
        other => panic!("unexpected getattr payload: {other:?}"),
    };
    assert_eq!(source_attr.inode, hardlink_attr.inode);
    assert_eq!(source_attr.nlink, 2);
    assert_eq!(hardlink_attr.nlink, 2);

    let root_readdir = dispatch_common(
        &dispatcher,
        common_request(
            "svc-hardlink-readdir",
            RequestPayload::Readdir(pb::ReaddirRequest {
                path: Some(path("/").expect("valid path")),
                offset: 0,
                max_entries: 16,
            }),
        ),
    );
    let entries = match root_readdir.payload.as_ref().expect("readdir payload") {
        ResponsePayload::Readdir(value) => &value.entries,
        other => panic!("unexpected readdir payload: {other:?}"),
    };
    let file_inode = entries
        .iter()
        .find(|entry| entry.name == "file.txt")
        .expect("source appears in readdir")
        .inode;
    let hard_inode = entries
        .iter()
        .find(|entry| entry.name == "hard.txt")
        .expect("hardlink appears in readdir")
        .inode;
    assert_eq!(file_inode, hard_inode);

    let setattr = dispatch_common(
        &dispatcher,
        common_request(
            "svc-setattr",
            RequestPayload::Setattr(pb::SetattrRequest {
                path: Some(path("/file.txt").expect("valid path")),
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: Some(3),
                handle: None,
            }),
        ),
    );
    assert_success_invalidation(&setattr, Operation::Setattr, InvalidationKind::Metadata);
    match setattr.payload.as_ref().expect("setattr payload") {
        ResponsePayload::Setattr(value) => assert_eq!(value.attr.as_ref().expect("attr").size, 3),
        other => panic!("unexpected setattr payload: {other:?}"),
    }

    let source_handle = open_common(&dispatcher, "/file.txt", libc::O_RDWR, "svc-open-source");
    let copy_handle = create_common(&dispatcher, "/copy.txt", "svc-create-copy");

    let flush = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flush",
            RequestPayload::Flush(pb::FlushRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                lock_owner: 7,
            }),
        ),
    );
    assert_success_without_invalidation(&flush, Operation::Flush);

    let fsync = dispatch_common(
        &dispatcher,
        common_request(
            "svc-fsync",
            RequestPayload::Fsync(pb::FsyncRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                datasync: true,
            }),
        ),
    );
    assert_success_without_invalidation(&fsync, Operation::Fsync);

    let dir_handle = opendir_common(&dispatcher, "/", libc::O_RDONLY, "svc-opendir-root");
    let fsyncdir = dispatch_common(
        &dispatcher,
        common_request(
            "svc-fsyncdir",
            RequestPayload::Fsyncdir(pb::FsyncdirRequest {
                path: Some(path("/").expect("valid path")),
                handle: dir_handle,
                datasync: false,
            }),
        ),
    );
    assert_success_without_invalidation(&fsyncdir, Operation::Fsyncdir);

    let setlk = dispatch_common(
        &dispatcher,
        common_request(
            "svc-setlk",
            RequestPayload::Setlk(pb::SetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                owner: 10,
                start: 0,
                end: 10,
                typ: libc::F_WRLCK,
                pid: 123,
                wait: false,
            }),
        ),
    );
    assert_success_without_invalidation(&setlk, Operation::Setlk);

    let getlk = dispatch_common(
        &dispatcher,
        common_request(
            "svc-getlk",
            RequestPayload::Getlk(pb::GetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                owner: 11,
                start: 0,
                end: 10,
                typ: libc::F_WRLCK,
                pid: 999,
            }),
        ),
    );
    match getlk.payload.expect("getlk payload") {
        ResponsePayload::Getlk(value) => {
            assert_eq!(value.lock.expect("conflicting lock").pid, 123)
        }
        other => panic!("unexpected getlk payload: {other:?}"),
    }

    let blocking_setlk = dispatch_common(
        &dispatcher,
        common_request(
            "svc-blocking-setlk",
            RequestPayload::Setlk(pb::SetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                owner: 10,
                start: 0,
                end: 10,
                typ: libc::F_WRLCK,
                pid: 123,
                wait: true,
            }),
        ),
    );
    assert_eq!(blocking_setlk.errno, Some(Errno::NotSupported));
    assert!(blocking_setlk.payload.is_none());

    let blocking_unlock = dispatch_common(
        &dispatcher,
        common_request(
            "svc-blocking-unlock",
            RequestPayload::Setlk(pb::SetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                owner: 10,
                start: 0,
                end: 10,
                typ: libc::F_UNLCK,
                pid: 123,
                wait: true,
            }),
        ),
    );
    assert_success_without_invalidation(&blocking_unlock, Operation::Setlk);

    let flock = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flock",
            RequestPayload::Flock(pb::FlockRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: source_handle,
                owner: 12,
                operation: LOCK_EXCLUSIVE,
            }),
        ),
    );
    assert_success_without_invalidation(&flock, Operation::Flock);

    let flock_contender = open_common(
        &dispatcher,
        "/file.txt",
        libc::O_RDONLY,
        "svc-flock-contender",
    );
    let blocking_flock_conflict = dispatch_common(
        &dispatcher,
        common_request(
            "svc-blocking-flock-conflict",
            RequestPayload::Flock(pb::FlockRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: flock_contender,
                owner: 13,
                operation: LOCK_SHARED,
            }),
        ),
    );
    assert_eq!(blocking_flock_conflict.errno, Some(Errno::NotSupported));
    assert!(blocking_flock_conflict.payload.is_none());

    let nonblocking_flock_conflict = dispatch_common(
        &dispatcher,
        common_request(
            "svc-nonblocking-flock-conflict",
            RequestPayload::Flock(pb::FlockRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: flock_contender,
                owner: 13,
                operation: LOCK_SHARED | LOCK_NONBLOCK,
            }),
        ),
    );
    assert_eq!(nonblocking_flock_conflict.errno, Some(Errno::WouldBlock));
    assert!(nonblocking_flock_conflict.payload.is_none());

    let copy = dispatch_common(
        &dispatcher,
        common_request(
            "svc-copy-file-range",
            RequestPayload::CopyFileRange(pb::CopyFileRangeRequest {
                input_path: Some(path("/file.txt").expect("valid path")),
                input_handle: source_handle,
                input_offset: 0,
                output_path: Some(path("/copy.txt").expect("valid path")),
                output_handle: copy_handle,
                output_offset: 0,
                length: 3,
                flags: 0,
            }),
        ),
    );
    assert_success_invalidation(&copy, Operation::CopyFileRange, InvalidationKind::Modify);
    match copy.payload.as_ref().expect("copy payload") {
        ResponsePayload::CopyFileRange(value) => assert_eq!(value.bytes_copied, 3),
        other => panic!("unexpected copy payload: {other:?}"),
    }

    let fallocate = dispatch_common(
        &dispatcher,
        common_request(
            "svc-fallocate",
            RequestPayload::Fallocate(pb::FallocateRequest {
                path: Some(path("/copy.txt").expect("valid path")),
                handle: copy_handle,
                offset: 0,
                length: 4096,
                mode: 0,
            }),
        ),
    );
    assert_success_invalidation(&fallocate, Operation::Fallocate, InvalidationKind::Modify);

    let lseek = dispatch_common(
        &dispatcher,
        common_request(
            "svc-lseek",
            RequestPayload::Lseek(pb::LseekRequest {
                path: Some(path("/copy.txt").expect("valid path")),
                handle: copy_handle,
                offset: 0,
                whence: SEEK_SET,
            }),
        ),
    );
    match lseek.payload.expect("lseek payload") {
        ResponsePayload::Lseek(value) => assert_eq!(value.offset, 0),
        other => panic!("unexpected lseek payload: {other:?}"),
    }

    let seek_data = dispatch_common(
        &dispatcher,
        common_request(
            "svc-lseek-seek-data",
            RequestPayload::Lseek(pb::LseekRequest {
                path: Some(path("/copy.txt").expect("valid path")),
                handle: copy_handle,
                offset: 0,
                whence: SEEK_DATA,
            }),
        ),
    );
    assert_eq!(seek_data.errno, Some(Errno::NotSupported));
    assert!(seek_data.payload.is_none());
}

#[test]
fn service_flush_releases_posix_locks_for_lock_owner() {
    let root = temp_dir();
    fs::write(root.path().join("file.txt"), b"hello").expect("seed source");
    let dispatcher = common_dispatcher_for_passthrough(root.path().to_path_buf());

    let first = open_common(
        &dispatcher,
        "/file.txt",
        libc::O_RDWR,
        "svc-flush-open-first",
    );
    let second = open_common(
        &dispatcher,
        "/file.txt",
        libc::O_RDWR,
        "svc-flush-open-second",
    );

    let setlk = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flush-setlk",
            RequestPayload::Setlk(pb::SetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: first,
                owner: 10,
                start: 0,
                end: 10,
                typ: libc::F_WRLCK,
                pid: 123,
                wait: false,
            }),
        ),
    );
    assert_success_without_invalidation(&setlk, Operation::Setlk);

    let flush = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flush-release-owner",
            RequestPayload::Flush(pb::FlushRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: first,
                lock_owner: 10,
            }),
        ),
    );
    assert_success_without_invalidation(&flush, Operation::Flush);

    let setlk_after_flush = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flush-setlk-after",
            RequestPayload::Setlk(pb::SetlkRequest {
                path: Some(path("/file.txt").expect("valid path")),
                handle: second,
                owner: 11,
                start: 0,
                end: 10,
                typ: libc::F_WRLCK,
                pid: 456,
                wait: false,
            }),
        ),
    );
    assert_success_without_invalidation(&setlk_after_flush, Operation::Setlk);
}

#[test]
fn service_routes_setattr_and_flock_through_handle_semantics() {
    let root = temp_dir();
    let dispatcher = common_dispatcher_for_passthrough(root.path().to_path_buf());

    let handle = create_common(&dispatcher, "/open.txt", "svc-handle-create");
    let unlink = dispatch_common(
        &dispatcher,
        common_request(
            "svc-handle-unlink",
            RequestPayload::Unlink(pb::UnlinkRequest {
                path: Some(path("/open.txt").expect("valid path")),
            }),
        ),
    );
    assert_success_invalidation(&unlink, Operation::Unlink, InvalidationKind::Delete);

    let setattr = dispatch_common(
        &dispatcher,
        common_request(
            "svc-handle-setattr",
            RequestPayload::Setattr(pb::SetattrRequest {
                path: Some(path("/open.txt").expect("valid path")),
                mode: None,
                uid: None,
                gid: None,
                size: Some(0),
                handle: Some(handle),
            }),
        ),
    );
    assert_success_invalidation(&setattr, Operation::Setattr, InvalidationKind::Metadata);
    match setattr.payload.as_ref().expect("setattr payload") {
        ResponsePayload::Setattr(value) => assert_eq!(value.attr.as_ref().expect("attr").size, 0),
        other => panic!("unexpected setattr payload: {other:?}"),
    }

    let read_only = create_common(&dispatcher, "/flock.txt", "svc-flock-create");
    let release_created = dispatch_common(
        &dispatcher,
        common_request(
            "svc-flock-release-created",
            RequestPayload::Release(pb::ReleaseRequest {
                path: Some(path("/flock.txt").expect("valid path")),
                handle: read_only,
                flags: 0,
            }),
        ),
    );
    assert_success_without_invalidation(&release_created, Operation::Release);
    let read_only = open_common(
        &dispatcher,
        "/flock.txt",
        libc::O_RDONLY,
        "svc-flock-read-only",
    );
    let flock = dispatch_common(
        &dispatcher,
        common_request(
            "svc-read-only-flock",
            RequestPayload::Flock(pb::FlockRequest {
                path: Some(path("/flock.txt").expect("valid path")),
                handle: read_only,
                owner: 77,
                operation: LOCK_EXCLUSIVE,
            }),
        ),
    );
    assert_success_without_invalidation(&flock, Operation::Flock);
}

fn common_dispatcher_for_passthrough(
    root: std::path::PathBuf,
) -> Dispatcher<FabricFsFileSystemService> {
    let fs = Arc::new(passthrough_fs(root, FsOptions::default()));
    Dispatcher::new(FabricFsFileSystemService::new(fs))
}

fn common_request(id: &str, payload: RequestPayload) -> RequestEnvelope {
    RequestEnvelope::new(id, "ops-test", 0, pb::TraceContext::default(), payload)
        .expect("request is valid")
        .with_caller(pb::CallerContext {
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            pid: std::process::id(),
        })
}

fn dispatch_common(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    request: RequestEnvelope,
) -> ResponseEnvelope {
    dispatcher.dispatch(request.clone(), RpcMetadata::for_request(&request, 0))
}

fn open_common(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    path_value: &str,
    flags: i32,
    request_id: &str,
) -> u64 {
    open_common_with_kind(
        dispatcher,
        path_value,
        flags,
        request_id,
        pb::OpenKind::File,
    )
}

fn opendir_common(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    path_value: &str,
    flags: i32,
    request_id: &str,
) -> u64 {
    open_common_with_kind(
        dispatcher,
        path_value,
        flags,
        request_id,
        pb::OpenKind::Directory,
    )
}

fn open_common_with_kind(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    path_value: &str,
    flags: i32,
    request_id: &str,
    kind: pb::OpenKind,
) -> u64 {
    let response = dispatch_common(
        dispatcher,
        common_request(
            request_id,
            RequestPayload::Open(pb::OpenRequest {
                path: Some(path(path_value).expect("valid path")),
                flags: flags as u32,
                kind: kind as i32,
            }),
        ),
    );
    match response.payload.expect("open payload") {
        ResponsePayload::Open(value) => value.handle,
        other => panic!("unexpected open payload: {other:?}"),
    }
}

fn create_common(
    dispatcher: &Dispatcher<FabricFsFileSystemService>,
    path_value: &str,
    request_id: &str,
) -> u64 {
    let response = dispatch_common(
        dispatcher,
        common_request(
            request_id,
            RequestPayload::Create(pb::CreateRequest {
                path: Some(path(path_value).expect("valid path")),
                flags: libc::O_RDWR as u32,
                mode: 0o644,
            }),
        ),
    );
    match response.payload.expect("create payload") {
        ResponsePayload::Create(value) => value.handle,
        other => panic!("unexpected create payload: {other:?}"),
    }
}

fn assert_success_invalidation(
    response: &ResponseEnvelope,
    operation: Operation,
    kind: InvalidationKind,
) {
    assert!(response.ok, "{operation:?} failed: {response:?}");
    assert_eq!(response.operation, operation);
    assert_eq!(response.invalidations.len(), 1);
    assert_eq!(response.invalidations[0].kind, kind.wire_value());
    assert_eq!(response.invalidations[0].request_id, response.request_id);
}

fn assert_success_without_invalidation(response: &ResponseEnvelope, operation: Operation) {
    assert!(response.ok, "{operation:?} failed: {response:?}");
    assert_eq!(response.operation, operation);
    assert!(response.invalidations.is_empty());
}
