use fs_core::{RpcClient, RpcError};
use fs_fuse::{FuseAdapter, FuseError, FuseSetlk};
use fs_protocol::{
    directory_attr, file_attr, pb, Errno, InvalidationKind, Operation, RequestEnvelope,
    RequestPayload, ResponseEnvelope, ResponsePayload, LOCK_EXCLUSIVE, PROTOCOL_VERSION, SEEK_SET,
};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

#[test]
fn fuse_adapter_maps_supported_callbacks_to_rpc_operations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    assert_eq!(adapter.lookup(1, "file.txt").expect("lookup").attr.inode, 2);
    assert_eq!(adapter.getattr(2).expect("getattr").attr.inode, 2);
    assert_eq!(adapter.readdir(1, 0, 16).expect("readdir").len(), 1);
    assert_eq!(adapter.open(2, 0).expect("open").handle, 7);
    assert_eq!(adapter.read(2, 7, 0, 5).expect("read"), b"hello");
    assert_eq!(
        adapter
            .write(2, 7, 0, b"hello".to_vec())
            .expect("write")
            .bytes_written,
        5
    );
    assert_eq!(
        adapter
            .create(1, "created.txt", 0, 0o644)
            .expect("create")
            .0
            .attr
            .inode,
        3
    );
    assert_eq!(adapter.mkdir(1, "dir", 0o755).expect("mkdir").attr.inode, 4);
    assert_eq!(adapter.statfs(1).expect("statfs").stat.block_size, 4096);
    assert_eq!(
        adapter.getxattr(2, "user.key", 64).expect("getxattr"),
        b"value"
    );
    adapter
        .setxattr(2, "user.key", b"value".to_vec(), 0)
        .expect("setxattr");
    assert_eq!(
        adapter.listxattr(2, 128).expect("listxattr"),
        vec!["user.key"]
    );
    adapter.removexattr(2, "user.key").expect("removexattr");
    assert_eq!(
        adapter
            .symlink(1, "link.txt", b"file.txt")
            .expect("symlink")
            .attr
            .inode,
        5
    );
    assert_eq!(adapter.readlink(5).expect("readlink"), b"file.txt");
    assert_eq!(
        adapter
            .hardlink(2, 1, "hard.txt")
            .expect("hardlink")
            .attr
            .inode,
        2
    );
    assert_eq!(
        adapter
            .setattr(2, None, Some(0o600), None, None, Some(16))
            .expect("setattr")
            .attr
            .size,
        16
    );
    adapter.flush(2, 7, 99).expect("flush");
    adapter.fsync(2, 7, true).expect("fsync");
    let dir_handle = adapter.opendir(1, 0).expect("opendir").handle;
    adapter.fsyncdir(1, dir_handle, false).expect("fsyncdir");
    assert_eq!(
        adapter.getlk(2, 7, 123, 0, u64::MAX, 1, 42).expect("getlk"),
        None
    );
    adapter
        .setlk(
            2,
            FuseSetlk {
                handle: 7,
                owner: 123,
                start: 0,
                end: u64::MAX,
                typ: 1,
                pid: 42,
                wait: false,
            },
        )
        .expect("setlk");
    adapter.flock(2, 7, 123, LOCK_EXCLUSIVE).expect("flock");
    assert_eq!(
        adapter
            .copy_file_range(2, 7, 0, 3, 8, 0, 5, 0)
            .expect("copy_file_range"),
        5
    );
    adapter.fallocate(2, 7, 0, 4096, 0).expect("fallocate");
    assert_eq!(adapter.lseek(2, 7, 0, SEEK_SET).expect("lseek"), 5);
    adapter.release(2, 7, 0).expect("release");
    adapter
        .rename(1, "created.txt", 1, "renamed.txt")
        .expect("rename");
    assert_eq!(adapter.cached_path(3), Some("/renamed.txt".into()));
    adapter.unlink(1, "renamed.txt").expect("unlink");
    assert_eq!(adapter.cached_path(3), None);
    adapter.rmdir(1, "dir").expect("rmdir");
    assert_eq!(adapter.cached_path(4), None);
    adapter.forget(2, 1);
    assert_eq!(adapter.cached_path(2), Some("/file.txt".into()));
    adapter.forget(2, 1);
    assert_eq!(adapter.cached_path(2), None);

    assert_eq!(
        client.operations(),
        vec![
            Operation::Lookup,
            Operation::Getattr,
            Operation::Readdir,
            Operation::Open,
            Operation::Read,
            Operation::Write,
            Operation::Create,
            Operation::Mkdir,
            Operation::Statfs,
            Operation::Getxattr,
            Operation::Setxattr,
            Operation::Listxattr,
            Operation::Removexattr,
            Operation::Symlink,
            Operation::Readlink,
            Operation::Hardlink,
            Operation::Setattr,
            Operation::Flush,
            Operation::Fsync,
            Operation::Open,
            Operation::Fsyncdir,
            Operation::Getlk,
            Operation::Setlk,
            Operation::Flock,
            Operation::CopyFileRange,
            Operation::Fallocate,
            Operation::Lseek,
            Operation::Release,
            Operation::Rename,
            Operation::Unlink,
            Operation::Rmdir,
        ]
    );
}

#[test]
fn fuse_adapter_sends_explicit_file_and_directory_open_kinds() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("cache file path");
    adapter.open(2, 0).expect("open file");
    adapter.opendir(1, 0).expect("opendir root");

    let open_kinds = client
        .requests()
        .into_iter()
        .filter_map(|request| match request.payload {
            RequestPayload::Open(value) => Some(value.kind),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        open_kinds,
        vec![pb::OpenKind::File as i32, pb::OpenKind::Directory as i32]
    );
}

#[test]
fn fuse_adapter_retains_hardlink_aliases_after_unlinking_one_path() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup source");
    assert_eq!(adapter.cached_path(2), Some("/file.txt".into()));

    let hardlink = adapter
        .hardlink(2, 1, "hard.txt")
        .expect("create hardlink alias");
    assert_eq!(hardlink.attr.inode, 2);
    assert_eq!(adapter.cached_path(2), Some("/file.txt".into()));

    adapter.unlink(1, "hard.txt").expect("unlink alias");
    assert_eq!(
        adapter.cached_path(2),
        Some("/file.txt".into()),
        "removing one hardlink alias must not evict the still-cached source path"
    );

    adapter
        .getattr(2)
        .expect("source path remains usable after alias unlink");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("getattr request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/file.txt".into())
    );
}

#[test]
fn fuse_adapter_routes_setattr_through_open_handle_after_unlink() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup source");
    let handle = adapter.open(2, 0).expect("open source").handle;
    adapter.unlink(1, "file.txt").expect("unlink source path");
    assert_eq!(adapter.cached_path(2), None);

    adapter
        .setattr(2, Some(handle), None, None, None, Some(0))
        .expect("ftruncate through open unlinked handle");

    let requests = client.requests();
    let RequestPayload::Setattr(request) = &requests.last().expect("setattr request").payload
    else {
        panic!("expected setattr request");
    };
    assert_eq!(
        request.path.as_ref().map(|path| path.path.as_str()),
        Some("/file.txt")
    );
    assert_eq!(request.handle, Some(handle));
}

#[test]
fn fuse_adapter_routes_fsyncdir_through_open_handle_after_rmdir() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.mkdir(1, "dir", 0o755).expect("mkdir");
    let handle = adapter.opendir(4, 0).expect("opendir").handle;
    adapter.rmdir(1, "dir").expect("rmdir");
    assert_eq!(adapter.cached_path(4), None);

    adapter
        .fsyncdir(4, handle, false)
        .expect("fsyncdir through retained directory handle");

    let requests = client.requests();
    let RequestPayload::Fsyncdir(request) = &requests.last().expect("fsyncdir request").payload
    else {
        panic!("expected fsyncdir request");
    };
    assert_eq!(
        request.path.as_ref().map(|path| path.path.as_str()),
        Some("/dir")
    );
    assert_eq!(request.handle, handle);
}

#[test]
fn fuse_adapter_rejects_unsupported_copy_file_range_flags_before_rpc() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    let error = adapter
        .copy_file_range(2, 7, 0, 3, 8, 0, 5, 1)
        .expect_err("nonzero copy_file_range flags are unsupported");

    assert_eq!(error.errno(), Errno::NotSupported);
    assert!(client.operations().is_empty());
}

#[test]
fn fuse_adapter_bounds_copy_file_range_for_reply_write_before_rpc() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup source");
    adapter.open(2, 0).expect("open source");
    adapter
        .create(1, "created.txt", 0, 0o644)
        .expect("create destination");

    assert_eq!(
        adapter
            .copy_file_range_for_reply_write(2, 7, 0, 3, 8, 0, u64::MAX, 0)
            .expect("mounted copy_file_range clamps oversized reply-write length")
            .bytes_written,
        5
    );

    let requests = client.requests();
    let RequestPayload::CopyFileRange(request) = &requests
        .last()
        .expect("copy_file_range request should be emitted")
        .payload
    else {
        panic!("expected copy_file_range request");
    };
    assert_eq!(request.length, u64::from(u32::MAX));
}

#[test]
fn fuse_adapter_forwards_readdir_callback_offset() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    let entries = adapter
        .readdir(1, 42, 16)
        .expect("readdir with callback offset");

    assert_eq!(entries.len(), 1);
    let requests = client.requests();
    let RequestPayload::Readdir(request) = &requests[0].payload else {
        panic!("expected readdir request, got {:?}", requests[0].payload);
    };
    assert_eq!(request.offset, 42);
    assert_eq!(request.max_entries, 16);
}

#[test]
fn fuse_adapter_create_does_not_echo_posix_request_flags_as_fuse_reply_flags() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    const O_WRONLY: u32 = 1;
    const O_CREAT: u32 = 64;
    const O_EXCL: u32 = 128;
    let posix_flags = O_WRONLY | O_CREAT | O_EXCL;

    let (_, handle) = adapter
        .create(1, "created.txt", posix_flags, 0o644)
        .expect("create");

    assert_eq!(handle.flags, 0);
}

#[test]
fn fuse_adapter_maps_errno_and_transport_errors_to_fuse_errors() {
    let client = FakeClient::default();
    client.set_response_errno(Errno::PermissionDenied);
    let adapter = FuseAdapter::new(client, "fuse-ns");
    let error = adapter
        .lookup(1, "file.txt")
        .expect_err("errno response fails");
    assert_eq!(error.errno(), Errno::PermissionDenied);

    let client = FakeClient::default();
    client.set_transport_error(RpcError::ConnectionClosed);
    let adapter = FuseAdapter::new(client, "fuse-ns");
    let error = adapter
        .lookup(1, "file.txt")
        .expect_err("transport failure fails");
    assert_eq!(error.errno(), Errno::ConnectionReset);
    assert!(matches!(error, FuseError::Transport { .. }));
}

#[test]
fn fuse_adapter_keeps_open_handles_after_path_invalidation_and_forget() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open file").handle;
    adapter.unlink(1, "file.txt").expect("unlink open file");
    assert_eq!(adapter.cached_path(2), None);

    assert_eq!(
        adapter
            .read(2, handle, 0, 5)
            .expect("read open unlinked file"),
        b"hello"
    );
    assert_eq!(
        adapter
            .write(2, handle, 0, b"hello".to_vec())
            .expect("write open unlinked file")
            .bytes_written,
        5
    );
    adapter
        .release(2, handle, 0)
        .expect("release open unlinked file");

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open file").handle;
    adapter.forget(2, 1);
    assert_eq!(adapter.cached_path(2), None);
    adapter
        .release(2, handle, 0)
        .expect("release forgotten open file");

    let operations = client.operations();
    assert_eq!(
        operations,
        vec![Operation::Lookup, Operation::Open, Operation::Release]
    );
}

#[test]
fn fuse_adapter_decrements_lookup_references_before_forgetting_inode_paths() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client, "fuse-ns");

    adapter.lookup(1, "file.txt").expect("first lookup");
    adapter.lookup(1, "file.txt").expect("second lookup");
    assert_eq!(adapter.cached_path(2), Some("/file.txt".into()));

    adapter.forget(2, 1);
    assert_eq!(
        adapter.cached_path(2),
        Some("/file.txt".into()),
        "a partial FUSE forget must keep mappings for remaining lookup refs"
    );
    assert_eq!(
        adapter
            .getattr(2)
            .expect("live inode remains usable")
            .attr
            .inode,
        2
    );

    adapter.forget(2, 1);
    assert_eq!(adapter.cached_path(2), None);
    assert_eq!(
        adapter
            .getattr(2)
            .expect_err("all lookup refs were forgotten")
            .errno(),
        Errno::Stale
    );
}

#[test]
fn fuse_adapter_allows_xattr_size_probe_results() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    assert_eq!(
        adapter
            .getxattr(2, "user.key", 0)
            .expect("getxattr size probe"),
        b"value"
    );
    assert_eq!(
        adapter.listxattr(2, 0).expect("listxattr size probe"),
        vec!["user.key"]
    );
}

#[test]
fn fuse_adapter_applies_invalidations_and_poison_on_sequence_gaps() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");

    adapter
        .apply_invalidation(&invalidation(
            1,
            InvalidationKind::Create,
            "/created.txt",
            "",
            "",
            10,
        ))
        .expect("create invalidation applies");
    assert_eq!(adapter.cached_path(10), Some("/created.txt".into()));

    adapter
        .apply_invalidation(&invalidation(
            2,
            InvalidationKind::Rename,
            "",
            "/created.txt",
            "/renamed.txt",
            0,
        ))
        .expect("rename invalidation applies");
    assert_eq!(adapter.cached_path(10), Some("/renamed.txt".into()));

    adapter
        .apply_invalidation(&invalidation(
            3,
            InvalidationKind::Delete,
            "/renamed.txt",
            "",
            "",
            0,
        ))
        .expect("delete invalidation applies");
    assert_eq!(adapter.cached_path(10), None);

    adapter
        .lookup(1, "file.txt")
        .expect("lookup keeps cache live");
    let gap = adapter
        .apply_invalidation(&invalidation(
            5,
            InvalidationKind::Modify,
            "/file.txt",
            "",
            "",
            0,
        ))
        .expect_err("gap poisons cache");
    assert_eq!(gap.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(
        adapter
            .lookup(1, "file.txt")
            .expect_err("poisoned cache blocks lookups")
            .errno(),
        Errno::Stale
    );

    adapter
        .apply_invalidation(&invalidation(
            6,
            InvalidationKind::FullResync,
            "",
            "",
            "",
            0,
        ))
        .expect("full resync clears poison");
    assert!(!adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(1), Some("/".into()));
}

#[test]
fn fuse_adapter_accepts_per_namespace_sequences_when_other_namespaces_interleave() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");

    adapter
        .apply_invalidation(&invalidation(
            1,
            InvalidationKind::Create,
            "/created.txt",
            "",
            "",
            10,
        ))
        .expect("first local invalidation applies");
    adapter
        .apply_invalidation(&invalidation_in_namespace(
            "other-ns",
            1,
            InvalidationKind::Create,
            "/other.txt",
            "",
            "",
            20,
        ))
        .expect("other namespace invalidation is ignored");
    adapter
        .apply_invalidation(&invalidation(
            2,
            InvalidationKind::Modify,
            "/created.txt",
            "",
            "",
            0,
        ))
        .expect("second local invalidation remains contiguous");

    assert!(!adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(10), Some("/created.txt".into()));
    assert_eq!(adapter.cached_path(20), None);
}

#[test]
fn fuse_adapter_rewrites_and_removes_cached_descendants() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");

    assert_eq!(adapter.lookup(1, "dir").expect("lookup dir").attr.inode, 10);
    assert_eq!(
        adapter
            .lookup(10, "file.txt")
            .expect("lookup child")
            .attr
            .inode,
        11
    );
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));
    assert_eq!(adapter.cached_path(11), Some("/dir/file.txt".into()));

    adapter
        .rename(1, "dir", 1, "renamed")
        .expect("rename cached directory");
    assert_eq!(adapter.cached_path(10), Some("/renamed".into()));
    assert_eq!(adapter.cached_path(11), Some("/renamed/file.txt".into()));

    adapter
        .rmdir(1, "renamed")
        .expect("remove cached directory");
    assert_eq!(adapter.cached_path(10), None);
    assert_eq!(adapter.cached_path(11), None);
}

#[test]
fn fuse_adapter_applies_response_rename_invalidation_once() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup dir");
    adapter
        .lookup(10, "file.txt")
        .expect("lookup cached descendant");
    client.set_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter
        .rename(1, "dir", 1, "renamed")
        .expect("rename consumes response invalidation");

    assert_eq!(adapter.cached_path(10), Some("/renamed".into()));
    assert_eq!(adapter.cached_path(11), Some("/renamed/file.txt".into()));
}

#[test]
fn fuse_adapter_response_rename_gap_rewrites_retained_open_handles_before_poisoning() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup dir");
    adapter
        .lookup(10, "file.txt")
        .expect("lookup cached descendant");
    let handle = adapter.open(11, 0).expect("open descendant").handle;
    client.set_response_invalidations(vec![invalidation(
        5,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter
        .rename(1, "dir", 1, "renamed")
        .expect("committed rename with sequence gap returns success");

    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(11), None);
    assert_eq!(
        adapter
            .getattr(11)
            .expect_err("poisoned inode-only getattr must fail closed despite retained handle")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .open(11, 0)
            .expect_err("poisoned inode-only open must fail closed despite retained handle")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .statfs(11)
            .expect_err("poisoned inode-only statfs must fail closed despite retained handle")
            .errno(),
        Errno::Stale
    );

    adapter
        .read(11, handle, 0, 5)
        .expect("retained open handle follows committed rename despite cache poison");
    assert_eq!(
        client.operations(),
        vec![
            Operation::Lookup,
            Operation::Lookup,
            Operation::Open,
            Operation::Rename,
            Operation::Read,
        ]
    );
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("read request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/renamed/file.txt".into())
    );
}

#[test]
fn fuse_adapter_poisons_cache_when_path_mutation_response_has_no_invalidation() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup dir");
    client.disable_default_invalidations();

    let error = adapter
        .rename(1, "dir", 1, "renamed")
        .expect_err("rename without a covering invalidation must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(10), None);
}

#[test]
fn fuse_adapter_poisons_cache_when_path_mutation_response_has_only_unrelated_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup dir");
    client.set_response_invalidations(vec![invalidation_in_namespace(
        "other-ns",
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    let error = adapter
        .rename(1, "dir", 1, "renamed")
        .expect_err("rename with no same-namespace invalidation must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(10), None);
}

#[test]
fn fuse_adapter_accepts_full_resync_as_covering_path_mutation_invalidation() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    client.set_response_invalidations(vec![invalidation(
        0,
        InvalidationKind::FullResync,
        "",
        "",
        "",
        0,
    )]);

    adapter
        .create(1, "created.txt", 0, 0o644)
        .expect("full resync covers uncertain create invalidation state");
    assert!(!adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_poisons_cache_when_create_response_has_no_covering_create_invalidation() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    client.set_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/other.txt",
        "",
        "",
        99,
    )]);

    let error = adapter
        .create(1, "created.txt", 0, 0o644)
        .expect_err("create without a matching create invalidation must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(3), None);
}

#[test]
fn fuse_adapter_poisons_cache_when_new_mutation_lacks_covering_invalidation() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.disable_default_invalidations();

    let error = adapter
        .symlink(1, "link.txt", b"file.txt")
        .expect_err("symlink without create invalidation must fail closed");
    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    client.disable_default_invalidations();

    let error = adapter
        .setattr(2, None, Some(0o600), None, None, None)
        .expect_err("setattr without metadata invalidation must fail closed");
    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    let handle = adapter.open(2, 0).expect("open source").handle;
    client.disable_default_invalidations();

    let error = adapter
        .fallocate(2, handle, 0, 4096, 0)
        .expect_err("fallocate without modify invalidation must fail closed");
    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_sends_raw_symlink_target_bytes() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter
        .symlink(1, "link.txt", b"target-\xff")
        .expect("symlink succeeds");

    let requests = client.requests();
    let RequestPayload::Symlink(request) = &requests.last().expect("symlink request").payload
    else {
        panic!("expected symlink request");
    };
    assert_eq!(request.target, b"target-\xff");
}

#[test]
fn fuse_adapter_poisons_cache_when_create_or_mkdir_invalidation_lacks_created_inode() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/created.txt",
        "",
        "",
        0,
    )]);

    let error = adapter
        .create(1, "created.txt", 0, 0o644)
        .expect_err("create invalidation without created inode must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(3), None);

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/dir",
        "",
        "",
        0,
    )]);

    let error = adapter
        .mkdir(1, "dir", 0o755)
        .expect_err("mkdir invalidation without created inode must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(4), None);
}

#[test]
fn fuse_adapter_poisons_cache_when_create_invalidation_uses_wrong_inode() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/created.txt",
        "",
        "",
        99,
    )]);

    let error = adapter
        .create(1, "created.txt", 0, 0o644)
        .expect_err("create invalidation with a different inode must fail closed");

    assert_eq!(error.errno(), Errno::Stale);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(3), None);
    assert_eq!(adapter.cached_path(99), None);
}

#[test]
fn fuse_adapter_poisons_cache_on_transport_error_after_path_mutation_may_have_dispatched() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    client.set_transport_error(RpcError::Transport("response encode failed".into()));

    let error = adapter
        .unlink(1, "file.txt")
        .expect_err("path mutation transport failure is uncertain");

    assert_eq!(error.errno(), Errno::Io);
    assert!(matches!(error, FuseError::Transport { .. }));
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(2), None);
    assert_eq!(
        adapter
            .lookup(1, "other.txt")
            .expect_err("poisoned cache blocks later operations")
            .errno(),
        Errno::Stale
    );
}

#[test]
fn fuse_adapter_poisons_cache_on_transport_error_after_truncating_open_may_have_dispatched() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    client.set_transport_error(RpcError::Transport("response encode failed".into()));

    let error = adapter
        .open(2, fs_protocol::OPEN_FLAG_TRUNCATE)
        .expect_err("truncating open transport failure is uncertain");

    assert_eq!(error.errno(), Errno::Io);
    assert!(matches!(error, FuseError::Transport { .. }));
    assert!(adapter.cache_poisoned());
    assert_eq!(
        adapter
            .lookup(1, "other.txt")
            .expect_err("poisoned cache blocks later operations")
            .errno(),
        Errno::Stale
    );
}

#[test]
fn fuse_adapter_rejects_overlapping_rename_requests_before_rpc() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup dir");
    adapter
        .lookup(10, "file.txt")
        .expect("lookup cached descendant");
    let operation_count = client.operations().len();

    let error = adapter
        .rename(1, "dir", 10, "sub")
        .expect_err("source-to-descendant rename is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(client.operations().len(), operation_count);
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));
    assert_eq!(adapter.cached_path(11), Some("/dir/file.txt".into()));

    let error = adapter
        .rename(10, "file.txt", 1, "dir")
        .expect_err("descendant-to-ancestor rename is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(client.operations().len(), operation_count);
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));
    assert_eq!(adapter.cached_path(11), Some("/dir/file.txt".into()));
}

#[test]
fn fuse_adapter_applies_success_response_invalidations_before_returning() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup file");
    client.set_response_invalidations(vec![
        invalidation(1, InvalidationKind::Xattr, "/file.txt", "", "", 0),
        invalidation(
            2,
            InvalidationKind::Create,
            "/server-created.txt",
            "",
            "",
            99,
        ),
    ]);

    adapter
        .setxattr(2, "user.key", b"value".to_vec(), 0)
        .expect("setxattr consumes response invalidation");
    assert_eq!(adapter.cached_path(99), Some("/server-created.txt".into()));

    adapter
        .apply_invalidation(&invalidation(
            3,
            InvalidationKind::Modify,
            "/file.txt",
            "",
            "",
            0,
        ))
        .expect("next sequence is accepted");
    assert!(!adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_returns_success_and_poisons_cache_when_response_sequence_gap_is_observed() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    client.set_response_invalidations(vec![invalidation(
        2,
        InvalidationKind::Create,
        "/raced.txt",
        "",
        "",
        3,
    )]);

    let (entry, handle) = adapter
        .create(1, "raced.txt", 0, 0)
        .expect("committed create returns success even when cache is poisoned");

    assert_eq!(entry.attr.inode, 3);
    assert_eq!(handle.handle, 8);
    assert!(adapter.cache_poisoned());
    assert_eq!(
        adapter
            .lookup(1, "other.txt")
            .expect_err("cache-backed lookup requires full resync")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .read(3, handle.handle, 0, 5)
            .expect("created handle keeps its path after committed response"),
        b"hello"
    );
}

#[test]
fn fuse_adapter_drains_out_of_band_invalidations_before_cache_backed_calls() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    client.queue_drained_invalidations(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/remote-created.txt",
        "",
        "",
        99,
    )]);

    adapter.getattr(1).expect("root getattr drains first");
    assert_eq!(adapter.cached_path(99), Some("/remote-created.txt".into()));
}

#[test]
fn fuse_adapter_drains_remote_rename_before_getattr_and_open_path_resolution() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter
        .getattr(10)
        .expect("getattr uses the renamed path after drain");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("getattr request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/renamed".into())
    );

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter.open(10, 0).expect("open uses renamed path");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("open request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/renamed".into())
    );
}

#[test]
fn fuse_adapter_does_not_drain_after_request_payload_path_is_resolved() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations_after_next_empty_drain(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter
        .getattr(10)
        .expect("first getattr uses path observed at pre-resolution drain");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("first getattr request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/dir".into())
    );
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));

    adapter
        .getattr(10)
        .expect("next getattr drains delayed rename before path resolution");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("second getattr request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/renamed".into())
    );
}

#[test]
fn fuse_adapter_rename_resolves_both_paths_from_one_invalidation_snapshot() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup seeds directory");
    adapter
        .lookup(10, "file.txt")
        .expect("lookup seeds child file");
    client.queue_drained_invalidations_after_next_empty_drain(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    adapter
        .rename(10, "file.txt", 1, "target.txt")
        .expect("rename uses one pre-resolution invalidation snapshot");

    let requests = client.requests();
    let request = requests.last().expect("rename request");
    let RequestPayload::Rename(rename) = &request.payload else {
        panic!("expected rename request, got {:?}", request.payload);
    };
    assert_eq!(
        rename.old_path.as_ref().map(|path| path.path.as_str()),
        Some("/dir/file.txt")
    );
    assert_eq!(
        rename.new_path.as_ref().map(|path| path.path.as_str()),
        Some("/target.txt")
    );
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));
}

#[test]
fn fuse_adapter_readdir_parent_and_payload_path_share_one_invalidation_snapshot() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations_after_next_empty_drain(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/dir",
        "/renamed",
        0,
    )]);

    let (parent, entries) = adapter
        .readdir_with_parent(10, 0, 16)
        .expect("readdir uses one pre-resolution invalidation snapshot");

    assert_eq!(parent, 1);
    assert_eq!(entries.len(), 1);
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("readdir request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/dir".into())
    );
    assert_eq!(adapter.cached_path(10), Some("/dir".into()));
}

#[test]
fn fuse_adapter_scopes_caller_context_to_overlapping_requests() {
    let client = FakeClient::default();
    let adapter = Arc::new(FuseAdapter::new(client.clone(), "fuse-ns"));
    client.set_call_barrier(Arc::new(Barrier::new(2)));

    let first = {
        let adapter = Arc::clone(&adapter);
        thread::spawn(move || {
            adapter.with_caller_context(caller(100, 200, 300), || {
                adapter.getattr(1).expect("first getattr")
            });
        })
    };
    let second = {
        let adapter = Arc::clone(&adapter);
        thread::spawn(move || {
            adapter.with_caller_context(caller(101, 201, 301), || {
                adapter.getattr(1).expect("second getattr")
            });
        })
    };

    first.join().expect("first request thread");
    second.join().expect("second request thread");

    let mut callers: Vec<_> = client
        .requests()
        .into_iter()
        .map(|request| request.caller.expect("caller context"))
        .collect();
    callers.sort_by_key(|caller| caller.uid);
    assert_eq!(callers, vec![caller(100, 200, 300), caller(101, 201, 301)]);
}

#[test]
fn fuse_adapter_drains_remote_rename_before_open_handle_read_and_write_paths() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds file");
    let handle = adapter.open(2, 0).expect("open seeds handle").handle;
    client.queue_drained_invalidations(vec![invalidation(
        1,
        InvalidationKind::Rename,
        "",
        "/file.txt",
        "/renamed.txt",
        0,
    )]);

    adapter
        .read(2, handle, 0, 5)
        .expect("read uses renamed handle path");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("read request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/renamed.txt".into())
    );

    client.queue_drained_invalidations(vec![invalidation(
        2,
        InvalidationKind::Rename,
        "",
        "/renamed.txt",
        "/again.txt",
        0,
    )]);

    adapter
        .write(2, handle, 0, b"hello".to_vec())
        .expect("write uses renamed handle path");
    let requests = client.requests();
    assert_eq!(
        requests
            .last()
            .expect("write request")
            .payload
            .primary_path()
            .map(str::to_owned),
        Some("/again.txt".into())
    );
}

#[test]
fn fuse_adapter_drains_full_resync_before_cache_path_and_parent_resolution() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations(vec![invalidation(
        0,
        InvalidationKind::FullResync,
        "",
        "",
        "",
        0,
    )]);

    assert_eq!(
        adapter
            .getattr(10)
            .expect_err("full resync clears cached getattr path before request")
            .errno(),
        Errno::Stale
    );
    assert_eq!(client.operations(), vec![Operation::Lookup]);

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations(vec![invalidation(
        0,
        InvalidationKind::FullResync,
        "",
        "",
        "",
        0,
    )]);

    assert_eq!(
        adapter
            .open(10, 0)
            .expect_err("full resync clears cached open path before request")
            .errno(),
        Errno::Stale
    );
    assert_eq!(client.operations(), vec![Operation::Lookup]);

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "dir").expect("lookup seeds directory");
    client.queue_drained_invalidations(vec![invalidation(
        0,
        InvalidationKind::FullResync,
        "",
        "",
        "",
        0,
    )]);

    assert_eq!(
        adapter
            .parent_inode(10)
            .expect_err("product readdir parent lookup drains full resync first")
            .errno(),
        Errno::Stale
    );
}

#[test]
fn fuse_adapter_accepts_first_remote_sequence_as_baseline_only_for_empty_state() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.queue_drained_invalidations(vec![invalidation(
        5,
        InvalidationKind::Create,
        "/late.txt",
        "",
        "",
        50,
    )]);

    adapter
        .getattr(1)
        .expect("late join with empty cache baselines on first sequence");

    assert!(!adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(50), Some("/late.txt".into()));

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter
        .lookup(1, "file.txt")
        .expect("lookup makes cache non-empty");
    client.queue_drained_invalidations(vec![invalidation(
        5,
        InvalidationKind::Modify,
        "/file.txt",
        "",
        "",
        0,
    )]);

    assert_eq!(
        adapter
            .getattr(1)
            .expect_err("missed sequence with live cache poisons")
            .errno(),
        Errno::Stale
    );
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_poisons_cache_when_out_of_band_drain_is_uncertain() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open seeds handle path").handle;
    client.set_drain_error(RpcError::Malformed("bad invalidation frame".into()));

    let error = adapter
        .getattr(1)
        .expect_err("malformed drained invalidation fails closed");

    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(2), None);
    assert_eq!(
        adapter
            .read(2, handle, 0, 5)
            .expect("open handle read survives path-cache poison"),
        b"hello"
    );
    adapter
        .release(2, handle, 0)
        .expect("release is best-effort after cache poison");
    assert!(
        client.operations().contains(&Operation::Release),
        "poisoning the path cache must not leak backend handles"
    );
}

#[test]
fn fuse_adapter_drains_full_resync_after_sequence_gap_in_same_batch() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    client.queue_drained_invalidations(vec![
        invalidation(2, InvalidationKind::Modify, "/file.txt", "", "", 0),
        invalidation(0, InvalidationKind::FullResync, "", "", "", 0),
    ]);

    adapter
        .getattr(1)
        .expect("later full resync in a consumed drain batch recovers cache state");

    assert!(!adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(1), Some("/".into()));
    assert_eq!(client.operations(), vec![Operation::Getattr]);
}

#[test]
fn fuse_adapter_release_reaches_backend_without_draining_uncertain_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open seeds handle path").handle;
    client.set_drain_error(RpcError::Malformed("bad invalidation frame".into()));

    adapter
        .release(2, handle, 0)
        .expect("release bypasses invalidation drain to avoid leaking backend handle");

    assert_eq!(
        client.operations(),
        vec![Operation::Lookup, Operation::Open, Operation::Release]
    );
    assert_eq!(
        adapter
            .getattr(1)
            .expect_err("next cache-backed operation still fails closed on pending drain error")
            .errno(),
        Errno::InvalidArgument
    );
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_full_resync_clears_path_cache_but_keeps_open_handle_lifecycle() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open file").handle;

    adapter
        .apply_invalidation(&invalidation(
            0,
            InvalidationKind::FullResync,
            "",
            "",
            "",
            0,
        ))
        .expect("full resync is accepted");

    assert_eq!(adapter.cached_path(2), None);
    assert_eq!(
        adapter
            .read(2, handle, 0, 5)
            .expect("open handle read survives full resync"),
        b"hello"
    );
    adapter
        .release(2, handle, 0)
        .expect("release reaches the backend after full resync");
    assert_eq!(
        client.operations(),
        vec![
            Operation::Lookup,
            Operation::Open,
            Operation::Read,
            Operation::Release
        ]
    );
}

#[test]
fn fuse_adapter_full_resync_blocks_path_based_inode_calls_even_with_open_handle() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup seeds cache");
    let handle = adapter.open(2, 0).expect("open file").handle;

    adapter
        .apply_invalidation(&invalidation(
            0,
            InvalidationKind::FullResync,
            "",
            "",
            "",
            0,
        ))
        .expect("full resync is accepted");

    assert_eq!(adapter.cached_path(2), None);
    assert_eq!(
        adapter
            .getattr(2)
            .expect_err("full resync must block inode-only getattr until revalidation")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .open(2, 0)
            .expect_err("full resync must block inode-only open until revalidation")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .statfs(2)
            .expect_err("full resync must block inode-only statfs until revalidation")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .getxattr(2, "user.key", 64)
            .expect_err("full resync must block inode-only xattr reads until revalidation")
            .errno(),
        Errno::Stale
    );
    assert_eq!(
        adapter
            .read(2, handle, 0, 5)
            .expect("real handle-bound reads still survive full resync"),
        b"hello"
    );
    adapter
        .release(2, handle, 0)
        .expect("release reaches the backend after full resync");
    assert_eq!(
        client.operations(),
        vec![
            Operation::Lookup,
            Operation::Open,
            Operation::Read,
            Operation::Release
        ]
    );
}

#[test]
fn fuse_adapter_conservatively_removes_created_paths_without_inode() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    assert_eq!(adapter.cached_path(2), Some("/file.txt".into()));

    adapter
        .apply_invalidation(&invalidation(
            1,
            InvalidationKind::Create,
            "/file.txt",
            "",
            "",
            0,
        ))
        .expect("create without inode conservatively invalidates path");
    assert_eq!(adapter.cached_path(2), None);

    adapter
        .apply_invalidation(&invalidation(
            2,
            InvalidationKind::Modify,
            "/file.txt",
            "",
            "",
            0,
        ))
        .expect("next sequence remains contiguous");
    assert!(!adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_rejects_invalid_success_responses_before_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_malformed_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply.txt",
        "",
        "",
        99,
    )]);

    let error = adapter
        .lookup(1, "bad.txt")
        .expect_err("invalid response is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(99), None);
    assert!(!adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_poisons_cache_on_malformed_success_response_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    client.set_unvalidated_success_response_invalidations(vec![invalidation(
        1,
        InvalidationKind::Delete,
        "",
        "",
        "",
        0,
    )]);

    let error = adapter
        .setxattr(2, "user.key", b"value".to_vec(), 0)
        .expect_err("malformed response invalidation fails");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());
    assert_eq!(adapter.cached_path(2), None);
}

#[test]
fn fuse_adapter_poisons_cache_on_unclassifiable_success_response_invalidations() {
    for invalidation in [
        invalidation_in_namespace("", 1, InvalidationKind::Delete, "/file.txt", "", "", 0),
        invalidation_in_namespace(
            "",
            1,
            InvalidationKind::Rename,
            "",
            "/file.txt",
            "/renamed.txt",
            0,
        ),
    ] {
        let client = FakeClient::default();
        let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
        adapter.lookup(1, "file.txt").expect("lookup file");
        client.set_unvalidated_success_response_invalidations(vec![invalidation]);

        let error = adapter
            .setxattr(2, "user.key", b"value".to_vec(), 0)
            .expect_err("unclassifiable response invalidation fails");
        assert_eq!(error.errno(), Errno::InvalidArgument);
        assert!(adapter.cache_poisoned());
        assert_eq!(adapter.cached_path(2), None);
    }
}

#[test]
fn fuse_adapter_rejects_uncorrelated_success_responses_before_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_wrong_request_id_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply.txt",
        "",
        "",
        99,
    )]);

    let error = adapter
        .lookup(1, "wrong-request.txt")
        .expect_err("wrong request id response is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(99), None);
    assert!(!adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_wrong_operation_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-either.txt",
        "",
        "",
        100,
    )]);

    let error = adapter
        .lookup(1, "wrong-operation.txt")
        .expect_err("wrong operation response is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(100), None);
    assert!(!adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_rejects_request_contradictory_success_responses_before_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-read.txt",
        "",
        "",
        99,
    )]);
    let error = adapter
        .read(2, 7, 0, 5)
        .expect_err("oversized read response is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(99), None);
    assert!(!adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-readdir.txt",
        "",
        "",
        100,
    )]);
    let error = adapter
        .readdir(1, 0, 1)
        .expect_err("too many readdir entries are rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(100), None);
    assert!(!adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-write.txt",
        "",
        "",
        101,
    )]);
    let error = adapter
        .write(2, 7, 0, b"hello".to_vec())
        .expect_err("impossible write count is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(101), None);
    assert!(adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    adapter.open(2, 0).expect("open source");
    adapter
        .create(1, "created.txt", 0, 0o644)
        .expect("create destination");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-copy.txt",
        "",
        "",
        102,
    )]);
    let error = adapter
        .copy_file_range(2, 7, 0, 3, 8, 0, 5, 0)
        .expect_err("impossible copy count is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(102), None);
    assert!(adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    adapter.open(2, 0).expect("open source");
    adapter
        .create(1, "created.txt", 0, 0o644)
        .expect("create destination");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/must-not-apply-mounted-copy.txt",
        "",
        "",
        103,
    )]);
    let error = adapter
        .copy_file_range_for_reply_write(2, 7, 0, 3, 8, 0, u64::MAX, 0)
        .expect_err("mounted copy_file_range rejects reply counts above ReplyWrite capacity");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(103), None);
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_rejects_create_and_mkdir_attr_kind_mismatches_before_invalidations() {
    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/created.txt",
        "",
        "",
        3,
    )]);
    let error = adapter
        .create(1, "created.txt", 0, 0o644)
        .expect_err("wrong create attr kind is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(3), None);
    assert!(adapter.cache_poisoned());

    let client = FakeClient::default();
    let adapter = FuseAdapter::new(client.clone(), "fuse-ns");
    client.set_request_contradictory_success_response(vec![invalidation(
        1,
        InvalidationKind::Create,
        "/dir",
        "",
        "",
        4,
    )]);
    let error = adapter
        .mkdir(1, "dir", 0o755)
        .expect_err("wrong mkdir attr kind is rejected");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert_eq!(adapter.cached_path(4), None);
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_poisons_cache_on_malformed_delete_or_rename_invalidations() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");

    let error = adapter
        .apply_invalidation(&invalidation(1, InvalidationKind::Delete, "", "", "", 0))
        .expect_err("delete without path is malformed");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());

    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    adapter.lookup(1, "file.txt").expect("lookup file");
    let error = adapter
        .apply_invalidation(&invalidation(1, InvalidationKind::Delete, "/", "", "", 0))
        .expect_err("root delete cannot safely apply");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());

    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    adapter.lookup(1, "dir").expect("lookup dir");
    let error = adapter
        .apply_invalidation(&invalidation(
            1,
            InvalidationKind::Rename,
            "",
            "/dir",
            "",
            0,
        ))
        .expect_err("rename without destination is malformed");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());

    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
    adapter.lookup(1, "dir").expect("lookup dir");
    let error = adapter
        .apply_invalidation(&invalidation(
            1,
            InvalidationKind::Rename,
            "",
            "/",
            "/renamed",
            0,
        ))
        .expect_err("root rename cannot safely apply");
    assert_eq!(error.errno(), Errno::InvalidArgument);
    assert!(adapter.cache_poisoned());
}

#[test]
fn fuse_adapter_poisons_cache_on_overlapping_rename_invalidations() {
    for (old_path, new_path) in [("/dir", "/dir/file.txt"), ("/dir/file.txt", "/dir")] {
        let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");
        adapter.lookup(1, "dir").expect("lookup dir");
        adapter
            .lookup(10, "file.txt")
            .expect("lookup cached descendant");
        assert_eq!(adapter.cached_path(10), Some("/dir".into()));
        assert_eq!(adapter.cached_path(11), Some("/dir/file.txt".into()));

        let error = adapter
            .apply_invalidation(&invalidation(
                1,
                InvalidationKind::Rename,
                "",
                old_path,
                new_path,
                0,
            ))
            .expect_err("overlapping rename invalidation is unsafe");
        assert_eq!(error.errno(), Errno::InvalidArgument);
        assert!(adapter.cache_poisoned());
        assert_eq!(adapter.cached_path(10), None);
        assert_eq!(adapter.cached_path(11), None);

        let gap = adapter
            .apply_invalidation(&invalidation(
                2,
                InvalidationKind::Create,
                "/after-overlap.txt",
                "",
                "",
                99,
            ))
            .expect_err("rejected overlapping rename must not advance sequence");
        assert_eq!(gap.errno(), Errno::Stale);
    }
}

#[test]
fn fuse_adapter_metrics_track_successful_calls() {
    let adapter = FuseAdapter::new(FakeClient::default(), "fuse-ns");

    adapter.lookup(1, "file.txt").expect("lookup succeeds");

    let metrics = adapter.metrics();
    assert_eq!(metrics.calls_total, 1);
    assert_eq!(metrics.call_failures, 0);
    assert_eq!(metrics.invalidation_drains_total, 1);
    assert_eq!(metrics.cache_poison_total, 0);
    assert_eq!(metrics.call_latency_micros.total, 1);
}

#[test]
fn fuse_adapter_metrics_track_invalidation_drain_failures() {
    let client = FakeClient::default();
    client.set_drain_error(RpcError::Malformed("bad invalidation frame".into()));
    let adapter = FuseAdapter::new(client, "fuse-ns");

    let error = adapter
        .lookup(1, "file.txt")
        .expect_err("lookup fails closed");
    assert_eq!(error.errno(), Errno::InvalidArgument);

    let metrics = adapter.metrics();
    assert_eq!(metrics.calls_total, 0);
    assert_eq!(metrics.call_failures, 0);
    assert_eq!(metrics.invalidation_drains_total, 1);
    assert_eq!(metrics.invalidation_errors_total, 1);
    assert_eq!(metrics.cache_poison_total, 1);
}

#[derive(Clone, Default)]
struct FakeClient {
    inner: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    operations: Vec<Operation>,
    requests: Vec<RequestEnvelope>,
    response_errno: Option<Errno>,
    transport_error: Option<RpcError>,
    drain_error: Option<RpcError>,
    response_invalidations: Vec<pb::Invalidation>,
    drained_invalidations: Vec<pb::Invalidation>,
    delayed_drained_invalidations: Vec<pb::Invalidation>,
    call_barrier: Option<Arc<Barrier>>,
    malformed_success_response: bool,
    wrong_request_id_response: bool,
    wrong_operation_response: bool,
    request_contradictory_success_response: bool,
    unvalidated_success_response: bool,
    disable_default_invalidations: bool,
    next_invalidation_sequence: u64,
}

impl FakeClient {
    fn operations(&self) -> Vec<Operation> {
        self.inner
            .lock()
            .expect("fake state lock")
            .operations
            .clone()
    }

    fn requests(&self) -> Vec<RequestEnvelope> {
        self.inner.lock().expect("fake state lock").requests.clone()
    }

    fn set_response_errno(&self, errno: Errno) {
        self.inner.lock().expect("fake state lock").response_errno = Some(errno);
    }

    fn set_transport_error(&self, error: RpcError) {
        self.inner.lock().expect("fake state lock").transport_error = Some(error);
    }

    fn set_drain_error(&self, error: RpcError) {
        self.inner.lock().expect("fake state lock").drain_error = Some(error);
    }

    fn set_response_invalidations(&self, invalidations: Vec<pb::Invalidation>) {
        self.inner
            .lock()
            .expect("fake state lock")
            .response_invalidations = invalidations;
    }

    fn set_malformed_success_response(&self, invalidations: Vec<pb::Invalidation>) {
        let mut state = self.inner.lock().expect("fake state lock");
        state.response_invalidations = invalidations;
        state.malformed_success_response = true;
    }

    fn set_wrong_request_id_response(&self, invalidations: Vec<pb::Invalidation>) {
        let mut state = self.inner.lock().expect("fake state lock");
        state.response_invalidations = invalidations;
        state.wrong_request_id_response = true;
    }

    fn set_wrong_operation_response(&self, invalidations: Vec<pb::Invalidation>) {
        let mut state = self.inner.lock().expect("fake state lock");
        state.response_invalidations = invalidations;
        state.wrong_operation_response = true;
    }

    fn set_request_contradictory_success_response(&self, invalidations: Vec<pb::Invalidation>) {
        let mut state = self.inner.lock().expect("fake state lock");
        state.response_invalidations = invalidations;
        state.request_contradictory_success_response = true;
    }

    fn set_unvalidated_success_response_invalidations(&self, invalidations: Vec<pb::Invalidation>) {
        let mut state = self.inner.lock().expect("fake state lock");
        state.response_invalidations = invalidations;
        state.unvalidated_success_response = true;
    }

    fn queue_drained_invalidations(&self, invalidations: Vec<pb::Invalidation>) {
        self.inner
            .lock()
            .expect("fake state lock")
            .drained_invalidations
            .extend(invalidations);
    }

    fn queue_drained_invalidations_after_next_empty_drain(
        &self,
        invalidations: Vec<pb::Invalidation>,
    ) {
        self.inner
            .lock()
            .expect("fake state lock")
            .delayed_drained_invalidations
            .extend(invalidations);
    }

    fn disable_default_invalidations(&self) {
        self.inner
            .lock()
            .expect("fake state lock")
            .disable_default_invalidations = true;
    }

    fn set_call_barrier(&self, barrier: Arc<Barrier>) {
        self.inner.lock().expect("fake state lock").call_barrier = Some(barrier);
    }
}

impl RpcClient for FakeClient {
    fn call(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError> {
        let barrier = {
            let mut state = self.inner.lock().expect("fake state lock");
            state.operations.push(request.operation);
            state.requests.push(request.clone());
            state.call_barrier.clone()
        };
        if let Some(barrier) = barrier {
            barrier.wait();
        }
        let mut state = self.inner.lock().expect("fake state lock");
        if let Some(error) = state.transport_error.clone() {
            return Err(error);
        }
        if let Some(errno) = state.response_errno {
            return Ok(ResponseEnvelope::failure_for(&request, errno, "fake errno"));
        }
        let explicit_invalidations = std::mem::take(&mut state.response_invalidations);
        if state.malformed_success_response {
            state.malformed_success_response = false;
            return Ok(malformed_success_response(&request, explicit_invalidations));
        }
        if state.wrong_request_id_response {
            state.wrong_request_id_response = false;
            return ResponseEnvelope::success_for(
                &request,
                response_for_request(&request),
                explicit_invalidations,
            )
            .map(|mut response| {
                response.request_id = "other-request".into();
                response
            })
            .map_err(|error| RpcError::Malformed(error.to_string()));
        }
        if state.wrong_operation_response {
            state.wrong_operation_response = false;
            return Ok(wrong_operation_success_response(
                &request,
                explicit_invalidations,
            ));
        }
        if state.request_contradictory_success_response {
            state.request_contradictory_success_response = false;
            return Ok(request_contradictory_success_response(
                &request,
                explicit_invalidations,
            ));
        }
        if state.unvalidated_success_response {
            state.unvalidated_success_response = false;
            return Ok(unvalidated_success_response(
                &request,
                explicit_invalidations,
            ));
        }
        let payload = response_for_request(&request);
        let invalidations = if state.disable_default_invalidations {
            state.disable_default_invalidations = false;
            Vec::new()
        } else if explicit_invalidations.is_empty() {
            default_invalidations_for_success(
                &request,
                &payload,
                &mut state.next_invalidation_sequence,
            )
        } else {
            response_invalidations_for_request(explicit_invalidations, &request)
        };
        ResponseEnvelope::success_for(&request, payload, invalidations)
            .map_err(|error| RpcError::Malformed(error.to_string()))
    }

    fn drain_invalidations(&self, namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
        let mut state = self.inner.lock().expect("fake state lock");
        if let Some(error) = state.drain_error.take() {
            return Err(error);
        }
        let delayed = std::mem::take(&mut state.delayed_drained_invalidations);
        let mut drained = Vec::new();
        let mut retained = Vec::new();
        for invalidation in std::mem::take(&mut state.drained_invalidations) {
            if invalidation.namespace == namespace {
                drained.push(invalidation);
            } else {
                retained.push(invalidation);
            }
        }
        retained.extend(delayed);
        state.drained_invalidations = retained;
        Ok(drained)
    }
}

fn malformed_success_response(
    request: &RequestEnvelope,
    invalidations: Vec<pb::Invalidation>,
) -> ResponseEnvelope {
    ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        operation: request.operation,
        namespace: request.namespace.clone(),
        deadline_unix_nanos: request.deadline_unix_nanos,
        trace: request.trace.clone(),
        ok: true,
        errno: None,
        error_message: String::new(),
        payload: Some(ResponsePayload::Read(pb::ReadResponse {
            data: b"wrong operation".to_vec(),
        })),
        observations: Vec::new(),
        invalidations,
    }
}

fn wrong_operation_success_response(
    request: &RequestEnvelope,
    invalidations: Vec<pb::Invalidation>,
) -> ResponseEnvelope {
    ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        operation: Operation::Write,
        namespace: request.namespace.clone(),
        deadline_unix_nanos: request.deadline_unix_nanos,
        trace: request.trace.clone(),
        ok: true,
        errno: None,
        error_message: String::new(),
        payload: Some(ResponsePayload::Write(pb::WriteResponse {
            bytes_written: 0,
        })),
        observations: Vec::new(),
        invalidations,
    }
}

fn request_contradictory_success_response(
    request: &RequestEnvelope,
    invalidations: Vec<pb::Invalidation>,
) -> ResponseEnvelope {
    ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        operation: request.operation,
        namespace: request.namespace.clone(),
        deadline_unix_nanos: request.deadline_unix_nanos,
        trace: request.trace.clone(),
        ok: true,
        errno: None,
        error_message: String::new(),
        payload: Some(request_contradictory_payload(request)),
        observations: Vec::new(),
        invalidations,
    }
}

fn unvalidated_success_response(
    request: &RequestEnvelope,
    invalidations: Vec<pb::Invalidation>,
) -> ResponseEnvelope {
    ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        operation: request.operation,
        namespace: request.namespace.clone(),
        deadline_unix_nanos: request.deadline_unix_nanos,
        trace: request.trace.clone(),
        ok: true,
        errno: None,
        error_message: String::new(),
        payload: Some(response_for_request(request)),
        observations: Vec::new(),
        invalidations,
    }
}

fn request_contradictory_payload(request: &RequestEnvelope) -> ResponsePayload {
    match &request.payload {
        RequestPayload::Readdir(_) => ResponsePayload::Readdir(pb::ReaddirResponse {
            entries: vec![
                pb::DirectoryEntry {
                    inode: 2,
                    name: "a.txt".into(),
                    kind: pb::FileKind::File as i32,
                },
                pb::DirectoryEntry {
                    inode: 3,
                    name: "b.txt".into(),
                    kind: pb::FileKind::File as i32,
                },
            ],
            end: false,
        }),
        RequestPayload::Read(_) => ResponsePayload::Read(pb::ReadResponse {
            data: b"too many bytes".to_vec(),
        }),
        RequestPayload::Write(value) => ResponsePayload::Write(pb::WriteResponse {
            bytes_written: value.data.len() as u32 + 1,
        }),
        RequestPayload::Create(_) => ResponsePayload::Create(pb::CreateResponse {
            attr: Some(file_attr(3, pb::FileKind::Directory, 0)),
            handle: 8,
        }),
        RequestPayload::Mkdir(_) => ResponsePayload::Mkdir(pb::LookupResponse {
            attr: Some(file_attr(4, pb::FileKind::File, 0)),
        }),
        RequestPayload::CopyFileRange(value) => {
            ResponsePayload::CopyFileRange(pb::CopyFileRangeResponse {
                bytes_copied: value.length + 1,
            })
        }
        _ => response_for_request(request),
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
    let mut invalidation = pb::Invalidation {
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
    };
    if let RequestPayload::Rename(value) = &request.payload {
        invalidation.path.clear();
        invalidation.old_path = value
            .old_path
            .as_ref()
            .map(|path| path.path.clone())
            .unwrap_or_default();
        invalidation.new_path = value
            .new_path
            .as_ref()
            .map(|path| path.path.clone())
            .unwrap_or_default();
    }
    vec![invalidation]
}

fn response_invalidations_for_request(
    invalidations: Vec<pb::Invalidation>,
    request: &RequestEnvelope,
) -> Vec<pb::Invalidation> {
    invalidations
        .into_iter()
        .map(|mut invalidation| {
            if invalidation.request_id == "mutation" {
                invalidation.request_id = request.request_id.clone();
            }
            invalidation
        })
        .collect()
}

fn invalidation_kind_for_request(payload: &RequestPayload) -> Option<InvalidationKind> {
    match payload {
        RequestPayload::Open(value) if value.flags & fs_protocol::OPEN_FLAG_TRUNCATE != 0 => {
            Some(InvalidationKind::Modify)
        }
        payload => invalidation_kind_for_effect(payload.operation().spec().effect),
    }
}

fn invalidation_kind_for_effect(effect: fs_protocol::OperationEffect) -> Option<InvalidationKind> {
    match effect {
        fs_protocol::OperationEffect::ContentMutation => Some(InvalidationKind::Modify),
        fs_protocol::OperationEffect::CreateNode => Some(InvalidationKind::Create),
        fs_protocol::OperationEffect::RenameNode => Some(InvalidationKind::Rename),
        fs_protocol::OperationEffect::DeleteNode => Some(InvalidationKind::Delete),
        fs_protocol::OperationEffect::MetadataMutation => Some(InvalidationKind::Metadata),
        fs_protocol::OperationEffect::XattrMutation => Some(InvalidationKind::Xattr),
        _ => None,
    }
}

fn response_for_request(request: &RequestEnvelope) -> ResponsePayload {
    match request.operation {
        Operation::Lookup => ResponsePayload::Lookup(pb::LookupResponse {
            attr: Some(attr_for_lookup(request)),
        }),
        Operation::Getattr => ResponsePayload::Getattr(pb::GetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        }),
        Operation::Readdir => ResponsePayload::Readdir(pb::ReaddirResponse {
            entries: vec![pb::DirectoryEntry {
                inode: 2,
                name: "file.txt".into(),
                kind: pb::FileKind::File as i32,
            }],
            end: true,
        }),
        Operation::Open => ResponsePayload::Open(pb::OpenResponse {
            handle: 7,
            flags: 0,
        }),
        Operation::Read => ResponsePayload::Read(pb::ReadResponse {
            data: b"hello".to_vec(),
        }),
        Operation::Write => ResponsePayload::Write(pb::WriteResponse { bytes_written: 5 }),
        Operation::Create => ResponsePayload::Create(pb::CreateResponse {
            attr: Some(file_attr(3, pb::FileKind::File, 0)),
            handle: 8,
        }),
        Operation::Rename => ResponsePayload::Rename(pb::EmptyResponse {}),
        Operation::Unlink => ResponsePayload::Unlink(pb::EmptyResponse {}),
        Operation::Mkdir => ResponsePayload::Mkdir(pb::LookupResponse {
            attr: Some(file_attr(4, pb::FileKind::Directory, 0)),
        }),
        Operation::Rmdir => ResponsePayload::Rmdir(pb::EmptyResponse {}),
        Operation::Statfs => ResponsePayload::Statfs(pb::StatfsResponse {
            stat: Some(pb::StatFs {
                blocks: 100,
                blocks_free: 50,
                files: 10,
                files_free: 5,
                block_size: 4096,
                name_max: 255,
                blocks_available: 40,
                fragment_size: 2048,
            }),
        }),
        Operation::Getxattr => ResponsePayload::Getxattr(pb::GetxattrResponse {
            value: b"value".to_vec(),
        }),
        Operation::Setxattr => ResponsePayload::Setxattr(pb::EmptyResponse {}),
        Operation::Listxattr => ResponsePayload::Listxattr(pb::ListxattrResponse {
            names: vec!["user.key".into()],
        }),
        Operation::Removexattr => ResponsePayload::Removexattr(pb::EmptyResponse {}),
        Operation::Release => ResponsePayload::Release(pb::EmptyResponse {}),
        Operation::Readlink => ResponsePayload::Readlink(pb::ReadlinkResponse {
            target: b"file.txt".to_vec(),
        }),
        Operation::Symlink => ResponsePayload::Symlink(pb::SymlinkResponse {
            attr: Some(file_attr(5, pb::FileKind::Symlink, 8)),
        }),
        Operation::Hardlink => ResponsePayload::Hardlink(pb::HardlinkResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 128)),
        }),
        Operation::Setattr => ResponsePayload::Setattr(pb::SetattrResponse {
            attr: Some(file_attr(2, pb::FileKind::File, 16)),
        }),
        Operation::Flush => ResponsePayload::Flush(pb::EmptyResponse {}),
        Operation::Fsync => ResponsePayload::Fsync(pb::EmptyResponse {}),
        Operation::Fsyncdir => ResponsePayload::Fsyncdir(pb::EmptyResponse {}),
        Operation::Getlk => ResponsePayload::Getlk(pb::GetlkResponse { lock: None }),
        Operation::Setlk => ResponsePayload::Setlk(pb::EmptyResponse {}),
        Operation::Flock => ResponsePayload::Flock(pb::EmptyResponse {}),
        Operation::CopyFileRange => {
            ResponsePayload::CopyFileRange(pb::CopyFileRangeResponse { bytes_copied: 5 })
        }
        Operation::Fallocate => ResponsePayload::Fallocate(pb::EmptyResponse {}),
        Operation::Lseek => ResponsePayload::Lseek(pb::LseekResponse { offset: 5 }),
    }
}

fn attr_for_lookup(request: &RequestEnvelope) -> pb::FileAttr {
    let RequestPayload::Lookup(value) = &request.payload else {
        return file_attr(2, pb::FileKind::File, 128);
    };
    match value.path.as_ref().map(|path| path.path.as_str()) {
        Some("/dir") | Some("/renamed") => directory_attr(10),
        Some("/dir/file.txt") | Some("/renamed/file.txt") => file_attr(11, pb::FileKind::File, 128),
        _ => file_attr(2, pb::FileKind::File, 128),
    }
}

fn caller(uid: u32, gid: u32, pid: u32) -> pb::CallerContext {
    pb::CallerContext { uid, gid, pid }
}

fn invalidation(
    sequence: u64,
    kind: InvalidationKind,
    path: &str,
    old_path: &str,
    new_path: &str,
    inode: u64,
) -> pb::Invalidation {
    invalidation_in_namespace("fuse-ns", sequence, kind, path, old_path, new_path, inode)
}

fn invalidation_in_namespace(
    namespace: &str,
    sequence: u64,
    kind: InvalidationKind,
    path: &str,
    old_path: &str,
    new_path: &str,
    inode: u64,
) -> pb::Invalidation {
    pb::Invalidation {
        namespace: namespace.into(),
        sequence,
        kind: kind.wire_value(),
        path: path.into(),
        old_path: old_path.into(),
        new_path: new_path.into(),
        inode,
        handle: 0,
        request_id: "mutation".into(),
    }
}
