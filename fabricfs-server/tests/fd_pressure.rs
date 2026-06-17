#![cfg(target_os = "linux")]

use std::fs::File;

use fabricfs_server::passthrough::PassthroughFs;
use fabricfs_server::server::{FsOptions, FuseContext, HandleKind, OpenedObjectStorage};

fn ctx() -> FuseContext {
    FuseContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: 0,
    }
}

struct NoFileLimitGuard {
    original: libc::rlimit,
}

impl NoFileLimitGuard {
    fn lower_soft_limit(max_limit: libc::rlim_t) -> Self {
        let mut original = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let get_result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original) };
        assert_eq!(get_result, 0, "getrlimit(RLIMIT_NOFILE) failed");

        let limit = max_limit.min(original.rlim_cur).min(original.rlim_max);
        let lowered = libc::rlimit {
            rlim_cur: limit,
            rlim_max: original.rlim_max,
        };
        let set_result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) };
        assert_eq!(set_result, 0, "setrlimit(RLIMIT_NOFILE) failed");
        Self { original }
    }
}

impl Drop for NoFileLimitGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.original) };
    }
}

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .count()
}

fn exhaust_fds() -> Vec<File> {
    let mut files = Vec::new();
    loop {
        match File::open("/dev/null") {
            Ok(file) => files.push(file),
            Err(error) if error.raw_os_error() == Some(libc::EMFILE) => break,
            Err(error) => panic!("opening /dev/null failed before fd exhaustion: {error}"),
        }
    }
    files
}

#[test]
fn fsync_handles_do_not_allocate_extra_file_descriptors() {
    let root = tempfile::tempdir().expect("tempdir");
    let fs = PassthroughFs::new(root.path().to_path_buf(), FsOptions::default())
        .expect("passthrough fs init");

    let (file_fh, _) = fs
        .create_file("/tracked", 0o644, libc::O_RDWR, Some(ctx()))
        .expect("create tracked file");
    fs.write_fh("/tracked", file_fh, 0, b"durable", Some(ctx()))
        .expect("write tracked file");

    fs.mkdir("/syncdir", 0o755, Some(ctx()))
        .expect("create sync directory");
    let (dir_fh, _) = fs
        .open("/syncdir", HandleKind::Dir, libc::O_RDONLY, Some(ctx()))
        .expect("open sync directory");

    let soft_limit = (open_fd_count() + 8) as libc::rlim_t;
    let _limit = NoFileLimitGuard::lower_soft_limit(soft_limit);
    let fillers = exhaust_fds();

    fs.sync_file_fh("/tracked", file_fh, true)
        .expect("fdatasync should use the stored file handle without dup");
    fs.sync_file_fh("/tracked", file_fh, false)
        .expect("fsync should use the stored file handle without dup");
    OpenedObjectStorage::sync_dir_fh(&fs, "/syncdir", dir_fh, false)
        .expect("fsyncdir should use the stored directory handle without dup");

    drop(fillers);
    fs.release_fh(file_fh);
    fs.release_fh(dir_fh);
}
