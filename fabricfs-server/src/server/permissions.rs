use std::fs;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use super::errors::{errno, io_errno};
use super::paths::path_to_cstring;
use super::{metadata_to_stat, FuseContext, Stat};

pub fn apply_handle_setattr(
    file: &File,
    access_bits: u32,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    ctx: FuseContext,
) -> Result<Stat, i32> {
    let meta = file.metadata().map_err(io_errno)?;
    if mode.is_some() {
        ensure_owner_or_root(&meta, ctx.uid)?;
    }
    if uid.is_some() || gid.is_some() {
        ensure_setattr_ownership_allowed(&meta, ctx.uid, ctx.gid, uid, gid)?;
    }
    if size.is_some() && access_bits & 0o2 == 0 {
        return Err(libc::EBADF);
    }

    if let Some(size) = size {
        file.set_len(size).map_err(io_errno)?;
    }
    if let Some(mode) = mode {
        let rc = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
        if rc != 0 {
            return Err(errno());
        }
    }
    if uid.is_some() || gid.is_some() {
        let uid = uid.unwrap_or(u32::MAX) as libc::uid_t;
        let gid = gid.unwrap_or(u32::MAX) as libc::gid_t;
        let rc = unsafe { libc::fchown(file.as_raw_fd(), uid, gid) };
        if rc != 0 {
            return Err(errno());
        }
    }

    let meta = file.metadata().map_err(io_errno)?;
    Ok(metadata_to_stat(&meta))
}

pub fn ensure_access_bits(
    meta: &fs::Metadata,
    uid: u32,
    gid: u32,
    required: u32,
) -> Result<(), i32> {
    if uid == 0 {
        if required & 0o1 != 0 && meta.mode() & 0o111 == 0 {
            return Err(libc::EACCES);
        }
        return Ok(());
    }
    let mode = meta.mode();
    let class_bits = if meta.uid() == uid {
        (mode >> 6) & 0o7
    } else if meta.gid() == gid {
        (mode >> 3) & 0o7
    } else {
        mode & 0o7
    };
    if class_bits & required == required {
        Ok(())
    } else {
        Err(libc::EACCES)
    }
}

pub fn ensure_stat_access_bits(stat: &Stat, uid: u32, gid: u32, required: u32) -> Result<(), i32> {
    if uid == 0 {
        if required & 0o1 != 0 && stat.mode & 0o111 == 0 {
            return Err(libc::EACCES);
        }
        return Ok(());
    }
    let class_bits = if stat.uid == uid {
        (stat.mode >> 6) & 0o7
    } else if stat.gid == gid {
        (stat.mode >> 3) & 0o7
    } else {
        stat.mode & 0o7
    };
    if class_bits & required == required {
        Ok(())
    } else {
        Err(libc::EACCES)
    }
}

pub fn ensure_write_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o2)
}

pub fn ensure_read_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o4)
}

pub fn ensure_read_write_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o6)
}

pub fn ensure_search_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o1)
}

pub fn open_access_bits(flags: i32) -> Result<u32, i32> {
    let mut required = match flags & libc::O_ACCMODE {
        libc::O_RDONLY => 0o4,
        libc::O_WRONLY => 0o2,
        libc::O_RDWR => 0o6,
        _ => return Err(libc::EINVAL),
    };
    if flags & libc::O_TRUNC != 0 {
        required |= 0o2;
    }
    Ok(required)
}

pub fn ensure_open_flags_allowed(
    meta: &fs::Metadata,
    uid: u32,
    gid: u32,
    flags: i32,
) -> Result<u32, i32> {
    let required = open_access_bits(flags)?;
    ensure_access_bits(meta, uid, gid, required)?;
    Ok(required)
}

pub fn ensure_regular_file(meta: &fs::Metadata) -> Result<(), i32> {
    if meta.file_type().is_file() {
        Ok(())
    } else if meta.file_type().is_dir() {
        Err(libc::EISDIR)
    } else {
        Err(libc::EINVAL)
    }
}

pub fn ensure_owner_or_root(meta: &fs::Metadata, uid: u32) -> Result<(), i32> {
    if uid == 0 || meta.uid() == uid {
        Ok(())
    } else {
        Err(libc::EACCES)
    }
}

pub fn ensure_root(uid: u32) -> Result<(), i32> {
    if uid == 0 {
        Ok(())
    } else {
        Err(libc::EACCES)
    }
}

pub fn ensure_setattr_allowed(
    meta: &fs::Metadata,
    uid: u32,
    gid: u32,
    mode: Option<u32>,
    new_uid: Option<u32>,
    new_gid: Option<u32>,
    size: Option<u64>,
) -> Result<(), i32> {
    if mode.is_some() {
        ensure_owner_or_root(meta, uid)?;
    }
    if new_uid.is_some() || new_gid.is_some() {
        ensure_setattr_ownership_allowed(meta, uid, gid, new_uid, new_gid)?;
    }
    if size.is_some() {
        ensure_write_allowed(meta, uid, gid)?;
    }
    Ok(())
}

fn ensure_setattr_ownership_allowed(
    meta: &fs::Metadata,
    uid: u32,
    gid: u32,
    new_uid: Option<u32>,
    new_gid: Option<u32>,
) -> Result<(), i32> {
    if uid == 0 {
        return Ok(());
    }
    if meta.uid() != uid {
        return Err(libc::EACCES);
    }

    let uid_ok = match new_uid {
        Some(requested) => requested == meta.uid(),
        None => true,
    };
    let gid_ok = match new_gid {
        Some(requested) => requested == meta.gid() || requested == gid,
        None => true,
    };

    if uid_ok && gid_ok {
        Ok(())
    } else {
        Err(libc::EACCES)
    }
}

pub fn ensure_dir_creation_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o3)
}

pub fn ensure_file_creation_allowed(meta: &fs::Metadata, uid: u32, gid: u32) -> Result<(), i32> {
    ensure_access_bits(meta, uid, gid, 0o3)
}

pub fn ensure_removal_allowed(
    parent_meta: &fs::Metadata,
    target_meta: &fs::Metadata,
    uid: u32,
    gid: u32,
) -> Result<(), i32> {
    ensure_dir_creation_allowed(parent_meta, uid, gid)?;
    ensure_sticky_allowed(parent_meta, target_meta, uid)
}

pub fn ensure_sticky_allowed(
    parent_meta: &fs::Metadata,
    target_meta: &fs::Metadata,
    uid: u32,
) -> Result<(), i32> {
    if parent_meta.mode() & libc::S_ISVTX != 0
        && uid != 0
        && uid != parent_meta.uid()
        && uid != target_meta.uid()
    {
        return Err(libc::EACCES);
    }
    Ok(())
}

pub fn ensure_not_symlink(path: &Path) -> Result<(), i32> {
    let meta = fs::symlink_metadata(path).map_err(io_errno)?;
    if meta.file_type().is_symlink() {
        return Err(libc::ELOOP);
    }
    Ok(())
}

pub fn apply_ownership(path: &Path, uid: u32, gid: u32, follow_symlink: bool) -> Result<(), i32> {
    let cstr = path_to_cstring(path)?;
    let rc = unsafe {
        if follow_symlink {
            libc::chown(cstr.as_ptr(), uid, gid)
        } else {
            libc::lchown(cstr.as_ptr(), uid, gid)
        }
    };
    if rc != 0 {
        return Err(errno());
    }
    Ok(())
}

pub fn current_process_umask() -> u32 {
    let current = unsafe { libc::umask(0) };
    unsafe {
        libc::umask(current);
    }
    (current & 0o777) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_preserving_setattr_ids_do_not_require_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("owned.txt");
        fs::write(&path, b"owned").expect("seed file");
        let meta = fs::metadata(&path).expect("metadata");

        ensure_setattr_allowed(
            &meta,
            meta.uid(),
            meta.gid(),
            None,
            Some(meta.uid()),
            Some(meta.gid()),
            None,
        )
        .expect("owner preserving chown/chgrp is allowed");
    }

    #[test]
    fn owner_can_set_group_to_current_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("owned.txt");
        fs::write(&path, b"owned").expect("seed file");
        let meta = fs::metadata(&path).expect("metadata");
        let requested_gid = meta.gid().saturating_add(1);

        ensure_setattr_allowed(
            &meta,
            meta.uid(),
            requested_gid,
            None,
            None,
            Some(requested_gid),
            None,
        )
        .expect("owner may request their current group");
    }

    #[test]
    fn non_owner_cannot_request_owner_preserving_setattr_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("owned.txt");
        fs::write(&path, b"owned").expect("seed file");
        let meta = fs::metadata(&path).expect("metadata");
        let other_uid = meta.uid().saturating_add(1);

        assert_eq!(
            ensure_setattr_allowed(
                &meta,
                other_uid,
                meta.gid(),
                None,
                Some(meta.uid()),
                Some(meta.gid()),
                None,
            ),
            Err(libc::EACCES)
        );
    }
}
