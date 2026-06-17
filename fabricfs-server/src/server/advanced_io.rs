use std::fs::File;
use std::os::unix::io::AsRawFd;

pub fn apply_mode_zero_fallocate(file: &File, offset: i64, length: i64) -> Result<(), i32> {
    checked_fallocate_end(offset, length)?;

    let rc = unsafe {
        libc::posix_fallocate(
            file.as_raw_fd(),
            offset as libc::off_t,
            length as libc::off_t,
        )
    };
    if rc != 0 {
        return Err(rc);
    }
    Ok(())
}

pub fn checked_fallocate_end(offset: i64, length: i64) -> Result<u64, i32> {
    if offset < 0 || length <= 0 {
        return Err(libc::EINVAL);
    }
    let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
    let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
    offset.checked_add(length).ok_or(libc::EINVAL)
}

pub fn ensure_fallocate_mode_supported(mode: i32) -> Result<(), i32> {
    if mode == 0 {
        Ok(())
    } else {
        Err(libc::EOPNOTSUPP)
    }
}

pub fn seek_offset(current: u64, file_len: u64, offset: i64, whence: i32) -> Result<u64, i32> {
    let base = match whence {
        libc::SEEK_SET => 0,
        libc::SEEK_CUR => current,
        libc::SEEK_END => file_len,
        _ => return Err(libc::EINVAL),
    };
    let next = i128::from(base) + i128::from(offset);
    if next < 0 || next > i128::from(i64::MAX) {
        return Err(libc::EINVAL);
    }
    Ok(next as u64)
}
