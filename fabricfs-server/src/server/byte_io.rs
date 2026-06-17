use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt};

use super::errors::io_errno;
use super::FsLimits;

pub fn read_file_at(
    file: &mut File,
    offset: i64,
    size: i64,
    limits: &FsLimits,
) -> Result<Vec<u8>, i32> {
    if offset < 0 {
        return Err(libc::EINVAL);
    }
    file.seek(SeekFrom::Start(offset as u64))
        .map_err(io_errno)?;

    let chunk_size = limits.io_chunk_bytes;
    let max_read = limits.max_read_bytes;
    let to_read = size.min(max_read as i64).max(0) as usize;
    let mut buf = vec![0u8; to_read.min(chunk_size)];
    let mut total = Vec::with_capacity(to_read);

    while total.len() < to_read {
        let remaining = to_read - total.len();
        let chunk = remaining.min(chunk_size);
        buf.resize(chunk, 0);
        match file.read(&mut buf[..chunk]) {
            Ok(0) => break,
            Ok(n) => total.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(io_errno(e)),
        }
    }

    Ok(total)
}

pub fn write_file_at(file: &mut File, offset: i64, data: &[u8]) -> Result<usize, i32> {
    if offset < 0 {
        return Err(libc::EINVAL);
    }
    file.seek(SeekFrom::Start(offset as u64))
        .map_err(io_errno)?;

    let written = file.write(data).map_err(io_errno)?;
    file.sync_all().map_err(io_errno)?;
    Ok(written)
}

pub fn copy_file_range_at(
    src_file: &File,
    dst_file: &File,
    offset_in: i64,
    offset_out: i64,
    len: u64,
    limits: &FsLimits,
) -> Result<u64, i32> {
    if offset_in < 0 || offset_out < 0 {
        return Err(libc::EINVAL);
    }
    if len == 0 {
        return Ok(0);
    }

    let base_in = offset_in as u64;
    let base_out = offset_out as u64;
    reject_overlapping_same_file_copy(src_file, dst_file, base_in, base_out, len)?;

    let requested = usize::try_from(len).unwrap_or(usize::MAX);
    let chunk_size = limits.io_chunk_bytes.min(requested).max(1);
    let mut buf = vec![0u8; chunk_size];
    let mut copied = 0u64;

    while copied < len {
        let to_read = ((len - copied).min(buf.len() as u64)) as usize;
        let read_offset = base_in.checked_add(copied).ok_or(libc::EINVAL)?;
        match src_file.read_at(&mut buf[..to_read], read_offset) {
            Ok(0) => break,
            Ok(n) => {
                let mut written = 0usize;
                while written < n {
                    let write_offset = base_out
                        .checked_add(copied)
                        .and_then(|offset| offset.checked_add(written as u64))
                        .ok_or(libc::EINVAL)?;
                    match dst_file.write_at(&buf[written..n], write_offset) {
                        Ok(0) => return Err(libc::EIO),
                        Ok(bytes) => written += bytes,
                        Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(io_errno(err)),
                    }
                }
                copied += n as u64;
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(io_errno(err)),
        }
    }

    Ok(copied)
}

fn reject_overlapping_same_file_copy(
    src_file: &File,
    dst_file: &File,
    offset_in: u64,
    offset_out: u64,
    len: u64,
) -> Result<(), i32> {
    let src_meta = src_file.metadata().map_err(io_errno)?;
    let dst_meta = dst_file.metadata().map_err(io_errno)?;
    if src_meta.dev() != dst_meta.dev() || src_meta.ino() != dst_meta.ino() {
        return Ok(());
    }

    let input_end = offset_in.checked_add(len).ok_or(libc::EINVAL)?;
    let output_end = offset_out.checked_add(len).ok_or(libc::EINVAL)?;
    if offset_in < output_end && offset_out < input_end {
        Err(libc::EINVAL)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn read_file_at_seeks_for_explicit_zero_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("data.txt");
        fs::write(&path, b"abcdef").expect("seed file");
        let mut file = File::open(&path).expect("open file");
        let limits = FsLimits::new(2, 16);

        assert_eq!(
            read_file_at(&mut file, 2, 2, &limits).expect("read at offset 2"),
            b"cd"
        );
        assert_eq!(
            read_file_at(&mut file, 0, 2, &limits).expect("read at offset 0"),
            b"ab"
        );
    }

    #[test]
    fn write_file_at_seeks_for_explicit_zero_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("data.txt");
        fs::write(&path, b"abcdef").expect("seed file");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open file");

        assert_eq!(
            write_file_at(&mut file, 3, b"XY").expect("write at offset 3"),
            2
        );
        assert_eq!(
            write_file_at(&mut file, 0, b"12").expect("write at offset 0"),
            2
        );
        drop(file);

        assert_eq!(fs::read(&path).expect("read file"), b"12cXYf");
    }

    #[test]
    fn file_io_rejects_negative_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("data.txt");
        fs::write(&path, b"abcdef").expect("seed file");
        let limits = FsLimits::default();

        let mut read_file = File::open(&path).expect("open file for read");
        assert_eq!(
            read_file_at(&mut read_file, -1, 1, &limits),
            Err(libc::EINVAL)
        );

        let mut write_file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open file for write");
        assert_eq!(write_file_at(&mut write_file, -1, b"x"), Err(libc::EINVAL));
    }
}
