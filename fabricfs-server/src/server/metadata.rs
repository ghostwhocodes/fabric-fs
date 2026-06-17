use std::fs;
use std::os::unix::fs::MetadataExt;

use super::Stat;

pub fn metadata_to_stat(meta: &fs::Metadata) -> Stat {
    let atime_ns = nanos_saturated(meta.atime(), meta.atime_nsec());
    let mtime_ns = nanos_saturated(meta.mtime(), meta.mtime_nsec());
    let ctime_ns = nanos_saturated(meta.ctime(), meta.ctime_nsec());
    Stat {
        dev: meta.dev(),
        ino: meta.ino(),
        mode: meta.mode(),
        nlink: meta.nlink() as u32,
        uid: meta.uid(),
        gid: meta.gid(),
        size: meta.size(),
        atime_ns,
        mtime_ns,
        ctime_ns,
    }
}

fn nanos_saturated(secs: i64, nsecs: i64) -> i64 {
    let raw = (secs as i128)
        .saturating_mul(1_000_000_000i128)
        .saturating_add(nsecs as i128);
    raw.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
