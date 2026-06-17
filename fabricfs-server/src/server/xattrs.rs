use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;

use super::errors::errno;
use super::paths::path_to_cstring;

const ACL_NAMES: [&str; 2] = ["system.posix_acl_access", "system.posix_acl_default"];

pub fn is_acl_name(name: &str) -> bool {
    ACL_NAMES.contains(&name)
}

pub fn copy_acl_xattrs(
    src: &Path,
    dst: &Path,
    follow_src: bool,
    follow_dst: bool,
) -> Result<(), i32> {
    for name in ACL_NAMES {
        if let Some(value) = read_xattr(src, name, follow_src)? {
            write_xattr(dst, name, &value, follow_dst)?;
        }
    }
    Ok(())
}

pub fn read_xattr(path: &Path, name: &str, follow: bool) -> Result<Option<Vec<u8>>, i32> {
    let p = path_to_cstring(path)?;
    let n = CString::new(name).map_err(|_| libc::EIO)?;
    let size = unsafe {
        if follow {
            libc::getxattr(p.as_ptr(), n.as_ptr(), std::ptr::null_mut(), 0)
        } else {
            libc::lgetxattr(p.as_ptr(), n.as_ptr(), std::ptr::null_mut(), 0)
        }
    };
    if size < 0 {
        let err = errno();
        if is_missing_xattr(err) || is_xattr_unsupported(err) {
            return Ok(None);
        }
        return Err(err);
    }

    let mut buf = vec![0u8; size as usize];
    let read = unsafe {
        if follow {
            libc::getxattr(
                p.as_ptr(),
                n.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        } else {
            libc::lgetxattr(
                p.as_ptr(),
                n.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        }
    };
    if read < 0 {
        let err = errno();
        if is_missing_xattr(err) || is_xattr_unsupported(err) {
            return Ok(None);
        }
        return Err(err);
    }
    buf.truncate(read as usize);
    Ok(Some(buf))
}

pub fn write_xattr(path: &Path, name: &str, value: &[u8], follow: bool) -> Result<(), i32> {
    let p = path_to_cstring(path)?;
    let n = CString::new(name).map_err(|_| libc::EIO)?;
    let rc = unsafe {
        if follow {
            libc::setxattr(
                p.as_ptr(),
                n.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                0,
            )
        } else {
            libc::lsetxattr(
                p.as_ptr(),
                n.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                0,
            )
        }
    };
    if rc != 0 {
        let err = errno();
        if is_xattr_unsupported(err) {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

pub fn list_xattr_names(path: &Path, follow: bool) -> Result<Vec<String>, i32> {
    let p = path_to_cstring(path)?;
    let size = unsafe {
        if follow {
            libc::listxattr(p.as_ptr(), std::ptr::null_mut(), 0)
        } else {
            libc::llistxattr(p.as_ptr(), std::ptr::null_mut(), 0)
        }
    };
    if size < 0 {
        let err = errno();
        if is_xattr_unsupported(err) {
            return Ok(Vec::new());
        }
        return Err(err);
    }
    if size == 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; size as usize];
    let read = unsafe {
        if follow {
            libc::listxattr(p.as_ptr(), buf.as_mut_ptr() as *mut libc::c_char, buf.len())
        } else {
            libc::llistxattr(p.as_ptr(), buf.as_mut_ptr() as *mut libc::c_char, buf.len())
        }
    };
    if read < 0 {
        let err = errno();
        if is_xattr_unsupported(err) {
            return Ok(Vec::new());
        }
        return Err(err);
    }
    let mut names = Vec::new();
    let mut start = 0usize;
    for i in 0..buf.len() {
        if buf[i] == 0 {
            if i > start {
                if let Ok(name) = std::str::from_utf8(&buf[start..i]) {
                    names.push(name.to_string());
                }
            }
            start = i + 1;
        }
    }
    Ok(names)
}

pub fn read_all_xattrs(path: &Path, follow: bool) -> Result<HashMap<String, Vec<u8>>, i32> {
    let mut out = HashMap::new();
    for name in list_xattr_names(path, follow)? {
        if is_acl_name(&name) {
            continue;
        }
        if let Some(value) = read_xattr(path, &name, follow)? {
            out.insert(name, value);
        }
    }
    Ok(out)
}

#[cfg(target_os = "linux")]
fn is_missing_xattr(err: i32) -> bool {
    err == libc::ENODATA
}

#[cfg(not(target_os = "linux"))]
fn is_missing_xattr(err: i32) -> bool {
    err == libc::ENODATA || err == libc::ENOATTR
}

fn is_xattr_unsupported(err: i32) -> bool {
    err == libc::ENOTSUP || err == libc::EOPNOTSUPP
}
