use std::ffi::CString;
use std::fs;
use std::path::{Component, Path};

use super::errors::io_errno;
use super::FuseContext;

pub fn normalize_path(path: &str) -> Result<String, i32> {
    let mut parts = Vec::new();
    let p = Path::new(if path.is_empty() { "/" } else { path });
    for comp in p.components() {
        match comp {
            Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = parts.pop();
            }
            Component::Normal(seg) => {
                parts.push(seg.to_string_lossy().to_string());
            }
            _ => {}
        }
    }
    let normalized = if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    };
    Ok(normalized)
}

pub fn strip_root(path: &str) -> &str {
    path.trim_start_matches('/').trim_end_matches('/')
}

pub fn append_rel(base: &str, name: &str) -> String {
    if base == "/" {
        format!("/{}", name)
    } else {
        format!("{}/{}", base.trim_end_matches('/'), name)
    }
}

pub fn ensure_parent_search_allowed<F>(
    rel: &str,
    mut ensure_dir_search_allowed: F,
) -> Result<(), i32>
where
    F: FnMut(&str) -> Result<(), i32>,
{
    let parent = parent_rel(rel);
    let mut current = "/".to_string();
    ensure_dir_search_allowed(&current)?;

    for component in strip_root(&parent)
        .split('/')
        .filter(|part| !part.is_empty())
    {
        current = append_rel(&current, component);
        ensure_dir_search_allowed(&current)?;
    }

    Ok(())
}

pub fn dir_is_empty(path: &Path) -> Result<bool, i32> {
    let mut entries = fs::read_dir(path).map_err(io_errno)?;
    Ok(entries.next().is_none())
}

pub fn path_to_cstring(path: &Path) -> Result<CString, i32> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| libc::EIO)
}

pub fn require_context(ctx: Option<FuseContext>) -> Result<FuseContext, i32> {
    ctx.ok_or(libc::EINVAL)
}

pub fn parent_rel(rel: &str) -> String {
    let path = Path::new(rel);
    path.parent()
        .map(|p| {
            let s = p.to_string_lossy();
            if s.is_empty() {
                "/".to_string()
            } else {
                s.to_string()
            }
        })
        .unwrap_or_else(|| "/".to_string())
}

pub fn descendant_suffix<'a>(path: &'a str, ancestor: &str) -> Option<&'a str> {
    if path == ancestor {
        return Some("");
    }
    if ancestor == "/" {
        return path.starts_with('/').then_some(path);
    }

    let suffix = path.strip_prefix(ancestor)?;
    if suffix.starts_with('/') {
        Some(suffix)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descendant_suffix_requires_exact_or_path_boundary_match() {
        assert_eq!(descendant_suffix("/foo", "/foo"), Some(""));
        assert_eq!(descendant_suffix("/foo/bar", "/foo"), Some("/bar"));
        assert_eq!(descendant_suffix("/foobar", "/foo"), None);
        assert_eq!(descendant_suffix("/foo/bar", "/"), Some("/foo/bar"));
    }
}
