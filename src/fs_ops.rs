use rusqlite::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

/// One regular file observed during a walk.
pub struct Entry {
    /// `./relative/to/root`, matching the v1 convention.
    pub path: String,
    pub name: String,
    pub size: u64,
    pub mtime: i64,
    pub inode: i64,
}

/// Walks `root` and hands every regular file to `cb`. Excluded paths are skipped
/// before the metadata call, so a pruned `node_modules` costs nothing.
pub fn walk<F>(root: &str, excludes: &[String], mut cb: F) -> Result<u64>
where
    F: FnMut(Entry) -> Result<()>,
{
    let root_path = Path::new(root);
    let mut seen = 0u64;

    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let rel = match normalize(path, root_path) {
            Some(r) => r,
            None => continue,
        };
        if is_excluded(&rel, excludes) {
            continue;
        }

        // metadata follows symlinks, matching the previous behaviour
        let md = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let mtime = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        cb(Entry {
            path: rel,
            name,
            size: md.len(),
            mtime,
            inode: inode_of(&md),
        })?;
        seen += 1;
    }

    Ok(seen)
}

/// Normalises to `./relative` so scanning `.` and an absolute directory agree.
fn normalize(path: &Path, root: &Path) -> Option<String> {
    let rel: PathBuf = path.strip_prefix(root).unwrap_or(path).to_path_buf();
    let rel = rel.to_str()?;
    if rel.is_empty() || rel == "." {
        return None;
    }
    Some(format!("./{}", rel.trim_start_matches("./")))
}

/// A pattern matches if it equals the path, prefixes it, or names any single
/// path component. Enough for `node_modules`, `.mypy_cache`, `./Library/Caches`.
fn is_excluded(rel: &str, excludes: &[String]) -> bool {
    if excludes.is_empty() {
        return false;
    }
    let trimmed = rel.trim_start_matches("./");
    let components: Vec<&str> = trimmed.split('/').collect();

    excludes.iter().any(|raw| {
        let pat = raw.trim().trim_matches('/').trim_start_matches("./");
        !pat.is_empty()
            && (trimmed == pat
                || trimmed.starts_with(&format!("{}/", pat))
                || components.contains(&pat))
    })
}

#[cfg(unix)]
fn inode_of(md: &fs::Metadata) -> i64 {
    md.ino() as i64
}

#[cfg(not(unix))]
fn inode_of(_md: &fs::Metadata) -> i64 {
    0
}

/// Filesystem marker used to identify the volume a root lives on.
pub fn filesystem_type(root: &str) -> String {
    match fs::metadata(root) {
        #[cfg(unix)]
        Ok(md) => format!("FS_TYPE_FOR_DEV_{}", md.dev()),
        #[cfg(not(unix))]
        Ok(_) => "FS_TYPE_UNKNOWN".to_string(),
        Err(_) => "FS_TYPE_UNKNOWN".to_string(),
    }
}
