use crate::db;
use rusqlite::Connection;
use rusqlite::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

/// Walks `directory` and records every regular file. Returns the row count.
pub fn process_directory(conn: &Connection, directory: &str) -> Result<u64> {
    let root = Path::new(directory);
    let mut indexed = 0u64;

    for entry in WalkDir::new(directory).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if process_file(conn, path, root)? {
            indexed += 1;
        }
    }

    Ok(indexed)
}

/// Normalises a path to `./relative/to/root` so that scanning `.` and scanning
/// an absolute directory produce identical rows for the same file.
fn normalize(path: &Path, root: &Path) -> Option<String> {
    let rel: PathBuf = path.strip_prefix(root).unwrap_or(path).to_path_buf();
    let rel = rel.to_str()?;
    if rel.is_empty() || rel == "." {
        return None;
    }
    Some(format!("./{}", rel.trim_start_matches("./")))
}

/// Handles a single file: metadata lookup and insert. Returns true if a row was written.
fn process_file(conn: &Connection, path: &Path, root: &Path) -> Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        // a file removed between the walk and the stat is not an error
        Err(_) => return Ok(false),
    };

    #[cfg(unix)]
    let fs_type = get_filesystem_type_unix(&metadata);

    #[cfg(windows)]
    let fs_type = get_filesystem_type_windows(&metadata);

    let filesystem_id = db::insert_or_get_filesystem(conn, &fs_type)?;
    let file_size = metadata.len();

    let modified_time = match metadata.modified() {
        Ok(t) => t,
        Err(_) => return Ok(false),
    };
    let modified_secs = match modified_time.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        // pre-1970 timestamps: record as zero rather than aborting the walk
        Err(_) => 0,
    };

    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => {
            println!("File name is not valid Unicode: {:?}", path);
            return Ok(false);
        }
    };

    let path_str = match normalize(path, root) {
        Some(p) => p,
        None => {
            println!("Path is not valid Unicode: {:?}", path);
            return Ok(false);
        }
    };

    db::insert_file(
        conn,
        filesystem_id,
        name,
        &path_str,
        file_size,
        modified_secs,
    )?;

    Ok(true)
}

/// Determines the filesystem type in a Unix-specific manner
#[cfg(unix)]
fn get_filesystem_type_unix(metadata: &fs::Metadata) -> String {
    let dev = metadata.dev();
    format!("FS_TYPE_FOR_DEV_{}", dev)
}

/// Determines the filesystem type in a Windows-specific manner
#[cfg(windows)]
fn get_filesystem_type_windows(metadata: &fs::Metadata) -> String {
    let file_index = metadata.file_index();
    format!("FS_TYPE_FOR_FILE_INDEX_{:?}", file_index)
}
