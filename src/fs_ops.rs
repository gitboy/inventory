use crate::db;
use rusqlite::Connection;
use rusqlite::Result;
use std::fs;
use std::time::UNIX_EPOCH;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

/// Processes a single file, extracts metadata and inserts it into the database
fn process_file(conn: &Connection, path: &std::path::Path) -> Result<()> {
    let metadata = fs::metadata(path).expect("Failed to get metadata");

    #[cfg(unix)]
    let fs_type = get_filesystem_type_unix(&metadata);

    #[cfg(windows)]
    let fs_type = get_filesystem_type_windows(&metadata);

    let filesystem_id = db::insert_or_get_filesystem(conn, &fs_type)?;
    let file_size = metadata.len();

    // Get the absolute modified time
    let modified_time = metadata.modified().expect("Failed to get modified time");
    let duration_since_epoch = modified_time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let modified_secs = duration_since_epoch.as_secs();

    if let Some(filename) = path.file_name() {
        if let Some(name) = filename.to_str() {
            if let Some(path_str) = path.to_str() {
                insert_file_metadata(
                    conn,
                    filesystem_id,
                    name,
                    path_str,
                    file_size,
                    modified_secs,
                )?;
            } else {
                println!("Path is not valid Unicode: {:?}", path);
            }
        } else {
            println!(
                "File name is not valid Unicode: path: {:?}",
                path.file_name()
            );
        }
    } else {
        println!("No file name present in the path: {:?}", path);
    }

    Ok(())
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

/// Inserts the file metadata into the database
fn insert_file_metadata(
    conn: &Connection,
    filesystem_id: i64,
    name: &str,
    path: &str,
    size: u64,
    modified: u64,
) -> Result<()> {
    db::insert_file(conn, filesystem_id, name, path, size, modified)?;
    Ok(())
}
