use crate::db;
use rusqlite::Connection;
use rusqlite::Result;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

/// Processes the entire directory and delegates file handling to `process_file`
pub fn process_directory(conn: &Connection, directory: &str) -> Result<()> {
    for entry in WalkDir::new(directory).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            process_file(conn, path)?;
        }
    }
    Ok(())
}

/// Processes a single file, extracts metadata and inserts it into the database
fn process_file(conn: &Connection, path: &std::path::Path) -> Result<()> {
    let metadata = fs::metadata(path).expect("Failed to get metadata");
    let fs_type = get_filesystem_type(&metadata);
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

fn get_filesystem_type(metadata: &fs::Metadata) -> String {
    let dev = metadata.dev();
    format!("FS_TYPE_FOR_DEV_{}", dev)
}
