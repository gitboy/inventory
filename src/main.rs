use rusqlite::{params, Connection, Result};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <directory_path> <database_path>", args[0]);
        std::process::exit(1);
    }

    let directory = &args[1];
    let database_path = &args[2];
    let conn = Connection::open(database_path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS filesystems (
            id INTEGER PRIMARY KEY,
            fs_type TEXT NOT NULL
         )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            filesystem_id INTEGER,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            FOREIGN KEY(filesystem_id) REFERENCES filesystems(id)
         )",
        [],
    )?;

    for entry in WalkDir::new(directory).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            let metadata = fs::metadata(path).expect("Failed to get metadata");
            let fs_type = get_filesystem_type(&metadata);
            let filesystem_id =
                insert_or_get_filesystem(&conn, &fs_type).expect("Failed to get filesystem ID");
            let file_size = metadata.len();

            // Get the absolute modified time
            let modified_time = metadata.modified().expect("Failed to get modified time");
            let duration_since_epoch = modified_time
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let modified_secs = duration_since_epoch.as_secs();
            let modified = modified_secs;

            // conn.execute(
            //     "INSERT INTO files (filesystem_id, name, path, size, modified) VALUES (?1, ?2, ?3, ?4, ?5)",
            //     params![filesystem_id, path.file_name().unwrap().to_str(), path.to_str(), file_size, modified],
            // )?;

            match path.file_name() {
                Some(filename) => {
                    // Attempt to convert OsStr to a &str
                    match filename.to_str() {
                        Some(name) => {
                            // Check if path is valid Unicode
                            if let Some(path_str) = path.to_str() {
                                // Execute the SQL command
                                conn.execute(
                                    "INSERT INTO files (filesystem_id, name, path, size, modified) VALUES (?1, ?2, ?3, ?4, ?5)",
                                    params![filesystem_id, name, path_str, file_size, modified],
                                )?;
                            } else {
                                println!("Path is not valid Unicode: {:?}", path);
                            }
                        }
                        None => println!(
                            "File name is not valid Unicode: path: {:?}",
                            path.file_name()
                        ),
                    }
                }
                None => println!("No file name present in the path: {:?}", path),
            }
        }
    }

    Ok(())
}

fn get_filesystem_type(metadata: &fs::Metadata) -> String {
    let dev = metadata.dev();
    // This is a placeholder for getting the file system type.
    // You may need to use system-specific methods to find the file system type from `dev`.
    format!("FS_TYPE_FOR_DEV_{}", dev)
}

fn insert_or_get_filesystem(conn: &Connection, fs_type: &str) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT id FROM filesystems WHERE fs_type = ?1")?;
    let mut rows = stmt.query([fs_type])?;

    if let Some(row) = rows.next()? {
        Ok(row.get(0)?)
    } else {
        conn.execute("INSERT INTO filesystems (fs_type) VALUES (?1)", [fs_type])?;
        Ok(conn.last_insert_rowid())
    }
}
