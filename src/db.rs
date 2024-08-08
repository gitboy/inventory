use rusqlite::{params, Connection, Result};

pub fn connect(database_path: &str) -> Result<Connection> {
    let conn = Connection::open(database_path)?;
    Ok(conn)
}

pub fn create_tables(conn: &Connection) -> Result<()> {
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

    Ok(())
}

pub fn insert_or_get_filesystem(conn: &Connection, fs_type: &str) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT id FROM filesystems WHERE fs_type = ?1")?;
    let mut rows = stmt.query([fs_type])?;
    if let Some(row) = rows.next()? {
        Ok(row.get(0)?)
    } else {
        conn.execute("INSERT INTO filesystems (fs_type) VALUES (?1)", [fs_type])?;
        Ok(conn.last_insert_rowid())
    }
}

pub fn insert_file(
    conn: &Connection,
    filesystem_id: i64,
    name: &str,
    path: &str,
    size: u64,
    modified: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO files (filesystem_id, name, path, size, modified) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![filesystem_id, name, path, size, modified],
    )?;
    Ok(())
}