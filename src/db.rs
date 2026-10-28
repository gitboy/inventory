use rusqlite::{params, Connection, Result};

pub const SCHEMA_VERSION: i64 = 2;

pub fn connect(database_path: &str) -> Result<Connection> {
    let conn = Connection::open(database_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA synchronous = NORMAL;",
    )?;
    Ok(conn)
}

pub fn schema_version(conn: &Connection) -> Result<i64> {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
}

pub fn set_schema_version(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION))
}

/// True when this database still holds the v1 single-snapshot table.
pub fn is_legacy(conn: &Connection) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'files'",
        [],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

pub fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS roots (
            id      INTEGER PRIMARY KEY,
            root    TEXT NOT NULL UNIQUE,
            fs_type TEXT NOT NULL
         );

         CREATE TABLE IF NOT EXISTS runs (
            id           INTEGER PRIMARY KEY,
            root_id      INTEGER NOT NULL REFERENCES roots(id),
            started_at   INTEGER NOT NULL,
            finished_at  INTEGER,
            status       TEXT NOT NULL CHECK (status IN ('running','ok','failed','suspect')),
            tool_version TEXT,
            seen         INTEGER NOT NULL DEFAULT 0,
            added        INTEGER NOT NULL DEFAULT 0,
            removed      INTEGER NOT NULL DEFAULT 0,
            modified     INTEGER NOT NULL DEFAULT 0
         );

         CREATE TABLE IF NOT EXISTS paths (
            id      INTEGER PRIMARY KEY,
            root_id INTEGER NOT NULL REFERENCES roots(id),
            path    TEXT NOT NULL,
            UNIQUE (root_id, path)
         );

         CREATE TABLE IF NOT EXISTS episodes (
            id           INTEGER PRIMARY KEY,
            path_id      INTEGER NOT NULL REFERENCES paths(id),
            seq          INTEGER NOT NULL,
            name         TEXT    NOT NULL,
            size         INTEGER NOT NULL,
            modified     INTEGER NOT NULL,
            inode        INTEGER,
            first_run_id INTEGER NOT NULL REFERENCES runs(id),
            last_run_id  INTEGER NOT NULL REFERENCES runs(id),
            gone_run_id  INTEGER          REFERENCES runs(id),
            CHECK (gone_run_id IS NULL OR gone_run_id > last_run_id),
            UNIQUE (path_id, seq)
         );

         CREATE TABLE IF NOT EXISTS changes (            id           INTEGER PRIMARY KEY,
            run_id       INTEGER NOT NULL REFERENCES runs(id),
            episode_id   INTEGER NOT NULL REFERENCES episodes(id),
            kind         TEXT NOT NULL CHECK (kind IN ('added','removed','modified')),
            size_before  INTEGER,
            size_after   INTEGER,
            mtime_before INTEGER,
            mtime_after  INTEGER
         );

         CREATE TABLE IF NOT EXISTS root_excludes (
            root_id INTEGER NOT NULL REFERENCES roots(id),
            pattern TEXT    NOT NULL,
            PRIMARY KEY (root_id, pattern)
         );

         CREATE INDEX IF NOT EXISTS idx_episodes_live  ON episodes(path_id) WHERE gone_run_id IS NULL;         CREATE INDEX IF NOT EXISTS idx_episodes_gone  ON episodes(gone_run_id) WHERE gone_run_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_episodes_first ON episodes(first_run_id);
         CREATE INDEX IF NOT EXISTS idx_changes_run     ON changes(run_id, kind);
         CREATE INDEX IF NOT EXISTS idx_changes_episode ON changes(episode_id);
         CREATE INDEX IF NOT EXISTS idx_runs_root       ON runs(root_id, id);",
    )
}

/// The current state of the tree. Replaces the v1 `files` table.
pub fn create_current_view(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE VIEW IF NOT EXISTS files AS
           SELECT e.id, p.root_id, e.name, p.path, e.size, e.modified
           FROM episodes e
           JOIN paths p ON p.id = e.path_id
           WHERE e.gone_run_id IS NULL;",
    )
}

pub fn ensure_root(conn: &Connection, root: &str, fs_type: &str) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO roots(root, fs_type) VALUES (?1, ?2)",
        params![root, fs_type],
    )?;
    conn.query_row("SELECT id FROM roots WHERE root = ?1", [root], |r| {
        r.get(0)
    })
}

/// Exclusions live in the database, not in the command line, so a forgotten
/// flag cannot flood the timeline with cache churn.
pub fn add_excludes(conn: &Connection, root_id: i64, patterns: &[String]) -> Result<()> {
    for p in patterns {
        conn.execute(
            "INSERT OR IGNORE INTO root_excludes(root_id, pattern) VALUES (?1, ?2)",
            params![root_id, p],
        )?;
    }
    Ok(())
}

pub fn load_excludes(conn: &Connection, root_id: i64) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT pattern FROM root_excludes WHERE root_id = ?1 ORDER BY pattern")?;
    let rows = stmt.query_map([root_id], |r| r.get::<_, String>(0))?;
    rows.collect()
}

pub fn begin_run(conn: &Connection, root_id: i64, started_at: i64, tool_version: &str) -> Result<i64> {
    conn.execute(
        "INSERT INTO runs(root_id, started_at, status, tool_version)
         VALUES (?1, ?2, 'running', ?3)",
        params![root_id, started_at, tool_version],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn finish_run(
    conn: &Connection,
    run_id: i64,
    seen: u64,
    added: u64,
    removed: u64,
    modified: u64,
    status: &str,
    finished_at: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE runs
            SET seen = ?1, added = ?2, removed = ?3, modified = ?4,
                status = ?5, finished_at = ?6
          WHERE id = ?7",
        params![seen as i64, added as i64, removed as i64, modified as i64, status, finished_at, run_id],
    )?;
    Ok(())
}

pub struct Migration {
    pub run_id: i64,
    pub paths: i64,
}

/// Turns a v1 database into a v2 baseline: the existing snapshot becomes run 1,
/// every row an episode with seq = 1, and the old table is kept as files_v1.
pub fn migrate_v1(conn: &Connection, root: &str, fs_type: &str, now: i64) -> Result<Migration> {
    let tx = conn.unchecked_transaction()?;
    create_tables(&tx)?;

    let root_id = ensure_root(&tx, root, fs_type)?;

    let rows: i64 = tx.query_row("SELECT count(*) FROM files", [], |r| r.get(0))?;
    let run_id = begin_run(&tx, root_id, now, "migrate-v1")?;

    tx.execute_batch(&format!(
        "ALTER TABLE files RENAME TO files_v1;
         DROP INDEX IF EXISTS idx_files_path;"
    ))?;
    create_current_view(&tx)?;

    // de-duplicate on path: a database written by v1 could hold the tree twice
    tx.execute(
        "INSERT OR IGNORE INTO paths(root_id, path)
           SELECT DISTINCT ?1, path FROM files_v1",
        params![root_id],
    )?;

    tx.execute(
        "INSERT OR IGNORE INTO episodes(path_id, seq, name, size, modified, first_run_id, last_run_id)
           SELECT p.id, 1, max(f.name), max(f.size), max(f.modified), ?2, ?2
             FROM files_v1 f
             JOIN paths p ON p.path = f.path AND p.root_id = ?1
            GROUP BY p.id",
        params![root_id, run_id],
    )?;

    let paths: i64 = tx.query_row(
        "SELECT count(*) FROM episodes WHERE first_run_id = ?1",
        [run_id],
        |r| r.get(0),
    )?;

    finish_run(&tx, run_id, paths as u64, paths as u64, 0, 0, "ok", now)?;
    tx.execute(
        "UPDATE runs SET seen = ?1, added = ?1 WHERE id = ?2",
        params![rows, run_id],
    )?;
    set_schema_version(&tx)?;
    tx.commit()?;

    Ok(Migration { run_id, paths })
}
