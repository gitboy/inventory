mod db;
mod fs_ops;
mod history;

use std::error::Error;
use std::path::Path;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn usage() {
    println!(
        "inventory {VERSION}

Scan a directory tree into a SQLite inventory that keeps history: every run
records which paths appeared, disappeared or changed, and a path that is
removed and recreated gets a new episode rather than overwriting its past.

Usage:
  inventory <directory_path> <database_path> [options]
  inventory --migrate <directory_path> <database_path>
  inventory --history <path> <database_path>
  inventory --runs <database_path>

Options:
  --exclude <pattern>      skip paths matching pattern; repeatable and persisted
                           per root. Matches a
                           full path, a prefix, or a single path component
                           (e.g. --exclude node_modules --exclude .mypy_cache)
  --suspect-ratio <float>  refuse to close episodes when a run sees less than
                           this fraction of the previous run's files (default 0.9)
  --prune-before <days>    delete change events attached to runs older than N days
                           (episodes and runs are always kept)

Exit status is non-zero on error; a suspect run still exits 0 and is labelled."
    );
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut positional: Vec<String> = Vec::new();
    let mut excludes: Vec<String> = Vec::new();
    let mut suspect_ratio = 0.9f64;
    let mut prune_days: Option<u32> = None;
    let mut mode = "scan";

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--migrate" => mode = "migrate",
            "--history" => mode = "history",
            "--runs" => mode = "runs",
            "-h" | "--help" => {
                usage();
                return Ok(());
            }
            "--version" => {
                println!("inventory {VERSION}");
                return Ok(());
            }
            "--exclude" => {
                i += 1;
                let p = args.get(i).ok_or("--exclude needs a pattern")?;
                excludes.push(p.clone());
            }
            "--suspect-ratio" => {
                i += 1;
                let v = args.get(i).ok_or("--suspect-ratio needs a number")?;
                suspect_ratio = v.parse().map_err(|_| format!("bad ratio: {v}"))?;
                if !(0.1..=1.0).contains(&suspect_ratio) {
                    return Err("--suspect-ratio must be between 0.1 and 1.0".into());
                }
            }
            "--prune-before" => {
                i += 1;
                let v = args.get(i).ok_or("--prune-before needs a number of days")?;
                prune_days = Some(v.parse().map_err(|_| format!("bad day count: {v}"))?);
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown option {other}").into())
            }
            other => positional.push(other.to_string()),
        }
        i += 1;
    }

    match mode {
        "runs" => {
            let db_path = positional.first().ok_or("--runs needs a database path")?;
            let conn = db::connect(db_path)?;
            print_runs(&conn)?;
        }
        "history" => {
            if positional.len() != 2 {
                usage();
                return Err("--history needs <path> <database_path>".into());
            }
            let conn = db::connect(&positional[1])?;
            history::print_history(&conn, &positional[0])?;
        }
        "migrate" => {
            if positional.len() != 2 {
                usage();
                return Err("--migrate needs <directory_path> <database_path>".into());
            }
            let directory = &positional[0];
            let db_path = &positional[1];
            if !Path::new(directory).is_dir() {
                return Err(format!("not a directory: {directory}").into());
            }
            let conn = db::connect(db_path)?;
            if !db::is_legacy(&conn)? {
                return Err("database is not a v1 schema (no `files` table to migrate)".into());
            }
            let root = std::fs::canonicalize(directory)?;
            let root = root.to_string_lossy().to_string();
            let fs_type = fs_ops::filesystem_type(&root);
            let m = db::migrate_v1(&conn, &root, &fs_type, history::now())?;
            println!(
                "migrated: {} paths became baseline run {} for {}",
                m.paths, m.run_id, root
            );
        }
        _ => {
            if positional.len() != 2 {
                usage();
                return Err("need <directory_path> <database_path>".into());
            }
            let directory = &positional[0];
            let db_path = &positional[1];
            if !Path::new(directory).is_dir() {
                return Err(format!("not a directory: {directory}").into());
            }

            let conn = db::connect(db_path)?;
            let found = db::schema_version(&conn)?;
            if found > db::SCHEMA_VERSION {
                return Err(format!(
                    "{db_path} was written by a newer schema (v{found}, this build understands v{})",
                    db::SCHEMA_VERSION
                )
                .into());
            }
            if db::is_legacy(&conn)? {
                return Err(format!(
                    "{db_path} is a v1 database; run `inventory --migrate <dir> <db>` first"
                )
                .into());
            }
            db::create_tables(&conn)?;
            db::create_current_view(&conn)?;
            db::set_schema_version(&conn)?;

            let root = std::fs::canonicalize(directory)?;
            let root = root.to_string_lossy().to_string();
            let fs_type = fs_ops::filesystem_type(&root);
            let root_id = db::ensure_root(&conn, &root, &fs_type)?;
            db::add_excludes(&conn, root_id, &excludes)?;
            let active_excludes = db::load_excludes(&conn, root_id)?;

            let st = history::scan(
                &conn,
                directory,
                root_id,
                &active_excludes,
                suspect_ratio,
                prune_days,
                VERSION,
            )?;

            println!(
                "run {}: {} seen, {} added, {} removed, {} modified [{}]",
                st.run_id, st.seen, st.added, st.removed, st.modified, st.status
            );
            if st.status == "suspect" {
                eprintln!(
                    "warning: only {} files seen vs a previous run against {:.0}% of which \
                     episodes are closed; nothing was marked removed. Re-run once the tree \
                     is complete, or lower --suspect-ratio if the drop is real.",
                    st.seen,
                    suspect_ratio * 100.0
                );
            }
            if st.pruned > 0 {
                println!("pruned {} change events older than the retention window", st.pruned);
            }
        }
    }

    Ok(())
}

fn print_runs(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT r.id,
                r.root_id,
                datetime(r.started_at, 'unixepoch', 'localtime'),
                r.status,
                r.seen, r.added, r.removed, r.modified
           FROM runs r
          ORDER BY r.id",
    )?;
    println!(
        "{:>5}  {:<6} {:<20} {:<8} {:>7} {:>6} {:>8} {:>9}",
        "run", "root", "started", "status", "seen", "added", "removed", "modified"
    );
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let root_id: i64 = row.get(1)?;
        let started: String = row.get(2)?;
        let status: String = row.get(3)?;
        let seen: i64 = row.get(4)?;
        let added: i64 = row.get(5)?;
        let removed: i64 = row.get(6)?;
        let modified: i64 = row.get(7)?;
        println!(
            "{:>5}  {:<6} {:<20} {:<8} {:>7} {:>6} {:>8} {:>9}",
            id, root_id, started, status, seen, added, removed, modified
        );
    }
    Ok(())
}
