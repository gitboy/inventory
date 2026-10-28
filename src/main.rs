mod db;
mod fs_ops;

use rusqlite::Result;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <directory_path> <database_path>", args[0]);
        std::process::exit(1);
    }

    let directory = &args[1];
    let database_path = &args[2];

    let conn = db::connect(database_path)?;
    db::create_tables(&conn)?;

    // One transaction for the whole walk: without it every INSERT is its own
    // autocommit and a 13k-file tree takes minutes instead of seconds.
    let tx = conn.unchecked_transaction()?;

    // Rebuild semantics: this run is the complete picture of the tree, so
    // stale rows from a previous run must not survive.
    let purged = db::purge_files(&tx)?;

    let indexed = fs_ops::process_directory(&tx, directory)?;

    tx.commit()?;

    eprintln!(
        "indexed {} files from {} into {} (dropped {} stale rows)",
        indexed, directory, database_path, purged
    );

    Ok(())
}
