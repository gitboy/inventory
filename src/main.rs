use rusqlite::Result;

mod db;
mod fs_ops;

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
    fs_ops::process_directory(&conn, directory)?;
    Ok(())
}
