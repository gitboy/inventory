use crate::db;
use crate::fs_ops;
use rusqlite::{params, Connection, Result};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Below this many previously-seen files the ratio test is meaningless.
const SUSPECT_FLOOR: i64 = 50;

pub struct Stats {
    pub run_id: i64,
    pub seen: u64,
    pub added: u64,
    pub removed: u64,
    pub modified: u64,
    pub status: String,
    pub pruned: usize,
}

struct Live {
    episode_id: i64,
    size: i64,
    modified: i64,
}

#[derive(Default)]
struct Counts {
    seen: u64,
    added: u64,
    removed: u64,
    modified: u64,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One observation of one root: opens episodes for new paths, closes episodes
/// for missing ones, records modifications. Nothing is ever deleted.
pub fn scan(
    conn: &Connection,
    directory: &str,
    root_id: i64,
    excludes: &[String],
    suspect_ratio: f64,
    prune_days: Option<u32>,
    tool_version: &str,
) -> Result<Stats> {
    let started = now();
    let tx = conn.unchecked_transaction()?;
    let run_id = db::begin_run(&tx, root_id, started, tool_version)?;

    let previous_seen: i64 = tx.query_row(
        "SELECT coalesce(
                  (SELECT seen FROM runs
                    WHERE root_id = ?1 AND id < ?2 AND status IN ('ok','suspect')
                    ORDER BY id DESC LIMIT 1),
                  0)",
        params![root_id, run_id],
        |r| r.get(0),
    )?;

    let mut live: HashMap<String, Live> = HashMap::new();
    {
        let mut stmt = tx.prepare(
            "SELECT p.path, e.id, e.size, e.modified
               FROM episodes e
               JOIN paths p ON p.id = e.path_id
              WHERE p.root_id = ?1 AND e.gone_run_id IS NULL",
        )?;
        let mut rows = stmt.query([root_id])?;
        while let Some(row) = rows.next()? {
            live.insert(
                row.get(0)?,
                Live {
                    episode_id: row.get(1)?,
                    size: row.get(2)?,
                    modified: row.get(3)?,
                },
            );
        }
    }

    let mut counts = Counts::default();
    {
        let mut ins_path =
            tx.prepare("INSERT OR IGNORE INTO paths(root_id, path) VALUES (?1, ?2)")?;
        let mut get_path =
            tx.prepare("SELECT id FROM paths WHERE root_id = ?1 AND path = ?2")?;
        let mut next_seq = tx.prepare(
            "SELECT coalesce(max(seq), 0) + 1 FROM episodes WHERE path_id = ?1",
        )?;
        let mut ins_episode = tx.prepare(
            "INSERT INTO episodes(path_id, seq, name, size, modified, inode, first_run_id, last_run_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        )?;
        let mut touch = tx.prepare("UPDATE episodes SET last_run_id = ?1 WHERE id = ?2")?;
        let mut update = tx.prepare(
            "UPDATE episodes SET size = ?1, modified = ?2, inode = ?3, last_run_id = ?4
              WHERE id = ?5",
        )?;
        let mut ins_change = tx.prepare(
            "INSERT INTO changes(run_id, episode_id, kind, size_before, size_after, mtime_before, mtime_after)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        fs_ops::walk(directory, excludes, |entry| {
            counts.seen += 1;
            let size = entry.size as i64;
            let mtime = entry.mtime;

            match live.remove(&entry.path) {
                Some(prev) => {
                    if prev.size != size || prev.modified != mtime {
                        update.execute(params![
                            size,
                            mtime,
                            entry.inode,
                            run_id,
                            prev.episode_id
                        ])?;
                        ins_change.execute(params![
                            run_id,
                            prev.episode_id,
                            "modified",
                            prev.size,
                            size,
                            prev.modified,
                            mtime
                        ])?;
                        counts.modified += 1;
                    } else {
                        touch.execute(params![run_id, prev.episode_id])?;
                    }
                }
                None => {
                    ins_path.execute(params![root_id, entry.path])?;
                    let path_id: i64 =
                        get_path.query_row(params![root_id, entry.path], |r| r.get(0))?;
                    let seq: i64 = next_seq.query_row([path_id], |r| r.get(0))?;
                    ins_episode.execute(params![
                        path_id, seq, entry.name, size, mtime, entry.inode, run_id
                    ])?;
                    let episode_id = tx.last_insert_rowid();
                    ins_change.execute(params![
                        run_id,
                        episode_id,
                        "added",
                        None::<i64>,
                        size,
                        None::<i64>,
                        mtime
                    ])?;
                    counts.added += 1;
                }
            }
            Ok(())
        })?;
    }

    // A run that sees far less than the previous one is almost never a real
    // deletion event: unmounted volume, permission error, wrong directory.
    // Record what was seen, but never close episodes on that evidence. Small
    // trees are exempt — one deletion in a five-file tree is 20%.
    let suspect = previous_seen >= SUSPECT_FLOOR
        && (counts.seen as f64) < suspect_ratio * (previous_seen as f64);

    if !suspect {
        let mut close = tx.prepare(
            "UPDATE episodes SET gone_run_id = ?1 WHERE id = ?2 AND gone_run_id IS NULL",
        )?;
        let mut ins_change = tx.prepare(
            "INSERT INTO changes(run_id, episode_id, kind, size_before, size_after, mtime_before, mtime_after)
             VALUES (?1, ?2, 'removed', ?3, NULL, ?4, NULL)",
        )?;
        for (_, prev) in live.iter() {
            close.execute(params![run_id, prev.episode_id])?;
            ins_change.execute(params![run_id, prev.episode_id, prev.size, prev.modified])?;
            counts.removed += 1;
        }
    }

    let status = if suspect { "suspect" } else { "ok" };
    db::finish_run(
        &tx,
        run_id,
        counts.seen,
        counts.added,
        counts.removed,
        counts.modified,
        status,
        now(),
    )?;

    let mut pruned = 0usize;
    if let Some(days) = prune_days {
        let cutoff = now() - (days as i64) * 86_400;
        pruned = tx.execute(
            "DELETE FROM changes
              WHERE run_id IN (SELECT id FROM runs WHERE started_at < ?1)",
            [cutoff],
        )?;
    }

    tx.commit()?;

    Ok(Stats {
        run_id,
        seen: counts.seen,
        added: counts.added,
        removed: counts.removed,
        modified: counts.modified,
        status: status.to_string(),
        pruned,
    })
}

/// Prints every lifetime of paths matching `needle` (exact, or suffix match).
pub fn print_history(conn: &Connection, needle: &str) -> Result<()> {
    let needle_clean = needle.trim_start_matches("./").trim_start_matches('/');
    let exact = format!("./{}", needle_clean);
    let suffix = format!("%/{}", needle_clean);
    let prefix = format!("./{}/%", needle_clean);

    let mut stmt = conn.prepare(
        "SELECT p.path,
                e.seq,
                datetime(r1.started_at, 'unixepoch', 'localtime'),
                datetime(r2.started_at, 'unixepoch', 'localtime'),
                e.size,
                e.gone_run_id IS NULL
           FROM episodes e
           JOIN paths p ON p.id = e.path_id
           JOIN runs r1 ON r1.id = e.first_run_id
           LEFT JOIN runs r2 ON r2.id = e.gone_run_id
          WHERE p.path = ?1 OR p.path LIKE ?2 OR p.path LIKE ?3
          ORDER BY p.path, e.seq",
    )?;

    let mut rows = stmt.query(params![exact, suffix, prefix])?;
    let mut found = 0;
    while let Some(row) = rows.next()? {
        let path: String = row.get(0)?;
        let seq: i64 = row.get(1)?;
        let appeared: String = row.get(2)?;
        let disappeared: Option<String> = row.get(3)?;
        let size: i64 = row.get(4)?;
        let present: bool = row.get::<_, i64>(5)? != 0;

        println!(
            "{:<60} #{}  {:>10} B  {} -> {}",
            path,
            seq,
            size,
            appeared,
            match disappeared {
                Some(t) => t,
                None if present => "present".to_string(),
                None => "?".to_string(),
            }
        );
        found += 1;
    }

    if found == 0 {
        println!("no episodes match {:?}", needle);
    }
    Ok(())
}
