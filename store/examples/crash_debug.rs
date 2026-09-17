//! Replays a snapshot saved by the crash harness (`--dump-dir`) offline.
//!
//!     cargo run --example crash_debug -- <name>.data <name>.wal.<n>... [--table tN]
//!
//! Opens the data file twice: once with an empty WAL (one header-only
//! segment), showing the checkpoint state recovery started from, and once
//! with the real segments, showing what recovery produced. Prints each
//! table's structure both times and the WAL records in between.

use store::{db::Db, logger, memfile::MemFile};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut only_table = None;
    let mut paths = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--table" {
            only_table = args.get(i + 1).cloned();
            i += 2;
        } else {
            paths.push(args[i].clone());
            i += 1;
        }
    }
    let (data_path, seg_paths) = paths.split_first().expect("usage: crash_debug <data> <wal segments...>");
    let data = std::fs::read(data_path).unwrap();
    // Segments in numeric order, regardless of how the shell sorted them.
    let mut segments: Vec<(u64, Vec<u8>)> = seg_paths
        .iter()
        .map(|p| {
            let n = p.rsplit('.').next().and_then(|s| s.parse().ok()).unwrap_or(0);
            (n, std::fs::read(p).unwrap())
        })
        .collect();
    segments.sort_by_key(|(n, _)| *n);

    println!("===== BEFORE REPLAY (checkpoint state) =====");
    let header_only = segments
        .first()
        .map(|(_, b)| b[..logger::header_len()].to_vec())
        .unwrap_or_default();
    dump(&data, &[(1, header_only)], only_table.as_deref());

    println!("\n===== WAL =====");
    for (n, bytes) in &segments {
        println!("--- segment {n} ---");
        for line in logger::describe_wal(bytes) {
            println!("{line}");
        }
    }

    println!("\n===== AFTER REPLAY =====");
    dump(&data, &segments, only_table.as_deref());
}

fn dump(data: &[u8], segments: &[(u64, Vec<u8>)], only: Option<&str>) {
    let disk = MemFile::new();
    for (n, bytes) in segments {
        disk.add_sibling_from_bytes(&format!("crash_debug.wal.{n}"), bytes.clone());
    }
    let db = match Db::<MemFile>::open_using("crash_debug", MemFile::from_bytes(data.to_vec()), disk) {
        Ok(db) => db,
        Err(e) => {
            println!("open failed: {e}");
            return;
        }
    };
    println!("stats: {:?}", db.stats());
    for i in 0..16 {
        let name = format!("t{i}");
        if let Some(o) = only && o != name {
            continue;
        }
        let Ok(Some(tid)) = db.table_id_by_name(&name) else { continue };
        for line in db.debug_dump_table(tid).unwrap() {
            println!("{line}");
        }
    }
    let _ = db.close();
}
