//! Prints every record in one or more squeal_db WAL segments, one per line,
//! plus each segment's header and scan verdict (torn tail / corruption).
//!
//!     cargo run --example wal_dump -- path/to/db.wal.1 [path/to/db.wal.2 ...]
//!     cargo run --example wal_dump -- path/to/db          # every db.wal.<n>

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: wal_dump <segment file>... | <database path>");
        std::process::exit(2);
    }
    let mut paths: Vec<(u64, String)> = Vec::new();
    for a in &args {
        if std::path::Path::new(a).is_file() {
            let n = a.rsplit('.').next().and_then(|s| s.parse().ok()).unwrap_or(0);
            paths.push((n, a.clone()));
        } else {
            let prefix = format!("{a}.wal.");
            for p in store::memfile::list_files_with_prefix(&prefix).unwrap_or_default() {
                if let Some(n) = p.strip_prefix(&prefix).and_then(|s| s.parse().ok()) {
                    paths.push((n, p));
                }
            }
        }
    }
    paths.sort();
    for (n, path) in paths {
        let bytes = std::fs::read(&path).unwrap_or_else(|e| {
            eprintln!("cannot read {path}: {e}");
            std::process::exit(1);
        });
        println!("=== segment {n}: {path} ({} bytes) ===", bytes.len());
        for line in store::logger::describe_wal(&bytes) {
            println!("{line}");
        }
    }
}
