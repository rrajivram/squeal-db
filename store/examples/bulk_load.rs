//! Bulk-load volume test for the B+tree.
//!
//! Inserts a large number of rows (default 10M) using a small page size so
//! the index stays deep with relatively few rows, and small row values so
//! total memory/disk footprint stays modest. Uses the real `File` backend
//! by default — the page cache is bounded (see `PageBuffer`'s `max_entries`
//! eviction), so RAM usage is governed by page_size * cache size, not by
//! the dataset size, unlike the in-memory `MemFile` backend.
//!
//! Run with:
//!     cargo run --release --example bulk_load -- --rows 10000000
//!     cargo run --release --example bulk_load -- --rows 10000000 --random
//!
//!     cargo run --release --example bulk_load -- --help

use std::sync::Arc;
use std::time::{Duration, Instant};

use store::db::{DBFile, Db};
use store::error::StoreError;
use store::tuple::{DBIdType, Tuple};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Mem,
    File,
}

#[derive(Debug, Clone, Copy)]
struct Config {
    rows: u64,
    page_size: u64,
    value_size: usize,
    random_order: bool,
    backend: Backend,
    batch_size: u64,
    verify_every: u64,
    checkpoint_every: u64,
    seed: u64,
    max_pending_writes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: 10_000_000,
            page_size: 512,
            value_size: 16,
            random_order: false,
            backend: Backend::File,
            batch_size: 200,
            verify_every: 1,
            checkpoint_every: 500_000,
            seed: 0xB0BA_CAFE_1234_5678,
            max_pending_writes: 1024,
        }
    }
}

impl Config {
    fn from_args() -> Self {
        let mut cfg = Self::default();
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            let key = args[i].as_str();
            let mut next = || {
                i += 1;
                args.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("missing value for {key}");
                    std::process::exit(2);
                })
            };
            match key {
                "--rows" => cfg.rows = next().parse().expect("--rows must be a number"),
                "--page-size" => {
                    cfg.page_size = next().parse().expect("--page-size must be a number")
                }
                "--value-size" => {
                    cfg.value_size = next().parse().expect("--value-size must be a number")
                }
                "--random" => cfg.random_order = true,
                "--backend" => {
                    cfg.backend = match next().as_str() {
                        "mem" => Backend::Mem,
                        "file" => Backend::File,
                        other => {
                            eprintln!("unknown backend '{other}', expected mem|file");
                            std::process::exit(2);
                        }
                    }
                }
                "--batch-size" => {
                    cfg.batch_size = next().parse().expect("--batch-size must be a number")
                }
                "--verify-every" => {
                    cfg.verify_every = next().parse().expect("--verify-every must be a number")
                }
                "--checkpoint-every" => {
                    cfg.checkpoint_every =
                        next().parse().expect("--checkpoint-every must be a number")
                }
                "--seed" => cfg.seed = next().parse().expect("--seed must be a number"),
                "--max-pending-writes" => {
                    cfg.max_pending_writes = next()
                        .parse()
                        .expect("--max-pending-writes must be a number")
                }
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("unknown flag '{other}'");
                    print_help();
                    std::process::exit(2);
                }
            }
            i += 1;
        }
        cfg
    }
}

fn print_help() {
    println!(
        "Bulk-load volume test: inserts many rows with a small page size to keep\n\
         the B+tree deep, and small values to keep memory/disk use modest.\n\n\
         USAGE: cargo run --release --example bulk_load -- [FLAGS]\n\n\
         FLAGS:\n\
         \x20\x20--rows <N>              rows to insert (default: 10000000)\n\
         \x20\x20--page-size <N>         page size in bytes (default: 512 — small,\n\
         \x20\x20                        pushes nodes_per_page down to ~8)\n\
         \x20\x20--value-size <N>        bytes per row value (default: 16)\n\
         \x20\x20--random                shuffle insertion order (default: sequential)\n\
         \x20\x20--backend mem|file      storage backend (default: file — bounded RAM;\n\
         \x20\x20                        mem keeps the whole dataset resident, use with\n\
         \x20\x20                        care at large --rows)\n\
         \x20\x20--batch-size <N>        rows per transaction (default: 200)\n\
         \x20\x20--verify-every <N>      verify every Nth row after loading, 1 = all\n\
         \x20\x20                        (default: 1)\n\
         \x20\x20--checkpoint-every <N>  checkpoint every N rows, 0 = never\n\
         \x20\x20                        (default: 500000)\n\
         \x20\x20--seed <N>              PRNG seed for --random (default: fixed constant)\n\
         \x20\x20--max-pending-writes <N> cap on dirty pages the writer thread holds in\n\
         \x20\x20                        memory awaiting durable redo before blocking\n\
         \x20\x20                        callers (default: 1024)\n"
    );
}

// Deterministic xorshift64* — no external RNG dependency, matches the
// pattern already used for shuffling in the bplustree unit tests.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn next_range(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

fn main() {
    let cfg = Config::from_args();
    println!("{cfg:?}");
    let ok = match cfg.backend {
        Backend::Mem => {
            println!(
                "WARNING: --backend mem keeps the entire {} rows resident in memory.",
                cfg.rows
            );
            run::<store::memfile::MemFile>(cfg)
        }
        Backend::File => run::<std::fs::File>(cfg),
    };
    std::process::exit(if ok { 0 } else { 1 });
}

fn run<F>(cfg: Config) -> bool
where
    F: DBFile<Item = F> + 'static,
{
    let db_name = match cfg.backend {
        Backend::Mem => "bulk-load-mem".to_string(),
        Backend::File => std::env::temp_dir()
            .join(format!("squeal_bulk_load_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned(),
    };

    let db: Arc<Db<F>> = Db::create_with_limits(&db_name, cfg.page_size, cfg.max_pending_writes)
        .expect("failed to create bulk-load db");
    let tid = db.create_table("bulk".to_string()).unwrap();

    let value = vec![b'v'; cfg.value_size];

    let mut ids: Vec<u64> = (0..cfg.rows).collect();
    if cfg.random_order {
        let mut rng = Rng(cfg.seed);
        for i in (1..ids.len()).rev() {
            let j = rng.next_range(i as u64 + 1) as usize;
            ids.swap(i, j);
        }
    }

    println!(
        "Inserting {} rows (page_size={}, value_size={}B, order={}, backend={:?})...",
        cfg.rows,
        cfg.page_size,
        cfg.value_size,
        if cfg.random_order {
            "random"
        } else {
            "sequential"
        },
        cfg.backend,
    );

    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut last_report_rows = 0u64;
    let mut inserted = 0u64;

    for chunk in ids.chunks(cfg.batch_size.max(1) as usize) {
        let txn = db.begin().expect("begin");
        for &id in chunk {
            retry_on_contention(|| db.insert(tid, Tuple::new(id, &value), &txn)).unwrap();
        }
        db.commit(txn).expect("commit");
        inserted += chunk.len() as u64;

        if cfg.checkpoint_every > 0 && inserted % cfg.checkpoint_every < chunk.len() as u64 {
            db.checkpoint().expect("checkpoint");
        }

        if last_report.elapsed() >= Duration::from_secs(5) {
            let interval_rows = inserted - last_report_rows;
            let interval_secs = last_report.elapsed().as_secs_f64();
            println!(
                "  {inserted}/{} rows ({:.0} rows/sec this interval, {:.0} rows/sec avg, {:.1}% done)",
                cfg.rows,
                interval_rows as f64 / interval_secs,
                inserted as f64 / start.elapsed().as_secs_f64(),
                100.0 * inserted as f64 / cfg.rows as f64,
            );
            last_report = Instant::now();
            last_report_rows = inserted;
        }
    }

    let insert_elapsed = start.elapsed();
    println!(
        "Insert done: {} rows in {:?} ({:.0} rows/sec avg)",
        cfg.rows,
        insert_elapsed,
        cfg.rows as f64 / insert_elapsed.as_secs_f64()
    );

    println!(
        "Verifying (every {} row{})...",
        cfg.verify_every,
        if cfg.verify_every == 1 { "" } else { "s" }
    );
    let verify_start = Instant::now();
    let mut checked = 0u64;
    let mut mismatches = 0u64;
    for &id in ids.iter().filter(|&&id| id % cfg.verify_every == 0) {
        checked += 1;
        let txn = db.begin().expect("begin for verify");
        let found = retry_on_contention(|| db.find(tid, DBIdType::Int(id), &txn)).unwrap();
        let _ = db.rollback(txn);
        match found {
            Some(t) if t.data().to_vec() == value => {}
            Some(t) => {
                mismatches += 1;
                if mismatches <= 20 {
                    eprintln!("MISMATCH id={id}: wrong value {:?}", t.data());
                }
            }
            None => {
                mismatches += 1;
                if mismatches <= 20 {
                    eprintln!("MISMATCH id={id}: missing");
                }
            }
        }
    }
    println!(
        "Verify done: checked {checked} rows in {:?}, {mismatches} mismatches",
        verify_start.elapsed()
    );

    drop(ids);
    let _ = db.close();
    if cfg.backend == Backend::File {
        let _ = Db::<std::fs::File>::delete(&db_name);
    }

    println!(
        "========================================================\nRESULT: {}\n========================================================",
        if mismatches == 0 { "PASS" } else { "FAIL" }
    );
    mismatches == 0
}

// Matches the retry pattern used throughout this codebase's own tests and
// the `stress` example: LockContentionError is expected under any
// concurrent access and is safe to retry; nothing else should be retried.
fn retry_on_contention<T>(mut f: impl FnMut() -> Result<T, StoreError>) -> Result<T, StoreError> {
    let mut attempt = 0u32;
    loop {
        match f() {
            Err(StoreError::LockContentionError) if attempt < 8 => {
                attempt += 1;
                std::thread::sleep(Duration::from_micros(100 * attempt as u64));
            }
            other => return other,
        }
    }
}
