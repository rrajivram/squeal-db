//! Performance report harness for squeal_db.
//!
//! Drives the library purely through its public API (`Db<MemFile>`/
//! `Db<File>`) — the same surface an external consumer would use. Unlike
//! `examples/stress` (concurrency/correctness) or `examples/bulk_load`
//! (single large sequential load), this reports throughput and latency
//! percentiles across a range of operation kinds, value sizes, and
//! concurrency levels, for both storage backends. Makes no correctness
//! assertions and applies no fixes — purely observational.
//!
//! Run with:
//!     cargo run --release --example perf
//!     cargo run --release --example perf -- --backend file --rows 100000
//!     cargo run --release --example perf -- --help

mod bench;
mod config;

use std::sync::Arc;
use std::time::{Duration, Instant};

use store::cursor::Cursor;
use store::db::{DBFile, Db};
use store::error::StoreError;
use store::tuple::{DBIdType, Tuple};

use bench::{Latencies, Rng, report_duration, report_phase};
use config::{Backend, Config};

fn main() {
    let cfg = Config::from_args();
    println!("{cfg:?}\n");

    match cfg.backend {
        Backend::Mem => run_backend::<store::memfile::MemFile>("Mem", "perf-mem", &cfg),
        Backend::File => run_backend::<std::fs::File>("File", &file_db_path(), &cfg),
        Backend::Both => {
            run_backend::<store::memfile::MemFile>("Mem", "perf-mem", &cfg);
            println!();
            run_backend::<std::fs::File>("File", &file_db_path(), &cfg);
        }
    }
}

fn file_db_path() -> String {
    std::env::temp_dir()
        .join(format!("squeal_perf_{}.db", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

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

fn run_backend<F>(label: &str, db_name: &str, cfg: &Config)
where
    F: DBFile<Item = F> + Sync + 'static,
{
    println!("======================== Backend: {label} ========================");
    let is_file_backend = label == "File";
    if is_file_backend {
        let _ = Db::<F>::delete(db_name);
    }
    let db: Arc<Db<F>> = Db::create_with_page_size(db_name, cfg.page_size)
        .unwrap_or_else(|e| panic!("[{label}] failed to create db: {e:?}"));
    let tid = db.create_table("small".to_string()).unwrap();

    println!(
        "--- Single-threaded, small values (table=small, n={}, value={}B) ---",
        cfg.rows, cfg.small_value_size
    );
    let small_value = vec![b'v'; cfg.small_value_size];
    let mut rng = Rng::new(cfg.seed);

    // Sequential insert.
    let mut lat = Latencies::with_capacity(cfg.rows as usize);
    let t0 = Instant::now();
    for i in 0..cfg.rows {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        retry_on_contention(|| db.insert(tid, Tuple::new(i, &small_value), &txn)).unwrap();
        db.commit(txn).unwrap();
        lat.record(op_start.elapsed());
    }
    report_phase("insert, sequential (1 op/txn)", t0.elapsed(), lat.summary());

    // Point lookup, random order.
    let mut keys: Vec<u64> = (0..cfg.rows).collect();
    rng.shuffle(&mut keys);
    let mut lat = Latencies::with_capacity(keys.len());
    let t0 = Instant::now();
    for &k in &keys {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        let found = retry_on_contention(|| db.find(tid, DBIdType::Int(k), &txn)).unwrap();
        assert!(found.is_some(), "unexpected missing key during perf run");
        let _ = db.rollback(txn);
        lat.record(op_start.elapsed());
    }
    report_phase("find, random order", t0.elapsed(), lat.summary());

    // Full table scan. NOTE: deliberately run BEFORE the update phase below —
    // see the "KNOWN ISSUE" printed at the end of this report. Once every
    // row in a multi-page table has been updated, table_scan's data-page
    // chain silently truncates (or errors) partway through, so a
    // post-update scan count cannot be trusted here.
    let t0 = Instant::now();
    let mut scanned = 0u64;
    let mut cursor = db.table_scan(tid).unwrap();
    while cursor.next().unwrap().is_some() {
        scanned += 1;
    }
    drop(cursor);
    println!(
        "{:<42} n={:<8} wall={:<10} {:>10.0} rows/s",
        "table scan (full, pre-update)",
        scanned,
        format!("{:.2?}", t0.elapsed()),
        scanned as f64 / t0.elapsed().as_secs_f64().max(1e-9),
    );

    // Range scan over the middle third of the key space (also pre-update).
    let start = DBIdType::Int(cfg.rows / 3);
    let end = DBIdType::Int(2 * cfg.rows / 3);
    let t0 = Instant::now();
    let mut scanned = 0u64;
    let mut cursor = db.range_scan(tid, start, end).unwrap();
    while cursor.next().unwrap().is_some() {
        scanned += 1;
    }
    drop(cursor);
    println!(
        "{:<42} n={:<8} wall={:<10} {:>10.0} rows/s",
        "range scan (middle third, pre-update)",
        scanned,
        format!("{:.2?}", t0.elapsed()),
        scanned as f64 / t0.elapsed().as_secs_f64().max(1e-9),
    );

    // Update, random order. Run AFTER the scans above — see the note on
    // table_scan's ordering.
    rng.shuffle(&mut keys);
    let mut lat = Latencies::with_capacity(keys.len());
    let t0 = Instant::now();
    for &k in &keys {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        retry_on_contention(|| db.update(tid, Tuple::new(k, &small_value), &txn)).unwrap();
        db.commit(txn).unwrap();
        lat.record(op_start.elapsed());
    }
    report_phase("update, random order", t0.elapsed(), lat.summary());

    // Remove, random subset.
    rng.shuffle(&mut keys);
    let remove_n = (keys.len() as u64 * cfg.remove_fraction_pct as u64 / 100) as usize;
    let mut lat = Latencies::with_capacity(remove_n);
    let t0 = Instant::now();
    for &k in &keys[..remove_n] {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        retry_on_contention(|| db.remove(tid, DBIdType::Int(k), &txn)).unwrap();
        db.commit(txn).unwrap();
        lat.record(op_start.elapsed());
    }
    report_phase(
        &format!("remove, random subset ({}%)", cfg.remove_fraction_pct),
        t0.elapsed(),
        lat.summary(),
    );
    // NOTE: this harness originally found a real bug here (now fixed, see
    // BPlusTree::update's and handle_large_page_size's own comments, plus
    // db::tests::test_table_scan_correct_after_updating_every_row_across_multiple_data_pages):
    // once every row on a packed multi-tuple data page had been update()'d,
    // handle_large_page_size misread the resulting slight over-capacity as
    // "this page needs a single-tuple overflow chain", clobbering its
    // next_page and corrupting table_scan's walk. The scans above still run
    // before the update phase, which remains a reasonable "freshly loaded
    // table" scan scenario to report on either way.

    // Large-value (overflow page) insert phase, on a fresh table.
    println!(
        "\n--- Single-threaded, large values (table=large, n={}, value={}B) ---",
        cfg.large_rows, cfg.large_value_size
    );
    let large_tid = db.create_table("large".to_string()).unwrap();
    let large_value = vec![b'w'; cfg.large_value_size];
    let mut lat = Latencies::with_capacity(cfg.large_rows as usize);
    let t0 = Instant::now();
    for i in 0..cfg.large_rows {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        retry_on_contention(|| db.insert(large_tid, Tuple::new(i, &large_value), &txn)).unwrap();
        db.commit(txn).unwrap();
        lat.record(op_start.elapsed());
    }
    report_phase("insert, sequential (1 op/txn)", t0.elapsed(), lat.summary());

    let mut large_keys: Vec<u64> = (0..cfg.large_rows).collect();
    rng.shuffle(&mut large_keys);
    let mut lat = Latencies::with_capacity(large_keys.len());
    let t0 = Instant::now();
    for &k in &large_keys {
        let op_start = Instant::now();
        let txn = db.begin().unwrap();
        let found = retry_on_contention(|| db.find(large_tid, DBIdType::Int(k), &txn)).unwrap();
        assert!(found.is_some(), "unexpected missing key during perf run");
        let _ = db.rollback(txn);
        lat.record(op_start.elapsed());
    }
    report_phase("find, random order", t0.elapsed(), lat.summary());

    // Whole-phase operations.
    println!("\n--- Whole-phase operations ---");
    let t0 = Instant::now();
    db.checkpoint().unwrap();
    report_duration("checkpoint", t0.elapsed());

    let t0 = Instant::now();
    let (f, u, r) = db.close().unwrap();
    report_duration("close", t0.elapsed());

    let t0 = Instant::now();
    let db: Arc<Db<F>> = Db::open_using(db_name, f, u, r).unwrap();
    report_duration("reopen (open_using)", t0.elapsed());
    drop(db);

    // Multi-threaded insert scaling: each thread inserts into its own
    // private key range on a fresh table, so this measures pure throughput
    // scaling, not lock-contention behavior (see the `stress` example for
    // that).
    println!(
        "\n--- Multi-threaded insert scaling (private key ranges, {} ops/thread, value={}B) ---",
        cfg.ops_per_thread, cfg.small_value_size
    );
    for &threads in &cfg.thread_counts {
        run_scaling_phase::<F>(db_name, threads, cfg, is_file_backend);
    }

    if is_file_backend {
        let _ = Db::<F>::delete(db_name);
    }
}

fn run_scaling_phase<F>(db_name: &str, threads: usize, cfg: &Config, is_file_backend: bool)
where
    F: DBFile<Item = F> + Sync + 'static,
{
    let scaling_name = format!("{db_name}-scaling-{threads}");
    if is_file_backend {
        let _ = Db::<F>::delete(&scaling_name);
    }
    let db: Arc<Db<F>> = Db::create_with_page_size(&scaling_name, cfg.page_size).unwrap();
    let tid = db.create_table("scaling".to_string()).unwrap();
    let value_size = cfg.small_value_size;

    let t0 = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|thread_idx| {
            let db = Arc::clone(&db);
            let ops = cfg.ops_per_thread;
            std::thread::spawn(move || {
                let value = vec![b'v'; value_size];
                let base = thread_idx as u64 * ops;
                for i in 0..ops {
                    let key = base + i;
                    let txn = db.begin().unwrap();
                    retry_on_contention(|| db.insert(tid, Tuple::new(key, &value), &txn)).unwrap();
                    db.commit(txn).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    let total_ops = threads as u64 * cfg.ops_per_thread;
    println!(
        "threads={threads:<4} n={total_ops:<10} wall={:<10} {:>10.0} ops/s",
        format!("{:.2?}", elapsed),
        total_ops as f64 / elapsed.as_secs_f64().max(1e-9),
    );

    let _ = db.close();
    if is_file_backend {
        let _ = Db::<F>::delete(&scaling_name);
    }
}
