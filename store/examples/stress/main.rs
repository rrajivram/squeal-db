//! Concurrency / correctness / throughput stress harness for squeal_db.
//!
//! Deliberately lives under `examples/` (not `tests/` or the lib itself) so it
//! only ever sees the crate's `pub` surface — exactly what an external
//! consumer of this library would have available. Run with:
//!
//!     cargo run --release --example stress -- --help
//!
//! Defaults to an in-memory backend (`MemFile`) because the goal here is
//! sussing out races and deadlocks in the locking/transaction machinery as
//! fast as possible — real disk I/O latency just adds noise and masks timing
//! bugs. Pass `--backend file` to switch to the real `File` backend instead.

mod config;
mod report;
mod stats;
mod workload;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use store::db::{DBFile, Db};
use store::table::TableIdType;
use store::tuple::DBIdType;

use config::{Backend, Config};
use report::CorrectnessReport;
use stats::Stats;

fn main() {
    let cfg = Config::from_args();
    println!("{cfg:?}");
    let passed = match cfg.backend {
        Backend::Mem => run::<store::memfile::MemFile>(cfg),
        Backend::File => run::<std::fs::File>(cfg),
    };
    std::process::exit(if passed { 0 } else { 1 });
}

fn run<F>(cfg: Config) -> bool
where
    F: DBFile<Item = F> + Sync + 'static,
{
    let cfg = Arc::new(cfg);
    let db_name = match cfg.backend {
        Backend::Mem => "stress-mem".to_string(),
        Backend::File => std::env::temp_dir()
            .join(format!("squeal_stress_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned(),
    };

    let db: Db<F> = Db::create(&db_name).expect("failed to create stress db");
    let mut table_ids = Vec::with_capacity(cfg.tables);
    for i in 0..cfg.tables {
        table_ids.push(db.create_table(format!("stress_table_{i}")).unwrap());
    }
    let tables = Arc::new(table_ids);
    let db = Arc::new(db);

    let stats = Arc::new(Stats::new(cfg.threads));
    let stop = Arc::new(AtomicBool::new(false));
    let watchdog = report::spawn_watchdog(Arc::clone(&cfg), Arc::clone(&stats), Arc::clone(&stop));

    println!("Starting {} worker threads...", cfg.threads);
    let mut handles = Vec::with_capacity(cfg.threads);
    for thread_idx in 0..cfg.threads {
        let db = Arc::clone(&db);
        let tables = Arc::clone(&tables);
        let cfg = Arc::clone(&cfg);
        let stats = Arc::clone(&stats);
        handles.push(std::thread::spawn(move || {
            workload::run_worker(thread_idx, db, tables, cfg, stats)
        }));
    }

    let mut worker_results = Vec::with_capacity(handles.len());
    let mut any_panicked = false;
    for h in handles {
        match h.join() {
            Ok(r) => worker_results.push(r),
            Err(e) => {
                any_panicked = true;
                eprintln!("worker thread panicked: {e:?}");
            }
        }
    }

    println!("All workers joined. Verifying correctness...");
    let correctness = verify_correctness(&db, &tables, &cfg, &stats, worker_results);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = watchdog.join();

    // Reclaim sole ownership to close cleanly, the way a real caller would.
    drop(tables);
    let db = Arc::try_unwrap(db)
        .unwrap_or_else(|_| panic!("Db still has outstanding references after all workers joined"));
    println!("Calling close");
    let _ = db.close();
    if cfg.backend == Backend::File {
        let _ = store::db::Db::<std::fs::File>::delete(&db_name);
    }

    report::print_final_report(&cfg, &stats, &correctness);
    correctness.passed() && !any_panicked
}

fn verify_correctness<F>(
    db: &Db<F>,
    tables: &[TableIdType],
    cfg: &Config,
    stats: &Stats,
    worker_results: Vec<workload::WorkerResult>,
) -> CorrectnessReport
where
    F: DBFile<Item = F> + Sync + 'static,
{
    let mut private_checked = 0u64;
    let mut private_mismatches = Vec::new();

    for wr in &worker_results {
        for (&(table_idx, key), expected) in &wr.private_expectations {
            private_checked += 1;
            // Tick so the watchdog sees liveness during verification.
            stats
                .completed_ops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let txn = db.begin().expect("begin for verification");
            let actual = db
                .find(tables[table_idx], DBIdType::Int(key), &txn)
                .expect("find for verification");
            let _ = db.rollback(txn);

            let actual_data = actual.map(|t| t.data().to_vec());
            if actual_data != *expected {
                report::record_mismatch(
                    &mut private_mismatches,
                    format!(
                        "thread {} table {} key {key}: expected {:?}, found {:?}",
                        wr.thread_idx,
                        table_idx,
                        expected
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned()),
                        actual_data
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned()),
                    ),
                );
                // Dump this key's full event history — invaluable for root-causing
                // a mismatch (see key_history's own doc comment in workload.rs).
                if let Some(hist) = wr.key_history.get(&(table_idx, key)) {
                    eprintln!(
                        "  history thread={} table={table_idx} key={key}:",
                        wr.thread_idx
                    );
                    for line in hist {
                        eprintln!("    {line}");
                    }
                }
            }
        }
    }

    let mut hot_checked = 0u64;
    let mut hot_corrupt = Vec::new();
    for (table_idx, &tid) in tables.iter().enumerate() {
        for key in 0..cfg.hot_keys {
            hot_checked += 1;
            stats
                .completed_ops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let txn = db.begin().expect("begin for hot verification");
            let found = db.find(tid, DBIdType::Int(key), &txn);
            let _ = db.rollback(txn);
            match found {
                Ok(Some(t)) if !workload::validate_hot_encoding(t.data()) => {
                    report::record_mismatch(
                        &mut hot_corrupt,
                        format!(
                            "table {table_idx} key {key}: malformed value {:?}",
                            String::from_utf8_lossy(t.data())
                        ),
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    report::record_mismatch(
                        &mut hot_corrupt,
                        format!("table {table_idx} key {key}: find errored: {e:?}"),
                    );
                }
            }
        }
    }

    CorrectnessReport {
        private_checked,
        private_mismatches,
        hot_checked,
        hot_corrupt,
    }
}
