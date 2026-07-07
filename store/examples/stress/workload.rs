use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use store::db::{DBFile, Db};
use store::error::StoreError;
use store::table::TableIdType;
use store::tuple::{DBIdType, Tuple};

use crate::config::Config;
use crate::stats::{OpCounters, Stats};

/// Tiny xorshift64* PRNG — deterministic per (seed, thread_idx) so a run is
/// reproducible via --seed without pulling in the `rand` crate.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn next_range(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    pub fn next_pct(&mut self) -> u8 {
        (self.next_u64() % 100) as u8
    }

    pub fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Insert,
    Update,
    Remove,
    Find,
}

pub type PrivateKey = (usize, u64); // (table_idx, key)
pub type PrivateExpectations = HashMap<PrivateKey, Option<Vec<u8>>>;

pub struct WorkerResult {
    pub thread_idx: usize,
    pub private_expectations: PrivateExpectations,
    // Full per-key event history (one line per op: txn, kind, whether the op
    // itself succeeded, and the batch's own commit/rollback outcome) — dumped
    // by main.rs for any key that ends up mismatched. This is what root-caused
    // todo.txt item [11] (a committed remove whose key later resolved to a
    // stale value under extreme contention): the trace showed a successful
    // commit-ok remove followed by every subsequent insert attempt failing
    // forever, yet the final read still found a value — impossible to see
    // from the mismatch line alone.
    pub key_history: HashMap<PrivateKey, Vec<String>>,
}

pub fn encode_private(thread_idx: usize, key: u64, seq: u64) -> Vec<u8> {
    format!("p|t{thread_idx}|k{key}|seq{seq}").into_bytes()
}

pub fn encode_hot(thread_idx: usize, key: u64, seq: u64, salt: u32) -> Vec<u8> {
    format!("h|t{thread_idx}|k{key}|seq{seq}|salt{salt}").into_bytes()
}

/// Structural integrity check for hot-key values: confirms the bytes are
/// exactly what *some* thread wrote via encode_hot, never a torn/mixed write
/// from two threads colliding under a locking bug.
pub fn validate_hot_encoding(data: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(data) else {
        return false;
    };
    let parts: Vec<&str> = s.split('|').collect();
    parts.len() == 5
        && parts[0] == "h"
        && parts[1].starts_with('t')
        && parts[1][1..].parse::<usize>().is_ok()
        && parts[2].starts_with('k')
        && parts[2][1..].parse::<u64>().is_ok()
        && parts[3].starts_with("seq")
        && parts[3][3..].parse::<u64>().is_ok()
        && parts[4].starts_with("salt")
        && parts[4][4..].parse::<u32>().is_ok()
}

fn counters_for<'a>(stats: &'a Stats, kind: OpKind) -> &'a OpCounters {
    match kind {
        OpKind::Insert => &stats.insert,
        OpKind::Update => &stats.update,
        OpKind::Remove => &stats.remove,
        OpKind::Find => &stats.find,
    }
}

fn record_err(stats: &Stats, kind: OpKind, e: &StoreError) {
    let c = counters_for(stats, kind);
    match e {
        StoreError::KeyNotFound(_) => c
            .key_not_found
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        StoreError::DuplicateKey(_) => c
            .duplicate_key
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        StoreError::LockContentionError => c
            .lock_contention
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        _ => c
            .other_error
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    };
}

/// Runs `f` (one db operation), retrying on LockContentionError with a short
/// randomized backoff up to `cfg.max_lock_retries` times. Returns the final
/// Result and records timing/outcome into `stats`.
fn run_op<T>(
    stats: &Stats,
    cfg: &Config,
    rng: &mut Rng,
    kind: OpKind,
    mut f: impl FnMut() -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let start = Instant::now();
    let mut attempt = 0;
    loop {
        match f() {
            Ok(v) => {
                stats.record_latency(start.elapsed());
                counters_for(stats, kind)
                    .success
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(v);
            }
            Err(StoreError::LockContentionError) if attempt < cfg.max_lock_retries => {
                attempt += 1;
                std::thread::sleep(Duration::from_micros(100 + rng.next_range(400)));
                continue;
            }
            Err(e) => {
                stats.record_latency(start.elapsed());
                if matches!(e, StoreError::LockContentionError) {
                    stats
                        .dropped_after_retry_exhaustion
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                record_err(stats, kind, &e);
                return Err(e);
            }
        }
    }
}

pub fn run_worker<F>(
    thread_idx: usize,
    db: Arc<Db<F>>,
    tables: Arc<Vec<TableIdType>>,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
) -> WorkerResult
where
    F: DBFile<Item = F> + Sync + 'static,
{
    let mut rng =
        Rng::new(cfg.seed ^ ((thread_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1));
    let mut private_state: PrivateExpectations = HashMap::new();
    let mut private_seq: HashMap<PrivateKey, u64> = HashMap::new();
    let mut hot_seq: u64 = 0;
    // See WorkerResult::key_history's doc comment.
    let mut key_history: HashMap<PrivateKey, Vec<String>> = HashMap::new();
    let mut txn_counter: u64 = 0;

    // Offset past the shared hot-key range [0, hot_keys) so a thread's
    // "private" keys never alias the keys every thread contends on.
    let private_base = cfg.hot_keys + thread_idx as u64 * cfg.private_keys;

    for _ in 0..cfg.ops_per_thread {
        let txn = match db.begin() {
            Ok(t) => t,
            Err(_) => continue,
        };

        let batch_size = 1 + rng.next_range(cfg.max_ops_per_txn as u64) as u32;
        let mut pending: Vec<(PrivateKey, Option<Vec<u8>>)> = Vec::new();
        let mut batch_trace: Vec<(PrivateKey, OpKind, bool)> = Vec::new();

        for _ in 0..batch_size {
            let table_idx = rng.next_range(tables.len() as u64) as usize;
            let tid = tables[table_idx];
            let use_hot = cfg.hot_keys > 0 && rng.next_bool();

            if use_hot && cfg.hot_keys > 0 {
                let key = rng.next_range(cfg.hot_keys);
                let kind = match rng.next_range(4) {
                    0 => OpKind::Insert,
                    1 => OpKind::Update,
                    2 => OpKind::Remove,
                    _ => OpKind::Find,
                };
                hot_seq += 1;
                let salt = rng.next_u64() as u32;
                apply_hot_op(
                    &db, &txn, &cfg, &stats, &mut rng, tid, key, kind, thread_idx, hot_seq, salt,
                );
            } else {
                let key = private_base + rng.next_range(cfg.private_keys.max(1));
                let pkey: PrivateKey = (table_idx, key);
                let exists = private_state
                    .get(&pkey)
                    .map(|v| v.is_some())
                    .unwrap_or(false);
                let kind = if !exists {
                    OpKind::Insert
                } else {
                    match rng.next_range(3) {
                        0 => OpKind::Update,
                        1 => OpKind::Remove,
                        _ => OpKind::Find,
                    }
                };
                let seq = private_seq.entry(pkey).or_insert(0);
                *seq += 1;
                let seq_val = *seq;
                let new_value = apply_private_op(
                    &db, &txn, &cfg, &stats, &mut rng, tid, key, kind, thread_idx, seq_val,
                );
                batch_trace.push((pkey, kind, new_value.is_some()));
                if let Some(outcome) = new_value {
                    pending.push((pkey, outcome));
                }
            }

            stats.mark_thread_progress(thread_idx);
        }

        txn_counter += 1;
        let this_txn = txn_counter;

        let commit = rng.next_pct() < cfg.commit_probability_pct;
        let outcome = if commit {
            let ok = db.commit(txn).is_ok();
            if ok {
                stats
                    .txn_committed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                for (k, v) in &pending {
                    private_state.insert(*k, v.clone());
                }
            }
            if ok { "commit-ok" } else { "commit-ERR" }
        } else {
            let ok = db.rollback(txn).is_ok();
            stats
                .txn_rolled_back
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // pending writes are discarded: private_state is left untouched,
            // matching the fact that the txn never committed.
            if ok { "rollback-ok" } else { "rollback-ERR" }
        };

        // Record this batch's full per-key trace, annotated with the
        // batch's own outcome — see WorkerResult::key_history.
        for (k, kind, ok) in &batch_trace {
            key_history.entry(*k).or_default().push(format!(
                "txn{this_txn} {kind:?} op_ok={ok} batch={outcome}"
            ));
        }
    }

    WorkerResult {
        thread_idx,
        private_expectations: private_state,
        key_history,
    }
}

/// Applies one op against a private (single-writer) key and returns the new
/// expected value (Some(data) or Some(_) for tombstoned-as-removed represented
/// by None) IF the op should update the caller's expectation once committed.
/// Returns None when the op doesn't change expected state (e.g. a Find, or an
/// op that errored).
fn apply_private_op<F>(
    db: &Db<F>,
    txn: &store::txn::Transaction,
    cfg: &Config,
    stats: &Stats,
    rng: &mut Rng,
    tid: TableIdType,
    key: u64,
    kind: OpKind,
    thread_idx: usize,
    seq: u64,
) -> Option<Option<Vec<u8>>>
where
    F: DBFile<Item = F> + Sync + 'static,
{
    let id = DBIdType::Int(key);
    match kind {
        OpKind::Insert => {
            let data = encode_private(thread_idx, key, seq);
            let r = run_op(stats, cfg, rng, kind, || {
                db.insert(tid, Tuple::new(key, &data), txn)
            });
            r.ok().map(|_| Some(data))
        }
        OpKind::Update => {
            let data = encode_private(thread_idx, key, seq);
            let r = run_op(stats, cfg, rng, kind, || {
                db.update(tid, Tuple::new(key, &data), txn)
            });
            r.ok().map(|_| Some(data))
        }
        OpKind::Remove => {
            let r = run_op(stats, cfg, rng, kind, || db.remove(tid, id.clone(), txn));
            r.ok().map(|_| None)
        }
        OpKind::Find => {
            let _ = run_op(stats, cfg, rng, kind, || db.find(tid, id.clone(), txn));
            None
        }
    }
}

fn apply_hot_op<F>(
    db: &Db<F>,
    txn: &store::txn::Transaction,
    cfg: &Config,
    stats: &Stats,
    rng: &mut Rng,
    tid: TableIdType,
    key: u64,
    kind: OpKind,
    thread_idx: usize,
    seq: u64,
    salt: u32,
) where
    F: DBFile<Item = F> + Sync + 'static,
{
    let id = DBIdType::Int(key);
    match kind {
        OpKind::Insert => {
            let data = encode_hot(thread_idx, key, seq, salt);
            let _ = run_op(stats, cfg, rng, kind, || {
                db.insert(tid, Tuple::new(key, &data), txn)
            });
        }
        OpKind::Update => {
            let data = encode_hot(thread_idx, key, seq, salt);
            let _ = run_op(stats, cfg, rng, kind, || {
                db.update(tid, Tuple::new(key, &data), txn)
            });
        }
        OpKind::Remove => {
            let _ = run_op(stats, cfg, rng, kind, || db.remove(tid, id.clone(), txn));
        }
        OpKind::Find => {
            let _ = run_op(stats, cfg, rng, kind, || db.find(tid, id.clone(), txn));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use store::db::Db;
    use store::memfile::MemFile;
    use store::tuple::DBIdType;

    use crate::config::{Backend, Config};
    use crate::stats::Stats;

    #[test]
    fn stress_regression_16t_3000ops() {
        let cfg = Arc::new(Config {
            threads: 2,
            tables: 4,
            ops_per_thread: 10,
            hot_keys: 16,
            private_keys: 2000,
            backend: Backend::Mem,
            seed: 16045690984503111693,
            report_interval_secs: 2,
            watchdog_timeout_secs: 60,
            commit_probability_pct: 90,
            max_ops_per_txn: 5,
            max_lock_retries: 5,
        });

        let db: Db<MemFile> = Db::create("stress-regression").unwrap();
        let mut table_ids = Vec::with_capacity(cfg.tables);
        for i in 0..cfg.tables {
            table_ids.push(db.create_table(format!("stress_table_{i}")).unwrap());
        }
        let tables = Arc::new(table_ids);
        let mut db = Arc::new(db);
        let stats = Arc::new(Stats::new(cfg.threads));

        let mut handles = Vec::with_capacity(cfg.threads);
        for thread_idx in 0..cfg.threads {
            let db = Arc::clone(&db);
            let tables = Arc::clone(&tables);
            let cfg = Arc::clone(&cfg);
            let stats = Arc::clone(&stats);
            handles.push(std::thread::spawn(move || {
                run_worker(thread_idx, db, tables, cfg, stats)
            }));
        }

        let mut worker_results = Vec::new();
        for h in handles {
            worker_results.push(h.join().expect("worker panicked"));
        }

        // Verify every private key expectation.
        let mut mismatches = Vec::new();
        for wr in &worker_results {
            for (&(table_idx, key), expected) in &wr.private_expectations {
                let txn = db.begin().expect("begin for verify");
                let actual = db
                    .find(tables[table_idx], DBIdType::Int(key), &txn)
                    .expect("find for verify");
                let _ = db.rollback(txn);
                let actual_data = actual.map(|t| t.data().to_vec());
                if actual_data != *expected {
                    mismatches.push(format!(
                        "thread {} table {} key {}: expected {:?}, found {:?}",
                        wr.thread_idx,
                        table_idx,
                        key,
                        expected
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned()),
                        actual_data
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned()),
                    ));
                }
            }
        }
        let db = Arc::try_unwrap(db).unwrap_or_else(|_| {
            panic!("Db still has outstanding references after all workers joined")
        });
        let _ = db.close();
        assert!(
            mismatches.is_empty(),
            "{} correctness failure(s):\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    }
}
