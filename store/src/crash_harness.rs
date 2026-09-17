//! Crash-consistency harness (TXN_SIMPLIFICATION_PLAN.md phase 0).
//!
//! Runs a seeded, multi-threaded workload against a `MemFile`-backed `Db`,
//! "cuts the power" at a random moment by taking `Db::synced_snapshot` (only
//! bytes some `do_sync` published survive), reopens from that snapshot, and
//! checks that:
//!
//! 1. every transaction whose `commit()` returned before the cut is fully
//!    present;
//! 2. every transaction that never committed is fully absent;
//! 3. a transaction whose `commit()` was in flight at the cut is either fully
//!    present or fully absent, and the set of such transactions that made it
//!    is a prefix of each thread's commit order (durability is monotonic in
//!    log order);
//! 4. a table scan sees exactly the rows that should exist, each once.
//!
//! Every thread writes only its own keys, so the expected state of a key is
//! a function of that thread's own commit history alone — no cross-thread
//! ordering to reason about. Values encode `(thread, commit sequence)` so a
//! key's final value says exactly which transaction wrote it.
//!
//! Multiple rounds continue the workload on the recovered database, so a
//! recovered state is itself crash-tested. Checkpoints are fired from a
//! separate thread at random intervals so the cut can land before, during,
//! or after one.
//!
//! Lives in the library (not `tests/`) so both the unit tests below and the
//! `crash` example binary (a soak runner) share one implementation, and so it
//! can reach `Db::synced_snapshot`.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use crate::{
    cursor::Cursor,
    db::Db,
    error::StoreError,
    memfile::MemFile,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
};

#[derive(Debug, Clone)]
pub struct CrashConfig {
    pub seed: u64,
    pub threads: usize,
    pub tables: usize,
    /// Distinct keys each thread owns per table.
    pub keys_per_thread: u64,
    pub max_ops_per_txn: u32,
    pub commit_probability_pct: u8,
    /// How many crash/recover rounds to run.
    pub rounds: usize,
    /// The cut lands a random time in this range (ms) after a round starts.
    pub cut_after_ms: (u64, u64),
    /// Checkpoints fire every random interval in this range (ms); `None`
    /// disables the checkpoint thread.
    pub checkpoint_every_ms: Option<(u64, u64)>,
    /// Page size for the database (small forces splits/overflow quickly).
    pub page_size: u64,
    /// Where to save the failing snapshot's files, if anywhere.
    pub dump_dir: Option<String>,
    /// Phase 6: readers that hold one transaction open for the whole round,
    /// across every checkpoint, re-reading a fixed key sample. Each must see
    /// the same values throughout (repeatable read), and its open
    /// transaction pins WAL segments instead of stalling checkpoints.
    pub long_readers: usize,
}

impl Default for CrashConfig {
    fn default() -> Self {
        Self {
            seed: 0x5EED_CAFE,
            threads: 4,
            tables: 2,
            keys_per_thread: 64,
            max_ops_per_txn: 4,
            commit_probability_pct: 85,
            rounds: 3,
            cut_after_ms: (30, 120),
            checkpoint_every_ms: Some((20, 60)),
            page_size: 4096,
            dump_dir: None,
            long_readers: 1,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CrashReport {
    pub rounds: usize,
    pub committed_definite: u64,
    pub committed_ambiguous: u64,
    pub ambiguous_recovered: u64,
    pub rolled_back: u64,
    pub checkpoints: u64,
    pub rows_verified: u64,
    pub long_reader_reads: u64,
    pub long_reader_violations: u64,
    /// The most WAL segments seen retained at once (long readers pin them).
    pub max_wal_segments: usize,
}

/// Tiny xorshift so the harness has no `rand` dependency and a seed
/// reproduces a run exactly (modulo thread scheduling, which is the point).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo { lo } else { lo + self.next() % (hi - lo) }
    }
    fn pct(&mut self) -> u8 {
        (self.next() % 100) as u8
    }
}

type Key = (usize, u64); // (table index, key)
type Value = Option<Vec<u8>>; // None = absent
type State = BTreeMap<Key, Value>;

/// One committed transaction as seen by its own thread: the global commit
/// sequence it drew right after `commit()` returned, and what it wrote.
struct CommittedTxn {
    seq: u64,
    writes: Vec<(Key, Value)>,
}

struct WorkerOut {
    thread_idx: usize,
    committed: Vec<CommittedTxn>,
    rolled_back: u64,
    /// The keys this thread ever touched — the domain of its state.
    keys: Vec<Key>,
}

fn encode(thread_idx: usize, seq: u64) -> Vec<u8> {
    format!("t{thread_idx}|c{seq}").into_bytes()
}

fn apply(state: &mut State, writes: &[(Key, Value)]) {
    for (k, v) in writes {
        state.insert(*k, v.clone());
    }
}

/// Runs the whole harness. `Err` carries a human-readable diagnosis with the
/// seed, round, thread, and the keys that disagreed.
pub fn run(cfg: &CrashConfig) -> Result<CrashReport, String> {
    let mut rng = Rng::new(cfg.seed);
    let mut report = CrashReport::default();
    let name = format!("crash_harness_{}", cfg.seed);

    let db = Db::<MemFile>::create_with_page_size(&name, cfg.page_size)
        .map_err(|e| format!("create: {e}"))?;
    let mut table_ids = Vec::new();
    for i in 0..cfg.tables {
        table_ids.push(
            db.create_table(format!("t{i}"))
                .map_err(|e| format!("create_table: {e}"))?,
        );
    }
    // Db::create writes both file headers without syncing them; a cut
    // before the first checkpoint would otherwise leave nothing to reopen.
    db.checkpoint().map_err(|e| format!("initial checkpoint: {e}"))?;

    // Per-thread committed state carried across rounds (what recovery was
    // verified to have kept).
    let mut baseline: Vec<State> = (0..cfg.threads).map(|_| State::new()).collect();
    let mut db = db;

    for round in 0..cfg.rounds {
        let stop = Arc::new(AtomicBool::new(false));
        let commit_seq = Arc::new(AtomicU64::new(1));
        let checkpoints = Arc::new(AtomicU64::new(0));
        let tables = Arc::new(table_ids.clone());

        let mut workers = Vec::new();
        for (t, start_state) in baseline.iter().enumerate().take(cfg.threads) {
            let db = db.clone();
            let tables = tables.clone();
            let stop = stop.clone();
            let commit_seq = commit_seq.clone();
            let cfg = cfg.clone();
            let start_state = start_state.clone();
            let seed = rng.next();
            workers.push(thread::spawn(move || {
                worker(t, seed, db, tables, stop, commit_seq, &cfg, start_state)
            }));
        }
        let readers: Vec<_> = (0..cfg.long_readers)
            .map(|r| {
                let db = db.clone();
                let tables = tables.clone();
                let stop = stop.clone();
                let sample: Vec<Key> = (0..cfg.tables)
                    .flat_map(|t| (0..8u64).map(move |i| (t, (r as u64) * 8 + i)))
                    .collect();
                thread::spawn(move || long_reader(db, tables, stop, sample))
            })
            .collect();
        let ckpt_thread = cfg.checkpoint_every_ms.map(|(lo, hi)| {
            let db = db.clone();
            let stop = stop.clone();
            let checkpoints = checkpoints.clone();
            let mut r = Rng::new(rng.next());
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(r.range(lo, hi)));
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if db.checkpoint().is_ok() {
                        checkpoints.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        });

        // --- the cut ---
        thread::sleep(Duration::from_millis(rng.range(cfg.cut_after_ms.0, cfg.cut_after_ms.1)));
        // Read the sequence BEFORE snapshotting: a commit whose `seq` is
        // below `cut_seq` returned before this load, therefore before the
        // snapshot, therefore (commit waits for its record to be synced)
        // its record is in the snapshot. Reading it after the snapshot
        // would let a commit that landed in between be misclassified as
        // definite.
        let cut_seq = commit_seq.load(Ordering::SeqCst);
        let (data_snap, log_snap) = db.synced_snapshot();
        // Kept for diagnostics: the exact bytes recovery saw. Written out to
        // `dump_dir` on failure so the recovery can be replayed offline.
        let segments_for_dump = log_snap.synced_siblings(&format!("{name}.wal."));
        let data_for_dump = MemFile::from_bytes(data_snap.data());
        stop.store(true, Ordering::SeqCst);

        let mut outs = Vec::new();
        for w in workers {
            outs.push(w.join().map_err(|_| "worker panicked".to_string())?);
        }
        if let Some(h) = ckpt_thread {
            h.join().map_err(|_| "checkpoint thread panicked".to_string())?;
        }
        for h in readers {
            let (reads, violations, max_segments) =
                h.join().map_err(|_| "long reader panicked".to_string())?;
            report.long_reader_reads += reads;
            report.long_reader_violations += violations;
            report.max_wal_segments = report.max_wal_segments.max(max_segments);
        }
        if report.long_reader_violations > 0 {
            return Err(format!(
                "seed {} round {round}: a long-lived reader saw a value change inside its own \
                 transaction ({} violation(s))",
                cfg.seed, report.long_reader_violations
            ));
        }
        report.checkpoints += checkpoints.load(Ordering::Relaxed);
        // Simulate the crash: drop the live engine without close().
        drop(db);

        let save_snapshot = |msg: &mut String| {
            if let Some(dir) = &cfg.dump_dir {
                let base = format!("{dir}/crash_{}_round{round}", cfg.seed);
                let _ = std::fs::write(format!("{base}.data"), data_for_dump.data());
                for (path, bytes) in &segments_for_dump {
                    let n = path.rsplit('.').next().unwrap_or("0");
                    let _ = std::fs::write(format!("{base}.wal.{n}"), bytes);
                }
                msg.push_str(&format!(
                    "\n  snapshot saved: {base}.data / {base}.wal.<n> ({} segment(s))",
                    segments_for_dump.len()
                ));
            }
        };

        // --- recovery ---
        let reopened = match Db::<MemFile>::open_using(&name, data_snap, log_snap) {
            Ok(db) => db,
            Err(e) => {
                let mut msg = format!("seed {} round {round}: reopen failed: {e}", cfg.seed);
                save_snapshot(&mut msg);
                return Err(msg);
            }
        };
        let mut reopened_ids = Vec::new();
        for i in 0..cfg.tables {
            reopened_ids.push(
                reopened
                    .table_id_by_name(format!("t{i}"))
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("seed {} round {round}: table t{i} missing after reopen", cfg.seed))?,
            );
        }

        // --- verification, per thread ---
        let mut all_expected_present: HashMap<Key, Vec<u8>> = HashMap::new();
        for out in &outs {
            let definite: Vec<&CommittedTxn> =
                out.committed.iter().filter(|c| c.seq < cut_seq).collect();
            let ambiguous: Vec<&CommittedTxn> =
                out.committed.iter().filter(|c| c.seq >= cut_seq).collect();
            report.committed_definite += definite.len() as u64;
            report.committed_ambiguous += ambiguous.len() as u64;
            report.rolled_back += out.rolled_back;

            let mut expected = baseline[out.thread_idx].clone();
            for c in &definite {
                apply(&mut expected, &c.writes);
            }
            // Actual state of this thread's keys after recovery.
            let mut actual = State::new();
            for k in &out.keys {
                let v = read_key(&reopened, &reopened_ids, *k)
                    .map_err(|e| format!("seed {} round {round}: read {k:?}: {e}", cfg.seed))?;
                actual.insert(*k, v);
            }
            // Find the prefix of ambiguous commits that made it.
            let mut accepted = None;
            let mut candidate = expected.clone();
            for k in 0..=ambiguous.len() {
                if k > 0 {
                    apply(&mut candidate, &ambiguous[k - 1].writes);
                }
                if states_equal(&candidate, &actual, &out.keys) {
                    accepted = Some(k);
                    break;
                }
            }
            let Some(k) = accepted else {
                let mut msg = diagnose(cfg.seed, round, out, &expected, &ambiguous, &actual, cut_seq);
                save_snapshot(&mut msg);
                return Err(msg);
            };
            report.ambiguous_recovered += k as u64;
            for c in ambiguous.iter().take(k) {
                apply(&mut expected, &c.writes);
            }
            for (key, v) in &expected {
                if let Some(v) = v {
                    all_expected_present.insert(*key, v.clone());
                }
            }
            report.rows_verified += out.keys.len() as u64;
            baseline[out.thread_idx] = expected;
        }

        // --- structural check: scans see exactly the expected rows, once ---
        for (ti, tid) in reopened_ids.iter().enumerate() {
            let mut seen: HashMap<u64, Vec<u8>> = HashMap::new();
            let mut cursor = reopened.table_scan(*tid).map_err(|e| e.to_string())?;
            while let Some(t) = cursor.next().map_err(|e| e.to_string())? {
                let DBIdType::Int(k) = t.id else {
                    return Err(format!("seed {} round {round}: non-int key in scan", cfg.seed));
                };
                if seen.insert(k, t.data.to_vec()).is_some() {
                    return Err(format!(
                        "seed {} round {round}: table t{ti} key {k} appears twice in a scan",
                        cfg.seed
                    ));
                }
            }
            let expected_in_table: HashMap<u64, Vec<u8>> = all_expected_present
                .iter()
                .filter(|((t, _), _)| *t == ti)
                .map(|((_, k), v)| (*k, v.clone()))
                .collect();
            if seen != expected_in_table {
                let missing: Vec<u64> = expected_in_table
                    .keys()
                    .filter(|k| !seen.contains_key(k))
                    .copied()
                    .collect();
                let extra: Vec<u64> = seen
                    .keys()
                    .filter(|k| !expected_in_table.contains_key(k))
                    .copied()
                    .collect();
                let mut msg = format!(
                    "seed {} round {round}: table t{ti} scan mismatch: {} expected, {} seen; \
                     missing {:?}; unexpected {:?}",
                    cfg.seed,
                    expected_in_table.len(),
                    seen.len(),
                    missing,
                    extra
                );
                for k in missing.iter().chain(extra.iter()) {
                    let via_find = read_key(&reopened, &reopened_ids, (ti, *k))
                        .map(|v| v.map(|v| String::from_utf8_lossy(&v).into_owned()));
                    msg.push_str(&format!("\n  key {k}: find() = {via_find:?}; WAL records mentioning it:"));
                    let needle = format!("table={} key={}", tid, k);
                    for (path, bytes) in &segments_for_dump {
                        msg.push_str(&format!("\n    segment {path}:"));
                        for line in crate::logger::describe_wal(bytes) {
                            if line.contains(&needle) || line.starts_with("LogHeader") || line.contains("record(s)") {
                                msg.push_str(&format!("\n    {line}"));
                            }
                        }
                    }
                }
                msg.push_str(&format!("\n  stats after recovery: {:?}", reopened.stats()));
                if let Ok(lines) = reopened.debug_dump_table(*tid) {
                    msg.push_str("\n  structure after recovery:");
                    for l in lines {
                        msg.push_str(&format!("\n    {l}"));
                    }
                }
                save_snapshot(&mut msg);
                return Err(msg);
            }
        }

        table_ids = reopened_ids;
        db = reopened;
        report.rounds += 1;
    }
    // A clean close at the end must also succeed.
    db.close().map_err(|e| format!("final close: {e}"))?;
    Ok(report)
}

fn read_key(db: &Arc<Db<MemFile>>, tables: &[TableIdType], key: Key) -> Result<Value, StoreError> {
    let txn = db.begin()?;
    let r = db.find(tables[key.0], DBIdType::Int(key.1), &txn)?;
    db.rollback(txn)?;
    Ok(r.map(|t| t.data.to_vec()))
}

/// One transaction held open across the whole round: every re-read of the
/// sample must return what the first read returned. Returns (reads,
/// violations, most WAL segments seen retained).
fn long_reader(
    db: Arc<Db<MemFile>>,
    tables: Arc<Vec<TableIdType>>,
    stop: Arc<AtomicBool>,
    sample: Vec<Key>,
) -> (u64, u64, usize) {
    let txn = match db.begin() {
        Ok(t) => t,
        Err(_) => return (0, 0, 0),
    };
    let mut first: std::collections::HashMap<Key, Value> = std::collections::HashMap::new();
    let (mut reads, mut violations, mut max_segments) = (0u64, 0u64, 0usize);
    while !stop.load(Ordering::Relaxed) {
        for k in &sample {
            let Ok(v) = db.find(tables[k.0], DBIdType::Int(k.1), &txn) else { continue };
            let v = v.map(|t| t.data.to_vec());
            reads += 1;
            match first.get(k) {
                None => {
                    first.insert(*k, v);
                }
                Some(seen) if *seen != v => violations += 1,
                Some(_) => {}
            }
        }
        max_segments = max_segments.max(db.stats().wal_segments);
        thread::sleep(Duration::from_millis(1));
    }
    let _ = db.rollback(txn);
    (reads, violations, max_segments)
}

fn states_equal(expected: &State, actual: &State, keys: &[Key]) -> bool {
    keys.iter().all(|k| {
        let e = expected.get(k).cloned().flatten();
        let a = actual.get(k).cloned().flatten();
        e == a
    })
}

fn diagnose(
    seed: u64,
    round: usize,
    out: &WorkerOut,
    definite_state: &State,
    ambiguous: &[&CommittedTxn],
    actual: &State,
    cut_seq: u64,
) -> String {
    let mut lines = vec![format!(
        "seed {seed} round {round} thread {}: no prefix of the {} in-flight commit(s) explains the \
         recovered state (cut at commit seq {cut_seq})",
        out.thread_idx,
        ambiguous.len()
    )];
    for k in &out.keys {
        let e = definite_state.get(k).cloned().flatten();
        let a = actual.get(k).cloned().flatten();
        if e != a {
            let e = e.map(|v| String::from_utf8_lossy(&v).into_owned());
            let a = a.map(|v| String::from_utf8_lossy(&v).into_owned());
            lines.push(format!("  key {k:?}: definite-expected {e:?}, actual {a:?}"));
        }
    }
    for (i, c) in ambiguous.iter().enumerate() {
        let ks: Vec<String> = c
            .writes
            .iter()
            .map(|(k, v)| format!("{k:?}={}", v.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_else(|| "ABSENT".into())))
            .collect();
        lines.push(format!("  ambiguous[{i}] seq {}: {}", c.seq, ks.join(", ")));
    }
    lines.join("\n")
}

#[allow(clippy::too_many_arguments)]
fn worker(
    thread_idx: usize,
    seed: u64,
    db: Arc<Db<MemFile>>,
    tables: Arc<Vec<TableIdType>>,
    stop: Arc<AtomicBool>,
    commit_seq: Arc<AtomicU64>,
    cfg: &CrashConfig,
    start_state: State,
) -> WorkerOut {
    let mut rng = Rng::new(seed);
    let mut model = start_state;
    let mut committed = Vec::new();
    let mut rolled_back = 0u64;
    let base = thread_idx as u64 * cfg.keys_per_thread;
    let mut keys: Vec<Key> = model.keys().copied().collect();
    // Per-thread attempt counter: every transaction this thread ever
    // starts gets a distinct value, so a key's stored bytes name the exact
    // transaction that wrote them (committed or not).
    let mut txn_no: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        let Ok(txn) = db.begin() else { continue };
        txn_no += 1;
        let n = 1 + rng.range(0, cfg.max_ops_per_txn as u64);
        // Writes this transaction made, in order, with the value it wrote.
        let mut writes: Vec<(Key, Value)> = Vec::new();
        // The model as this transaction sees it (its own writes included).
        let mut local = model.clone();
        let mut failed = false;
        for _ in 0..n {
            let ti = rng.range(0, cfg.tables as u64) as usize;
            let k = base + rng.range(0, cfg.keys_per_thread);
            let key = (ti, k);
            if !keys.contains(&key) {
                keys.push(key);
            }
            let exists = local.get(&key).map(|v| v.is_some()).unwrap_or(false);
            let payload = encode(thread_idx, txn_no);
            let res: Result<Value, StoreError> = if !exists {
                db.insert(tables[ti], Tuple::new(k, &payload), &txn)
                    .map(|_| Some(payload.clone()))
            } else {
                match rng.range(0, 3) {
                    0 => db
                        .update(tables[ti], Tuple::new(k, &payload), &txn)
                        .map(|_| Some(payload.clone())),
                    1 => db
                        .remove(tables[ti], DBIdType::Int(k), &txn)
                        .map(|_| None),
                    _ => {
                        // A read; must see this transaction's own view.
                        match db.find(tables[ti], DBIdType::Int(k), &txn) {
                            Ok(v) => {
                                let got = v.map(|t| t.data.to_vec());
                                let want = local.get(&key).cloned().flatten();
                                if got != want {
                                    // Not a crash bug but a visibility bug; surface it loudly.
                                    panic!(
                                        "thread {thread_idx}: own-view read of {key:?} returned {got:?}, expected {want:?}"
                                    );
                                }
                                continue;
                            }
                            Err(_) => {
                                failed = true;
                                break;
                            }
                        }
                    }
                }
            };
            match res {
                Ok(v) => {
                    local.insert(key, v.clone());
                    writes.push((key, v));
                }
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        let want_commit = !failed && rng.pct() < cfg.commit_probability_pct;
        if want_commit {
            match db.commit(txn) {
                Ok(()) => {
                    let seq = commit_seq.fetch_add(1, Ordering::SeqCst);
                    apply(&mut model, &writes);
                    committed.push(CommittedTxn { seq, writes });
                }
                Err(_) => {
                    // A failed commit leaves the transaction active/aborted;
                    // its writes must never become visible.
                    rolled_back += 1;
                }
            }
        } else {
            let _ = db.rollback(txn);
            rolled_back += 1;
        }
    }
    WorkerOut {
        thread_idx,
        committed,
        rolled_back,
        keys,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_seed(seed: u64) {
        let cfg = CrashConfig {
            seed,
            ..Default::default()
        };
        match run(&cfg) {
            Ok(r) => {
                assert_eq!(r.rounds, cfg.rounds);
                assert!(r.rows_verified > 0);
            }
            Err(e) => panic!("crash harness failed:\n{e}"),
        }
    }

    #[test]
    fn crash_harness_seed_1() {
        run_seed(1);
    }

    #[test]
    fn crash_harness_seed_2() {
        run_seed(2);
    }

    #[test]
    fn crash_harness_seed_3_no_checkpoints() {
        let cfg = CrashConfig {
            seed: 3,
            checkpoint_every_ms: None,
            ..Default::default()
        };
        if let Err(e) = run(&cfg) {
            panic!("crash harness failed:\n{e}");
        }
    }
}
