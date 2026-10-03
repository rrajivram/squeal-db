// A concurrent soak at the SQL level, with crash rounds. Connections move
// money between accounts of a partitioned table (each transfer also
// writing a ledger row, in the same transaction), move accounts between
// partitions, read the total, and change the tables' definitions
// (ADD PARTITION, CREATE INDEX, ADD COLUMN) — while the main thread now and
// then takes the database as a crash would leave it (Db::synced_snapshot),
// reopens that, and checks it:
//   - the balances add up to what they started at (transfers are atomic);
//   - every balance is its start plus its ledger rows (the two tables
//     agree);
//   - every transfer acknowledged before the snapshot is in it (durable);
//   - every row is in the partition it routes to, and every index tree
//     holds exactly its partition's rows.
// The live database is checked the same way at the end, and every read of
// the total, all along, must find the starting total.
//
// The suite runs it briefly. For a long run:
//     SQ_SOAK_SECS=120 SQ_SOAK_SEED=7 cargo test --release -p squeal-sql --lib \
//         soak -- --ignored --nocapture
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use super::partition_races::storage_problems;
use super::*;

const ACCOUNTS: i64 = 40;
const START: i64 = 1000;
const REGIONS: &[&str] = &["ca", "wa", "ny", "tx", "fl"];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn ints(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<Vec<Vec<i64>>, SchemaError> {
    let mut stmt = c.clone().create_statement(sql)?;
    stmt.execute()?;
    let Some(Some(ResultType::StreamingResult(mut s))) = stmt.results.pop() else {
        return Err(SchemaError::InternalSchemaError("no result".into()));
    };
    let mut rows = vec![];
    while let Some(r) = s.next_result()? {
        rows.push(
            r.values()
                .iter()
                .map(|v| match v {
                    ValueItem::Integer(n) => *n,
                    _ => i64::MIN,
                })
                .collect(),
        );
    }
    Ok(rows)
}

// What must hold of a database at rest; every violation, described.
fn problems(c: &Arc<Connection<MemFile>>, acked: &HashSet<i64>) -> Vec<String> {
    let mut out = vec![];
    let balances = match ints(c, "select id, bal from acct") {
        Ok(rows) => rows,
        Err(e) => return vec![format!("reading acct: {e}")],
    };
    let total: i64 = balances.iter().map(|r| r[1]).sum();
    if total != ACCOUNTS * START {
        out.push(format!(
            "balances add up to {total}, not {}",
            ACCOUNTS * START
        ));
    }
    if balances.len() != ACCOUNTS as usize {
        out.push(format!("{} accounts, not {ACCOUNTS}", balances.len()));
    }
    let ledger = match ints(c, "select lid, src, dst, amt from ledger") {
        Ok(rows) => rows,
        Err(e) => return vec![format!("reading ledger: {e}")],
    };
    let mut want: HashMap<i64, i64> = (0..ACCOUNTS).map(|a| (a, START)).collect();
    for r in &ledger {
        *want.entry(r[1]).or_default() -= r[3];
        *want.entry(r[2]).or_default() += r[3];
    }
    for r in &balances {
        if want.get(&r[0]) != Some(&r[1]) {
            out.push(format!(
                "account {} has {} but its ledger says {:?}",
                r[0],
                r[1],
                want.get(&r[0])
            ));
        }
    }
    let present: HashSet<i64> = ledger.iter().map(|r| r[0]).collect();
    let lost: Vec<_> = acked
        .iter()
        .filter(|l| !present.contains(l))
        .take(5)
        .collect();
    if !lost.is_empty() {
        out.push(format!("acknowledged transfers missing: {lost:?} ..."));
    }
    for t in ["acct", "ledger"] {
        out.extend(
            storage_problems(c, t)
                .into_iter()
                .map(|p| format!("{t}: {p}")),
        );
    }
    out
}

struct Counts {
    transfers: AtomicU64,
    moves: AtomicU64,
    reads: AtomicU64,
    ddl: AtomicU64,
    refused: AtomicU64,
}

// Runs `stmts` as one transaction; rolls back if any fails. True when it
// committed.
fn transaction(c: &Arc<Connection<MemFile>>, stmts: &[String]) -> bool {
    if run(c, "begin").is_err() {
        return false;
    }
    for s in stmts {
        if run(c, s).is_err() {
            let _ = run(c, "rollback");
            return false;
        }
    }
    run(c, "commit").is_ok()
}

fn soak(seconds: u64, seed: u64, workers: usize, crash_rounds: usize) {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let name = format!("soak_db_{seed}");
    let c = mgr.create_and_connect(&name).unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    run(
        &c,
        "create table acct (id integer not null, region varchar(4) not null, bal integer not null, \
         primary key(id, region)) partition by list (region) ( \
         partition west values in ('ca', 'wa'), partition east values in ('ny'), \
         partition other default)",
    )
    .unwrap();
    run(
        &c,
        "create table ledger (lid integer not null, src integer not null, dst integer not null, \
         amt integer not null, primary key(lid, src)) partition by range (src) ( \
         partition low values less than (20), partition high values less than maxvalue)",
    )
    .unwrap();
    let rows: Vec<String> = (0..ACCOUNTS)
        .map(|i| format!("({i}, '{}', {START})", REGIONS[i as usize % REGIONS.len()]))
        .collect();
    run(&c, &format!("insert into acct values {}", rows.join(", "))).unwrap();

    let acked = Mutex::new(HashSet::<i64>::new());
    let stop = AtomicBool::new(false);
    let counts = Counts {
        transfers: AtomicU64::new(0),
        moves: AtomicU64::new(0),
        reads: AtomicU64::new(0),
        ddl: AtomicU64::new(0),
        refused: AtomicU64::new(0),
    };
    let bad_reads = Mutex::new(vec![]);
    let mut findings: Vec<String> = vec![];

    std::thread::scope(|s| {
        for w in 0..workers {
            let (mgr, name, acked, stop, counts, bad_reads) =
                (&mgr, &name, &acked, &stop, &counts, &bad_reads);
            s.spawn(move || {
                let c = mgr.connect(name).unwrap();
                c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
                let mut rng = Rng(seed.wrapping_mul(1000003) + w as u64 * 7919 + 1);
                let mut n = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    // Each account's region, as this worker last saw it,
                    // is not needed: UPDATE finds the row by id.
                    match rng.below(10) {
                        0..=5 => {
                            let (a, b) = (rng.below(ACCOUNTS as u64), rng.below(ACCOUNTS as u64));
                            if a == b {
                                continue;
                            }
                            let amt = 1 + rng.below(50) as i64;
                            n += 1;
                            let lid = w as i64 * 1_000_000_000 + n;
                            let ok = transaction(
                                &c,
                                &[
                                    format!("update acct set bal = bal - {amt} where id = {a}"),
                                    format!("update acct set bal = bal + {amt} where id = {b}"),
                                    format!(
                                        "insert into ledger (lid, src, dst, amt) values \
                                         ({lid}, {a}, {b}, {amt})"
                                    ),
                                ],
                            );
                            if ok {
                                acked.lock().unwrap().insert(lid);
                                counts.transfers.fetch_add(1, Ordering::Relaxed);
                            } else {
                                counts.refused.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        6 | 7 => {
                            // Moves the account to another partition.
                            let a = rng.below(ACCOUNTS as u64);
                            let r = REGIONS[rng.below(REGIONS.len() as u64) as usize];
                            if run(
                                &c,
                                &format!("update acct set region = '{r}' where id = {a}"),
                            )
                            .is_ok()
                            {
                                counts.moves.fetch_add(1, Ordering::Relaxed);
                            } else {
                                counts.refused.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            // One transaction, one snapshot: the total holds.
                            if run(&c, "begin").is_err() {
                                continue;
                            }
                            match ints(&c, "select sum(bal), count(*) from acct") {
                                Ok(rows) if rows[0] == [ACCOUNTS * START, ACCOUNTS] => {
                                    counts.reads.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(rows) => bad_reads
                                    .lock()
                                    .unwrap()
                                    .push(format!("worker {w} read {rows:?}")),
                                Err(_) => {
                                    counts.refused.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            let _ = run(&c, "commit");
                        }
                    }
                }
            });
        }
        // Changes to the tables' definitions, now and then.
        {
            let (mgr, name, stop, counts) = (&mgr, &name, &stop, &counts);
            s.spawn(move || {
                let c = mgr.connect(name).unwrap();
                c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
                let mut i = 0;
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(80));
                    i += 1;
                    let ddl = match i % 4 {
                        0 => format!("alter table acct add partition q{i} values in ('q{i}')"),
                        1 if i < 20 => format!("create index acct_bal_{i} on acct (bal)"),
                        2 => format!("alter table ledger add column note{i} integer"),
                        _ => format!("alter table acct add partition r{i} values in ('r{i}')"),
                    };
                    if run(&c, &ddl).is_ok() {
                        counts.ddl.fetch_add(1, Ordering::Relaxed);
                    } else {
                        counts.refused.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        // Crash rounds, spread over the run.
        let start = Instant::now();
        let mut rng = Rng(seed ^ 0xC0FFEE);
        for round in 0..crash_rounds {
            let at = Duration::from_millis(
                seconds * 1000 * (round as u64 + 1) / (crash_rounds as u64 + 1) + rng.below(50),
            );
            if let Some(wait) = at.checked_sub(start.elapsed()) {
                std::thread::sleep(wait);
            }
            // Acknowledged before the snapshot is taken: must be in it.
            let acked_now = acked.lock().unwrap().clone();
            let (data, log) = c.synced_snapshot();
            let recovered_mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
            let r = match recovered_mgr.connect_using(&name, data, log) {
                Ok(r) => r,
                Err(e) => {
                    findings.push(format!("round {round}: reopening failed: {e}"));
                    continue;
                }
            };
            r.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
            for p in problems(&r, &acked_now) {
                findings.push(format!("crash round {round}: {p}"));
            }
            let _ = r.close();
        }
        let left = Duration::from_secs(seconds).saturating_sub(start.elapsed());
        std::thread::sleep(left);
        stop.store(true, Ordering::Relaxed);
    });

    let acked = acked.into_inner().unwrap();
    for p in problems(&c, &acked) {
        findings.push(format!("at the end: {p}"));
    }
    findings.extend(bad_reads.into_inner().unwrap());
    println!(
        "seed {seed}: {} transfers, {} moves, {} reads, {} DDL, {} refused (conflicts, \
         deadlocks), {crash_rounds} crash rounds",
        counts.transfers.load(Ordering::Relaxed),
        counts.moves.load(Ordering::Relaxed),
        counts.reads.load(Ordering::Relaxed),
        counts.ddl.load(Ordering::Relaxed),
        counts.refused.load(Ordering::Relaxed),
    );
    assert!(findings.is_empty(), "seed {seed}:\n{}", findings.join("\n"));
    assert!(
        counts.transfers.load(Ordering::Relaxed) > 0,
        "nothing committed"
    );
}

#[test]
fn test_a_short_soak_with_crash_rounds_keeps_every_invariant() {
    soak(3, 1, 4, 3);
}

#[test]
#[ignore]
fn soak_long() {
    let seconds = std::env::var("SQ_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let seed = std::env::var("SQ_SOAK_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let workers = std::env::var("SQ_SOAK_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    soak(seconds, seed, workers, (seconds / 2).max(1) as usize);
}
