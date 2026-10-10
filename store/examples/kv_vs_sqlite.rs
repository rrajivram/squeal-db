//! The store alone against SQLite used as a key-value store: the same keyed
//! blobs, the same operations, phase by phase, with each phase's answers
//! compared. No SQL layer on the store's side — this is what the engine
//! costs, where squeal-sql's `load_vs_sqlite` example measures the engine
//! plus parsing, planning and row encoding.
//!
//!   cargo run --release -p store --example kv_vs_sqlite -- [rows] [threads]
//!
//! Two tables, `rows` rows each, 100-byte values:
//! - `kv`: an integer key (the store's `DBIdType::Int`; SQLite's `INTEGER
//!   PRIMARY KEY`) — point operations. (The store orders integer keys by
//!   hash, so they are not scanned by range.)
//! - `ordered`: a two-integer key `(grp, seq)`, 100 rows a group (the
//!   store's `DBIdType::Rec`; SQLite's `PRIMARY KEY (grp, seq) WITHOUT
//!   ROWID`) — prefix and range scans, the shape an index has.
//!
//! Both are on disk and equally durable: every commit waits for a full
//! flush (the store's `sync_data` is F_FULLFSYNC on macOS; SQLite runs WAL
//! with synchronous=FULL and fullfsync=ON). SQLite is driven through
//! prepared statements, so it still runs a compiled statement per
//! operation where the store is called directly.
use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

use store::{
    db::Db,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

type Store = Arc<Db<std::fs::File>>;

const VALUE_LEN: usize = 100;
const GROUP: u64 = 100;

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

// A row's value: 100 bytes that depend on its key and a version.
fn value(key: u64, version: u8) -> [u8; VALUE_LEN] {
    let mut v = [0u8; VALUE_LEN];
    let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version as u64;
    for chunk in v.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let b = x.to_le_bytes();
        chunk.copy_from_slice(&b[..chunk.len()]);
    }
    v
}

// What a phase read, as something both engines must agree on: how many
// rows, and a sum over their bytes.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct Digest {
    rows: u64,
    sum: u64,
}

impl Digest {
    fn add(&mut self, bytes: &[u8]) {
        self.rows += 1;
        self.sum = self.sum.wrapping_add(bytes.len() as u64).wrapping_add(
            u64::from_le_bytes(bytes[..8].try_into().unwrap())
                ^ u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().unwrap()),
        );
    }
    fn merge(&mut self, o: Digest) {
        self.rows += o.rows;
        self.sum = self.sum.wrapping_add(o.sum);
    }
}

fn pair(grp: u64, seq: u64) -> DBIdType {
    DBIdType::Rec(
        IndexKey::new_from(&[ValueItem::Integer(grp as i64), ValueItem::Integer(seq as i64)])
            .unwrap(),
    )
}

fn lite_conn(path: &str) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "pragma journal_mode = wal; pragma synchronous = full; \
         pragma fullfsync = on; pragma checkpoint_fullfsync = on;",
    )
    .unwrap();
    c
}

fn phase(
    disagreements: &mut usize,
    name: &str,
    ops: u64,
    run_store: &mut dyn FnMut() -> Digest,
    run_lite: &mut dyn FnMut() -> Digest,
) {
    let t = Instant::now();
    let a = run_store();
    let store = t.elapsed();
    let t = Instant::now();
    let b = run_lite();
    let lite = t.elapsed();
    let per = |d: Duration| {
        let ns = d.as_nanos() as f64 / ops as f64;
        if ns >= 1e6 {
            format!("{:.2} ms", ns / 1e6)
        } else if ns >= 1e3 {
            format!("{:.2} us", ns / 1e3)
        } else {
            format!("{ns:.0} ns")
        }
    };
    let rate = |d: Duration| ops as f64 / d.as_secs_f64();
    let agree = a == b;
    if !agree {
        *disagreements += 1;
        eprintln!("  {name}: store {a:?} sqlite {b:?}");
    }
    println!(
        "| {:<46} | {:>8} | {:>10} | {:>12.0}/s | {:>10} | {:>12.0}/s | {:>6.2}x | {} |",
        name,
        ops,
        per(store),
        rate(store),
        per(lite),
        rate(lite),
        lite.as_secs_f64() / store.as_secs_f64(),
        if agree { "yes" } else { "NO" }
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let threads: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let groups = rows / GROUP;

    let dir = std::env::temp_dir().join(format!("store_vs_sqlite_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store_path = dir.join("kv.store").to_string_lossy().into_owned();
    let lite_path = dir.join("kv.sqlite").to_string_lossy().into_owned();

    let db: Store = Db::<std::fs::File>::create(&store_path).unwrap();
    let kv = db.create_table("kv".into()).unwrap();
    let ordered = db
        .create_table_with_index_entry_size("ordered".into(), 96)
        .unwrap();
    let l = lite_conn(&lite_path);
    l.execute_batch(
        "create table kv (k integer primary key, v blob not null); \
         create table ordered (grp integer not null, seq integer not null, v blob not null, \
         primary key (grp, seq)) without rowid;",
    )
    .unwrap();

    println!(
        "store vs SQLite {} as a key-value store: {rows} rows a table, {VALUE_LEN}-byte values, \
         {threads} reader threads",
        rusqlite::version()
    );
    println!("both on disk, every commit fully flushed (F_FULLFSYNC); dir {}\n", dir.display());
    println!(
        "| phase | ops | store per op | store rate | SQLite per op | SQLite rate | store speed vs SQLite | same answers |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---|");
    let mut bad = 0usize;
    let none = Digest::default;

    // ---- load ----------------------------------------------------------------
    phase(
        &mut bad,
        "insert, integer keys, one transaction",
        rows,
        &mut || {
            let txn = db.begin().unwrap();
            for k in 0..rows {
                db.insert(kv, Tuple::new(k, &value(k, 0)), &txn).unwrap();
            }
            db.commit(txn).unwrap();
            none()
        },
        &mut || {
            l.execute_batch("begin").unwrap();
            {
                let mut st = l.prepare_cached("insert into kv values (?, ?)").unwrap();
                for k in 0..rows {
                    st.execute(rusqlite::params![k as i64, &value(k, 0)[..]]).unwrap();
                }
            }
            l.execute_batch("commit").unwrap();
            none()
        },
    );
    phase(
        &mut bad,
        "insert, composite keys in order, one transaction",
        rows,
        &mut || {
            let txn = db.begin().unwrap();
            for k in 0..rows {
                let t = Tuple::new_with(pair(k / GROUP, k % GROUP), &value(k, 0), None, None);
                db.insert(ordered, t, &txn).unwrap();
            }
            db.commit(txn).unwrap();
            none()
        },
        &mut || {
            l.execute_batch("begin").unwrap();
            {
                let mut st = l.prepare_cached("insert into ordered values (?, ?, ?)").unwrap();
                for k in 0..rows {
                    st.execute(rusqlite::params![
                        (k / GROUP) as i64,
                        (k % GROUP) as i64,
                        &value(k, 0)[..]
                    ])
                    .unwrap();
                }
            }
            l.execute_batch("commit").unwrap();
            none()
        },
    );
    phase(
        &mut bad,
        "checkpoint",
        1,
        &mut || {
            db.checkpoint().unwrap();
            none()
        },
        &mut || {
            l.execute_batch("pragma wal_checkpoint(truncate)").unwrap();
            none()
        },
    );

    // ---- reads ---------------------------------------------------------------
    let mut rng = Rng(0xFEED_0000_0000_0002);
    let keys: Vec<u64> = (0..200_000).map(|_| rng.below(rows)).collect();
    phase(
        &mut bad,
        "point lookup, one read transaction",
        keys.len() as u64,
        &mut || {
            let mut d = none();
            let txn = db.begin().unwrap();
            for k in &keys {
                db.find_with(kv, &DBIdType::Int(*k), &txn, |t| d.add(t.data()))
                    .unwrap()
                    .expect("row");
            }
            db.commit(txn).unwrap();
            d
        },
        &mut || {
            let mut d = none();
            l.execute_batch("begin").unwrap();
            {
                let mut st = l.prepare_cached("select v from kv where k = ?").unwrap();
                for k in &keys {
                    let mut r = st.query([*k as i64]).unwrap();
                    let row = r.next().unwrap().expect("row");
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            l.execute_batch("commit").unwrap();
            d
        },
    );
    let few = &keys[..50_000];
    phase(
        &mut bad,
        "point lookup, a transaction each",
        few.len() as u64,
        &mut || {
            let mut d = none();
            for k in few {
                let txn = db.begin().unwrap();
                db.find_with(kv, &DBIdType::Int(*k), &txn, |t| d.add(t.data()))
                    .unwrap()
                    .expect("row");
                db.commit(txn).unwrap();
            }
            d
        },
        &mut || {
            let mut d = none();
            let mut st = l.prepare_cached("select v from kv where k = ?").unwrap();
            for k in few {
                let mut r = st.query([*k as i64]).unwrap();
                let row = r.next().unwrap().expect("row");
                d.add(row.get_ref(0).unwrap().as_blob().unwrap());
            }
            d
        },
    );
    let pairs: Vec<u64> = (0..200_000).map(|_| rng.below(rows)).collect();
    phase(
        &mut bad,
        "point lookup, composite key",
        pairs.len() as u64,
        &mut || {
            let mut d = none();
            let txn = db.begin().unwrap();
            for k in &pairs {
                db.find_with(ordered, &pair(k / GROUP, k % GROUP), &txn, |t| d.add(t.data()))
                    .unwrap()
                    .expect("row");
            }
            db.commit(txn).unwrap();
            d
        },
        &mut || {
            let mut d = none();
            l.execute_batch("begin").unwrap();
            {
                let mut st = l
                    .prepare_cached("select v from ordered where grp = ? and seq = ?")
                    .unwrap();
                for k in &pairs {
                    let mut r = st.query([(k / GROUP) as i64, (k % GROUP) as i64]).unwrap();
                    let row = r.next().unwrap().expect("row");
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            l.execute_batch("commit").unwrap();
            d
        },
    );
    let scans = 10u64;
    phase(
        &mut bad,
        "full scan (per row)",
        scans * rows,
        &mut || {
            let mut d = none();
            for _ in 0..scans {
                let txn = db.begin().unwrap();
                let mut c = db.table_scan_in_txn(kv, &txn).unwrap();
                while let Some(t) = c.next_ref().unwrap() {
                    d.add(t.data());
                }
                drop(c);
                db.commit(txn).unwrap();
            }
            d
        },
        &mut || {
            let mut d = none();
            let mut st = l.prepare_cached("select v from kv").unwrap();
            for _ in 0..scans {
                let mut r = st.query([]).unwrap();
                while let Some(row) = r.next().unwrap() {
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            d
        },
    );
    let prefixes: Vec<u64> = (0..5_000).map(|_| rng.below(groups)).collect();
    phase(
        &mut bad,
        "prefix scan, 100 rows (per scan)",
        prefixes.len() as u64,
        &mut || {
            let mut d = none();
            for g in &prefixes {
                let prefix = IndexKey::new_from(&[ValueItem::Integer(*g as i64)]).unwrap();
                let mut c = db.prefix_scan(ordered, prefix).unwrap();
                while let Some(t) = c.next_ref().unwrap() {
                    d.add(t.data());
                }
            }
            d
        },
        &mut || {
            let mut d = none();
            let mut st = l.prepare_cached("select v from ordered where grp = ?").unwrap();
            for g in &prefixes {
                let mut r = st.query([*g as i64]).unwrap();
                while let Some(row) = r.next().unwrap() {
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            d
        },
    );
    let span = (groups / 100).max(1); // ~1% of the table
    let starts: Vec<u64> = (0..200).map(|_| rng.below(groups - span)).collect();
    phase(
        &mut bad,
        "range scan, 1% of the table (per row)",
        starts.len() as u64 * span * GROUP,
        &mut || {
            let mut d = none();
            for g in &starts {
                let mut c = db
                    .range_scan_bounds(
                        ordered,
                        Bound::Included(pair(*g, 0)),
                        Bound::Excluded(pair(g + span, 0)),
                    )
                    .unwrap();
                while let Some(t) = c.next_ref().unwrap() {
                    d.add(t.data());
                }
            }
            d
        },
        &mut || {
            let mut d = none();
            let mut st = l
                .prepare_cached("select v from ordered where grp >= ? and grp < ?")
                .unwrap();
            for g in &starts {
                let mut r = st.query([*g as i64, (g + span) as i64]).unwrap();
                while let Some(row) = r.next().unwrap() {
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            d
        },
    );

    // ---- durable writes --------------------------------------------------------
    let single: Vec<u64> = (0..1_000).map(|_| rng.below(rows)).collect();
    phase(
        &mut bad,
        "update one row, commit (flush each)",
        single.len() as u64,
        &mut || {
            for k in &single {
                let txn = db.begin().unwrap();
                db.update(kv, Tuple::new(*k, &value(*k, 1)), &txn).unwrap();
                db.commit(txn).unwrap();
            }
            none()
        },
        &mut || {
            let mut st = l.prepare_cached("update kv set v = ? where k = ?").unwrap();
            for k in &single {
                st.execute(rusqlite::params![&value(*k, 1)[..], *k as i64]).unwrap();
            }
            none()
        },
    );
    phase(
        &mut bad,
        "insert one row, commit (flush each)",
        1_000,
        &mut || {
            for i in 0..1_000u64 {
                let k = rows + i;
                let txn = db.begin().unwrap();
                db.insert(kv, Tuple::new(k, &value(k, 0)), &txn).unwrap();
                db.commit(txn).unwrap();
            }
            none()
        },
        &mut || {
            let mut st = l.prepare_cached("insert into kv values (?, ?)").unwrap();
            for i in 0..1_000u64 {
                let k = rows + i;
                st.execute(rusqlite::params![k as i64, &value(k, 0)[..]]).unwrap();
            }
            none()
        },
    );
    let batch: Vec<u64> = (0..20_000).map(|_| rng.below(rows)).collect();
    phase(
        &mut bad,
        "update, 100 rows a transaction",
        batch.len() as u64,
        &mut || {
            for chunk in batch.chunks(100) {
                let txn = db.begin().unwrap();
                for k in chunk {
                    db.update(kv, Tuple::new(*k, &value(*k, 2)), &txn).unwrap();
                }
                db.commit(txn).unwrap();
            }
            none()
        },
        &mut || {
            for chunk in batch.chunks(100) {
                l.execute_batch("begin").unwrap();
                {
                    let mut st = l.prepare_cached("update kv set v = ? where k = ?").unwrap();
                    for k in chunk {
                        st.execute(rusqlite::params![&value(*k, 2)[..], *k as i64]).unwrap();
                    }
                }
                l.execute_batch("commit").unwrap();
            }
            none()
        },
    );
    let doomed = (rows / 10).min(20_000);
    phase(
        &mut bad,
        "delete, one transaction",
        doomed,
        &mut || {
            let txn = db.begin().unwrap();
            for k in 0..doomed {
                db.remove(ordered, pair(k / GROUP, k % GROUP), &txn).unwrap();
            }
            db.commit(txn).unwrap();
            none()
        },
        &mut || {
            l.execute_batch("begin").unwrap();
            {
                let mut st = l
                    .prepare_cached("delete from ordered where grp = ? and seq = ?")
                    .unwrap();
                for k in 0..doomed {
                    st.execute([(k / GROUP) as i64, (k % GROUP) as i64]).unwrap();
                }
            }
            l.execute_batch("commit").unwrap();
            none()
        },
    );
    // Everything the writes left, read back from both.
    phase(
        &mut bad,
        "check: both tables after the writes (per row)",
        2 * rows + 1_000 - doomed,
        &mut || {
            let mut d = none();
            let txn = db.begin().unwrap();
            for table in [kv, ordered] {
                let mut c = db.table_scan_in_txn(table, &txn).unwrap();
                while let Some(t) = c.next_ref().unwrap() {
                    d.add(t.data());
                }
            }
            db.commit(txn).unwrap();
            d
        },
        &mut || {
            let mut d = none();
            for sql in ["select v from kv", "select v from ordered"] {
                let mut st = l.prepare_cached(sql).unwrap();
                let mut r = st.query([]).unwrap();
                while let Some(row) = r.next().unwrap() {
                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                }
            }
            d
        },
    );

    // ---- concurrency ----------------------------------------------------------
    let per_thread = 100_000usize;
    let lookups: Vec<Vec<u64>> = (0..threads)
        .map(|t| {
            let mut r = Rng(0xABCD_0000 + t as u64 * 7919 + 1);
            (0..per_thread).map(|_| r.below(rows)).collect()
        })
        .collect();
    phase(
        &mut bad,
        &format!("point lookups, {threads} threads"),
        (threads * per_thread) as u64,
        &mut || {
            std::thread::scope(|scope| {
                let handles: Vec<_> = lookups
                    .iter()
                    .map(|ks| {
                        let db = db.clone();
                        scope.spawn(move || {
                            let mut d = Digest::default();
                            let txn = db.begin().unwrap();
                            for k in ks {
                                db.find_with(kv, &DBIdType::Int(*k), &txn, |t| d.add(t.data()))
                                    .unwrap()
                                    .expect("row");
                            }
                            db.commit(txn).unwrap();
                            d
                        })
                    })
                    .collect();
                let mut total = Digest::default();
                for h in handles {
                    total.merge(h.join().unwrap());
                }
                total
            })
        },
        &mut || {
            std::thread::scope(|scope| {
                let handles: Vec<_> = lookups
                    .iter()
                    .map(|ks| {
                        let path = lite_path.clone();
                        scope.spawn(move || {
                            let c = lite_conn(&path);
                            let mut d = Digest::default();
                            c.execute_batch("begin").unwrap();
                            {
                                let mut st =
                                    c.prepare_cached("select v from kv where k = ?").unwrap();
                                for k in ks {
                                    let mut r = st.query([*k as i64]).unwrap();
                                    let row = r.next().unwrap().expect("row");
                                    d.add(row.get_ref(0).unwrap().as_blob().unwrap());
                                }
                            }
                            c.execute_batch("commit").unwrap();
                            d
                        })
                    })
                    .collect();
                let mut total = Digest::default();
                for h in handles {
                    total.merge(h.join().unwrap());
                }
                total
            })
        },
    );

    println!();
    println!(
        "store speed vs SQLite = SQLite time / store time (above 1: the store is faster). \
         {bad} phase(s) with different answers."
    );
    db.checkpoint().unwrap();
    l.execute_batch("pragma wal_checkpoint(truncate)").unwrap();
    let total = |prefix: &str| -> u64 {
        std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
            .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
            .sum()
    };
    println!(
        "on disk after a checkpoint: store {:.1} MB, SQLite {:.1} MB",
        total("kv.store") as f64 / 1e6,
        total("kv.sqlite") as f64 / 1e6
    );
    drop(l);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
