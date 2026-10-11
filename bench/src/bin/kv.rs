//! The store against two other embedded key-value stores — SQLite holding
//! keyed blobs, and redb — on one workload, each in a process of its own
//! with its memory measured, under each memory profile.
//!
//!   cargo run --release -p bench --bin kv -- [--size ROWS] [--threads N]
//!       [--runs N] [--engines store,sqlite,redb] [--profiles default,large]
//!
//! Two tables of `size` rows with 100-byte values: `kv`, keyed by an
//! integer, and `ordered`, keyed by (grp, seq) with 100 rows a group, for
//! prefix and range scans. Every commit waits for a full flush
//! (F_FULLFSYNC on macOS): the store and redb (Durability::Immediate)
//! through Rust's sync_data, SQLite in WAL mode with synchronous=FULL and
//! fullfsync on. SQLite runs a prepared statement per operation; the store
//! and redb are called directly.
use std::ops::Bound;
use std::sync::Arc;

use bench::{ChildArgs, Digest, child_args, drive, phase, plan_from_args};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use store::{
    config::{CreateConfig, OpenConfig},
    db::Db,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

const GIB: u64 = 1 << 30;
const VALUE_LEN: usize = 100;
const GROUP: u64 = 100;

const ENGINES: &[(&str, &str)] = &[
    ("store", "squeal-db's store, called directly"),
    ("sqlite", "SQLite 3.53 through rusqlite, bundled, as a table of (key, blob)"),
    ("redb", "redb 4.3, a pure-Rust embedded key-value store"),
];

const PROFILES: &[(&str, &str)] = &[
    (
        "default",
        "each engine as it ships — store: 128 MiB page cache; SQLite: ~2 MiB; redb: 1 GiB",
    ),
    ("large", "1 GiB of cache each"),
];

/// What the workload needs from an engine. Keys are u64s, or (grp, seq).
trait Engine {
    /// Inserts `rows` keys into `kv` and as many into `ordered`, in one
    /// transaction each.
    fn load(&mut self, rows: u64);
    fn checkpoint(&mut self);
    fn get(&mut self, keys: &[u64], txn_each: bool) -> Digest;
    fn get_pair(&mut self, keys: &[u64]) -> Digest;
    fn scan(&mut self) -> Digest;
    fn prefix(&mut self, grp: u64) -> Digest;
    fn range(&mut self, from_grp: u64, to_grp: u64) -> Digest;
    /// One transaction of updates to `kv` (version `v`), or inserts of new keys.
    fn update(&mut self, keys: &[u64], v: u8);
    fn insert(&mut self, keys: &[u64]);
    fn delete_pairs(&mut self, keys: &[u64]);
    fn scan_both(&mut self) -> Digest;
    fn parallel_get(&self, per_thread: &[Vec<u64>]) -> Digest;
}

// The load's transactions: 100,000 rows each. The store keeps a version
// record per row a transaction writes until it commits, capped at a
// million by default, so one transaction can't load a million rows.
fn chunks(rows: u64) -> impl Iterator<Item = std::ops::Range<u64>> {
    (0..rows).step_by(100_000).map(move |s| s..(s + 100_000).min(rows))
}

// A value: 100 bytes that depend on its key and a version.
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

fn add(d: &mut Digest, bytes: &[u8]) {
    d.row();
    d.value(
        bytes.len() as f64
            + (u32::from_le_bytes(bytes[..4].try_into().unwrap())
                ^ u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap())) as f64,
    );
}

// ---- the store ----------------------------------------------------------------------

struct StoreEngine {
    db: Arc<Db<std::fs::File>>,
    kv: store::table::TableIdType,
    ordered: store::table::TableIdType,
}

fn pair(grp: u64, seq: u64) -> DBIdType {
    DBIdType::Rec(
        IndexKey::new_from(&[ValueItem::Integer(grp as i64), ValueItem::Integer(seq as i64)])
            .unwrap(),
    )
}

impl StoreEngine {
    fn new(path: &str, profile: &str) -> Self {
        let open = match profile {
            "large" => OpenConfig::default().page_cache_bytes(GIB),
            _ => OpenConfig::default(),
        };
        let db = Db::<std::fs::File>::create_with(path, &CreateConfig::default().open(open)).unwrap();
        let kv = db.create_table("kv".into()).unwrap();
        let ordered = db.create_table_with_index_entry_size("ordered".into(), 96).unwrap();
        Self { db, kv, ordered }
    }
}

impl Engine for StoreEngine {
    fn load(&mut self, rows: u64) {
        for chunk in chunks(rows) {
            let txn = self.db.begin().unwrap();
            for k in chunk {
                self.db.insert(self.kv, Tuple::new(k, &value(k, 0)), &txn).unwrap();
            }
            self.db.commit(txn).unwrap();
        }
        for chunk in chunks(rows) {
            let txn = self.db.begin().unwrap();
            for k in chunk {
                let t = Tuple::new_with(pair(k / GROUP, k % GROUP), &value(k, 0), None, None);
                self.db.insert(self.ordered, t, &txn).unwrap();
            }
            self.db.commit(txn).unwrap();
        }
    }
    fn checkpoint(&mut self) {
        self.db.checkpoint().unwrap();
    }
    fn get(&mut self, keys: &[u64], txn_each: bool) -> Digest {
        let mut d = Digest::default();
        let mut txn = Some(self.db.begin().unwrap());
        for k in keys {
            if txn_each && txn.is_none() {
                txn = Some(self.db.begin().unwrap());
            }
            let t = txn.as_ref().unwrap();
            self.db
                .find_with(self.kv, &DBIdType::Int(*k), t, |r| add(&mut d, r.data()))
                .unwrap()
                .expect("row");
            if txn_each {
                self.db.commit(txn.take().unwrap()).unwrap();
            }
        }
        if let Some(t) = txn {
            self.db.commit(t).unwrap();
        }
        d
    }
    fn get_pair(&mut self, keys: &[u64]) -> Digest {
        let mut d = Digest::default();
        let txn = self.db.begin().unwrap();
        for k in keys {
            self.db
                .find_with(self.ordered, &pair(k / GROUP, k % GROUP), &txn, |r| add(&mut d, r.data()))
                .unwrap()
                .expect("row");
        }
        self.db.commit(txn).unwrap();
        d
    }
    fn scan(&mut self) -> Digest {
        let mut d = Digest::default();
        let txn = self.db.begin().unwrap();
        let mut c = self.db.table_scan_in_txn(self.kv, &txn).unwrap();
        while let Some(t) = c.next_ref().unwrap() {
            add(&mut d, t.data());
        }
        drop(c);
        self.db.commit(txn).unwrap();
        d
    }
    fn prefix(&mut self, grp: u64) -> Digest {
        let mut d = Digest::default();
        let prefix = IndexKey::new_from(&[ValueItem::Integer(grp as i64)]).unwrap();
        let mut c = self.db.prefix_scan(self.ordered, prefix).unwrap();
        while let Some(t) = c.next_ref().unwrap() {
            add(&mut d, t.data());
        }
        d
    }
    fn range(&mut self, from: u64, to: u64) -> Digest {
        let mut d = Digest::default();
        let mut c = self
            .db
            .range_scan_bounds(self.ordered, Bound::Included(pair(from, 0)), Bound::Excluded(pair(to, 0)))
            .unwrap();
        while let Some(t) = c.next_ref().unwrap() {
            add(&mut d, t.data());
        }
        d
    }
    fn update(&mut self, keys: &[u64], v: u8) {
        let txn = self.db.begin().unwrap();
        for k in keys {
            self.db.update(self.kv, Tuple::new(*k, &value(*k, v)), &txn).unwrap();
        }
        self.db.commit(txn).unwrap();
    }
    fn insert(&mut self, keys: &[u64]) {
        let txn = self.db.begin().unwrap();
        for k in keys {
            self.db.insert(self.kv, Tuple::new(*k, &value(*k, 0)), &txn).unwrap();
        }
        self.db.commit(txn).unwrap();
    }
    fn delete_pairs(&mut self, keys: &[u64]) {
        let txn = self.db.begin().unwrap();
        for k in keys {
            self.db.remove(self.ordered, pair(k / GROUP, k % GROUP), &txn).unwrap();
        }
        self.db.commit(txn).unwrap();
    }
    fn scan_both(&mut self) -> Digest {
        let mut d = Digest::default();
        let txn = self.db.begin().unwrap();
        for t in [self.kv, self.ordered] {
            let mut c = self.db.table_scan_in_txn(t, &txn).unwrap();
            while let Some(r) = c.next_ref().unwrap() {
                add(&mut d, r.data());
            }
        }
        self.db.commit(txn).unwrap();
        d
    }
    fn parallel_get(&self, per_thread: &[Vec<u64>]) -> Digest {
        std::thread::scope(|scope| {
            let hs: Vec<_> = per_thread
                .iter()
                .map(|ks| {
                    let db = self.db.clone();
                    let kv = self.kv;
                    scope.spawn(move || {
                        let mut d = Digest::default();
                        let txn = db.begin().unwrap();
                        for k in ks {
                            db.find_with(kv, &DBIdType::Int(*k), &txn, |r| add(&mut d, r.data()))
                                .unwrap()
                                .expect("row");
                        }
                        db.commit(txn).unwrap();
                        d
                    })
                })
                .collect();
            let mut total = Digest::default();
            for h in hs {
                total.merge(h.join().unwrap());
            }
            total
        })
    }
}

// ---- SQLite -------------------------------------------------------------------------

struct Sqlite {
    c: rusqlite::Connection,
    path: String,
    profile: String,
}

fn sqlite_open(path: &str, profile: &str) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "pragma journal_mode = wal; pragma synchronous = full; \
         pragma fullfsync = on; pragma checkpoint_fullfsync = on;",
    )
    .unwrap();
    if profile == "large" {
        c.execute_batch("pragma cache_size = -1048576;").unwrap();
    }
    c
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[i64], d: &mut Digest) {
    let mut st = c.prepare_cached(sql).unwrap();
    let mut rows = st.query(rusqlite::params_from_iter(params.iter())).unwrap();
    while let Some(r) = rows.next().unwrap() {
        add(d, r.get_ref(0).unwrap().as_blob().unwrap());
    }
}

impl Sqlite {
    fn in_txn(&mut self, f: impl FnOnce(&rusqlite::Connection)) {
        self.c.execute_batch("begin").unwrap();
        f(&self.c);
        self.c.execute_batch("commit").unwrap();
    }
}

impl Engine for Sqlite {
    fn load(&mut self, rows: u64) {
        self.c
            .execute_batch(
                "create table kv (k integer primary key, v blob not null); \
                 create table ordered (grp integer not null, seq integer not null, v blob not null, \
                 primary key (grp, seq)) without rowid;",
            )
            .unwrap();
        for chunk in chunks(rows) {
            self.in_txn(|c| {
                let mut st = c.prepare_cached("insert into kv values (?, ?)").unwrap();
                for k in chunk {
                    st.execute(rusqlite::params![k as i64, &value(k, 0)[..]]).unwrap();
                }
            });
        }
        for chunk in chunks(rows) {
            self.in_txn(|c| {
                let mut st = c.prepare_cached("insert into ordered values (?, ?, ?)").unwrap();
                for k in chunk {
                    st.execute(rusqlite::params![
                        (k / GROUP) as i64,
                        (k % GROUP) as i64,
                        &value(k, 0)[..]
                    ])
                    .unwrap();
                }
            });
        }
    }
    fn checkpoint(&mut self) {
        self.c.execute_batch("pragma wal_checkpoint(truncate)").unwrap();
    }
    fn get(&mut self, keys: &[u64], txn_each: bool) -> Digest {
        let mut d = Digest::default();
        if !txn_each {
            self.c.execute_batch("begin").unwrap();
        }
        for k in keys {
            sqlite_rows(&self.c, "select v from kv where k = ?", &[*k as i64], &mut d);
        }
        if !txn_each {
            self.c.execute_batch("commit").unwrap();
        }
        d
    }
    fn get_pair(&mut self, keys: &[u64]) -> Digest {
        let mut d = Digest::default();
        self.c.execute_batch("begin").unwrap();
        for k in keys {
            sqlite_rows(
                &self.c,
                "select v from ordered where grp = ? and seq = ?",
                &[(k / GROUP) as i64, (k % GROUP) as i64],
                &mut d,
            );
        }
        self.c.execute_batch("commit").unwrap();
        d
    }
    fn scan(&mut self) -> Digest {
        let mut d = Digest::default();
        sqlite_rows(&self.c, "select v from kv", &[], &mut d);
        d
    }
    fn prefix(&mut self, grp: u64) -> Digest {
        let mut d = Digest::default();
        sqlite_rows(&self.c, "select v from ordered where grp = ?", &[grp as i64], &mut d);
        d
    }
    fn range(&mut self, from: u64, to: u64) -> Digest {
        let mut d = Digest::default();
        sqlite_rows(
            &self.c,
            "select v from ordered where grp >= ? and grp < ?",
            &[from as i64, to as i64],
            &mut d,
        );
        d
    }
    fn update(&mut self, keys: &[u64], v: u8) {
        self.in_txn(|c| {
            let mut st = c.prepare_cached("update kv set v = ? where k = ?").unwrap();
            for k in keys {
                st.execute(rusqlite::params![&value(*k, v)[..], *k as i64]).unwrap();
            }
        });
    }
    fn insert(&mut self, keys: &[u64]) {
        self.in_txn(|c| {
            let mut st = c.prepare_cached("insert into kv values (?, ?)").unwrap();
            for k in keys {
                st.execute(rusqlite::params![*k as i64, &value(*k, 0)[..]]).unwrap();
            }
        });
    }
    fn delete_pairs(&mut self, keys: &[u64]) {
        self.in_txn(|c| {
            let mut st = c.prepare_cached("delete from ordered where grp = ? and seq = ?").unwrap();
            for k in keys {
                st.execute([(k / GROUP) as i64, (k % GROUP) as i64]).unwrap();
            }
        });
    }
    fn scan_both(&mut self) -> Digest {
        let mut d = Digest::default();
        self.c.execute_batch("begin").unwrap();
        sqlite_rows(&self.c, "select v from kv", &[], &mut d);
        sqlite_rows(&self.c, "select v from ordered", &[], &mut d);
        self.c.execute_batch("commit").unwrap();
        d
    }
    fn parallel_get(&self, per_thread: &[Vec<u64>]) -> Digest {
        std::thread::scope(|scope| {
            let hs: Vec<_> = per_thread
                .iter()
                .map(|ks| {
                    let (path, profile) = (self.path.clone(), self.profile.clone());
                    scope.spawn(move || {
                        let c = sqlite_open(&path, &profile);
                        let mut d = Digest::default();
                        c.execute_batch("begin").unwrap();
                        for k in ks {
                            sqlite_rows(&c, "select v from kv where k = ?", &[*k as i64], &mut d);
                        }
                        c.execute_batch("commit").unwrap();
                        d
                    })
                })
                .collect();
            let mut total = Digest::default();
            for h in hs {
                total.merge(h.join().unwrap());
            }
            total
        })
    }
}

// ---- redb ---------------------------------------------------------------------------

const KV: TableDefinition<u64, &[u8]> = TableDefinition::new("kv");
const ORDERED: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("ordered");

struct Redb {
    db: Arc<redb::Database>,
}

impl Redb {
    fn new(path: &str, profile: &str) -> Self {
        let mut b = redb::Database::builder();
        if profile == "large" {
            b.set_cache_size(GIB as usize);
        }
        Self {
            db: Arc::new(b.create(path).unwrap()),
        }
    }
    fn write(&self, f: impl FnOnce(&redb::WriteTransaction)) {
        let w = self.db.begin_write().unwrap();
        f(&w);
        w.commit().unwrap();
    }
}

impl Engine for Redb {
    fn load(&mut self, rows: u64) {
        for chunk in chunks(rows) {
            self.write(|w| {
                let mut t = w.open_table(KV).unwrap();
                for k in chunk {
                    t.insert(k, &value(k, 0)[..]).unwrap();
                }
            });
        }
        for chunk in chunks(rows) {
            self.write(|w| {
                let mut t = w.open_table(ORDERED).unwrap();
                for k in chunk {
                    t.insert((k / GROUP, k % GROUP), &value(k, 0)[..]).unwrap();
                }
            });
        }
    }
    fn checkpoint(&mut self) {
        // Every commit is written in place: there is no log to fold back.
    }
    fn get(&mut self, keys: &[u64], txn_each: bool) -> Digest {
        let mut d = Digest::default();
        if txn_each {
            for k in keys {
                let r = self.db.begin_read().unwrap();
                let t = r.open_table(KV).unwrap();
                add(&mut d, t.get(*k).unwrap().expect("row").value());
            }
        } else {
            let r = self.db.begin_read().unwrap();
            let t = r.open_table(KV).unwrap();
            for k in keys {
                add(&mut d, t.get(*k).unwrap().expect("row").value());
            }
        }
        d
    }
    fn get_pair(&mut self, keys: &[u64]) -> Digest {
        let mut d = Digest::default();
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(ORDERED).unwrap();
        for k in keys {
            add(&mut d, t.get((k / GROUP, k % GROUP)).unwrap().expect("row").value());
        }
        d
    }
    fn scan(&mut self) -> Digest {
        let mut d = Digest::default();
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(KV).unwrap();
        for e in t.iter().unwrap() {
            add(&mut d, e.unwrap().1.value());
        }
        d
    }
    fn prefix(&mut self, grp: u64) -> Digest {
        self.range(grp, grp + 1)
    }
    fn range(&mut self, from: u64, to: u64) -> Digest {
        let mut d = Digest::default();
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(ORDERED).unwrap();
        for e in t.range((from, 0)..(to, 0)).unwrap() {
            add(&mut d, e.unwrap().1.value());
        }
        d
    }
    fn update(&mut self, keys: &[u64], v: u8) {
        self.write(|w| {
            let mut t = w.open_table(KV).unwrap();
            for k in keys {
                t.insert(*k, &value(*k, v)[..]).unwrap();
            }
        });
    }
    fn insert(&mut self, keys: &[u64]) {
        self.update(keys, 0);
    }
    fn delete_pairs(&mut self, keys: &[u64]) {
        self.write(|w| {
            let mut t = w.open_table(ORDERED).unwrap();
            for k in keys {
                t.remove((k / GROUP, k % GROUP)).unwrap();
            }
        });
    }
    fn scan_both(&mut self) -> Digest {
        let mut d = Digest::default();
        let r = self.db.begin_read().unwrap();
        for e in r.open_table(KV).unwrap().iter().unwrap() {
            add(&mut d, e.unwrap().1.value());
        }
        for e in r.open_table(ORDERED).unwrap().iter().unwrap() {
            add(&mut d, e.unwrap().1.value());
        }
        d
    }
    fn parallel_get(&self, per_thread: &[Vec<u64>]) -> Digest {
        std::thread::scope(|scope| {
            let hs: Vec<_> = per_thread
                .iter()
                .map(|ks| {
                    let db = self.db.clone();
                    scope.spawn(move || {
                        let mut d = Digest::default();
                        let r = db.begin_read().unwrap();
                        let t = r.open_table(KV).unwrap();
                        for k in ks {
                            add(&mut d, t.get(*k).unwrap().expect("row").value());
                        }
                        d
                    })
                })
                .collect();
            let mut total = Digest::default();
            for h in hs {
                total.merge(h.join().unwrap());
            }
            total
        })
    }
}

// ---- the workload ---------------------------------------------------------------------

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

fn workload(e: &mut dyn Engine, rows: u64, threads: usize) {
    let groups = rows / GROUP;
    phase("after open", 0, || None);
    phase("Insert, both tables, 100k rows a transaction (per row)", 2 * rows, || {
        e.load(rows);
        None
    });
    phase("Checkpoint after the load", 1, || {
        e.checkpoint();
        None
    });
    phase("after load", 0, || None);

    let mut rng = Rng(0xFEED_0000_0000_0002);
    let keys: Vec<u64> = (0..200_000).map(|_| rng.below(rows)).collect();
    phase("Point lookup, integer key", keys.len() as u64, || Some(e.get(&keys, false)));
    let few = &keys[..50_000];
    phase("Point lookup, a transaction each", few.len() as u64, || Some(e.get(few, true)));
    let pairs: Vec<u64> = (0..200_000).map(|_| rng.below(rows)).collect();
    phase("Point lookup, composite key", pairs.len() as u64, || Some(e.get_pair(&pairs)));
    phase("Full scan (per row)", 10 * rows, || {
        let mut d = Digest::default();
        for _ in 0..10 {
            d.merge(e.scan());
        }
        Some(d)
    });
    let prefixes: Vec<u64> = (0..5_000).map(|_| rng.below(groups)).collect();
    phase("Prefix scan of 100 rows (per scan)", prefixes.len() as u64, || {
        let mut d = Digest::default();
        for g in &prefixes {
            d.merge(e.prefix(*g));
        }
        Some(d)
    });
    let span = (groups / 100).max(1);
    let starts: Vec<u64> = (0..200).map(|_| rng.below(groups - span)).collect();
    phase("Range scan of 1% of the table (per row)", 200 * span * GROUP, || {
        let mut d = Digest::default();
        for g in &starts {
            d.merge(e.range(*g, g + span));
        }
        Some(d)
    });
    phase("after reads", 0, || None);

    let single: Vec<u64> = (0..1_000).map(|_| rng.below(rows)).collect();
    phase("Update one row and commit", 1_000, || {
        for k in &single {
            e.update(&[*k], 1);
        }
        None
    });
    phase("Insert one row and commit", 1_000, || {
        for i in 0..1_000 {
            e.insert(&[rows + i]);
        }
        None
    });
    let batch: Vec<u64> = (0..20_000).map(|_| rng.below(rows)).collect();
    phase("Update, 100 rows a transaction (per row)", batch.len() as u64, || {
        for chunk in batch.chunks(100) {
            e.update(chunk, 2);
        }
        None
    });
    let doomed: Vec<u64> = (0..(rows / 10).min(20_000)).collect();
    phase("Delete, one transaction (per row)", doomed.len() as u64, || {
        e.delete_pairs(&doomed);
        None
    });
    phase(
        "Check: both tables after the writes (per row)",
        2 * rows + 1_000 - doomed.len() as u64,
        || Some(e.scan_both()),
    );
    phase("after writes", 0, || None);

    let per_thread = 100_000usize;
    let lookups: Vec<Vec<u64>> = (0..threads)
        .map(|t| {
            let mut r = Rng(0xABCD_0000 + t as u64 * 7919 + 1);
            (0..per_thread).map(|_| r.below(rows)).collect()
        })
        .collect();
    phase(
        &format!("Point lookups on {threads} threads (per lookup)"),
        (threads * per_thread) as u64,
        || Some(e.parallel_get(&lookups)),
    );
    phase("at end", 0, || None);
}

fn child(a: ChildArgs) {
    let path = |name: &str| a.dir.join(name).to_string_lossy().into_owned();
    let mut engine: Box<dyn Engine> = match a.engine.as_str() {
        "store" => {
            let e = StoreEngine::new(&path("kv.store"), &a.profile);
            bench::info(&format!("page cache {} MiB", e.db.cache_bytes() >> 20));
            Box::new(e)
        }
        "sqlite" => {
            let p = path("kv.sqlite");
            Box::new(Sqlite {
                c: sqlite_open(&p, &a.profile),
                path: p,
                profile: a.profile.clone(),
            })
        }
        "redb" => Box::new(Redb::new(&path("kv.redb"), &a.profile)),
        other => panic!("unknown engine {other}"),
    };
    workload(engine.as_mut(), a.size, a.threads);
}

fn main() {
    if let Some(a) = child_args() {
        return child(a);
    }
    let plan = plan_from_args(ENGINES, PROFILES, 200_000);
    drive(&plan, "The store, SQLite and redb as key-value stores");
}
