//! squeal-db, SQLite and Turso on one SQL workload — the one squeal-sql's
//! `load_vs_sqlite` example runs — each in a process of its own, with its
//! memory measured, under each memory profile.
//!
//!   cargo run --release -p bench --bin sql -- [--size ORDERS] [--threads N]
//!       [--runs N] [--engines squeal,sqlite,turso] [--profiles default,large]
//!
//! Every engine is on disk and every commit waits for a full flush
//! (F_FULLFSYNC on macOS): squeal-db always; SQLite and Turso in WAL mode
//! with synchronous=FULL and fullfsync on. SQLite and Turso run prepared
//! statements; squeal-db is sent SQL text (its parse cache shares parses).
use std::sync::Arc;

use bench::{ChildArgs, Digest, block_on, child_args, drive, phase, plan_from_args};
use squeal_sql::{
    CreateConfig, OpenConfig,
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::ResultType,
};
use store::valueitem::ValueItem;

const GIB: u64 = 1 << 30;

const ENGINES: &[(&str, &str)] = &[
    ("squeal", "squeal-db (squeal-sql on store)"),
    ("sqlite", "SQLite 3.53 through rusqlite, bundled"),
    ("turso", "Turso 0.8.2, the Rust rewrite of SQLite"),
];

const PROFILES: &[(&str, &str)] = &[
    (
        "default",
        "each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; \
         SQLite: ~2 MiB cache; Turso: 2,000-page cache",
    ),
    (
        "large",
        "1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, \
         SQLite and Turso temp tables in memory",
    ),
];

/// What the workload needs from an engine.
trait Engine {
    /// A statement with no results: DDL, BEGIN/COMMIT, a literal INSERT.
    fn run(&mut self, sql: &str);
    /// A query with `?` parameters, its rows digested.
    fn query(&mut self, sql: &str, params: &[i64]) -> Digest;
    /// A write with `?` parameters.
    fn exec(&mut self, sql: &str, params: &[i64]);
    /// `sql` once per parameter, each thread its own connection.
    fn parallel(&self, sql: &str, per_thread: &[Vec<i64>]) -> Digest;
}

// ---- squeal-db --------------------------------------------------------------------

struct Squeal {
    mgr: Arc<ConnectionManager<std::fs::File>>,
    conn: Arc<Connection<std::fs::File>>,
    path: String,
}

// `?` replaced by the parameters: squeal-db is sent text.
fn bind(sql: &str, params: &[i64]) -> String {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut p = params.iter();
    for ch in sql.chars() {
        if ch == '?' {
            out.push_str(&p.next().expect("a value per ?").to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

fn squeal_query(conn: &Arc<Connection<std::fs::File>>, sql: &str) -> Digest {
    let mut stmt = conn
        .clone()
        .create_statement(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    stmt.execute().unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut d = Digest::default();
    while let Some(r) = stmt.get_nextresult().unwrap() {
        if let ResultType::StreamingResult(mut s) = r {
            while let Some(row) = s.next_result().unwrap() {
                d.row();
                for v in row.values() {
                    d.value(match v {
                        ValueItem::Integer(i) => *i as f64,
                        ValueItem::Double(f) => *f,
                        ValueItem::Str((s, _)) => s.len() as f64,
                        ValueItem::Boolean(b) => *b as i64 as f64,
                        _ => 0.0,
                    });
                }
            }
        }
    }
    d
}

fn a_config(profile: &str) -> OpenConfig {
    match profile {
        "large" => OpenConfig::default()
            .page_cache_bytes(GIB)
            .query_memory_bytes(GIB / 2)
            .temp_cache_bytes(GIB / 2),
        _ => OpenConfig::default(),
    }
}

impl Squeal {
    fn new(path: &str, profile: &str) -> Self {
        let config = CreateConfig::default().open(a_config(profile));
        let mgr = Arc::new(ConnectionManager::with_config(config));
        let conn = mgr.create_and_connect(path).unwrap();
        conn.use_schema("default").unwrap();
        Self {
            mgr,
            conn,
            path: path.into(),
        }
    }
}

impl Engine for Squeal {
    fn run(&mut self, sql: &str) {
        // squeal-db's spelling of SQLite's ANALYZE.
        let sql = if sql == "analyze" { "analyze tables" } else { sql };
        squeal_query(&self.conn, sql);
    }
    fn query(&mut self, sql: &str, params: &[i64]) -> Digest {
        squeal_query(&self.conn, &bind(sql, params))
    }
    fn exec(&mut self, sql: &str, params: &[i64]) {
        squeal_query(&self.conn, &bind(sql, params));
    }
    fn parallel(&self, sql: &str, per_thread: &[Vec<i64>]) -> Digest {
        std::thread::scope(|scope| {
            let handles: Vec<_> = per_thread
                .iter()
                .map(|ids| {
                    let mgr = self.mgr.clone();
                    let path = self.path.clone();
                    scope.spawn(move || {
                        let c = mgr.connect(&path).unwrap();
                        c.use_schema("default").unwrap();
                        let mut d = Digest::default();
                        for id in ids {
                            d.merge(squeal_query(&c, &bind(sql, &[*id])));
                        }
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
    }
}

// ---- SQLite -----------------------------------------------------------------------

struct Sqlite {
    conn: rusqlite::Connection,
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
        // Negative: KiB.
        c.execute_batch("pragma cache_size = -1048576; pragma temp_store = memory;")
            .unwrap();
    }
    c
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[i64]) -> Digest {
    let mut stmt = c.prepare_cached(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let n = stmt.column_count();
    let mut rows = stmt.query(rusqlite::params_from_iter(params.iter())).unwrap();
    let mut d = Digest::default();
    while let Some(r) = rows.next().unwrap() {
        d.row();
        for i in 0..n {
            d.value(match r.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Integer(i) => i as f64,
                rusqlite::types::ValueRef::Real(f) => f,
                rusqlite::types::ValueRef::Text(t) => t.len() as f64,
                rusqlite::types::ValueRef::Blob(b) => b.len() as f64,
                rusqlite::types::ValueRef::Null => 0.0,
            });
        }
    }
    d
}

impl Engine for Sqlite {
    fn run(&mut self, sql: &str) {
        self.conn.execute_batch(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    fn query(&mut self, sql: &str, params: &[i64]) -> Digest {
        sqlite_rows(&self.conn, sql, params)
    }
    fn exec(&mut self, sql: &str, params: &[i64]) {
        let mut st = self.conn.prepare_cached(sql).unwrap();
        st.execute(rusqlite::params_from_iter(params.iter())).unwrap();
    }
    fn parallel(&self, sql: &str, per_thread: &[Vec<i64>]) -> Digest {
        std::thread::scope(|scope| {
            let handles: Vec<_> = per_thread
                .iter()
                .map(|ids| {
                    let (path, profile) = (self.path.clone(), self.profile.clone());
                    scope.spawn(move || {
                        let c = sqlite_open(&path, &profile);
                        let mut d = Digest::default();
                        for id in ids {
                            d.merge(sqlite_rows(&c, sql, &[*id]));
                        }
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
    }
}

// ---- Turso ------------------------------------------------------------------------

struct Turso {
    db: turso::Database,
    conn: turso::Connection,
    profile: String,
}

fn turso_setup(conn: &turso::Connection, profile: &str) {
    for pragma in ["pragma synchronous = full", "pragma fullfsync = on"] {
        block_on(conn.execute(pragma, ())).unwrap_or_else(|e| panic!("{pragma}: {e}"));
    }
    if profile == "large" {
        for pragma in ["pragma cache_size = -1048576", "pragma temp_store = memory"] {
            // Not every SQLite pragma is in Turso: what it refuses is left
            // at its default.
            if let Err(e) = block_on(conn.execute(pragma, ())) {
                eprintln!("turso: {pragma}: {e}");
            }
        }
    }
}

fn turso_rows(c: &turso::Connection, sql: &str, params: &[i64]) -> Digest {
    let mut stmt = block_on(c.prepare_cached(sql)).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = block_on(stmt.query(params.to_vec())).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut d = Digest::default();
    while let Some(r) = block_on(rows.next()).unwrap() {
        d.row();
        for i in 0..r.column_count() {
            d.value(match r.get_value(i).unwrap() {
                turso::Value::Integer(i) => i as f64,
                turso::Value::Real(f) => f,
                turso::Value::Text(t) => t.len() as f64,
                turso::Value::Blob(b) => b.len() as f64,
                turso::Value::Null => 0.0,
            });
        }
    }
    d
}

impl Engine for Turso {
    fn run(&mut self, sql: &str) {
        block_on(self.conn.execute_batch(sql)).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    fn query(&mut self, sql: &str, params: &[i64]) -> Digest {
        turso_rows(&self.conn, sql, params)
    }
    fn exec(&mut self, sql: &str, params: &[i64]) {
        let mut st = block_on(self.conn.prepare_cached(sql)).unwrap();
        block_on(st.execute(params.to_vec())).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    fn parallel(&self, sql: &str, per_thread: &[Vec<i64>]) -> Digest {
        std::thread::scope(|scope| {
            let handles: Vec<_> = per_thread
                .iter()
                .map(|ids| {
                    let c = self.db.connect().unwrap();
                    turso_setup(&c, &self.profile);
                    scope.spawn(move || {
                        let mut d = Digest::default();
                        for id in ids {
                            d.merge(turso_rows(&c, sql, &[*id]));
                        }
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
    }
}

// ---- the workload -------------------------------------------------------------------

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

const CITIES: &[&str] = &["london", "paris", "tokyo", "seattle", "lagos", "lima", "oslo", "pune"];
const ITEMS: &[&str] = &[
    "pen", "ink", "pad", "stapler", "clip", "tape", "glue", "ruler", "marker", "folder",
];

fn workload(e: &mut dyn Engine, orders: u64, threads: usize) {
    let customers = (orders / 10).max(10);
    let days = 365u64;
    phase("after open", 0, || None);

    for d in [
        "create table customers (id integer not null, name varchar(20), city varchar(12), \
         tier integer, primary key(id))",
        "create table orders (id integer not null, customer_id integer, item varchar(12), \
         qty integer, price double, day integer, primary key(id))",
    ] {
        e.run(d);
    }
    let mut rng = Rng(0x5EED_CAFE_F00D_0001);
    let customer_rows: Vec<String> = (0..customers)
        .map(|id| {
            format!(
                "({id}, 'customer {id}', '{}', {})",
                CITIES[rng.below(CITIES.len() as u64) as usize],
                rng.below(5)
            )
        })
        .collect();
    let order_rows: Vec<String> = (0..orders)
        .map(|id| {
            let qty = 1 + rng.below(10);
            let price = (rng.below(5000) as f64) / 100.0 + 0.25;
            format!(
                "({id}, {}, '{}', {qty}, {price}, {})",
                rng.below(customers),
                ITEMS[rng.below(ITEMS.len() as u64) as usize],
                rng.below(days)
            )
        })
        .collect();
    let batches = |rows: &[String], table: &str| -> Vec<String> {
        rows.chunks(500)
            .map(|c| format!("insert into {table} values {}", c.join(", ")))
            .collect()
    };
    let inserts: Vec<String> = batches(&customer_rows, "customers")
        .into_iter()
        .chain(batches(&order_rows, "orders"))
        .collect();
    drop((customer_rows, order_rows));

    // 100,000 rows (200 INSERTs) a transaction. squeal-db keeps a version
    // record per row a transaction writes until it commits, capped at a
    // million by default, so one transaction can't load a million rows.
    phase("Bulk load, 100k rows a transaction (per row)", customers + orders, || {
        for chunk in inserts.chunks(200) {
            e.run("begin");
            for i in chunk {
                e.run(i);
            }
            e.run("commit");
        }
        None
    });
    drop(inserts);
    phase("Create 3 indexes (per index)", 3, || {
        e.run("create index orders_customer on orders (customer_id)");
        e.run("create index orders_day on orders (day)");
        e.run("create index customers_tier on customers (tier)");
        None
    });
    phase("`ANALYZE`", 1, || {
        e.run("analyze");
        None
    });
    phase("after load", 0, || None);

    let mut rng = Rng(0xFEED_0000_0000_0002);
    let ids = |rng: &mut Rng, n: usize, below: u64| -> Vec<Vec<i64>> {
        (0..n).map(|_| vec![rng.below(below) as i64]).collect()
    };
    fn queries(e: &mut dyn Engine, name: &str, sql: &str, params: Vec<Vec<i64>>) {
        phase(name, params.len() as u64, || {
            let mut d = Digest::default();
            for p in &params {
                d.merge(e.query(sql, p));
            }
            Some(d)
        });
    }
    queries(
        e,
        "Point lookup by primary key",
        "select id, customer_id, item, qty, price from orders where id = ?",
        ids(&mut rng, 20_000, orders),
    );
    queries(
        e,
        "Secondary-index lookup + aggregate",
        "select count(*), sum(qty) from orders where customer_id = ?",
        ids(&mut rng, 5_000, customers),
    );
    queries(
        e,
        "Index range (1 week of 52) + aggregate",
        "select count(*), sum(qty * price) from orders where day >= ? and day < ?",
        (0..200)
            .map(|_| {
                let d = rng.below(days - 7) as i64;
                vec![d, d + 7]
            })
            .collect(),
    );
    queries(
        e,
        "Full scan with `GROUP BY`",
        "select item, count(*), sum(qty), avg(price) from orders group by item",
        vec![vec![]; 10],
    );
    queries(
        e,
        "Join + `GROUP BY`",
        "select c.city, count(*), sum(o.qty * o.price) from customers c \
         join orders o on o.customer_id = c.id group by c.city",
        vec![vec![]; 5],
    );
    queries(
        e,
        "Join, filtered, `ORDER BY` + `LIMIT`",
        "select c.name, o.id, o.qty from orders o join customers c on c.id = o.customer_id \
         where o.day = ? order by o.qty desc, o.id limit 10",
        ids(&mut rng, 200, days),
    );
    queries(
        e,
        "Sort the whole table (`ORDER BY` without an index)",
        "select id, qty from orders order by price desc, id",
        vec![vec![]; 3],
    );
    queries(
        e,
        "`IN (subquery)`",
        "select count(*), sum(qty) from orders where customer_id in \
         (select id from customers where tier = ?)",
        ids(&mut rng, 20, 5),
    );
    queries(
        e,
        "Correlated `EXISTS`",
        "select count(*) from customers c where exists \
         (select 1 from orders o where o.customer_id = c.id and o.qty > ?)",
        ids(&mut rng, 20, 10),
    );
    queries(
        e,
        "Correlated `NOT EXISTS`",
        "select count(*) from customers c where not exists \
         (select 1 from orders o where o.customer_id = c.id and o.day < ?)",
        ids(&mut rng, 20, 100),
    );
    phase("after reads", 0, || None);

    let updates: Vec<i64> = (0..1_000).map(|_| rng.below(orders) as i64).collect();
    phase("`UPDATE` one row, autocommit", updates.len() as u64, || {
        for id in &updates {
            e.exec("update orders set qty = qty + 1 where id = ?", &[*id]);
        }
        None
    });
    phase("`INSERT` one row, autocommit", 1_000, || {
        for i in 0..1_000u64 {
            e.exec(
                "insert into orders values (?, ?, 'pen', 1, 1.5, 7)",
                &[(orders + i) as i64, (i % customers) as i64],
            );
        }
        None
    });
    phase("`UPDATE`, 100 rows per transaction (per row)", 1_000, || {
        for chunk in updates.chunks(100) {
            e.run("begin");
            for id in chunk {
                e.exec("update orders set price = price + 1 where id = ?", &[*id]);
            }
            e.run("commit");
        }
        None
    });
    queries(
        e,
        "Check: totals after the writes",
        "select count(*), sum(qty), sum(price) from orders",
        vec![vec![]],
    );
    phase("after writes", 0, || None);

    let per_thread = 10_000usize;
    let lookups: Vec<Vec<i64>> = (0..threads)
        .map(|t| {
            let mut r = Rng(0xABCD_0000 + t as u64 * 7919 + 1);
            (0..per_thread).map(|_| r.below(orders) as i64).collect()
        })
        .collect();
    phase(
        &format!("Point lookups, {threads} threads (per lookup)"),
        (threads * per_thread) as u64,
        || {
            Some(e.parallel(
                "select id, customer_id, item, qty, price from orders where id = ?",
                &lookups,
            ))
        },
    );
    phase("at end", 0, || None);
}

fn child(a: ChildArgs) {
    let path = |name: &str| a.dir.join(name).to_string_lossy().into_owned();
    let mut engine: Box<dyn Engine> = match a.engine.as_str() {
        "squeal" => Box::new(Squeal::new(&path("bench.sq"), &a.profile)),
        "sqlite" => Box::new(Sqlite {
            conn: sqlite_open(&path("bench.sqlite"), &a.profile),
            path: path("bench.sqlite"),
            profile: a.profile.clone(),
        }),
        "turso" => {
            let db = block_on(turso::Builder::new_local(&path("bench.turso")).build()).unwrap();
            let conn = db.connect().unwrap();
            turso_setup(&conn, &a.profile);
            Box::new(Turso {
                db,
                conn,
                profile: a.profile.clone(),
            })
        }
        other => panic!("unknown engine {other}"),
    };
    // What it is really running with, read back.
    match a.engine.as_str() {
        "squeal" => bench::info(&format!(
            "page cache {} MiB, query memory {} MiB, scratch {} MiB",
            a_config(&a.profile).page_cache_bytes >> 20,
            a_config(&a.profile).query_memory_bytes >> 20,
            a_config(&a.profile).temp_cache_bytes >> 20,
        )),
        _ => {
            let mut said = vec![];
            for p in ["journal_mode", "synchronous", "fullfsync", "cache_size"] {
                let d = engine.query(&format!("pragma {p}"), &[]);
                // Digested: text by its length, so WAL reads 3.
                said.push(match (p, d.sum as i64) {
                    ("journal_mode", 3) => "WAL".to_string(),
                    ("synchronous", 2) => "synchronous=FULL".to_string(),
                    ("fullfsync", 1) => "fullfsync on".to_string(),
                    ("cache_size", n) if n < 0 => {
                        format!("{:.1} MiB page cache", -n as f64 / 1024.0)
                    }
                    (p, n) => format!("{p}={n}"),
                });
            }
            bench::info(&said.join(", "));
        }
    }
    workload(engine.as_mut(), a.size, a.threads);
}

fn main() {
    if let Some(a) = child_args() {
        return child(a);
    }
    let plan = plan_from_args(ENGINES, PROFILES, 200_000);
    drive(&plan, "squeal-db, SQLite and Turso: one SQL workload");
}
