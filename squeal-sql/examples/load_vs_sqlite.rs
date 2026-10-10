//! One workload, run against squeal-db (squeal-sql on a file) and SQLite
//! (rusqlite, bundled), phase by phase, with each phase's answers compared.
//!
//!   cargo run --release -p squeal-sql --example load_vs_sqlite -- [orders] [threads]
//!
//! Both are on disk and equally durable: every commit waits for a full
//! flush to stable storage. squeal-db's commit is `File::sync_data`, which
//! on macOS is F_FULLFSYNC, so SQLite runs WAL with synchronous=FULL and
//! fullfsync=ON. SQLite uses prepared statements throughout (its usual
//! use); squeal-db is sent SQL text, which its parse cache shares a parse
//! across (see sql_parser::parse_sql_cached).
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::ResultType,
};
use store::valueitem::ValueItem;

type Sq = Arc<Connection<std::fs::File>>;

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

// A query's answer, reduced to something both engines agree on: the row
// count and the sum of every number in it (rounded), strings by length.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Digest {
    rows: u64,
    sum: f64,
}

impl Digest {
    fn add(&mut self, other: Digest) {
        self.rows += other.rows;
        self.sum += other.sum;
    }
    fn agrees(&self, o: &Digest) -> bool {
        self.rows == o.rows && (self.sum - o.sum).abs() <= 1e-6 * self.sum.abs().max(1.0)
    }
}

fn sq_conn(path: &str) -> (Arc<ConnectionManager<std::fs::File>>, Sq) {
    let mgr = Arc::new(ConnectionManager::<std::fs::File>::new());
    let c = mgr.create_and_connect(path).expect("create squeal db");
    c.use_schema("default").expect("default schema");
    (mgr, c)
}

fn sq(c: &Sq, sql: &str) -> Digest {
    let mut stmt = c.clone().create_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    stmt.execute().unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut d = Digest { rows: 0, sum: 0.0 };
    while let Some(r) = stmt.get_nextresult().unwrap() {
        if let ResultType::StreamingResult(mut s) = r {
            while let Some(row) = s.next_result().unwrap() {
                d.rows += 1;
                for v in row.values() {
                    d.sum += match v {
                        ValueItem::Integer(i) => *i as f64,
                        ValueItem::Double(f) => *f,
                        ValueItem::Str((s, _)) => s.len() as f64,
                        ValueItem::Boolean(b) => *b as i64 as f64,
                        _ => 0.0,
                    };
                }
            }
        }
    }
    d
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

fn lite(c: &rusqlite::Connection, sql: &str, params: &[i64]) -> Digest {
    let mut stmt = c.prepare_cached(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let n = stmt.column_count();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .unwrap();
    let mut d = Digest { rows: 0, sum: 0.0 };
    while let Some(r) = rows.next().unwrap() {
        d.rows += 1;
        for i in 0..n {
            d.sum += match r.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Integer(i) => i as f64,
                rusqlite::types::ValueRef::Real(f) => f,
                rusqlite::types::ValueRef::Text(t) => t.len() as f64,
                _ => 0.0,
            };
        }
    }
    d
}

// `sql` with each `?` replaced by the next of `params`: the text squeal is
// sent for what SQLite runs as a prepared statement.
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

struct Phase {
    name: String,
    ops: u64,
    squeal: Duration,
    sqlite: Duration,
    agree: bool,
}

fn report(p: &Phase) {
    let rate = |d: Duration| p.ops as f64 / d.as_secs_f64();
    let ratio = p.sqlite.as_secs_f64() / p.squeal.as_secs_f64();
    println!(
        "| {:<44} | {:>7} | {:>10.1} ms | {:>11.0}/s | {:>10.1} ms | {:>11.0}/s | {:>6.2}x | {} |",
        p.name,
        p.ops,
        p.squeal.as_secs_f64() * 1e3,
        rate(p.squeal),
        p.sqlite.as_secs_f64() * 1e3,
        rate(p.sqlite),
        ratio,
        if p.agree { "yes" } else { "NO" }
    );
}

fn phase(
    phases: &mut Vec<Phase>,
    name: &str,
    ops: u64,
    run_sq: &mut dyn FnMut() -> Digest,
    run_lite: &mut dyn FnMut() -> Digest,
) {
    let t = Instant::now();
    let a = run_sq();
    let squeal = t.elapsed();
    let t = Instant::now();
    let b = run_lite();
    let sqlite = t.elapsed();
    let agree = a.agrees(&b);
    if !agree {
        eprintln!("  {name}: squeal {a:?} sqlite {b:?}");
    }
    let p = Phase {
        name: name.into(),
        ops,
        squeal,
        sqlite,
        agree,
    };
    report(&p);
    phases.push(p);
}

// Runs `sql` once per parameter set on each engine, summing the answers.
fn queries(
    phases: &mut Vec<Phase>,
    s: &Sq,
    l: &rusqlite::Connection,
    name: &str,
    sql: &str,
    params: Vec<Vec<i64>>,
) {
    let n = params.len() as u64;
    phase(
        phases,
        name,
        n,
        &mut || {
            let mut d = Digest { rows: 0, sum: 0.0 };
            for p in &params {
                d.add(sq(s, &bind(sql, p)));
            }
            d
        },
        &mut || {
            let mut d = Digest { rows: 0, sum: 0.0 };
            for p in &params {
                d.add(lite(l, sql, p));
            }
            d
        },
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let orders: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let threads: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let customers = (orders / 10).max(10);
    let days = 365u64;

    let dir = std::env::temp_dir().join(format!("squeal_vs_sqlite_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let sq_path = dir.join("bench.sq").to_string_lossy().into_owned();
    let lite_path = dir.join("bench.sqlite").to_string_lossy().into_owned();
    let (mgr, s) = sq_conn(&sq_path);
    let l = lite_conn(&lite_path);

    println!(
        "squeal-db vs SQLite {}: {customers} customers, {orders} orders, {threads} reader threads",
        rusqlite::version()
    );
    println!("both on disk, every commit fully flushed (F_FULLFSYNC); dir {}\n", dir.display());
    println!(
        "| phase | ops | squeal-db | squeal-db rate | SQLite | SQLite rate | squeal-db speed vs SQLite | same answers |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---|");

    let mut phases: Vec<Phase> = vec![];

    // ---- schema and load -------------------------------------------------
    let ddl = [
        "create table customers (id integer not null, name varchar(20), city varchar(12), \
         tier integer, primary key(id))",
        "create table orders (id integer not null, customer_id integer, item varchar(12), \
         qty integer, price double, day integer, primary key(id))",
    ];
    for d in ddl {
        sq(&s, d);
        l.execute_batch(d).unwrap();
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
    phase(
        &mut phases,
        "bulk load (500-row INSERTs, one transaction)",
        customers + orders,
        &mut || {
            sq(&s, "begin");
            for i in &inserts {
                sq(&s, i);
            }
            sq(&s, "commit");
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            l.execute_batch("begin").unwrap();
            for i in &inserts {
                l.execute_batch(i).unwrap();
            }
            l.execute_batch("commit").unwrap();
            Digest { rows: 0, sum: 0.0 }
        },
    );
    let indexes = [
        "create index orders_customer on orders (customer_id)",
        "create index orders_day on orders (day)",
        "create index customers_tier on customers (tier)",
    ];
    phase(
        &mut phases,
        "create 3 indexes",
        3,
        &mut || {
            for i in indexes {
                sq(&s, i);
            }
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            for i in indexes {
                l.execute_batch(i).unwrap();
            }
            Digest { rows: 0, sum: 0.0 }
        },
    );
    phase(
        &mut phases,
        "ANALYZE",
        1,
        &mut || {
            sq(&s, "analyze tables");
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            l.execute_batch("analyze").unwrap();
            Digest { rows: 0, sum: 0.0 }
        },
    );

    // ---- reads -------------------------------------------------------------
    // SQ_EXPLAIN=1: the plans squeal-db picks for the read phases.
    if std::env::var_os("SQ_EXPLAIN").is_some() {
        for q in [
            "select count(*), sum(qty * price) from orders where day >= 10 and day < 17",
            "select c.name, o.id, o.qty from orders o join customers c on c.id = o.customer_id \
             where o.day = 10 order by o.qty desc, o.id limit 10",
            "select count(*) from customers c where not exists \
             (select 1 from orders o where o.customer_id = c.id and o.day < 50)",
        ] {
            let mut stmt = s.clone().create_statement(&format!("explain {q}")).unwrap();
            stmt.execute().unwrap();
            if let Some(ResultType::ResultString(plan)) = stmt.get_results().unwrap() {
                eprintln!("{q}\n{plan}");
            }
        }
    }
    let mut rng = Rng(0xFEED_0000_0000_0002);
    let ids = |rng: &mut Rng, n: usize, below: u64| -> Vec<Vec<i64>> {
        (0..n).map(|_| vec![rng.below(below) as i64]).collect()
    };
    queries(
        &mut phases,
        &s,
        &l,
        "point lookup by primary key",
        "select id, customer_id, item, qty, price from orders where id = ?",
        ids(&mut rng, 20_000, orders),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "secondary-index lookup + aggregate",
        "select count(*), sum(qty) from orders where customer_id = ?",
        ids(&mut rng, 5_000, customers),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "range scan on index (1 week) + aggregate",
        "select count(*), sum(qty * price) from orders where day >= ? and day < ?",
        (0..200)
            .map(|_| {
                let d = rng.below(days - 7) as i64;
                vec![d, d + 7]
            })
            .collect(),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "full scan GROUP BY",
        "select item, count(*), sum(qty), avg(price) from orders group by item",
        vec![vec![]; 10],
    );
    queries(
        &mut phases,
        &s,
        &l,
        "join + GROUP BY",
        "select c.city, count(*), sum(o.qty * o.price) from customers c \
         join orders o on o.customer_id = c.id group by c.city",
        vec![vec![]; 5],
    );
    queries(
        &mut phases,
        &s,
        &l,
        "join, filtered, ORDER BY + LIMIT",
        "select c.name, o.id, o.qty from orders o join customers c on c.id = o.customer_id \
         where o.day = ? order by o.qty desc, o.id limit 10",
        ids(&mut rng, 200, days),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "IN (subquery)",
        "select count(*), sum(qty) from orders where customer_id in \
         (select id from customers where tier = ?)",
        ids(&mut rng, 20, 5),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "correlated EXISTS",
        "select count(*) from customers c where exists \
         (select 1 from orders o where o.customer_id = c.id and o.qty > ?)",
        ids(&mut rng, 20, 10),
    );
    queries(
        &mut phases,
        &s,
        &l,
        "correlated NOT EXISTS",
        "select count(*) from customers c where not exists \
         (select 1 from orders o where o.customer_id = c.id and o.day < ?)",
        ids(&mut rng, 20, 100),
    );

    // ---- durable writes ------------------------------------------------------
    let updates: Vec<i64> = (0..1_000).map(|_| rng.below(orders) as i64).collect();
    phase(
        &mut phases,
        "UPDATE one row, autocommit (fsync each)",
        updates.len() as u64,
        &mut || {
            for id in &updates {
                sq(&s, &format!("update orders set qty = qty + 1 where id = {id}"));
            }
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            let mut st = l.prepare_cached("update orders set qty = qty + 1 where id = ?").unwrap();
            for id in &updates {
                st.execute([id]).unwrap();
            }
            Digest { rows: 0, sum: 0.0 }
        },
    );
    phase(
        &mut phases,
        "INSERT one row, autocommit (fsync each)",
        1_000,
        &mut || {
            for i in 0..1_000u64 {
                let id = orders + i;
                sq(
                    &s,
                    &format!("insert into orders values ({id}, {}, 'pen', 1, 1.5, 7)", i % customers),
                );
            }
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            let mut st = l
                .prepare_cached("insert into orders values (?, ?, 'pen', 1, 1.5, 7)")
                .unwrap();
            for i in 0..1_000u64 {
                st.execute([(orders + i) as i64, (i % customers) as i64]).unwrap();
            }
            Digest { rows: 0, sum: 0.0 }
        },
    );
    phase(
        &mut phases,
        "UPDATE 100 rows per transaction x 10",
        1_000,
        &mut || {
            for chunk in updates.chunks(100) {
                sq(&s, "begin");
                for id in chunk {
                    sq(&s, &format!("update orders set price = price + 1 where id = {id}"));
                }
                sq(&s, "commit");
            }
            Digest { rows: 0, sum: 0.0 }
        },
        &mut || {
            for chunk in updates.chunks(100) {
                l.execute_batch("begin").unwrap();
                let mut st = l
                    .prepare_cached("update orders set price = price + 1 where id = ?")
                    .unwrap();
                for id in chunk {
                    st.execute([id]).unwrap();
                }
                drop(st);
                l.execute_batch("commit").unwrap();
            }
            Digest { rows: 0, sum: 0.0 }
        },
    );
    queries(
        &mut phases,
        &s,
        &l,
        "check: totals after the writes",
        "select count(*), sum(qty), sum(price) from orders",
        vec![vec![]],
    );

    // ---- concurrency -------------------------------------------------------
    let per_thread = 10_000usize;
    let lookups: Vec<Vec<i64>> = (0..threads)
        .map(|t| {
            let mut r = Rng(0xABCD_0000 + t as u64 * 7919 + 1);
            (0..per_thread).map(|_| r.below(orders) as i64).collect()
        })
        .collect();
    let sql = "select id, customer_id, item, qty, price from orders where id = ?";
    phase(
        &mut phases,
        &format!("point lookups, {threads} threads"),
        (threads * per_thread) as u64,
        &mut || {
            let total = std::thread::scope(|scope| {
                let handles: Vec<_> = lookups
                    .iter()
                    .map(|ids| {
                        let mgr = mgr.clone();
                        let path = sq_path.clone();
                        scope.spawn(move || {
                            let c = mgr.connect(&path).unwrap();
                            c.use_schema("default").unwrap();
                            let mut d = Digest { rows: 0, sum: 0.0 };
                            for id in ids {
                                d.add(sq(&c, &bind(sql, &[*id])));
                            }
                            d
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
            });
            total.into_iter().fold(Digest { rows: 0, sum: 0.0 }, |mut a, b| {
                a.add(b);
                a
            })
        },
        &mut || {
            let total = std::thread::scope(|scope| {
                let handles: Vec<_> = lookups
                    .iter()
                    .map(|ids| {
                        let path = lite_path.clone();
                        scope.spawn(move || {
                            let c = lite_conn(&path);
                            let mut d = Digest { rows: 0, sum: 0.0 };
                            for id in ids {
                                d.add(lite(&c, sql, &[*id]));
                            }
                            d
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
            });
            total.into_iter().fold(Digest { rows: 0, sum: 0.0 }, |mut a, b| {
                a.add(b);
                a
            })
        },
    );

    let disagreements = phases.iter().filter(|p| !p.agree).count();
    println!();
    println!(
        "squeal-db speed vs SQLite = SQLite time / squeal-db time (above 1: squeal-db faster). \
         {disagreements} phase(s) with different answers."
    );
    let size = |p: &std::path::Path| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    let total = |prefix: &str| -> u64 {
        std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
            .map(|e| size(&e.path()))
            .sum()
    };
    println!(
        "on disk: squeal-db {:.1} MB, SQLite {:.1} MB",
        total("bench.sq.") as f64 / 1e6 + size(std::path::Path::new(&sq_path)) as f64 / 1e6,
        total("bench.sqlite") as f64 / 1e6
    );
    drop(l);
    let _ = std::fs::remove_dir_all(&dir);
}
