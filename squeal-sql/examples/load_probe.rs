// Bulk-load timing alone: `cargo run --release -p squeal-sql --example load_probe -- [rows] [index]`.
use std::{sync::Arc, time::Instant};
use squeal_sql::conn::connection::ConnectionManager;
fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let dir = std::env::temp_dir().join(format!("load_probe_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("p.sq").to_string_lossy().into_owned();
    let mgr = Arc::new(ConnectionManager::<std::fs::File>::new());
    let c = mgr.create_and_connect(&path).unwrap();
    c.use_schema("default").unwrap();
    let run = |sql: &str| { let mut s = c.clone().create_statement(sql).unwrap(); s.execute().unwrap(); while let Some(_) = s.get_nextresult().unwrap() {} };
    run("create table orders (id integer not null, customer_id integer, item varchar(12), qty integer, price double, day integer, primary key(id))");
    let rows: Vec<String> = (0..n).map(|i| format!("({i}, {}, 'stapler', {}, {}.25, {})", (i * 7919) % 20000, i % 10, i % 50, (i * 31) % 365)).collect();
    let stmts: Vec<String> = rows.chunks(500).map(|ch| format!("insert into orders values {}", ch.join(","))).collect();
    run("begin");
    let t = Instant::now();
    for s in &stmts { run(s); }
    run("commit");
    println!("load {n}: {:?}", t.elapsed());
    for (name, sql) in [("index customer_id", "create index o_c on orders (customer_id)"), ("index day", "create index o_d on orders (day)")] {
        let t = Instant::now(); run(sql); println!("{name}: {:?}", t.elapsed());
    }
    let _ = std::fs::remove_dir_all(&dir);
}
