//! Rough timings at scale: `cargo run --release -p sq-json --example bench
//! -- <scratch file> [documents]`. The file is created, so it must not
//! exist.

use std::time::Instant;

use sq_json::{Client, Collection, Document, FindOptions, IndexOptions};

fn d(json: &str) -> Document {
    Document::parse(json).unwrap()
}

fn time<T>(what: &str, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    println!("{what:<48} {:>9.1} ms", start.elapsed().as_secs_f64() * 1000.0);
    out
}

fn q(c: &Collection, what: &str, filter: &str, options: FindOptions) {
    let e = c.explain_find(d(filter), &options).unwrap();
    let docs = time(&format!("{what} [{}]", e.get("stage").unwrap().to_json()), || {
        c.find(d(filter), options.clone()).unwrap()
    });
    println!("{:<48} {:>9} docs", "", docs.len());
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: bench <scratch file> [documents]");
    let n: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(200_000);
    let client = Client::<std::fs::File>::create(&path).unwrap();
    let c = client.database("bench").collection("orders");
    let doc = |i: usize| {
        d(&format!(
            r#"{{"_id": {i}, "cust": "c{}", "qty": {}, "price": {}.25, "status": "{}", "tags": ["t{}", "t{}"], "addr": {{"city": "city{}", "zip": "{:05}"}}}}"#,
            i % 5000,
            i % 100,
            i % 997,
            ["new", "paid", "shipped", "done"][i % 4],
            i % 10,
            i % 7 + 10,
            i % 50,
            i % 90000
        ))
    };
    time(&format!("insert {n} docs, batches of 1000"), || {
        for start in (0..n).step_by(1000) {
            c.insert_many((start..(start + 1000).min(n)).map(doc).collect()).unwrap();
        }
    });
    time("insert 1000 docs one at a time", || {
        for i in n..n + 1000 {
            c.insert_one(doc(i)).unwrap();
        }
    });
    let unjournaled = c.with_journal(false);
    time("insert 1000 docs one at a time, j: false", || {
        for i in n + 1000..n + 2000 {
            unjournaled.insert_one(doc(i)).unwrap();
        }
    });
    drop(unjournaled);
    q(&c, "find by _id", r#"{"_id": 12345}"#, FindOptions::new());
    q(&c, "find cust (no index)", r#"{"cust": "c42"}"#, FindOptions::new());
    q(&c, "count all (no filter)", r#"{}"#, FindOptions::new());
    time("create index cust", || c.create_index(d(r#"{"cust": 1}"#), IndexOptions::default()).unwrap());
    time("create index qty", || c.create_index(d(r#"{"qty": 1}"#), IndexOptions::default()).unwrap());
    time("create index tags (multikey)", || c.create_index(d(r#"{"tags": 1}"#), IndexOptions::default()).unwrap());
    q(&c, "find cust (index)", r#"{"cust": "c42"}"#, FindOptions::new());
    q(&c, "find qty range 10..12", r#"{"qty": {"$gte": 10, "$lt": 12}}"#, FindOptions::new());
    q(&c, "find tags t3", r#"{"tags": "t3"}"#, FindOptions::new());
    q(&c, "sort qty limit 10 (index order)", r#"{}"#, FindOptions::new().sort(d(r#"{"qty": 1}"#)).limit(10));
    q(&c, "sort price limit 10 (memory)", r#"{}"#, FindOptions::new().sort(d(r#"{"price": 1}"#)).limit(10));
    let out = time("aggregate group by status", || {
        c.aggregate(vec![d(r#"{"$group": {"_id": "$status", "n": {"$sum": 1}, "q": {"$sum": "$qty"}}}"#)]).unwrap()
    });
    println!("{:<48} {:>9} groups", "", out.len());
    time("update_many qty 5 -> $inc", || c.update_many(d(r#"{"qty": 5}"#), d(r#"{"$inc": {"qty": 1000}}"#), false).unwrap());
    time("delete_many cust c7", || c.delete_many(d(r#"{"cust": "c7"}"#)).unwrap());
    drop(c);
    time("close", || client.close().unwrap());
    let client = time("reopen", || Client::<std::fs::File>::open(&path).unwrap());
    let c = client.database("bench").collection("orders");
    q(&c, "find cust after reopen (index)", r#"{"cust": "c43"}"#, FindOptions::new());
}
