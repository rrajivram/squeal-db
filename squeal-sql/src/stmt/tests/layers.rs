//! Where a query's time goes, layer by layer: parsing, the whole SQL path,
//! and store alone. Not a correctness test — run by hand, in release, on a
//! scratch copy of a retail database:
//!
//! SQ_LAYERS_DB=/tmp/x/r.db cargo test --release -p squeal-sql --lib \
//!     layers -- --ignored --nocapture

use std::fs::File;
use std::time::Instant;

use store::cursor::{Cursor, KeyRange};
use store::tuple::DBIdType;
use store::valueitem::{IndexKey, ValueItem};

use super::*;
use crate::conn::connection::ConnectionManager;

fn per_op(what: &str, n: usize, start: Instant) {
    let us = start.elapsed().as_secs_f64() * 1e6 / n as f64;
    println!("{what:<58} {us:>9.2} us/op  ({n} ops)");
}

fn drain(stmt: &mut Statement<File>) -> usize {
    let mut rows = 0;
    for r in stmt.results.iter_mut() {
        if let Some(ResultType::StreamingResult(mut s)) = r.take() {
            while s.next_result().unwrap().is_some() {
                rows += 1;
            }
        }
    }
    rows
}

#[test]
#[ignore]
fn layers() {
    let path = std::env::var("SQ_LAYERS_DB").expect("SQ_LAYERS_DB: a scratch retail database");
    let c = ConnectionManager::<File>::get_manager().connect(&path).unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let db = c.database.read().db.clone();
    let orders = db.table_id_by_name("default.orders").unwrap().expect("an orders table");

    // 200 keys, each read 200 times (warm; within the parse cache); and
    // 40000 keys read once.
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % 330_000 + 1
    };
    let few: Vec<String> = (0..200).map(|_| format!("ORD{:07}", next())).collect();
    let repeated: Vec<&String> = (0..200).flat_map(|_| few.iter()).collect();
    let many: Vec<String> = (0..40_000).map(|_| format!("ORD{:07}", next())).collect();
    let sql = |id: &str| format!("select * from orders where order_id = '{id}'");
    let key = |id: &str| IndexKey::new_from(&[ValueItem::Str((id.to_string(), 12))]).unwrap();

    // Warm everything the runs below read.
    let txn = db.begin().unwrap();
    for id in many.iter().chain(&few) {
        db.find(orders, DBIdType::Rec(key(id)), &txn).unwrap();
    }
    db.rollback(txn).unwrap();

    println!("\n-- point lookups by primary key (warm) --");
    let start = Instant::now();
    for id in &repeated {
        sql_parser::parse_sql(&sql(id)).unwrap();
    }
    per_op("parse, uncached", repeated.len(), start);
    let start = Instant::now();
    for id in &repeated {
        sql_parser::parse_sql_cached(&sql(id)).unwrap();
    }
    per_op("parse, cached (200 texts, 200 times each)", repeated.len(), start);

    let start = Instant::now();
    let mut rows = 0;
    for id in &many {
        let mut stmt = c.clone().create_statement(&sql(id)).unwrap();
        stmt.execute().unwrap();
        rows += drain(&mut stmt);
    }
    per_op("SQL, every text new (parse + plan + execute)", many.len(), start);
    assert_eq!(rows, many.len());
    let start = Instant::now();
    for id in &repeated {
        let mut stmt = c.clone().create_statement(&sql(id)).unwrap();
        stmt.execute().unwrap();
        drain(&mut stmt);
    }
    per_op("SQL, texts repeated (parse cached)", repeated.len(), start);

    let start = Instant::now();
    let txn = db.begin().unwrap();
    for id in &repeated {
        db.find(orders, DBIdType::Rec(key(id)), &txn).unwrap().unwrap();
    }
    db.rollback(txn).unwrap();
    per_op("store: Db::find, one transaction", repeated.len(), start);
    let start = Instant::now();
    for id in &repeated {
        let txn = db.begin().unwrap();
        let mut cur = db
            .key_ranges_scan(orders, Some(txn.id()), vec![KeyRange::prefix(vec![ValueItem::Str(((*id).clone(), 12))])])
            .unwrap();
        assert!(cur.next().unwrap().is_some());
        drop(cur);
        db.commit(txn).unwrap();
    }
    per_op("store: key range seek, a transaction each (as SQL)", repeated.len(), start);

    println!("\n-- 100k-row primary key range (warm), 10 times --");
    let range_sql = "select count(*) from orders where order_id >= 'ORD0100000' and order_id < 'ORD0200000'";
    let start = Instant::now();
    for _ in 0..10 {
        let mut stmt = c.clone().create_statement(range_sql).unwrap();
        stmt.execute().unwrap();
        drain(&mut stmt);
    }
    per_op("SQL count(*) (index seek on the primary key)", 10, start);
    let pk_index = c
        .current_schema()
        .unwrap()
        .get_table("orders")
        .unwrap()
        .primary_key()
        .unwrap()
        .db_table_id;
    let range = || KeyRange {
        prefix: vec![],
        lower: std::ops::Bound::Included(ValueItem::Str(("ORD0100000".into(), 12))),
        upper: std::ops::Bound::Excluded(ValueItem::Str(("ORD0200000".into(), 12))),
    };
    for (what, tid) in [("store: same range of the primary key index", pk_index), ("store: same range of the row table (rows)", orders)] {
        let start = Instant::now();
        let mut n = 0;
        for _ in 0..10 {
            let txn = db.begin().unwrap();
            let mut cur = db.key_ranges_scan(tid, Some(txn.id()), vec![range()]).unwrap();
            while cur.next().unwrap().is_some() {
                n += 1;
            }
            drop(cur);
            db.commit(txn).unwrap();
        }
        per_op(what, 10, start);
        assert_eq!(n % 10, 0);
    }
    println!();
}
