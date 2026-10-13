// Whole-table aggregates read in batches (source::total): the same answers
// as row by row, across batches, NULLs, types and row formats.

use super::*;

// The query's one row, checked to come from batches.
fn one(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<ValueItem> {
    let totals = || crate::source::total::TOTALS.with(|t| t.get());
    let before = totals();
    let rows = select_rows(c, sql).1;
    assert_eq!(totals(), before + 1, "read in batches: {sql}");
    assert_eq!(rows.len(), 1, "{sql}");
    rows.into_iter().next().unwrap()
}

fn s(v: &str, cap: u32) -> ValueItem {
    ValueItem::Str((v.into(), cap))
}

fn filled(c: &Arc<Connection<MemFile>>, rows: i64) {
    run(c, "create table bt (id integer not null, n integer, d double, name varchar(8), primary key(id))").unwrap();
    // Every 7th n NULL, every 5th d NULL: several batches' worth.
    let values: Vec<String> = (1..=rows)
        .map(|i| {
            let n = if i % 7 == 0 { "null".into() } else { (i % 100).to_string() };
            let d = if i % 5 == 0 { "null".into() } else { format!("{}.5", i % 10) };
            format!("({i}, {n}, {d}, 'n{}')", i % 13)
        })
        .collect();
    for chunk in values.chunks(500) {
        run(c, &format!("insert into bt values {}", chunk.join(", "))).unwrap();
    }
}

#[test]
fn test_totals_over_many_batches_with_nulls() {
    let c = conn();
    let rows = 3000i64;
    filled(&c, rows);
    let ns: Vec<i64> = (1..=rows).filter(|i| i % 7 != 0).map(|i| i % 100).collect();
    let ds: Vec<f64> = (1..=rows).filter(|i| i % 5 != 0).map(|i| (i % 10) as f64 + 0.5).collect();
    let got = one(
        &c,
        "select count(*), count(n), sum(n), min(n), max(n), count(d), sum(d), avg(d), min(name), max(name) from bt",
    );
    let sum_d = ds.iter().fold(0.0, |t, d| t + d);
    assert_eq!(
        got,
        vec![
            ValueItem::Integer(rows),
            ValueItem::Integer(ns.len() as i64),
            ValueItem::Integer(ns.iter().sum()),
            ValueItem::Integer(*ns.iter().min().unwrap()),
            ValueItem::Integer(*ns.iter().max().unwrap()),
            ValueItem::Integer(ds.len() as i64),
            ValueItem::Double(sum_d),
            ValueItem::Double(sum_d / ds.len() as f64),
            s("n0", 8),
            s("n9", 8),
        ]
    );
    // Expressions over the totals.
    assert_eq!(
        one(&c, "select count(*) + 1, sum(n) * 2 from bt"),
        vec![ValueItem::Integer(rows + 1), ValueItem::Integer(2 * ns.iter().sum::<i64>())]
    );
}

#[test]
fn test_totals_over_nothing() {
    let c = conn();
    filled(&c, 0);
    assert_eq!(
        one(&c, "select count(*), sum(n), avg(d), min(name), count(n) from bt"),
        vec![
            ValueItem::Integer(0),
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Integer(0),
        ]
    );
}

#[test]
fn test_total_errors_as_row_by_row() {
    let c = conn();
    run(&c, "create table bo (id integer not null, n integer, name varchar(4), primary key(id))").unwrap();
    run(&c, &format!("insert into bo values (1, {}, 'a'), (2, 1, 'b')", i64::MAX)).unwrap();
    let mut stmt = c.clone().create_statement("select sum(n) from bo").unwrap();
    assert!(stmt.execute().is_err() || {
        let ResultType::StreamingResult(mut r) = stmt.get_nextresult().unwrap().unwrap() else {
            panic!()
        };
        r.next_result().is_err()
    });
    let mut stmt = c.clone().create_statement("select sum(name) from bo").unwrap();
    let failed = stmt.execute().is_err() || {
        let ResultType::StreamingResult(mut r) = stmt.get_nextresult().unwrap().unwrap() else {
            panic!()
        };
        r.next_result().is_err()
    };
    assert!(failed, "sum of strings");
}

#[test]
fn test_totals_read_rows_of_an_older_version() {
    let c = conn();
    filled(&c, 50);
    // Rows written before the column was added hold no value for it.
    run(&c, "alter table bt add column extra integer").unwrap();
    run(&c, "insert into bt values (1000, 1, 1.5, 'x', 7)").unwrap();
    assert_eq!(
        one(&c, "select count(*), count(extra), sum(extra), max(name) from bt"),
        vec![
            ValueItem::Integer(51),
            ValueItem::Integer(1),
            ValueItem::Integer(7),
            s("x", 8),
        ]
    );
}

#[test]
fn test_totals_over_partitions() {
    let c = conn();
    run(
        &c,
        "create table bp (k integer not null, v integer, primary key(k)) \
         partition by range (k) (partition a values less than (100), \
         partition b values less than (maxvalue))",
    )
    .unwrap();
    let values: Vec<String> = (0..300).map(|k| format!("({k}, {})", k * 2)).collect();
    run(&c, &format!("insert into bp values {}", values.join(", "))).unwrap();
    assert_eq!(
        one(&c, "select count(*), sum(v), min(v), max(v) from bp"),
        ints(&[&[300, (0..300).map(|k| k * 2).sum(), 0, 598]])[0]
    );
}
