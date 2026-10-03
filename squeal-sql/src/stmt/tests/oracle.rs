// Seeded random queries, each answered three ways — squeal on plain tables,
// squeal on the same rows partitioned, and SQLite — which must agree.
use super::partition_diff::{Outcome, ordered};
use super::*;

#[test]
#[ignore]
fn probe() {
    let c = conn();
    run(&c, "create table p (a integer, b integer, s varchar(8), d double)").unwrap();
    run(&c, "insert into p values (7, 2, 'abc', 1.5), (-7, 2, 'xbz', null), (null, 3, null, 2.0)").unwrap();
    let lite = rusqlite::Connection::open_in_memory().unwrap();
    lite.execute_batch("create table p (a integer, b integer, s varchar(8), d double); insert into p values (7, 2, 'abc', 1.5), (-7, 2, 'xbz', null), (null, 3, null, 2.0);").unwrap();
    for q in [
        "select a / b from p",
        "select a % b from p",
        "select a from p where a between -10 and 0",
        "select s from p where s like '%b%'",
        "select avg(a), sum(a), count(a), count(*), min(s), max(d) from p",
        "select a from p order by a",
        "select a from p order by a desc",
        "select s || 'x' from p",
        "select a * 2 + b from p",
        "select d * 2 from p",
        "select a + d from p",
        "select avg(b) from p",
        "select sum(d) from p",
        "select count(distinct b) from p",
        "select -a from p",
        "select a from p where not (a > 0)",
        "select coalesce(a, 0) from p",
        "select case when a > 0 then 1 else 0 end from p",
        "select abs(a) from p",
        "select a = b from p",
        "select 1 / 2, 7 / 2.0",
    ] {
        let mine = ordered(&c, q);
        let mut stmt = lite.prepare(q).unwrap();
        let n = stmt.column_count();
        let theirs: Vec<Vec<String>> = stmt
            .query_map([], |r| {
                Ok((0..n).map(|i| format!("{:?}", r.get_ref(i).unwrap())).collect())
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        println!("{q}\n   squeal: {mine:?}\n   sqlite: {theirs:?}");
    }
}
