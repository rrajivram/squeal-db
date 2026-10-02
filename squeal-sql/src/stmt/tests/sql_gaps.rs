// SQL that squeal-sql does not run yet, written down as tests of what it
// should do. None of it is about partitions (see partition_diff.rs for
// those): these fail on a plain table.
//
// Where a query has an equivalent the engine does run, that is its expected
// answer — no result is written out by hand. Every test here is #[ignore]d
// as a known gap; test_every_known_gap_is_still_one fails the day one of
// them starts working, so a fixed gap gets its test switched on.
//     cargo test -p squeal-sql sql_gaps -- --ignored
use super::partition_diff::{outcome, twin_conn};
use super::*;

// (what is missing, the query, an equivalent that runs today)
const GAPS: &[(&str, &str, &str)] = &[
    (
        "JOIN ... USING",
        "select e.id from plain_ev e join days d using (day)",
        "select e.id from plain_ev e join days d on e.day = d.day",
    ),
    (
        "WITH (a common table expression)",
        "with x as (select id, day from plain_ev where cat = 1) select id from x where day > 10",
        "select id from (select id, day from plain_ev where cat = 1) x where day > 10",
    ),
    (
        // id % 7 = 0 are the rows whose cat is NULL (see twin_conn).
        "IS NULL",
        "select id from plain_ev where cat is null",
        "select id from plain_ev where id % 7 = 0",
    ),
    (
        "IS NOT NULL",
        "select id from plain_ev where cat is not null",
        "select id from plain_ev where id % 7 <> 0",
    ),
];

// Gaps since closed: the same check, now part of the suite.
const CLOSED: &[(&str, &str, &str)] = &[
    (
        "join ON with a term on one table (hash join)",
        "select e.id, d.label from plain_ev e join days d on e.day = d.day and d.day = 10",
        "select e.id, d.label from plain_ev e join days d on e.day = d.day where d.day = 10",
    ),
    (
        "join ON with a term on one table (hash join)",
        "select e.id, d.label from plain_ev e join days d on e.day = d.day and e.cat = 1",
        "select e.id, d.label from plain_ev e join days d on e.day = d.day where e.cat = 1",
    ),
    (
        "join ON with a term on one table (hash join)",
        "select d.day, b.id from days d join plain_big b on b.k = d.day and b.v = 0",
        "select d.day, b.id from days d join plain_big b on b.k = d.day where b.v = 0",
    ),
    (
        // Not a WHERE: the outer rows with no match must stay.
        "LEFT JOIN ON with a term on the inner table",
        "select d.label, e.id from days d left join plain_ev e on e.day = d.day and e.cat = 1",
        "select d.label, e.id from days d left join \
         (select id, day from plain_ev where cat = 1) e on e.day = d.day",
    ),
    (
        "join on an inequality",
        "select e.id, d.label from plain_ev e join days d on e.day < d.day where e.id < 4",
        "select e.id, d.label from plain_ev e cross join days d \
         where e.day < d.day and e.id < 4",
    ),
    (
        "join on an expression",
        "select d.day, b.k from days d join plain_big b on b.id = d.day + 0",
        "select d.day, b.k from days d join plain_big b on b.id = d.day",
    ),
];

fn check(what: &str) {
    check_in(GAPS, what)
}

fn check_in(list: &[(&str, &str, &str)], what: &str) {
    let c = twin_conn();
    let mut checked = 0;
    for (gap, query, equivalent) in list.iter().filter(|g| g.0 == what) {
        let want = outcome(&c, equivalent);
        assert!(want.is_ok(), "{gap}: the equivalent does not run: {want:?}");
        assert!(
            !want.as_ref().unwrap().is_empty(),
            "{gap}: the equivalent returns nothing"
        );
        assert_eq!(outcome(&c, query), want, "{gap}: {query}");
        checked += 1;
    }
    assert!(checked > 0, "no gap is called {what:?}");
}

#[test]
fn test_a_join_on_condition_may_be_more_than_column_equalities() {
    for what in [
        "join ON with a term on one table (hash join)",
        "LEFT JOIN ON with a term on the inner table",
        "join on an inequality",
        "join on an expression",
    ] {
        check_in(CLOSED, what);
    }
}

#[test]
#[ignore = "known gap: JOIN ... USING is not supported"]
fn test_join_using() {
    check("JOIN ... USING");
}

#[test]
#[ignore = "known gap: WITH is not supported"]
fn test_with_names_a_query() {
    check("WITH (a common table expression)");
}

#[test]
#[ignore = "known gap: IS [NOT] NULL is not supported"]
fn test_is_null_and_is_not_null() {
    check("IS NULL");
    check("IS NOT NULL");
}

// DROP TABLE and TRUNCATE parse; neither does anything.
#[test]
#[ignore = "known gap: DROP TABLE is not executed"]
fn test_drop_table_removes_the_table() {
    let c = conn();
    run(&c, "create table gone (id integer not null)").unwrap();
    run(&c, "insert into gone values (1)").unwrap();
    run(&c, "drop table gone").unwrap();
    assert!(c.current_schema().unwrap().get_table("gone").is_none());
    // The name is free again, and the new table starts empty.
    run(&c, "create table gone (id integer not null)").unwrap();
    assert_eq!(outcome(&c, "select id from gone"), Ok(vec![]));
}

#[test]
#[ignore = "known gap: TRUNCATE is not executed"]
fn test_truncate_empties_the_table() {
    let c = conn();
    run(&c, "create table emptied (id integer not null)").unwrap();
    run(&c, "insert into emptied values (1), (2)").unwrap();
    run(&c, "truncate table emptied").unwrap();
    assert_eq!(outcome(&c, "select id from emptied"), Ok(vec![]));
}

// Keeps GAPS honest: when one starts working, this fails, and its test
// above loses its #[ignore].
#[test]
fn test_every_known_gap_is_still_one() {
    let c = twin_conn();
    for (gap, query, _) in GAPS {
        assert!(
            outcome(&c, query).is_err(),
            "{gap} now runs: {query}\nun-ignore its test and take it out of GAPS"
        );
    }
}
