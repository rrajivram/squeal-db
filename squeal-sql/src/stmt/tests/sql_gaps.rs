// SQL that squeal-sql once did not run, each written down first as a
// failing test of what it should do. None of it is about partitions (see
// partition_diff.rs for those): these failed on a plain table.
//
// Where a query has an equivalent the engine already ran, that is its
// expected answer — no result is written out by hand.
//
// A new gap goes into GAPS with an #[ignore]d test;
// test_every_known_gap_is_still_one fails the day it starts working, and
// its entry then moves to CLOSED and its test joins the suite.
use super::partition_diff::{outcome, twin_conn};
use super::*;

// (what is missing, the query, an equivalent that runs today)
const GAPS: &[(&str, &str, &str)] = &[];

// Gaps since closed: the same check, now part of the suite.
const CLOSED: &[(&str, &str, &str)] = &[
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
fn test_join_using() {
    check_in(CLOSED, "JOIN ... USING");
}

#[test]
fn test_with_names_a_query() {
    check_in(CLOSED, "WITH (a common table expression)");
}

#[test]
fn test_is_null_and_is_not_null() {
    check_in(CLOSED, "IS NULL");
    check_in(CLOSED, "IS NOT NULL");
}

#[test]
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
fn test_drop_table_frees_every_name_the_table_held() {
    let c = conn();
    let create = "create table t (id integer not null, k integer not null, v integer, \
                  primary key(id, k), unique(k)) partition by range (k) \
                  (partition a values less than (10), partition b values less than maxvalue)";
    for round in 0..3 {
        run(&c, create).unwrap();
        run(&c, "create index t_v on t (v)").unwrap();
        run(
            &c,
            &format!("insert into t values (1, 5, {round}), (2, 50, {round})"),
        )
        .unwrap();
        // Rows of the table dropped before are not in this one.
        assert_eq!(ints(&c, "select count(*) from t"), [2]);
        assert_eq!(ints(&c, "select v from t where k = 50"), [round]);
        run(&c, "drop table t").unwrap();
        assert!(run(&c, "select * from t").is_err());
    }
    // A table with no primary key: its row ids start over too.
    for _ in 0..2 {
        run(&c, "create table plain (v integer)").unwrap();
        run(&c, "insert into plain values (1), (2)").unwrap();
        assert_eq!(ints(&c, "select count(*) from plain"), [2]);
        run(&c, "drop table plain").unwrap();
    }
    // A name a live table's tree holds is still refused.
    run(&c, "create table keep (id integer not null)").unwrap();
    run(&c, "create index keep_id on keep (id)").unwrap();
    run(&c, "create table other (id integer not null)").unwrap();
    assert!(run(&c, "create index keep_id on other (id)").is_err());
    assert!(run(&c, "create table keep (id integer not null)").is_err());
}

#[test]
fn test_drop_table_if_exists_several_tables_temp_tables_and_foreign_keys() {
    let c = conn();
    assert!(run(&c, "drop table nope").is_err());
    run(&c, "drop table if exists nope").unwrap();
    run(&c, "create table a (id integer not null, primary key(id))").unwrap();
    run(
        &c,
        "create table b (id integer not null, a_id integer references a(id))",
    )
    .unwrap();
    // b's foreign key refers to a.
    let e = run(&c, "drop table a").unwrap_err().to_string();
    assert!(e.contains("foreign key"), "{e}");
    run(&c, "drop table b, a").unwrap();
    let schema = c.current_schema().unwrap();
    assert!(schema.get_table("a").is_none() && schema.get_table("b").is_none());
    // One missing name fails the statement at that name.
    run(&c, "create table x (id integer)").unwrap();
    assert!(run(&c, "drop table x, nope").is_err());
    run(&c, "drop table if exists x, nope").unwrap();

    run(&c, "create table temp.scratch (id integer)").unwrap();
    run(&c, "drop table temp.scratch").unwrap();
    assert!(run(&c, "select * from temp.scratch").is_err());
    run(&c, "create table temp.scratch (id integer)").unwrap();
}

// A dropped table is gone after the database is loaded again.
#[test]
fn test_a_dropped_table_stays_dropped() {
    let c = conn();
    run(&c, "create table gone (id integer not null)").unwrap();
    run(&c, "create table kept (id integer not null)").unwrap();
    run(&c, "drop table gone").unwrap();
    let schema = c.current_schema().unwrap();
    let reloaded = crate::schema_ops::schema::Schema::<MemFile>::load(
        DEFAULT_SCHEMA_NAME.to_string(),
        schema.db.clone(),
    )
    .unwrap();
    assert!(reloaded.get_table("gone").is_none());
    assert!(reloaded.get_table("kept").is_some());
    reloaded.persist_and_shutdown_stats().unwrap();
}

#[test]
fn test_truncate_empties_the_table() {
    let c = conn();
    run(&c, "create table emptied (id integer not null)").unwrap();
    run(&c, "insert into emptied values (1), (2)").unwrap();
    run(&c, "truncate table emptied").unwrap();
    assert_eq!(outcome(&c, "select id from emptied"), Ok(vec![]));
}

#[test]
fn test_truncate_empties_every_partition_and_index_and_can_be_rolled_back() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, k integer not null, primary key(id, k)) \
         partition by range (k) (partition a values less than (10), \
         partition b values less than maxvalue)",
    )
    .unwrap();
    run(&c, "create index t_k on t (k)").unwrap();
    run(&c, "insert into t values (1, 5), (2, 50), (3, 500)").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "truncate t").unwrap();
    assert_eq!(ints(&c, "select count(*) from t"), [0]);
    run(&c, "rollback").unwrap();
    assert_eq!(ints(&c, "select count(*) from t"), [3]);
    run(&c, "truncate table t").unwrap();
    assert_eq!(ints(&c, "select count(*) from t"), [0]);
    assert!(super::partition_races::storage_problems(&c, "t").is_empty());
    // The keys are free again.
    run(&c, "insert into t values (1, 5)").unwrap();
    assert!(run(&c, "truncate table nope").is_err());
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

// ---- beyond the one query each gap was written down as ----

fn rows(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    outcome(c, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn ints(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<i64> {
    rows(c, sql)
        .into_iter()
        .map(|r| match r[0] {
            ValueItem::Integer(n) => n,
            ref other => panic!("{sql}: expected an integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn test_is_null_finds_the_rows_an_outer_join_did_not_match() {
    let c = twin_conn();
    // days with no event: 25 and 60.
    assert_eq!(
        ints(
            &c,
            "select d.day from days d left join plain_ev e on e.day = d.day where e.id is null"
        ),
        [25, 60]
    );
    assert_eq!(
        ints(
            &c,
            "select count(*) from days d left join plain_ev e on e.day = d.day \
             where e.id is not null"
        ),
        [7]
    );
    // In an expression, in ON, and of an expression.
    assert_eq!(
        ints(
            &c,
            "select count(*) from plain_ev where not (cat is null) and day < 10"
        ),
        [8]
    );
    assert_eq!(
        ints(&c, "select count(*) from plain_ev where (cat + 1) is null"),
        [6]
    );
    assert_eq!(
        ints(
            &c,
            "select count(*) from days d join plain_ev e on e.day = d.day and e.cat is null"
        ),
        [1]
    );
    assert_eq!(
        rows(
            &c,
            "select cat is null, count(*) from plain_ev group by cat is null"
        ),
        vec![
            vec![ValueItem::Boolean(false), ValueItem::Integer(34)],
            vec![ValueItem::Boolean(true), ValueItem::Integer(6)]
        ]
    );
    // The partitioned twin agrees.
    assert_eq!(
        ints(&c, "select count(*) from part_ev where cat is null"),
        [6]
    );
}

#[test]
fn test_using_joins_on_the_named_columns_and_merges_them() {
    let c = conn();
    run(
        &c,
        "create table a (k integer not null, m integer, x varchar(4))",
    )
    .unwrap();
    run(
        &c,
        "create table b (k integer not null, m integer, y varchar(4))",
    )
    .unwrap();
    run(
        &c,
        "insert into a values (1, 10, 'a1'), (2, 20, 'a2'), (3, 30, 'a3')",
    )
    .unwrap();
    run(
        &c,
        "insert into b values (1, 10, 'b1'), (2, 99, 'b2'), (4, 40, 'b4')",
    )
    .unwrap();
    // The merged column is one column: named without a table, and once in *.
    assert_eq!(ints(&c, "select k from a join b using (k)"), [1, 2]);
    let mut stmt = c
        .clone()
        .create_statement("select * from a join b using (k)")
        .unwrap();
    stmt.execute().unwrap();
    let (columns, mut all) = take_streaming_result(&mut stmt, 0);
    all.sort();
    assert_eq!(columns, ["k", "m", "x", "m", "y"]);
    assert_eq!(all.len(), 2);
    // Several columns: all must be equal.
    assert_eq!(
        rows(&c, "select k, m, x, y from a join b using (k, m)"),
        vec![vec![
            ValueItem::Integer(1),
            ValueItem::Integer(10),
            ValueItem::Str(("a1".into(), 4)),
            ValueItem::Str(("b1".into(), 4))
        ]]
    );
    // LEFT JOIN: the merged column is the kept side's.
    assert_eq!(ints(&c, "select k from a left join b using (k)"), [1, 2, 3]);
    assert_eq!(
        ints(&c, "select k from a left join b using (k) where y is null"),
        [3]
    );
    // Either table's own column can still be named.
    assert_eq!(
        ints(&c, "select b.k from a join b using (k) where a.k = 2"),
        [2]
    );
    // RIGHT and FULL joins keep both columns (neither side's alone is the
    // merged value), so the bare name is ambiguous there.
    assert_eq!(
        ints(&c, "select b.k from a right join b using (k)"),
        [1, 2, 4]
    );
    assert_eq!(
        rows(&c, "select a.k, b.k from a full join b using (k)").len(),
        4
    );
    assert!(run(&c, "select k from a right join b using (k)").is_err());
    // A name that is not a column of both sides.
    assert!(run(&c, "select * from a join b using (x)").is_err());
    assert!(run(&c, "select * from a join b using (nope)").is_err());
    // A third table joins on the merged column.
    run(&c, "create table d (k integer not null, z varchar(4))").unwrap();
    run(&c, "insert into d values (2, 'd2'), (3, 'd3')").unwrap();
    assert_eq!(
        ints(&c, "select k from a join b using (k) join d using (k)"),
        [2]
    );
}

#[test]
fn test_with_queries_can_build_on_each_other_and_be_read_more_than_once() {
    let c = twin_conn();
    // A later WITH query reads an earlier one.
    assert_eq!(
        ints(
            &c,
            "with low as (select id, day from plain_ev where day < 10), \
                  odd as (select id from low where id % 2 = 1) \
             select count(*) from odd"
        ),
        [5]
    );
    // Read twice in one query: each reference reads it anew.
    assert_eq!(
        ints(
            &c,
            "with low as (select id from plain_ev where day < 3) \
             select count(*) from low a, low b"
        ),
        [9]
    );
    assert_eq!(
        ints(
            &c,
            "with low as (select id from plain_ev where day < 3) \
             select a.id from low a join low b on a.id = b.id"
        ),
        [0, 1, 2]
    );
    // Its columns renamed.
    assert_eq!(
        ints(
            &c,
            "with x (n, d) as (select id, day from plain_ev) select n from x where d = 33"
        ),
        [23]
    );
    assert!(
        run(
            &c,
            "with x (n) as (select id, day from plain_ev) select n from x"
        )
        .is_err()
    );
    // Joined with a table, aggregated, aliased.
    assert_eq!(
        rows(
            &c,
            "with per_cat as (select cat, count(*) as n from plain_ev group by cat) \
             select p.cat, p.n from per_cat p where p.cat is not null"
        ),
        vec![
            vec![ValueItem::Integer(0), ValueItem::Integer(12)],
            vec![ValueItem::Integer(1), ValueItem::Integer(11)],
            vec![ValueItem::Integer(2), ValueItem::Integer(11)]
        ]
    );
    assert_eq!(
        ints(
            &c,
            "with ev as (select id, day from plain_ev where id > 35) \
             select d.day from days d join ev on ev.day = d.day"
        ),
        [49]
    );
    // It shadows a table of the same name, inside its query only.
    assert_eq!(
        ints(
            &c,
            "with days as (select id as day from plain_ev) select count(*) from days"
        ),
        [40]
    );
    assert_eq!(ints(&c, "select count(*) from days"), [9]);
    // In a FROM subquery, with a WITH of its own.
    assert_eq!(
        ints(
            &c,
            "select s.id from (with t as (select id from plain_ev where id < 2) \
             select id from t) s"
        ),
        [0, 1]
    );
    // A WITH query cannot read itself, or one defined after it.
    assert!(run(&c, "with x as (select id from x) select id from x").is_err());
    assert!(
        run(
            &c,
            "with x as (select id from y), y as (select id from plain_ev) select id from x"
        )
        .is_err()
    );
    assert!(
        run(
            &c,
            "with recursive x as (select id from plain_ev) select id from x"
        )
        .is_err()
    );
}
