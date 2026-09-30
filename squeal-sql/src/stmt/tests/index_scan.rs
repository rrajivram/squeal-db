//! Covering index scans (see optim::picker and source::index): an index
//! that holds every column a query reads from a table is scanned instead
//! of the table, producing rows in the table's own layout.

use super::*;
use store::valueitem::ValueItem;

// `note` is wide and in no index, so any index that covers a query is
// cheaper than the table; `name` is UNIQUE, `city` is not.
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(20), city varchar(20), \
         note varchar(200), primary key(id))",
    )
    .unwrap();
    run(&c, "create unique index t_name on t (name)").unwrap();
    run(&c, "create index t_city on t (city)").unwrap();
    let rows = (0..50)
        .map(|i| {
            format!(
                "({i}, 'name{i:02}', 'city{}', 'a long note for row {i}')",
                i % 3
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into t values {rows}")).unwrap();
    run(&c, "analyze table t").unwrap();
    c
}

fn rows(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    select_rows(c, sql).1
}

fn s(v: &str) -> ValueItem {
    ValueItem::Str((v.into(), 20))
}

#[test]
fn test_a_covering_index_is_scanned_instead_of_the_table() {
    let c = setup();
    let plan = explain(&c, "select name from t");
    assert!(plan.contains("IndexScan t using t_name"), "{plan}");
    // Same answer the table gives: `note` forces a table scan.
    let mut from_index = rows(&c, "select name from t");
    let mut from_table: Vec<_> = rows(&c, "select name, note from t")
        .into_iter()
        .map(|r| vec![r[0].clone()])
        .collect();
    from_index.sort();
    from_table.sort();
    assert_eq!(from_index.len(), 50);
    assert_eq!(from_index, from_table);
}

#[test]
fn test_an_index_that_does_not_cover_the_query_is_not_used() {
    let c = setup();
    let plan = explain(&c, "select name, note from t");
    assert!(plan.contains("TableScan t"), "{plan}");
    // Read in WHERE only still counts.
    let plan = explain(&c, "select name from t where note = 'x'");
    assert!(plan.contains("TableScan t"), "{plan}");
}

#[test]
fn test_primary_key_columns_come_through_a_secondary_index() {
    let c = setup();
    let sql = "select id, name from t where name = 'name07'";
    let plan = explain(&c, sql);
    // A seek of it, now that the WHERE can use it (see index_seek).
    assert!(plan.contains("IndexSeek t using t_name"), "{plan}");
    assert_eq!(
        rows(&c, sql),
        vec![vec![ValueItem::Integer(7), s("name07")]]
    );
    // A non-unique index carries the key too.
    let sql = "select id, city from t where id = 8";
    assert!(
        explain(&c, sql).contains("using t_city"),
        "{}",
        explain(&c, sql)
    );
    assert_eq!(rows(&c, sql), vec![vec![ValueItem::Integer(8), s("city2")]]);
}

#[test]
fn test_a_non_unique_index_returns_every_row() {
    let c = setup();
    let sql = "select city, count(*) from t group by city order by city";
    assert!(explain(&c, sql).contains("using t_city"));
    assert_eq!(
        rows(&c, sql),
        vec![
            vec![s("city0"), ValueItem::Integer(17)],
            vec![s("city1"), ValueItem::Integer(17)],
            vec![s("city2"), ValueItem::Integer(16)],
        ]
    );
}

#[test]
fn test_count_star_reads_an_index() {
    let c = setup();
    let plan = explain(&c, "select count(*) from t");
    assert!(plan.contains("IndexScan"), "{plan}");
    assert_eq!(
        rows(&c, "select count(*) from t"),
        vec![vec![ValueItem::Integer(50)]]
    );
}

#[test]
fn test_each_side_of_a_self_join_picks_its_own_index() {
    let c = setup();
    let sql =
        "select a.name, b.city from t a join t b on a.id = b.id where a.id < 3 order by a.name";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("using t_name") && plan.contains("using t_city"),
        "{plan}"
    );
    assert_eq!(
        rows(&c, sql),
        vec![
            vec![s("name00"), s("city0")],
            vec![s("name01"), s("city1")],
            vec![s("name02"), s("city2")],
        ]
    );
}

#[test]
fn test_an_aggregate_nested_in_a_function_still_counts_as_read() {
    let c = setup();
    // `note` only appears inside max(), inside upper(): no index covers it.
    let plan = explain(&c, "select upper(max(note)) from t");
    assert!(plan.contains("TableScan t"), "{plan}");
    assert_eq!(
        rows(&c, "select upper(max(note)) from t"),
        vec![vec![s("A LONG NOTE FOR ROW 9")]]
    );
}

#[test]
fn test_an_index_scan_sees_the_statements_transaction() {
    let c = setup();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (100, 'fresh', 'city9', 'n')").unwrap();
    run(&c, "delete from t where id = 0").unwrap();
    let sql = "select name from t where name = 'fresh' or name = 'name00'";
    // An OR of equalities on one column is a seek of those values.
    let plan = explain(&c, sql);
    assert!(plan.contains("IndexSeek t using t_name"), "{plan}");
    assert_eq!(rows(&c, sql), vec![vec![s("fresh")]]);
    run(&c, "rollback").unwrap();
    assert_eq!(rows(&c, sql), vec![vec![s("name00")]]);
}

#[test]
fn test_nulls_in_an_indexed_column_are_kept() {
    let c = setup();
    run(&c, "insert into t values (200, 'nocity', null, 'n')").unwrap();
    run(&c, "analyze table t").unwrap();
    let sql = "select city, count(*) from t group by city order by city";
    assert!(explain(&c, sql).contains("using t_city"));
    let r = rows(&c, sql);
    assert_eq!(r.len(), 4);
    assert!(
        r.contains(&vec![ValueItem::Null, ValueItem::Integer(1)]),
        "{r:?}"
    );
}

#[test]
fn test_a_table_without_a_primary_key() {
    let c = conn();
    run(
        &c,
        "create table u (a integer, b varchar(10), note varchar(200))",
    )
    .unwrap();
    run(&c, "create index u_b on u (b)").unwrap();
    run(
        &c,
        "insert into u values (1, 'x', 'n'), (2, 'y', 'n'), (3, 'x', 'n')",
    )
    .unwrap();
    run(&c, "analyze table u").unwrap();
    let plan = explain(&c, "select b from u");
    assert!(plan.contains("IndexScan u using u_b"), "{plan}");
    let mut r = rows(&c, "select b from u");
    r.sort();
    assert_eq!(r, vec![vec![s("x")], vec![s("x")], vec![s("y")]]);
    // `a` is not the row identity here, so the index can't supply it.
    assert!(explain(&c, "select a, b from u").contains("TableScan u"));
}
