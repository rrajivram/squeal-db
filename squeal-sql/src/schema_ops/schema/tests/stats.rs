// optim::table_stats::SchemaStats actually being driven by real
// statements — insert_rows_in_txn's log_stat calls (async, sampled, so
// polled for here — see wait_for_row_count) and analyze_table's
// exhaustive, synchronous rebuild. The lifecycle side (create/load/
// persist/shutdown, the stats table surviving close/reopen) is in
// `contract` instead.
use std::time::{Duration, Instant};

use store::table::TableIdType;
use store::valueitem::ValueItem;

use super::*;
use crate::optim::table_stats::TableStat;

// log_stat (the path insert_rows_in_txn uses) is fire-and-forget over a
// capacity-1 channel drained by a background thread — there's no
// synchronous "flushed" signal, so a test that inserts rows and then
// immediately reads stats has to tolerate the collector not having
// caught up yet. Polling briefly (as store/db.rs's own
// wait_for_durable_logs does for its own async-delivery gap) is far less
// flaky than a fixed sleep.
fn wait_for_row_count(
    schema: &Arc<Schema<MemFile>>,
    table_id: TableIdType,
    expected: usize,
) -> TableStat {
    let start = Instant::now();
    loop {
        let found = schema
            .stats
            .lock()
            .as_ref()
            .and_then(|s| s.get_table_stats(table_id));
        if let Some(stat) = &found
            && stat.row_count == expected
        {
            return found.unwrap();
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "timed out waiting for SchemaStats row_count to reach {expected}, last saw {:?}",
            found.map(|s| s.row_count)
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn test_insert_feeds_schema_stats_row_count_and_column_stats() {
    let c = conn();
    execute(
        &c,
        "create table customers (id integer not null, age integer, city varchar(20), \
         primary key(id))",
    )
    .unwrap();
    let schema = c.current_schema().unwrap();
    let table = schema.get_table("customers").unwrap();

    execute(&c, "insert into customers values (1, 30, 'nyc')").unwrap();
    execute(&c, "insert into customers values (2, 40, 'sf')").unwrap();
    execute(&c, "insert into customers values (3, 25, 'nyc')").unwrap();

    let stat = wait_for_row_count(&schema, table.db_table_id, 3);
    assert_eq!(stat.name, "customers");

    // `id` is the sole PRIMARY KEY column — excluded from col_stats (see
    // SchemaStats::table_data's own comment: a lone PRIMARY KEY/UNIQUE
    // column is already known-unique, nothing to track).
    let id_idx = table.fields().iter().position(|f| f.name == "id").unwrap();
    assert!(
        !stat.col_stats.contains_key(&id_idx),
        "a lone PRIMARY KEY column should not get its own bloom/min/max tracking"
    );

    let age_idx = table.fields().iter().position(|f| f.name == "age").unwrap();
    let age_stat = stat.col_stats.get(&age_idx).unwrap();
    assert_eq!(age_stat.min, ValueItem::Integer(25));
    assert_eq!(age_stat.max, ValueItem::Integer(40));
    assert_eq!(age_stat.null, 0);
}

#[test]
fn test_analyze_table_exhaustively_rebuilds_stats_from_a_full_scan() {
    let c = conn();
    execute(
        &c,
        "create table products (id integer not null, price double, primary key(id))",
    )
    .unwrap();
    let schema = c.current_schema().unwrap();
    let table = schema.get_table("products").unwrap();

    for (id, price) in [(1, 9.99), (2, 19.99), (3, 4.99)] {
        execute(&c, &format!("insert into products values ({id}, {price})")).unwrap();
    }

    // Doesn't rely on log_stat's async delivery at all — analyze_table is
    // synchronous, so its result must be exact and immediate.
    schema.analyze_table("products").unwrap();
    let stat = schema
        .stats
        .lock()
        .as_ref()
        .unwrap()
        .get_table_stats(table.db_table_id)
        .unwrap();
    assert_eq!(stat.row_count, 3);
    let price_idx = table.fields().iter().position(|f| f.name == "price").unwrap();
    let price_stat = stat.col_stats.get(&price_idx).unwrap();
    assert_eq!(price_stat.min, ValueItem::Double(4.99));
    assert_eq!(price_stat.max, ValueItem::Double(19.99));
}

#[test]
fn test_analyze_table_resets_stale_stats_before_rebuilding() {
    let c = conn();
    execute(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let schema = c.current_schema().unwrap();
    let table = schema.get_table("t").unwrap();

    execute(&c, "insert into t values (1)").unwrap();
    schema.analyze_table("t").unwrap();
    assert_eq!(
        schema
            .stats
            .lock()
            .as_ref()
            .unwrap()
            .get_table_stats(table.db_table_id)
            .unwrap()
            .row_count,
        1
    );

    execute(&c, "insert into t values (2)").unwrap();
    execute(&c, "insert into t values (3)").unwrap();
    schema.analyze_table("t").unwrap();
    assert_eq!(
        schema
            .stats
            .lock()
            .as_ref()
            .unwrap()
            .get_table_stats(table.db_table_id)
            .unwrap()
            .row_count,
        3,
        "a second analyze must reflect the current row count, not double-count the first \
         analyze's rows"
    );
}

// Regression test: ValueItem::Ord ranks Null lowest of every variant (see
// its own type_rank), so a naive `v < min` comparison let a single NULL
// row make a column's tracked min "Null" forever — the opposite of a
// real bound, and not what SQL's own MIN/MAX do (they ignore NULL).
#[test]
fn test_null_values_are_excluded_from_min_max_and_unique() {
    let c = conn();
    execute(&c, "create table t (id integer not null, age integer, primary key(id))").unwrap();
    let schema = c.current_schema().unwrap();
    let table = schema.get_table("t").unwrap();

    execute(&c, "insert into t values (1, 30)").unwrap();
    execute(&c, "insert into t values (2, null)").unwrap();
    execute(&c, "insert into t values (3, 10)").unwrap();

    schema.analyze_table("t").unwrap();
    let stat = schema
        .stats
        .lock()
        .as_ref()
        .unwrap()
        .get_table_stats(table.db_table_id)
        .unwrap();
    let age_idx = table.fields().iter().position(|f| f.name == "age").unwrap();
    let age_stat = stat.col_stats.get(&age_idx).unwrap();
    assert_eq!(age_stat.min, ValueItem::Integer(10), "NULL must not win min");
    assert_eq!(age_stat.max, ValueItem::Integer(30));
    assert_eq!(age_stat.null, 1);
    assert_eq!(age_stat.unique, 2, "NULL must not be counted as a distinct value");
}

#[test]
fn test_analyze_table_fails_for_an_unknown_table() {
    let c = conn();
    let schema = c.current_schema().unwrap();
    let err = schema.analyze_table("nope").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}
