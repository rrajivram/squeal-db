use std::sync::Arc;

use store::memfile::MemFile;

use super::*;
use crate::conn::connection::{ConMgr, ConnectionManager};
use crate::constant::DEFAULT_SCHEMA_NAME;
use crate::rslt::resultset::{ResultType, StreamingResultSet};

fn conn() -> Arc<Connection<MemFile>> {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn run(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<(), SchemaError> {
    c.clone().create_statement(sql)?.execute()
}

// Test-only accessor: get_results/get_nextresult now *take* a result
// out of Statement::results (see get_results' own doc comment — a
// StreamingResult can't be cloned, so every result is retrievable at
// most once), but plenty of tests want to just peek at
// Statement::results by index, without going through that consuming
// path. Panics with a clearer message than a raw index would if the
// slot is empty (out of range, or already taken by a real
// get_results/get_nextresult call elsewhere in the same test).
fn nth_result(stmt: &Statement<MemFile>, i: usize) -> &ResultType {
    stmt.results
        .get(i)
        .and_then(|r| r.as_ref())
        .unwrap_or_else(|| panic!("no result at index {i} (out of range, or already taken)"))
}

// Like nth_result, but for the non-streaming ResultType::Result case
// (SHOW/DESCRIBE, which materialize eagerly) — unlike StreamingResult,
// ResultSet is plain Clone-able data, so this can borrow via nth_result
// instead of taking the slot.
fn nth_result_as_result(stmt: &Statement<MemFile>, i: usize) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    match nth_result(stmt, i) {
        ResultType::Result(rs) => (rs.columns().to_vec(), rs.rows().to_vec()),
        other => panic!("expected a Result at index {i}, got {other:?}"),
    }
}

// Drains a StreamingResultSet into plain (columns, rows) — the
// streaming equivalent of ResultSet::columns()/rows(), for tests that
// just want to assert on fully-materialized data rather than exercise
// incremental streaming itself.
fn drain_streaming(mut stream: StreamingResultSet) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let columns = stream.columns();
    let mut rows = Vec::new();
    while let Some(key) = stream.next_result().unwrap() {
        rows.push(key.values().to_vec());
    }
    (columns, rows)
}

// SELECT always produces a ResultType::StreamingResult (see
// Statement::execute's Select arm) — since a StreamingResult can't be
// cloned/peeked (see nth_result's own doc comment), reading one
// requires *taking* the slot, not borrowing it the way nth_result does.
// Panics if the slot is empty/already taken, or holds a different
// ResultType variant.
fn take_streaming_result(
    stmt: &mut Statement<MemFile>,
    i: usize,
) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let result = stmt
        .results
        .get_mut(i)
        .and_then(Option::take)
        .unwrap_or_else(|| panic!("no result at index {i} (out of range, or already taken)"));
    match result {
        ResultType::StreamingResult(stream) => drain_streaming(stream),
        other => panic!("expected a StreamingResult at index {i}, got {other:?}"),
    }
}

#[test]
fn test_execute_creates_table_via_current_schema() {
    let c = conn();
    let mut stmt = c
        .clone()
        .create_statement("create table t (id integer not null, primary key(id))")
        .unwrap();
    stmt.execute().unwrap();

    let schema = c.current_schema().unwrap();
    assert!(schema.table_exists("t"));
    assert_eq!(schema.get_table("t").unwrap().indices.len(), 1);
}

#[test]
fn test_execute_records_a_result_string_for_create_table() {
    let c = conn();
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    let ResultType::ResultString(s) = nth_result(&stmt, 0) else {
        panic!("expected a ResultString, got a different ResultType variant");
    };
    assert_eq!(s, "Table 't' created");
}

#[test]
fn test_execute_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema").unwrap();
    // Deliberately skip use_schema/create_schema.
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_propagates_create_table_errors() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t (id integer, primary key(id))")
        .unwrap();
    // A nullable primary key is rejected by TableBuilder::build, deep
    // inside Schema::create_table — must surface through execute()
    // unchanged, not get swallowed or mistranslated.
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_ignores_non_create_table_statements() {
    let c = conn();
    let mut stmt = c.clone().create_statement("drop table t").unwrap();
    stmt.execute().unwrap();
    assert!(stmt.results.is_empty());
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_create_database_switches_the_connection() {
    let c = conn();
    run(&c, "create database db2").unwrap();
    assert_eq!(c.database_name(), "db2");
    // The old database's schema selection doesn't carry over.
    assert!(c.current_schema().is_none());
}

#[test]
fn test_execute_create_database_rejects_an_already_open_name_by_default() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    let err = run(&c, "create database db2").unwrap_err();
    assert!(matches!(err, SchemaError::DatabaseInUseError(_)));
}

#[test]
fn test_execute_create_database_if_not_exists_is_idempotent() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    // db2 already open — must not error, and must still switch to it.
    run(&c, "create database if not exists db2").unwrap();
    assert_eq!(c.database_name(), "db2");
}

#[test]
fn test_execute_use_database_switches_the_connection() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c, "use database db2").unwrap();
    assert_eq!(c.database_name(), "db2");
    assert!(c.current_schema().is_none());
}

#[test]
fn test_execute_use_database_reuses_the_already_open_instance() {
    // Two independent connections to the same manager; c1 switches to
    // db2 via SQL and must land on the *same* Database c2 is already
    // connected to (sharing state), not a freshly re-opened, separate
    // instance.
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("db1").unwrap();
    let c2 = mgr.create_and_connect("db2").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    run(&c2, "create table shared (id integer)").unwrap();

    run(&c1, "use database db2").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    assert!(
        c1.current_schema().unwrap().table_exists("shared"),
        "c1 must see c2's table after switching to the same open database"
    );
}

#[test]
fn test_execute_create_schema_switches_current_schema() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap(); // lives only in "default"

    run(&c, "create schema extra").unwrap();
    // create_schema also makes the new schema current — it must start
    // empty, not somehow see "default"'s table.
    assert!(!c.current_schema().unwrap().table_exists("t"));

    run(&c, "use schema default").unwrap();
    assert!(c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_create_schema_rejects_an_already_existing_name_by_default() {
    let c = conn();
    run(&c, "create schema extra").unwrap();
    let err = run(&c, "create schema extra").unwrap_err();
    assert!(matches!(err, SchemaError::SchemaInUseError(_)));
}

#[test]
fn test_execute_create_schema_if_not_exists_is_idempotent() {
    let c = conn();
    run(&c, "create schema extra").unwrap();
    run(&c, "create schema if not exists extra").unwrap();
}

#[test]
fn test_execute_use_schema_fails_for_an_unknown_name() {
    let c = conn();
    let err = run(&c, "use schema nonexistent").unwrap_err();
    assert!(matches!(err, SchemaError::SchemaNotFound(_)));
}

#[test]
fn test_execute_insert_stores_a_row_and_records_a_count_result() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_statement("insert into t values (1)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert!(matches!(nth_result(&stmt, 0), ResultType::Count(1)));
}

#[test]
fn test_execute_insert_multi_row_records_the_right_count() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement("insert into t values (1), (2), (3)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert!(matches!(nth_result(&stmt, 0), ResultType::Count(3)));
}

#[test]
fn test_execute_insert_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_insert").unwrap();
    let mut stmt = c.create_statement("insert into t values (1)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_insert_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c.create_statement("insert into nope values (1)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_returns_a_result_set() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(50), primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 'alice')").unwrap();
    let mut stmt = c.create_statement("select * from t").unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    let (columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![vec![
            store::valueitem::ValueItem::Integer(1),
            store::valueitem::ValueItem::Str(("alice".into(), 50))
        ]]
    );
}

#[test]
fn test_execute_select_distinct_deduplicates_on_the_projected_column_not_the_raw_row() {
    // Regression test: DISTINCT used to sort/dedup the raw table row
    // (every column) and project down to just `name` afterward — so two
    // rows sharing a `name` but differing in `id` never compared equal,
    // and every row came back unchanged instead of collapsing.
    let c = conn();
    run(
        &c,
        "create table t1 (id integer not null, name varchar(50), primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t1 values (1, 'alice')").unwrap();
    run(&c, "insert into t1 values (2, 'bob')").unwrap();
    run(&c, "insert into t1 values (3, 'alice')").unwrap();

    let mut stmt = c.create_statement("select distinct name from t1").unwrap();
    stmt.execute().unwrap();
    let (columns, mut rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["name".to_string()]);
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![store::valueitem::ValueItem::Str(("alice".into(), 50))],
            vec![store::valueitem::ValueItem::Str(("bob".into(), 50))],
        ],
        "alice's two rows (different id, same name) must collapse into one"
    );
}

#[test]
fn test_execute_select_group_by_collapses_rows_sharing_a_group_key() {
    // Regression test: GROUP BY used to not collapse anything — the SELECT
    // list (including the COUNT(*) call) was evaluated once per raw row,
    // before grouping, and AggregatingSource compared whole already-
    // evaluated rows, so the ever-changing count column meant no two rows
    // ever compared equal.
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, category varchar(10), primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 'a')").unwrap();
    run(&c, "insert into t values (2, 'a')").unwrap();
    run(&c, "insert into t values (3, 'b')").unwrap();

    let mut stmt = c
        .create_statement("select category, count(*) from t group by category")
        .unwrap();
    stmt.execute().unwrap();
    let (columns, mut rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["category".to_string(), "none".to_string()]);
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![
                store::valueitem::ValueItem::Str(("a".into(), 10)),
                store::valueitem::ValueItem::Integer(2)
            ],
            vec![
                store::valueitem::ValueItem::Str(("b".into(), 10)),
                store::valueitem::ValueItem::Integer(1)
            ],
        ]
    );
}

#[test]
fn test_execute_select_group_by_collapses_correctly_at_a_scale_spanning_multiple_pages() {
    // Originally a regression test for GROUP BY silently fragmenting into
    // far more groups than actually existed once the sort behind it
    // (SortSource, via GroupSource -> SortSource::with_fields in
    // logical.rs) had enough rows for a single Run to span more than one
    // page — build_initial_runs used to sort and flush one page's worth
    // at a time, in a fresh heap per page, so each PAGE came out
    // internally sorted but consecutive pages within the same run were
    // never ordered relative to each other.
    //
    // At this data size (well within DEFAULT_QUERY_MEMORY_LIMIT's 64 MiB
    // budget) that bug can no longer reproduce here at all: SortSource now
    // sorts a build this small entirely in memory (one plain Vec, no Run,
    // no page split — see build_initial_runs' own doc comment), so this
    // now mainly checks GROUP BY correctness at a few thousand rows. The
    // original multi-page-run regression is still covered, at the
    // SortSource level (forcing a real spill via a tight memory budget),
    // by test_a_run_spanning_multiple_pages_is_globally_sorted_not_just_
    // page_locally in source/sort.rs.
    const N: i64 = 5_000;
    const NUM_CATEGORIES: i64 = 5;
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, cat integer, primary key(id))",
    )
    .unwrap();
    let mut ins = c
        .clone()
        .create_prepared_statement("insert into t values (?, ?)")
        .unwrap();
    for i in 0..N {
        ins.set_field(0, ValueItem::Integer(i)).unwrap();
        ins.set_field(1, ValueItem::Integer(i % NUM_CATEGORIES))
            .unwrap();
        ins.execute().unwrap();
    }

    let mut stmt = c
        .create_statement("select cat, count(*) from t group by cat")
        .unwrap();
    stmt.execute().unwrap();
    let (_, mut rows) = take_streaming_result(&mut stmt, 0);
    rows.sort();
    assert_eq!(
        rows.len(),
        NUM_CATEGORIES as usize,
        "expected exactly one group per distinct category, not one per accidental page boundary"
    );
    for (cat, row) in rows.into_iter().enumerate() {
        assert_eq!(
            row,
            vec![
                ValueItem::Integer(cat as i64),
                ValueItem::Integer(N / NUM_CATEGORIES)
            ],
            "every row for category {cat} must have collapsed into one group"
        );
    }
}

// Empirical answer to "which Source is the biggest laggard": runs a
// realistic JOIN through the real planner (TableSource x2 ->
// HashedSource -> UnionJoin -> Projection, the same tree logical.rs
// actually builds for this SQL shape — not a synthetic VecSource-only
// rig like HashedSource's own bench_hash_join_50k_rows), then reads
// QueryStats off the drained StreamingResult and prints each Source's
// own share of the total.
//
// No GROUP BY here on purpose, to keep this benchmark focused on
// HashedSource/UnionJoin/Projection specifically. (A join+group-by
// version of this benchmark is what originally surfaced a real,
// separate SortSource bug — a run spanning more than one page wasn't
// actually globally sorted, only page-locally, so GroupSource silently
// over-fragmented into far more groups than actually existed. Now
// fixed — see build_initial_runs/close_run in source/sort.rs and the
// regression tests
// test_a_run_spanning_multiple_pages_is_globally_sorted_not_just_page_locally
// (sort.rs) and
// test_execute_select_group_by_collapses_correctly_at_a_scale_spanning_multiple_pages
// (this file).)
//
// Ignored (manual-only, prints to stderr) — run with:
//     cargo test -p squeal-sql --release -- --ignored --nocapture \
//         stmt::tests::bench_full_pipeline_join_50k_rows
#[test]
#[ignore]
fn bench_full_pipeline_join_50k_rows() {
    const N: i64 = 50_000;

    let c = conn();
    run(
        &c,
        "create table t1 (id integer not null, cat integer, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table t2 (id integer not null, val integer, primary key(id))",
    )
    .unwrap();

    // Prepared + reused (not a fresh `run(&c, "insert into ...")` string
    // per row) so SQL parsing overhead isn't what this benchmark ends up
    // measuring — only the actual table-write cost.
    let mut ins1 = c
        .clone()
        .create_prepared_statement("insert into t1 values (?, ?)")
        .unwrap();
    let mut ins2 = c
        .clone()
        .create_prepared_statement("insert into t2 values (?, ?)")
        .unwrap();
    let insert_start = std::time::Instant::now();
    for i in 0..N {
        ins1.set_field(0, ValueItem::Integer(i)).unwrap();
        ins1.set_field(1, ValueItem::Integer(i % 5)).unwrap();
        ins1.execute().unwrap();

        ins2.set_field(0, ValueItem::Integer(i)).unwrap();
        ins2.set_field(1, ValueItem::Integer(i * 3)).unwrap();
        ins2.execute().unwrap();
    }
    let insert_elapsed = insert_start.elapsed();

    let mut stmt = c
        .create_statement("select t1.id, t1.cat, t2.val from t1 join t2 on t1.id = t2.id")
        .unwrap();
    stmt.execute().unwrap();

    let query_start = std::time::Instant::now();
    let result = stmt
        .results
        .get_mut(0)
        .and_then(Option::take)
        .expect("query must produce a result");
    let ResultType::StreamingResult(mut stream) = result else {
        panic!("expected a StreamingResult");
    };
    let mut rows = vec![];
    while let Some(row) = stream.next_result_as_strings().unwrap() {
        rows.push(row);
    }
    let query_elapsed = query_start.elapsed();
    assert_eq!(rows.len(), N as usize, "every row must find its one match");

    let stats = stream
        .get_query_stats()
        .expect("a join query must report stats at every level");

    eprintln!(
        "\nbench_full_pipeline_join_50k_rows: inserted {N} rows into each of 2 tables in \
         {insert_elapsed:?}; query returned {} rows in {query_elapsed:?}\n\
         per-Source breakdown (indented by pipeline depth):",
        rows.len()
    );
    for (name, s) in &stats {
        let indent = "  ".repeat(s.level());
        let mut entries: Vec<(String, f64)> =
            s.stats().iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let breakdown = entries
            .iter()
            .map(|(k, v)| format!("{k}={:.2}ms", v / 1_000_000.0))
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!("{indent}{name}: {breakdown}");
    }
}

#[test]
fn test_execute_select_bare_aggregate_with_no_group_by_collapses_to_one_row() {
    // Regression test: a bare aggregate with no GROUP BY clause at all
    // (an implicit single group over the whole table) used to return one
    // row per input row instead of collapsing to exactly one.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "insert into t values (2)").unwrap();
    run(&c, "insert into t values (3)").unwrap();

    let mut stmt = c.create_statement("select count(*) from t").unwrap();
    stmt.execute().unwrap();
    let (_columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![store::valueitem::ValueItem::Integer(3)]]);
}

#[test]
fn test_execute_select_bare_aggregate_over_an_empty_table_still_reports_zero() {
    // COUNT(*) over an empty table is 0, not "no rows" — the one case a
    // real GROUP BY (a non-empty key list) must NOT do, where zero input
    // rows correctly means zero groups.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();

    let mut stmt = c.create_statement("select count(*) from t").unwrap();
    stmt.execute().unwrap();
    let (_columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![store::valueitem::ValueItem::Integer(0)]]);
}

#[test]
fn test_execute_insert_and_select_support_schema_qualified_table_names() {
    let c = conn();
    // "default" is the schema `conn()` already selected — create a
    // second schema and a table in it, then switch back to "default" so
    // `other.t` can only resolve by explicitly qualifying it, not by
    // accidentally falling back to whatever's current.
    run(&c, "create schema other").unwrap();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c, "insert into other.t values (1)").unwrap();

    let mut stmt = c.clone().create_statement("select * from other.t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![store::valueitem::ValueItem::Integer(1)]]);

    // "default" never got the table — proves the qualified INSERT above
    // actually landed in "other", not silently in whatever's current.
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_rejects_a_table_reference_with_too_many_parts() {
    // Three parts (schema.table.field) is now a legal, meaningful shape
    // (see Connection::resolve_table_ref) — this needs a genuinely
    // too-long, four-part reference to still exercise the "too many
    // parts" rejection.
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c.create_statement("select * from a.b.default.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_select_rejects_a_field_qualified_table_reference() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    // Three parts resolves to schema="default", table="t", field="id" —
    // valid for resolve_table_ref, but a FROM target can't carry a
    // trailing field (see QueryVisitor::validate_table's own rejection).
    let mut stmt = c.create_statement("select * from default.t.id").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_qualified_table_reference_fails_for_an_unknown_schema() {
    let c = conn();
    let mut stmt = c.create_statement("select * from nope.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::SchemaNotFound(_)), "got {err:?}");
}

#[test]
fn test_temp_table_create_insert_select_roundtrip() {
    let c = conn();
    run(
        &c,
        "create table temp.t (id integer not null, name varchar(50))",
    )
    .unwrap();
    run(&c, "insert into temp.t values (1, 'alice')").unwrap();
    run(&c, "insert into temp.t values (2, 'bob')").unwrap();

    let mut stmt = c.clone().create_statement("select * from temp.t").unwrap();
    stmt.execute().unwrap();
    let (columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Str(("alice".into(), 50))],
            vec![ValueItem::Integer(2), ValueItem::Str(("bob".into(), 50))],
        ]
    );

    // A temp table never touches the real schema system at all.
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_temp_table_is_private_to_its_own_connection() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("temp_isolation_db").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let c2 = mgr.connect("temp_isolation_db").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c1, "create table temp.t (id integer not null)").unwrap();
    run(&c1, "insert into temp.t values (1)").unwrap();

    // c2 is a different connection to the SAME database — its own
    // temp.t must not exist at all, let alone see c1's row.
    let mut stmt = c2.create_statement("select * from temp.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_temp_table_create_rejects_constraints() {
    let c = conn();
    let err = run(
        &c,
        "create table temp.t (id integer not null, primary key(id))",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_temp_table_insert_fails_for_an_unknown_table() {
    let c = conn();
    let err = run(&c, "insert into temp.nope values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_use_schema_temp_is_rejected() {
    let c = conn();
    let err = run(&c, "use schema temp").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_schema_temp_is_rejected() {
    let c = conn();
    let err = run(&c, "create schema temp").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_select").unwrap();
    let mut stmt = c.create_statement("select * from t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_select_star_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c.create_statement("select * from nope").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_participates_in_a_multi_statement_batch() {
    // parse_sql returns a Vec<Statement> for a single `;`-separated
    // input, and both semantic_validate and execute already loop over
    // every element — this confirms SELECT's new arm actually
    // participates in that loop rather than only working as the sole
    // statement in a batch.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let mut stmt = c
        .create_statement("select * from t; insert into t values (2); select * from t")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 3);

    let (_, first_rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(first_rows.len(), 1);

    assert!(matches!(nth_result(&stmt, 1), ResultType::Count(1)));

    let (_, third_rows) = take_streaming_result(&mut stmt, 2);
    assert_eq!(third_rows.len(), 2);
}

#[test]
fn test_new_accepts_select_with_a_join() {
    // Was test_new_rejects_select_with_a_join: joins weren't implemented
    // at all when this was written, so Statement::new correctly rejected
    // any query containing one. Full JOIN support (see
    // stmt::tests::join_tests) means this exact query is now valid and
    // must be accepted, not rejected.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "create table u (id integer not null, primary key(id))").unwrap();
    c.create_statement("select * from t join u on t.id = u.id")
        .unwrap();
}

#[test]
fn test_execute_insert_without_a_transaction_still_autocommits() {
    // No BEGIN issued — each INSERT manages (and commits) its own
    // transaction, same as before explicit transactions existed.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let err = run(&c, "insert into t values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
}

#[test]
fn test_execute_begin_commit_persists_inserts() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "insert into t values (2)").unwrap();
    run(&c, "commit").unwrap();

    let s = c.current_schema().unwrap();
    assert!(s.table_exists("t"));
    assert_eq!(select_row_count(&c, "select * from t"), 2);
}

#[test]
fn test_execute_select_star_sees_uncommitted_inserts_within_the_same_transaction() {
    // The actual ask: a connection must be able to read its own writes
    // before COMMIT, not just after — see Db::table_scan_in_txn and
    // find_visible_to's self-write exception in store.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 1);
    run(&c, "insert into t values (2)").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 2);
    run(&c, "commit").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 2);
}

#[test]
fn test_execute_select_star_on_a_different_connection_does_not_see_uncommitted_inserts() {
    // Read-your-own-writes must not leak into cross-connection isolation:
    // a second, separate connection to the SAME database (autocommit, no
    // BEGIN of its own) still can't see the first connection's
    // uncommitted insert — only after commit does it become visible.
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("shared_db").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let c2 = mgr.connect("shared_db").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c1, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c1, "begin").unwrap();
    run(&c1, "insert into t values (1)").unwrap();
    assert_eq!(select_row_count(&c1, "select * from t"), 1);
    assert_eq!(select_row_count(&c2, "select * from t"), 0);

    run(&c1, "commit").unwrap();
    assert_eq!(select_row_count(&c2, "select * from t"), 1);
}

// Materializes a SELECT's row count via a fresh Statement — the direct
// way every SELECT-visibility test below checks what a connection can
// currently see, mirroring take_streaming_result's own draining.
fn select_row_count(c: &Arc<Connection<MemFile>>, sql: &str) -> usize {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    rows.len()
}

#[test]
fn test_execute_begin_rollback_discards_inserts() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "rollback").unwrap();

    // If the row had survived the rollback, this would fail with
    // DuplicateKey instead of succeeding.
    run(&c, "insert into t values (1)").unwrap();
}

#[test]
fn test_execute_rollback_discards_every_insert_in_the_transaction_not_just_the_last() {
    // No auto-abort-on-error: a failed statement inside an open
    // transaction doesn't end it, and rows from *earlier*, individually
    // successful statements in the same transaction stay uncommitted
    // until an explicit COMMIT/ROLLBACK — so ROLLBACK here must discard
    // row 1 too, not just row 2's failed attempt.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let err = run(&c, "insert into t values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
    run(&c, "rollback").unwrap();

    // Row 1 must be gone too — succeeds only if nothing from the aborted
    // transaction survived.
    run(&c, "insert into t values (1)").unwrap();
}

#[test]
fn test_execute_begin_twice_errors() {
    let c = conn();
    run(&c, "begin").unwrap();
    let err = run(&c, "begin").unwrap_err();
    assert!(matches!(err, SchemaError::TransactionAlreadyActive));
}

#[test]
fn test_execute_commit_without_begin_errors() {
    let c = conn();
    let err = run(&c, "commit").unwrap_err();
    assert!(matches!(err, SchemaError::NoActiveTransaction));
}

#[test]
fn test_execute_rollback_without_begin_errors() {
    let c = conn();
    let err = run(&c, "rollback").unwrap_err();
    assert!(matches!(err, SchemaError::NoActiveTransaction));
}

#[test]
fn test_execute_begin_again_after_commit_succeeds() {
    let c = conn();
    run(&c, "begin").unwrap();
    run(&c, "commit").unwrap();
    // The slot was cleared by commit, so a second BEGIN must not hit
    // TransactionAlreadyActive.
    run(&c, "begin").unwrap();
    run(&c, "rollback").unwrap();
}

// semantic_validate: caught at Statement::new() (create_statement) time,
// before execute() even runs — so these all fail there, not on execute().

#[test]
fn test_new_rejects_duplicate_columns_in_create_table() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("create table t (id integer, id integer)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_constraint_referencing_an_unknown_column() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("create table t (id integer, primary key(nope))")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_an_oversized_table_name() {
    let c = conn();
    let long_name = "a".repeat(129);
    let err = c
        .clone()
        .create_statement(&format!("create table {long_name} (id integer)"))
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_an_oversized_database_name() {
    let c = conn();
    let long_name = "a".repeat(129);
    let err = c
        .clone()
        .create_statement(&format!("create database {long_name}"))
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_duplicate_columns_in_insert_column_list() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("insert into t (id, id) values (1, 2)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_values_row_that_does_not_match_the_column_list_width() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("insert into t (id, name) values (1)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_accepts_a_values_row_matching_the_implicit_column_count() {
    // No explicit column list — width can't be checked without the
    // schema, so this must pass semantic_validate cleanly regardless of
    // whether "t" even exists (any mismatch surfaces later, in
    // rows_from_insert, once there's a real schema to check against).
    let c = conn();
    c.clone()
        .create_statement("insert into t values (1, 2, 3)")
        .unwrap();
}

#[test]
fn test_new_rejects_a_begin_end_block() {
    // sql-parser's StartTransaction::Begin grammar has no body at all
    // (just the bare BEGIN [TRANSACTION] keyword(s)) — a BEGIN...END
    // block fails to parse rather than parsing and being rejected
    // semantically, but either way the SQL is refused.
    let c = conn();
    let err = c
        .clone()
        .create_statement("begin select 1; end")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_rollback_to_savepoint() {
    // sql-parser's Rollback grammar is just the bare ROLLBACK keyword —
    // TO SAVEPOINT has no equivalent, so this fails to parse.
    let c = conn();
    let err = c
        .clone()
        .create_statement("rollback to savepoint sp1")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

fn result_string(r: &ResultType) -> &str {
    match r {
        ResultType::ResultString(s) => s,
        _ => panic!("expected a ResultString, got a different ResultType variant"),
    }
}

#[test]
fn test_get_results_returns_the_first_result_then_takes_it() {
    let c = conn();
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    stmt.execute().unwrap();

    let first = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't' created");

    // Calling it again without advancing no longer returns a second copy
    // of the same result — a StreamingResult can't be cloned to produce
    // one (see get_results' own doc comment), so every result, streaming
    // or not, is retrievable exactly once. The slot at this position is
    // already empty.
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_results_returns_none_when_there_are_no_results() {
    let c = conn();
    let mut stmt = c.create_statement("drop table t").unwrap();
    stmt.execute().unwrap();
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_nextresult_walks_through_multiple_statements() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t1 (id integer); create table t2 (id integer)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 2);

    let first = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't1' created");

    let second = stmt.get_nextresult().unwrap().unwrap();
    assert_eq!(result_string(&second), "Table 't2' created");

    assert!(stmt.get_nextresult().unwrap().is_none());
    // Cursor position didn't move past the end (still points at index
    // 1), but the result *at* that position was already taken by the
    // get_nextresult() call above — a second read finds the slot empty,
    // same as test_get_results_returns_the_first_result_then_takes_it.
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_nextresult_before_get_results_starts_from_the_first_result() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t1 (id integer); create table t2 (id integer)")
        .unwrap();
    stmt.execute().unwrap();

    // get_nextresult() with no prior get_results() call must return the
    // *first* result (index 0), not skip it.
    let first = stmt.get_nextresult().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't1' created");
}

#[test]
fn test_execute_alter_table_add_column_records_a_result_string() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement("alter table t add column plan varchar(10) default 'free'")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(result_string(nth_result(&stmt, 0)), "Table \"t\" altered");
}

#[test]
fn test_execute_alter_table_drop_column_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("alter table nope drop column x")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_alter_table_rename_column_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_alter").unwrap();
    let mut stmt = c
        .create_statement("alter table t rename column a to b")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_new_rejects_alter_table_with_multiple_operations() {
    // sql-parser's AlterTable grammar has exactly one `operation`, not a
    // list — a second operation fails to parse rather than parsing and
    // being rejected semantically.
    let c = conn();
    let err = c
        .create_statement("alter table t add column x integer, add column y integer")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_drop_column_if_exists() {
    // sql-parser's DropColumn grammar has no IF EXISTS.
    let c = conn();
    let err = c
        .create_statement("alter table t drop column if exists x")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_dropping_multiple_columns() {
    // sql-parser's DropColumn grammar takes exactly one Ident, not a list.
    let c = conn();
    let err = c
        .create_statement("alter table t drop column x, y")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_rename_table() {
    let c = conn();
    let err = c.create_statement("alter table t rename to u").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_alter_table_add_foreign_key_records_a_result_string() {
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id))",
    )
    .unwrap();
    let mut stmt = c
        .create_statement(
            "alter table orders add foreign key (customer_id) references customers(id)",
        )
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(
        result_string(nth_result(&stmt, 0)),
        "Table \"orders\" altered"
    );
}

#[test]
fn test_execute_alter_table_drop_foreign_key_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("alter table nope drop constraint fk_cust")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_add_constraint_not_valid() {
    // sql-parser's AddConstraint grammar has no NOT VALID.
    let c = conn();
    let err = c
        .create_statement(
            "alter table t add constraint fk_x foreign key (x) references y(id) not valid",
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_drop_constraint_if_exists() {
    // sql-parser's DropConstraint grammar has no IF EXISTS.
    let c = conn();
    let err = c
        .create_statement("alter table t drop constraint if exists fk_x")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_composite_foreign_key() {
    let c = conn();
    let err = c
        .create_statement(
            "create table t (a integer, b integer, foreign key(a, b) references u(x, y))",
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_tolerates_a_placeholder_in_ordinary_sql() {
    // The actual ask: semantic_validate must not reject "?" outright —
    // Statement::new (which PreparedStatement::new itself calls) has to
    // succeed for SQL containing a placeholder, the same way it would
    // for any other well-formed INSERT.
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    c.create_statement("insert into t values (?)").unwrap();
}

#[test]
fn test_prepared_insert_executes_with_bound_values() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(10), primary key(id))",
    )
    .unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?, ?)")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 2);
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.set_field(1, ValueItem::Str(("alice".into(), 10)))
        .unwrap();
    let result = stmt.execute().unwrap();
    assert!(matches!(result, ResultType::Count(1)), "got {result:?}");

    let mut check = c.create_statement("select * from t").unwrap();
    check.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut check, 0);
    assert_eq!(
        rows,
        vec![vec![
            ValueItem::Integer(1),
            ValueItem::Str(("alice".into(), 10))
        ]]
    );
}

#[test]
fn test_prepared_insert_can_be_reused_with_different_bound_values() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();

    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.execute().unwrap();
    stmt.set_field(0, ValueItem::Integer(2)).unwrap();
    stmt.execute().unwrap();

    let mut check = c.create_statement("select * from t").unwrap();
    check.execute().unwrap();
    let (_, mut rows) = take_streaming_result(&mut check, 0);
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
    );
}

#[test]
fn test_prepared_insert_type_checks_bound_values() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    stmt.set_field(0, ValueItem::Str(("nope".into(), 10)))
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_insert_enforces_not_null() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    stmt.set_field(0, ValueItem::Null).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_execute_fails_when_a_parameter_is_unbound() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_set_field_rejects_an_out_of_range_index() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    let err = stmt.set_field(1, ValueItem::Integer(1)).unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_rejects_multiple_sql_statements() {
    let c = conn();
    let err = c
        .create_prepared_statement("insert into t values (?); insert into u values (?)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::TooManyPreparedStatement));
}

#[test]
fn test_prepared_statement_rejects_ddl() {
    let c = conn();
    let err = c
        .create_prepared_statement("create table t (id integer)")
        .unwrap_err();
    assert!(
        matches!(err, SchemaError::BadPreparedStatement(_)),
        "got {err:?}"
    );
}

#[test]
fn test_prepared_update_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("update t set id = ? where id = ?")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 2);
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.set_field(1, ValueItem::Integer(2)).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_delete_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("delete from t where id = ?")
        .unwrap();
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_select_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("select * from t")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 0);
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_copy_into_records_load_counts() {
    let path = std::env::temp_dir().join(format!(
        "squeal_sql_stmt_copy_test_{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, "id\n1\n2\n").unwrap();

    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement(&format!("copy into t from @{}", path.to_str().unwrap()))
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(
        result_string(nth_result(&stmt, 0)),
        "2 row(s) loaded, 0 row(s) failed"
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_execute_copy_into_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("copy into nope from @/tmp/x.csv")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_copy_into_with_a_file_format_clause() {
    // sql-parser's CopyInto grammar is exactly "COPY INTO <table> FROM
    // @<path>" — a trailing FILE_FORMAT clause is leftover, unparsed
    // input after a complete statement, so this fails to parse rather
    // than parsing and being rejected semantically.
    let c = conn();
    let err = c
        .create_statement("copy into t from @stage file_format = (type = csv)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_copy_into_with_a_pattern_clause() {
    let c = conn();
    let err = c
        .create_statement("copy into t from @stage pattern = '.*.csv'")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

// ---- WHERE clause ----

#[test]
fn test_execute_select_where_filters_by_integer_comparison() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "insert into t values (2)").unwrap();
    run(&c, "insert into t values (3)").unwrap();
    let mut stmt = c.create_statement("select * from t where id > 1").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(2)], vec![ValueItem::Integer(3)],]
    );
}

#[test]
fn test_execute_select_where_filters_by_string_equality() {
    // Regression test: a varchar(n) column's stored capacity (n) used to
    // make this always compare unequal to a literal's own capacity (its
    // length), so `WHERE name = 'raj'` matched nothing — see
    // plan::eval::values_equal's own doc comment.
    let c = conn();
    run(&c, "create table t (name varchar(10))").unwrap();
    run(&c, "insert into t values ('raj')").unwrap();
    run(&c, "insert into t values ('kav')").unwrap();
    let mut stmt = c
        .create_statement("select * from t where name = 'raj'")
        .unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![ValueItem::Str(("raj".into(), 10))]]);
}

#[test]
fn test_execute_select_where_with_and_or() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    for i in 1..=5 {
        run(&c, &format!("insert into t values ({i})")).unwrap();
    }
    let mut stmt = c
        .clone()
        .create_statement("select * from t where id > 1 and id < 4")
        .unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(2)], vec![ValueItem::Integer(3)]]
    );

    let mut stmt = c
        .create_statement("select * from t where id = 1 or id = 5")
        .unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(5)]]
    );
}

#[test]
fn test_execute_select_where_on_unknown_column_fails() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let err = run(&c, "select * from t where nope = 1").unwrap_err();
    assert!(matches!(err, SchemaError::FieldNotFound(_)), "got {err:?}");
}

// ---- boolean columns ----

#[test]
fn test_execute_create_table_with_boolean_column_insert_and_select() {
    let c = conn();
    run(&c, "create table t (active boolean)").unwrap();
    run(&c, "insert into t values (true)").unwrap();
    run(&c, "insert into t values (false)").unwrap();
    let mut stmt = c.create_statement("select * from t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Boolean(true)],
            vec![ValueItem::Boolean(false)],
        ]
    );
}

#[test]
fn test_execute_select_where_boolean_equals_true() {
    let c = conn();
    run(&c, "create table t (id integer, active boolean)").unwrap();
    run(&c, "insert into t values (1, true)").unwrap();
    run(&c, "insert into t values (2, false)").unwrap();
    let mut stmt = c
        .create_statement("select * from t where active = true")
        .unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1), ValueItem::Boolean(true)]]
    );
}

// ---- multi-table FROM / cross join ----

#[test]
fn test_execute_select_two_tables_produces_the_full_cross_product() {
    let c = conn();
    run(&c, "create table t1 (id integer)").unwrap();
    run(&c, "create table t2 (code integer)").unwrap();
    run(&c, "insert into t1 values (1)").unwrap();
    run(&c, "insert into t1 values (2)").unwrap();
    run(&c, "insert into t2 values (10)").unwrap();
    run(&c, "insert into t2 values (20)").unwrap();
    let mut stmt = c.create_statement("select * from t1, t2").unwrap();
    stmt.execute().unwrap();
    let (columns, mut rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["id".to_string(), "code".to_string()]);
    rows.sort_by_key(|r| match (&r[0], &r[1]) {
        (ValueItem::Integer(a), ValueItem::Integer(b)) => (*a, *b),
        _ => panic!("unexpected row shape"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Integer(10)],
            vec![ValueItem::Integer(1), ValueItem::Integer(20)],
            vec![ValueItem::Integer(2), ValueItem::Integer(10)],
            vec![ValueItem::Integer(2), ValueItem::Integer(20)],
        ]
    );
}

#[test]
fn test_execute_select_qualified_columns_across_two_different_tables() {
    // Regression test: EvalExpr::Value used to be a (table_id, field_id)
    // pair indexed directly into UnionJoin's single combined row, so any
    // column from a table after the first silently read the wrong
    // table's value at that same position — see
    // EvalExpr::flat_position's own doc comment.
    let c = conn();
    run(&c, "create table t1 (id integer, name varchar(10))").unwrap();
    run(&c, "create table t2 (code integer, label varchar(10))").unwrap();
    run(&c, "insert into t1 values (1, 'raj')").unwrap();
    run(&c, "insert into t2 values (99, 'zzz')").unwrap();
    let mut stmt = c.create_statement("select * from t1, t2").unwrap();
    stmt.execute().unwrap();
    let (columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        columns,
        vec![
            "id".to_string(),
            "name".to_string(),
            "code".to_string(),
            "label".to_string()
        ]
    );
    assert_eq!(
        rows,
        vec![vec![
            ValueItem::Integer(1),
            ValueItem::Str(("raj".into(), 10)),
            ValueItem::Integer(99),
            ValueItem::Str(("zzz".into(), 10)),
        ]]
    );
}

#[test]
fn test_execute_select_self_join_produces_the_full_cross_product() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "insert into t values (2)").unwrap();
    run(&c, "insert into t values (3)").unwrap();
    let mut stmt = c.create_statement("select * from t, t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows.len(), 9, "3x3 self-join must produce 9 rows, not 3");
}

#[test]
fn test_execute_select_unqualified_ambiguous_column_across_tables_fails() {
    let c = conn();
    run(&c, "create table t1 (id integer)").unwrap();
    run(&c, "create table t2 (id integer)").unwrap();
    run(&c, "insert into t1 values (1)").unwrap();
    run(&c, "insert into t2 values (2)").unwrap();
    let err = run(&c, "select id from t1, t2").unwrap_err();
    assert!(
        matches!(err, SchemaError::AmbiguousFieldError(_)),
        "got {err:?}"
    );
}

// ---- LIMIT ----

#[test]
fn test_execute_select_limit_caps_rows() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    for i in 1..=5 {
        run(&c, &format!("insert into t values ({i})")).unwrap();
    }
    let mut stmt = c.create_statement("select * from t limit 2").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_execute_select_limit_larger_than_row_count_returns_every_row() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let mut stmt = c.create_statement("select * from t limit 100").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows.len(), 1);
}

// ---- computed SELECT-list expressions ----

#[test]
fn test_execute_select_computed_arithmetic_expression() {
    let c = conn();
    run(&c, "create table t (a integer, b integer)").unwrap();
    run(&c, "insert into t values (2, 3)").unwrap();
    let mut stmt = c.create_statement("select a+b from t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![ValueItem::Integer(5)]]);
}

#[test]
fn test_execute_select_literal_expression_with_no_from_clause() {
    let c = conn();
    let mut stmt = c.create_statement("select 1+2").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![ValueItem::Integer(3)]]);
}

// ---- SHOW TABLE INDEX ----

#[test]
fn test_show_table_index_lists_primary_key_and_unique_index() {
    // Only PRIMARY KEY and UNIQUE are achievable today: standalone
    // CREATE INDEX parses but isn't wired up in Statement::execute() at
    // all (falls through its `_ => {}` catch-all, silently a no-op), and
    // CREATE TABLE's own grammar has no plain, non-unique index
    // constraint — so there's no way yet to produce a bare "INDEX" kind
    // row to also assert on here.
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, email varchar(50) not null, \
         name varchar(50), primary key(id), unique(email))",
    )
    .unwrap();

    let mut stmt = c.create_statement("show table index customers").unwrap();
    stmt.execute().unwrap();
    let (columns, rows) = nth_result_as_result(&stmt, 0);
    assert_eq!(columns, vec!["Name", "Kind", "Columns", "Details"]);

    let kinds = rows
        .iter()
        .map(|r| match &r[1] {
            ValueItem::Str((s, _)) => s.clone(),
            other => panic!("expected a Str, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"PRIMARY KEY".to_string()), "got {kinds:?}");
    assert!(kinds.contains(&"UNIQUE".to_string()), "got {kinds:?}");
}

#[test]
fn test_show_table_index_lists_foreign_keys() {
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table orders (id integer not null, customer_id integer \
         references customers(id), primary key(id))",
    )
    .unwrap();

    let mut stmt = c.create_statement("show table index orders").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = nth_result_as_result(&stmt, 0);

    let fk_row = rows
        .iter()
        .find(|r| matches!(&r[1], ValueItem::Str((s, _)) if s == "FOREIGN KEY"))
        .unwrap_or_else(|| panic!("no FOREIGN KEY row in {rows:?}"));
    assert_eq!(
        fk_row[2],
        ValueItem::Str(("customer_id".into(), DEFAULT_VAR_SIZE as u32))
    );
    assert_eq!(
        fk_row[3],
        ValueItem::Str(("-> customers.id".into(), DEFAULT_VAR_SIZE as u32))
    );
}

#[test]
fn test_show_table_index_fails_for_an_unknown_table() {
    let c = conn();
    let err = run(&c, "show table index nope").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_show_table_index_fails_for_a_temp_table() {
    let c = conn();
    run(&c, "create table temp.t (id integer)").unwrap();
    let err = run(&c, "show table index temp.t").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

// ---- ANALYZE ----
//
// Schema::analyze_table (schema_ops/schema.rs) is a no-op stub for now
// (see its own comment) — these only prove the SQL surface (parsing,
// table resolution, dispatch) works end-to-end, not that any stats
// actually get collected yet.

#[test]
fn test_analyze_table_parses_and_dispatches() {
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, primary key(id))",
    )
    .unwrap();

    let mut stmt = c.create_statement("analyze table customers").unwrap();
    stmt.execute().unwrap();
    let ResultType::ResultString(s) = nth_result(&stmt, 0) else {
        panic!("expected a ResultString, got a different ResultType variant");
    };
    assert_eq!(s, "Table 'customers' analyzed");
}

#[test]
fn test_analyze_table_fails_for_an_unknown_table() {
    let c = conn();
    let err = run(&c, "analyze table nope").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_analyze_table_fails_for_a_temp_table() {
    let c = conn();
    run(&c, "create table temp.t (id integer)").unwrap();
    let err = run(&c, "analyze table temp.t").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_analyze_tables_covers_every_table_in_the_current_schema() {
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table orders (id integer not null, primary key(id))",
    )
    .unwrap();

    let mut stmt = c.create_statement("analyze tables").unwrap();
    stmt.execute().unwrap();
    let ResultType::ResultString(s) = nth_result(&stmt, 0) else {
        panic!("expected a ResultString, got a different ResultType variant");
    };
    assert_eq!(s, "2 table(s) analyzed");
}

// ---- CREATE INDEX ----

#[test]
fn test_create_index_unique_succeeds_and_shows_up_in_show_table_index() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, code integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 10)").unwrap();
    run(&c, "insert into t values (2, 20)").unwrap();
    run(&c, "create unique index idx_code on t(code)").unwrap();

    let mut stmt = c.create_statement("show table index t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = nth_result_as_result(&stmt, 0);
    let row = rows
        .iter()
        .find(|r| r[0] == ValueItem::Str(("idx_code".into(), DEFAULT_VAR_SIZE as u32)))
        .unwrap_or_else(|| panic!("no idx_code row in {rows:?}"));
    assert_eq!(
        row[1],
        ValueItem::Str(("UNIQUE".into(), DEFAULT_VAR_SIZE as u32))
    );
}

#[test]
fn test_create_unique_index_rejects_existing_duplicate_values() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, code integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 10)").unwrap();
    run(&c, "insert into t values (2, 10)").unwrap();
    let err = run(&c, "create unique index idx_code on t(code)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    // Must not leave a half-created index behind.
    assert!(
        c.current_schema()
            .unwrap()
            .get_table("t")
            .unwrap()
            .indices
            .len()
            == 1
    );
}

#[test]
fn test_create_plain_index_tolerates_and_survives_duplicate_values() {
    // The actual point of appending the row's own identity to a
    // non-unique index's key (see Schema::create_index's own doc
    // comment): both the backfill over already-duplicated data and a
    // later INSERT adding yet another duplicate must succeed, not hit
    // the backing BPlusTree's own duplicate-key rejection.
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, code integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 10)").unwrap();
    run(&c, "insert into t values (2, 10)").unwrap();
    run(&c, "create index idx_code on t(code)").unwrap();
    run(&c, "insert into t values (3, 10)").unwrap();

    let mut stmt = c.create_statement("select * from t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows.len(),
        3,
        "all three duplicate-valued rows must survive"
    );
}

#[test]
fn test_create_index_if_not_exists_is_idempotent() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    run(&c, "create index idx_id on t(id)").unwrap();
    run(&c, "create index if not exists idx_id on t(id)").unwrap();
    assert_eq!(
        c.current_schema()
            .unwrap()
            .get_table("t")
            .unwrap()
            .indices
            .len(),
        1
    );
}

#[test]
fn test_create_index_rejects_a_duplicate_name_without_if_not_exists() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    run(&c, "create index idx_id on t(id)").unwrap();
    let err = run(&c, "create index idx_id on t(id)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_index_fails_for_an_unknown_column() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let err = run(&c, "create index idx_nope on t(nope)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_index_fails_for_an_unknown_table() {
    let c = conn();
    let err = run(&c, "create index idx_id on nope(id)").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

// KNOWN BUG, not join-specific — found while testing ORDER BY on a
// joined column, but reproduces with a single table too: post_visit_query
// only ever applies query.order_by inside the `if let Some(limit) = ...`
// branch. A bare `ORDER BY` with no `LIMIT` is silently discarded — the
// query still succeeds, just returns rows in scan order instead of the
// requested order.
#[test]
fn test_order_by_without_limit_is_applied() {
    // Regression test: post_visit_query used to only ever apply
    // query.order_by inside the `if let Some(limit) = ...` branch, so a
    // bare ORDER BY with no LIMIT silently did nothing.
    //
    // Sorting by `rank`, not the primary key `id` — a table scan
    // naturally comes back in primary-key (B+tree) order, so ordering
    // by `id` itself would "accidentally" look correct even if ORDER BY
    // were a complete no-op. `rank` is deliberately inserted in the
    // OPPOSITE order from `id` so the two orderings can't coincide.
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, rank integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 30)").unwrap();
    run(&c, "insert into t values (2, 20)").unwrap();
    run(&c, "insert into t values (3, 10)").unwrap();

    let mut stmt = c
        .create_statement("select rank from t order by rank")
        .unwrap();
    stmt.execute().unwrap();
    let (_columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(10)],
            vec![ValueItem::Integer(20)],
            vec![ValueItem::Integer(30)],
        ]
    );
}

#[test]
fn test_order_by_desc_without_limit_is_applied() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, rank integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 10)").unwrap();
    run(&c, "insert into t values (2, 20)").unwrap();
    run(&c, "insert into t values (3, 30)").unwrap();

    let mut stmt = c
        .create_statement("select rank from t order by rank desc")
        .unwrap();
    stmt.execute().unwrap();
    let (_columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(30)],
            vec![ValueItem::Integer(20)],
            vec![ValueItem::Integer(10)],
        ]
    );
}

#[cfg(test)]
mod join_tests {
    use super::*;

    fn setup(c: &Arc<Connection<MemFile>>) {
        run(
            c,
            "create table t1 (id integer not null, name varchar(20), primary key(id))",
        )
        .unwrap();
        run(
            c,
            "create table t2 (id integer not null, val integer, primary key(id))",
        )
        .unwrap();
        run(c, "insert into t1 values (1, 'alice')").unwrap();
        run(c, "insert into t1 values (2, 'bob')").unwrap();
        run(c, "insert into t2 values (2, 200)").unwrap();
        run(c, "insert into t2 values (3, 300)").unwrap();
    }

    // Regression test: get_tables used to resolve the joined-in relation
    // from `qtable.relation` (the BASE table) instead of `j.relation`
    // (the actual table named after JOIN) — so every join silently
    // self-joined the base table against itself instead of the table it
    // actually named, and any reference to the real second table (e.g.
    // its own ON-clause column) failed with "table not found".
    #[test]
    fn test_inner_join_matches_against_the_actual_joined_table() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select * from t1 join t2 on t1.id = t2.id")
            .unwrap();
        stmt.execute().unwrap();
        let (columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(columns, vec!["id", "name", "id", "val"]);
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Integer(2),
                ValueItem::Str(("bob".into(), 20)),
                ValueItem::Integer(2),
                ValueItem::Integer(200),
            ]]
        );
    }

    // Regression test: SELECT *, qualified column references, WHERE,
    // and GROUP BY all resolved column names against the top-level FROM
    // table list only, never descending into a table's nested `.joins`
    // — so a joined table's columns were invisible everywhere except
    // the ON-clause's own private 2-table resolution list. Fixed via
    // QueryVisitor::flatten_tables.
    #[test]
    fn test_qualified_column_from_the_joined_table_resolves() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select t1.name, t2.val from t1 join t2 on t1.id = t2.id")
            .unwrap();
        stmt.execute().unwrap();
        let (columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(columns, vec!["name", "val"]);
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Str(("bob".into(), 20)),
                ValueItem::Integer(200)
            ]]
        );
    }

    #[test]
    fn test_ambiguous_column_across_joined_tables_is_rejected() {
        let c = conn();
        setup(&c);
        let err = run(&c, "select id from t1 join t2 on t1.id = t2.id").unwrap_err();
        assert!(
            matches!(err, SchemaError::AmbiguousFieldError(_)),
            "{err:?}"
        );
    }

    #[test]
    fn test_where_clause_can_reference_a_joined_column() {
        let c = conn();
        setup(&c);
        run(&c, "insert into t1 values (3, 'carol')").unwrap();
        run(&c, "insert into t2 values (1, 50)").unwrap();
        let mut stmt = c
            .create_statement("select t1.id from t1 join t2 on t1.id = t2.id where t2.val > 100")
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, mut rows) = take_streaming_result(&mut stmt, 0);
        rows.sort();
        // id=1 has val=50 (excluded); id=2 has val=200 and id=3 has
        // val=300 (both > 100).
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(2)], vec![ValueItem::Integer(3)]]
        );
    }

    #[test]
    fn test_order_by_a_joined_column_sorts_the_projected_output_correctly() {
        let c = conn();
        setup(&c);
        run(&c, "insert into t1 values (3, 'carol')").unwrap();
        run(&c, "insert into t2 values (1, 999)").unwrap();
        // SELECT list only has 2 columns (t1.id, t2.val), but ORDER BY's
        // raw FROM-clause position for t2.val would be 3 (t1 has 2
        // columns) — resolving against that raw position instead of the
        // projected output's own field list used to index a 2-element
        // row with index 3 and panic.
        let mut stmt = c
            .create_statement(
                "select t1.id, t2.val from t1 join t2 on t1.id = t2.id order by t2.val",
            )
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Integer(2), ValueItem::Integer(200)],
                vec![ValueItem::Integer(3), ValueItem::Integer(300)],
                vec![ValueItem::Integer(1), ValueItem::Integer(999)],
            ]
        );
    }

    #[test]
    fn test_left_join_emits_unmatched_left_rows_with_right_nulls() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select * from t1 left join t2 on t1.id = t2.id")
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, mut rows) = take_streaming_result(&mut stmt, 0);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![
                    ValueItem::Integer(1),
                    ValueItem::Str(("alice".into(), 20)),
                    ValueItem::Null,
                    ValueItem::Null,
                ],
                vec![
                    ValueItem::Integer(2),
                    ValueItem::Str(("bob".into(), 20)),
                    ValueItem::Integer(2),
                    ValueItem::Integer(200),
                ],
            ]
        );
    }

    #[test]
    fn test_right_join_emits_unmatched_right_rows_with_left_nulls() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select * from t1 right join t2 on t1.id = t2.id")
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, mut rows) = take_streaming_result(&mut stmt, 0);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![
                    ValueItem::Null,
                    ValueItem::Null,
                    ValueItem::Integer(3),
                    ValueItem::Integer(300)
                ],
                vec![
                    ValueItem::Integer(2),
                    ValueItem::Str(("bob".into(), 20)),
                    ValueItem::Integer(2),
                    ValueItem::Integer(200),
                ],
            ]
        );
    }

    #[test]
    fn test_full_join_emits_unmatched_rows_from_both_sides() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select * from t1 full join t2 on t1.id = t2.id")
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(rows.len(), 3, "{rows:?}");
    }

    #[test]
    fn test_missing_on_clause_is_rejected_for_a_non_cross_join() {
        let c = conn();
        setup(&c);
        let err = run(&c, "select * from t1 join t2").unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "{err:?}");
    }

    #[test]
    fn test_using_clause_is_rejected_as_unsupported() {
        let c = conn();
        setup(&c);
        let err = run(&c, "select * from t1 join t2 using (id)").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedFeature(_)), "{err:?}");
    }

    // KNOWN BUG, not yet fixed: JoinSource::new calls equi_join_fields
    // on the ON expression unconditionally, before checking join_type —
    // but a CROSS JOIN's on_expr is EvalExpr::None (there is no ON
    // clause), which equi_join_fields has no case for, so every CROSS
    // JOIN fails outright instead of falling through to the
    // JoinType::Cross => UnionJoin branch that already exists right
    // below it.
    // Regression test: JoinSource::new used to call equi_join_fields
    // unconditionally, even for CROSS JOIN (whose on_expr is
    // EvalExpr::None — there's no ON clause) — equi_join_fields has no
    // case for that, so every CROSS JOIN failed outright before ever
    // reaching the JoinType::Cross => UnionJoin branch right below it.
    #[test]
    fn test_cross_join_produces_the_full_cross_product() {
        let c = conn();
        setup(&c);
        let mut stmt = c
            .create_statement("select * from t1 cross join t2")
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, mut rows) = take_streaming_result(&mut stmt, 0);
        rows.sort();
        assert_eq!(rows.len(), 4, "2 t1 rows * 2 t2 rows = 4: {rows:?}");
    }

    // Regression test: chaining a second JOIN onto the same base table
    // used to build two INDEPENDENT JoinSources (t1⋈t2 and t1⋈t3) and
    // cross-product them via UnionJoin, instead of folding them into a
    // proper left-deep join chain. That didn't just produce the wrong
    // row count — flatten_tables' logical column numbering (t1, t2, t3
    // concatenated) didn't match the actual physical row (t1++t2, then
    // ANOTHER independent t1++t3), so projection read the wrong
    // physical column outright (confirmed via direct repro: t3.extra,
    // an integer column, came back holding t1.name's string value).
    // Fixed by folding chained joins into (t1⋈t2)⋈t3 (handle_select)
    // and resolving each join's ON clause against every table already
    // joined so far, not just [base, this relation] (get_tables) — the
    // second join's `t1.id = t3.id` needs `t1`'s position relative to
    // the FULL (t1, t2, t3) resolution list to match the running
    // left-deep side's actual field width at that point.
    #[test]
    fn test_three_way_join_produces_correct_data() {
        let c = conn();
        setup(&c);
        run(
            &c,
            "create table t3 (id integer not null, extra integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t3 values (2, 7000)").unwrap();
        run(&c, "insert into t3 values (3, 8000)").unwrap();
        let mut stmt = c
            .create_statement(
                "select t1.id, t1.name, t2.val, t3.extra from t1 join t2 on t1.id = t2.id join \
                 t3 on t1.id = t3.id",
            )
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Integer(2),
                ValueItem::Str(("bob".into(), 20)),
                ValueItem::Integer(200),
                ValueItem::Integer(7000),
            ]]
        );
    }

    // A third join whose ON clause references the FIRST joined table
    // (t2), not the base table (t1) — exercises resolving against the
    // full running chain rather than just [base, this-relation].
    #[test]
    fn test_three_way_join_where_the_last_on_clause_references_the_middle_table() {
        let c = conn();
        setup(&c);
        run(
            &c,
            "create table t3 (id integer not null, extra integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t3 values (200, 7777)").unwrap();
        let mut stmt = c
            .create_statement(
                "select t3.extra from t1 join t2 on t1.id = t2.id join t3 on t2.val = t3.id",
            )
            .unwrap();
        stmt.execute().unwrap();
        let (_columns, rows) = take_streaming_result(&mut stmt, 0);
        assert_eq!(rows, vec![vec![ValueItem::Integer(7777)]]);
    }
}

// Phase 7: outside an explicit BEGIN block a statement reads every table
// under one transaction of its own — a join over two tables holds one
// snapshot, not one per table — and that transaction lives exactly as long
// as the client holds the streaming result.
#[test]
fn test_a_select_over_several_tables_holds_one_statement_transaction() {
    let c = conn();
    run(
        &c,
        "create table a (id integer not null, v integer, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table b (id integer not null, w integer, primary key(id))",
    )
    .unwrap();
    run(&c, "insert into a values (1, 10), (2, 20)").unwrap();
    run(&c, "insert into b values (1, 100), (2, 200)").unwrap();
    let db = c.database.read().db.clone();
    assert_eq!(db.stats().active_transactions, 0);

    let mut stmt = c
        .clone()
        .create_statement("select a.v, b.w from a join b on a.id = b.id")
        .unwrap();
    stmt.execute().unwrap();
    let result = stmt.get_results().unwrap();
    let Some(ResultType::StreamingResult(mut rs)) = result else {
        panic!("expected a streaming result");
    };
    assert_eq!(
        db.stats().active_transactions,
        1,
        "one statement transaction for both tables, alive while the result is held"
    );
    let mut rows = 0;
    while rs.next_result().unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 2);
    assert_eq!(
        db.stats().active_transactions,
        1,
        "still held until the result is dropped"
    );
    drop(rs);
    assert_eq!(db.stats().active_transactions, 0);

    // Inside an explicit block the statement uses that block's transaction.
    run(&c, "begin").unwrap();
    let mut stmt = c
        .clone()
        .create_statement("select a.v, b.w from a join b on a.id = b.id")
        .unwrap();
    stmt.execute().unwrap();
    let Some(ResultType::StreamingResult(rs)) = stmt.get_results().unwrap() else {
        panic!("expected a streaming result");
    };
    assert_eq!(
        db.stats().active_transactions,
        1,
        "the BEGIN block's transaction only"
    );
    drop(rs);
    run(&c, "commit").unwrap();
    assert_eq!(db.stats().active_transactions, 0);
}


// ---- subqueries in FROM, end to end ----

fn ints(rows: &[&[i64]]) -> Vec<Vec<ValueItem>> {
    rows.iter()
        .map(|r| r.iter().map(|i| ValueItem::Integer(*i)).collect())
        .collect()
}

fn select_rows(c: &Arc<Connection<MemFile>>, sql: &str) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    take_streaming_result(&mut stmt, 0)
}

fn select_sorted(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    let mut rows = select_rows(c, sql).1;
    rows.sort();
    rows
}

// t1(id, cat): (1,0) (2,1) (3,0) (4,1) (5,0);  t2(id, val): (1,10) (2,20) (3,30) (4,40)
fn subquery_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(&c, "create table t1 (id integer not null, cat integer, primary key(id))").unwrap();
    run(&c, "create table t2 (id integer not null, val integer, primary key(id))").unwrap();
    for (id, cat) in [(1, 0), (2, 1), (3, 0), (4, 1), (5, 0)] {
        run(&c, &format!("insert into t1 values ({id}, {cat})")).unwrap();
    }
    for (id, val) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
        run(&c, &format!("insert into t2 values ({id}, {val})")).unwrap();
    }
    c
}

#[test]
fn test_select_star_from_a_subquery() {
    let c = subquery_conn();
    let (cols, rows) = select_rows(&c, "select * from (select id, cat from t1) x");
    assert_eq!(cols, ["id", "cat"]);
    let mut rows = rows;
    rows.sort();
    assert_eq!(rows, ints(&[&[1, 0], &[2, 1], &[3, 0], &[4, 1], &[5, 0]]));
}

#[test]
fn test_the_outer_query_filters_and_projects_the_subquery() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(&c, "select x.id from (select id, cat from t1 where cat = 0) x where x.id > 1"),
        ints(&[&[3], &[5]])
    );
}

#[test]
fn test_a_subquery_column_alias_is_what_the_outer_query_sees() {
    let c = subquery_conn();
    let (cols, rows) = select_rows(&c, "select renamed from (select id as renamed from t1 where id = 4) x");
    assert_eq!(cols, ["renamed"]);
    assert_eq!(rows, ints(&[&[4]]));
}

#[test]
fn test_aggregating_inside_a_subquery_and_using_it_outside() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(&c, "select c from (select count(*) as c from t1) x"),
        ints(&[&[5]])
    );
    assert_eq!(
        select_sorted(
            &c,
            "select cat, n from (select cat, count(*) as n from t1 group by cat) g where n > 2"
        ),
        ints(&[&[0, 3]])
    );
}

#[test]
fn test_order_by_and_limit_inside_a_subquery() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(&c, "select id from (select id from t1 order by id desc limit 2) x"),
        ints(&[&[4], &[5]])
    );
}

#[test]
fn test_order_by_on_the_outer_query_of_a_subquery() {
    let c = subquery_conn();
    let (_, rows) = select_rows(&c, "select id from (select id from t1 where cat = 1) x order by id desc");
    assert_eq!(rows, ints(&[&[4], &[2]]));
}

#[test]
fn test_subqueries_nest() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(
            &c,
            "select b from (select a as b from (select id as a from t1 where id < 3) i) j"
        ),
        ints(&[&[1], &[2]])
    );
}

#[test]
fn test_joining_a_subquery_to_a_real_table() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(
            &c,
            "select x.id, t2.val from (select id, cat from t1 where cat = 1) x join t2 on x.id = t2.id"
        ),
        ints(&[&[2, 20], &[4, 40]])
    );
}

#[test]
fn test_joining_a_real_table_to_a_subquery() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(
            &c,
            "select t2.val, x.cat from t2 join (select id, cat from t1) x on t2.id = x.id where x.cat = 0"
        ),
        ints(&[&[10, 0], &[30, 0]])
    );
}

#[test]
fn test_joining_two_subqueries() {
    let c = subquery_conn();
    assert_eq!(
        select_sorted(
            &c,
            "select a.id, b.val from (select id from t1 where cat = 0) a \
             join (select id, val from t2 where val > 10) b on a.id = b.id"
        ),
        ints(&[&[3, 30]])
    );
}

#[test]
fn test_left_join_from_a_subquery_keeps_unmatched_rows() {
    let c = subquery_conn();
    let rows = select_sorted(
        &c,
        "select x.id, t2.val from (select id from t1) x left join t2 on x.id = t2.id",
    );
    assert_eq!(rows.len(), 5);
    assert!(rows.contains(&vec![ValueItem::Integer(5), ValueItem::Null]));
}

#[test]
fn test_a_subquery_in_from_needs_an_alias() {
    let c = subquery_conn();
    let err = c
        .clone()
        .create_statement("select id from (select id from t1)")
        .and_then(|mut s| s.execute())
        .unwrap_err();
    assert!(err.to_string().contains("alias"), "{err}");
}

#[test]
fn test_a_subquery_over_an_empty_table_yields_no_rows() {
    let c = subquery_conn();
    run(&c, "create table empty (id integer not null, primary key(id))").unwrap();
    assert!(select_rows(&c, "select id from (select id from empty) e").1.is_empty());
}

#[test]
fn test_a_subquery_sees_the_same_snapshot_as_the_outer_query_inside_a_transaction() {
    let c = subquery_conn();
    run(&c, "begin").unwrap();
    run(&c, "insert into t1 values (99, 9)").unwrap();
    assert_eq!(
        select_sorted(&c, "select id from (select id from t1 where id = 99) x"),
        ints(&[&[99]]),
        "the subquery reads the open transaction's own uncommitted row"
    );
    run(&c, "rollback").unwrap();
}

// ---- EXPLAIN ----

fn explain(c: &Arc<Connection<MemFile>>, sql: &str) -> String {
    let mut stmt = c.clone().create_statement(&format!("explain {sql}")).unwrap();
    stmt.execute().unwrap();
    match stmt.get_results().unwrap().expect("EXPLAIN must produce a result") {
        ResultType::ResultString(s) => s,
        other => panic!("expected the rendered plan, got {other:?}"),
    }
}

// Whole plans, not fragments: a wrong column name, indentation or shape has
// to fail. (Row estimates appear because the retail tables here have stats.)
#[test]
fn test_explain_a_plain_scan() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select * from t1"),
        "Projection id, cat\n  TableScan t1 (~5 rows)"
    );
}

#[test]
fn test_explain_shows_the_filter_predicate_with_column_names() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select id from t1 where cat = 1 and id > 2"),
        "Projection id\n  Filter ((cat = 1) AND (id > 2))\n    TableScan t1 (~5 rows)"
    );
}

#[test]
fn test_explain_a_join_shows_the_algorithm_keys_and_both_inputs() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select t1.id, t2.val from t1 join t2 on t1.id = t2.id"),
        "Projection id#0, val\n  HashJoin Inner on build(id) = probe(id)\n    \
         [build] TableScan t1 (~5 rows)\n    [probe] TableScan t2 (~4 rows)"
    );
}

#[test]
fn test_explain_names_the_join_type() {
    let c = subquery_conn();
    let plan = explain(&c, "select t1.id from t1 left join t2 on t1.id = t2.id");
    assert!(plan.contains("HashJoin Left on"), "{plan}");
}

#[test]
fn test_explain_shows_sort_topn_limit() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select id from t1 order by id desc"),
        "Sort id DESC\n  Projection id\n    TableScan t1 (~5 rows)"
    );
    assert_eq!(
        explain(&c, "select id from t1 order by id limit 2"),
        "TopN 2 by id ASC\n  Projection id\n    TableScan t1 (~5 rows)"
    );
    assert_eq!(
        explain(&c, "select id from t1 limit 3"),
        "Limit 3\n  Projection id\n    TableScan t1 (~5 rows)"
    );
}

#[test]
fn test_explain_shows_aggregation_grouping_and_distinct() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select count(*) as n from t1"),
        "Aggregate count(*) AS n\n  TableScan t1 (~5 rows)"
    );
    assert_eq!(
        explain(&c, "select cat, count(*) as n from t1 group by cat"),
        "GroupAggregate by cat: cat, count(*) AS n\n  Sort cat ASC NULLS FIRST\n    TableScan t1 (~5 rows)"
    );
    assert_eq!(
        explain(&c, "select distinct cat from t1"),
        "Distinct\n  Sort cat ASC NULLS FIRST\n    Projection cat\n      TableScan t1 (~5 rows)"
    );
}

#[test]
fn test_explain_shows_a_subquery_as_a_nested_plan() {
    let c = subquery_conn();
    assert_eq!(
        explain(&c, "select x.id from (select id, cat from t1 where cat = 0) x where x.id > 1"),
        "Projection id\n  Filter (id > 1)\n    Projection id, cat\n      Filter (cat = 0)\n        TableScan t1 (~5 rows)"
    );
}

#[test]
fn test_explain_a_join_of_subqueries_keeps_each_side_a_full_plan() {
    let c = subquery_conn();
    let plan = explain(
        &c,
        "select a.id, b.val from (select id from t1 where cat = 0) a \
         join (select id, val from t2 where val > 10) b on a.id = b.id",
    );
    assert!(plan.starts_with("Projection id#0, val\n  HashJoin Inner on"), "{plan}");
    assert!(plan.contains("Filter (cat = 0)") && plan.contains("Filter (val > 10)"), "{plan}");
}

#[test]
fn test_explain_row_estimates_follow_analyze() {
    let c = conn();
    run(&c, "create table w (id integer not null, primary key(id))").unwrap();
    for i in 0..7 {
        run(&c, &format!("insert into w values ({i})")).unwrap();
    }
    run(&c, "analyze table w").unwrap();
    assert_eq!(explain(&c, "select * from w"), "Projection id\n  TableScan w (~7 rows)");
}

#[test]
fn test_explain_does_not_run_the_query_or_change_anything() {
    let c = subquery_conn();
    let before = select_sorted(&c, "select id from t1");
    let _ = explain(&c, "select id from t1 where id > 2");
    assert_eq!(select_sorted(&c, "select id from t1"), before);
}

#[test]
fn test_explain_of_something_other_than_a_select_is_rejected() {
    let c = subquery_conn();
    let err = c
        .clone()
        .create_statement("explain insert into t1 values (100, 1)")
        .and_then(|mut s| s.execute())
        .unwrap_err();
    assert!(err.to_string().contains("EXPLAIN"), "{err}");
    // ...and it did not execute the insert.
    assert_eq!(select_sorted(&c, "select id from t1 where id = 100").len(), 0);
}

#[test]
fn test_explain_reports_a_bad_query_like_the_query_would() {
    let c = subquery_conn();
    let mut stmt = c.clone().create_statement("explain select nope from t1").unwrap();
    assert!(stmt.execute().is_err());
    let mut stmt = c.clone().create_statement("explain select * from missing").unwrap();
    assert!(stmt.execute().is_err());
}

// ---- WHERE equalities across FROM items become joins ----

// Whether some plan line is the step `what` (ignoring indentation and a
// leading `[build]`/`[probe]` role).
fn has(plan: &str, what: &str) -> bool {
    plan.lines().any(|l| {
        let l = l.trim_start();
        let l = l.strip_prefix('[').and_then(|r| r.split_once("] ")).map_or(l, |(_, r)| r);
        l.starts_with(what)
    })
}

// a(id, x): (1,10) (2,20) (3,30) (4,NULL) (5,20); b(id, y): (1,100) (2,200) (2,201) (6,600); c(id, z): (2,7) (5,8)
fn where_join_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(&c, "create table a (id integer not null, x integer, primary key(id))").unwrap();
    run(&c, "create table b (id integer not null, y integer)").unwrap();
    run(&c, "create table c (id integer not null, z integer)").unwrap();
    for (id, x) in [("1", "10"), ("2", "20"), ("3", "30"), ("4", "null"), ("5", "20")] {
        run(&c, &format!("insert into a values ({id}, {x})")).unwrap();
    }
    for (id, y) in [(1, 100), (2, 200), (2, 201), (6, 600)] {
        run(&c, &format!("insert into b values ({id}, {y})")).unwrap();
    }
    for (id, z) in [(2, 7), (5, 8)] {
        run(&c, &format!("insert into c values ({id}, {z})")).unwrap();
    }
    c
}

#[test]
fn test_a_where_equality_between_comma_joined_tables_plans_a_hash_join_not_a_cross_join() {
    let c = where_join_conn();
    let plan = explain(&c, "select a.id, b.y from a, b where a.id = b.id");
    eprintln!("{plan}");
    assert!(has(&plan, "HashJoin Inner on"), "{plan}");
    assert!(!has(&plan, "CrossJoin"), "{plan}");
    assert_eq!(
        select_sorted(&c, "select a.id, b.y from a, b where a.id = b.id"),
        ints(&[&[1, 100], &[2, 200], &[2, 201]])
    );
}

#[test]
fn test_the_join_gives_exactly_the_rows_cross_join_then_filter_gives() {
    let c = where_join_conn();
    // `a.id + 0` is not a bare column, so this cannot become a join and runs
    // as a true cross join + filter: an independent path to compare against.
    for (joined, crossed) in [
        (
            "select a.id, b.y from a, b where a.id = b.id",
            "select a.id, b.y from a, b where a.id + 0 = b.id",
        ),
        (
            "select a.id, b.y from a, b where b.id = a.id and b.y > 100",
            "select a.id, b.y from a, b where b.id = a.id + 0 and b.y > 100",
        ),
        (
            "select a.id, a.x, b.y from a, b where a.x = b.y",
            "select a.id, a.x, b.y from a, b where a.x + 0 = b.y",
        ),
    ] {
        assert_eq!(select_sorted(&c, joined), select_sorted(&c, crossed), "{joined}");
    }
}

#[test]
fn test_a_null_join_key_matches_nothing() {
    let c = where_join_conn();
    // a.x has a NULL row; no other table has a NULL to pair with either.
    let rows = select_sorted(&c, "select a.id from a, c where a.x = c.z");
    assert!(rows.is_empty());
    // NULL on both sides must not match each other in the join.
    run(&c, "insert into b values (7, null)").unwrap();
    run(&c, "insert into c values (8, null)").unwrap();
    assert!(select_sorted(&c, "select b.id, c.id from b, c where b.y = c.z and b.y > 1000").is_empty());
    assert_eq!(
        select_sorted(&c, "select b.id from b, c where b.id = c.id"),
        ints(&[&[2], &[2]])
    );
}

#[test]
fn test_three_tables_chain_into_two_joins() {
    let c = where_join_conn();
    let sql = "select a.id, b.y, c.z from a, b, c where a.id = b.id and b.id = c.id";
    let plan = explain(&c, sql);
    eprintln!("{plan}");
    assert_eq!(plan.matches("HashJoin").count(), 2, "{plan}");
    assert!(!has(&plan, "CrossJoin"), "{plan}");
    assert_eq!(select_sorted(&c, sql), ints(&[&[2, 200, 7], &[2, 201, 7]]));
}

#[test]
fn test_an_item_with_no_linking_equality_is_cross_joined_and_the_rest_still_join() {
    let c = where_join_conn();
    let sql = "select a.id, b.id, c.id from a, b, c where a.id = b.id";
    let plan = explain(&c, sql);
    eprintln!("{plan}");
    assert!(has(&plan, "CrossJoin"), "{plan}");
    assert!(plan.contains("HashJoin Inner"), "{plan}");
    // 3 matching (a,b) pairs x 2 rows of c.
    assert_eq!(select_rows(&c, sql).1.len(), 3 * 2);
}

#[test]
fn test_conditions_that_are_not_a_cross_table_column_equality_stay_a_cross_join() {
    let c = where_join_conn();
    for sql in [
        "select a.id from a, b where a.id = b.id or a.id = 1",
        "select a.id from a, b where a.id > b.id",
        "select a.id from a, b where a.id = a.x",
        "select a.id from a, b where a.id = 1",
        "select a.id from a, b",
    ] {
        let plan = explain(&c, sql);
        assert!(has(&plan, "CrossJoin"), "{sql}\n{plan}");
        assert!(!plan.contains("HashJoin"), "{sql}\n{plan}");
    }
    // ...and still compute the right answer: a.id = 1 pairs with all four b
    // rows, and a.id = 2 with the two b rows of id 2.
    assert_eq!(
        select_sorted(&c, "select a.id, b.id from a, b where a.id = b.id or a.id = 1"),
        ints(&[&[1, 1], &[1, 2], &[1, 2], &[1, 6], &[2, 2], &[2, 2]])
    );
}

// Comparing columns of different data types is an error (see plan::eval's
// same_type), and such an equality is not planned as a join — the
// comparison itself is what rejects it.
#[test]
fn test_comparing_columns_of_different_types_is_an_error_not_a_join() {
    let c = conn();
    run(&c, "create table i (id integer not null, primary key(id))").unwrap();
    run(&c, "create table d (v double)").unwrap();
    run(&c, "insert into i values (1)").unwrap();
    run(&c, "insert into d values (1.0)").unwrap();
    let sql = "select i.id from i, d where i.id = d.v";
    let plan = explain(&c, sql);
    assert!(has(&plan, "CrossJoin") && !plan.contains("HashJoin"), "{plan}");
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let ResultType::StreamingResult(mut s) = stmt.results[0].take().unwrap() else {
        panic!("expected a stream")
    };
    let err = s.next_result().unwrap_err().to_string();
    assert!(err.contains("different data types"), "{err}");
}

#[test]
fn test_string_keys_join_across_different_varchar_lengths() {
    let c = conn();
    run(&c, "create table s1 (k varchar(5) not null, v integer)").unwrap();
    run(&c, "create table s2 (k varchar(20) not null, w integer)").unwrap();
    run(&c, "insert into s1 values ('ann', 1)").unwrap();
    run(&c, "insert into s1 values ('bob', 2)").unwrap();
    run(&c, "insert into s2 values ('bob', 20)").unwrap();
    run(&c, "insert into s2 values ('cy', 30)").unwrap();
    let sql = "select s1.v, s2.w from s1, s2 where s1.k = s2.k";
    assert!(explain(&c, sql).contains("HashJoin Inner"));
    assert_eq!(select_sorted(&c, sql), ints(&[&[2, 20]]));
}

#[test]
fn test_a_join_between_a_table_and_an_outer_join_chain_uses_the_equality() {
    let c = where_join_conn();
    let sql = "select a.id, b.y, c.z from a left join b on a.id = b.id, c where a.id = c.id";
    let plan = explain(&c, sql);
    eprintln!("{plan}");
    assert!(plan.contains("HashJoin Left") && plan.contains("HashJoin Inner"), "{plan}");
    // a=2 matches b twice; a=5 has no b row (NULL y) but does match c.
    let mut want = ints(&[&[2, 200, 7], &[2, 201, 7]]);
    want.push(vec![ValueItem::Integer(5), ValueItem::Null, ValueItem::Integer(8)]);
    want.sort();
    assert_eq!(select_sorted(&c, sql), want);
}

#[test]
fn test_where_null_predicates_filter_the_row_instead_of_erroring() {
    let c = where_join_conn();
    assert_eq!(select_sorted(&c, "select id from a where x = 20"), ints(&[&[2], &[5]]));
    assert_eq!(select_sorted(&c, "select id from a where x > 15 and x < 100"), ints(&[&[2], &[3], &[5]]));
}

// ---- pushdown safety, checked against the engine ----
//
// For each outer/inner join and each side, apply a predicate two ways:
//   WHERE above the join, versus
//   pushed into that table (wrapped as a derived table filtered by it).
// plan::conjuncts says the second is equivalent exactly when the table is not
// NULL-extended by a join in its chain. Both directions are asserted: where
// it says pushable the results must be identical, and where it says not, they
// must really differ on this data (so the rule is not just timid).
fn pushdown_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(&c, "create table a (id integer not null, primary key(id))").unwrap();
    run(&c, "create table b (id integer not null, y integer)").unwrap();
    run(&c, "create table c (id integer not null, z integer)").unwrap();
    for id in [1, 2, 3] {
        run(&c, &format!("insert into a values ({id})")).unwrap();
    }
    // b(9) matches no a; a(3) matches no b; b(2)'s y fails the predicate
    for (id, y) in [(1, 100), (2, 5), (9, 7)] {
        run(&c, &format!("insert into b values ({id}, {y})")).unwrap();
    }
    for (id, z) in [(1, 10), (2, 1), (3, 10)] {
        run(&c, &format!("insert into c values ({id}, {z})")).unwrap();
    }
    c
}

// (table, its columns, qualified predicate, unqualified predicate)
const PUSHDOWN_TABLES: [(&str, &str, &str, &str); 3] = [
    ("a", "id", "a.id > 1", "id > 1"),
    ("b", "id, y", "b.y > 50", "y > 50"),
    ("c", "id, z", "c.z > 5", "z > 5"),
];

#[test]
fn test_pushing_a_predicate_below_a_join_matches_where_exactly_when_the_table_is_not_null_supplied() {
    use crate::plan::conjuncts::null_supplied_tables;
    use crate::source::join::JoinType;
    let c = pushdown_conn();
    // (FROM-clause text, the chain's join types, tables in chain order)
    let chains: [(&str, Vec<JoinType>, Vec<usize>); 8] = [
        ("{a} join {b} on a.id = b.id", vec![JoinType::Inner], vec![0, 1]),
        ("{a} left join {b} on a.id = b.id", vec![JoinType::Left], vec![0, 1]),
        ("{a} right join {b} on a.id = b.id", vec![JoinType::Right], vec![0, 1]),
        ("{a} full join {b} on a.id = b.id", vec![JoinType::Full], vec![0, 1]),
        (
            "{a} left join {b} on a.id = b.id join {c} on a.id = c.id",
            vec![JoinType::Left, JoinType::Inner],
            vec![0, 1, 2],
        ),
        (
            "{a} join {b} on a.id = b.id right join {c} on a.id = c.id",
            vec![JoinType::Inner, JoinType::Right],
            vec![0, 1, 2],
        ),
        (
            "{a} left join {b} on a.id = b.id full join {c} on a.id = c.id",
            vec![JoinType::Left, JoinType::Full],
            vec![0, 1, 2],
        ),
        (
            "{a} join {b} on a.id = b.id join {c} on a.id = c.id",
            vec![JoinType::Inner, JoinType::Inner],
            vec![0, 1, 2],
        ),
    ];
    let mut differing = 0;
    for (from, joins, in_chain) in chains {
        let supplied = null_supplied_tables(&joins);
        for (pos, &t) in in_chain.iter().enumerate() {
            let (_, _, qualified, unqualified) = PUSHDOWN_TABLES[t];
            let render = |wrapped: Option<usize>| {
                let mut f = from.to_string();
                for (i, (name, cols, _, plain)) in PUSHDOWN_TABLES.iter().enumerate() {
                    let text = if Some(i) == wrapped {
                        format!("(select {cols} from {name} where {plain}) {name}")
                    } else {
                        name.to_string()
                    };
                    f = f.replace(&format!("{{{name}}}"), &text);
                }
                f
            };
            let select = "select a.id, b.id, c.id";
            let cols_available: Vec<&str> = ["a", "b", "c"][..in_chain.len()].to_vec();
            let list = cols_available.iter().map(|n| format!("{n}.id")).collect::<Vec<_>>().join(", ");
            let _ = select;
            let above = select_sorted(&c, &format!("select {list} from {} where {qualified}", render(None)));
            let pushed = select_sorted(&c, &format!("select {list} from {}", render(Some(t))));
            let ctx = format!("{from}  [{} on {}, {joins:?}]", qualified, ["a", "b", "c"][t]);
            if supplied[pos] {
                assert_ne!(above, pushed, "NULL-extended table: pushing must change the result — {ctx}");
                differing += 1;
            } else {
                assert_eq!(above, pushed, "pushable table: pushing must not change the result — {ctx}");
            }
        }
    }
    assert!(differing >= 6, "the unsafe cases were actually exercised ({differing})");
}


// DISTINCT and GROUP BY both sort via SortSource::with_fields, the same
// unlimited path ORDER BY/the merge join use (see plan::logical's two call
// sites) — this confirms they actually take the in-memory fast path (no
// Run, no temp page) for data that fits the query's memory budget, not just
// that the SQL happens to still return the right rows.
#[test]
fn test_distinct_and_group_by_use_the_in_memory_sort_fast_path_when_they_fit() {
    let c = conn();
    run(&c, "create table t (id integer not null, cat integer, primary key(id))").unwrap();
    for i in 0..200 {
        run(&c, &format!("insert into t values ({i}, {})", i % 5)).unwrap();
    }
    let db = c.database.read().db.clone();

    let before = db.stats().temp;
    run(&c, "select distinct cat from t").unwrap();
    assert_eq!(db.stats().temp, before, "DISTINCT must not touch a Run page here");

    run(&c, "select cat, count(*) from t group by cat").unwrap();
    assert_eq!(db.stats().temp, before, "GROUP BY must not touch a Run page here");
}

// ---- new SQL functions: min, max, sum, lower, concat ----

fn new_funcs_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(&c, "create table t (id integer not null, cat integer, name varchar(20), primary key(id))").unwrap();
    for (id, cat, name) in [(1, 0, "Bob"), (2, 1, "alice"), (3, 0, "Cy"), (4, 1, "Dee")] {
        run(&c, &format!("insert into t values ({id}, {cat}, '{name}')")).unwrap();
    }
    c
}

#[test]
fn test_min_max_sum_as_bare_aggregates() {
    let c = new_funcs_conn();
    assert_eq!(select_sorted(&c, "select min(id) from t"), ints(&[&[1]]));
    assert_eq!(select_sorted(&c, "select max(id) from t"), ints(&[&[4]]));
    assert_eq!(select_sorted(&c, "select sum(id) from t"), ints(&[&[10]]));
}

#[test]
fn test_min_max_sum_grouped_by_category() {
    let c = new_funcs_conn();
    assert_eq!(
        select_sorted(&c, "select cat, min(id), max(id), sum(id) from t group by cat"),
        ints(&[&[0, 1, 3, 4], &[1, 2, 4, 6]])
    );
}

#[test]
fn test_min_max_sum_over_an_empty_table_report_null() {
    let c = conn();
    run(&c, "create table e (id integer not null, primary key(id))").unwrap();
    let (_, rows) = select_rows(&c, "select min(id), max(id), sum(id) from e");
    assert_eq!(rows, vec![vec![ValueItem::Null, ValueItem::Null, ValueItem::Null]]);
}

#[test]
fn test_lower_and_upper_as_scalar_functions() {
    let c = new_funcs_conn();
    let mut rows = select_rows(&c, "select lower(name), upper(name) from t").1;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Str(("alice".into(), 20)), ValueItem::Str(("ALICE".into(), 20))],
            vec![ValueItem::Str(("bob".into(), 20)), ValueItem::Str(("BOB".into(), 20))],
            vec![ValueItem::Str(("cy".into(), 20)), ValueItem::Str(("CY".into(), 20))],
            vec![ValueItem::Str(("dee".into(), 20)), ValueItem::Str(("DEE".into(), 20))],
        ]
    );
}

#[test]
fn test_concat_as_a_scalar_function() {
    let c = new_funcs_conn();
    let mut rows = select_rows(&c, "select concat(name, '-', id) from t").1;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Str(("Bob-1".into(), 6))],
            vec![ValueItem::Str(("Cy-3".into(), 5))],
            vec![ValueItem::Str(("Dee-4".into(), 6))],
            vec![ValueItem::Str(("alice-2".into(), 8))],
        ]
    );
}

#[test]
fn test_min_max_work_on_strings_through_sql() {
    let c = new_funcs_conn();
    let (_, rows) = select_rows(&c, "select min(name), max(name) from t");
    assert_eq!(
        rows,
        vec![vec![ValueItem::Str(("Bob".into(), 20)), ValueItem::Str(("alice".into(), 20))]]
    );
}

#[test]
fn test_explain_shows_min_max_sum_and_scalar_functions() {
    let c = new_funcs_conn();
    let plan = explain(&c, "select cat, min(id), sum(id) from t group by cat");
    assert!(plan.contains("min(id)") && plan.contains("sum(id)"), "{plan}");
    // A variadic argument list, literal included (describe() renders each
    // argument via its own describe(), not just the columns it reads —
    // see EvalExpr::describe's own comment).
    let plan = explain(&c, "select concat(name, '-', id) from t");
    assert!(plan.contains("concat(name, '-', id)"), "{plan}");
    let plan = explain(&c, "select upper(name), lower(name) from t");
    assert!(plan.contains("upper(name)") && plan.contains("lower(name)"), "{plan}");
    // A call nested inside another function's argument list renders as
    // itself, not the column it ultimately reads (this used to collapse
    // to `concat(name, name)` — see FuncTrait::args' own doc comment on
    // the fix).
    let plan = explain(&c, "select concat(name, upper(name)) from t");
    assert!(plan.contains("concat(name, upper(name))"), "{plan}");
}
