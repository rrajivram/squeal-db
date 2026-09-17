// Consolidated validation for every `Source` implementor:
//
//  - `direct`: constructs TableSource/RunSource by hand and drives
//    their Source trait impl directly (next/fields/reset). Every OTHER
//    Source type already has thorough per-file `#[cfg(test)] mod
//    tests` coverage in its own source/*.rs file (hash.rs, group.rs,
//    aggr.rs, join.rs, limit.rs, where_source.rs, proj.rs, sort.rs) —
//    TableSource and RunSource had none at all before this.
//  - `via_logical_plan`: the same Source types, plus everything above,
//    as actually assembled by the real query planner (plan/logical.rs)
//    and driven end-to-end through Connection/Statement — one test per
//    Source type/SQL shape, organized as a checklist by *physical
//    operator* rather than by SQL feature (stmt/tests/mod.rs already
//    covers SQL features in depth; this is the complementary "does
//    every Source actually get exercised" view).
//
// Deliberately NOT covered: nested SELECTs (a subquery in FROM hits
// `TableRef::Derived => todo!()` in logical.rs — unimplemented, would
// panic) and HAVING (`select.having` is never read anywhere in the
// real handle_select path — silently has no effect, not real support).
// Neither is supported by the engine yet.

use std::sync::Arc;

use store::{memfile::MemFile, valueitem::ValueItem};

use crate::{
    conn::connection::{ConMgr, Connection, ConnectionManager},
    constant::DEFAULT_SCHEMA_NAME,
    error::SchemaError,
    rslt::resultset::ResultType,
    source::{ProjectableField, Source, run::RunSource, table::TableSource},
};

fn conn() -> Arc<Connection<MemFile>> {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("source_tests_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn run(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<(), SchemaError> {
    c.clone().create_statement(sql)?.execute()
}

fn drain(source: &mut dyn Source) -> Vec<Vec<ValueItem>> {
    let mut out = vec![];
    while let Some(row) = source.next().unwrap() {
        out.push(row.values().to_vec());
    }
    out
}

// The "via logical plan" equivalent of `drain` above: runs `sql` (a
// single SELECT) end to end through the real Connection/Statement/
// StreamingResultSet path, rather than holding a Source directly.
fn select_rows(c: &Arc<Connection<MemFile>>, sql: &str) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let result = stmt
        .get_results()
        .unwrap()
        .expect("SELECT must produce a result");
    let ResultType::StreamingResult(mut stream) = result else {
        panic!("expected a StreamingResult");
    };
    let columns = stream.columns();
    let mut rows = vec![];
    while let Some(row) = stream.next_result().unwrap() {
        rows.push(row.values().to_vec());
    }
    (columns, rows)
}

mod direct {
    use super::*;

    #[test]
    fn table_source_scans_every_row_and_reset_rescans_from_the_start() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, name varchar(10), primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 'a')").unwrap();
        run(&c, "insert into t values (2, 'b')").unwrap();

        let table = c.current_schema().unwrap().get_table("t").unwrap();
        let db = c.database.read().db.clone();
        let mut source = TableSource::new(db, table, None).unwrap();

        let names: Vec<String> = source
            .fields()
            .iter()
            .map(|f| f.display_name.clone())
            .collect();
        assert_eq!(names, vec!["id".to_string(), "name".to_string()]);

        let mut rows = drain(&mut source);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Integer(1), ValueItem::Str(("a".into(), 10))],
                vec![ValueItem::Integer(2), ValueItem::Str(("b".into(), 10))],
            ]
        );

        source.reset().unwrap();
        let mut rows_again = drain(&mut source);
        rows_again.sort();
        assert_eq!(rows_again, rows, "reset must allow a full rescan");
    }

    #[test]
    fn table_source_on_an_empty_table_yields_no_rows() {
        let c = conn();
        run(&c, "create table t (id integer not null, primary key(id))").unwrap();
        let table = c.current_schema().unwrap().get_table("t").unwrap();
        let db = c.database.read().db.clone();
        let mut source = TableSource::new(db, table, None).unwrap();
        assert_eq!(drain(&mut source), Vec::<Vec<ValueItem>>::new());
    }

    // RunSource backs temp tables (see plan/logical.rs's OpenSource impl
    // for Arc<RwLock<TempTable<F>>>) — a real one is built via SQL, same
    // as TableSource's setup above, then driven directly.
    #[test]
    fn run_source_scans_every_row_and_reset_rescans_from_the_start() {
        let c = conn();
        run(&c, "create table temp.t (id integer not null, val integer)").unwrap();
        run(&c, "insert into temp.t values (1, 10)").unwrap();
        run(&c, "insert into temp.t values (2, 20)").unwrap();

        let temp_table = c.temp_tables().get("t").unwrap();
        let guard = temp_table.read();
        let cursor = guard.cursor().unwrap();
        let fields: Vec<ProjectableField> = guard
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect();
        drop(guard);
        let mut source = RunSource::new(cursor, &fields);

        let mut rows = drain(&mut source);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(10)],
                vec![ValueItem::Integer(2), ValueItem::Integer(20)],
            ]
        );

        source.reset().unwrap();
        let mut rows_again = drain(&mut source);
        rows_again.sort();
        assert_eq!(rows_again, rows, "reset must allow a full rescan");
    }

    #[test]
    fn run_source_on_an_empty_temp_table_yields_no_rows() {
        let c = conn();
        run(&c, "create table temp.t (id integer not null)").unwrap();
        let temp_table = c.temp_tables().get("t").unwrap();
        let guard = temp_table.read();
        let cursor = guard.cursor().unwrap();
        let fields: Vec<ProjectableField> = guard
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect();
        drop(guard);
        let mut source = RunSource::new(cursor, &fields);
        assert_eq!(drain(&mut source), Vec::<Vec<ValueItem>>::new());
    }
}

mod via_logical_plan {
    use super::*;

    #[test]
    fn table_source_plain_scan_returns_every_row() {
        let c = conn();
        run(&c, "create table t (id integer not null, primary key(id))").unwrap();
        run(&c, "insert into t values (1)").unwrap();
        run(&c, "insert into t values (2)").unwrap();
        let (_, mut rows) = select_rows(&c, "select * from t");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
        );
    }

    #[test]
    fn run_source_temp_table_scan_returns_every_row() {
        let c = conn();
        run(&c, "create table temp.t (id integer not null)").unwrap();
        run(&c, "insert into temp.t values (1)").unwrap();
        run(&c, "insert into temp.t values (2)").unwrap();
        let (_, mut rows) = select_rows(&c, "select * from temp.t");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
        );
    }

    #[test]
    fn where_source_filters_to_matching_rows() {
        let c = conn();
        run(&c, "create table t (id integer not null, primary key(id))").unwrap();
        run(&c, "insert into t values (1)").unwrap();
        run(&c, "insert into t values (2)").unwrap();
        run(&c, "insert into t values (3)").unwrap();
        let (_, mut rows) = select_rows(&c, "select id from t where id > 1");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(2)], vec![ValueItem::Integer(3)]]
        );
    }

    // Limit (the standalone post-scan cap) only backs LIMIT when there's
    // no ORDER BY alongside it — see post_visit_query in logical.rs: an
    // ORDER BY + LIMIT combination instead routes the limit into
    // SortSource's own top-K path (covered separately below).
    #[test]
    fn limit_without_order_by_caps_row_count() {
        let c = conn();
        run(&c, "create table t (id integer not null, primary key(id))").unwrap();
        run(&c, "insert into t values (1)").unwrap();
        run(&c, "insert into t values (2)").unwrap();
        run(&c, "insert into t values (3)").unwrap();
        let (_, rows) = select_rows(&c, "select id from t limit 2");
        assert_eq!(rows.len(), 2, "{rows:?}");
    }

    #[test]
    fn projection_evaluates_a_computed_expression() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, val integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 10)").unwrap();
        run(&c, "insert into t values (2, 20)").unwrap();
        let (columns, mut rows) = select_rows(&c, "select val + 1 from t");
        assert_eq!(columns.len(), 1);
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(11)], vec![ValueItem::Integer(21)]]
        );
    }

    fn setup_two_tables(c: &Arc<Connection<MemFile>>) {
        run(
            &c,
            "create table t1 (id integer not null, name varchar(10), primary key(id))",
        )
        .unwrap();
        run(
            &c,
            "create table t2 (id integer not null, val integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t1 values (1, 'alice')").unwrap();
        run(&c, "insert into t1 values (2, 'bob')").unwrap();
        run(&c, "insert into t2 values (2, 200)").unwrap();
        run(&c, "insert into t2 values (3, 300)").unwrap();
    }

    #[test]
    fn inner_join_returns_only_matched_pairs() {
        let c = conn();
        setup_two_tables(&c);
        let (_, rows) = select_rows(&c, "select * from t1 join t2 on t1.id = t2.id");
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Integer(2),
                ValueItem::Str(("bob".into(), 10)),
                ValueItem::Integer(2),
                ValueItem::Integer(200),
            ]]
        );
    }

    #[test]
    fn left_join_includes_unmatched_left_rows_with_right_nulls() {
        let c = conn();
        setup_two_tables(&c);
        let (_, mut rows) = select_rows(&c, "select * from t1 left join t2 on t1.id = t2.id");
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![
                    ValueItem::Integer(1),
                    ValueItem::Str(("alice".into(), 10)),
                    ValueItem::Null,
                    ValueItem::Null,
                ],
                vec![
                    ValueItem::Integer(2),
                    ValueItem::Str(("bob".into(), 10)),
                    ValueItem::Integer(2),
                    ValueItem::Integer(200),
                ],
            ]
        );
    }

    #[test]
    fn right_join_includes_unmatched_right_rows_with_left_nulls() {
        let c = conn();
        setup_two_tables(&c);
        let (_, mut rows) = select_rows(&c, "select * from t1 right join t2 on t1.id = t2.id");
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![
                    ValueItem::Null,
                    ValueItem::Null,
                    ValueItem::Integer(3),
                    ValueItem::Integer(300),
                ],
                vec![
                    ValueItem::Integer(2),
                    ValueItem::Str(("bob".into(), 10)),
                    ValueItem::Integer(2),
                    ValueItem::Integer(200),
                ],
            ]
        );
    }

    #[test]
    fn full_join_includes_unmatched_rows_from_both_sides() {
        let c = conn();
        setup_two_tables(&c);
        let (_, rows) = select_rows(&c, "select * from t1 full join t2 on t1.id = t2.id");
        assert_eq!(
            rows.len(),
            3,
            "alice (left-only) + bob/2 (matched) + 3/300 (right-only): {rows:?}"
        );
    }

    // UnionJoin only actually cross-products with 2+ top-level FROM
    // items (see logical.rs's skip-when-single-source optimization) —
    // a comma-joined FROM list is the one SQL shape that exercises that
    // path at all.
    #[test]
    fn cross_join_produces_the_full_cross_product() {
        let c = conn();
        run(&c, "create table t1 (id integer not null, primary key(id))").unwrap();
        run(&c, "create table t2 (id integer not null, primary key(id))").unwrap();
        run(&c, "insert into t1 values (1)").unwrap();
        run(&c, "insert into t1 values (2)").unwrap();
        run(&c, "insert into t2 values (10)").unwrap();
        run(&c, "insert into t2 values (20)").unwrap();
        let (_, mut rows) = select_rows(&c, "select * from t1, t2");
        rows.sort();
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

    // A single top-level FROM item with several chained JOIN...ON
    // clauses folds into one left-deep JoinSource chain (no UnionJoin
    // involved at all — see logical.rs) — this is the shape a query
    // like `orders JOIN customers JOIN order_details ...` actually
    // takes.
    #[test]
    fn multi_way_join_chains_three_tables_correctly() {
        let c = conn();
        run(&c, "create table t1 (id integer not null, primary key(id))").unwrap();
        run(
            &c,
            "create table t2 (id integer not null, val integer, primary key(id))",
        )
        .unwrap();
        run(
            &c,
            "create table t3 (id integer not null, extra integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t1 values (1)").unwrap();
        run(&c, "insert into t2 values (1, 100)").unwrap();
        run(&c, "insert into t3 values (1, 999)").unwrap();
        let (_, rows) = select_rows(
            &c,
            "select t1.id, t2.val, t3.extra from t1 \
             join t2 on t1.id = t2.id \
             join t3 on t3.id = t1.id",
        );
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Integer(1),
                ValueItem::Integer(100),
                ValueItem::Integer(999),
            ]]
        );
    }

    #[test]
    fn group_source_collapses_rows_and_counts_each_group() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, category varchar(10), primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 'a')").unwrap();
        run(&c, "insert into t values (2, 'a')").unwrap();
        run(&c, "insert into t values (3, 'b')").unwrap();
        let (_, mut rows) = select_rows(&c, "select category, count(*) from t group by category");
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Str(("a".into(), 10)), ValueItem::Integer(2)],
                vec![ValueItem::Str(("b".into(), 10)), ValueItem::Integer(1)],
            ]
        );
    }

    #[test]
    fn aggregating_source_distinct_removes_duplicate_rows() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, name varchar(10), primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 'alice')").unwrap();
        run(&c, "insert into t values (2, 'bob')").unwrap();
        run(&c, "insert into t values (3, 'alice')").unwrap();
        let (_, mut rows) = select_rows(&c, "select distinct name from t");
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Str(("alice".into(), 10))],
                vec![ValueItem::Str(("bob".into(), 10))],
            ]
        );
    }

    #[test]
    fn sort_source_unlimited_orders_every_row() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, rank integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 30)").unwrap();
        run(&c, "insert into t values (2, 10)").unwrap();
        run(&c, "insert into t values (3, 20)").unwrap();
        let (_, rows) = select_rows(&c, "select rank from t order by rank");
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Integer(10)],
                vec![ValueItem::Integer(20)],
                vec![ValueItem::Integer(30)],
            ]
        );
    }

    // ORDER BY + LIMIT together route the limit into SortSource's own
    // in-memory top-K path (CrateHeap), not the standalone Limit
    // wrapper tested above.
    #[test]
    fn sort_source_limited_orders_and_caps_to_top_n() {
        let c = conn();
        run(
            &c,
            "create table t (id integer not null, rank integer, primary key(id))",
        )
        .unwrap();
        run(&c, "insert into t values (1, 30)").unwrap();
        run(&c, "insert into t values (2, 10)").unwrap();
        run(&c, "insert into t values (3, 20)").unwrap();
        let (_, rows) = select_rows(&c, "select rank from t order by rank limit 2");
        assert_eq!(
            rows,
            vec![vec![ValueItem::Integer(10)], vec![ValueItem::Integer(20)]]
        );
    }
}
