//! Seeks (see optim::picker::AccessPath, plan::sarg): WHERE conditions on
//! a key read just the matching range — of the table's primary key
//! (TableSeek), of a covering index (IndexSeek), or of an index whose
//! entries then fetch their rows (IndexLookup).
//!
//! The main check is an oracle: each condition is also run in a form no
//! seek can use (`id + 0 > 5`, `name || '' = 'x'`), which scans; both must
//! return the same rows.

use super::*;
use store::valueitem::ValueItem;

// id: primary key. name: unique. (city, n): composite, not unique, with
// NULL cities. note: wide and in no index. x: double, including both zeros.
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(20), city varchar(20), \
         n integer, x double, note varchar(200), primary key(id))",
    )
    .unwrap();
    run(&c, "create unique index t_name on t (name)").unwrap();
    run(&c, "create index t_city_n on t (city, n)").unwrap();
    run(&c, "create index t_x on t (x)").unwrap();
    let rows = (0..300)
        .map(|i| {
            let city = if i % 11 == 0 {
                "null".to_string()
            } else {
                format!("'city{}'", i % 7)
            };
            format!(
                "({i}, 'name{i:04}', {city}, {}, {}.5, 'a long note for row {i}')",
                i % 50,
                i % 10
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into t values {rows}")).unwrap();
    run(
        &c,
        "insert into t select 1000, 'zero', 'city1', 1, 0.0, 'plus zero'",
    )
    .unwrap();
    run(
        &c,
        "insert into t select 1001, 'negzero', 'city1', 1, 0.0 * -1.0, 'minus zero'",
    )
    .unwrap();
    run(&c, "analyze table t").unwrap();
    c
}

fn sorted(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    let mut r = select_rows(c, sql).1;
    r.sort();
    r
}

// `sql` with {} for the condition: the seek form must be planned as a
// seek, and return what the scan form returns.
fn agree(c: &Arc<Connection<MemFile>>, sql: &str, seek: &str, scan: &str) {
    let with_seek = sql.replace("{}", seek);
    let plan = explain(c, &with_seek);
    assert!(
        plan.contains("Seek") || plan.contains("IndexLookup"),
        "{with_seek}\n{plan}"
    );
    let with_scan = sql.replace("{}", scan);
    assert!(!explain(c, &with_scan).contains("Seek"), "{with_scan}");
    assert_eq!(sorted(c, &with_seek), sorted(c, &with_scan), "{with_seek}");
}

#[test]
fn test_primary_key_conditions_seek_the_table() {
    let c = setup();
    let sql = "select * from t where {}";
    for (seek, scan) in [
        ("id = 42", "id + 0 = 42"),
        ("id >= 10 and id < 13", "id + 0 >= 10 and id + 0 < 13"),
        ("id > 297", "id + 0 > 297"),
        ("42 = id", "42 = id + 0"),
        ("id <= 3", "id + 0 <= 3"),
        ("id > 5 and id < 3", "id + 0 > 5 and id + 0 < 3"),
        // Integer column, double constants: exact, like the evaluator.
        ("id = 5.0", "id + 0 = 5.0"),
        ("id = 5.5", "id + 0 = 5.5"),
        ("id > 296.5", "id + 0 > 296.5"),
        ("id < 2.5", "id + 0 < 2.5"),
        ("id <= -0.5", "id + 0 <= -0.5"),
        (
            "id >= 99999999999999999999.0",
            "id + 0 >= 99999999999999999999.0",
        ),
        ("id = null", "id + 0 = null"),
    ] {
        agree(&c, sql, seek, scan);
    }
    let plan = explain(&c, "select * from t where id = 42");
    assert!(plan.contains("TableSeek t (id = 42) (~1 rows)"), "{plan}");
    let plan = explain(&c, "select * from t where id >= 10 and id < 13");
    assert!(
        plan.contains("TableSeek t (id >= 10 AND id < 13)"),
        "{plan}"
    );
    let plan = explain(&c, "select * from t where id = 5.5");
    assert!(
        plan.contains("TableSeek t (matches nothing) (~0 rows)"),
        "{plan}"
    );
}

#[test]
fn test_a_covering_index_seek() {
    let c = setup();
    let plan = explain(&c, "select id, name from t where name = 'name0007'");
    assert!(
        plan.contains("IndexSeek t using t_name (name = 'name0007')"),
        "{plan}"
    );
    agree(
        &c,
        "select id, name from t where {}",
        "name = 'name0007'",
        "name || '' = 'name0007'",
    );
    agree(
        &c,
        "select name from t where {}",
        "name >= 'name0290'",
        "name || '' >= 'name0290'",
    );
}

#[test]
fn test_an_index_that_does_not_cover_looks_rows_up() {
    let c = setup();
    let sql = "select * from t where name = 'name0123'";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("IndexLookup t using t_name (name = 'name0123') (~1 rows)"),
        "{plan}"
    );
    let r = select_rows(&c, sql).1;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], ValueItem::Integer(123));
    assert_eq!(
        r[0][5],
        ValueItem::Str(("a long note for row 123".into(), 200))
    );
}

#[test]
fn test_a_composite_index_takes_equalities_then_a_range() {
    let c = setup();
    let plan = explain(&c, "select id, n from t where city = 'city3' and n > 40");
    assert!(plan.contains("(city = 'city3' AND n > 40)"), "{plan}");
    for (seek, scan) in [
        (
            "city = 'city3' and n > 40",
            "city || '' = 'city3' and n + 0 > 40",
        ),
        (
            "city = 'city3' and n = 3",
            "city || '' = 'city3' and n + 0 = 3",
        ),
        ("city = 'city3'", "city || '' = 'city3'"),
        ("city > 'city4'", "city || '' > 'city4'"),
        // NULL cities never satisfy a comparison.
        ("city < 'city2'", "city || '' < 'city2'"),
    ] {
        agree(&c, "select id, n from t where {}", seek, scan);
    }
    // Selective enough to be worth fetching rows for.
    agree(
        &c,
        "select id, note from t where {}",
        "city = 'city3' and n = 3",
        "city || '' = 'city3' and n + 0 = 3",
    );
}

#[test]
fn test_a_lookup_for_many_rows_loses_to_a_scan() {
    let c = setup();
    // About a seventh of the table: fetching each row costs more than
    // reading them all.
    let plan = explain(&c, "select note from t where city = 'city3'");
    assert!(plan.contains("TableScan t"), "{plan}");
}

#[test]
fn test_a_double_key_treats_both_zeros_as_equal() {
    let c = setup();
    for (seek, scan) in [
        ("x = 0", "x + 0 = 0"),
        ("x = 0.0", "x + 0 = 0.0"),
        ("x >= 0", "x + 0 >= 0"),
        ("x > 0", "x + 0 > 0"),
        ("x <= 0", "x + 0 <= 0"),
        ("x < 0.5", "x + 0 < 0.5"),
        ("x = 3", "x + 0 = 3"),
    ] {
        agree(&c, "select id from t where {}", seek, scan);
    }
    assert_eq!(sorted(&c, "select id from t where x = 0").len(), 2);
}

#[test]
fn test_a_type_mismatch_is_still_an_error() {
    let c = setup();
    // Not quietly "no rows": the scan runs and the comparison reports it
    // when the rows are read.
    assert!(!explain(&c, "select id from t where id = 'x'").contains("Seek"));
    let mut stmt = c
        .clone()
        .create_statement("select id from t where id = 'x'")
        .unwrap();
    stmt.execute().unwrap();
    let Some(ResultType::StreamingResult(mut rows)) = stmt.get_results().unwrap() else {
        panic!("expected rows");
    };
    let mut failed = false;
    loop {
        match rows.next_result() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed, "comparing an integer with a string must fail");
}

#[test]
fn test_seeks_see_the_statements_transaction() {
    let c = setup();
    run(&c, "begin").unwrap();
    run(
        &c,
        "insert into t values (5000, 'fresh', 'city1', 1, 1.5, 'new')",
    )
    .unwrap();
    run(&c, "delete from t where id = 7").unwrap();
    assert!(explain(&c, "select note from t where name = 'fresh'").contains("IndexLookup"));
    assert_eq!(
        select_rows(&c, "select note from t where name = 'fresh'")
            .1
            .len(),
        1
    );
    assert!(
        select_rows(&c, "select note from t where name = 'name0007'")
            .1
            .is_empty()
    );
    assert!(
        select_rows(&c, "select note from t where id = 7")
            .1
            .is_empty()
    );
    run(&c, "rollback").unwrap();
    assert!(
        select_rows(&c, "select note from t where name = 'fresh'")
            .1
            .is_empty()
    );
    assert_eq!(
        select_rows(&c, "select note from t where id = 7").1.len(),
        1
    );
}

#[test]
fn test_seeks_inside_a_join() {
    let c = setup();
    agree(
        &c,
        "select a.id, b.note from t a join t b on a.id = b.id where {}",
        "a.name = 'name0100'",
        "a.name || '' = 'name0100'",
    );
}

#[test]
fn test_no_seek_on_the_null_extended_side_of_an_outer_join() {
    let c = setup();
    // WHERE on b applies after the join (b's columns may be NULL-extended),
    // so it can't narrow what is read from b.
    let sql = "select a.id from t a left join t b on a.id = b.id where b.name = 'name0001'";
    let plan = explain(&c, sql);
    assert!(!plan.contains("(name = 'name0001')"), "{plan}");
    assert_eq!(select_rows(&c, sql).1, vec![vec![ValueItem::Integer(1)]]);
}

// A condition on one side of a join equality restricts the other side too:
// both are read by seeks.
#[test]
fn test_conditions_carry_across_join_equalities() {
    let c = setup();
    let sql = "select a.id, b.note from t a join t b on a.id = b.id where a.id = 42";
    let plan = explain(&c, sql);
    assert_eq!(plan.matches("(id = 42)").count(), 2, "{plan}");
    agree(
        &c,
        "select a.id, b.note from t a join t b on a.id = b.id where {}",
        "a.id = 42",
        "a.id + 0 = 42",
    );
    // Ranges carry too, and so do WHERE equalities of a comma join.
    let sql = "select count(*) from t a, t b where a.id = b.id and b.id >= 290 and b.id < 295";
    let plan = explain(&c, sql);
    assert_eq!(
        plan.matches("(id >= 290 AND id < 295)").count(),
        2,
        "{plan}"
    );
    agree(
        &c,
        "select a.id, b.name from t a, t b where a.id = b.id and {}",
        "b.id >= 290 and b.id < 295",
        "b.id + 0 >= 290 and b.id + 0 < 295",
    );
    // Through a chain: a = b, b = c.
    let sql = "select c.note from t a join t b on a.id = b.id join t c on b.id = c.id \
               where a.id = 7";
    let plan = explain(&c, sql);
    assert_eq!(plan.matches("(id = 7)").count(), 3, "{plan}");
    assert_eq!(select_rows(&c, sql).1.len(), 1);
}

// The NULL-extended side of an outer join keeps its rows unmatched ones
// need, so nothing is carried onto it; results stay the scan's.
#[test]
fn test_conditions_do_not_carry_onto_an_outer_joins_null_side() {
    let c = setup();
    let sql = "select a.id, b.id from t a left join t b on a.id = b.id where a.id = 42";
    let plan = explain(&c, sql);
    assert_eq!(plan.matches("(id = 42)").count(), 1, "{plan}");
    agree(
        &c,
        "select a.id, b.name from t a left join t b on a.id = b.id where {}",
        "a.id = 42",
        "a.id + 0 = 42",
    );
}

// IN (and an OR of equalities on one column) seeks each value.
#[test]
fn test_in_lists_seek_each_value() {
    let c = setup();
    for (sql, seek, scan) in [
        (
            "select * from t where {}",
            "id in (5, 17, 299, 4000)",
            "id + 0 in (5, 17, 299, 4000)",
        ),
        (
            "select * from t where {}",
            "id = 3 or id = 9",
            "id + 0 = 3 or id + 0 = 9",
        ),
        (
            "select * from t where {}",
            "id in (1, null)",
            "id + 0 in (1, null)",
        ),
        (
            "select note from t where {}",
            "name in ('name0003', 'name0100', 'nobody')",
            "name || '' in ('name0003', 'name0100', 'nobody')",
        ),
        (
            "select id, n from t where {}",
            "city in ('city1', 'city2') and n = 7",
            "city || '' in ('city1', 'city2') and n + 0 = 7",
        ),
        (
            "select id, n from t where {}",
            "city in ('city1', 'city2') and n in (7, 8)",
            "city || '' in ('city1', 'city2') and n + 0 in (7, 8)",
        ),
        (
            "select id from t where {}",
            "id in (1, 2, 3) and id > 1",
            "id + 0 in (1, 2, 3) and id + 0 > 1",
        ),
    ] {
        agree(&c, sql, seek, scan);
    }
    let plan = explain(&c, "select * from t where id in (5, 17)");
    assert!(
        plan.contains("TableSeek t (id IN (5, 17)) (~2 rows)"),
        "{plan}"
    );
    let plan = explain(
        &c,
        "select id, n from t where city in ('city1', 'city2') and n = 7",
    );
    assert!(
        plan.contains("(city IN ('city1', 'city2') AND n = 7)"),
        "{plan}"
    );
    // NOT IN excludes values: no seek.
    assert!(!explain(&c, "select id from t where id not in (1, 2)").contains("Seek"));
}

// A condition the seek reads exactly isn't checked again; the rest are.
#[test]
fn test_conditions_a_seek_reads_exactly_are_not_rechecked() {
    let c = setup();
    let plan = explain(&c, "select * from t where id = 42");
    assert!(!plan.contains("Filter"), "{plan}");
    let plan = explain(
        &c,
        "select id from t where city = 'city3' and n = 5 and id > 100",
    );
    assert!(plan.contains("Filter (id > 100)"), "{plan}");
    assert!(plan.contains("(city = 'city3' AND n = 5)"), "{plan}");
    agree(
        &c,
        "select id from t where {}",
        "city = 'city3' and n = 5 and id > 100",
        "city || '' = 'city3' and n + 0 = 5 and id + 0 > 100",
    );
    // Not on the NULL-extended side of an outer join: there the condition
    // also removes NULL-extended rows, which only WHERE can do.
    let sql = "select a.id from t a left join t b on a.id = b.id where b.name = 'name0001'";
    assert!(explain(&c, sql).contains("Filter"), "{}", explain(&c, sql));
}

// The cost model counts a row at the schema's width, whatever its table's
// tree was sized for (a row table's tree entries hold just its key — see
// SqlTable::row_entry_size): a count over a primary key range reads the
// narrow primary key index, not the rows.
#[test]
fn test_a_key_range_count_reads_the_primary_key_index() {
    let c = conn();
    run(
        &c,
        "create table o (order_id varchar(12) not null, customer_id varchar(12), \
         order_date datetime, order_time datetime, primary key(order_id))",
    )
    .unwrap();
    let rows = (0..2000)
        .map(|i| format!("('ORD{i:07}', 'C{:05}', null, null)", i % 300))
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into o values {rows}")).unwrap();
    run(&c, "analyze table o").unwrap();
    let sql = "select count(*) from o where order_id >= 'ORD0000500' and order_id < 'ORD0001200'";
    let plan = explain(&c, sql);
    assert!(plan.contains("IndexSeek o using primary key"), "{plan}");
    assert_eq!(select_rows(&c, sql).1, vec![vec![ValueItem::Integer(700)]]);
}

// Operators over literals are computed once, when the statement is planned,
// so a bound written as arithmetic still seeks (see EvalExpr::folded).
#[test]
fn test_constant_expressions_bound_a_seek() {
    let c = setup();
    let plan = explain(&c, "select * from t where id >= 5 * 2 and id < 10 + 3");
    assert!(
        plan.contains("TableSeek t (id >= 10 AND id < 13)"),
        "{plan}"
    );
    agree(&c, "select id from t where {}", "id < 10 + 3", "id + 0 < 13");
    let plan = explain(&c, "select id from t where id = -(-7)");
    assert!(plan.contains("(id = 7) (~1 rows)"), "{plan}");
    // One that fails to compute fails where it did: only for a row.
    run(&c, "create table empty (k integer not null, primary key(k))").unwrap();
    assert_eq!(
        super::partition_diff::outcome(&c, "select k from empty where k = 'a' + 1"),
        Ok(vec![])
    );
    assert!(super::partition_diff::outcome(&c, "select id from t where id = 'a' + 1").is_err());
}
