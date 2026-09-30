//! ORDER BY read straight off a key (see optim::picker::OrderWanted): when
//! the table's primary key or an index gives the wanted order, there is no
//! sort, and a LIMIT stops reading early.
//!
//! Checked against a reference: the same rows fetched in no particular
//! order, sorted here by SQL's rules (NULLs last unless NULLS FIRST).

use std::cmp::Ordering;

use super::*;
use store::valueitem::ValueItem;

// id: primary key. name: unique, one NULL. (city, n): n has NULLs.
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(20), city varchar(20), \
         n integer, note varchar(200), primary key(id))",
    )
    .unwrap();
    run(&c, "create unique index t_name on t (name)").unwrap();
    run(&c, "create index t_city_n on t (city, n)").unwrap();
    let rows = (0..400)
        .map(|i| {
            let n = if i % 13 == 0 {
                "null".to_string()
            } else {
                (i % 40).to_string()
            };
            format!(
                "({i}, 'name{i:04}', 'city{}', {n}, 'a long note for row {i}')",
                i % 5
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into t values {rows}")).unwrap();
    run(
        &c,
        "insert into t values (1000, null, 'city3', 7, 'no name')",
    )
    .unwrap();
    run(&c, "analyze table t").unwrap();
    c
}

// SQL's order for one column: NULLs last unless `nulls_first`.
fn sql_cmp(a: &ValueItem, b: &ValueItem, nulls_first: bool) -> Ordering {
    match (a, b) {
        (ValueItem::Null, ValueItem::Null) => Ordering::Equal,
        (ValueItem::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, ValueItem::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => a.cmp(b),
    }
}

// `select {cols} from t {where} order by {key}` must match the reference,
// with or without a LIMIT; with one, it must also plan without a sort (it
// reads 7 rows in key order). Without a LIMIT, reading everything in key
// order can cost more than scanning and sorting, so either plan is fine.
// `key` names one column of `cols`, at `key_pos`. Ties may come in any
// order, so for a LIMIT only the key values are compared; without one,
// the whole rows.
fn check(c: &Arc<Connection<MemFile>>, cols: &str, filter: &str, key: &str, key_pos: usize) {
    let nulls_first = key.contains("nulls first");
    for limit in ["", " limit 7"] {
        let sql = format!("select {cols} from t {filter} order by {key}{limit}");
        let plan = explain(c, &sql);
        if !limit.is_empty() {
            assert!(
                !plan.contains("Sort") && !plan.contains("TopN"),
                "{sql}\n{plan}"
            );
        }
        let got = select_rows(c, &sql).1;
        let mut reference = select_rows(c, &format!("select {cols} from t {filter}")).1;
        reference.sort_by(|a, b| sql_cmp(&a[key_pos], &b[key_pos], nulls_first));
        if limit.is_empty() {
            let mut got_sorted = got.clone();
            let mut ref_sorted = reference.clone();
            got_sorted.sort();
            ref_sorted.sort();
            assert_eq!(got_sorted, ref_sorted, "{sql}: the same rows");
            reference.truncate(got.len());
        } else {
            reference.truncate(7);
        }
        let keys =
            |rows: &[Vec<ValueItem>]| rows.iter().map(|r| r[key_pos].clone()).collect::<Vec<_>>();
        assert_eq!(keys(&got), keys(&reference), "{sql}: in order");
    }
}

#[test]
fn test_the_primary_key_gives_its_order() {
    let c = setup();
    check(&c, "id, name", "", "id", 0);
    check(&c, "*", "", "id", 0);
    check(&c, "id, note", "where id > 100", "id", 0);
    let plan = explain(&c, "select * from t order by id limit 3");
    assert!(plan.contains("TableScan t (in id order)"), "{plan}");
}

#[test]
fn test_an_index_gives_its_order_with_nulls_last() {
    let c = setup();
    check(&c, "id, name", "", "name", 1);
    check(&c, "id, name, note", "", "name", 1);
    check(&c, "id, name", "", "name nulls first", 1);
    let plan = explain(&c, "select id, name from t order by name limit 3");
    assert!(plan.contains("(in index order, NULLs last)"), "{plan}");
    // The NULL name comes last, and first with NULLS FIRST.
    let all = select_rows(&c, "select name from t order by name").1;
    assert_eq!(all.last().unwrap()[0], ValueItem::Null);
    let first = select_rows(&c, "select name from t order by name nulls first limit 1").1;
    assert_eq!(first[0][0], ValueItem::Null);
}

#[test]
fn test_an_equality_on_leading_columns_leaves_the_next_in_order() {
    let c = setup();
    check(&c, "id, n", "where city = 'city3'", "n", 1);
    check(&c, "id, n, note", "where city = 'city3'", "n", 1);
    check(&c, "id, n", "where city = 'city3' and n > 30", "n", 1);
    let plan = explain(
        &c,
        "select id, n from t where city = 'city3' order by n limit 3",
    );
    assert!(plan.contains("(city = 'city3', NULLs last)"), "{plan}");
}

#[test]
fn test_orders_no_key_gives_still_sort() {
    let c = setup();
    for sql in [
        "select id from t order by id desc",
        "select id, n from t order by n",
        // IN gives several city values, so n is not in order overall.
        "select id, n from t where city in ('city1', 'city2') order by n",
        "select id, name from t order by name, id",
        "select city, count(*) from t group by city order by city",
    ] {
        let plan = explain(&c, sql);
        assert!(
            plan.contains("Sort") || plan.contains("TopN"),
            "{sql}\n{plan}"
        );
    }
}
