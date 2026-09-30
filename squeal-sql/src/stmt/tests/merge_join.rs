//! Merge joins over inputs already in join-key order (see QueryVisitor::
//! merge_plan, SortJoinSource::presorted): when both tables can be read in
//! the key's order as cheaply as any other way, they are merged in one pass
//! with no sort and no hash table.
//!
//! Checked against a reference join computed here, with SQL's rules: a NULL
//! key matches nothing; outer joins keep their side's unmatched rows.

use super::*;
use store::valueitem::ValueItem;

// a and b: keys k (integer), s (string), x (double), each indexed, with
// duplicates on both sides (so key groups meet many-to-many) and NULLs.
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    for (t, n, m) in [("a", 300, 7), ("b", 250, 11)] {
        run(
            &c,
            &format!(
                "create table {t} (id integer not null, k integer, s varchar(10), x double, \
                 primary key(id))"
            ),
        )
        .unwrap();
        for col in ["k", "s", "x"] {
            run(&c, &format!("create index {t}_{col} on {t} ({col})")).unwrap();
        }
        let rows = (0..n)
            .map(|i| {
                let (k, s) = if i % m == 0 {
                    ("null".to_string(), "null".to_string())
                } else {
                    ((i % 40).to_string(), format!("'s{:02}'", i % 30))
                };
                format!("({i}, {k}, {s}, {}.5)", i % 20)
            })
            .collect::<Vec<_>>()
            .join(", ");
        run(&c, &format!("insert into {t} values {rows}")).unwrap();
        run(&c, &format!("analyze table {t}")).unwrap();
    }
    c
}

type Row = Vec<ValueItem>;

// `a {kind} JOIN b ON a.{col} = b.{col}`, as (a.id, b.id) pairs.
fn reference(c: &Arc<Connection<MemFile>>, kind: &str, col: &str) -> Vec<Row> {
    let a = select_rows(c, &format!("select id, {col} from a")).1;
    let b = select_rows(c, &format!("select id, {col} from b")).1;
    let matches = |l: &Row, r: &Row| l[1] != ValueItem::Null && l[1] == r[1];
    let mut out = vec![];
    let mut b_hit = vec![false; b.len()];
    for l in &a {
        let mut hit = false;
        for (i, r) in b.iter().enumerate() {
            if matches(l, r) {
                hit = true;
                b_hit[i] = true;
                out.push(vec![l[0].clone(), r[0].clone()]);
            }
        }
        if !hit && matches!(kind, "left" | "full") {
            out.push(vec![l[0].clone(), ValueItem::Null]);
        }
    }
    if matches!(kind, "right" | "full") {
        for (i, r) in b.iter().enumerate() {
            if !b_hit[i] {
                out.push(vec![ValueItem::Null, r[0].clone()]);
            }
        }
    }
    out.sort();
    out
}

fn joined(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Row> {
    let mut r = select_rows(c, sql).1;
    r.sort();
    r
}

// Merge wherever the inputs can be read in key order, for the rest of
// this test's thread (see plan::logical's FORCE_MERGE): to check the merge
// path's results whatever the costs would choose.
fn force_merge() {
    crate::plan::logical::FORCE_MERGE.with(|f| f.set(true));
}

#[test]
fn test_every_join_type_merges_correctly() {
    let c = setup();
    force_merge();
    for col in ["k", "s"] {
        for kind in ["inner", "left", "right", "full"] {
            let sql = format!("select a.id, b.id from a {kind} join b on a.{col} = b.{col}");
            let plan = explain(&c, &sql);
            assert!(
                plan.contains("SortMergeJoin") && plan.contains("inputs in key order"),
                "{sql}\n{plan}"
            );
            assert!(
                !plan.contains("Sort ") && !plan.contains("TopN"),
                "{sql}\n{plan}"
            );
            let got = joined(&c, &sql);
            assert_eq!(got, reference(&c, kind, col), "{sql}");
            assert!(!got.is_empty());
        }
    }
}

#[test]
fn test_a_double_key_is_never_merged() {
    let c = setup();
    force_merge();
    // -0.0 and 0.0 sort apart in a key but join as equal.
    let sql = "select a.id, b.id from a join b on a.x = b.x";
    let plan = explain(&c, sql);
    assert!(!plan.contains("SortMergeJoin"), "{plan}");
    assert_eq!(joined(&c, sql), reference(&c, "inner", "x"));
}

#[test]
fn test_reading_everything_in_key_order_loses_to_hashing() {
    let c = setup();
    // Every row is read either way; reading them in key order through
    // the index costs more than scanning and hashing.
    for sql in [
        "select a.id, a.x, b.x from a join b on a.k = b.k",
        "select a.id, b.id from a join b on a.k = b.k",
    ] {
        let plan = explain(&c, sql);
        assert!(plan.contains("HashJoin"), "{sql}\n{plan}");
    }
}

#[test]
fn test_a_merge_join_with_where_conditions_on_either_side() {
    let c = setup();
    force_merge();
    let sql = "select a.id, b.id from a join b on a.k = b.k where a.k > 30 and b.id < 100";
    let plan = explain(&c, sql);
    assert!(plan.contains("SortMergeJoin"), "{plan}");
    let expected: Vec<Row> = reference(&c, "inner", "k")
        .into_iter()
        .filter(|r| matches!(r[1], ValueItem::Integer(b) if b < 100))
        .filter(|r| {
            let a_k = select_rows(&c, &format!("select k from a where id = {}", r[0])).1;
            matches!(a_k[0][0], ValueItem::Integer(k) if k > 30)
        })
        .collect();
    assert_eq!(joined(&c, sql), expected);
    assert!(!expected.is_empty());
}

// Per key value, how many rows of `t` have it.
fn key_counts(c: &Arc<Connection<MemFile>>, t: &str) -> std::collections::BTreeMap<i64, i64> {
    let mut m = std::collections::BTreeMap::new();
    for r in select_rows(c, &format!("select k from {t}")).1 {
        if let ValueItem::Integer(k) = r[0] {
            *m.entry(k).or_insert(0) += 1;
        }
    }
    m
}

// The merge's output comes in key order: a GROUP BY on the join key needs
// no sort of its own.
#[test]
fn test_grouping_by_the_join_key_needs_no_sort() {
    let c = setup();
    force_merge();
    let sql = "select a.k, count(*) from a join b on a.k = b.k group by a.k";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("SortMergeJoin") && !plan.contains("Sort "),
        "{plan}"
    );
    let (a, b) = (key_counts(&c, "a"), key_counts(&c, "b"));
    let expected: Vec<Row> = a
        .iter()
        .filter_map(|(k, n)| {
            b.get(k)
                .map(|m| vec![ValueItem::Integer(*k), ValueItem::Integer(n * m)])
        })
        .collect();
    // Grouped in key order already, so compare as returned.
    assert_eq!(select_rows(&c, sql).1, expected);
    let plan = explain(
        &c,
        "select b.k, count(*) from a join b on a.k = b.k group by b.k",
    );
    assert!(!plan.contains("Sort "), "{plan}");
    // Not the join key: sorted as usual.
    let plan = explain(
        &c,
        "select a.id, count(*) from a join b on a.k = b.k group by a.id",
    );
    assert!(plan.contains("Sort "), "{plan}");
}

// ORDER BY the join key with a LIMIT: merging stops after LIMIT rows, which
// clearly beats hashing everything — chosen on cost, not forced.
#[test]
fn test_order_by_the_join_key_with_a_limit_merges_and_stops_early() {
    let c = setup();
    let sql = "select a.k, a.id, b.id from a join b on a.k = b.k order by a.k limit 5";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("SortMergeJoin") && !plan.contains("TopN"),
        "{plan}"
    );
    let got = select_rows(&c, sql).1;
    assert_eq!(got.len(), 5);
    let (a, b) = (key_counts(&c, "a"), key_counts(&c, "b"));
    let first = *a.keys().find(|k| b.contains_key(k)).unwrap();
    assert!(
        got.iter().all(|r| r[0] == ValueItem::Integer(first)),
        "{got:?}"
    );
}

// A third table, joined on the same key: chains of merges.
fn setup_chain() -> Arc<Connection<MemFile>> {
    let c = setup();
    run(
        &c,
        "create table t3 (id integer not null, k integer, primary key(id))",
    )
    .unwrap();
    run(&c, "create index t3_k on t3 (k)").unwrap();
    let rows = (0..200)
        .map(|i| {
            let k = if i % 13 == 0 {
                "null".to_string()
            } else {
                (i % 50).to_string()
            };
            format!("({i}, {k})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into t3 values {rows}")).unwrap();
    run(&c, "analyze table t3").unwrap();
    c
}

// (a.id, b.id, t3.id) of `a JOIN b ON a.k = b.k {kind} JOIN t3 ON b.k = t3.k`.
fn chain_reference(c: &Arc<Connection<MemFile>>, kind: &str) -> Vec<Row> {
    let ids = |t: &str| select_rows(c, &format!("select id, k from {t}")).1;
    let (a, b, t3) = (ids("a"), ids("b"), ids("t3"));
    let mut out = vec![];
    for x in &a {
        for y in &b {
            if x[1] == ValueItem::Null || x[1] != y[1] {
                continue;
            }
            let matches: Vec<_> = t3.iter().filter(|z| z[1] == y[1]).collect();
            if matches.is_empty() && kind == "left" {
                out.push(vec![x[0].clone(), y[0].clone(), ValueItem::Null]);
            }
            for z in matches {
                out.push(vec![x[0].clone(), y[0].clone(), z[0].clone()]);
            }
        }
    }
    out.sort();
    out
}

#[test]
fn test_a_chain_of_joins_on_one_key_merges_throughout() {
    let c = setup_chain();
    force_merge();
    for kind in ["inner", "left"] {
        let sql = format!(
            "select a.id, b.id, t3.id from a join b on a.k = b.k {kind} join t3 on b.k = t3.k"
        );
        let plan = explain(&c, &sql);
        assert_eq!(plan.matches("SortMergeJoin").count(), 2, "{sql}\n{plan}");
        assert_eq!(joined(&c, &sql), chain_reference(&c, kind), "{sql}");
    }
    // The order lasts to the end of the chain: GROUP BY the key, no sort.
    let sql = "select a.k, count(*) from a join b on a.k = b.k join t3 on b.k = t3.k group by a.k";
    let plan = explain(&c, sql);
    assert!(!plan.contains("Sort "), "{plan}");
    let mut expected: std::collections::BTreeMap<i64, i64> = Default::default();
    for r in chain_reference(&c, "inner") {
        let k = select_rows(&c, &format!("select k from a where id = {}", r[0])).1;
        if let ValueItem::Integer(k) = k[0][0] {
            *expected.entry(k).or_insert(0) += 1;
        }
    }
    let expected: Vec<Row> = expected
        .into_iter()
        .map(|(k, n)| vec![ValueItem::Integer(k), ValueItem::Integer(n)])
        .collect();
    assert_eq!(select_rows(&c, sql).1, expected);
}

#[test]
fn test_a_chain_with_order_by_the_key_and_a_limit_merges_on_cost() {
    let c = setup_chain();
    let sql = "select a.k, a.id, b.id, t3.id from a join b on a.k = b.k \
               join t3 on b.k = t3.k order by a.k limit 5";
    let plan = explain(&c, sql);
    assert_eq!(plan.matches("SortMergeJoin").count(), 2, "{plan}");
    assert!(!plan.contains("TopN") && !plan.contains("Sort "), "{plan}");
    let got = select_rows(&c, sql).1;
    assert_eq!(got.len(), 5);
    assert!(got.windows(2).all(|w| w[0][0] <= w[1][0]), "{got:?}");
}
