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

#[test]
fn test_every_join_type_merges_inputs_in_key_order() {
    let c = setup();
    // The integer key's index entries are smaller than the rows, so reading
    // it in order is the cheap way in: merged. The string key's entries
    // aren't, so either plan may win there; the rows must be right anyway.
    for (col, merged) in [("k", true), ("s", false)] {
        for kind in ["inner", "left", "right", "full"] {
            let sql = format!("select a.id, b.id from a {kind} join b on a.{col} = b.{col}");
            let plan = explain(&c, &sql);
            if merged {
                assert!(
                    plan.contains("SortMergeJoin") && plan.contains("inputs in key order"),
                    "{sql}\n{plan}"
                );
                assert!(
                    !plan.contains("Sort ") && !plan.contains("TopN"),
                    "{sql}\n{plan}"
                );
            }
            let got = joined(&c, &sql);
            assert_eq!(got, reference(&c, kind, col), "{sql}");
            assert!(!got.is_empty());
        }
    }
}

#[test]
fn test_a_double_key_is_never_merged() {
    let c = setup();
    // -0.0 and 0.0 sort apart in a key but join as equal.
    let sql = "select a.id, b.id from a join b on a.x = b.x";
    let plan = explain(&c, sql);
    assert!(!plan.contains("SortMergeJoin"), "{plan}");
    assert_eq!(joined(&c, sql), reference(&c, "inner", "x"));
}

#[test]
fn test_reading_whole_rows_in_key_order_loses_to_hashing() {
    let c = setup();
    // No index holds every column wanted: getting the rows in key order
    // would mean reading the tables through their keys.
    let plan = explain(&c, "select a.id, a.x, b.x from a join b on a.k = b.k");
    assert!(plan.contains("HashJoin"), "{plan}");
}

#[test]
fn test_a_merge_join_with_where_conditions_on_either_side() {
    let c = setup();
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

// The merge's output comes in key order: a GROUP BY or ORDER BY on the
// join key needs no sort of its own.
#[test]
fn test_grouping_or_ordering_by_the_join_key_needs_no_sort() {
    let c = setup();
    let sql = "select a.k, count(*) from a join b on a.k = b.k group by a.k";
    let plan = explain(&c, sql);
    assert!(plan.contains("SortMergeJoin"), "{plan}");
    assert!(!plan.contains("Sort "), "{plan}");
    // By hand: per key, (a rows with it) x (b rows with it).
    let count = |t: &str| {
        let mut m = std::collections::BTreeMap::new();
        for r in select_rows(&c, &format!("select k from {t}")).1 {
            if let ValueItem::Integer(k) = r[0] {
                *m.entry(k).or_insert(0i64) += 1;
            }
        }
        m
    };
    let (a, b) = (count("a"), count("b"));
    let expected: Vec<Row> = a
        .iter()
        .filter_map(|(k, n)| {
            b.get(k)
                .map(|m| vec![ValueItem::Integer(*k), ValueItem::Integer(n * m)])
        })
        .collect();
    // Grouped in key order already, so compare as returned.
    assert_eq!(select_rows(&c, sql).1, expected);

    // The right side's key works too, and ORDER BY with a LIMIT stops early.
    let plan = explain(
        &c,
        "select b.k, count(*) from a join b on a.k = b.k group by b.k",
    );
    assert!(!plan.contains("Sort "), "{plan}");
    let sql = "select a.k, a.id, b.id from a join b on a.k = b.k order by a.k limit 5";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("SortMergeJoin") && !plan.contains("TopN"),
        "{plan}"
    );
    let got = select_rows(&c, sql).1;
    assert_eq!(got.len(), 5);
    assert_eq!(
        got[0][0],
        ValueItem::Integer(*a.keys().find(|k| b.contains_key(k)).unwrap())
    );
    assert!(got.windows(2).all(|w| w[0][0] <= w[1][0]), "{got:?}");

    // Not the join key: sorted as usual.
    let plan = explain(
        &c,
        "select a.id, count(*) from a join b on a.k = b.k group by a.id",
    );
    assert!(
        plan.contains("Sort ") || !plan.contains("SortMergeJoin"),
        "{plan}"
    );
}
