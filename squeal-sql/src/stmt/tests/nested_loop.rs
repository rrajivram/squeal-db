//! Index nested-loop joins (see source::nestloop, optim::picker::
//! pick_join_seek) and WHERE conditions applied at each table's scan,
//! below the joins.
//!
//! Inner joins are checked against a reference no join algorithm touches:
//! the same condition with `+ 0` on one side (`o.id = d.order_id + 0`),
//! which isn't a column-to-column equality, so it runs as a cross product
//! filtered by WHERE.

use super::*;
use store::valueitem::ValueItem;

// o: a few orders per customer; d: many detail rows per order, keyed by
// (order_id, line) — so order_id alone is a prefix of d's primary key — and
// indexed by product. Some orders have no details; some details' product
// is NULL.
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table o (id integer not null, customer integer, note varchar(100), primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table d (order_id integer not null, line integer not null, product integer, \
         qty integer, primary key(order_id, line))",
    )
    .unwrap();
    run(&c, "create index d_product on d (product)").unwrap();
    let orders = (0..400)
        .map(|i| format!("({i}, {}, 'order {i}')", i % 100))
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into o values {orders}")).unwrap();
    // Orders 0..350 have 1..=5 lines; 350..400 have none.
    let details = (0..350)
        .flat_map(|o| {
            (0..(o % 5 + 1)).map(move |l| {
                let product = if (o + l) % 17 == 0 {
                    "null".to_string()
                } else {
                    ((o * 7 + l) % 600).to_string()
                };
                format!("({o}, {l}, {product}, {})", l + 1)
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into d values {details}")).unwrap();
    // Filler for orders that don't exist: enough of d that seeking a few
    // keys beats reading all of it, without changing any join's result.
    let filler = (0..3000)
        .map(|i| format!("({}, 0, {}, 1)", 10_000 + i, 10_000 + i))
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into d values {filler}")).unwrap();
    run(&c, "insert into o values (1000, null, 'no customer')").unwrap();
    run(&c, "analyze table o").unwrap();
    run(&c, "analyze table d").unwrap();
    c
}

fn sorted(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    let mut r = select_rows(c, sql).1;
    r.sort();
    r
}

#[test]
fn test_a_selective_outer_side_seeks_the_primary_key_per_row() {
    let c = setup();
    let sql = "select o.id, d.line, d.qty from o join d on o.id = d.order_id where o.customer = 7";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("NestedLoopJoin Inner on outer(id) = inner(order_id)"),
        "{plan}"
    );
    assert!(
        plan.contains("[each outer row] TableSeek d (order_id = outer id)"),
        "{plan}"
    );
    // The condition on o is checked at o's scan, below the join.
    assert!(plan.contains("[outer] Filter (customer = 7)"), "{plan}");
    let reference =
        "select o.id, d.line, d.qty from o, d where o.id = d.order_id + 0 and o.customer = 7";
    let plan = explain(&c, reference);
    assert!(plan.contains("CrossJoin"), "{plan}");
    assert_eq!(sorted(&c, sql), sorted(&c, reference));
    assert!(!sorted(&c, sql).is_empty());
}

#[test]
fn test_a_secondary_index_seeks_and_fetches_rows() {
    let c = setup();
    // d joined by product (an index, not unique) from a selective outer.
    let sql = "select o.id, d.order_id, d.line from o join d on o.customer = d.product \
               where o.id = 21";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("IndexLookup d using d_product (product = outer customer)"),
        "{plan}"
    );
    let reference =
        "select o.id, d.order_id, d.line from o, d where o.customer = d.product + 0 and o.id = 21";
    assert_eq!(sorted(&c, sql), sorted(&c, reference));
    assert!(!sorted(&c, sql).is_empty());
}

#[test]
fn test_a_left_join_keeps_unmatched_and_null_keyed_outer_rows() {
    let c = setup();
    // Some of these customers match no product; order 1000's is NULL.
    let sql = "select o.id, d.line from o left join d on o.customer = d.product \
               where o.id in (21, 395, 1000)";
    let plan = explain(&c, sql);
    assert!(plan.contains("NestedLoopJoin Left"), "{plan}");
    let got = sorted(&c, sql);
    // By hand: every outer row with its matches, or NULL-extended.
    let products: Vec<(i64, Option<i64>)> = select_rows(&c, "select line, product from d")
        .1
        .into_iter()
        .map(|r| {
            let line = match r[0] {
                ValueItem::Integer(l) => l,
                _ => unreachable!(),
            };
            let product = match r[1] {
                ValueItem::Integer(p) => Some(p),
                _ => None,
            };
            (line, product)
        })
        .collect();
    let mut expected = vec![];
    for (id, customer) in [21, 395]
        .into_iter()
        .map(|i| (i, Some(i % 100)))
        .chain([(1000, None)])
    {
        let matches: Vec<_> = products
            .iter()
            .filter(|(_, p)| customer.is_some() && *p == customer)
            .map(|(l, _)| ValueItem::Integer(*l))
            .collect();
        if matches.is_empty() {
            expected.push(vec![ValueItem::Integer(id), ValueItem::Null]);
        }
        for l in matches {
            expected.push(vec![ValueItem::Integer(id), l]);
        }
    }
    expected.sort();
    assert_eq!(got, expected);
    assert!(got.contains(&vec![ValueItem::Integer(1000), ValueItem::Null]));
}

#[test]
fn test_both_key_columns_seek_one_row() {
    let c = setup();
    // customer = 3 carries to d.line, but line isn't a leading key column,
    // so it can't narrow a read of d by itself.
    let sql = "select o.id, d.qty from o join d on o.id = d.order_id and o.customer = d.line \
               where o.customer = 3";
    let plan = explain(&c, sql);
    assert!(
        plan.contains("TableSeek d (order_id = outer id AND line = outer customer) (~1 rows)"),
        "{plan}"
    );
    let reference = "select o.id, d.qty from o, d where o.id = d.order_id + 0 \
                     and o.customer = d.line + 0 and o.customer = 3";
    assert_eq!(sorted(&c, sql), sorted(&c, reference));
    assert_eq!(sorted(&c, sql).len(), 4);
}

#[test]
fn test_an_unselective_outer_side_still_hashes() {
    let c = setup();
    let plan = explain(&c, "select count(*) from o join d on o.id = d.order_id");
    assert!(plan.contains("HashJoin"), "{plan}");
}

#[test]
fn test_a_nested_loop_join_sees_the_statements_transaction() {
    let c = setup();
    let sql = "select d.qty from o join d on o.id = d.order_id where o.customer = 99";
    assert!(explain(&c, sql).contains("NestedLoopJoin"));
    let before = select_rows(&c, sql).1.len();
    run(&c, "begin").unwrap();
    run(&c, "insert into d values (399, 0, 1, 42)").unwrap();
    assert_eq!(select_rows(&c, sql).1.len(), before + 1);
    run(&c, "rollback").unwrap();
    assert_eq!(select_rows(&c, sql).1.len(), before);
}

#[test]
fn test_where_on_the_null_extended_side_stays_after_the_join() {
    let c = setup();
    // d.qty = 1 must remove NULL-extended rows too, so it can't go below
    // the LEFT JOIN.
    let sql = "select o.id, d.qty from o left join d on o.id = d.order_id \
               where o.id >= 340 and o.id < 360 and d.qty = 1";
    let plan = explain(&c, sql);
    assert!(plan.contains("Filter (qty = 1)"), "{plan}");
    assert!(!plan.contains("[each outer row] Filter"), "{plan}");
    let got = sorted(&c, sql);
    assert!(got.iter().all(|r| r[1] == ValueItem::Integer(1)), "{got:?}");
    assert!(!got.is_empty());
}

// A WHERE equality between FROM items (a comma join) is an inner join too,
// and can seek the same way as JOIN ... ON.
#[test]
fn test_a_comma_join_can_seek_per_row() {
    let c = setup();
    let sql = "select o.id, d.line, d.qty from o, d where o.id = d.order_id and o.customer = 7";
    let plan = explain(&c, sql);
    assert!(plan.contains("NestedLoopJoin Inner"), "{plan}");
    assert!(
        plan.contains("[each outer row] TableSeek d (order_id = outer id)"),
        "{plan}"
    );
    let reference =
        "select o.id, d.line, d.qty from o, d where o.id = d.order_id + 0 and o.customer = 7";
    assert_eq!(sorted(&c, sql), sorted(&c, reference));
    assert!(!sorted(&c, sql).is_empty());
    // d's own condition is still checked (after the join), since its rows
    // come from the seeks rather than its scan.
    let sql = "select o.id, d.line from o, d where o.id = d.order_id and o.customer = 7 \
               and d.qty = 2";
    let plan = explain(&c, sql);
    assert!(plan.contains("NestedLoopJoin"), "{plan}");
    assert!(plan.contains("qty = 2"), "{plan}");
    let reference = "select o.id, d.line from o, d where o.id = d.order_id + 0 \
                     and o.customer = 7 and d.qty = 2";
    assert_eq!(sorted(&c, sql), sorted(&c, reference));
    assert!(!sorted(&c, sql).is_empty());
    // Unselective: still hashed.
    let plan = explain(&c, "select count(*) from o, d where o.id = d.order_id");
    assert!(plan.contains("HashJoin"), "{plan}");
}
