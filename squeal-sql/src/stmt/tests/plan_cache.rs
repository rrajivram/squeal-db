// Plans kept per statement shape (plan::logical::PlanCache): each
// execution gets its own values, and a change to the catalog is seen.

use super::*;

fn values(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    select_rows(c, sql).1
}

fn s(v: &str) -> ValueItem {
    ValueItem::Str((v.into(), v.len() as u32))
}

#[test]
fn test_one_shape_runs_with_each_texts_values() {
    let c = conn();
    run(&c, "create table pc_t (id integer not null, name varchar(10), primary key(id))").unwrap();
    run(&c, "insert into pc_t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')").unwrap();
    for (id, name) in [(1, "a"), (3, "c"), (2, "b"), (9, "")] {
        let got = values(&c, &format!("select name from pc_t where id = {id}"));
        let want = if name.is_empty() { vec![] } else { vec![vec![s(name)]] };
        assert_eq!(got, want, "id = {id}");
    }
    assert!(c.plans.len() > 0, "the shape's plan is kept");
    // Ranges, a LIMIT and a string, through one kept plan each.
    for (low, limit, want) in [(2, 2, vec![2, 3]), (1, 3, vec![1, 2, 3]), (4, 5, vec![4])] {
        let got = values(&c, &format!("select id from pc_t where id >= {low} order by id limit {limit}"));
        assert_eq!(got, ints(&want.iter().map(std::slice::from_ref).collect::<Vec<_>>()));
    }
    for name in ["b", "d", "zz"] {
        let got = values(&c, &format!("select id from pc_t where name = '{name}'"));
        assert_eq!(got.len(), usize::from(name != "zz"), "{name}");
    }
}

#[test]
fn test_a_kept_plan_sees_tables_change() {
    let c = conn();
    run(&c, "create table pc_a (id integer not null, n integer, primary key(id))").unwrap();
    run(&c, "insert into pc_a values (1, 10), (2, 20)").unwrap();
    assert_eq!(values(&c, "select * from pc_a where id = 1"), ints(&[&[1, 10]]));
    // A column added: `*` reads it.
    run(&c, "alter table pc_a add column m integer").unwrap();
    assert_eq!(
        values(&c, "select * from pc_a where id = 2"),
        vec![vec![ValueItem::Integer(2), ValueItem::Integer(20), ValueItem::Null]]
    );
    // Dropped and made again, differently.
    run(&c, "drop table pc_a").unwrap();
    run(&c, "create table pc_a (id integer not null, label varchar(5), primary key(id))").unwrap();
    run(&c, "insert into pc_a values (1, 'x')").unwrap();
    assert_eq!(values(&c, "select * from pc_a where id = 1"), vec![vec![ValueItem::Integer(1), s("x")]]);
    // An index made after the plan: still the right rows.
    run(&c, "insert into pc_a values (2, 'y'), (3, 'x')").unwrap();
    assert_eq!(values(&c, "select id from pc_a where label = 'x' order by id"), ints(&[&[1], &[3]]));
    run(&c, "create index pc_a_label on pc_a (label)").unwrap();
    assert_eq!(values(&c, "select id from pc_a where label = 'x' order by id"), ints(&[&[1], &[3]]));
    assert_eq!(values(&c, "select id from pc_a where label = 'y' order by id"), ints(&[&[2]]));
}

#[test]
fn test_a_kept_plan_is_for_one_current_schema() {
    let c = conn();
    run(&c, "create table pc_s (id integer not null, n integer, primary key(id))").unwrap();
    run(&c, "insert into pc_s values (1, 10)").unwrap();
    run(&c, "create schema pc_other").unwrap();
    run(&c, "use pc_other").unwrap();
    run(&c, "create table pc_s (id integer not null, n integer, primary key(id))").unwrap();
    run(&c, "insert into pc_s values (1, 99)").unwrap();
    assert_eq!(values(&c, "select n from pc_s where id = 1"), ints(&[&[99]]));
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    assert_eq!(values(&c, "select n from pc_s where id = 1"), ints(&[&[10]]));
}

#[test]
fn test_group_by_expressions_are_matched_on_their_values() {
    let c = conn();
    run(&c, "create table pc_g (id integer not null, a integer, primary key(id))").unwrap();
    run(&c, "insert into pc_g values (1, 1), (2, 1), (3, 2)").unwrap();
    let kept = c.plans.len();
    for k in [1, 5] {
        let mut got = values(&c, &format!("select a + {k}, count(*) from pc_g group by a + {k}"));
        got.sort_by(|x, y| x[0].cmp(&y[0]));
        assert_eq!(got, ints(&[&[1 + k, 2], &[2 + k, 1]]), "+{k}");
    }
    assert_eq!(c.plans.len(), kept + 1, "one plan for the shape");
    // Same shape, different literals: not a grouped expression.
    let mut stmt = c
        .clone()
        .create_statement("select a + 1, count(*) from pc_g group by a + 2")
        .unwrap();
    assert!(stmt.execute().is_err());
}

#[test]
fn test_shapes_that_run_while_planned_are_not_kept_but_still_right() {
    let c = conn();
    run(&c, "create table pc_q (id integer not null, n integer, primary key(id))").unwrap();
    run(&c, "insert into pc_q values (1, 10), (2, 20), (3, 30)").unwrap();
    for k in [10, 20] {
        let sub = values(&c, &format!("select id from pc_q where n > (select min(n) from pc_q where n >= {k}) order by id"));
        let want: Vec<i64> = [1, 2, 3].into_iter().filter(|i| i * 10 > k).collect();
        assert_eq!(sub, ints(&want.iter().map(std::slice::from_ref).collect::<Vec<_>>()));
        let derived = values(&c, &format!("select count(*) from (select id from pc_q where n > {k}) d"));
        assert_eq!(derived, ints(&[&[want.len() as i64]]));
        let with = values(&c, &format!("with w as (select id from pc_q where n > {k}) select count(*) from w"));
        assert_eq!(with, ints(&[&[want.len() as i64]]));
    }
}
