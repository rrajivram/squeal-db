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

// ---- plans kept whole: a key lookup's operators reopened per execution ----

fn reopened() -> usize {
    crate::plan::logical::REOPENED.with(|r| r.get())
}

// A table of three people, named `table`: each test its own, so no other
// test shares (and replaces, running at the same time) its statements'
// shapes in the parse cache.
fn people(c: &Arc<Connection<MemFile>>, table: &str) {
    run(c, &format!("create table {table} (id integer not null, name varchar(10), n integer, primary key(id))")).unwrap();
    run(c, &format!("insert into {table} values (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")).unwrap();
}

#[test]
fn test_a_key_lookup_is_kept_and_reopened_for_each_key() {
    let c = conn();
    people(&c, "pk_1");
    let before = reopened();
    for (id, want) in [(1, Some("a")), (2, Some("b")), (9, None), (3, Some("c")), (1, Some("a"))] {
        let got = values(&c, &format!("select name from pk_1 where id = {id}"));
        let want: Vec<Vec<ValueItem>> = want.map(|w| vec![s(w)]).into_iter().collect();
        assert_eq!(got, want, "id = {id}");
    }
    // The first execution builds the plan; each after it reopens it.
    assert_eq!(reopened(), before + 4);
    // With a condition besides the key that holds no value of its own
    // (every literal in the text becomes one): kept too.
    let before = reopened();
    for (id, want) in [(1, 1), (9, 0), (2, 1)] {
        let got = values(&c, &format!("select id from pk_1 where id = {id} and n > id"));
        assert_eq!(got.len(), want, "id = {id}");
    }
    assert_eq!(reopened(), before + 2);
}

#[test]
fn test_a_lookup_with_a_value_outside_the_key_is_planned_each_time() {
    let c = conn();
    people(&c, "pk_2");
    let before = reopened();
    for (id, n, want) in [(1, 10, 1), (1, 20, 0), (2, 20, 1)] {
        let got = values(&c, &format!("select name from pk_2 where id = {id} and n = {n}"));
        assert_eq!(got.len(), want, "id = {id}, n = {n}");
    }
    assert_eq!(reopened(), before, "n's value would be stale in a kept plan");
}

#[test]
fn test_a_kept_lookup_held_open_while_the_shape_runs_again() {
    let c = conn();
    people(&c, "pk_3");
    values(&c, "select name from pk_3 where id = 1");
    let mut first = c.clone().create_statement("select name from pk_3 where id = 2").unwrap();
    first.execute().unwrap();
    // The kept plan is out with `first`: this one is planned anew.
    assert_eq!(values(&c, "select name from pk_3 where id = 3"), vec![vec![s("c")]]);
    assert_eq!(take_streaming_result(&mut first, 0).1, vec![vec![s("b")]]);
    assert_eq!(values(&c, "select name from pk_3 where id = 1"), vec![vec![s("a")]]);
}

#[test]
fn test_a_kept_lookup_sees_its_transaction_and_others_commits() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("pk_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    people(&c, "pk_4");
    values(&c, "select n from pk_4 where id = 1");
    run(&c, "begin").unwrap();
    run(&c, "insert into pk_4 values (4, 'd', 40)").unwrap();
    assert_eq!(values(&c, "select n from pk_4 where id = 4"), ints(&[&[40]]));
    run(&c, "rollback").unwrap();
    assert_eq!(values(&c, "select n from pk_4 where id = 4"), Vec::<Vec<ValueItem>>::new());
    // Another connection's commit, between two executions.
    let other = mgr.connect("pk_db").unwrap();
    other.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    run(&other, "update pk_4 set n = 11 where id = 1").unwrap();
    assert_eq!(values(&c, "select n from pk_4 where id = 1"), ints(&[&[11]]));
}

#[test]
fn test_a_kept_lookup_sees_the_table_change() {
    let c = conn();
    people(&c, "pk_5");
    values(&c, "select * from pk_5 where id = 1");
    values(&c, "select * from pk_5 where id = 2");
    run(&c, "alter table pk_5 add column m integer").unwrap();
    assert_eq!(
        values(&c, "select * from pk_5 where id = 3"),
        vec![vec![ValueItem::Integer(3), s("c"), ValueItem::Integer(30), ValueItem::Null]]
    );
}

#[test]
fn test_composite_and_unique_index_lookups_are_kept() {
    let c = conn();
    run(&c, "create table ck (a integer not null, b varchar(4) not null, v integer, primary key(a, b))").unwrap();
    run(&c, "insert into ck values (1, 'x', 10), (1, 'y', 11), (2, 'x', 20)").unwrap();
    let before = reopened();
    for (a, b, want) in [(1, "x", Some(10)), (1, "y", Some(11)), (2, "y", None), (2, "x", Some(20))] {
        let got = values(&c, &format!("select v from ck where a = {a} and b = '{b}'"));
        let want: Vec<Vec<ValueItem>> = want.map(|w| vec![ValueItem::Integer(w)]).into_iter().collect();
        assert_eq!(got, want, "({a}, {b})");
    }
    assert_eq!(reopened(), before + 3);
    // Only part of the key: many rows may match, so planned each time.
    let before = reopened();
    for a in [1, 2] {
        values(&c, &format!("select v from ck where a = {a}"));
    }
    assert_eq!(reopened(), before);

    // Enough rows that the index is read, not the table.
    run(&c, "create table uk (id integer not null, name varchar(10), primary key(id))").unwrap();
    // Names of one length: a literal's length is part of its shape.
    let rows: Vec<String> = (1..=500).map(|i| format!("({i}, 'n{i:03}')")).collect();
    run(&c, &format!("insert into uk values {}", rows.join(", "))).unwrap();
    run(&c, "create unique index uk_name on uk (name)").unwrap();
    run(&c, "analyze tables").unwrap();
    let before = reopened();
    for (name, want) in [("n001", Some(1)), ("n300", Some(300)), ("zzzz", None), ("n002", Some(2))] {
        let got = values(&c, &format!("select id from uk where name = '{name}'"));
        let want: Vec<Vec<ValueItem>> = want.map(|w| vec![ValueItem::Integer(w)]).into_iter().collect();
        assert_eq!(got, want, "{name}");
    }
    assert_eq!(reopened(), before + 3);
}

#[test]
fn test_a_prepared_lookup_is_kept() {
    let c = conn();
    people(&c, "pk_6");
    let mut stmt = c
        .clone()
        .create_prepared_statement("select name from pk_6 where id = ?")
        .unwrap();
    let before = reopened();
    for (id, want) in [(1, "a"), (3, "c"), (2, "b")] {
        stmt.set_field(0, ValueItem::Integer(id)).unwrap();
        assert_eq!(prepared_rows(stmt.execute().unwrap()), vec![vec![s(want)]]);
    }
    assert_eq!(reopened(), before + 2);
}


#[test]
fn test_a_kept_lookup_reads_doubles_as_the_planner_does() {
    // `200`, `2.0` and `2.5` are one shape: a kept plan reads each key as a
    // seek planned for it would.
    let c = conn();
    run(&c, "create table pk_d (id integer not null, name varchar(10), primary key(id))").unwrap();
    run(&c, "insert into pk_d values (2, 'two'), (200, 'many')").unwrap();
    let before = reopened();
    for (key, want) in [("200", Some("many")), ("2.0", Some("two")), ("2.5", None), ("201", None)] {
        let got = values(&c, &format!("select name from pk_d where id = {key}"));
        let want: Vec<Vec<ValueItem>> = want.map(|w| vec![s(w)]).into_iter().collect();
        assert_eq!(got, want, "id = {key}");
    }
    assert_eq!(reopened(), before + 3);
}
