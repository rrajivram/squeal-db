// Subqueries in expressions (see plan::subquery): the cases the random
// oracle (oracle::test_random_subqueries_...) may not reach often — NULL
// semantics, UPDATE and DELETE, what is refused, and EXPLAIN.
use super::partition_diff::outcome;
use super::*;

fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    for sql in [
        "create table p (id integer not null, name varchar(10), primary key(id))",
        "create table c (id integer not null, pid integer, qty integer, primary key(id))",
        "insert into p values (1, 'a'), (2, 'b'), (3, 'c'), (4, null)",
        "insert into c values (10, 1, 5), (11, 1, 7), (12, 2, 1), (13, null, 9)",
    ] {
        run(&c, sql).unwrap();
    }
    c
}

fn ids(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<i64> {
    outcome(c, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| match &r[0] {
            ValueItem::Integer(i) => *i,
            other => panic!("{sql}: {other:?}"),
        })
        .collect()
}

#[test]
fn test_in_and_not_in_follow_sql_null_rules() {
    let c = setup();
    assert_eq!(ids(&c, "select id from p where id in (select pid from c)"), [1, 2]);
    // c.pid holds a NULL: NOT IN is never true.
    assert_eq!(ids(&c, "select id from p where id not in (select pid from c)"), Vec::<i64>::new());
    assert_eq!(
        ids(&c, "select id from p where id not in (select pid from c where pid is not null)"),
        [3, 4]
    );
    // Over no rows, NOT IN is true, even for... every row.
    assert_eq!(
        ids(&c, "select id from p where id not in (select pid from c where qty > 100)"),
        [1, 2, 3, 4]
    );
    // An integer matches the double that equals it.
    assert_eq!(ids(&c, "select id from p where id in (select qty / 5.0 from c)"), [1]);
}

#[test]
fn test_correlated_exists_and_in() {
    let c = setup();
    assert_eq!(
        ids(&c, "select id from p where exists (select 1 from c where c.pid = p.id)"),
        [1, 2]
    );
    assert_eq!(
        ids(&c, "select id from p where not exists (select 1 from c where c.pid = p.id)"),
        [3, 4]
    );
    assert_eq!(
        ids(&c, "select id from p where exists (select 1 from c where p.id = c.pid and c.qty > 6)"),
        [1]
    );
    // An unqualified column only the outer table has is the outer one.
    run(&c, "create table q (k integer not null, primary key(k))").unwrap();
    run(&c, "insert into q values (1), (2), (5)").unwrap();
    assert_eq!(
        ids(&c, "select k from q where 7 in (select qty from c where c.pid = k)"),
        [1]
    );
    // One both have is the inner one, as in SQL: `c.pid = c.id`, never.
    assert_eq!(
        ids(&c, "select id from p where 7 in (select qty from c where c.pid = id)"),
        Vec::<i64>::new()
    );
}

#[test]
fn test_scalar_subqueries() {
    let c = setup();
    assert_eq!(ids(&c, "select id from p where id = (select max(pid) from c)"), [2]);
    assert_eq!(ids(&c, "select (select count(*) from c) from p where id = 1"), [4]);
    // No rows: NULL.
    assert_eq!(
        outcome(&c, "select (select qty from c where id = 99) from p where id = 1").unwrap(),
        vec![vec![ValueItem::Null]]
    );
    let err = outcome(&c, "select id from p where id = (select pid from c)").unwrap_err();
    assert!(err.contains("more than one row"), "{err}");
}

#[test]
fn test_update_and_delete_with_subqueries() {
    let c = setup();
    run(&c, "update c set qty = 0 where pid in (select id from p where name = 'a')").unwrap();
    assert_eq!(ids(&c, "select id from c where qty = 0"), [10, 11]);
    run(&c, "update c set qty = (select max(id) from p) where id = 12").unwrap();
    assert_eq!(ids(&c, "select qty from c where id = 12"), [4]);
    run(&c, "delete from p where not exists (select 1 from c where c.pid = p.id)").unwrap();
    assert_eq!(ids(&c, "select id from p"), [1, 2]);
    run(&c, "begin").unwrap();
    run(&c, "delete from c where pid in (select id from p)").unwrap();
    assert_eq!(ids(&c, "select id from c"), [13]);
    run(&c, "rollback").unwrap();
    assert_eq!(ids(&c, "select id from c"), [10, 11, 12, 13]);
}

#[test]
fn test_subqueries_read_the_transactions_own_writes() {
    let c = setup();
    run(&c, "begin").unwrap();
    run(&c, "insert into c values (20, 3, 1)").unwrap();
    assert_eq!(
        ids(&c, "select id from p where exists (select 1 from c where c.pid = p.id)"),
        [1, 2, 3]
    );
    run(&c, "rollback").unwrap();
}

#[test]
fn test_unsupported_correlation_is_refused_not_misanswered() {
    let c = setup();
    for sql in [
        "select id from p where exists (select 1 from c where c.pid < p.id)",
        "select id from p where exists (select p.name from c where c.pid = p.id)",
        "select id from p where exists (select count(*) from c where c.pid = p.id)",
        "select id from p where exists (select 1 from c where c.pid = p.id limit 1)",
        "select id, (select max(qty) from c where c.pid = p.id) from p",
    ] {
        let err = outcome(&c, sql).unwrap_err();
        assert!(err.contains("correlated subquery"), "{sql}: {err}");
    }
    // A name that is nobody's is still the error it was.
    let err = outcome(&c, "select id from p where exists (select 1 from c where c.nope = 1)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("nope"), "{err}");
    let err = outcome(&c, "select id from p where id in (select id, pid from c)").unwrap_err();
    assert!(err.contains("one column"), "{err}");
}

#[test]
fn test_explain_shows_the_subquery() {
    let c = setup();
    let text = super::explain(
        &c,
        "select id from p where exists (select 1 from c where c.pid = p.id)",
    );
    assert!(text.contains("EXISTS (subquery: 3 row(s) by id)"), "{text}");
}
