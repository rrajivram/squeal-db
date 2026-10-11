// A PRIMARY KEY has no tree of its own: the rows tree is keyed by it, and
// rejects a duplicate key itself (see SqlIndex::has_tree).
use store::cursor::Cursor;

use super::*;

fn count(c: &Arc<Connection<MemFile>>, sql: &str) -> i64 {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let crate::rslt::resultset::ResultType::StreamingResult(mut rows) =
        stmt.get_nextresult().unwrap().unwrap()
    else {
        panic!("{sql}: no rows");
    };
    match rows.next_result().unwrap().unwrap().values()[0] {
        ValueItem::Integer(n) => n,
        ref v => panic!("{sql}: {v:?}"),
    }
}

#[test]
fn test_a_primary_key_is_the_rows_tree_and_has_no_tree_of_its_own() {
    let c = conn();
    execute(
        &c,
        "create table t (id integer not null, v integer not null, primary key(id), unique(v))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let t = s.get_table("t").unwrap();
    assert!(t.indices[0].is_primary && !t.indices[0].has_tree());
    assert!(t.indices[1].has_tree());
    assert!(s.db.table_id_by_name("default.t0").unwrap().is_none());
    assert!(s.db.table_id_by_name("default.t1").unwrap().is_some());

    execute(&c, "insert into t values (1, 10), (2, 20), (3, 30)").unwrap();
    let err = execute(&c, "insert into t values (2, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
    assert_eq!(count(&c, "select count(*) from t where id = 2"), 1);
    assert_eq!(count(&c, "select count(*) from t where id >= 2"), 2);
    execute(&c, "update t set id = 4 where id = 3").unwrap();
    execute(&c, "delete from t where id = 1").unwrap();
    assert_eq!(count(&c, "select count(*) from t where id = 4 and v = 30"), 1);
    assert_eq!(count(&c, "select count(*) from t"), 2);
}

// The rows tree is written before any index: a duplicate primary key fails
// with nothing written, so inside a transaction that goes on, no index
// entry is left behind for a row that isn't there.
#[test]
fn test_a_duplicate_primary_key_leaves_no_index_entry_behind() {
    let c = conn();
    execute(
        &c,
        "create table t (id integer not null, v integer not null, primary key(id), unique(v))",
    )
    .unwrap();
    execute(&c, "begin").unwrap();
    execute(&c, "insert into t values (1, 10)").unwrap();
    let err = execute(&c, "insert into t values (1, 20)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
    execute(&c, "commit").unwrap();
    // A unique entry for v = 20 would make this a duplicate.
    execute(&c, "insert into t values (2, 20)").unwrap();
    assert_eq!(count(&c, "select count(*) from t"), 2);
}

// Databases written before PRIMARY KEYs lost their trees still name one in
// the catalog. It is neither read nor written: rows, lookups, foreign keys
// and statistics all go through the rows tree.
#[test]
fn test_a_primary_key_tree_left_by_an_older_build_is_neither_read_nor_written() {
    let c = conn();
    execute(
        &c,
        "create table parent (id integer not null, v integer, primary key(id))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let legacy = s.db.create_table("default.parent0".to_string()).unwrap();
    s.alter_table("parent", |t| {
        t.partitions[0].set_index(0, legacy);
        Ok(())
    })
    .unwrap();
    execute(
        &c,
        "create table child (id integer not null, parent_id integer references parent(id), \
         primary key(id))",
    )
    .unwrap();

    execute(&c, "insert into parent values (1, 10), (2, 20), (3, 30)").unwrap();
    execute(&c, "insert into child values (1, 2)").unwrap();
    let err = execute(&c, "insert into child values (2, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    execute(&c, "delete from parent where id = 3").unwrap();
    execute(&c, "analyze tables").unwrap();
    assert_eq!(count(&c, "select count(*) from parent where id >= 2"), 1);
    assert_eq!(count(&c, "select count(*) from parent where id = 1"), 1);

    let mut cursor = s.db.table_scan(legacy).unwrap();
    assert!(cursor.next().unwrap().is_none(), "the old tree was written");
}
