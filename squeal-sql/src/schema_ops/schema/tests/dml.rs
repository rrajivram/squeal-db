use store::table::TableIdType;
use store::tuple::DBIdType;
use store::valueitem::{IndexKey, ValueItem};

use super::*;

// Reads a stored Tuple's raw bytes back as an IndexKey's own values —
// works for both a row in a table's own row-storage backing table (the
// full row) and an entry in an index's backing table (the row's
// identity, per Schema::insert_rows' own encoding).
fn find_row(s: &Arc<Schema<MemFile>>, table_id: TableIdType, key: DBIdType) -> Option<Vec<ValueItem>> {
    let txn = s.db.begin().unwrap();
    let tuple = s.db.find(table_id, key, &txn).unwrap()?;
    let ik = postcard::from_bytes::<IndexKey>(tuple.data()).unwrap();
    Some(ik.values().to_vec())
}

fn pk_key(id: i64) -> DBIdType {
    DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(id)]).unwrap())
}

#[test]
fn test_insert_stores_a_row_keyed_by_primary_key() {
    let conn = conn();
    execute(
        &conn,
        "create table users (id integer not null, name varchar(50), primary key(id))",
    )
    .unwrap();
    execute(&conn, "insert into users values (1, 'alice')").unwrap();

    let s = conn.current_schema().unwrap();
    let table = s.get_table("users").unwrap();
    let row = find_row(&s, table.db_table_id, pk_key(1)).unwrap();
    assert_eq!(
        row,
        vec![ValueItem::Integer(1), ValueItem::Str(("alice".into(), 50))]
    );
}

#[test]
fn test_insert_auto_generates_distinct_row_ids_without_a_primary_key() {
    let conn = conn();
    execute(&conn, "create table logs (message varchar(50))").unwrap();
    execute(&conn, "insert into logs values ('a')").unwrap();
    execute(&conn, "insert into logs values ('b')").unwrap();

    let s = conn.current_schema().unwrap();
    let table = s.get_table("logs").unwrap();
    // Two auto-generated ids, each holding exactly one of the two rows
    // (not asserting which literal ids — just that they're distinct and
    // both readable).
    let mut found = Vec::new();
    for candidate in 0..10u64 {
        if let Some(row) = find_row(&s, table.db_table_id, DBIdType::Int(candidate)) {
            found.push(row);
        }
    }
    assert_eq!(found.len(), 2, "expected exactly two distinct auto-id rows");
    assert!(found.contains(&vec![ValueItem::Str(("a".into(), 50))]));
    assert!(found.contains(&vec![ValueItem::Str(("b".into(), 50))]));
}

#[test]
fn test_insert_populates_secondary_unique_index() {
    let conn = conn();
    execute(
        &conn,
        "create table users (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))",
    )
    .unwrap();
    execute(&conn, "insert into users values (1, 'a@example.com')").unwrap();

    let s = conn.current_schema().unwrap();
    let table = s.get_table("users").unwrap();
    let idx = table.indices.iter().find(|i| !i.is_primary).unwrap();
    let identity = find_row(
        &s,
        idx.db_table_id,
        DBIdType::Rec(IndexKey::new_from(&[ValueItem::Str(("a@example.com".into(), 50))]).unwrap()),
    )
    .unwrap();
    // The index entry's value is the row's own identity — here, the
    // PRIMARY KEY's IndexKey values.
    assert_eq!(identity, vec![ValueItem::Integer(1)]);
}

#[test]
fn test_insert_enforces_primary_key_uniqueness() {
    let conn = conn();
    execute(&conn, "create table users (id integer not null, primary key(id))").unwrap();
    execute(&conn, "insert into users values (1)").unwrap();
    let err = execute(&conn, "insert into users values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
}

#[test]
fn test_insert_enforces_unique_constraint() {
    let conn = conn();
    execute(
        &conn,
        "create table users (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))",
    )
    .unwrap();
    execute(&conn, "insert into users values (1, 'a@example.com')").unwrap();
    let err = execute(&conn, "insert into users values (2, 'a@example.com')").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
}

#[test]
fn test_insert_multi_row_batch_is_atomic_on_constraint_violation() {
    let conn = conn();
    execute(&conn, "create table users (id integer not null, primary key(id))").unwrap();
    // Second row in the SAME statement collides with the first — the
    // whole batch, including the first, individually-valid row, must
    // not be committed.
    let err = execute(&conn, "insert into users values (1), (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");

    let s = conn.current_schema().unwrap();
    let table = s.get_table("users").unwrap();
    assert!(
        find_row(&s, table.db_table_id, pk_key(1)).is_none(),
        "neither row from the failed batch must have been committed"
    );
}

#[test]
fn test_insert_rejects_value_type_mismatch() {
    let conn = conn();
    execute(&conn, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&conn, "insert into users values ('not a number')").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_insert_rejects_null_into_not_null_column() {
    let conn = conn();
    execute(&conn, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&conn, "insert into users values (null)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_insert_rejects_into_an_unknown_table() {
    let conn = conn();
    let err = execute(&conn, "insert into nope values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_insert_rejects_an_unknown_column_in_explicit_list() {
    let conn = conn();
    execute(&conn, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&conn, "insert into users (nope) values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_insert_explicit_columns_fill_omitted_nullable_columns_with_null() {
    let conn = conn();
    execute(
        &conn,
        "create table users (id integer not null, nickname varchar(50), primary key(id))",
    )
    .unwrap();
    execute(&conn, "insert into users (id) values (1)").unwrap();

    let s = conn.current_schema().unwrap();
    let table = s.get_table("users").unwrap();
    let row = find_row(&s, table.db_table_id, pk_key(1)).unwrap();
    assert_eq!(row, vec![ValueItem::Integer(1), ValueItem::Null]);
}
