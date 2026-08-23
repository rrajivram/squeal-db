use store::named_memfile::NamedMemFile;
use store::valueitem::ValueItem;

use super::*;

fn select_rows(c: &Arc<Connection<MemFile>>, table_name: &str) -> Vec<Vec<ValueItem>> {
    let s = c.current_schema().unwrap();
    s.select_all(table_name, None).unwrap().rows().to_vec()
}

#[test]
fn test_add_column_backfills_default_for_rows_written_before_it_existed() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    execute(&c, "insert into users values (1)").unwrap();

    execute(&c, "alter table users add column plan varchar(10) default 'free'").unwrap();
    execute(&c, "insert into users (id, plan) values (2, 'pro')").unwrap();

    let mut rows = select_rows(&c, "users");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Str(("free".into(), 10))],
            vec![ValueItem::Integer(2), ValueItem::Str(("pro".into(), 10))],
        ],
        "the pre-ALTER row must be backfilled with the new column's default, not rewritten"
    );
}

#[test]
fn test_add_column_nullable_without_default_backfills_null_for_old_rows() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    execute(&c, "insert into users values (1)").unwrap();

    execute(&c, "alter table users add column nickname varchar(10)").unwrap();

    let rows = select_rows(&c, "users");
    assert_eq!(rows, vec![vec![ValueItem::Integer(1), ValueItem::Null]]);
}

#[test]
fn test_add_column_not_null_without_default_is_rejected() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users add column plan varchar(10) not null").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_add_column_rejects_a_duplicate_name() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users add column id integer").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_add_column_rejects_an_unknown_table() {
    let c = conn();
    let err = execute(&c, "alter table nope add column x integer").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_insert_omitting_a_column_added_by_alter_uses_its_default() {
    // DEFAULT applies to any INSERT that omits the column, not just the
    // read-time backfill of rows that predate it (see Field::default's
    // own doc comment).
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    execute(&c, "alter table users add column plan varchar(10) default 'free'").unwrap();
    execute(&c, "insert into users (id) values (1)").unwrap();

    let rows = select_rows(&c, "users");
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1), ValueItem::Str(("free".into(), 10))]]
    );
}

#[test]
fn test_drop_column_omits_it_from_both_old_and_new_rows() {
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, nickname varchar(10), primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into users values (1, 'al')").unwrap();

    execute(&c, "alter table users drop column nickname").unwrap();
    execute(&c, "insert into users values (2)").unwrap();

    let mut rows = select_rows(&c, "users");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]],
        "a dropped column must vanish from every row, including ones written before the drop"
    );
    let s = c.current_schema().unwrap();
    assert_eq!(s.get_table("users").unwrap().fields().len(), 1);
}

#[test]
fn test_drop_column_rejects_a_column_used_by_an_index() {
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))",
    )
    .unwrap();
    let err = execute(&c, "alter table users drop column email").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    // Rejected, not partially applied — the column must still be there.
    let s = c.current_schema().unwrap();
    assert_eq!(s.get_table("users").unwrap().fields().len(), 2);
}

#[test]
fn test_drop_column_rejects_the_primary_key_column() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users drop column id").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_drop_column_rejects_an_unknown_column() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users drop column nope").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_rename_column_preserves_values_for_rows_old_and_new() {
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, nickname varchar(10), primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into users values (1, 'al')").unwrap();

    execute(&c, "alter table users rename column nickname to handle").unwrap();
    execute(&c, "insert into users values (2, 'bo')").unwrap();

    let s = c.current_schema().unwrap();
    let result = s.select_all("users", None).unwrap();
    assert_eq!(result.columns(), &["id".to_string(), "handle".to_string()]);
    let mut rows = result.rows().to_vec();
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Str(("al".into(), 10))],
            vec![ValueItem::Integer(2), ValueItem::Str(("bo".into(), 10))],
        ]
    );
}

#[test]
fn test_rename_column_rejects_a_duplicate_name() {
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, nickname varchar(10), primary key(id))",
    )
    .unwrap();
    let err = execute(&c, "alter table users rename column nickname to id").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_rename_column_rejects_a_column_used_by_an_index() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users rename column id to uid").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_rename_column_rejects_an_unknown_column() {
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table users rename column nope to id2").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_multiple_alters_stack_versions_and_every_generation_still_decodes() {
    // Three distinct SchemaVersions in play at once: a row written at
    // v0 (just id), one at v1 (id, plan added), one at v2 (id, plan,
    // rank added) — reproject must bridge every one of them onto v2.
    let c = conn();
    execute(&c, "create table users (id integer not null, primary key(id))").unwrap();
    execute(&c, "insert into users values (1)").unwrap();

    execute(&c, "alter table users add column plan varchar(10) default 'free'").unwrap();
    execute(&c, "insert into users values (2, 'pro')").unwrap();

    execute(&c, "alter table users add column rank integer default 0").unwrap();
    execute(&c, "insert into users values (3, 'pro', 5)").unwrap();

    let mut rows = select_rows(&c, "users");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![
                ValueItem::Integer(1),
                ValueItem::Str(("free".into(), 10)),
                ValueItem::Integer(0)
            ],
            vec![
                ValueItem::Integer(2),
                ValueItem::Str(("pro".into(), 10)),
                ValueItem::Integer(0)
            ],
            vec![
                ValueItem::Integer(3),
                ValueItem::Str(("pro".into(), 10)),
                ValueItem::Integer(5)
            ],
        ]
    );
}

#[test]
fn test_alter_table_metadata_and_old_rows_survive_close_and_reopen() {
    // Same shape as contract::test_schema_state_survives_close_and_reopen,
    // but for ALTER's own new persistence path (Schema::alter_table's
    // db.update — a plain SqlTable metadata row, exactly like
    // create_table's) and for reproject specifically: a row written
    // under version 0, read back after a reopen, still needs the
    // backfilled default for a column that didn't exist when it was
    // written.
    let path = temp_schema_path("alter_close_reopen_roundtrip");
    NamedMemFile::delete(&path);

    let db = Database::<NamedMemFile>::create(path.clone()).unwrap();
    let s = db.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    create_table_directly(&s, "create table users (id integer not null, primary key(id))");
    s.insert_rows("users", vec![vec![ValueItem::Integer(1)]], None).unwrap();
    s.add_column(
        "users",
        crate::table::Field::new(
            "plan".into(),
            crate::datatype::DataType::Str(10),
            true,
            Some(ValueItem::Str(("free".into(), 10))),
        )
        .unwrap(),
    )
    .unwrap();
    drop(s);
    db.close().unwrap();

    let db2 = Database::<NamedMemFile>::open(path.clone()).unwrap();
    let s2 = db2.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let table = s2.get_table("users").unwrap();
    assert_eq!(table.fields().len(), 2);
    assert_eq!(table.version(), 1);

    let result = s2.select_all("users", None).unwrap();
    assert_eq!(
        result.rows(),
        &[vec![
            ValueItem::Integer(1),
            ValueItem::Str(("free".into(), 10))
        ]],
        "a row written before ALTER TABLE, in a schema loaded fresh after reopen, \
         must still reproject with the backfilled default"
    );

    NamedMemFile::delete(&path);
}
