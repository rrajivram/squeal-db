use store::valueitem::ValueItem;

use super::*;

fn temp_csv_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "squeal_sql_copy_into_test_{tag}_{}.csv",
        std::process::id()
    ))
}

fn write_csv(tag: &str, contents: &str) -> std::path::PathBuf {
    let path = temp_csv_path(tag);
    std::fs::write(&path, contents).unwrap();
    path
}

fn select_rows(c: &Arc<Connection<MemFile>>, table_name: &str) -> Vec<Vec<ValueItem>> {
    let s = c.current_schema().unwrap();
    s.select_all(table_name, None).unwrap().rows().to_vec()
}

#[test]
fn test_copy_csv_into_loads_rows_positionally_skipping_the_header() {
    let path = write_csv(
        "basic",
        "id,name\n1,alice\n2,bob\n",
    );
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, name varchar(10), primary key(id))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s
        .copy_csv_into("users", path.to_str().unwrap())
        .unwrap();
    assert_eq!((loaded, failed), (2, 0));

    let mut rows = select_rows(&c, "users");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Str(("alice".into(), 10))],
            vec![ValueItem::Integer(2), ValueItem::Str(("bob".into(), 10))],
        ]
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_treats_an_empty_field_as_null() {
    let path = write_csv("null", "id,nickname\n1,\n");
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, nickname varchar(10), primary key(id))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s
        .copy_csv_into("users", path.to_str().unwrap())
        .unwrap();
    assert_eq!((loaded, failed), (1, 0));
    assert_eq!(
        select_rows(&c, "users"),
        vec![vec![ValueItem::Integer(1), ValueItem::Null]]
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_counts_an_empty_not_null_field_as_a_failure() {
    let path = write_csv("not_null", "id,name\n1,\n");
    let c = conn();
    execute(
        &c,
        "create table users (id integer not null, name varchar(10) not null, primary key(id))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s
        .copy_csv_into("users", path.to_str().unwrap())
        .unwrap();
    assert_eq!((loaded, failed), (0, 1));
    assert!(select_rows(&c, "users").is_empty());

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_continues_past_a_bad_row_instead_of_aborting() {
    // Row 2 has a non-integer id — must be skipped and counted, not
    // fail the whole load; rows 1 and 3 must still land.
    let path = write_csv("continue", "id\n1\nnot-a-number\n3\n");
    let c = conn();
    execute(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s.copy_csv_into("t", path.to_str().unwrap()).unwrap();
    assert_eq!((loaded, failed), (2, 1));

    let mut rows = select_rows(&c, "t");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(3)]]
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_counts_a_constraint_violation_as_a_failure_not_an_abort() {
    // Row 2 duplicates row 1's primary key — must be skipped/counted,
    // not fail the whole load.
    let path = write_csv("constraint", "id\n1\n1\n2\n");
    let c = conn();
    execute(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s.copy_csv_into("t", path.to_str().unwrap()).unwrap();
    assert_eq!((loaded, failed), (2, 1));

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_parses_date_strings_into_a_datetime_column() {
    let path = write_csv("dates", "id,happened_on\n1,2020-04-13\n2,12:53:24\n");
    let c = conn();
    execute(
        &c,
        "create table events (id integer not null, happened_on datetime, primary key(id))",
    )
    .unwrap();
    let s = c.current_schema().unwrap();
    let (loaded, failed) = s
        .copy_csv_into("events", path.to_str().unwrap())
        .unwrap();
    assert_eq!((loaded, failed), (2, 0));

    let mut rows = select_rows(&c, "events");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Datetime(18365 * 86400)],
            vec![
                ValueItem::Integer(2),
                ValueItem::Datetime(12 * 3600 + 53 * 60 + 24)
            ],
        ]
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_rejects_an_unknown_table() {
    let path = write_csv("unknown_table", "id\n1\n");
    let c = conn();
    let s = c.current_schema().unwrap();
    let err = s.copy_csv_into("nope", path.to_str().unwrap()).unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");

    std::fs::remove_file(path).ok();
}

#[test]
fn test_copy_csv_into_rejects_a_missing_file() {
    let c = conn();
    execute(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let s = c.current_schema().unwrap();
    let err = s
        .copy_csv_into("t", "/nonexistent/squeal_sql_test_path.csv")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}
