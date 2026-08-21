// SQL text -> Schema/SqlTable/Field/SqlIndex object correctness. Not
// interested in sqlparser's own grammar/error behavior here — only in
// whether valid SQL ends up as the struct shape schema_ops promises.
use crate::datatype::DataType;

use super::*;

#[test]
fn test_create_table_extracts_basic_shape() {
    let t = create_and_fetch(
        "create table users (c integer, a integer not null, b varchar(50))",
        "users",
    );
    assert_eq!(t.name, "users");
    let names: Vec<_> = t.fields.iter().map(|f| f.name.clone()).collect();
    assert_eq!(
        names,
        vec!["c", "a", "b"],
        "declared field order must be preserved"
    );
    assert!(field(&t, "c").nullable, "columns are nullable by default");
    assert!(!field(&t, "a").nullable, "NOT NULL must be honored");
}

#[test]
fn test_create_table_maps_datatypes() {
    let cases = [
        ("id integer", "id", DataType::Integer),
        ("name varchar(50)", "name", DataType::Str(50)),
        ("bio text", "bio", DataType::Str(32)),
        ("price double", "price", DataType::Double),
        // sqlparser::ast::DataType::Boolean has no ValueItem counterpart
        // yet, so it silently falls through to Unsupported instead of
        // erroring — documents current behavior, not desired behavior.
        ("active boolean", "active", DataType::Unsupported),
    ];
    for (col_sql, col_name, expected) in cases {
        let t = create_and_fetch(&format!("create table t ({col_sql})"), "t");
        assert_eq!(
            field(&t, col_name).datatype,
            expected,
            "datatype mapping for `{col_sql}`"
        );
    }
}

#[test]
fn test_create_table_name_is_case_insensitive() {
    let s = schema();
    s.execute("create table Users (id integer)".to_string())
        .unwrap();
    for variant in ["users", "Users", "USERS"] {
        assert!(
            s.table_exists(variant),
            "table_exists({variant:?}) should find the table regardless of case"
        );
        assert_eq!(s.get_table(variant).unwrap().name, "users");
    }
}

#[test]
fn test_create_table_extracts_index_shape() {
    struct Case {
        sql: &'static str,
        is_primary: bool,
        is_unique: bool,
        field_name: &'static str,
        index_name: Option<&'static str>,
    }
    let cases = [
        // table-level PRIMARY KEY(...)
        Case {
            sql: "create table t (id integer not null, name varchar(50), primary key(id))",
            is_primary: true,
            is_unique: true,
            field_name: "id",
            index_name: None,
        },
        // table-level UNIQUE(...)
        Case {
            sql: "create table t (id integer, email varchar(100) not null, unique(email))",
            is_primary: false,
            is_unique: true,
            field_name: "email",
            index_name: None,
        },
        // inline column-level PRIMARY KEY
        Case {
            sql: "create table t (id integer not null primary key)",
            is_primary: true,
            is_unique: true,
            field_name: "id",
            index_name: None,
        },
        // inline column-level UNIQUE
        Case {
            sql: "create table t (email varchar(100) not null unique)",
            is_primary: false,
            is_unique: true,
            field_name: "email",
            index_name: None,
        },
        // explicitly named constraint
        Case {
            sql: "create table t (id integer not null, constraint my_pk primary key(id))",
            is_primary: true,
            is_unique: true,
            field_name: "id",
            index_name: Some("my_pk"),
        },
    ];
    for c in cases {
        let t = create_and_fetch(c.sql, "t");
        assert_eq!(t.indices.len(), 1, "for `{}`", c.sql);
        let idx = &t.indices[0];
        assert_eq!(idx.is_primary, c.is_primary, "is_primary for `{}`", c.sql);
        assert_eq!(idx.is_unique, c.is_unique, "is_unique for `{}`", c.sql);
        assert_eq!(
            idx.fields[0].name, c.field_name,
            "indexed field for `{}`",
            c.sql
        );
        assert_eq!(
            idx.name,
            c.index_name.map(String::from),
            "index name for `{}`",
            c.sql
        );
    }
}
