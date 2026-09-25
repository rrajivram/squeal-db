use store::valueitem::ValueItem;

use super::*;

fn select_rows(c: &Arc<Connection<MemFile>>, table_name: &str) -> Vec<Vec<ValueItem>> {
    let s = c.current_schema().unwrap();
    s.select_all(table_name, None).unwrap().rows().to_vec()
}

#[test]
fn test_infers_and_creates_the_table_then_loads_every_row() {
    let c = conn();
    let s = c.current_schema().unwrap();
    let content = "id,name,age,active\n1,alice,30,true\n2,bob,25,false\n";
    let (loaded, failed) = s.create_table_from_csv("t", content, false).unwrap();
    assert_eq!((loaded, failed), (2, 0));

    let table = s.get_table("t").unwrap();
    let fields = table.fields();
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].datatype, crate::datatype::DataType::Integer);
    assert!(!fields[0].nullable);
    assert!(matches!(fields[1].datatype, crate::datatype::DataType::Str(_)));
    assert_eq!(fields[2].datatype, crate::datatype::DataType::Integer);
    assert_eq!(fields[3].datatype, crate::datatype::DataType::Boolean);

    let mut rows = select_rows(&c, "t");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![
                ValueItem::Integer(1),
                ValueItem::Str(("alice".into(), 32)),
                ValueItem::Integer(30),
                ValueItem::Boolean(true),
            ],
            vec![
                ValueItem::Integer(2),
                ValueItem::Str(("bob".into(), 32)),
                ValueItem::Integer(25),
                ValueItem::Boolean(false),
            ],
        ]
    );
}

#[test]
fn test_a_column_with_an_empty_cell_becomes_nullable() {
    let c = conn();
    let s = c.current_schema().unwrap();
    let content = "id,note\n1,hi\n2,\n";
    s.create_table_from_csv("t", content, false).unwrap();
    let table = s.get_table("t").unwrap();
    assert!(!table.fields()[0].nullable);
    assert!(table.fields()[1].nullable);

    let mut rows = select_rows(&c, "t");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(rows[1][1], ValueItem::Null);
}

#[test]
fn test_rejects_creating_over_an_already_existing_table_without_if_not_exists() {
    let c = conn();
    let s = c.current_schema().unwrap();
    execute(&c, "create table t (id integer not null)").unwrap();
    let err = s
        .create_table_from_csv("t", "id\n1\n", false)
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_rejects_a_headerless_csv() {
    let c = conn();
    let s = c.current_schema().unwrap();
    let err = s.create_table_from_csv("t", "", false).unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    assert!(!s.table_exists("t"));
}
