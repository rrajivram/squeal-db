//! Rows read in place (SqlTable::decode_row): a table scan, seek or index
//! lookup copies out only the columns the query reads, and leaves the rest
//! NULL. What reads whole rows — UPDATE, DELETE, `select *` — still gets
//! them whole.

use super::*;
use crate::table::VersionedRow;
use store::valueitem::{IndexKey, ValueItem};

// `name` is UNIQUE; `note` is in no index, so a query reading it reads the
// table (or looks rows up from `t_name`).
fn setup() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(20), city varchar(20), \
         note varchar(200), primary key(id))",
    )
    .unwrap();
    run(&c, "create unique index t_name on t (name)").unwrap();
    let rows = (0..50)
        .map(|i| format!("({i}, 'name{i:02}', 'city{}', 'note {i}')", i % 3))
        .collect::<Vec<_>>()
        .join(", ");
    run(&c, &format!("insert into t values {rows}")).unwrap();
    run(&c, "analyze table t").unwrap();
    c
}

fn rows(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    select_rows(c, sql).1
}

fn s(v: &str, cap: u32) -> ValueItem {
    ValueItem::Str((v.into(), cap))
}

#[test]
fn test_a_table_scan_reads_the_columns_the_query_uses() {
    let c = setup();
    let sql = "select note, id from t where city = 'city1' and note < 'note 8' and note > 'note ' \
               order by id limit 3";
    assert!(explain(&c, sql).contains("TableScan t"), "{}", explain(&c, sql));
    assert_eq!(
        rows(&c, sql),
        vec![
            vec![s("note 1", 200), ValueItem::Integer(1)],
            vec![s("note 4", 200), ValueItem::Integer(4)],
            vec![s("note 7", 200), ValueItem::Integer(7)],
        ]
    );
    assert_eq!(
        rows(&c, "select count(*) from t where note like 'note 4%'"),
        vec![vec![ValueItem::Integer(11)]]
    );
}

#[test]
fn test_a_table_seek_and_an_index_lookup_read_the_columns_the_query_uses() {
    let c = setup();
    let sql = "select note from t where id = 9";
    assert!(explain(&c, sql).contains("TableSeek t"), "{}", explain(&c, sql));
    assert_eq!(rows(&c, sql), vec![vec![s("note 9", 200)]]);

    let sql = "select city, note from t where name = 'name12'";
    assert!(explain(&c, sql).contains("using t_name"), "{}", explain(&c, sql));
    assert_eq!(rows(&c, sql), vec![vec![s("city0", 20), s("note 12", 200)]]);
    assert_eq!(
        rows(&c, "select * from t where name = 'name12'"),
        vec![vec![
            ValueItem::Integer(12),
            s("name12", 20),
            s("city0", 20),
            s("note 12", 200)
        ]]
    );
}

// UPDATE and DELETE read whole rows: an UPDATE writes back every column,
// so one it didn't read must still come through.
#[test]
fn test_update_and_delete_keep_the_columns_they_do_not_name() {
    let c = setup();
    run(&c, "update t set city = 'moved' where note = 'note 5'").unwrap();
    assert_eq!(
        rows(&c, "select * from t where id = 5"),
        vec![vec![
            ValueItem::Integer(5),
            s("name05", 20),
            s("moved", 20),
            s("note 5", 200)
        ]]
    );
    run(&c, "delete from t where note = 'note 6'").unwrap();
    assert_eq!(
        rows(&c, "select count(*) from t"),
        vec![vec![ValueItem::Integer(49)]]
    );
}

// A row written before an ALTER TABLE is decoded whole and reprojected
// (see SqlTable::reproject); one written after, in place. Both read alike.
#[test]
fn test_rows_from_before_and_after_an_alter_read_alike() {
    let c = setup();
    run(&c, "alter table t add column plan varchar(10) default 'free'").unwrap();
    run(&c, "alter table t drop column city").unwrap();
    run(&c, "insert into t values (100, 'name100', 'note 100', 'paid')").unwrap();
    assert_eq!(
        rows(&c, "select plan, note from t where id in (3, 100) order by id"),
        vec![
            vec![s("free", 10), s("note 3", 200)],
            vec![s("paid", 10), s("note 100", 200)],
        ]
    );
}

// decode_row reads the bytes VersionedRow's Serialize writes: the values
// were serialized as a Vec<u8>, which postcard lays out as it does bytes.
#[test]
fn test_decode_row_reads_what_versioned_row_writes() {
    let c = setup();
    let table = c.current_schema().unwrap().get_table("t").unwrap();
    let values = vec![
        ValueItem::Integer(7),
        s("name07", 20),
        ValueItem::Null,
        s("a note", 200),
    ];
    let bytes = postcard::to_allocvec(&VersionedRow {
        version: table.version(),
        values: IndexKey::new_from(&values).unwrap(),
    })
    .unwrap();
    assert_eq!(table.decode_row(&bytes, None).unwrap().values(), &values[..]);
    assert_eq!(
        table
            .decode_row(&bytes, Some(&[false, true, false, true]))
            .unwrap()
            .values(),
        &[ValueItem::Null, s("name07", 20), ValueItem::Null, s("a note", 200)]
    );
    let row: VersionedRow = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(row.values.values(), &values[..]);
    // Cut short anywhere: an error, not a panic.
    for cut in 0..bytes.len() {
        assert!(table.decode_row(&bytes[..cut], None).is_err(), "cut at {cut}");
    }
}

// A row is written fixed-width (see table.rs's RowLayout): the same bytes
// long whatever its strings' lengths and wherever its NULLs are, each
// column where the schema says, read without the others.
#[test]
fn test_rows_are_fixed_width_and_read_by_column() {
    let c = setup();
    let table = c.current_schema().unwrap().get_table("t").unwrap();
    let key = |v: Vec<ValueItem>| IndexKey::new_from(&v).unwrap();
    let full = vec![ValueItem::Integer(7), s("name07", 20), s("c", 20), s("a note", 200)];
    let sparse = vec![ValueItem::Integer(8), ValueItem::Null, ValueItem::Null, s("", 200)];
    let full_bytes = table.encode_row(&key(full.clone())).unwrap();
    let sparse_bytes = table.encode_row(&key(sparse.clone())).unwrap();
    assert_eq!(full_bytes.len(), sparse_bytes.len());
    assert_eq!(table.decode_row(&full_bytes, None).unwrap().values(), &full[..]);
    assert_eq!(table.decode_row(&sparse_bytes, None).unwrap().values(), &sparse[..]);
    assert_eq!(
        table
            .decode_row(&full_bytes, Some(&[false, false, true, true]))
            .unwrap()
            .values(),
        &[ValueItem::Null, ValueItem::Null, s("c", 20), s("a note", 200)]
    );
    // A string comes back with its column's capacity, whatever it was
    // written with.
    let odd = vec![ValueItem::Integer(9), s("n", 3), ValueItem::Null, s("x", 1)];
    let odd_bytes = table.encode_row(&key(odd)).unwrap();
    assert_eq!(odd_bytes.len(), full_bytes.len());
    assert_eq!(
        table.decode_row(&odd_bytes, None).unwrap().values(),
        &[ValueItem::Integer(9), s("n", 20), ValueItem::Null, s("x", 200)]
    );
    // A reader of the old layout refuses such a row rather than misread
    // the NULLs' padding; cut short anywhere, it is an error.
    assert!(postcard::from_bytes::<VersionedRow>(&sparse_bytes).is_err());
    for cut in 0..sparse_bytes.len() {
        assert!(table.decode_row(&sparse_bytes[..cut], None).is_err(), "cut at {cut}");
    }
}

// A value that can't take its column's width — here a string longer than
// the column, declared with room for it — leaves the row written the old
// way, each value its own size, and it reads back all the same.
#[test]
fn test_a_row_that_does_not_fit_its_columns_is_written_the_old_way() {
    let c = setup();
    let table = c.current_schema().unwrap().get_table("t").unwrap();
    let long = "x".repeat(30);
    let values = vec![ValueItem::Integer(1), s(&long, 40), ValueItem::Null, s("n", 200)];
    let bytes = table.encode_row(&IndexKey::new_from(&values).unwrap()).unwrap();
    assert_eq!(table.decode_row(&bytes, None).unwrap().values(), &values[..]);
    assert_eq!(postcard::from_bytes::<VersionedRow>(&bytes).unwrap().values.values(), &values[..]);
}
