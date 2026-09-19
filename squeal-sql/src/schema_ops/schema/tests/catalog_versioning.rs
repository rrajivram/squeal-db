// Persistence versioning Stage 7: the SQL catalog row (`SqlTable` and the
// structs nested in it) is stored as a versioned envelope, and a row written
// before that envelope existed must still load. See table.rs's
// `encode_catalog_row` / `decode_catalog_row`.
use store::named_memfile::NamedMemFile;
use store::valueitem::ValueItem;

use super::*;

// A table exercising everything a catalog row holds: a composite primary key,
// a UNIQUE index, a foreign key, column defaults of several types, and a
// two-version schema history (an ALTER ADD COLUMN with a default).
fn rich_table_sql() -> [&'static str; 4] {
    [
        "create table parents (pid integer not null, primary key(pid))",
        "create table rich (a integer not null, b varchar(10) not null, \
         note varchar(20) default 'n/a', score double default 1.5, flag boolean default true, \
         parent integer references parents(pid), code varchar(8) not null, \
         primary key(a, b), unique(code))",
        "alter table rich add column added integer default 7",
        "alter table rich add column late varchar(6)",
    ]
}

fn build_rich_table() -> Arc<SqlTable> {
    let c = conn();
    for sql in rich_table_sql() {
        execute(&c, sql).unwrap();
    }
    c.current_schema().unwrap().get_table("rich").unwrap()
}

// Captured from the pre-envelope encoder (raw postcard of `SqlTable`, no tag)
// by `build_rich_table` above, at the commit that introduced the envelope.
// This is exactly what a catalog row written by any earlier build looks
// like. DO NOT edit; it is the proof that old catalogs still load.
const LEGACY_RICH_ROW_HEX: &str =
    "04726963680307000161000000010162030a000002046e6f7465031401010403\
     6e2f6114030573636f726501010102000000000000f83f0404666c6167070101\
     06010506706172656e740001000604636f646503080000080001610000000101\
     62030a000002046e6f74650314010104036e2f6114030573636f726501010102\
     000000000000f83f0404666c616707010106010506706172656e740001000604\
     636f64650308000007056164646564000101010e09000161000000010162030a\
     000002046e6f74650314010104036e2f6114030573636f726501010102000000\
     000000f83f0404666c616707010106010506706172656e740001000604636f64\
     650308000007056164646564000101010e08046c617465030601000200060101\
     02000161000000010162030a000000070001010604636f646503080000010006\
     706172656e7407706172656e7473037069640509";

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn test_legacy_catalog_row_fixture_decodes_with_current_code() {
    let t = SqlTable::decode_catalog_row(&unhex(LEGACY_RICH_ROW_HEX)).unwrap();
    assert_eq!(t.name, "rich");
    assert_eq!(t.versions.len(), 3, "CREATE + two ALTER ADD COLUMN");
    assert_eq!(t.indices.len(), 2, "composite primary key + unique(code)");
    assert_eq!(t.foreign_keys.len(), 1);
    assert_eq!(t.foreign_keys[0].ref_table, "parents");
    let f = |name: &str| field(&t, name).clone();
    assert_eq!(f("note").default, Some(ValueItem::Str(("n/a".into(), 20))));
    assert_eq!(f("score").default, Some(ValueItem::Double(1.5)));
    assert_eq!(f("flag").default, Some(ValueItem::Boolean(true)));
    assert_eq!(f("added").default, Some(ValueItem::Integer(7)));
    assert_eq!(f("late").default, None);
    assert!(f("late").nullable);
    assert_eq!(t.fields().len(), 9);
}

#[test]
fn test_the_catalog_row_body_shape_is_byte_stable() {
    // Decode the legacy bytes and re-encode the body: it must reproduce the
    // captured bytes exactly. Fails the moment any nested struct (or a
    // ValueItem/DataType inside one) changes its wire shape without a frozen
    // copy of the old shape being kept.
    let legacy = unhex(LEGACY_RICH_ROW_HEX);
    let t = SqlTable::decode_catalog_row(&legacy).unwrap();
    assert_eq!(postcard::to_allocvec(&t).unwrap(), legacy);
}

#[test]
fn test_new_catalog_rows_are_enveloped_and_round_trip() {
    let t = build_rich_table();
    let row = t.encode_catalog_row().unwrap();
    assert_eq!(row[0], 0x00, "discriminator against legacy rows");
    assert_eq!(u16::from_le_bytes([row[1], row[2]]), crate::table::CATALOG_ROW_VERSION);
    let back = SqlTable::decode_catalog_row(&row).unwrap();
    assert_eq!(back.name, "rich");
    assert_eq!(back.versions.len(), t.versions.len());
    assert_eq!(back.db_table_id, t.db_table_id);
    // The body is byte-for-byte what a legacy row holds.
    assert_eq!(&row[3..], &postcard::to_allocvec(&*t).unwrap()[..]);
}

#[test]
fn test_a_legacy_row_never_starts_with_the_envelope_discriminator() {
    // The envelope's leading 0x00 is only unambiguous because a legacy row
    // starts with the name's length varint, which is 0 only for an empty
    // name — and an empty name is refused at encode time.
    let legacy = unhex(LEGACY_RICH_ROW_HEX);
    assert_ne!(legacy[0], 0x00);
    let mut t = SqlTable::decode_catalog_row(&legacy).unwrap();
    t.name = String::new();
    let err = t.encode_catalog_row().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_a_catalog_row_with_an_unknown_version_is_refused() {
    let mut row = vec![0x00, 99, 0];
    row.extend_from_slice(&unhex(LEGACY_RICH_ROW_HEX));
    let err = SqlTable::decode_catalog_row(&row).unwrap_err();
    assert!(err.to_string().contains("unsupported catalog row version 99"), "got {err}");
}

#[test]
fn test_truncated_and_empty_catalog_rows_are_errors_not_panics() {
    assert!(SqlTable::decode_catalog_row(&[]).is_err());
    assert!(SqlTable::decode_catalog_row(&[0x00]).is_err());
    assert!(SqlTable::decode_catalog_row(&[0x00, 1]).is_err());
    assert!(SqlTable::decode_catalog_row(&[0x00, 1, 0, 0xFF]).is_err());
}

// The real loader, end to end: a schema whose system table holds a LEGACY
// row (as an earlier build wrote it) loads, and rewriting the metadata
// upgrades that row to the envelope without changing what loads.
#[test]
fn test_schema_loads_a_legacy_catalog_row_and_upgrades_it_on_flush() {
    use store::cursor::Cursor;
    use store::tuple::{DBIdType, Tuple};
    let path = temp_schema_path("stage7_legacy_catalog_row");
    NamedMemFile::delete(&path);

    let db = Database::<NamedMemFile>::create(path.clone()).unwrap();
    let s = db.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    create_table_directly(&s, "create table users (id integer not null, primary key(id))");
    let table = s.get_table("users").unwrap();

    let key = || {
        DBIdType::Rec(
            store::valueitem::IndexKey::new_from(&[ValueItem::Str((
                "users".into(),
                crate::constant::MAX_TABLE_NAME_LEN as u32,
            ))])
            .unwrap(),
        )
    };
    let raw_first_byte = |s: &Arc<Schema<NamedMemFile>>| {
        let mut cur = s.db.table_scan(s.sys_table_id).unwrap();
        cur.next().unwrap().unwrap().data()[0]
    };
    // Overwrite the row with exactly what the pre-envelope code wrote.
    let tx = s.db.begin().unwrap();
    s.db.update(
        s.sys_table_id,
        Tuple::new_with(key(), &postcard::to_allocvec(&*table).unwrap(), Some(tx.id()), None),
        &tx,
    )
    .unwrap();
    s.db.commit(tx).unwrap();
    assert_ne!(raw_first_byte(&s), 0x00, "the row is now in the legacy layout");

    let s2 = Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), s.db.clone()).unwrap();
    let loaded = s2.get_table("users").expect("a legacy catalog row must still load");
    assert_eq!(loaded.name, "users");
    assert_eq!(loaded.db_table_id, table.db_table_id);
    assert_eq!(loaded.fields().len(), 1);

    s2.flush_metadata().unwrap();
    assert_eq!(raw_first_byte(&s2), 0x00, "flush rewrites the row in the envelope");
    let s3 = Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), s.db.clone()).unwrap();
    assert_eq!(s3.get_table("users").unwrap().db_table_id, table.db_table_id);

    for schema in [&s2, &s3] {
        schema.persist_and_shutdown_stats().unwrap();
    }
    drop((s, s2, s3, table));
    db.close().unwrap();
    NamedMemFile::delete(&path);
}
