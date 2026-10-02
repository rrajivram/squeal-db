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
    let v1 = crate::table::SqlTableV1Shape::from_table(&t);
    assert_eq!(postcard::to_allocvec(&v1).unwrap(), legacy);
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
    assert_eq!(back.rows_tree(), t.rows_tree());
    assert_eq!(back.partitions, t.partitions);
    // The body is the live shape's own postcard.
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
        Tuple::new_with(
            key(),
            &postcard::to_allocvec(&crate::table::SqlTableV1Shape::from_table(&table)).unwrap(),
            Some(tx.id()),
            None,
        ),
        &tx,
    )
    .unwrap();
    s.db.commit(tx).unwrap();
    assert_ne!(raw_first_byte(&s), 0x00, "the row is now in the legacy layout");

    let s2 = Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), s.db.clone()).unwrap();
    let loaded = s2.get_table("users").expect("a legacy catalog row must still load");
    assert_eq!(loaded.name, "users");
    assert_eq!(loaded.rows_tree(), table.rows_tree());
    assert_eq!(loaded.id, table.id);
    assert_eq!(loaded.fields().len(), 1);

    s2.flush_metadata().unwrap();
    assert_eq!(raw_first_byte(&s2), 0x00, "flush rewrites the row in the envelope");
    let s3 = Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), s.db.clone()).unwrap();
    assert_eq!(s3.get_table("users").unwrap().rows_tree(), table.rows_tree());

    for schema in [&s2, &s3] {
        schema.persist_and_shutdown_stats().unwrap();
    }
    drop((s, s2, s3, table));
    db.close().unwrap();
    NamedMemFile::delete(&path);
}

// Version 2: the same pinning for the shape partitions introduced. A table
// with everything version 2 added: a LIST partitioning with a DEFAULT, a
// string and a NULL among its values, an index (so each partition has an
// index tree), and a partition added later (so ids are not just positions).
fn partitioned_table_sql() -> [&'static str; 3] {
    [
        "create table parted (id integer not null, region varchar(4) not null, n integer, \
         primary key(id, region)) \
         partition by list (region) (partition west values in ('ca', 'wa'), \
         partition nulls values in (null), partition tmp values in ('zz'), \
         partition other default)",
        "alter table parted drop partition tmp",
        "alter table parted add partition east values in ('ny')",
    ]
}

// Captured from `partitioned_table_sql` at the commit that introduced
// version 2: the body of its catalog row (after the 3-byte envelope).
// DO NOT edit; it is the proof that version-2 catalogs still load.
const V2_PARTITIONED_BODY_HEX: &str =
    "067061727465640103000269640000000106726567696f6e0304000002016e00\
     01000100010102000269640000000106726567696f6e03040000000303010101\
     040004776573740302040263610404027761040003010401056e756c6c730301\
     000005010603056f74686572040009010a040465617374030104026e7904000b\
     010c05";

fn build_partitioned_table() -> Arc<SqlTable> {
    let c = conn();
    for sql in partitioned_table_sql() {
        execute(&c, sql).unwrap();
    }
    c.current_schema().unwrap().get_table("parted").unwrap()
}

#[test]
fn test_the_version_2_catalog_row_fixture_decodes_and_its_shape_is_byte_stable() {
    use crate::partition::{PartitionBound, PartitionKind};
    let mut row = vec![0x00, 2, 0];
    row.extend_from_slice(&unhex(V2_PARTITIONED_BODY_HEX));
    let t = SqlTable::decode_catalog_row(&row).unwrap();
    assert_eq!(t.name, "parted");
    assert_eq!(t.partitioning.as_ref().unwrap().kind, PartitionKind::List);
    assert_eq!(t.partition_column().unwrap().1.name, "region");
    let parts: Vec<(&str, u32)> = t.partitions.iter().map(|p| (p.name.as_str(), p.id)).collect();
    assert_eq!(parts, [("west", 0), ("nulls", 1), ("other", 3), ("east", 4)]);
    assert_eq!(t.next_partition_id, 5);
    assert_eq!(
        t.partitions[0].bound,
        PartitionBound::In(vec![
            ValueItem::Str(("ca".into(), 4)),
            ValueItem::Str(("wa".into(), 4))
        ])
    );
    assert_eq!(t.partitions[1].bound, PartitionBound::In(vec![ValueItem::Null]));
    assert_eq!(t.partitions[2].bound, PartitionBound::Default);
    assert_eq!(t.id, t.partitions[0].rows(), "keyed by its first partition's rows tree");
    for p in &t.partitions {
        assert_ne!(p.rows(), store::table::TableIdType::none());
        assert_ne!(p.index(0), store::table::TableIdType::none());
    }
    // Re-encoding reproduces the captured bytes exactly.
    assert_eq!(t.encode_catalog_row().unwrap(), row);
    // And it is what the same statements produce today.
    assert_eq!(build_partitioned_table().encode_catalog_row().unwrap(), row);
}

// The real loader, end to end: a partitioned table, its partitions' trees
// and the rows in them are all there after loading the schema from disk
// with no clean close.
#[test]
fn test_a_partitioned_table_loads_from_disk_with_its_partitions_and_rows() {
    let path = temp_schema_path("partitioned_table_durable");
    NamedMemFile::delete(&path);
    let db = Database::<NamedMemFile>::create(path.clone()).unwrap();
    let s = db.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    create_table_directly(
        &s,
        "create table p (id integer not null, k integer not null, primary key(id, k)) \
         partition by range (k) (partition a values less than (10), \
         partition b values less than (20))",
    );
    let int = ValueItem::Integer;
    s.insert_rows("p", vec![vec![int(1), int(5)], vec![int(2), int(15)]], None)
        .unwrap();
    let stmt = sql_parser::parse_sql("alter table p add partition c values less than (30)")
        .unwrap()
        .remove(0);
    let sql_parser::Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    let sql_parser::ddl::AlterTableOp::AddPartition(_, def) = &alter.operation else {
        panic!("expected ADD PARTITION");
    };
    s.add_partition("p", def).unwrap();
    s.insert_rows("p", vec![vec![int(3), int(25)]], None).unwrap();

    let reloaded = Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), s.db.clone())
        .unwrap();
    let live = s.get_table("p").unwrap();
    let disk = reloaded.get_table("p").unwrap();
    assert_eq!(disk.partitioning, live.partitioning);
    assert_eq!(disk.partitions, live.partitions);
    assert_eq!(disk.partitions.len(), 3);
    assert_eq!(disk.id, live.id);
    assert_eq!(disk.next_partition_id, 3);
    let mut rows = reloaded.select_all("p", None).unwrap().rows().to_vec();
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![int(1), int(5)], vec![int(2), int(15)], vec![int(3), int(25)]]
    );
    // And it still routes: a row for the added partition goes into it.
    reloaded.insert_rows("p", vec![vec![int(4), int(29)]], None).unwrap();
    let mut cursor = reloaded.db.table_scan(disk.partitions[2].rows()).unwrap();
    let mut n = 0;
    {
        use store::cursor::Cursor;
        while cursor.next().unwrap().is_some() {
            n += 1;
        }
    }
    assert_eq!(n, 2);

    for schema in [&s, &reloaded] {
        schema.persist_and_shutdown_stats().unwrap();
    }
    drop((s, reloaded, live, disk, cursor));
    db.close().unwrap();
    NamedMemFile::delete(&path);
}
