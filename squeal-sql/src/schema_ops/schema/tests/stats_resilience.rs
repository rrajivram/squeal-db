// Persistence versioning Stage 8: the deliberate stats exception. Persisted
// table stats are a rebuildable cache, so a row that cannot be decoded is
// discarded (that table starts fresh) instead of making the schema — and so
// the database — fail to load. See SchemaStats::load.
use store::cursor::Cursor;
use store::named_memfile::NamedMemFile;
use store::tuple::{DBIdType, Tuple};
use store::valueitem::ValueItem;

use super::*;

type NamedSchema = Arc<Schema<NamedMemFile>>;

fn row_count(schema: &NamedSchema, table: &SqlTable) -> Option<usize> {
    schema
        .stats
        .lock()
        .as_ref()
        .and_then(|s| s.get_table_stats(table.db_table_id))
        .map(|t| t.row_count)
}

fn stats_key(table: &SqlTable) -> DBIdType {
    DBIdType::Int(table.db_table_id.as_u64())
}

fn stats_row_bytes(schema: &NamedSchema, table: &SqlTable) -> Vec<u8> {
    let mut cur = schema.db.table_scan(schema.stats_table_id).unwrap();
    while let Some(t) = cur.next().unwrap() {
        if *t.id() == stats_key(table) {
            return t.data().to_vec();
        }
    }
    panic!("no persisted stats row for {:?}", table.name);
}

fn overwrite_stats_row(schema: &NamedSchema, table: &SqlTable, bytes: &[u8]) {
    let tx = schema.db.begin().unwrap();
    schema
        .db
        .update(
            schema.stats_table_id,
            Tuple::new_with(stats_key(table), bytes, Some(tx.id()), None),
            &tx,
        )
        .unwrap();
    schema.db.commit(tx).unwrap();
}

// What a real close + reopen does: flush_metadata first (a catalog row is
// written before its table's store id is assigned and only corrected at
// flush), then load fresh from what is on disk.
fn reload(schema: &NamedSchema) -> NamedSchema {
    schema.flush_metadata().unwrap();
    Schema::<NamedMemFile>::load(DEFAULT_SCHEMA_NAME.to_string(), schema.db.clone()).unwrap()
}

// Two tables (3 and 2 rows) whose stats have been analyzed and persisted.
fn setup(tag: &str) -> (String, Arc<Database<NamedMemFile>>, NamedSchema, Arc<SqlTable>, Arc<SqlTable>) {
    let path = temp_schema_path(tag);
    NamedMemFile::delete(&path);
    let db = Database::<NamedMemFile>::create(path.clone()).unwrap();
    let s = db.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    for name in ["a", "b"] {
        create_table_directly(
            &s,
            &format!("create table {name} (id integer not null, v integer, primary key(id))"),
        );
    }
    let rows = |n: i64| (1..=n).map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(i)]).collect();
    s.insert_rows("a", rows(3), None).unwrap();
    s.insert_rows("b", rows(2), None).unwrap();
    s.analyze_table("a").unwrap();
    s.analyze_table("b").unwrap();
    s.stats.lock().as_ref().unwrap().persist(s.stats_table_id).unwrap();
    let (a, b) = (s.get_table("a").unwrap(), s.get_table("b").unwrap());
    (path, db, s, a, b)
}

fn finish(path: String, db: Arc<Database<NamedMemFile>>, schemas: Vec<NamedSchema>) {
    for s in &schemas {
        s.persist_and_shutdown_stats().unwrap();
    }
    drop(schemas);
    db.close().unwrap();
    NamedMemFile::delete(&path);
}

#[test]
fn test_valid_persisted_stats_are_restored_on_load() {
    let (path, db, s, a, b) = setup("stats_res_control");
    let s2 = reload(&s);
    assert_eq!(row_count(&s2, &a), Some(3));
    assert_eq!(row_count(&s2, &b), Some(2));
    finish(path, db, vec![s, s2]);
}

#[test]
fn test_an_undecodable_stats_row_is_discarded_and_only_that_table_restarts() {
    let (path, db, s, a, b) = setup("stats_res_garbage");
    overwrite_stats_row(&s, &a, &[0xFF, 0xFF, 0xFF]);
    let s2 = reload(&s); // must NOT fail
    assert_eq!(row_count(&s2, &a), Some(0), "the bad row's table starts fresh");
    assert_eq!(row_count(&s2, &b), Some(2), "a good neighbor keeps its stats");
    finish(path, db, vec![s, s2]);
}

#[test]
fn test_a_stats_row_stored_under_the_wrong_key_is_not_trusted() {
    // Postcard is positional, so a changed shape can decode "successfully"
    // into nonsense; a row that claims a different table than its key is the
    // cheap tell, and is treated like any other unreadable row.
    let (path, db, s, a, b) = setup("stats_res_wrong_key");
    let b_bytes = stats_row_bytes(&s, &b);
    overwrite_stats_row(&s, &a, &b_bytes);
    let s2 = reload(&s);
    assert_eq!(row_count(&s2, &a), Some(0));
    assert_eq!(row_count(&s2, &b), Some(2));
    finish(path, db, vec![s, s2]);
}

// The real thing: a database whose stats row is garbage on disk still opens.
#[test]
fn test_a_database_with_an_unreadable_stats_row_still_opens() {
    let (path, db, s, a, _b) = setup("stats_res_reopen");
    let (a_key, stats_name) = (
        stats_key(&a),
        Schema::<NamedMemFile>::stats_table_name(DEFAULT_SCHEMA_NAME),
    );
    drop(s);
    db.close().unwrap();

    let raw = store::db::Db::<NamedMemFile>::open(&path).unwrap();
    let stats_table = raw.table_id_by_name(&stats_name).unwrap().unwrap();
    let tx = raw.begin().unwrap();
    raw.update(
        stats_table,
        Tuple::new_with(a_key, &[0xDE, 0xAD, 0xBE, 0xEF], Some(tx.id()), None),
        &tx,
    )
    .unwrap();
    raw.commit(tx).unwrap();
    raw.close().unwrap();

    let db2 = Database::<NamedMemFile>::open(path.clone()).unwrap();
    let s2 = db2.get_schema(DEFAULT_SCHEMA_NAME).unwrap();
    assert!(s2.table_exists("a"), "the database opened and its tables are intact");
    assert_eq!(row_count(&s2, &a), Some(0));
    finish(path, db2, vec![s2]);
}
