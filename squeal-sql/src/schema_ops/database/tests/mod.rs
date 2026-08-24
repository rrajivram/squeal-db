use std::sync::Arc;

use store::db::DBFile;
use store::memfile::MemFile;

use super::Database;
use crate::schema_ops::schema::Schema;
use crate::table::SqlTable;

// These tests are exercising Database/Schema's own multi-schema
// isolation and persistence guarantees, not statement dispatch (see
// stmt.rs's own tests for that) — so they call Schema::create_table
// directly rather than going through Connection+Statement.
fn create_table_directly<F>(s: &Arc<Schema<F>>, sql: &str)
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    let stmt = sql_parser::parse_one(sql).unwrap();
    let sql_parser::Statement::CreateTable(c) = &stmt else {
        panic!("expected a CREATE TABLE statement, got: {stmt:?}");
    };
    s.create_table(SqlTable::from_sql(s, c.clone()).unwrap())
        .unwrap();
}

fn temp_db_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("squeal_sql_database_test_{tag}_{}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_create_auto_creates_default_schema() {
    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
    assert!(db.schema_exists("default"));
    db.get_schema("default").unwrap();
}

#[test]
fn test_create_schema_rejects_duplicate_name() {
    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
    let err = db.create_schema("default").unwrap_err();
    assert!(matches!(err, crate::error::SchemaError::SchemaInUseError(_)));
}

#[test]
fn test_get_schema_missing_returns_schema_not_found() {
    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
    let err = db.get_schema("nope").unwrap_err();
    assert!(matches!(err, crate::error::SchemaError::SchemaNotFound(_)));
}

#[test]
fn test_tables_in_different_schemas_do_not_collide() {
    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
    let s1 = db.create_schema("schema1").unwrap();
    let s2 = db.create_schema("schema2").unwrap();

    create_table_directly(
        &s1,
        "create table customers (id integer not null, primary key(id))",
    );
    create_table_directly(
        &s2,
        "create table customers (id integer not null, name varchar(50))",
    );

    assert!(s1.table_exists("customers"));
    assert!(s2.table_exists("customers"));
    assert_eq!(s1.get_table("customers").unwrap().fields().len(), 1);
    assert_eq!(s2.get_table("customers").unwrap().fields().len(), 2);
    // s1's "customers" has a primary key index (a real store-level table
    // qualified as "schema1.customers0"); s2's same-named table has none
    // — proves the index name was qualified per-schema too, not just the
    // table's own metadata row.
    assert_eq!(s1.get_table("customers").unwrap().indices.len(), 1);
    assert_eq!(s2.get_table("customers").unwrap().indices.len(), 0);
}

#[test]
fn test_database_state_survives_close_and_reopen() {
    use store::named_memfile::NamedMemFile;

    let path = temp_db_path("close_reopen");
    NamedMemFile::delete(&path);

    let db = super::Database::<NamedMemFile>::create(path.clone()).unwrap();
    let s1 = db.create_schema("schema1").unwrap();
    create_table_directly(&s1, "create table t (id integer not null, primary key(id))");
    // Database::close requires unique ownership of its shared Db<F> —
    // this test's own `s1` clone (on top of the one Database itself
    // holds) must be dropped first.
    drop(s1);
    db.close().unwrap();

    let db2 = super::Database::<NamedMemFile>::open(path.clone()).unwrap();
    // "default" is auto-loaded on open...
    assert!(db2.schema_exists("default"));
    // ...while "schema1" wasn't explicitly created via Database::open, so
    // it must still be discoverable on demand, not just "default".
    let s1_reopened = db2.get_schema("schema1").unwrap();
    assert!(s1_reopened.table_exists("t"));
    assert_eq!(s1_reopened.get_table("t").unwrap().indices.len(), 1);

    NamedMemFile::delete(&path);
}

fn count_schema_registry_rows(db: &Database<MemFile>, name: &str) -> usize {
    use store::cursor::Cursor;
    let mut cursor = db.db.table_scan(db.schemas_table).unwrap();
    let mut count = 0;
    while let Some(tuple) = cursor.next().unwrap() {
        let stored: String = postcard::from_bytes(tuple.data()).unwrap();
        if stored == name {
            count += 1;
        }
    }
    count
}

// create_schema's own up-front checks (in-memory map + table_id_by_name)
// are check-then-act, not atomic against a concurrent racer — the real
// guarantee has to come from lower down: store's own table-name
// validation (for Schema::create's backing table) and duplicate-key
// detection (for the schemas_table row insert). This proves that
// guarantee holds end to end: exactly one of several racing
// create_schema("race") calls wins, and nothing is left half-created —
// no duplicate registry row, no missing/duplicate backing table.
#[test]
fn test_concurrent_create_schema_race_leaves_exactly_one_consistent_schema() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    for _ in 0..5 {
        let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
        let n = 8;
        let barrier = Arc::new(Barrier::new(n));
        let handles: Vec<_> = (0..n)
            .map(|_| {
                let db = db.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    db.create_schema("race")
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let successes = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(successes, 1, "exactly one racing create_schema call must win");

        assert!(db.schema_exists("race"));
        assert_eq!(
            count_schema_registry_rows(&db, "race"),
            1,
            "the losing racers' row inserts must not leave duplicate registry rows"
        );
        assert!(
            db.db
                .table_id_by_name(Schema::<MemFile>::system_table_name("race"))
                .unwrap()
                .is_some(),
            "the winner's own backing system table must exist"
        );
    }
}

// A more targeted, deterministic version of the race above: simulates a
// schemas_table row that already exists for a name without that
// schema's own backing table having been created (e.g. what a crash
// between the two could leave behind) — create_schema's row insert must
// fail on the duplicate key, and must not go on to half-create anything.
#[test]
fn test_create_schema_rejects_a_preexisting_registry_row_without_side_effects() {
    use postcard::to_allocvec;
    use store::tuple::{DBIdType, Tuple};
    use store::valueitem::{IndexKey, ValueItem};

    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();

    let txn = db.db.begin().unwrap();
    let ik = IndexKey::new_from(&[ValueItem::Str((
        "ghost".to_string(),
        crate::constant::MAX_TABLE_NAME_LEN as u32,
    ))])
    .unwrap();
    db.db
        .insert(
            db.schemas_table,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &to_allocvec(&"ghost".to_string()).unwrap(),
                Some(txn.id()),
                None,
            ),
            &txn,
        )
        .unwrap();
    db.db.commit(txn).unwrap();

    let err = match db.create_schema("ghost") {
        Err(e) => e,
        Ok(_) => panic!("expected a duplicate-key error from the row insert"),
    };
    assert!(matches!(err, crate::error::SchemaError::DuplicateKey(_)));

    assert!(
        !db.schema_exists("ghost"),
        "the in-memory map must not gain a half-created entry"
    );
    assert!(
        db.db
            .table_id_by_name(Schema::<MemFile>::system_table_name("ghost"))
            .unwrap()
            .is_none(),
        "Schema::create must never have run — the row insert failed first"
    );
}

// Database::close requires unique ownership of its shared Db<F> (see its
// own doc comment) — a live sibling reference must make it fail cleanly,
// not partially tear the Database down.
#[test]
fn test_close_with_other_references_fails_without_corrupting_database() {
    let db = Database::<MemFile>::create("test_db".to_string()).unwrap();
    let db_extra = db.clone();

    let err = match db.close() {
        Err(e) => e,
        Ok(_) => panic!("expected close() to fail while another Arc<Database> reference exists"),
    };
    assert!(matches!(err, crate::error::SchemaError::UnknownError(_)));

    // The other live reference must still be a fully working Database —
    // the failed close() must not have torn anything down.
    assert!(db_extra.schema_exists("default"));
    db_extra.create_schema("still_works").unwrap();
    assert!(db_extra.schema_exists("still_works"));
}

#[test]
fn test_open_tolerates_a_missing_default_schema() {
    use store::named_memfile::NamedMemFile;

    // Nothing today lets a caller actually drop a schema, so this builds
    // a database the same way Database::create would, minus the
    // "default" schema — simulating one that's missing out-of-band —
    // bypassing Database::create (which always creates one).
    let path = temp_db_path("open_no_default");
    NamedMemFile::delete(&path);
    {
        let raw = store::db::Db::<NamedMemFile>::create(&path).unwrap();
        raw.create_table(crate::constant::SYSTEM_SCHEMAS_TABLE.to_string())
            .unwrap();
        raw.close().unwrap();
    }

    // Must still open successfully — loading "default" is best-effort,
    // not a hard requirement of a successful open().
    let db = Database::<NamedMemFile>::open(path.clone()).unwrap();
    assert!(!db.schema_exists("default"));
    let err = db.get_schema("default").unwrap_err();
    assert!(matches!(
        err,
        crate::error::SchemaError::SchemaNotFound(_)
    ));

    NamedMemFile::delete(&path);
}
