use std::fs::File;

use store::memfile::MemFile;

use super::*;
use crate::datatype::DataType;

fn schema() -> Arc<Schema<MemFile>> {
    Schema::create("test_schema".to_string()).unwrap()
}

// Runs `sql` against a fresh in-memory schema and returns the table
// named `table_name` afterward — the create_table/from_sql helper
// methods used before this was wired up returned the built SqlTable
// directly; now that execute() persists (writes to the system table,
// commits, and only then updates the in-memory map) and returns just
// `()`, tests have to go back through the schema to see what actually
// landed.
fn create_and_fetch(sql: &str, table_name: &str) -> SqlTable {
    let s = schema();
    s.execute(sql.to_string()).unwrap();
    s.get_table(table_name)
        .unwrap_or_else(|| panic!("table {table_name:?} missing after successful execute()"))
}

fn field<'a>(t: &'a SqlTable, name: &str) -> &'a crate::table::Field {
    t.fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field named {name:?} in {t:#?}"))
}

fn temp_schema_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("squeal_sql_test_{tag}_{}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_create_table_extracts_name_and_field_count() {
    let t = create_and_fetch("create table users (id integer, name varchar(50))", "users");
    assert_eq!(t.name, "users");
    assert_eq!(t.fields.len(), 2);
}

#[test]
fn test_create_table_maps_integer_type() {
    let t = create_and_fetch("create table t (id integer)", "t");
    assert_eq!(field(&t, "id").datatype, DataType::Integer);
}

#[test]
fn test_create_table_maps_varchar_with_declared_length() {
    let t = create_and_fetch("create table t (name varchar(50))", "t");
    assert_eq!(field(&t, "name").datatype, DataType::Str(50));
}

#[test]
fn test_create_table_maps_text_type() {
    let t = create_and_fetch("create table t (bio text)", "t");
    assert_eq!(field(&t, "bio").datatype, DataType::Str(32));
}

#[test]
fn test_create_table_maps_double_type() {
    let t = create_and_fetch("create table t (price double)", "t");
    assert_eq!(field(&t, "price").datatype, DataType::Double);
}

#[test]
fn test_create_table_unrecognized_type_becomes_unsupported() {
    // Documents current behavior rather than asserting it's desirable:
    // sqlparser::ast::DataType::Boolean has no ValueItem counterpart yet,
    // so it silently falls through to Unsupported instead of erroring.
    let t = create_and_fetch("create table t (active boolean)", "t");
    assert_eq!(field(&t, "active").datatype, DataType::Unsupported);
}

#[test]
fn test_create_table_extracts_table_level_primary_key() {
    let t = create_and_fetch(
        "create table t (id integer not null, name varchar(50), primary key(id))",
        "t",
    );
    assert_eq!(t.indices.len(), 1);
    let idx = &t.indices[0];
    assert!(idx.is_primary);
    assert!(idx.is_unique);
    assert_eq!(idx.fields.len(), 1);
    assert_eq!(idx.fields[0].name, "id");
}

#[test]
fn test_create_table_rejects_nullable_primary_key_column() {
    // Columns are nullable by default (see the not-null tests below),
    // and TableBuilder::build rejects a nullable primary/unique key —
    // that validation was previously unreachable since nullable used to
    // be hardcoded false everywhere.
    let s = schema();
    let err = s
        .execute("create table t (id integer, primary key(id))".to_string())
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    assert!(!s.table_exists("t"), "a rejected create must not persist");
}

#[test]
fn test_create_table_extracts_table_level_unique_constraint() {
    let t = create_and_fetch(
        "create table t (id integer, email varchar(100) not null, unique(email))",
        "t",
    );
    assert_eq!(t.indices.len(), 1);
    let idx = &t.indices[0];
    assert!(!idx.is_primary);
    assert!(idx.is_unique);
    assert_eq!(idx.fields[0].name, "email");
}

#[test]
fn test_create_table_extracts_inline_column_level_primary_key() {
    let t = create_and_fetch("create table t (id integer not null primary key)", "t");
    assert_eq!(t.indices.len(), 1);
    let idx = &t.indices[0];
    assert!(idx.is_primary);
    assert!(idx.is_unique);
    assert_eq!(idx.fields.len(), 1);
    assert_eq!(idx.fields[0].name, "id");
}

#[test]
fn test_create_table_extracts_inline_column_level_unique() {
    let t = create_and_fetch("create table t (email varchar(100) not null unique)", "t");
    assert_eq!(t.indices.len(), 1);
    let idx = &t.indices[0];
    assert!(!idx.is_primary);
    assert!(idx.is_unique);
    assert_eq!(idx.fields[0].name, "email");
}

#[test]
fn test_create_table_rejects_field_datatype_over_4mb() {
    // Field::new's size cap must actually be reached from SQL parsing
    // now (previously bypassed: From<&ColumnDef> for Field built Field
    // via a struct literal instead of routing through Field::new).
    let s = schema();
    let err = s
        .execute("create table t (bio varchar(5000000))".to_string())
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_silently_ignores_non_create_table_statements() {
    // Documents current dispatch behavior: exec_statement only handles
    // Statement::CreateTable (and panics via todo!() on AlterCollation);
    // anything else, including an ordinary SELECT, falls through its
    // wildcard arm as a silent no-op rather than an error.
    let s = schema();
    s.execute("select 1".to_string()).unwrap();
    assert!(!s.table_exists("t"));
}

#[test]
fn test_execute_rejects_malformed_sql() {
    let s = schema();
    let err = s.execute("create table (((".to_string()).unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_create_table_rejects_field_name_over_128_chars() {
    let s = schema();
    let long_name = "a".repeat(129);
    let sql = format!("create table t ({long_name} integer)");
    let err = s.execute(sql).unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_table_rejects_table_name_over_128_chars() {
    let s = schema();
    let long_name = "a".repeat(129);
    let sql = format!("create table {long_name} (id integer)");
    let err = s.execute(sql).unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_create_table_rejects_a_table_that_already_exists() {
    let s = schema();
    s.execute("create table t (id integer)".to_string())
        .unwrap();
    let err = s
        .execute("create table t (id integer)".to_string())
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_create_table_name_is_case_insensitive() {
    let s = schema();
    s.execute("create table Users (id integer)".to_string())
        .unwrap();
    assert!(s.table_exists("users"));
    assert!(s.table_exists("USERS"));
    assert!(s.get_table("USERS").is_some());
}

#[test]
fn test_create_table_column_is_nullable_by_default() {
    let t = create_and_fetch("create table t (id integer)", "t");
    assert!(field(&t, "id").nullable);
}

#[test]
fn test_create_table_not_null_column_is_not_nullable() {
    let t = create_and_fetch("create table t (id integer not null)", "t");
    assert!(!field(&t, "id").nullable);
}

#[test]
fn test_create_table_preserves_declared_field_order() {
    let t = create_and_fetch("create table t (c integer, a integer, b integer)", "t");
    let names: Vec<_> = t.fields.iter().map(|f| f.name.clone()).collect();
    assert_eq!(
        names,
        vec!["c".to_string(), "a".to_string(), "b".to_string()]
    );
}

#[test]
fn test_create_table_rejects_duplicate_field_name() {
    let s = schema();
    let err = s
        .execute("create table t (id integer, id varchar(10))".to_string())
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

// The actual ask: does a created table really persist through a close
// + reopen, not just live in the in-memory `tables` map for the
// lifetime of the current Schema? MemFile can't answer this — its
// `open()` always hands back a fresh, empty buffer regardless of the
// name given, so "reopening" it never proves anything was written to
// durable storage. A real file-backed Schema is the only way to
// exercise this honestly.
#[test]
fn test_created_table_survives_close_and_reopen_on_file_backend() {
    let path = temp_schema_path("create_reopen");
    Db::<File>::delete(&path).unwrap_or_default();

    let s = Schema::<File>::create(path.clone()).unwrap();
    s.execute(
        "create table users (id integer not null, name varchar(50), primary key(id))"
            .to_string(),
    )
    .unwrap();
    assert!(s.table_exists("users"));
    let before = s.get_table("users").unwrap();

    s.close().unwrap();

    let s2 = Schema::<File>::open(path.clone()).unwrap();
    assert!(
        s2.table_exists("users"),
        "table created before close() must still exist after reopen"
    );
    let after = s2.get_table("users").unwrap();

    assert_eq!(before.name, after.name);
    assert_eq!(after.fields.len(), 2);
    assert_eq!(field(&after, "id").datatype, DataType::Integer);
    assert_eq!(field(&after, "name").datatype, DataType::Str(50));
    assert_eq!(after.indices.len(), 1);
    assert!(after.indices[0].is_primary);
    assert_eq!(after.indices[0].fields[0].name, "id");

    Db::<File>::delete(&path).unwrap_or_default();
}

#[test]
fn test_reopened_schema_rejects_recreating_an_existing_table() {
    // A more targeted version of the round-trip test above: confirms
    // load_schema() actually repopulates `tables` on open (not just
    // that get_table happens to still return something), by relying on
    // the duplicate-table-name check to fail for a genuinely fresh
    // Schema instance.
    let path = temp_schema_path("reopen_dup_check");
    Db::<File>::delete(&path).unwrap_or_default();

    let s = Schema::<File>::create(path.clone()).unwrap();
    s.execute("create table t (id integer)".to_string())
        .unwrap();
    s.close().unwrap();

    let s2 = Schema::<File>::open(path.clone()).unwrap();
    let err = s2
        .execute("create table t (id integer)".to_string())
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");

    Db::<File>::delete(&path).unwrap_or_default();
}

#[test]
fn test_create_table_with_no_indices_creates_no_backing_store_tables() {
    // Regression guard for the common case (no PRIMARY KEY / UNIQUE at
    // all): the indices.iter_mut().try_for_each(...) loop in
    // create_table must be a true no-op on an empty Vec, not error or
    // otherwise misbehave.
    let t = create_and_fetch("create table t (id integer, name varchar(50))", "t");
    assert!(t.indices.is_empty());
}

#[test]
fn test_create_table_index_gets_a_real_backing_store_table() {
    let t = create_and_fetch("create table t (id integer not null, primary key(id))", "t");
    assert_eq!(t.indices.len(), 1);
    let idx = &t.indices[0];
    assert_ne!(
        idx.db_table_id,
        TableIdType::none(),
        "a created index must be assigned a real table id, not the none() sentinel"
    );
}

#[test]
fn test_unnamed_index_gets_auto_generated_store_table_name() {
    let s = schema();
    s.execute("create table t (id integer not null, primary key(id))".to_string())
        .unwrap();
    let t = s.get_table("t").unwrap();
    let idx = &t.indices[0];
    // create_table's fallback is format!("{}{}", table.name, count) for
    // the first (count == 0) unnamed index.
    let found = s.db.table_id_by_name("t0").unwrap();
    assert_eq!(
        found,
        Some(idx.db_table_id),
        "the store-level table named \"t0\" must be exactly this index's own backing table"
    );
}

#[test]
fn test_multiple_unnamed_indices_get_distinct_incrementing_names() {
    let s = schema();
    s.execute(
        "create table t (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))"
            .to_string(),
    )
    .unwrap();
    let t = s.get_table("t").unwrap();
    assert_eq!(t.indices.len(), 2);
    assert_ne!(t.indices[0].db_table_id, t.indices[1].db_table_id);

    let t0 = s.db.table_id_by_name("t0").unwrap();
    let t1 = s.db.table_id_by_name("t1").unwrap();
    assert!(t0.is_some() && t1.is_some(), "both t0 and t1 must exist");
    // Order in `indices` mirrors the order they were folded in (table-
    // level constraints first, then inline column options) — primary
    // key here comes from the table-level PRIMARY KEY(id) clause, so
    // it's index 0 / "t0"; UNIQUE(email) is index 1 / "t1".
    assert_eq!(t0, Some(t.indices[0].db_table_id));
    assert_eq!(t1, Some(t.indices[1].db_table_id));
}

#[test]
fn test_explicitly_named_constraint_uses_that_name_for_the_backing_table() {
    let s = schema();
    s.execute(
        "create table t (id integer not null, constraint my_pk primary key(id))".to_string(),
    )
    .unwrap();
    let t = s.get_table("t").unwrap();
    assert_eq!(t.indices[0].name, Some("my_pk".to_string()));
    let found = s.db.table_id_by_name("my_pk").unwrap();
    assert_eq!(found, Some(t.indices[0].db_table_id));
    // The auto-generated fallback name must NOT have been used once an
    // explicit name was given.
    assert_eq!(s.db.table_id_by_name("t0").unwrap(), None);
}

#[test]
fn test_index_backing_table_size_reflects_field_datatypes() {
    // SqlIndex::size() sums each indexed field's DataType::size(), used
    // as the store-level index_entry_size — sanity-check it's at least
    // in the right ballpark for a Str(100) key (must be materially
    // larger than a bare Integer key's budget), rather than asserting
    // an exact byte count tied to ValueItem's own wire format.
    let s = schema();
    s.execute("create table small (id integer not null, primary key(id))".to_string())
        .unwrap();
    s.execute("create table big (email varchar(100) not null, primary key(email))".to_string())
        .unwrap();
    let small = s.get_table("small").unwrap();
    let big = s.get_table("big").unwrap();
    assert!(
        big.indices[0].size() > small.indices[0].size(),
        "a varchar(100) key's index budget ({}) should be larger than a bare \
         integer key's ({})",
        big.indices[0].size(),
        small.indices[0].size()
    );
}

#[test]
fn test_created_indices_survive_close_and_reopen_on_file_backend() {
    let path = temp_schema_path("indices_reopen");
    Db::<File>::delete(&path).unwrap_or_default();

    let s = Schema::<File>::create(path.clone()).unwrap();
    s.execute(
        "create table users (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))"
            .to_string(),
    )
    .unwrap();
    let before = s.get_table("users").unwrap();
    assert_eq!(before.indices.len(), 2);
    s.close().unwrap();

    let s2 = Schema::<File>::open(path.clone()).unwrap();
    let after = s2.get_table("users").unwrap();
    assert_eq!(after.indices.len(), 2);
    // The SqlTable row itself (name/fields/indices metadata, including
    // each index's db_table_id) round-trips via the system schema
    // table; separately, the index's own *backing* store table must
    // still be independently findable by name after reopen too — two
    // different persistence paths (the metadata row vs. store's own
    // table registry) that both need to survive.
    assert_eq!(before.indices[0].db_table_id, after.indices[0].db_table_id);
    assert_eq!(before.indices[1].db_table_id, after.indices[1].db_table_id);
    assert!(s2.db.table_id_by_name("users0").unwrap().is_some());
    assert!(s2.db.table_id_by_name("users1").unwrap().is_some());

    Db::<File>::delete(&path).unwrap_or_default();
}

#[test]
fn test_colliding_index_name_rejects_create_table_and_leaks_nothing() {
    // Regression test for a real bug found and fixed: create_table used
    // to interleave "create this index's backing store table" with the
    // rest of the loop, so a later index's name collision would leave
    // any *earlier* index in the same CREATE TABLE as an orphaned store
    // table — at the time, store had no drop_table at all, and
    // create_table_with_index_entry_size isn't a row-level, undo-logged
    // operation the way insert/update/remove are, so self.db.
    // rollback(txn) had no way to undo it. Fixed two ways, layered:
    // first by validating every index's target name up front (this
    // test) so a *collision* can never start the creation loop in the
    // first place; second, now that store::Db::drop_table exists, by
    // actually cleaning up any index the loop did create before some
    // *other* failure (see the test below, which exercises that path —
    // a collision can no longer reach it, per this test).
    //
    // t1 claims the name "shared_name" for its primary key's backing
    // table. t2 declares two indices — "idx_a" then "shared_name" (the
    // one that collides with t1's) — in declaration order. Both names
    // are checked before either is created, so "idx_a" must never be
    // created at all, not created-then-orphaned.
    let s = schema();
    s.execute(
        "create table t1 (id integer not null, constraint shared_name primary key(id))"
            .to_string(),
    )
    .unwrap();

    let err = s
        .execute(
            "create table t2 (id integer not null, val varchar(20) not null, \
             constraint idx_a unique(val), constraint shared_name primary key(id))"
                .to_string(),
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    assert!(!s.table_exists("t2"), "t2 must not be persisted");
    assert!(
        s.db.table_id_by_name("idx_a").unwrap().is_none(),
        "idx_a must never be created at all, not created-then-orphaned"
    );
}

#[test]
fn test_failed_second_index_creation_drops_the_first_instead_of_leaking_it() {
    // Exercises drop_table's actual wiring into create_table's cleanup
    // path — the validate-first name-collision check above closes that
    // one failure mode before the creation loop ever starts, so this
    // needs a *different* way for create_table_with_index_entry_size to
    // fail after an earlier index in the same statement already
    // succeeded: an index name that's individually invalid (too long),
    // which validate-first's "does this exact name already exist"
    // check can't catch, since nothing else has it yet.
    let s = schema();
    let too_long_name = "a".repeat(200);
    let err = s
        .execute(format!(
            "create table t (id integer not null, val varchar(20) not null, \
             constraint ok_idx unique(id), constraint {too_long_name} unique(val))"
        ))
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    assert!(!s.table_exists("t"), "t must not be persisted");
    assert!(
        s.db.table_id_by_name("ok_idx").unwrap().is_none(),
        "ok_idx succeeded before the second index failed — it must have \
         been dropped by the cleanup path, not left as an orphaned store table"
    );
}

#[test]
fn test_create_table_rejects_index_name_colliding_with_an_unrelated_store_table() {
    // Same validate-first check, from the angle of an index name
    // colliding with something that isn't even a squeal-sql table's
    // index — any name already registered in the underlying store::Db
    // must be rejected the same way.
    let s = schema();
    s.db.create_table("taken".to_string()).unwrap();

    let err = s
        .execute(
            "create table t (id integer not null, constraint taken primary key(id))"
                .to_string(),
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    assert!(!s.table_exists("t"));
}
