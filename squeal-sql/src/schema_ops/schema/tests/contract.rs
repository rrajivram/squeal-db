// Schema's public API guarantees: execute()/create()/open()/close()/
// table_exists()/get_table() behavior — rejecting invalid input with the
// right error, persisting across close/reopen, and not leaking partial
// state on failure. Not interested in SQL-to-object mapping details here
// (see `mapping`).
use std::fs::File;

use crate::datatype::DataType;

use super::*;

fn is_user_error(e: &SchemaError) -> bool {
    matches!(e, SchemaError::UserError(_))
}
fn is_bad_table_name(e: &SchemaError) -> bool {
    matches!(e, SchemaError::BadTableName(_))
}
fn is_parse_error(e: &SchemaError) -> bool {
    matches!(e, SchemaError::ParseError(_))
}

#[test]
fn test_execute_rejects_invalid_schema_definitions() {
    struct Case {
        // Runs first, on the same Schema, and must succeed — for cases
        // that need pre-existing state (e.g. a name collision).
        setup: Option<&'static str>,
        sql: String,
        matches: fn(&SchemaError) -> bool,
        desc: &'static str,
    }
    let long_name = "a".repeat(129);
    let cases = [
        Case {
            setup: None,
            sql: "create table t (id integer, primary key(id))".into(),
            matches: is_user_error,
            desc: "nullable primary key column",
        },
        Case {
            setup: None,
            sql: "create table t (bio varchar(5000000))".into(),
            matches: is_user_error,
            desc: "field datatype over 4MB",
        },
        Case {
            setup: None,
            sql: format!("create table t ({long_name} integer)"),
            matches: is_user_error,
            desc: "field name over 128 chars",
        },
        Case {
            setup: None,
            sql: "create table t (id integer, id varchar(10))".into(),
            matches: is_user_error,
            desc: "duplicate field name",
        },
        Case {
            setup: None,
            sql: format!("create table {long_name} (id integer)"),
            matches: is_bad_table_name,
            desc: "table name over 128 chars",
        },
        Case {
            setup: Some("create table t (id integer)"),
            sql: "create table t (id integer)".into(),
            matches: is_bad_table_name,
            desc: "table that already exists",
        },
        Case {
            setup: None,
            sql: "create table (((".into(),
            matches: is_parse_error,
            desc: "malformed sql",
        },
    ];
    for c in cases {
        let s = schema();
        if let Some(setup) = c.setup {
            s.execute(setup.to_string()).unwrap();
        }
        let err = s.execute(c.sql.clone()).unwrap_err();
        assert!((c.matches)(&err), "{}: got {err:?}", c.desc);
    }
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

// The actual ask: does created state really persist through a close +
// reopen, not just live in the in-memory maps for the lifetime of the
// current Schema? MemFile can't answer this — its `open()` always hands
// back a fresh, empty buffer regardless of the name given, so
// "reopening" it never proves anything was written to durable storage. A
// real file-backed Schema is the only way to exercise this honestly.
#[test]
fn test_schema_state_survives_close_and_reopen() {
    let path = temp_schema_path("close_reopen_roundtrip");
    Db::<File>::delete(&path).unwrap_or_default();

    let s = Schema::<File>::create_database(path.clone()).unwrap();
    s.execute(
        "create table users (id integer not null, email varchar(50) not null, \
         primary key(id), unique(email))"
            .to_string(),
    )
    .unwrap();
    let before = s.get_table("users").unwrap();
    assert_eq!(before.indices.len(), 2);
    s.close_database().unwrap();

    let s2 = Schema::<File>::open(path.clone()).unwrap();
    assert!(
        s2.table_exists("users"),
        "table created before close() must still exist after reopen"
    );
    let after = s2.get_table("users").unwrap();

    // Table/field shape round-trips via the system schema table...
    assert_eq!(before.name, after.name);
    assert_eq!(after.fields.len(), 2);
    assert_eq!(field(&after, "id").datatype, DataType::Integer);
    assert_eq!(field(&after, "email").datatype, DataType::Str(50));

    // ...and so does each index's own metadata, plus its *separate*
    // backing store table — two different persistence paths (the
    // SqlTable metadata row vs. store's own table registry) that both
    // need to survive.
    assert_eq!(after.indices.len(), 2);
    assert_eq!(before.indices[0].db_table_id, after.indices[0].db_table_id);
    assert_eq!(before.indices[1].db_table_id, after.indices[1].db_table_id);
    assert!(s2.db.table_id_by_name("users0").unwrap().is_some());
    assert!(s2.db.table_id_by_name("users1").unwrap().is_some());

    Db::<File>::delete(&path).unwrap_or_default();
}

#[test]
fn test_reopened_schema_rejects_recreating_an_existing_table() {
    // A more targeted version of the round-trip test above: confirms
    // load_schema() actually repopulates `tables` on open (not just that
    // get_table happens to still return something), by relying on the
    // duplicate-table-name check to fail for a genuinely fresh Schema
    // instance.
    let path = temp_schema_path("reopen_dup_check");
    Db::<File>::delete(&path).unwrap_or_default();

    let s = Schema::<File>::create_database(path.clone()).unwrap();
    s.execute("create table t (id integer)".to_string())
        .unwrap();
    s.close_database().unwrap();

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
fn test_index_backing_table_naming() {
    struct Case {
        sql: &'static str,
        // (index position, expected backing store table name, expected index.name)
        expected: &'static [(usize, &'static str, Option<&'static str>)],
    }
    let cases = [
        // A single unnamed index falls back to format!("{table}{count}").
        Case {
            sql: "create table t (id integer not null, primary key(id))",
            expected: &[(0, "t0", None)],
        },
        // Multiple unnamed indices get distinct, incrementing fallback
        // names, in declaration order (table-level constraints first).
        Case {
            sql: "create table t (id integer not null, email varchar(50) not null, \
                  primary key(id), unique(email))",
            expected: &[(0, "t0", None), (1, "t1", None)],
        },
        // An explicit constraint name is used for the backing table
        // instead of the auto-generated fallback.
        Case {
            sql: "create table t (id integer not null, constraint my_pk primary key(id))",
            expected: &[(0, "my_pk", Some("my_pk"))],
        },
    ];
    for c in cases {
        let s = schema();
        s.execute(c.sql.to_string()).unwrap();
        let t = s.get_table("t").unwrap();
        assert_eq!(t.indices.len(), c.expected.len(), "for `{}`", c.sql);
        for &(i, backing_name, index_name) in c.expected {
            assert_eq!(
                t.indices[i].name,
                index_name.map(String::from),
                "index name for `{}`",
                c.sql
            );
            let found = s.db.table_id_by_name(backing_name).unwrap();
            assert_eq!(
                found,
                Some(t.indices[i].db_table_id),
                "backing table {backing_name:?} for `{}`",
                c.sql
            );
        }
    }
}

#[test]
fn test_index_backing_table_size_reflects_field_datatypes() {
    // SqlIndex::size() sums each indexed field's DataType::size(), used
    // as the store-level index_entry_size — sanity-check it's at least
    // in the right ballpark for a Str(100) key (must be materially
    // larger than a bare Integer key's budget), rather than asserting an
    // exact byte count tied to ValueItem's own wire format.
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
        "create table t1 (id integer not null, constraint shared_name primary key(id))".to_string(),
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
    // which validate-first's "does this exact name already exist" check
    // can't catch, since nothing else has it yet.
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
            "create table t (id integer not null, constraint taken primary key(id))".to_string(),
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    assert!(!s.table_exists("t"));
}
