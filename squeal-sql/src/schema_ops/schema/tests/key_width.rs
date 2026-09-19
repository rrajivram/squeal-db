// Persistence versioning Stage 3: DDL-time rejection of an index/primary key
// whose worst-case width (summed declared column widths) would exceed this
// database's configured max_index_key_size (default 512 bytes). The store
// reserves header space for exactly that ceiling (see page_overhead), so a
// key wider than it is refused up front rather than risking a corrupt page
// header once a split stamps it as a high_key.
use super::*;

fn assert_key_too_wide(r: Result<(), SchemaError>, needle: &str) {
    match r {
        Err(SchemaError::UserError(msg)) => {
            assert!(msg.contains("max index key size"), "got {msg:?}");
            assert!(msg.contains(needle), "expected {needle:?} in {msg:?}");
        }
        other => panic!("expected a key-width UserError, got {other:?}"),
    }
}

#[test]
fn test_create_table_rejects_a_primary_key_wider_than_the_cap() {
    let c = conn();
    let r = execute(
        &c,
        "create table t (a varchar(200) not null, b varchar(200) not null, \
         c varchar(200) not null, primary key(a, b, c))",
    );
    assert_key_too_wide(r, "PRIMARY KEY");
    assert!(
        !c.current_schema().unwrap().table_exists("t"),
        "a rejected CREATE TABLE must not leave the table behind"
    );
}

#[test]
fn test_create_table_accepts_a_primary_key_within_the_cap() {
    let c = conn();
    execute(
        &c,
        "create table t (a varchar(100) not null, b varchar(100) not null, \
         primary key(a, b))",
    )
    .unwrap();
}

#[test]
fn test_wide_non_key_columns_are_not_subject_to_the_cap() {
    let c = conn();
    execute(
        &c,
        "create table t (id integer not null, body varchar(4000), primary key(id))",
    )
    .unwrap();
}

#[test]
fn test_create_table_rejects_a_wide_unique_constraint() {
    let c = conn();
    let r = execute(
        &c,
        "create table t (id integer not null, a varchar(300) not null, \
         b varchar(300) not null, primary key(id), unique(a, b))",
    );
    assert_key_too_wide(r, "index");
}

#[test]
fn test_create_index_rejects_a_key_wider_than_the_cap() {
    let c = conn();
    execute(
        &c,
        "create table t (id integer not null, a varchar(300), b varchar(300), primary key(id))",
    )
    .unwrap();
    let r = execute(&c, "create index idx_ab on t(a, b)");
    assert_key_too_wide(r, "idx_ab");
    // The rejection happens before anything is created, so the same name is
    // still free for a legitimately narrow index.
    execute(&c, "create index idx_ab on t(a)").unwrap();
}

#[test]
fn test_non_unique_index_key_counts_the_appended_row_identity() {
    // A non-unique index's tree key is (indexed columns + the row's own
    // identity), so a wide PRIMARY KEY pushes an otherwise-narrow index over
    // the cap; the same index declared UNIQUE (no identity appended) fits.
    let c = conn();
    execute(
        &c,
        "create table t (pk varchar(300) not null, v varchar(200) not null, primary key(pk))",
    )
    .unwrap();
    let r = execute(&c, "create index idx_v on t(v)");
    assert_key_too_wide(r, "idx_v");
    execute(&c, "create unique index uq_v on t(v)").unwrap();
}
