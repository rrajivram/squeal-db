use std::sync::Arc;

use store::memfile::MemFile;
use store::named_memfile::NamedMemFile;

use super::ConnectionManager;
use crate::error::SchemaError;

fn manager() -> Arc<ConnectionManager<MemFile>> {
    Arc::new(ConnectionManager::new())
}

#[test]
fn test_create_and_connect_is_tracked_as_active() {
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    assert!(mgr.active_conns.read().contains(&conn));
}

#[test]
fn test_each_manager_instance_is_independent() {
    let a = manager();
    let b = manager();
    a.create_and_connect("db1").unwrap();
    assert_eq!(a.active_conns.read().len(), 1);
    assert_eq!(b.active_conns.read().len(), 0);
}

#[test]
fn test_connect_reuses_the_open_database() {
    let mgr = manager();
    let c1 = mgr.create_and_connect("db1").unwrap();
    let c2 = mgr.connect("db1").unwrap();
    assert!(Arc::ptr_eq(&c1.database.read(), &c2.database.read()));
}

#[test]
fn test_create_and_connect_rejects_an_already_open_database() {
    let mgr = manager();
    mgr.create_and_connect("db1").unwrap();
    let err = match mgr.create_and_connect("db1") {
        Err(e) => e,
        Ok(_) => panic!("expected an error, got a second connection"),
    };
    assert!(matches!(err, SchemaError::DatabaseInUseError(_)));
}

#[test]
fn test_use_database_repoints_connection_and_clears_current_schema() {
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    conn.use_schema("default").unwrap();
    assert!(conn.current_schema.read().is_some());

    let conn2 = mgr.create_and_connect("db2").unwrap();
    conn.use_database("db2").unwrap();

    assert!(
        conn.current_schema.read().is_none(),
        "switching database must clear the current schema"
    );
    assert!(
        Arc::ptr_eq(&conn.database.read(), &conn2.database.read()),
        "use_database must reuse the already-open database, not create a separate instance"
    );
}

#[test]
fn test_create_database_on_existing_connection_switches_it() {
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    conn.use_schema("default").unwrap();

    conn.create_database("db2").unwrap();
    assert!(conn.current_schema.read().is_none());
    assert_eq!(conn.database.read().name(), "db2");
}

#[test]
fn test_create_database_on_existing_connection_rejects_already_open_name() {
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    let err = conn.create_database("db1").unwrap_err();
    assert!(matches!(err, SchemaError::DatabaseInUseError(_)));
}

#[test]
fn test_connection_is_one_to_one_with_a_database_not_a_schema() {
    // Connection no longer opens/tracks schemas as if each were its own
    // database — it's bound to exactly one Database and just points at
    // whichever schema within it is "current".
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    assert!(conn.current_schema.read().is_none());

    conn.use_schema("default").unwrap();
    assert!(conn.current_schema.read().is_some());

    conn.create_schema("extra").unwrap();
    assert!(
        !conn
            .current_schema
            .read()
            .as_ref()
            .unwrap()
            .table_exists("nonexistent")
    );
}

#[test]
fn test_close_removes_the_connection_and_database_from_the_manager() {
    let mgr = manager();
    let conn = mgr.create_and_connect("db1").unwrap();
    // Moved, not cloned: close() takes Arc<Self> by value specifically
    // so a caller moves its last handle in — a lingering clone (e.g.
    // this test's own `conn`, if kept alive past this point) would
    // still hold the connection's own Arc<Database> reference, which is
    // exactly what the next test below exercises deliberately.
    conn.close().unwrap();

    assert!(mgr.open_databases.read().is_empty());
    // connect() must reopen fresh (MemFile always hands back an empty
    // buffer for a "new" open) rather than find a stale entry still
    // sitting in the registry.
    assert!(mgr.connect("db1").is_err());
}

#[test]
fn test_close_rejects_when_another_connection_still_has_the_database_open() {
    let mgr = manager();
    let c1 = mgr.create_and_connect("db1").unwrap();
    let c2 = mgr.connect("db1").unwrap();

    let err = c1.close().unwrap_err();
    assert!(matches!(err, SchemaError::UnknownError(_)), "got {err:?}");
    // Rejected, not partially applied — db1 must still be open and
    // usable through the other connection.
    assert!(mgr.open_databases.read().contains_key("db1"));
    assert_eq!(c2.database_name(), "db1");
}

#[test]
fn test_close_persists_state_for_a_later_reopen() {
    // The actual point of Connection::close: data committed before it
    // must be visible after a fresh connect(), not just while the
    // original connection/manager instance is still alive. Needs
    // NamedMemFile (not plain MemFile) — see its own doc comment on why
    // MemFile's open() can't answer this.
    let path = format!("close_reopen_conn_{}", std::process::id());
    NamedMemFile::delete(&path);
    let mgr: Arc<ConnectionManager<NamedMemFile>> = Arc::new(ConnectionManager::new());

    let conn = mgr.create_and_connect(&path).unwrap();
    conn.create_schema("s").unwrap();
    conn.close().unwrap();

    let mgr2: Arc<ConnectionManager<NamedMemFile>> = Arc::new(ConnectionManager::new());
    let conn2 = mgr2.connect(&path).unwrap();
    conn2.use_schema("s").unwrap();
    assert!(conn2.current_schema.read().is_some());

    NamedMemFile::delete(&path);
}
