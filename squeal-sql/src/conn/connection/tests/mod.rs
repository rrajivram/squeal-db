use std::sync::Arc;

use store::memfile::MemFile;

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
    assert!(Arc::ptr_eq(&c1.database, &c2.database));
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
