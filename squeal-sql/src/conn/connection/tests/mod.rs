use std::sync::Arc;

use store::memfile::MemFile;

use super::ConnectionManager;

// Bypasses the File-backed singleton entirely — cheap, isolated,
// no real filesystem I/O, and a fresh instance per test.
fn manager() -> Arc<ConnectionManager<MemFile>> {
    Arc::new(ConnectionManager::new())
}

#[test]
fn test_new_conn_is_tracked_as_active() {
    let mgr = manager();
    let conn = mgr.clone().new_connection();
    assert!(mgr.active_conns.read().contains(&conn));
}

#[test]
fn test_each_manager_instance_is_independent() {
    let a = manager();
    let b = manager();
    a.clone().new_connection();
    assert_eq!(a.active_conns.read().len(), 1);
    assert_eq!(b.active_conns.read().len(), 0);
}
