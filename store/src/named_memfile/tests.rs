use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::db::Opener;

use super::NamedMemFile;

fn open(name: &str) -> NamedMemFile {
    NamedMemFile::open(OpenOptions::new().create(true).read(true).write(true).clone(), name)
        .unwrap()
}

#[test]
fn test_reopen_by_name_sees_prior_writes() {
    let name = "test_reopen_by_name_sees_prior_writes";
    NamedMemFile::delete(name);

    let mut f = open(name);
    f.write_all(b"hello").unwrap();

    // A fresh `open()` call for the *same name* — simulating a real
    // close-then-reopen — must see the same bytes, unlike MemFile (whose
    // open() always hands back an empty buffer regardless of name).
    let mut f2 = open(name);
    let mut buf = vec![0u8; 5];
    f2.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");

    NamedMemFile::delete(name);
}

#[test]
fn test_different_names_do_not_share_data() {
    let a = "test_different_names_do_not_share_data_a";
    let b = "test_different_names_do_not_share_data_b";
    NamedMemFile::delete(a);
    NamedMemFile::delete(b);

    let mut fa = open(a);
    fa.write_all(b"aaaaa").unwrap();
    let mut fb = open(b);
    let mut buf = vec![0u8; 5];
    // fb is a distinct name — reading from an empty buffer at EOF, not
    // fa's data.
    assert_eq!(fb.read(&mut buf).unwrap(), 0);

    NamedMemFile::delete(a);
    NamedMemFile::delete(b);
}

#[test]
fn test_delete_clears_prior_state_for_that_name() {
    let name = "test_delete_clears_prior_state_for_that_name";
    NamedMemFile::delete(name);

    let mut f = open(name);
    f.write_all(b"stale").unwrap();
    NamedMemFile::delete(name);

    // A fresh open() after delete() must not see the deleted data.
    let mut f2 = open(name);
    let mut buf = vec![0u8; 5];
    assert_eq!(f2.read(&mut buf).unwrap(), 0);

    NamedMemFile::delete(name);
}

#[test]
fn test_delete_also_clears_undo_and_redo_siblings() {
    let name = "test_delete_also_clears_undo_and_redo_siblings";
    let undo = format!("{name}.undo");
    NamedMemFile::delete(name);

    let mut f = open(&undo);
    f.write_all(b"stale-undo").unwrap();
    NamedMemFile::delete(name);

    let mut f2 = open(&undo);
    let mut buf = vec![0u8; 10];
    assert_eq!(f2.read(&mut buf).unwrap(), 0);
}

#[test]
fn test_db_close_then_reopen_round_trips_through_named_memfile() {
    // The actual point of this type: Db::create/close/open must round-trip
    // through it exactly like a real File-backed Db would, with no disk
    // I/O involved.
    use crate::db::Db;
    let name = "test_db_close_then_reopen_round_trips_through_named_memfile";
    NamedMemFile::delete(name);

    let db = Db::<NamedMemFile>::create(name).unwrap();
    let tid = db.create_table("t".to_string()).unwrap();
    db.close().unwrap();

    let db2 = Db::<NamedMemFile>::open(name).unwrap();
    assert_eq!(db2.table_id_by_name("t").unwrap(), Some(tid));

    NamedMemFile::delete(name);
}

#[test]
fn test_clone_shares_data_like_memfile() {
    let name = "test_clone_shares_data_like_memfile";
    NamedMemFile::delete(name);

    let mut f = open(name);
    f.write_all(b"shared").unwrap();
    let mut g = f.clone();
    g.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = vec![0u8; 6];
    g.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"shared");

    NamedMemFile::delete(name);
}
