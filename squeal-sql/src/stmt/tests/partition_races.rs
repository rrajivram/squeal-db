// Concurrency around partitions and DDL, with the interleaving forced (see
// crate::testhook) rather than left to chance: one operation is held at a
// named point mid-way, the other runs, and the first is let go.
//
// Each test states what must hold afterwards, in a way that does not
// depend on HOW it is made to hold: the second operation may fail, or wait
// for the first (the test lets the first go after a short while either
// way), or both may succeed with a consistent result. They failed before
// statements took table locks (conn::tablelock), which is what makes them
// pass: the second operation now waits for the first.
use std::time::{Duration, Instant};

use store::{cursor::Cursor, tuple::DBIdType, valueitem::IndexKey};

use super::*;
use crate::table::VersionedRow;

use ValueItem::Integer;

// Two connections to one database.
fn two_conns() -> (Arc<Connection<MemFile>>, Arc<Connection<MemFile>>) {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let a = mgr.create_and_connect("race_db").unwrap();
    a.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let b = mgr.connect("race_db").unwrap();
    b.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    (a, b)
}

// Runs `first` until it stops at `point` (armed for `table`), then `second`
// while it is held there, then lets `first` go. `second` gets a moment to
// finish first; if it is waiting for `first` (a design that excludes the
// two), it finishes after.
fn interleave<A: Send, B: Send>(
    point: &str,
    table: &str,
    first: impl FnOnce() -> A + Send,
    second: impl FnOnce() -> B + Send,
) -> (A, B) {
    let armed = crate::testhook::arm(point, table);
    std::thread::scope(|s| {
        let a = s.spawn(first);
        armed.wait_reached();
        let b = s.spawn(second);
        let deadline = Instant::now() + Duration::from_millis(500);
        while !b.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        armed.release();
        (a.join().unwrap(), b.join().unwrap())
    })
}

fn count(c: &Arc<Connection<MemFile>>, sql: &str) -> i64 {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    match take_streaming_result(&mut stmt, 0).1.as_slice() {
        [row] => match row[0] {
            Integer(n) => n,
            ref other => panic!("expected a count, got {other:?}"),
        },
        other => panic!("expected one row, got {other:?}"),
    }
}

// What must be true of a table's storage at rest (committed state):
//  - every row is in the partition its partition column's value routes to;
//  - every row has its entry in each index tree of its partition;
//  - an index tree holds nothing else.
// Returns every violation found.
pub(super) fn storage_problems(c: &Arc<Connection<MemFile>>, name: &str) -> Vec<String> {
    let table = c.current_schema().unwrap().get_table(name).unwrap();
    let db = c.database.read().db.clone();
    let mut problems = vec![];
    let txn = db.begin().unwrap();
    for (p, part) in table.partitions.iter().enumerate() {
        let label = if part.name.is_empty() {
            "<whole>"
        } else {
            &part.name
        };
        let mut rows = 0usize;
        let mut cursor = db.table_scan_in_txn(part.rows(), &txn).unwrap();
        while let Some(tuple) = cursor.next().unwrap() {
            rows += 1;
            let row = table.decode_row(tuple.data(), None).unwrap();
            match table.partition_for(row.values()) {
                Ok(want) if want == p => {}
                Ok(want) => problems.push(format!(
                    "row {:?} is in partition {label} but belongs in {}",
                    row.values(),
                    table.partitions[want].name
                )),
                Err(e) => problems.push(format!(
                    "row {:?} is in partition {label} but: {e}",
                    row.values()
                )),
            }
            let identity = match tuple.id() {
                DBIdType::Rec(ik) => ik.clone(),
                DBIdType::Int(n) => IndexKey::new_from(&[Integer(*n as i64)]).unwrap(),
            };
            for (i, index) in table.indices.iter().enumerate() {
                let mut values = table.extract_field_values(&index.fields, row.values());
                if !index.is_primary && !index.is_unique {
                    values.extend_from_slice(identity.values());
                }
                let key = DBIdType::Rec(IndexKey::new_from(&values).unwrap());
                if db.find(part.index(i), key, &txn).unwrap().is_none() {
                    problems.push(format!(
                        "row {:?} in partition {label} has no entry in index {:?}",
                        row.values(),
                        index.name.as_deref().unwrap_or("<unnamed>")
                    ));
                }
            }
        }
        for (i, index) in table.indices.iter().enumerate() {
            let mut entries = 0usize;
            let mut cursor = db.table_scan_in_txn(part.index(i), &txn).unwrap();
            while cursor.next().unwrap().is_some() {
                entries += 1;
            }
            if entries != rows {
                problems.push(format!(
                    "partition {label}: {rows} rows but {entries} entries in index {:?}",
                    index.name.as_deref().unwrap_or("<unnamed>")
                ));
            }
        }
    }
    db.commit(txn).unwrap();
    problems
}

fn assert_storage_sound(c: &Arc<Connection<MemFile>>, name: &str) {
    let problems = storage_problems(c, name);
    assert!(problems.is_empty(), "{name}: {problems:#?}");
}

// The checker itself: a table that went through every kind of write is
// sound, and it does report a row put where it does not belong.
#[test]
fn test_the_storage_check_passes_a_healthy_table_and_catches_a_misplaced_row() {
    let (c, _) = two_conns();
    run(
        &c,
        "create table checked (id integer not null, day integer not null, note varchar(8), \
         primary key(id, day)) partition by range (day) ( \
         partition a values less than (10), partition b values less than (20))",
    )
    .unwrap();
    run(&c, "create index checked_note on checked (note)").unwrap();
    run(
        &c,
        "insert into checked values (1, 1, 'x'), (2, 5, 'y'), (3, 15, 'x')",
    )
    .unwrap();
    run(&c, "update checked set day = 12 where id = 1").unwrap();
    run(&c, "delete from checked where id = 2").unwrap();
    run(
        &c,
        "alter table checked add partition z values less than (30)",
    )
    .unwrap();
    run(&c, "insert into checked values (4, 25, 'z')").unwrap();
    assert_storage_sound(&c, "checked");

    // Put a day-5 row straight into partition b's rows tree.
    let table = c.current_schema().unwrap().get_table("checked").unwrap();
    let db = c.database.read().db.clone();
    let txn = db.begin().unwrap();
    let values = vec![Integer(9), Integer(5), ValueItem::Str(("w".into(), 8))];
    let key = DBIdType::Rec(IndexKey::new_from(&values[..2]).unwrap());
    let data = VersionedRow {
        version: table.version(),
        values: IndexKey::new_from(&values).unwrap(),
    };
    db.insert(
        table.partitions[1].rows(),
        store::tuple::Tuple::new_with(
            key,
            &postcard::to_allocvec(&data).unwrap(),
            Some(txn.id()),
            None,
        ),
        &txn,
    )
    .unwrap();
    db.commit(txn).unwrap();
    let problems = storage_problems(&c, "checked");
    assert!(
        problems.iter().any(|p| p.contains("belongs in a")),
        "{problems:#?}"
    );
    assert!(
        problems.iter().any(|p| p.contains("has no entry in index")),
        "{problems:#?}"
    );
}

// A scan that is under way when its partition is dropped still reads every
// row it was going to: the dropped partition's trees stay in the store (see
// Schema::drop_partition).
#[test]
fn test_a_scan_in_progress_survives_its_partition_being_dropped() {
    let (c, other) = two_conns();
    run(
        &c,
        "create table scanned (id integer not null, day integer not null) \
         partition by range (day) (partition a values less than (10), \
         partition b values less than (20))",
    )
    .unwrap();
    for id in 0..200 {
        run(
            &c,
            &format!("insert into scanned values ({id}, {})", id % 20),
        )
        .unwrap();
    }
    let mut stmt = c
        .clone()
        .create_statement("select id from scanned")
        .unwrap();
    stmt.execute().unwrap();
    let Some(Some(ResultType::StreamingResult(mut stream))) = stmt.results.pop() else {
        panic!("expected a streaming result");
    };
    assert!(stream.next_result().unwrap().is_some());
    run(&other, "alter table scanned drop partition b").unwrap();
    run(&other, "alter table scanned drop partition a").unwrap_err();
    let mut read = 1;
    while stream.next_result().unwrap().is_some() {
        read += 1;
    }
    assert_eq!(read, 200);
    assert_eq!(count(&other, "select count(*) from scanned"), 100);
}

// ADD PARTITION checks that the DEFAULT partition holds no row with the new
// partition's values, then adds it. A row with such a value inserted in
// between is in DEFAULT, where nothing routes to any more.
#[test]
fn test_add_partition_and_an_insert_of_its_value_leave_the_row_where_it_routes() {
    let (c, other) = two_conns();
    run(
        &c,
        "create table race_add (id integer not null, region varchar(4)) \
         partition by list (region) (partition west values in ('ca'), partition rest default)",
    )
    .unwrap();
    let (added, inserted) = interleave(
        "add_partition.checked",
        "race_add",
        || {
            run(
                &c,
                "alter table race_add add partition south values in ('tx')",
            )
        },
        || run(&other, "insert into race_add values (1, 'tx')"),
    );
    println!("add partition: {added:?}, insert: {inserted:?}");
    assert_storage_sound(&c, "race_add");
    if inserted.is_ok() {
        // And the row can be found and removed like any other.
        assert_eq!(
            count(&c, "select count(*) from race_add where region = 'tx'"),
            1
        );
        run(&c, "delete from race_add where region = 'tx'").unwrap();
        assert_eq!(count(&c, "select count(*) from race_add"), 0);
    }
}

// An INSERT that resolved its table before a DROP PARTITION writes into the
// dropped partition's trees, which nothing reads any more. Its statement
// was told the row is in; the row is not there.
#[test]
fn test_an_insert_racing_a_drop_partition_is_stored_or_refused() {
    let (c, other) = two_conns();
    run(
        &c,
        "create table race_drop (id integer not null, day integer not null) \
         partition by range (day) (partition a values less than (10), \
         partition b values less than (20), partition c values less than (30))",
    )
    .unwrap();
    let (inserted, dropped) = interleave(
        "insert.resolved",
        "race_drop",
        || {
            // One transaction: the insert, then a read of what it wrote.
            run(&c, "begin")?;
            let inserted = run(&c, "insert into race_drop values (1, 15)");
            let found = count(&c, "select count(*) from race_drop where id = 1");
            run(&c, "commit")?;
            inserted.map(|()| found)
        },
        || run(&other, "alter table race_drop drop partition b"),
    );
    println!("insert, then rows found: {inserted:?}, drop partition: {dropped:?}");
    // A transaction sees its own write, whatever the drop does afterwards.
    if let Ok(found) = inserted {
        assert_eq!(found, 1, "the insert was acknowledged");
    }
    assert_storage_sound(&c, "race_drop");
}

// CREATE INDEX backfills from the rows it can see, then publishes the
// index. A row inserted in between was written by a statement that did not
// know of the index: it has no entry, and a query through the index misses
// it. Not specific to partitions.
fn create_index_racing_an_insert(table: &str, partition_by: &str) {
    let (c, other) = two_conns();
    run(
        &c,
        &format!(
            "create table {table} (id integer not null, day integer not null, \
             note varchar(8)) {partition_by}"
        ),
    )
    .unwrap();
    run(
        &c,
        &format!("insert into {table} values (1, 5, 'x'), (2, 15, 'y')"),
    )
    .unwrap();
    let (created, inserted) = interleave(
        "create_index.backfilled",
        table,
        || run(&c, &format!("create index {table}_note on {table} (note)")),
        || run(&other, &format!("insert into {table} values (3, 16, 'z')")),
    );
    println!("create index: {created:?}, insert: {inserted:?}");
    assert_storage_sound(&c, table);
}

#[test]
fn test_create_index_and_a_concurrent_insert_leave_every_row_indexed() {
    create_index_racing_an_insert("race_index_plain", "");
}

#[test]
fn test_create_index_on_a_partitioned_table_and_a_concurrent_insert_leave_every_row_indexed() {
    create_index_racing_an_insert(
        "race_index_part",
        "partition by range (day) (partition a values less than (10), \
         partition b values less than (20))",
    );
}

// ALTER TABLE reads the table's definition, changes its copy, and writes
// it back. Two of them at once each write their own copy: the later write
// drops the earlier one's change. When the lost change is ADD PARTITION,
// rows inserted into the new partition in between are lost with it.
#[test]
fn test_two_alter_tables_at_once_both_take_effect() {
    let (c, other) = two_conns();
    run(
        &c,
        "create table race_alter (id integer not null, day integer not null) \
         partition by range (day) (partition a values less than (10), \
         partition b values less than (20))",
    )
    .unwrap();
    let (column, partition) = interleave(
        "alter_table.applied",
        "race_alter",
        || {
            run(
                &c,
                "alter table race_alter add column extra integer default 7",
            )
        },
        || {
            run(
                &other,
                "alter table race_alter add partition late values less than (30)",
            )?;
            run(&other, "insert into race_alter (id, day) values (1, 25)")
        },
    );
    println!("add column: {column:?}, add partition + insert: {partition:?}");
    let table = c.current_schema().unwrap().get_table("race_alter").unwrap();
    if column.is_ok() {
        assert!(
            table.fields().iter().any(|f| f.name == "extra"),
            "ADD COLUMN was lost"
        );
    }
    if partition.is_ok() {
        assert!(
            table.partitions.iter().any(|p| p.name == "late"),
            "ADD PARTITION was lost"
        );
        assert_eq!(
            count(&c, "select count(*) from race_alter"),
            1,
            "its row was lost"
        );
    }
    assert_storage_sound(&c, "race_alter");
}

#[test]
fn test_two_add_columns_at_once_both_take_effect() {
    let (c, other) = two_conns();
    run(&c, "create table race_columns (id integer not null)").unwrap();
    let (x, y) = interleave(
        "alter_table.applied",
        "race_columns",
        || run(&c, "alter table race_columns add column x integer"),
        || run(&other, "alter table race_columns add column y integer"),
    );
    println!("add x: {x:?}, add y: {y:?}");
    let table = c
        .current_schema()
        .unwrap()
        .get_table("race_columns")
        .unwrap();
    let has = |name: &str| table.fields().iter().any(|f| f.name == name);
    assert_eq!((has("x"), has("y")), (x.is_ok(), y.is_ok()));
}

// A transaction reads the same table twice; between the reads another
// connection drops a partition. The definition of the table is not part of
// the transaction's snapshot, so the second read is of a different table.
#[test]
fn test_a_transaction_reads_the_same_rows_before_and_after_a_concurrent_drop_partition() {
    let (c, other) = two_conns();
    run(
        &c,
        "create table race_snapshot (id integer not null, day integer not null) \
         partition by range (day) (partition a values less than (10), \
         partition b values less than (20))",
    )
    .unwrap();
    run(
        &c,
        "insert into race_snapshot values (1, 5), (2, 6), (3, 15)",
    )
    .unwrap();
    run(&c, "begin").unwrap();
    let before = count(&c, "select count(*) from race_snapshot");
    // In a thread: a design that makes the drop wait for the transaction
    // would otherwise hang the test here.
    let after = std::thread::scope(|s| {
        let dropped = s.spawn(|| run(&other, "alter table race_snapshot drop partition b"));
        let deadline = Instant::now() + Duration::from_millis(500);
        while !dropped.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        let after = count(&c, "select count(*) from race_snapshot");
        run(&c, "commit").unwrap();
        println!("drop partition: {:?}", dropped.join().unwrap());
        after
    });
    assert_eq!(before, 3);
    assert_eq!(after, before, "one transaction, one snapshot");
}

// Two transactions each hold a table the other's DDL wants: the second to
// ask is refused at once with a deadlock error (not after the lock
// timeout), and once it rolls back the first goes on.
#[test]
fn test_two_transactions_waiting_for_each_others_tables_is_a_deadlock_error() {
    let (c, other) = two_conns();
    run(&c, "create table dl_a (id integer)").unwrap();
    run(&c, "create table dl_b (id integer)").unwrap();
    run(&c, "begin").unwrap();
    count(&c, "select count(*) from dl_a");
    run(&other, "begin").unwrap();
    count(&other, "select count(*) from dl_b");
    std::thread::scope(|s| {
        // c waits for other's hold on dl_b.
        let first = s.spawn(|| run(&c, "alter table dl_b add column x integer"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!first.is_finished(), "waits for the other transaction");
        let start = Instant::now();
        let err = run(&other, "alter table dl_a add column y integer").unwrap_err();
        assert!(err.to_string().contains("deadlock"), "{err}");
        assert!(start.elapsed() < Duration::from_secs(2));
        run(&other, "rollback").unwrap();
        first.join().unwrap().unwrap();
    });
    run(&c, "commit").unwrap();
    let table = c.current_schema().unwrap().get_table("dl_b").unwrap();
    assert!(table.fields().iter().any(|f| f.name == "x"));
}
