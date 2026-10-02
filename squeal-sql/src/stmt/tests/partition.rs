// Partitioned tables, end to end through SQL: CREATE TABLE ... PARTITION BY,
// ALTER TABLE ADD/DROP PARTITION, and rows going to (and being read back
// from) the partition their partition column's value puts them in. What a
// query reads is every partition, one after the other (source::append):
// skipping the ones a WHERE rules out is not done yet.
use store::cursor::Cursor;

use super::*;
use crate::partition::PartitionBound;

use ValueItem::Integer;

fn rows(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<Vec<ValueItem>> {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let mut rows = take_streaming_result(&mut stmt, 0).1;
    rows.sort();
    rows
}

fn ints(c: &Arc<Connection<MemFile>>, sql: &str) -> Vec<i64> {
    rows(c, sql)
        .into_iter()
        .map(|r| match r[0] {
            Integer(n) => n,
            ref other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

fn err(c: &Arc<Connection<MemFile>>, sql: &str) -> String {
    match run(c, sql) {
        Ok(()) => panic!("expected an error from: {sql}"),
        Err(e) => e.to_string(),
    }
}

fn table(c: &Arc<Connection<MemFile>>, name: &str) -> Arc<SqlTable> {
    c.current_schema().unwrap().get_table(name).unwrap()
}

// How many rows each partition's own rows tree holds, by partition name.
fn rows_per_partition(c: &Arc<Connection<MemFile>>, name: &str) -> Vec<(String, usize)> {
    let t = table(c, name);
    let db = c.database.read().db.clone();
    t.partitions
        .iter()
        .map(|p| {
            let mut cursor = db.table_scan(p.rows()).unwrap();
            let mut n = 0;
            while cursor.next().unwrap().is_some() {
                n += 1;
            }
            (p.name.clone(), n)
        })
        .collect()
}

fn counts(c: &Arc<Connection<MemFile>>, name: &str) -> Vec<usize> {
    rows_per_partition(c, name)
        .into_iter()
        .map(|p| p.1)
        .collect()
}

// events: id, day (the RANGE partition column), three partitions.
fn range_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table events (id integer not null, day integer not null, note varchar(10), \
         primary key(id, day)) \
         partition by range (day) ( \
           partition early values less than (10), \
           partition mid values less than (20), \
           partition late values less than (30))",
    )
    .unwrap();
    for (id, day) in [(1, 5), (2, 9), (3, 10), (4, 19), (5, 20), (6, 29)] {
        run(
            &c,
            &format!("insert into events values ({id}, {day}, 'n{id}')"),
        )
        .unwrap();
    }
    c
}

#[test]
fn test_a_range_partitioned_table_stores_each_row_in_its_bounds_partition() {
    let c = range_conn();
    let t = table(&c, "events");
    assert!(t.is_partitioned());
    assert_eq!(
        t.partitions
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["early", "mid", "late"]
    );
    assert_eq!(t.partitions[1].bound, PartitionBound::LessThan(Integer(20)));
    // A bound is exclusive: day 10 is mid's, day 20 late's.
    assert_eq!(counts(&c, "events"), [2, 2, 2]);
    // Each partition has trees of its own, for its rows and for each index.
    let mut trees: Vec<_> = t
        .partitions
        .iter()
        .flat_map(|p| [p.rows(), p.index(0)])
        .collect();
    trees.sort();
    trees.dedup();
    assert_eq!(trees.len(), 6);
    assert_eq!(ints(&c, "select id from events"), [1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_queries_read_every_partition() {
    let c = range_conn();
    assert_eq!(
        ints(&c, "select id from events where day >= 10"),
        [3, 4, 5, 6]
    );
    assert_eq!(ints(&c, "select id from events where note = 'n4'"), [4]);
    // A primary key lookup seeks each partition's tree.
    assert_eq!(
        ints(&c, "select day from events where id = 5 and day = 20"),
        [20]
    );
    assert_eq!(ints(&c, "select count(*) from events"), [6]);
    assert_eq!(
        rows(
            &c,
            "select day / 10, count(*) from events group by day / 10"
        ),
        vec![
            vec![Integer(0), Integer(2)],
            vec![Integer(1), Integer(2)],
            vec![Integer(2), Integer(2)]
        ]
    );
    // ORDER BY sorts: partitions are not read in key order.
    let mut stmt = c
        .clone()
        .create_statement("select id from events order by id desc limit 3")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(
        take_streaming_result(&mut stmt, 0).1,
        vec![vec![Integer(6)], vec![Integer(5)], vec![Integer(4)]]
    );
    let mut stmt = c
        .clone()
        .create_statement("select id from events order by id")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(
        take_streaming_result(&mut stmt, 0).1,
        (1..=6).map(|n| vec![Integer(n)]).collect::<Vec<_>>()
    );
}

#[test]
fn test_explain_shows_each_partition_under_an_append() {
    let c = range_conn();
    run(&c, "analyze table events").unwrap();
    assert_eq!(
        explain(&c, "select * from events"),
        "Projection id, day, note\n  Append events (3 partitions) (~6 rows)\n    \
         TableScan events partition early (< 10)\n    \
         TableScan events partition mid (< 20)\n    \
         TableScan events partition late (< 30)"
    );
    // A condition on the partition column reads only the partitions it
    // leaves.
    assert_eq!(
        explain(&c, "select note from events where id = 5 and day = 20"),
        "Projection note\n  Append events (1 of 3 partitions) (~1 rows)\n    \
         TableSeek events partition late (< 30) (id = 5 AND day = 20)"
    );
    let plan = explain(&c, "select id from events where day >= 15");
    assert!(plan.contains("Append events (2 of 3 partitions)"), "{plan}");
    assert!(!plan.contains("partition early"), "{plan}");
    let plan = explain(&c, "select id from events where day in (1, 25)");
    assert!(plan.contains("(2 of 3 partitions)"), "{plan}");
    assert!(
        plan.contains("partition early") && plan.contains("partition late"),
        "{plan}"
    );
    // Nothing can match: no partition is read at all.
    let plan = explain(&c, "select id from events where day > 100");
    assert!(plan.contains("Append events (0 of 3 partitions)"), "{plan}");
    // A condition pruning cannot use reads every partition.
    let plan = explain(&c, "select id from events where note = 'n1' or day = 5");
    assert!(plan.contains("Append events (3 partitions)"), "{plan}");
}

#[test]
fn test_pruned_reads_return_exactly_the_matching_rows() {
    let c = range_conn();
    assert_eq!(ints(&c, "select id from events where day = 10"), [3]);
    assert_eq!(ints(&c, "select id from events where day < 10"), [1, 2]);
    assert_eq!(
        ints(&c, "select id from events where day > 9 and day < 20"),
        [3, 4]
    );
    assert_eq!(
        ints(&c, "select id from events where day in (9, 20, 99)"),
        [2, 5]
    );
    assert_eq!(
        ints(&c, "select id from events where day > 100"),
        Vec::<i64>::new()
    );
    assert_eq!(ints(&c, "select count(*) from events where day > 100"), [0]);
    // The WHERE still applies within a kept partition.
    assert_eq!(
        ints(&c, "select id from events where day >= 19 and id <> 5"),
        [4, 6]
    );
    // UPDATE and DELETE prune too, and still change only matching rows.
    run(&c, "update events set note = 'x' where day >= 20").unwrap();
    assert_eq!(ints(&c, "select id from events where note = 'x'"), [5, 6]);
    run(&c, "delete from events where day < 10").unwrap();
    assert_eq!(counts(&c, "events"), [0, 2, 2]);
    // LIST.
    let c = list_conn(true);
    run(
        &c,
        "insert into regions values (1, 'ca', 10), (2, 'ny', 20), (3, 'tx', 30), (4, null, 40)",
    )
    .unwrap();
    assert_eq!(ints(&c, "select id from regions where region = 'ny'"), [2]);
    assert_eq!(
        ints(&c, "select id from regions where region = 'zz'"),
        Vec::<i64>::new()
    );
    assert_eq!(
        ints(&c, "select id from regions where region in ('ca', 'tx')"),
        [1, 3]
    );
    assert_eq!(
        ints(&c, "select id from regions where region > 'm'"),
        [2, 3]
    );
    let plan = explain(&c, "select id from regions where region = 'ny'");
    assert!(
        plan.contains("(1 of 3 partitions)") && plan.contains("partition east"),
        "{plan}"
    );
}

// Each partition read in the order wanted, the reads are merged into that
// order: no sort.
#[test]
fn test_an_ordered_read_of_several_partitions_merges_them_instead_of_sorting() {
    let c = conn();
    run(
        &c,
        "create table m (id integer not null, k integer not null, primary key(id, k)) \
         partition by range (k) (partition a values less than (100), \
         partition b values less than maxvalue)",
    )
    .unwrap();
    // ids interleave across the partitions.
    let values: Vec<String> = (0..300)
        .map(|id| format!("({id}, {})", (id * 37) % 200))
        .collect();
    run(&c, &format!("insert into m values {}", values.join(", "))).unwrap();
    run(&c, "analyze table m").unwrap();
    let plan = explain(&c, "select id from m order by id limit 5");
    assert!(plan.contains("MergeAppend m (2 partitions)"), "{plan}");
    assert!(!plan.contains("Sort") && !plan.contains("TopN"), "{plan}");
    let mut stmt = c
        .clone()
        .create_statement("select id from m order by id")
        .unwrap();
    stmt.execute().unwrap();
    let got: Vec<i64> = take_streaming_result(&mut stmt, 0)
        .1
        .into_iter()
        .map(|r| match r[0] {
            Integer(n) => n,
            ref o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(got, (0..300).collect::<Vec<_>>());
}

#[test]
fn test_joins_with_a_partitioned_table_on_either_side() {
    let c = range_conn();
    run(
        &c,
        "create table days (day integer not null, label varchar(10), primary key(day))",
    )
    .unwrap();
    for day in [5, 10, 29, 40] {
        run(&c, &format!("insert into days values ({day}, 'd{day}')")).unwrap();
    }
    assert_eq!(
        ints(&c, "select e.id from events e join days d on e.day = d.day"),
        [1, 3, 6]
    );
    // The partitioned table as the inner side: joined on its key columns,
    // which a table in one partition would be sought by per outer row.
    assert_eq!(
        ints(
            &c,
            "select e.id from days d join events e on e.day = d.day where e.id = 3"
        ),
        [3]
    );
    assert_eq!(
        rows(
            &c,
            "select d.day, e.id from days d left join events e on e.day = d.day"
        ),
        vec![
            vec![Integer(5), Integer(1)],
            vec![Integer(10), Integer(3)],
            vec![Integer(29), Integer(6)],
            vec![Integer(40), ValueItem::Null]
        ]
    );
    assert_eq!(
        ints(&c, "select e.id from events e, days d where e.day = d.day"),
        [1, 3, 6]
    );
}

#[test]
fn test_a_value_no_partition_takes_is_an_error_and_null_goes_to_the_first() {
    let c = range_conn();
    let e = err(&c, "insert into events values (7, 30, 'x')");
    assert!(e.contains("no partition for day = 30"), "{e}");
    assert_eq!(counts(&c, "events"), [2, 2, 2]);
    // The whole statement fails, its earlier rows included.
    let e = err(&c, "insert into events values (8, 1, 'x'), (9, 99, 'x')");
    assert!(e.contains("no partition"), "{e}");
    assert_eq!(ints(&c, "select count(*) from events"), [6]);

    run(
        &c,
        "create table n (id integer not null, k integer) \
         partition by range (k) (partition a values less than (10), \
         partition b values less than maxvalue)",
    )
    .unwrap();
    run(
        &c,
        "insert into n values (1, null), (2, 5), (3, 10), (4, 1000000)",
    )
    .unwrap();
    assert_eq!(counts(&c, "n"), [2, 2]);
    assert_eq!(
        ints(&c, "select id from n where k < 10"),
        [2],
        "NULL is not below 10"
    );
    assert_eq!(ints(&c, "select count(*) from n"), [4]);
}

#[test]
fn test_update_moves_a_row_whose_partition_column_changes() {
    let c = range_conn();
    run(&c, "update events set day = 25 where id = 1").unwrap();
    assert_eq!(counts(&c, "events"), [1, 2, 3]);
    assert_eq!(ints(&c, "select day from events where id = 1"), [25]);
    // Within its partition.
    run(
        &c,
        "update events set note = 'changed', day = day + 1 where id = 3",
    )
    .unwrap();
    assert_eq!(counts(&c, "events"), [1, 2, 3]);
    assert_eq!(
        ints(&c, "select day from events where note = 'changed'"),
        [11]
    );
    // Every row, each to wherever it now belongs.
    run(&c, "update events set day = day - 5").unwrap();
    assert_eq!(ints(&c, "select day from events"), [4, 6, 14, 15, 20, 24]);
    assert_eq!(counts(&c, "events"), [2, 2, 2]);
    // To a value no partition takes: nothing changes.
    let e = err(&c, "update events set day = 30 where id = 2");
    assert!(e.contains("no partition"), "{e}");
    assert_eq!(ints(&c, "select day from events where id = 2"), [4]);
    assert_eq!(counts(&c, "events"), [2, 2, 2]);
}

#[test]
fn test_delete_removes_rows_from_their_partitions() {
    let c = range_conn();
    run(&c, "delete from events where day >= 19").unwrap();
    assert_eq!(counts(&c, "events"), [2, 1, 0]);
    assert_eq!(ints(&c, "select id from events"), [1, 2, 3]);
    // The key is free again.
    run(&c, "insert into events values (4, 19, 'again')").unwrap();
    run(&c, "delete from events").unwrap();
    assert_eq!(counts(&c, "events"), [0, 0, 0]);
}

#[test]
fn test_a_rolled_back_transaction_leaves_every_partition_as_it_was() {
    let c = range_conn();
    run(&c, "begin").unwrap();
    run(
        &c,
        "insert into events values (7, 1, 'x'), (8, 15, 'x'), (9, 25, 'x')",
    )
    .unwrap();
    run(&c, "update events set day = 28 where id = 1").unwrap();
    assert_eq!(ints(&c, "select count(*) from events"), [9]);
    assert_eq!(ints(&c, "select day from events where id = 1"), [28]);
    run(&c, "rollback").unwrap();
    assert_eq!(ints(&c, "select id from events"), [1, 2, 3, 4, 5, 6]);
    assert_eq!(ints(&c, "select day from events where id = 1"), [5]);
    assert_eq!(counts(&c, "events"), [2, 2, 2]);
}

#[test]
fn test_keys_are_unique_across_partitions_because_they_include_the_partition_column() {
    let c = range_conn();
    let e = err(&c, "insert into events values (1, 5, 'dup')");
    assert!(e.to_lowercase().contains("duplicate"), "{e}");
    // The same id on another day is a different key.
    run(&c, "insert into events values (1, 15, 'ok')").unwrap();
    assert_eq!(ints(&c, "select day from events where id = 1"), [5, 15]);

    // A key that leaves the partition column out could repeat in another
    // partition unnoticed: refused.
    let e = err(
        &c,
        "create table bad (id integer not null, day integer not null, primary key(id)) \
         partition by range (day) (partition p values less than (10))",
    );
    assert!(e.contains("PRIMARY KEY must include that column"), "{e}");
    let e = err(
        &c,
        "create table bad (id integer not null, day integer not null, code integer not null, \
         primary key(id, day), unique(code)) \
         partition by range (day) (partition p values less than (10))",
    );
    assert!(e.contains("must include that column"), "{e}");
    let e = err(&c, "create unique index u on events (id)");
    assert!(e.contains("must include that column"), "{e}");
    run(&c, "create unique index u on events (day, id)").unwrap();
}

#[test]
fn test_an_index_on_a_partitioned_table_is_built_and_kept_per_partition() {
    let c = range_conn();
    run(&c, "create index by_note on events (note)").unwrap();
    let t = table(&c, "events");
    let index = t
        .indices
        .iter()
        .position(|i| i.name.as_deref() == Some("by_note"))
        .unwrap();
    let db = c.database.read().db.clone();
    let entries = |t: &SqlTable| -> Vec<usize> {
        t.partitions
            .iter()
            .map(|p| {
                let mut cursor = db.table_scan(p.index(index)).unwrap();
                let mut n = 0;
                while cursor.next().unwrap().is_some() {
                    n += 1;
                }
                n
            })
            .collect()
    };
    assert_eq!(
        entries(&t),
        [2, 2, 2],
        "backfilled from each partition's rows"
    );
    run(&c, "insert into events values (7, 12, 'n7')").unwrap();
    run(&c, "update events set day = 29 where id = 1").unwrap();
    run(&c, "delete from events where id = 2").unwrap();
    assert_eq!(entries(&t), [0, 3, 3]);
    run(&c, "analyze table events").unwrap();
    assert_eq!(ints(&c, "select id from events where note = 'n7'"), [7]);
    assert_eq!(ints(&c, "select day from events where note = 'n1'"), [29]);
}

// regions: id, region (the LIST partition column).
fn list_conn(default: bool) -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        &format!(
            "create table regions (id integer not null, region varchar(8), amount integer) \
             partition by list (region) ( \
               partition west values in ('ca', 'or', 'wa'), \
               partition east values in ('ny', 'ma'){})",
            if default {
                ", partition other default"
            } else {
                ""
            }
        ),
    )
    .unwrap();
    c
}

#[test]
fn test_a_list_partitioned_table_routes_listed_values_and_the_rest_to_default() {
    let c = list_conn(true);
    run(
        &c,
        "insert into regions values (1, 'ca', 10), (2, 'ny', 20), (3, 'wa', 30), \
         (4, 'tx', 40), (5, null, 50)",
    )
    .unwrap();
    assert_eq!(
        rows_per_partition(&c, "regions"),
        [
            ("west".to_string(), 2),
            ("east".to_string(), 1),
            ("other".to_string(), 2)
        ]
    );
    assert_eq!(ints(&c, "select id from regions where region = 'wa'"), [3]);
    assert_eq!(ints(&c, "select sum(amount) from regions"), [150]);
    run(&c, "update regions set region = 'ma' where id = 4").unwrap();
    assert_eq!(counts(&c, "regions"), [2, 2, 1]);

    // With no DEFAULT partition, an unlisted value (NULL included) has
    // nowhere to go.
    let c = list_conn(false);
    let e = err(&c, "insert into regions values (4, 'tx', 40)");
    assert!(e.contains("no partition for region = tx"), "{e}");
    let e = err(&c, "insert into regions values (5, null, 50)");
    assert!(e.contains("no partition"), "{e}");
    run(&c, "insert into regions values (1, 'or', 10)").unwrap();
    assert_eq!(counts(&c, "regions"), [1, 0]);
}

#[test]
fn test_create_table_rejects_partitions_that_do_not_split_the_values() {
    let c = conn();
    let cols = "create table t (id integer not null, k integer, s varchar(4), b bytea)";
    for (clause, wants) in [
        (
            "partition by range (nope) (partition a values less than (1))",
            "no column named",
        ),
        (
            "partition by range (b) (partition a values less than (1))",
            "cannot be partitioned by",
        ),
        (
            "partition by range (k) (partition a values less than (10), \
             partition b values less than (10))",
            "must be above",
        ),
        (
            "partition by range (k) (partition a values less than maxvalue, \
             partition b values less than (10))",
            "must be above",
        ),
        (
            "partition by range (k) (partition a values less than (1), \
             partition A values less than (2))",
            "duplicate partition name",
        ),
        (
            "partition by range (k) (partition a values in (1))",
            "VALUES LESS THAN",
        ),
        (
            "partition by range (k) (partition a values less than ('x'))",
            "",
        ),
        (
            "partition by range (k) (partition a values less than (null))",
            "cannot be NULL",
        ),
        (
            "partition by list (k) (partition a values less than (1))",
            "VALUES IN or DEFAULT",
        ),
        (
            "partition by list (k) (partition a values in (1, 2), partition b values in (2))",
            "more than one partition",
        ),
        (
            "partition by list (k) (partition a default, partition b default)",
            "only one DEFAULT",
        ),
    ] {
        let e = err(&c, &format!("{cols} {clause}"));
        assert!(e.contains(wants), "{clause}: {e}");
        assert!(
            c.current_schema().unwrap().get_table("t").is_none(),
            "{clause}: nothing was created"
        );
    }
    // A partitioned temp table is refused.
    let e = err(
        &c,
        "create table temp.t (k integer) partition by range (k) \
         (partition a values less than (1))",
    );
    assert!(e.contains("PARTITION BY"), "{e}");
}

// Statements differing only in literals share one parse (sql_parser's
// shape cache), with each statement's own literals bound back in: a
// partition's bounds are such literals.
#[test]
fn test_partition_bounds_are_each_statements_own_literals() {
    let c = conn();
    for (name, bound) in [("a", 10), ("b", 500)] {
        run(
            &c,
            &format!(
                "create table {name} (k integer) partition by range (k) \
                 (partition p values less than ({bound}))"
            ),
        )
        .unwrap();
        assert_eq!(
            table(&c, name).partitions[0].bound,
            PartitionBound::LessThan(Integer(bound))
        );
    }
    run(&c, "alter table a add partition q values less than (20)").unwrap();
    run(&c, "alter table b add partition q values less than (600)").unwrap();
    assert_eq!(
        table(&c, "a").partitions[1].bound,
        PartitionBound::LessThan(Integer(20))
    );
    assert_eq!(
        table(&c, "b").partitions[1].bound,
        PartitionBound::LessThan(Integer(600))
    );
}

#[test]
fn test_add_partition_extends_a_range_above_its_highest_bound() {
    let c = range_conn();
    run(&c, "create index by_note on events (note)").unwrap();
    for (sql, wants) in [
        (
            "alter table events add partition x values less than (30)",
            "must be above",
        ),
        (
            "alter table events add partition x values less than (15)",
            "must be above",
        ),
        (
            "alter table events add partition late values less than (40)",
            "duplicate partition",
        ),
        (
            "alter table events add partition x values in (40)",
            "VALUES LESS THAN",
        ),
        (
            "alter table events add partition x default",
            "VALUES LESS THAN",
        ),
    ] {
        let e = err(&c, sql);
        assert!(e.contains(wants), "{sql}: {e}");
    }
    assert_eq!(table(&c, "events").partitions.len(), 3);

    run(
        &c,
        "alter table events add partition later values less than (40)",
    )
    .unwrap();
    run(&c, "insert into events values (7, 30, 'n7'), (8, 39, 'n8')").unwrap();
    assert_eq!(counts(&c, "events"), [2, 2, 2, 2]);
    assert_eq!(ints(&c, "select id from events where day >= 30"), [7, 8]);
    // The new partition has a tree for every index the table has.
    let t = table(&c, "events");
    assert_ne!(t.partitions[3].index(1), store::table::TableIdType::none());
    assert_ne!(t.partitions[3].index(1), t.partitions[2].index(1));

    run(
        &c,
        "alter table events add partition rest values less than maxvalue",
    )
    .unwrap();
    run(&c, "insert into events values (9, 1000, 'n9')").unwrap();
    let e = err(
        &c,
        "alter table events add partition beyond values less than (5000)",
    );
    assert!(e.contains("MAXVALUE can only be the last"), "{e}");
    assert_eq!(counts(&c, "events"), [2, 2, 2, 2, 1]);

    run(&c, "create table plain (k integer)").unwrap();
    let e = err(&c, "alter table plain add partition p values less than (1)");
    assert!(e.contains("is not partitioned"), "{e}");
}

#[test]
fn test_add_partition_to_a_list_takes_only_values_nothing_else_holds() {
    let c = list_conn(true);
    run(
        &c,
        "insert into regions values (1, 'tx', 10), (2, 'ca', 20)",
    )
    .unwrap();
    let e = err(&c, "alter table regions add partition dup values in ('ny')");
    assert!(e.contains("more than one partition"), "{e}");
    let e = err(&c, "alter table regions add partition two default");
    assert!(e.contains("only one DEFAULT"), "{e}");
    // Rows with the value are already in the DEFAULT partition.
    let e = err(
        &c,
        "alter table regions add partition south values in ('fl', 'tx')",
    );
    assert!(e.contains("already holds rows with region = tx"), "{e}");
    assert_eq!(table(&c, "regions").partitions.len(), 3);

    run(
        &c,
        "alter table regions add partition south values in ('fl', 'ga')",
    )
    .unwrap();
    run(&c, "insert into regions values (3, 'fl', 30)").unwrap();
    assert_eq!(
        rows_per_partition(&c, "regions"),
        [
            ("west".to_string(), 1),
            ("east".to_string(), 0),
            ("other".to_string(), 1),
            ("south".to_string(), 1)
        ]
    );
}

#[test]
fn test_drop_partition_removes_it_and_its_rows() {
    let c = range_conn();
    run(&c, "alter table events drop partition mid").unwrap();
    assert_eq!(ints(&c, "select id from events"), [1, 2, 5, 6]);
    assert_eq!(
        rows_per_partition(&c, "events"),
        [("early".to_string(), 2), ("late".to_string(), 2)]
    );
    // What was mid's range is now late's: everything below 30 and not
    // below early's bound.
    run(&c, "insert into events values (7, 15, 'n7')").unwrap();
    assert_eq!(counts(&c, "events"), [2, 3]);

    let e = err(&c, "alter table events drop partition nope");
    assert!(e.contains("no partition named"), "{e}");
    run(&c, "alter table events drop partition LATE").unwrap();
    assert_eq!(ints(&c, "select id from events"), [1, 2]);
    let e = err(&c, "insert into events values (8, 15, 'n8')");
    assert!(e.contains("no partition"), "{e}");
    let e = err(&c, "alter table events drop partition early");
    assert!(e.contains("only partition"), "{e}");

    // A partition added under a dropped one's name is a new, empty one.
    run(
        &c,
        "alter table events add partition mid values less than (20)",
    )
    .unwrap();
    assert_eq!(counts(&c, "events"), [2, 0]);
    run(&c, "insert into events values (3, 10, 'back')").unwrap();
    assert_eq!(ints(&c, "select id from events"), [1, 2, 3]);

    run(&c, "create table plain (k integer)").unwrap();
    let e = err(&c, "alter table plain drop partition p");
    assert!(e.contains("is not partitioned"), "{e}");
}

#[test]
fn test_the_partition_column_cannot_be_dropped_but_other_columns_alter_as_usual() {
    let c = list_conn(true);
    run(
        &c,
        "insert into regions values (1, 'ca', 10), (2, 'tx', 20)",
    )
    .unwrap();
    let e = err(&c, "alter table regions drop column region");
    assert!(e.contains("is partitioned by"), "{e}");
    run(&c, "alter table regions drop column amount").unwrap();
    run(
        &c,
        "alter table regions add column note varchar(4) default 'x'",
    )
    .unwrap();
    // Renamed, it is still the partition column (by field id).
    run(&c, "alter table regions rename column region to area").unwrap();
    run(
        &c,
        "insert into regions values (3, 'ny', 'y'), (4, 'zz', 'z')",
    )
    .unwrap();
    assert_eq!(counts(&c, "regions"), [1, 1, 2]);
    assert_eq!(ints(&c, "select id from regions where note = 'x'"), [1, 2]);
    assert_eq!(ints(&c, "select id from regions where area = 'ny'"), [3]);
}

#[test]
fn test_a_foreign_key_finds_its_row_in_whichever_partition_holds_it() {
    let c = conn();
    run(
        &c,
        "create table parents (pid integer not null, primary key(pid)) \
         partition by range (pid) (partition a values less than (10), \
         partition b values less than (20))",
    )
    .unwrap();
    run(&c, "insert into parents values (1), (15)").unwrap();
    run(
        &c,
        "create table kids (id integer not null, parent integer references parents(pid))",
    )
    .unwrap();
    run(&c, "insert into kids values (1, 1), (2, 15), (3, null)").unwrap();
    let e = err(&c, "insert into kids values (4, 7)");
    assert!(e.contains("violates foreign key"), "{e}");

    run(
        &c,
        "create table more (id integer not null, parent integer)",
    )
    .unwrap();
    run(&c, "insert into more values (1, 15)").unwrap();
    run(
        &c,
        "alter table more add foreign key (parent) references parents(pid)",
    )
    .unwrap();
    run(&c, "insert into more values (2, 16)").unwrap_err();
}

#[test]
fn test_analyze_counts_the_rows_of_every_partition() {
    let c = range_conn();
    run(&c, "analyze table events").unwrap();
    let schema = c.current_schema().unwrap();
    let stat = schema
        .clone()
        .get_table_stats(table(&c, "events").id)
        .unwrap()
        .unwrap();
    assert_eq!(stat.row_count, 6);
    // The table's statistics stay its own when the partition they were
    // first keyed by is dropped.
    run(&c, "alter table events drop partition early").unwrap();
    run(&c, "analyze table events").unwrap();
    let stat = schema
        .get_table_stats(table(&c, "events").id)
        .unwrap()
        .unwrap();
    assert_eq!(stat.row_count, 4);
}

// big: 3000 rows in three RANGE partitions on id (none above 3000), with an
// index on k; few: a handful of keys. A join from few into big seeks big per
// row rather than reading it all.
fn seek_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    run(
        &c,
        "create table big (id integer not null, k integer, u integer, primary key(id)) \
         partition by range (id) (partition b0 values less than (1000), \
         partition b1 values less than (2000), partition b2 values less than (3000))",
    )
    .unwrap();
    for chunk in 0..6 {
        let values: Vec<String> = (chunk * 500..(chunk + 1) * 500)
            .map(|id| format!("({id}, {}, {})", id % 50, id * 7))
            .collect();
        run(&c, &format!("insert into big values {}", values.join(", "))).unwrap();
    }
    run(&c, "create index big_k on big (k)").unwrap();
    // Distinct values, but not the partition column: a unique index would
    // be refused, so a plain one.
    run(&c, "create index big_u on big (u)").unwrap();
    run(&c, "create table few (n integer not null, primary key(n))").unwrap();
    run(&c, "insert into few values (5), (1500), (2999), (7000)").unwrap();
    run(&c, "analyze table big").unwrap();
    run(&c, "analyze table few").unwrap();
    c
}

#[test]
fn test_a_join_seeks_into_a_partitioned_table_routing_by_its_key() {
    let c = seek_conn();
    // On the partition column: each row seeks the one partition it routes
    // to — and 7000, which none takes, seeks nothing.
    let plan = explain(&c, "select f.n, b.k from few f join big b on b.id = f.n");
    assert!(plan.contains("NestedLoopJoin"), "{plan}");
    assert!(
        plan.contains("the one of 3 partitions its key routes to"),
        "{plan}"
    );
    assert_eq!(
        rows(&c, "select f.n, b.k from few f join big b on b.id = f.n"),
        vec![
            vec![Integer(5), Integer(5)],
            vec![Integer(1500), Integer(0)],
            vec![Integer(2999), Integer(49)]
        ]
    );
    assert_eq!(
        rows(
            &c,
            "select f.n, b.k from few f left join big b on b.id = f.n"
        ),
        vec![
            vec![Integer(5), Integer(5)],
            vec![Integer(1500), Integer(0)],
            vec![Integer(2999), Integer(49)],
            vec![Integer(7000), ValueItem::Null]
        ]
    );
    // On another column: each row seeks every partition its WHERE leaves.
    // few.n * 7 = big.u for 5 and 1500 (ids 5 and 1500), none else.
    run(
        &c,
        "create table sevens (u integer not null, primary key(u))",
    )
    .unwrap();
    run(&c, "insert into sevens values (35), (10500), (20993), (2)").unwrap();
    run(&c, "analyze table sevens").unwrap();
    let sql = "select s.u, b.id from sevens s join big b on b.u = s.u";
    let plan = explain(&c, sql);
    assert!(plan.contains("NestedLoopJoin"), "{plan}");
    assert!(plan.contains("each of 3 partitions"), "{plan}");
    assert_eq!(
        rows(&c, sql),
        vec![
            vec![Integer(35), Integer(5)],
            vec![Integer(10500), Integer(1500)],
            vec![Integer(20993), Integer(2999)]
        ]
    );
    // Pruned to one 1000-row partition, reading it beats four seeks: the
    // plan may be either; the rows are these.
    let sql = "select s.u, b.id from sevens s join big b on b.u = s.u where b.id < 1000";
    assert_eq!(rows(&c, sql), vec![vec![Integer(35), Integer(5)]]);
    let sql = "select s.u, b.id from sevens s left join big b on b.u = s.u";
    assert_eq!(rows(&c, sql).len(), 4);
    // Many matches per key: whichever join wins, the same rows.
    assert_eq!(
        ints(&c, "select count(*) from few f join big b on b.k = f.n"),
        [60]
    );
}

// Partitions of very different sizes: a pruned read is estimated by the
// rows of the partitions it reads, not as a share of the table.
#[test]
fn test_a_pruned_read_is_estimated_by_the_rows_of_its_partitions() {
    let c = conn();
    run(
        &c,
        "create table skew (id integer not null, k integer not null, v integer, \
         primary key(id, k)) partition by range (k) (partition small values less than (10), \
         partition large values less than maxvalue)",
    )
    .unwrap();
    // 5 rows in small, 995 in large.
    let values: Vec<String> = (0..1000)
        .map(|id| format!("({id}, {}, {id})", if id < 5 { id } else { 10 + id }))
        .collect();
    run(
        &c,
        &format!("insert into skew values {}", values.join(", ")),
    )
    .unwrap();
    run(&c, "analyze table skew").unwrap();
    let estimate = |sql: &str| -> usize {
        let plan = explain(&c, sql);
        let line = plan
            .lines()
            .find(|l| l.contains("Append skew"))
            .unwrap_or_else(|| panic!("{plan}"));
        let n = line.rsplit("(~").next().unwrap().trim_end_matches(" rows)");
        n.parse().unwrap_or_else(|_| panic!("{plan}"))
    };
    assert_eq!(estimate("select v from skew where k < 10"), 5);
    assert_eq!(estimate("select v from skew where k >= 10"), 995);
    assert_eq!(estimate("select v from skew"), 1000);

    // The counts follow a dropped partition, and survive a reload.
    run(&c, "alter table skew drop partition large").unwrap();
    assert_eq!(estimate("select v from skew"), 5);
    let schema = c.current_schema().unwrap();
    schema.persist_and_shutdown_stats().ok();
    let reloaded = crate::schema_ops::schema::Schema::<MemFile>::load(
        DEFAULT_SCHEMA_NAME.to_string(),
        schema.db.clone(),
    )
    .unwrap();
    let table = reloaded.get_table("skew").unwrap();
    let stat = reloaded.clone().get_table_stats(table.id).unwrap().unwrap();
    assert_eq!(stat.row_count, 5);
    assert_eq!(stat.partition_rows.get(&table.partitions[0].id), Some(&5));
    reloaded.persist_and_shutdown_stats().unwrap();
}

#[test]
fn test_show_partitions_lists_each_partition_its_values_and_rows() {
    let c = range_conn();
    run(&c, "analyze table events").unwrap();
    let mut stmt = c.clone().create_statement("show partitions from events").unwrap();
    stmt.execute().unwrap();
    let ResultType::Result(rs) = nth_result(&stmt, 0) else {
        panic!("expected a result set");
    };
    let s = |v: &str| ValueItem::Str((v.into(), crate::constant::DEFAULT_VAR_SIZE as u32));
    assert_eq!(rs.columns(), ["Partition", "Values", "Rows"]);
    assert_eq!(
        rs.rows(),
        [
            vec![s("early"), s("< 10"), Integer(2)],
            vec![s("mid"), s("< 20"), Integer(2)],
            vec![s("late"), s("< 30"), Integer(2)]
        ]
    );
    let c = list_conn(true);
    let mut stmt = c.clone().create_statement("show partitions regions").unwrap();
    stmt.execute().unwrap();
    let ResultType::Result(rs) = nth_result(&stmt, 0) else {
        panic!("expected a result set");
    };
    assert_eq!(rs.rows()[0][1], s("in 'ca', 'or', 'wa'"));
    assert_eq!(rs.rows()[2][1], s("default"));
    run(&c, "create table plain (k integer)").unwrap();
    let mut stmt = c.clone().create_statement("show partitions plain").unwrap();
    stmt.execute().unwrap();
    let ResultType::Result(rs) = nth_result(&stmt, 0) else {
        panic!("expected a result set");
    };
    assert_eq!(rs.rows().len(), 1);
    assert_eq!(rs.rows()[0][1], s("all rows"));
}

// Ordered by a RANGE table's partition column, the partitions in bound
// order are the order: read one after the other, no merge.
#[test]
fn test_an_order_leading_with_the_range_column_reads_partitions_in_turn() {
    let c = conn();
    run(
        &c,
        "create table r (k integer not null, id integer not null, primary key(k, id)) \
         partition by range (k) (partition a values less than (100), \
         partition b values less than maxvalue)",
    )
    .unwrap();
    let values: Vec<String> = (0..300).map(|i| format!("({}, {i})", (i * 37) % 200)).collect();
    run(&c, &format!("insert into r values {}", values.join(", "))).unwrap();
    run(&c, "analyze table r").unwrap();
    let plan = explain(&c, "select k from r order by k limit 3");
    assert!(plan.contains("Append r (2 partitions)"), "{plan}");
    assert!(!plan.contains("MergeAppend") && !plan.contains("Sort"), "{plan}");
    let mut stmt = c.clone().create_statement("select k, id from r order by k, id").unwrap();
    stmt.execute().unwrap();
    let got = take_streaming_result(&mut stmt, 0).1;
    let mut want = got.clone();
    want.sort();
    assert_eq!(got, want);
    assert_eq!(got.len(), 300);
}

// The statistics a pruned read is planned with: the partition column's
// range narrowed to the kept partitions, distinct counts capped by their
// rows.
#[test]
fn test_a_pruned_reads_column_statistics_are_narrowed_to_its_partitions() {
    let c = range_conn();
    run(&c, "analyze table events").unwrap();
    let table = table(&c, "events");
    let stats = crate::optim::table_stats::compute_table_stats(&c, DEFAULT_SCHEMA_NAME, &table)
        .unwrap()
        .unwrap();
    let mid = crate::optim::table_stats::for_partitions(&table, &stats, &[1]);
    assert_eq!(mid.table_stat.row_count, 2);
    let day = &mid.table_stat.col_stats[&1];
    // The bound, exclusive, as the highest value: a limit, for estimates.
    assert_eq!((day.min.clone(), day.max.clone()), (Integer(10), Integer(20)));
    assert!(mid.table_stat.col_stats.values().all(|c| c.unique <= 2));
    let late = crate::optim::table_stats::for_partitions(&table, &stats, &[1, 2]);
    let day = &late.table_stat.col_stats[&1];
    assert_eq!((day.min.clone(), day.max.clone()), (Integer(10), Integer(29)));
}
