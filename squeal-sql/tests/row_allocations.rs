// A scan that filters, aggregates or groups reads its rows where the table
// scan put them (see source::Source: rows are lent, not handed out owned):
// it allocates nothing per row.
//
// Each operator used to build a row for every input row — two allocations
// and one more per string, to read a column or two of it — which a count(*)
// did 2 of and a GROUP BY 6. A binary of its own and one test, so no other
// test allocates while it counts.
use std::sync::Arc;

use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::ResultType,
};
use store::memfile::MemFile;

// Allocations so far, by the store's counting allocator (its
// `alloc-tracking` feature, on for this crate's tests).
fn allocations() -> u64 {
    let stats = store::alloc::stats();
    (stats.size_histogram.iter().sum::<usize>() + stats.realloc_count) as u64
}

fn run(c: &Arc<Connection<MemFile>>, sql: &str) -> usize {
    let mut stmt = c.clone().create_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    stmt.execute().unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = 0;
    while let Some(result) = stmt.get_nextresult().unwrap() {
        if let ResultType::StreamingResult(mut set) = result {
            while set.next_result().unwrap().is_some() {
                rows += 1;
            }
        }
    }
    rows
}

#[test]
fn test_scans_that_filter_aggregate_or_group_allocate_nothing_per_row() {
    const ROWS: usize = 20_000;
    let manager = Arc::new(ConnectionManager::<MemFile>::new());
    let c = manager.create_and_connect("row_allocations_db").unwrap();
    c.use_schema("default").unwrap();
    run(
        &c,
        "create table t (id integer not null, n integer, item varchar(12), price double, \
         primary key(id))",
    );
    let items = ["pen", "ink", "pad", "stapler", "clip"];
    let values: Vec<String> = (0..ROWS)
        .map(|i| format!("({i}, {}, '{}', {}.5)", i % 97, items[i % items.len()], i % 1000))
        .collect();
    for chunk in values.chunks(1000) {
        run(&c, &format!("insert into t values {}", chunk.join(", ")));
    }

    for sql in [
        "select count(*) from t",
        "select sum(price) from t",
        "select count(*) from t where n = 5",
        "select count(*) from t where item = 'pen'",
        "select sum(price) from t where n + 0 < 50 and item <> 'ink'",
        "select max(item), min(n) from t",
        "select item, count(*), sum(n) from t group by item",
        "select n, count(*) from t where item = 'clip' group by n",
    ] {
        // Once first: the parse, anything built once and kept.
        run(&c, sql);
        let before = allocations();
        run(&c, sql);
        let allocations = allocations() - before;
        // The statement itself (its plan, its few output rows) allocates a
        // few hundred times, whatever the table's size: far below one per
        // row, which is what a row built per input row would show.
        assert!(
            allocations < ROWS as u64 / 10,
            "{sql}: {allocations} allocations for {ROWS} rows"
        );
    }
}
