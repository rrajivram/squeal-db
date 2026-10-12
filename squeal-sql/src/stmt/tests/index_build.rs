// CREATE INDEX on a table with rows sorts their entries and inserts them in
// key order (see Schema::create_index): within the query's memory, or
// spilling past it.
use super::partition_races::storage_problems;
use super::*;
use crate::{CreateConfig, OpenConfig};

fn conn_with(query_memory: u64) -> Arc<Connection<MemFile>> {
    let config = CreateConfig::default().open(OpenConfig::default().query_memory_bytes(query_memory));
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::with_config(config));
    let c = mgr.create_and_connect("index_build_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn load(c: &Arc<Connection<MemFile>>, table: &str, rows: usize) {
    // n and s in no order the rows' ids give them: a pseudo-random walk.
    let mut seed = 0x5EED_u64;
    let mut next = move |below: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % below
    };
    let values: Vec<String> = (0..rows)
        .map(|i| {
            let s = if i % 11 == 0 { "null".to_string() } else { format!("'s{}'", next(997)) };
            format!("({i}, {}, {s})", next(5003))
        })
        .collect();
    for chunk in values.chunks(1000) {
        run(c, &format!("insert into {table} values {}", chunk.join(", "))).unwrap();
    }
}

fn count(c: &Arc<Connection<MemFile>>, sql: &str) -> i64 {
    match select_rows(c, sql).1[0][0] {
        ValueItem::Integer(n) => n,
        ref v => panic!("{sql}: {v:?}"),
    }
}

// Within memory and spilling past it, the index holds an entry for every
// row and nothing else, and answers as a scan does.
#[test]
fn test_an_index_built_by_sorting_holds_every_row_in_or_out_of_memory() {
    for query_memory in [64 << 20, 16 * 1024] {
        let c = conn_with(query_memory);
        run(
            &c,
            "create table t (id integer not null, n integer, s varchar(8), primary key(id))",
        )
        .unwrap();
        load(&c, "t", 20_000);
        run(&c, "create index t_n on t (n)").unwrap();
        run(&c, "create index t_s on t (s, n)").unwrap();
        assert!(storage_problems(&c, "t").is_empty(), "{:?}", storage_problems(&c, "t"));
        let plan = explain(&c, "select n from t where n = 17");
        assert!(plan.contains("Index"), "{plan}");
        for (indexed, scanned) in [
            ("n = 17", "n + 0 = 17"),
            ("n >= 4000", "n + 0 >= 4000"),
            ("s = 's5'", "s || '' = 's5'"),
            ("s is null", "s || '' is null"),
        ] {
            let q = |w: &str| count(&c, &format!("select count(*) from t where {w}"));
            assert_eq!(q(indexed), q(scanned), "{indexed}, query memory {query_memory}");
        }
    }
}

// A table without a primary key: its entries carry the generated row id.
#[test]
fn test_an_index_on_a_table_without_a_primary_key_points_at_its_rows() {
    let c = conn_with(16 * 1024);
    run(&c, "create table t (id integer, n integer, s varchar(8))").unwrap();
    load(&c, "t", 5_000);
    run(&c, "create index t_n on t (n)").unwrap();
    assert!(storage_problems(&c, "t").is_empty(), "{:?}", storage_problems(&c, "t"));
    assert_eq!(
        count(&c, "select count(*) from t where n = 17"),
        count(&c, "select count(*) from t where n + 0 = 17")
    );
}

// A duplicate is found however the sort ran, and leaves no index behind.
#[test]
fn test_a_unique_index_over_duplicates_is_refused_in_or_out_of_memory() {
    for query_memory in [64 << 20, 16 * 1024] {
        let c = conn_with(query_memory);
        run(&c, "create table t (id integer not null, n integer, s varchar(8), primary key(id))").unwrap();
        load(&c, "t", 20_000);
        let err = run(&c, "create unique index t_n on t (n)").unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "{err:?}");
        let table = c.current_schema().unwrap().get_table("t").unwrap();
        assert_eq!(table.indices.len(), 1, "only the primary key");
        run(&c, "create unique index t_id on t (id)").unwrap();
        assert!(storage_problems(&c, "t").is_empty());
    }
}

// Each partition's entries go to its own tree.
#[test]
fn test_an_index_on_a_partitioned_table_is_built_per_partition() {
    let c = conn_with(16 * 1024);
    run(
        &c,
        "create table t (id integer not null, n integer, s varchar(8), primary key(id)) \
         partition by range (id) (partition a values less than (7000), \
         partition b values less than (14000), partition c values less than maxvalue)",
    )
    .unwrap();
    load(&c, "t", 20_000);
    run(&c, "create index t_n on t (n)").unwrap();
    assert!(storage_problems(&c, "t").is_empty(), "{:?}", storage_problems(&c, "t"));
    assert_eq!(
        count(&c, "select count(*) from t where n < 100"),
        count(&c, "select count(*) from t where n + 0 < 100")
    );
}

// Loaded in key order, the index fills its leaves: the same rows and index
// take fewer pages than when the index is filled by inserts in the rows'
// order, whose keys land anywhere.
#[test]
fn test_an_index_built_in_key_order_fills_its_pages() {
    let pages = |index_first: bool| {
        let c = conn_with(64 << 20);
        run(&c, "create table t (id integer not null, n integer, s varchar(8), primary key(id))").unwrap();
        let empty = c.database.read().db.page_count();
        if index_first {
            run(&c, "create index t_n on t (n, s)").unwrap();
        }
        load(&c, "t", 30_000);
        if !index_first {
            run(&c, "create index t_n on t (n, s)").unwrap();
        }
        c.database.read().db.page_count() - empty
    };
    let table = {
        let c = conn_with(64 << 20);
        run(&c, "create table t (id integer not null, n integer, s varchar(8), primary key(id))").unwrap();
        let empty = c.database.read().db.page_count();
        load(&c, "t", 30_000);
        c.database.read().db.page_count() - empty
    };
    let sorted = pages(false) - table;
    let inserted = pages(true) - table;
    println!("index pages: sorted build {sorted}, filled by inserts {inserted}");
    // The data pages an index entry is also written to fill either way, so
    // the difference is the leaves' (the store's own test measures those).
    assert!(sorted * 100 < inserted * 97, "sorted build {sorted} pages, filled by inserts {inserted}");
}
