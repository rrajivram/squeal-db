# Open items

Measured on a generated retail database (1M order details), warm, release
build — see `squeal-sql/src/stmt/tests/layers.rs` to re-run.

## Where a primary key lookup's time goes (SQL: 33 us)

- [x] **Parse (~23 us of 33).** Done: texts differing only in literals share
  one parse (the shape cache in `sql_parser::parse_sql_cached`). Lookup now
  14.7 us with a new literal, 6.9 us repeated verbatim.
- [x] **Shape-cache hit** 6.1 -> 1.0 us: a hand-written lexer (lexing was
  5.5 us of it), checked against the chumsky one by a fuzz test.
- [x] **squeal-sql planning/execution** of a repeated-text lookup: 8.0 ->
  5.2 us (store's part 1.3 us). Gone: a getentropy syscall per statement
  (Uuid::new_v4), EXPLAIN text built on every seek, column statistics
  rebuilt and copied on every plan. Lookup with a new literal: 16.7 -> 7.9
  us. Measured on a scratch retail database in /tmp (1M details).
- [x] **Read-only transaction commit (~9.5 us of the remaining 10).** Done:
  `Logger::sync_pending` skips the writer round trip when every queued record
  is synced (counted, not by LSN — records reach the writer out of LSN order).
- [x] **Store lookup itself** (`Db::find`) 0.75 -> 0.54 us: pages' sorted
  Vec with inline key prefixes (below).

## Range scans (100k rows: SQL 46 -> 21 ms, store 18 -> 9.3 ms)

- [x] squeal-sql's per-row work: 100k-row count 46 -> 33 ms (store: 18 ms).
  Per-row clock reads now sampled; covering index scans fill only the
  columns read; aggregates build one output row per group. What is left is
  mostly store's cursor and the per-row Vec/Arc of a row.
- [x] Page redesign, part 1: profiled first — over half of store's scan was
  the data-page lookup per row (a BTreeMap of IndexKeys, a memcmp per
  comparison), a tenth SipHash of page ids; copying the leaf out was 7%.
  Now `AnyTuplePage` is a sorted Vec (same bytes on disk) with each key's
  leading bits inline (`order_prefix`), so most probes never follow the
  key's pointers; a range cursor tells the page where it expects the next
  row (`get_hinted`); page ids hash with a multiply. Store range 18 -> 9.3
  ms, SQL count 32 -> 21 ms, `Db::find` 0.75 -> 0.54 us, SQL lookup 8.2 ->
  7.3 us. Cost: a mid-page insert shifts the Vec (80-byte Tuples + 16-byte
  prefixes) — random bulk inserts ~25% slower on the memory backend (2.2 ->
  2.75 s for 500k); on the file backend it is lost in the noise.
- [ ] Page redesign, part 2: `Arc<Tuple>` entries (an insert shifts 8 bytes
  a row, not 80) and copy-on-write page snapshots (`Page::iter` without a
  copy). Measured separately, to keep or drop.
- [ ] `TableCursor::new` still copies a whole data page when a scan starts
  (`Page::iter`). Making it lazy changes what a scan sees when rows relocate
  mid-scan — needs its own look.

## Cold reads

- [ ] A store-cache miss decodes the whole page; leaves now hold ~6x more
  entries, so first-touch random lookups are ~15-20% slower in a fresh
  process (warm ones are faster). Fix: pages readable without a full decode
  (slotted layout / lazy decode). Store change.

## Bugs

- [x] Stats collector panic after ALTER TABLE (fixed: stats re-keyed by
  field id on ALTER; short rows skipped).
- [x] `GROUP BY` with no aggregate and no `HAVING` did not group (fixed).
- [x] `GROUP BY <expression>` was dropped from the group key (fixed: computed
  into a column the sort and grouping read; the SELECT list and HAVING may
  use the same expression).

## Known-failure tests

Written first as failing tests, then fixed; all now run in the normal suite.
A new known failure goes in the same files, `#[ignore]`d with its reason.

- [x] `stmt/tests/partition_races.rs` (7): DDL vs DML/DDL, forced
  interleavings (`src/testhook.rs`). Fixed by table locks
  (`conn/tablelock.rs`).
- [x] `stmt/tests/partition_diff.rs` (4 queries): joins into a partitioned
  table with more than column equalities in ON. Fixed by residual ON terms in
  the hash join.
- [x] `stmt/tests/sql_gaps.rs` (9): non-equality ON terms, inequality and
  expression joins, USING, WITH, IS [NOT] NULL, DROP TABLE, TRUNCATE.

Follow-ups from the fixes:
- [x] Table locks are per process (`Schema::locks`): by design — a database
  is opened by one process at a time, which is all squeal supports.
- [x] Lock waits detect deadlocks (wait-for graph; the latest waiter in a
  cycle is refused); the 10 s timeout is only a backstop.
- [ ] DROP TABLE, like DROP PARTITION, leaves its trees in the store
  unreferenced; a later table of the same name gets `name~N` trees. Reclaiming
  them needs a store change.
- [x] An inequality join on a key or indexed column seeks a range per outer
  row. Other joins with no column equality still try every pair.
- [x] A WITH query read more than once runs once; WITH RECURSIVE works.
- [x] UNION [ALL] / INTERSECT / EXCEPT (UNION ALL used to drop its second
  query's rows) and OFFSET (was ignored).

Test tools (all in `squeal-sql/src/stmt/tests/`):

- `oracle.rs` — seeded random queries answered by squeal on plain tables,
  squeal on partitioned twins, and SQLite (`rusqlite`, dev-dependency); all
  three must agree. 600 queries in the suite;
  `SQ_ORACLE_QUERIES=20000 SQ_ORACLE_SEED=7 cargo test --release -p squeal-sql
  --lib oracle::test_random -- --nocapture` for a long run.
- `soak.rs` — concurrent transfers, partition moves, reads and DDL, with
  crash rounds (snapshot, reopen, check totals / ledger / durability /
  storage). 3 s in the suite; `SQ_SOAK_SECS=120 cargo test --release -p
  squeal-sql --lib soak_long -- --ignored --nocapture` for a long run.
- `partition_races.rs` (forced interleavings), `partition_diff.rs` (plain vs
  partitioned, fixed queries), `sql_gaps.rs`.

Found by the oracle and fixed: COUNT(x) counted NULLs; UNION ALL dropped its
second query; OFFSET ignored; ORDER BY position, BETWEEN, LIKE, COALESCE,
CASE, abs() missing.

Still outside the oracle's grammar (squeal does not run them, so they would
be findings): ORDER BY an expression; SUM/AVG(DISTINCT); scalar subqueries,
IN (subquery), EXISTS; window functions; CAST.

## Partitions

Done: every table is a list of partitions with their own trees
(`squeal-sql/src/partition.rs`); `CREATE TABLE ... PARTITION BY RANGE|LIST`,
`ALTER TABLE ... ADD|DROP PARTITION`; INSERT/UPDATE/DELETE/COPY route rows by
the partition column; queries read every partition (`source/append.rs`).

- [x] Pruning: partitions a WHERE rules out are not read (SELECT, UPDATE,
  DELETE); EXPLAIN shows `(k of n partitions)`.
- [x] Order across partitions: per-partition ordered reads are merged
  (`MergeAppend`), so ORDER BY / merge joins / GROUP BY need no sort.
- [x] RANGE partitions are read in turn (no merge) for an order leading with
  the partition column.
- [x] A hash join skips the probe side's partitions none of its build keys
  route to.
- [x] Join seeks into partitioned tables: each partition sought, or only the
  one the key routes to when the key has the partition column.
- [x] Row counts per partition (stats row format version 2); pruned reads are
  estimated and costed by them; DROP PARTITION takes its rows off the count.
- [x] Pruned reads get column statistics narrowed to their partitions; tree
  shape is the deepest partition's.
- [ ] DROP PARTITION leaves the partition's trees in the store, unreferenced
  (their pages are not reused). Reclaiming them needs a `drop_table` in store
  that is safe against a scan still reading the tree. Store change.
- [x] `SHOW PARTITIONS [FROM] t`.
- [ ] External partitions (Parquet): a new `PartitionStorage` variant.

## Flaky tests

- [x] `squeal-sql` `source::sortjoin` spill tests (`QueryMemoryExceeded`, 2 in
  40 full-suite runs). Cause: the sort join sorts both sides on two threads
  against one memory budget, and a sort failed outright when the other had
  taken all of it (its first page's reservation used `?`). Fixed: the sort
  then starts out of memory; pinned by
  `sort::budget_tests::test_a_sort_whose_budget_another_operator_holds_still_sorts`.
- [x] `store` crash harness: two bugs, both fixed. Reproduce with the soak
  under contention (24 `examples/crash` processes at once, debug build):
  it failed 4-5 of 48 processes before, 0 of 144 since.
  - recovery lost a committed change that looked like an aborted one in the
    checkpoint image (redo's "already applied" ignored who wrote the row);
  - a reader could read a rolled-back write (a finished abort left no state,
    and no state means "committed long ago").
- [x] `store` `test_audit_t14...` / `Corruption("version record missing for
  pre_lsn ...")`: fixed — three windows between minting an id or commit
  timestamp and registering it (see the commit). 0 in 30 full runs since.
