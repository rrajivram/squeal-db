# Open items

Measured on a generated retail database (1M order details), warm, release
build — see `squeal-sql/src/stmt/tests/layers.rs` to re-run.

## Where a primary key lookup's time goes (SQL: 33 us)

- [x] **Parse (~23 us of 33).** Done: texts differing only in literals share
  one parse (the shape cache in `sql_parser::parse_sql_cached`). Lookup now
  14.7 us with a new literal, 6.9 us repeated verbatim.
- [ ] **Shape-cache hit costs ~7.7 us** (new literal vs repeated text): mostly
  lexing (chumsky lexer), then cloning the template. A hand-written lexer, or
  cloning only the statement being bound, would cut it.
- [ ] **squeal-sql planning/execution ~5.6 us** of a repeated-text lookup
  (store's part is 1.3 us).
- [x] **Read-only transaction commit (~9.5 us of the remaining 10).** Done:
  `Logger::sync_pending` skips the writer round trip when every queued record
  is synced (counted, not by LSN — records reach the writer out of LSN order).
- [ ] **Store lookup itself is ~0.7 us** (`Db::find`): a copy-on-write page
  iterator would not move SQL lookups noticeably.

## Range scans (100k rows: SQL 46 ms, store 18 ms)

- [ ] squeal-sql's per-row work (~280 ns/row: decoding rows into ValueItems,
  aggregation) is larger than store's. Profile before redesigning store's
  page iterator.
- [ ] Page iterator redesign (copy-on-write snapshot + `Arc<Tuple>` entries,
  sorted Vec instead of BTreeMap) — worth it only after the above; store
  change.
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
- [ ] Table locks are per process (`Schema::locks`): enough for one process
  per database, which is all squeal supports today.
- [ ] A lock wait gives up after 10 s instead of detecting a deadlock (two
  transactions each holding a table the other's DDL wants).
- [ ] DROP TABLE, like DROP PARTITION, leaves its trees in the store
  unreferenced; a later table of the same name gets `name~N` trees. Reclaiming
  them needs a store change.
- [ ] A join with no column equality runs every pair (INNER: a filtered cross
  join; outer: a hash join with one bucket). Fine for small inputs only.
- [ ] WITH queries are planned anew per reference (no materialization), and
  WITH RECURSIVE is refused.

Not written yet: a seeded query generator for the twins, the concurrent
soak with crash rounds, SQLite as a second oracle (`rusqlite` dev-dependency,
approved).

## Partitions

Done: every table is a list of partitions with their own trees
(`squeal-sql/src/partition.rs`); `CREATE TABLE ... PARTITION BY RANGE|LIST`,
`ALTER TABLE ... ADD|DROP PARTITION`; INSERT/UPDATE/DELETE/COPY route rows by
the partition column; queries read every partition (`source/append.rs`).

- [x] Pruning: partitions a WHERE rules out are not read (SELECT, UPDATE,
  DELETE); EXPLAIN shows `(k of n partitions)`.
- [x] Order across partitions: per-partition ordered reads are merged
  (`MergeAppend`), so ORDER BY / merge joins / GROUP BY need no sort.
- [ ] RANGE partitions read in bound order are already sorted by the
  partition column: plain concatenation would do instead of a merge.
- [ ] Pruning from join keys at run time (a hash join's build side could
  name the partitions the probe side needs); today only WHERE prunes.
- [x] Join seeks into partitioned tables: each partition sought, or only the
  one the key routes to when the key has the partition column.
- [x] Row counts per partition (stats row format version 2); pruned reads are
  estimated and costed by them; DROP PARTITION takes its rows off the count.
- [ ] Column statistics (distinct counts, min/max) are still per table, and
  tree shape is read off the first partition.
- [ ] DROP PARTITION leaves the partition's trees in the store, unreferenced
  (their pages are not reused). Reclaiming them needs a `drop_table` in store
  that is safe against a scan still reading the tree. Store change.
- [ ] No way to list a table's partitions from SQL except EXPLAIN (a
  `SHOW PARTITIONS`, or DESCRIBE showing them).
- [ ] External partitions (Parquet): a new `PartitionStorage` variant.

## Flaky tests

- [ ] `squeal-sql` `source::sortjoin` spill tests
  (`test_a_group_over_the_memory_budget_spills_and_stays_correct`,
  `test_reset_after_a_spilling_join_replays_it_and_releases_the_budget`):
  `QueryMemoryExceeded` / no spill, 2 in 40 full-suite runs at 9b53136, 0 in
  25 at 9aff786; never alone (0 in 30). Code unchanged between them; cause
  not found.
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
