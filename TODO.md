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
- [x] Page redesign, part 2: copy-on-write content. A page's content is an
  Arc shared with readers; `Page::iter` hands out that snapshot instead of
  copying every tuple, and a write copies the content only while a reader
  still holds one. Table scans and index leaves read in place (index
  leaves used to be copied out in chunks). A scan sees each page as it was
  when it got there, as before; the one difference is the leaf a seek
  lands in, which used to be re-read chunk by chunk. Store table scan 10.3
  -> 9.5 ms, key seek 1.01 -> 0.95 us; ranges flat.
- [x] Rows read in place (branch `value-ref`). Profiled `select count(*)
  from orders` first: 67% was decoding each row — postcard copying the
  payload into a Vec<u8> byte by byte, then IndexKey::from_bytes copying
  every value twice. Now `store::valueitem::ValueRef` borrows strings and
  blobs from the row bytes (`IndexKey::refs`), `StoredRow` borrows the
  payload (same bytes on disk), and `SqlTable::decode_row` copies out only
  the columns the query reads (table scans/seeks and index lookups get
  `reading`, as covering index scans already did; UPDATE/DELETE still read
  whole rows), skipping the rest without a UTF-8 check. Warm, main ->
  branch: orders count(*) 72 -> 41 ms; order_details `count(*) where
  quantity > 3` 0.95 -> 0.70 s, `sum(unit_price), max(return_reason)` 0.96
  -> 0.77 s; `select * from orders` 94 -> 79 ms; point lookups flat.
  What's left per row: the Vec plus Arc<[ValueItem]> of the row itself
  (~11%).
- [ ] `Arc<Tuple>` entries in `AnyTuplePage` (branch `arc-tuple-entries`,
  on top of part 2): undoes part 1's random-insert cost (500k on the
  memory backend 2.77 -> 2.21 s, the BTreeMap's figure) but reads pay a
  pointer more — table scan 9.5 -> 10.4 ms, ranges ~+4%, cold first-touch
  lookups 7.6 -> 8.4 us (a page decode allocates per tuple). Not merged;
  your call.
- [ ] A scan reads each page (TableCursor) and index leaf (RangeCursor) as
  it was when it got there. Whether a row relocated mid-scan can be missed
  or read twice that way (its old page behind the scan, or a leaf snapshot
  pointing at its old page) is unverified — the old TODO flagged the same
  question for TableCursor. Not changed by part 2: copying gave the same
  view.
- [ ] `RangeCursor::next` looks its table up by id every row
  (`Db::table_by_id`, a SipHash map): ~8% of a store range scan.

## Cold reads

- [ ] A store-cache miss decodes the whole page; leaves now hold ~6x more
  entries, so first-touch random lookups are ~15-20% slower in a fresh
  process (warm ones are faster). Fix: pages readable without a full decode
  (slotted layout / lazy decode). Store change.
- [x] Page checksum. A scan of order_details (1M rows, bigger than the
  cache) spent 28% in the page checksum, FNV-1a: a dependent multiply per
  byte, inlined into `get_page`. Page format 2 checksums with CRC-32
  (`crc32fast`, the CPU's own instructions); older pages verify with FNV-1a
  until rewritten. Same binary, format-1 vs format-2 database: order_details
  `count(*) where quantity > 3` 0.71 -> 0.53 s, `sum, max` 0.79 -> 0.60 s;
  cold first-touch `Db::find` (`store_cold_find`) 7.2 -> 5.35 us. Skipping
  verification entirely measured 0.515 s, so little is left there.
- [x] Two more on that scan (branch `page-decode`). A tuple's payload was
  deserialized as a seq — a byte at a time into a Box, then copied into
  its Arc — half of `Page::from_bytes`; now read as bytes, one copy, same
  bytes on disk. And the eviction queue (`ShardedPQ`, 819 shards at the
  default cache) read-locked every shard on each pop to find the oldest:
  13% of the scan; now a FIFO (`utils::fifo`), O(1) under one lock, same
  order and the same move-to-back on a re-push. Against main: order_details
  `count(*) where quantity > 3` 0.52 -> 0.375 s, `sum, max` 0.60 -> 0.45 s,
  cold `Db::find` 5.4 -> 4.2 us; 4 and 8 threads scanning at once ~20%
  faster too (the one lock doesn't cost them).
- [ ] What's left on that scan's page loads is building each tuple's key
  (an IndexKey: a String and an Arc per tuple, ~22%) and freeing them on
  eviction (~8%) — pages readable without a decode (above).
- [x] Borrowed tuples, step 1 of 3 (branch `tuple-ref`). `TupleRef` is
  an opaque lent tuple (id as `IdRef`: an int, or key fields as
  `ValueRef`s; data; the MVCC fields); `PageTuple::at_ref` lends one
  (AnyTuplePage/FixedTuplePage borrow, the rest copy), and
  `TableCursor`/`RangeCursor::next_ref` lend the row their reader sees —
  from the page snapshot, or an ancestor version walked back to — with no
  per-row Tuple clone. Visibility runs on txn_id/pre_lsn alone
  (`Db::walk_back`, `visible_version`). The owned `next` is `next_ref` +
  a copy. Table, index and lookup sources, CREATE INDEX, FK checks and
  ANALYZE read through it. Warm, against main: orders `count(*)` 41.5 ->
  37 ms, 100k-row range count 19.5 -> 17.3 ms, a filtered scan of orders
  71 -> 66 ms, order_details (bigger than the cache) 376 -> 361 ms.
- [x] Step 2 (branch `byte-page`, NOT merged — the trade-off below is
  your call): a table's data pages are SlottedPages read in place. A
  tuple is never decoded to be read: its id is read from its postcard
  bytes (`wire`: key values as ValueRefs, compared/equalled/hashed as
  DBIdType does — pinned against DBIdType over every pair of a set of
  edge-case ids), an in-memory prefix per slot decides most probes, and
  `at_ref` lends a view of the bytes (where each id ends kept per slot).
  Page bytes on disk are unchanged (SlottedPage's own layout, kind 4,
  which earlier builds already read). ValueItem's `==`/order/hash now
  live on ValueRef. Against main (1M-row retail):
  - wins: order_details count (bigger than the cache) 422 -> 346 ms, on
    4 threads 398 -> 247 ms (scans scale with threads now); sum/max 427
    -> 400 ms, 4 threads 582 -> 407 ms; cold `Db::find` 3.85 -> 3.4 us;
    bulk load 63 -> 48 s.
  - costs, warm (everything cached): SQL count(*) of orders 36 -> 44 ms,
    100k-row PK range count 16.6 -> 26.7 ms, `Db::find` 0.48 -> 0.63 us
    (SQL point lookups flat), store's owned `Cursor::next` (it now
    decodes each row) 3-4x. Database file +5% (slot directory).
  Left on the warm path: a range scan reads the data row's key to check
  its hint and parses each row's header to lend it, where AnyTuplePage
  compares/borrows in memory.
- [x] Fixed-width rows (branch `fixed-rows`, on `byte-page`, not merged).
  A row's columns each take their datatype's bytes whatever the value — a
  NULL its column's width, zero-padded; strings already padded to capacity
  — so where a column is follows from the SchemaVersion the row was
  written under (`RowLayout`, cached per version) and `decode_row` reads
  the wanted ones directly. No bytes per row: the count word's top bit
  says a row is fixed-width, old rows read as before, an old reader
  refuses a new row. Against `byte-page`: orders count(*) 46 -> 39 ms,
  filtered count 79 -> 67 ms, order_details count 325 -> 274 ms (main:
  37 / 65 / 352). The retail data stores no NULLs (sentinels instead), so
  its file is the same size; a table with NULLs in wide columns grows by
  their width.
- [x] One page format (branch `one-page-format`, on `fixed-rows`, not
  merged). Index pages — inner nodes and leaves — and pinned system pages
  are SlottedPages too: every page a tree writes. AnyTuplePage and
  FixedTuplePage only read what was written before. The tree's descent
  reads an entry's child pointer in place (`Page::with_entry`); a range
  scan checks an index entry against its row by comparing their key bytes
  (`IdRef::eq`, `find_hinted_ref`) and reads each leaf entry once; a tree
  remembers how many fields its keys have. The reader's helpers return
  Option, not Result<_, StoreError> (returned through memory, and not
  inlined): lending a tuple went from 23 ns to 5. Against main, each on
  its own load of the 1M-row retail data:
    SQL count(*) of orders                 36.8 -> 31.8 ms
    filtered count of orders               65.8 -> 56.0 ms
    select * from orders                   73.6 -> 64.7 ms
    order_details count (> cache)          359 -> 235 ms; 4 threads 454 -> 268
    order_details sum/max                  434 -> 306 ms
    cold first-touch Db::find              4.11 -> 2.57 us
    SQL point lookup                       4.62 -> 4.67 us
    100k-row PK range count                17.1 -> 18.3 ms
    Db::find (owned tuple)                 0.51 -> 0.69 us
    bulk load                              about even; file +12%
- [x] Lent `Db::find` (`find_with` / `find_as_with`: the row the reader
  sees, lent to a closure). The index-lookup source, the nested-loop
  join's inner rows and sq-json's index plan read through it; the owned
  `find_as` is it plus a copy. An index lookup fetching 9k rows: 64 ->
  17 ms. Still owned, by design: `Db::find` itself (0.71 us vs main's
  0.51) and store's `Cursor::next` — API for callers that keep the row.
- [x] The last two owned reads on a hot path: a foreign key check (per
  inserted row) asks whether the key is there through `find_with`, not a
  copied-out tuple — bulk load 70 -> 52 s; the nested-loop join's inner
  side reads through `next_ref`. Still owned, and fine so: spill runs
  (RunCursor, its own page type), catalog/stats loads at open, DDL scans.
- [ ] The file is 12% bigger: 8 bytes of slot directory per tuple, on
  index pages too. u16 offsets/lengths would halve that for pages under
  64 KiB — a change to SlottedPage's layout, so a page format version.
- [x] WHERE before the row is built: a table scan checks a fixed-width
  row's column against WHERE's plain comparisons (a column vs a literal
  of its own type — integer, datetime, string, boolean; `ColumnTest`) and
  passes over rows that fail without building them. WHERE still runs on
  what passes. orders `where customer_id = ...` 46 -> 13 ms;
  order_details `where quantity > 3` 235 -> 143 ms.
- [ ] If kept: step 3 is done by this (the default switched; old
  AnyTuplePage data pages still read). Still owned on the way: the
  nested-loop join's inner rows, Db::find.
- [ ] Concurrent scans don't scale: 4 threads scanning order_details finish
  no more queries than 1 (8 do worse), on main as on this branch. Not
  locks — the threads are in malloc/free (system allocator) for those keys
  and for rows (`new_from_owned`). Fewer allocations (above) is the real
  fix; a faster allocator in the binaries (mimalloc) would be a stopgap.
  `SQ_LOOP_THREADS` in `layers::range_loop` measures it.
- [ ] WAL records are checksummed with FNV-1a too (`logger.rs`), on every
  commit's write path. Same fix would need a WAL format version.

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
