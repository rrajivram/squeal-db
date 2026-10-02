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
- [ ] `GROUP BY` with no aggregate and no `HAVING` does not group
  (`select cat from t group by cat` returns one row per input row):
  `handle_select` only builds a `GroupSource` when the SELECT list or HAVING
  makes the query a grouped one. Read from the code, not yet reproduced.
- [ ] `GROUP BY <expression>` (anything but a plain column) is dropped from
  the group key without an error (`validate_aggreations`' `filter_map`).

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
