# Open items

Measured on a generated retail database (1M order details), warm, release
build — see `squeal-sql/src/stmt/tests/layers.rs` to re-run.

## Where a primary key lookup's time goes (SQL: 33 us)

- [ ] **Parse (~23 us of 33).** The parse cache (`sql_parser::parse_sql_cached`)
  removes it only for repeated *identical* text (33 -> 10 us). Lookups
  differing by a literal never hit. Next step: key the cache by the text with
  literals replaced by placeholders, and bind the literals into the cached
  AST (squeal-sql's PreparedStatement already substitutes into INSERT;
  extend it to WHERE / SELECT).
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

## Flaky tests

- [ ] `squeal-sql` `source::sortjoin` spill tests
  (`test_a_group_over_the_memory_budget_spills_and_stays_correct`,
  `test_reset_after_a_spilling_join_replays_it_and_releases_the_budget`):
  `QueryMemoryExceeded` / no spill, 2 in 40 full-suite runs at 9b53136, 0 in
  25 at 9aff786; never alone (0 in 30). Code unchanged between them; cause
  not found.
- [ ] `store` `crash_harness_seed_1/2`: recovered state not explained by any
  commit prefix, ~1 in 12 full-suite runs, before and after this session's
  changes.
- [ ] `store` `test_audit_t14...`: under full-suite load (~1-2 in 10 runs,
  with or without the read-only commit change) a reader that is the OLDEST
  active transaction gets `Corruption("version record missing for pre_lsn of
  ...")` — a version it still needs was discarded. A real MVCC retention bug,
  not just a flaky assertion.
