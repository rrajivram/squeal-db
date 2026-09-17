# Progress: transaction simplification implementation

Tracks `TXN_SIMPLIFICATION_PLAN.md` phase by phase. Nothing here is committed until approved.

Baseline (2026-09-16, `776af4b`): store 473 passed / 8 ignored; squeal-sql 367 passed / 4 ignored;
sql-parser green; workspace builds.

| phase | status | notes |
|---|---|---|
| 0 observability + crash harness | done | 4 pre-existing bugs found and fixed (below) |
| 1 one clock / TxnId(u64) / sequences | done | store 480 / sql 367 green; stress clean; 150-seed crash soak clean |
| 2 commit timestamps | done | store 479 / sql 367 green; stress clean; 150-seed soak clean |
| 3 version store / horizon / maintenance / one abort | done | store 484 / sql 367 green; stress clean both backends; 150-seed soak clean |
| 4 one write primitive | done | store 487 / sql 367 green; stress clean both backends; 150-seed soak clean |
| 5 enforced lock order, fail-fast | pending | |
| 6 segmented WAL, fuzzy checkpoint | pending | |
| 7 caps, async commit, SQL follow-ups | pending | |

## Phase 0 — done

Added: `Db::stats()` (`DbStats`), `Db::debug_dump_table`, `MemFile` synced-snapshot model
(`do_sync` publishes; `synced_snapshot()` is "the disk after a power cut"),
`Db<MemFile>::synced_snapshot`, `store::crash_harness` (+3 fixed-seed unit tests),
`examples/crash` (soak runner, `--dump-dir` saves the failing snapshot), `examples/crash_debug`
(offline replay: checkpoint state, WAL, recovered state), `examples/wal_dump`,
`logger::describe_wal`, a repeatable-read check on hot keys in `examples/stress`
(`isolation_violations` fails the run).

Bugs the harnesses found in the baseline engine, all fixed with red/green regression tests:

1. **Data-chain tail race** (`bplustree.rs::write_data`): the tail page's lock was released
   between "no successor" and "link my new page", so two concurrent writers both linked a new
   page to the same tail and the second overwrote the first. Rows on the orphaned page were
   reachable by `find` (index) but never by `table_scan` (chain); a checkpoint persisted it.
   Lost ~20% of rows from scans at 8 threads / 4 KiB pages. Test:
   `test_concurrent_inserts_extending_the_data_chain_are_all_reachable_by_scan`.
2. **Scan ends on two consecutive empty pages** (`cursor.rs`): both cursors advanced exactly one
   page when the current one was exhausted. Test: `test_table_scan_skips_consecutive_empty_data_pages`.
3. **Redo skips an `Add` over a checkpointed tombstone** (`insert_if_needed`): tombstone reclaim
   is unlogged; checkpoint captures the tombstone, reclaim runs, reinsert commits, crash → replay
   saw "key exists" and skipped the committed reinsert. Now an occupant with different
   data/flags is overwritten. Test: `test_replay_applies_a_committed_insert_over_a_checkpointed_tombstone`.
4. **`update`/`remove` on a committed, unreclaimed tombstone succeeded** and produced a new
   version that still carried the tombstone flag — the writer's own next read returned None
   (surfaced as an isolation violation in the stress harness). Now `KeyNotFound`. Test:
   `test_update_and_remove_on_a_committed_unreclaimed_tombstone_are_key_not_found`.
5. **Unlogged physical cleanups raced the checkpoint flush**: tombstone reclaim and abandoned-
   transaction reverts ran outside `checkpoint_gate` (from `begin()` before the gate, from
   `commit()` after leaving the active set), so a checkpoint could persist the leaf after the
   index entry's removal and the data page before the row's — an orphan tombstone the log knows
   nothing about, which a later reinsert then replays onto (`DuplicateKey` at open). Added
   `mutation_gate` (cleanups hold read; checkpoint takes write after quiescing). Verified by
   crash-harness soak (was ~1 failure per 30 seeds; 0 in 120 after). No deterministic unit test:
   the interleaving needs a checkpoint mid-cleanup. Phase 3 replaces the mechanism (vacuum logs
   its purges).

Known intermittent, not yet reproduced deterministically: one "no prefix of in-flight commits
explains the recovered state" at seed 1592642342 round 0 on the fixed binary (1 in ~180 runs).
Keep soaking at every phase; phases 2–4 rewrite this area.

Also: `test_audit_p10_an_isolated_write_skips_the_group_commit_linger` is a timing test that
flaked under the heavier parallel suite; now takes the best of 10 samples.

## Phase 1 — done

- `TransactionId(u64)`, `Copy`, drawn from `LsnClock` at `begin()` (id == start order).
  `TransactionInner`, the two transaction generators, `advance_ts_past`, `for_test(id, ts)`,
  `TransactionId::new(id, ts)` are gone.
- `LsnClock`: counter starts at 1, `last_written` at 0; no `u64::MAX` sentinel; `mark_written`
  is monotonic (`fetch_max`); `seed(next)` from the header on open.
- `Header.counter` (format version 2): the clock's next value at checkpoint/close.
- Log records: `Commit(id)`, `Rollback(id)`, no `Record.timestamp`, no wall clocks;
  new `Sequence { name, high_water, dropped }` (WAL version 2).
- `Generator`: chunked (32) allocation; logs a high-water record BEFORE handing out the first
  value of a chunk (send-then-raise, so a racing caller logs its own); creation and removal
  logged; recovery applies `Sequence` records in log order via `ensure_at_least`/`remove_unlogged`;
  `set_values` never lowers.
- `Page::set_dirty` no longer stamps an LSN; `Page::set_clock`/`lsn_clock` removed.
- Tests adapted: hand-built header bytes in two fixtures; record-count helpers ignore
  `Sequence` records; tie-break test replaced by `test_check_write_conflict_orders_writers_by_id`.

Two more baseline bugs the crash harness found while verifying phase 1, both fixed with red/green tests:

6. **Checkpoint could persist a commit whose record was not yet durable.** `commit()` flips the
   transaction out of the active set before waiting for its record to fsync (T1/T14 ordering);
   the quiesced checkpoint only waited for the active set, so it could flush and sync those pages
   while the record was unsynced — a crash then had data the log could not explain. Added
   `commits_in_flight` (counted from before the state flip until after the durability wait);
   checkpoint waits for zero. Test uses a `GatedSyncFile` (WAL fsync held open):
   `test_checkpoint_waits_for_a_commit_that_is_still_waiting_on_durability`.
7. **Recovery failed on a log older than the checkpoint.** The checkpoint's data sync and its log
   truncation are separate syncs; a crash in between leaves pre-checkpoint records in the log.
   Redoing a `Mod` for a key a later, checkpointed delete had reclaimed returned `KeyNotFound` and
   made the database unopenable. `update_if_needed` now treats a missing row as "nothing to do",
   symmetric with `Del`. Test: `test_recovery_tolerates_a_log_older_than_the_checkpoint`.

Phase 1 tests added: `test_reopen_after_crash_seeds_the_counter_above_every_id_and_lsn_in_the_log`,
`test_sequence_values_are_never_reissued_after_a_crash`,
`test_sequence_creation_and_removal_survive_a_crash`,
`test_ids_come_from_the_shared_counter_and_strictly_increase` (txn.rs),
`test_ensure_at_least_never_lowers_and_creates_when_missing` (generator.rs).

Fixture note: tree-only tests (`bplustree.rs`) now share one clock across buffer, logger and
transaction manager and declare everything durable up front (no WAL exists there); recovery marks
everything minted during replay durable, since every replayed write is backed by a durable record.

Throughput note: memory stress went 89.7k → 76.8k ops/s across phases 0–1. See the A/B in the
phase 2 notes; phase 3 removes the drains from `begin()` entirely.

## Phase 2 — done

- `TxnState { Active{policy}, Aborting, Committed{commit_ts} }` in one `BTreeMap` under one lock;
  `TransactionData` and the per-transaction snapshot sets are gone.
- Visibility: `is_visible(writer, reader)` = own write, or `commit_ts < reader.id`, or absent.
  Conflict: `conflicts(writer, me)` = in flight/aborting, or `commit_ts > me.id`. Both in `txn.rs`,
  unit-tested (`test_visibility_is_commit_before_reader_began`, `test_conflict_is_first_committer_wins`,
  `test_prune_keeps_commits_a_live_reader_must_not_see`).
- `prune_committed()` forgets entries with `commit_ts < oldest active id` (all, if none active).
- `find_visible_to(tuple, reader)` lost its snapshot parameter; cursors lost their snapshot field;
  `check_write_conflict` is one call. The synthetic-id conflict test was replaced by the rule tests.
- `DbStats.committed_retained` added.

## Phase 3 — done

- `version.rs`: `VersionStore` (records by LSN, per-txn LSN lists, a committed queue keyed by
  commit_ts, a tombstone queue). `vacuum(horizon)` is the single retention rule.
- `maintenance.rs`: one thread per Db (10 ms timer + wake on commit/abort): retries failed
  aborts, prunes the transaction table, vacuums, purges tombstones (logged as `Purge`, under
  `mutation_gate`), checkpoints when the WAL has grown 16 MiB (the runner counts bytes; the
  `fstat` in `begin()` is gone). Counters and last error in `Db::stats()`. Test-only `set_paused`.
- `Transaction` holds `Arc<dyn TxnSink>`; `Db<F>` implements it, so a dropped guard runs the full
  abort inline. `Db::begin` takes `self: &Arc<Self>`.
- One abort path: `Db::abort(id)` = flip to Aborting → revert newest-first (conditional) → log
  Rollback → discard versions → remove. Rollback of an already-finished id is a no-op.
- `begin()` is: gate read, counter increment, map insert. No drains.
- `commit()` is: append Commit, flip state, mark versions committed, wake, wait durable.
- Undo replays in reverse LSN order (live and recovery); `Mod.pre` is always a real pre-image
  (the transaction's own previous version for own chains); the `pre: None` and
  "own insert keeps pre_lsn None" special cases are gone.
- Tombstones are versions: `Del` redo re-tombstones in place; physical removal only by vacuum's
  `Purge` (logged; redo is conditional). An insert over a visible tombstone is a new version
  (`Mod` with the tombstone as pre-image).
- Logger is append-only (no in-memory maps); `MissingUndoRecord` is now `Corruption`.
- Deleted: `pending_undo_discards`, `discard_or_defer_undo`, `drain_ready_undo_discards`,
  `pending_tombstone_reclaims`, `drain_ready_tombstone_reclaims`, `reclaim_tombstones`,
  `drain_aborting`, `revert_aborted`, `rollback_by_id`, `update_checked_with_retry`'s
  drain-and-retry, `Logger::{records, by_txn, find_record, get_undo_operations}`.
- Tests added: `test_a_dropped_guard_is_fully_reverted_before_drop_returns`,
  `test_vacuum_never_reclaims_a_version_a_live_reader_can_reach`,
  `test_stats_report_zero_pending_work_after_quiescence`, version.rs unit tests.

Throughput (stress, mem, 16 threads, 20k ops/thread): baseline 89.7k ops/s → **134k ops/s** after
phase 3. The apparent drop to 76.8k after phases 0–1 was the crash-harness model in `MemFile`
copying the whole buffer on every sync (the WAL syncs per commit batch and grows to 16 MiB);
`do_sync` now copies only the dirty byte range. The gain itself comes from `begin()` doing no
cleanup, `Copy` ids, and no snapshot sets on the read path.

## Phase 4 — done

- `BPlusTree::write_version(id, lsn, decide)`: the one write path. Descends to the leaf
  (proactive splits on the way), holds it from the lookup through the data-page write and the
  leaf-entry change, and calls `decide(current)` once under that lock; `decide` does conflict
  checks, builds the version, records it in the version store and appends to the WAL before any
  byte moves. `Decision::{Insert, Replace, Delete, Skip}` → `Written::{Inserted, Replaced,
  Deleted, Skipped}`. Relocation, physical delete (row + entry atomically), duplicate detection
  (before any write, so no cleanup path) all live inside it.
- `insert`, `insert_at_lsn`, `insert_if_needed`, `update`, `update_if_needed`, `update_if_txn`,
  `remove`, `remove_if_txn` are each a few lines over `write_version`; `Db::insert/update/remove`
  and vacuum's purge pass a `decide` closure and log inside it. `Db::insert` handles insert-over-
  tombstone under the same lock as the duplicate check.
- Deleted: `insert_index`, `insert_recursive` (→ `descend_for_write`, which returns the locked
  leaf), `update_checked`, `relocate_tuple` (→ `relocate`, leaf held), `remove_index_entry`,
  `update_index_entry`, and their internal retries.
- Descent: optimistic B-link fast path (route unlocked, lock the leaf, revalidate flag and
  high_key under the lock), locked crabbing descent only when the leaf needs a split.
- A full non-root leaf on arrival (possible once fast-path writers can reach a freshly published
  sibling through its B-link `next` before the splitter descends into it) is now a retry from the
  root, not an invariant error. Found by `test_concurrent_inserts_at_small_page_size...`.
- Tests added: `test_insert_over_a_committed_tombstone_is_a_new_version`,
  `test_insert_over_an_uncommitted_tombstone_from_another_txn_conflicts`.
- Stress harness timing fixed (the watchdog slept whole report intervals, quantizing elapsed
  time to 2 s). With correct timing, mem/16 threads: baseline `776af4b` 102k ops/s (built in a
  worktree with the same harness; it also shows 4–5 isolation violations per run), phase 4 tree
  106–122k; hot-key config (1 table, 20 keys): baseline 88k, phase 4 92–100k.

## Phase 5 — done (early failure instead of hangs)

- Page locks carry a level, `LockLevel::{Index, Data}`, and a thread-local `HELD` stack records what
  this thread holds. `get_page_mut(page, level)` refuses at once with `LockOrderViolation` (naming
  the held set) if the request is out of order: Index after Data, or a second Data page. Index
  after Index (a descent) and re-entry of the same page are the only sanctioned nestings.
- One generous timeout, `Db::set_lock_timeout` (default 1 s, a detector with a thousandfold
  margin over any legitimate hold). A wait past it fails with `LockTimeout` whose message names
  the holder thread, how long it has held the lock, and what the waiter holds. Nothing retries a
  timeout: `write_with_policy` counts it (`DbStats::lock_timeouts`) and aborts the transaction
  through the one abort path. `retry_on_contention` and `LockContentionError` are gone from the
  engine, the examples and the SQL error mapping.
- Page locks are not held across the two waits that can depend on another user thread: the commit
  durability wait (`debug_assert_no_page_locks_held`, checked in debug builds) and page
  allocation in `write_data` (allocate with nothing held, re-lock the tail, re-check, link or
  `free_page` the loser's page). Sends to the writer thread's bounded channel (eviction flushes)
  may still happen under an outer lock: the writer never takes page locks, so that is a bounded
  delay, never a cycle, and it is documented at `flush_evicted` rather than asserted.
- `Degraded`: an abort whose revert fails is retried by the maintenance thread at most
  `ABORT_RETRY_BUDGET` (3) times; then `Db::stats().degraded` is `Some(reason)`, writes and
  commits return `EngineDegraded(reason)`, reads continue, and the failed transaction's rows
  stay invisible (it is Aborting, never Committed). Sticky until restart.
- Stress harness: `--lock-timeout-ms` replaces `--max-lock-retries`; the run fails on any
  lock timeout (workers' count, the engine's count, or a degraded engine).
- Tests added: `test_lock_order_violation_is_refused_immediately`,
  `test_lock_timeout_fails_fast_and_names_the_holder`,
  `test_blocking_wait_with_a_page_lock_held_is_caught`,
  `test_no_page_locks_held_passes_once_handles_drop`,
  `test_lock_timeout_aborts_the_transaction_and_is_counted`,
  `test_repeated_abort_failure_degrades_engine_instead_of_spinning`,
  `test_concurrent_chain_extension_neither_orphans_nor_leaks_pages` (page accounting closes via
  `BPlusTree::reachable_pages`).
- Verified: store 494 passed, squeal-sql 367 passed, stress mem 104k ops/s and file 12.1k ops/s
  (baseline file 12.0k) with `--lock-timeout-ms 50` and zero timeouts, 150-seed crash soak PASS.

## Phase 6 — done (segmented WAL, fuzzy checkpoint)

- The WAL is a set of segment files `<name>.wal.<n>`, each starting with the existing `LogHeader`.
  The runner appends to the current segment; `Logger::roll(floor)` opens the next one and deletes
  every older segment whose highest LSN is below `floor`. `Db::open` lists the segments by name,
  validates each header, scans them oldest-first and replays the concatenation with the unchanged
  three-pass recovery (`process_log` now takes records). `Db::close` returns a handle to the live
  segment. `Db::delete` removes every segment.
- File namespace: `Opener` gained `open_sibling`, `list_siblings`, `remove_sibling`, so the engine
  reaches segments through a handle it already holds — a real file's directory, or the in-memory
  namespace a `MemFile` was created in (fresh per `new`/`open`/`from_bytes`, shared by clones and
  siblings). `Db<MemFile>::synced_snapshot` returns the data file plus a namespace holding every
  segment's synced bytes; the crash harness, `crash_debug` and `wal_dump` speak segments.
- Fuzzy checkpoint (`Db::checkpoint`), never waiting for a transaction:
  0. tree writers excluded (a `RwLock` every `write_version`/table create/drop holds for its
     microseconds; the checkpoint takes the write side only around the capture), system pages
     written, then `floor = min(counter, oldest Active-or-Aborting id)` — counter read first;
  1. every dirty page copied and marked clean: one consistent instant of the tree;
  2. `Logger::sync` (every captured mutation's record was queued before its page was published),
     then the copies written and the data file fsynced;
  3. header (`page_count`, `counter`) written and fsynced;
  4–5. `roll(floor)`.
  One retention rule: a record below the floor belongs to a transaction that finished before the
  capture, so its page was flushed (no redo) and it needs no undo. An unfinished transaction's
  pages may reach disk; its records are above the floor and stay. A long-lived transaction costs
  retained segments (`DbStats::wal_segments`), never a stalled checkpoint.
- Checkpoint-only page flushing: eviction never picks a dirty page (it is parked until a capture
  cleans it), so the data file is always some past instant of the whole tree plus what the log
  replays — the structural guarantee recovery needs, since splits and chain links are not logged.
  The maintenance thread also checkpoints when more than `CHECKPOINT_DIRTY_PAGES` (4096) are
  waiting. The writer thread no longer writes pages (its `WritePage` path is now unused — a
  phase 7 cleanup).
- Deleted: `checkpoint_gate` and the read gate in `begin`, `wait_for_no_in_flight_transactions`,
  `mutation_gate`, `commits_in_flight`/`InFlightCommit`, `load_logs` (mmap + downcasts), the
  runner's truncate-seek-rewrite-header sequence, `flush_dirty_cached_pages`.
- Crash harness: `long_readers` (default 1) hold one transaction across every checkpoint of a
  round, re-reading a key sample; a value change inside the transaction fails the run; the report
  carries `max_wal_segments`.
- Tests added: memfile namespace tests, `test_checkpoint_does_not_wait_for_a_still_active_transaction`
  (replaces the T3 "waits" test), `test_checkpoint_syncs_the_log_before_flushing_a_committing_transaction`
  (replaces the in-flight-commit wait test), `test_retention_keeps_segments_for_an_aborting_transaction`,
  `test_recovery_replays_every_retained_segment_in_order`, `test_file_backed_open_finds_segments_by_name`;
  the single-log-file tests migrated to segments (`wal_segments_of`, `current_segment` helpers).
- Verified: store 499 passed, squeal-sql 367 passed, stress mem 99.8k ops/s and file 12.4k ops/s
  with `--lock-timeout-ms 50` and zero timeouts, 150-seed crash soak PASS with long readers.
- Known, pre-existing, out of scope: `drop_table` resets the dropped table's pages on disk at once
  while the catalog is only rewritten at the next checkpoint, so a crash in between leaves a
  catalog naming a table whose pages are blank; and `create_table` is not logged, so a crash
  before the next checkpoint loses the table.

## Phase 7 — done (caps and follow-ups)

- Recovery floor persisted: `Header.checkpoint_lsn` (format v3) is the floor of the last completed
  checkpoint; `open_using` replays only records at or above it. A segment is kept whole while
  anything in it is at or above the floor, so retained segments can hold older records — and a
  segment can, in principle, be deleted while an older one is kept (records land in append order,
  which is not LSN order across concurrent writers). One comparison decides what is replayed.
- Open never appends to a recovered segment: a crash may have left a torn tail, and records
  appended past it would be invisible to the scan (the scan stops at the tear). Recovery starts a
  fresh segment; the first checkpoint that passes the old ones deletes them. This was the likely
  cause of the rare "no prefix of the in-flight commits explains the recovered state" soak
  failure (a committed delete lost, an older value resurrected), seen once before phase 6 and once
  after: the old code appended to the single log after its torn tail too.
- `SnapshotTooOld`: `Db::set_snapshot_limits(SnapshotLimits { max_retained_wal_bytes, max_version_records })`
  (defaults 256 MiB, 1 M). Past either cap the maintenance thread aborts the oldest active
  transaction (one per pass) and checkpoints; the owner's next `find`/scan/`commit` fails with
  `SnapshotTooOld(reason)` naming the transaction and the cap, once. Reads on any finished
  transaction now fail (`require_active` in `find`, `table_scan_in_txn`, and each cursor step):
  a finished reader no longer pins its snapshot, so letting it read was a latent hole. Stats:
  `wal_retained_bytes`, `snapshot_too_old_aborts`, `recovered_records`.
- `Db::commit_with(txn, Durability::Async)`: returns once the Commit record is queued; visible at
  once, durable with the next log sync.
- SQL layer: outside an explicit BEGIN block a statement opens one transaction of its own
  (`QueryVisitor::stmt_txn`) and every table source reads under it — a join over five tables
  reads one snapshot, not five; the `StreamingResultSet` owns the transaction for as long as the
  client holds the result. `COPY INTO` loads 1000 rows per transaction and replays a failed batch
  row by row, so bad rows are still skipped and counted individually.
- The page-writer thread is gone as a page writer: `BufMsg::{WritePage, DiscardPending,
  Checkpoint}`, `WriteMsg`, the pending set with its LSN gate and transient retries, the
  backpressure cap (`max_pending_writes`, `create_with_limits`, `open_using_with_limits`,
  `DbStats::pending_page_writes`, bulk_load's `--max-pending-writes`) and `flush_evicted` are
  deleted. The thread only writes the header (plain and synced) and syncs at shutdown.
- Tests added: `test_async_commit_is_visible_at_once_and_durable_after_the_next_sync`,
  `test_snapshot_too_old_on_retained_wal_bytes`, `test_snapshot_too_old_on_version_records`,
  `test_recovery_skips_records_below_the_persisted_floor`,
  `test_reopen_starts_a_fresh_segment_and_a_torn_tail_never_hides_later_records`;
  squeal-sql `test_a_select_over_several_tables_holds_one_statement_transaction`,
  `test_copy_csv_into_batches_rows_and_still_skips_only_the_bad_ones`.
