# Transaction management hardening

Action items from a review of `Db::check_write_conflict` (write-write conflict detection,
added in `store/src/db.rs`) and the surrounding transaction machinery. Process: write a test
that fails for each item first (proving the gap is real), then fix it, then flip the test green.

Status legend: `[x-fixed]` real bug, had a failing test, now fixed and green · `[x-green]`
test written and passing (either already correct, or documents current behavior) · `[ ]` not
started / needs more infra before it can be written

All tests below live in `store/src/db.rs`'s `mod tests`, in the "txn hardening" section.

## Critical — fixed

- [x-fixed] **TOCTOU race between the conflict check and the physical write.**
  Fix: `BPlusTree::update_checked` (new, in `store/src/tables/bplustree.rs`) runs the
  ownership check, the pre-image resolution, undo/redo logging, and the physical write all
  inside the *same* page-lock critical section — mirroring `update_if_txn`'s already-correct
  pattern. `Db::update`/`Db::remove` now build two closures (`build`, `before_write`) and go
  through it instead of a separate `find()` + `check_write_conflict()` + `update()` sequence.
  Proven via `test_concurrent_updates_to_the_same_row_never_produce_two_winners_or_a_lost_commit`
  — two real threads, synchronized with a `Barrier`, race an update to the same row; exactly
  one must win (asserted via XOR) and the committed value must match the winner. The original
  deterministic single-threaded reproduction (manually replaying the old vulnerable sequence)
  no longer applies now that there's no seam left to interleave a racer into — replaced by this
  genuine-concurrency test, which exercises the real `Db::update` code path directly.
- [x-fixed] **Phantom reads for INSERT weren't actually prevented.** Root cause:
  `find_visible_to`'s fallback (`self.find_last_committed(tuple)`, for the narrow "undo record
  got discarded before a snapshot-respecting reader could use it" race) couldn't tell that case
  apart from "this tuple is a brand-new INSERT with no ancestor at all" — both looked like a
  flat `None` from `resolve_visible`. Fix: `resolve_visible` now returns a 3-way `Visibility`
  enum (`Found` / `NoAncestor` / `MissingUndoRecord`) instead of `Option`; `find_visible_to`
  only falls back on `MissingUndoRecord` (an ancestor genuinely existed, we lost track of it),
  never on `NoAncestor` (there never was one — exactly what a phantom-inserted row looks like).
  `find_last_committed` is unaffected (it never needed to distinguish the two cases). Proven via
  `test_find_does_not_see_a_row_inserted_and_committed_by_another_txn_after_this_txn_began`.
- [x-fixed] **Conflict against a dropped-but-undrained transaction didn't self-heal.**
  Fix: `Db::update_checked_with_retry` (new) catches a `WriteConflict` from the first attempt,
  calls `self.drain_aborting()` once, and retries — cheap when there's nothing to drain (a
  conflict against a genuinely still-active writer fails again, identically), and closes the
  footgun where a caller retrying `update()`/`remove()` directly (never calling `begin()`
  elsewhere) would otherwise spin forever waiting for something else to drain the aborting set.
  Proven via `test_update_self_heals_a_conflict_against_a_dropped_but_undrained_transaction` —
  now succeeds on a single `update()` call where it used to require an external `begin()`.

## ConflictPolicy — the "poison the transaction?" question, resolved as a user choice

Rather than picking one fixed policy, `WriteConflict` handling is now a per-transaction choice
(`ConflictPolicy`, in `store/src/txn.rs`), set at `begin()` time — similar to a SQL engine's
"continue/ignore on error" transaction option:

- `ConflictPolicy::ContinueOnConflict` (default — `db.begin()`) — unchanged from before: the
  conflicting operation fails, the transaction stays open and usable.
- `ConflictPolicy::AbortOnConflict` (`db.begin_with_conflict_policy(...)`) — the conflicting
  operation fails AND the entire transaction is immediately, automatically rolled back. The
  caller gets a distinct `WriteConflictTransactionAborted` (not a plain `WriteConflict`) so it
  knows more than just the one operation was undone. Any further use of that transaction
  (insert/update/remove/commit) returns `TransactionAlreadyFinished`; an explicit
  `db.rollback()` afterward is still a safe no-op.

Tests: `test_abort_on_conflict_rolls_back_the_whole_transaction_on_a_single_conflict` (an
earlier, valid write in the same transaction is rolled back too, not just the conflicting one;
further use of the transaction, including `commit()`, is rejected),
`test_explicit_rollback_after_an_auto_abort_is_a_harmless_no_op`,
`test_abort_on_conflict_does_not_change_behavior_when_nothing_conflicts`.

**Two more real, deeper races were found and fixed while building this** — both surfaced only
because `test_concurrent_updates_to_the_same_row_never_produce_two_winners_or_a_lost_commit`
(the TOCTOU regression test above) started failing intermittently (~4% of runs) once this work
began exercising real concurrent `begin()`s harder:

1. `TransactionManager::create_transaction` read the active-transactions snapshot and inserted
   itself as **two separate lock acquisitions** — a gap where two transactions beginning at
   nearly the same moment could each capture a snapshot that doesn't include the other. Fixed
   by moving snapshot-capture and self-registration under one `active_transactions.write()`
   critical section.
2. Even after (1), `TransactionId::new()` stamped `ts()` from `timestamp()` **before** that
   lock was acquired — so which thread actually won the lock race (determining whose snapshot
   saw whom) could disagree with which thread's `ts()` came out numerically smaller, since OS
   scheduling can reorder "call `timestamp()`" independently of "acquire the lock". That broke
   `check_write_conflict`'s `writer.ts() >= txn.ts()` fallback test once the first writer went
   on to commit before the second's retry ran. Fixed by stamping `ts()` inside the same locked
   critical section as snapshot capture, so ts-ordering and snapshot-registration-ordering are
   now provably consistent (whichever thread's critical section runs first always gets both the
   earlier `ts()` and the snapshot the other doesn't yet appear in).

Verified via repeated stress runs of the TOCTOU test: ~4% failure rate before either fix, ~0.3%
after fix (1) alone, 0 failures in 600 runs after fix (2). `TransactionData` also grew a
`policy` field.

## Sequential edge cases (no threads needed) — all green, unaffected by the fixes above

- `test_winner_of_a_conflict_rolling_back_still_frees_the_row_for_a_third_txn`
- `test_after_a_conflicting_transaction_gives_up_a_third_transaction_can_proceed`
- `test_update_against_an_uncommitted_insert_from_another_txn_conflicts`
- [x-green] **Timestamp tie** — added `TransactionId::for_test(id, ts)`
  (`#[cfg(test)]`, `pub(crate)`, in `store/src/txn.rs`) to force a deterministic collision.
  `test_check_write_conflict_treats_a_colliding_timestamp_as_conflicting` confirms the `>=`
  tie-break is intentional (conflicts on an exact tie) and specific to the tie (a strictly
  earlier, non-colliding ts does not conflict).

## Broader MVCC/snapshot gaps — all green

- `test_find_is_repeatable_across_more_than_one_intervening_commit`
- `test_table_scan_does_not_see_a_dropped_but_undrained_transactions_write`
- `test_find_does_not_see_a_row_inserted_and_committed_by_another_txn_after_this_txn_began` —
  moved to the critical/fixed section above; this is the phantom-insert bug's own test.

## Crash/replay interaction — green

- `test_close_reopen_preserves_data_written_after_a_resolved_write_conflict`

## Stress test — green

- `test_concurrent_writers_stress_no_panics_no_deadlocks_no_missing_rows` — 8 threads, 4
  shared rows, 50 iterations each, retry-on-conflict loop.

## Status

All three original fixes verified, the timestamp-tie test written, and ConflictPolicy
(including the two deeper races it surfaced) implemented and verified: full `store` suite
passes (366 tests under default parallel execution; the arclock tests always take ~60s by
design, unrelated to this work). Full `squeal-sql` suite: 304 passed, 1 pre-existing unrelated
failure (`test_new_rejects_select_with_a_join`). Whole workspace builds clean.

No open items remain in this document. `store`'s transaction machinery has not been wired into
squeal-sql's SQL surface at all yet (UPDATE/DELETE aren't implemented there) — exposing
`ConflictPolicy` as a SQL-level session/transaction option is a natural follow-up once that
exists, not something to do prematurely now.
