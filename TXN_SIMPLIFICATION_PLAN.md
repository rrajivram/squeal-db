# Plan: implementing the transaction simplification

Companion to `TXN_SIMPLIFICATION_PROPOSAL.md` (the "what" and "why"). This is the "in what
order, and how do we know each step worked". Written 2026-09-16. No code changes accompany it.

Assumes the proposal's default answers to its open questions: no on-disk compatibility, fuzzy
checkpoint, blocking locks, snapshot-too-old cap, guard holds a `Db` handle, keep
`ConflictPolicy`, WAL-logged sequences.

## 0. Ground rules for every phase

The goal is not fewer lines. It is that when something goes wrong at 2am, the failure points
at one mechanism, and that mechanism fits in one screen. Every phase is held to these rules:

1. **Remove before you add.** A phase that introduces a mechanism deletes the one it replaces
   in the same commit. No two mechanisms ever cover the same job "for safety".
2. **One owner per state transition.** Each transaction state change happens in exactly one
   function, `TransactionTable::transition(id, expected_from, to)`, which asserts the edge is
   legal and logs it at `debug` with the id. If a transaction is in a state it should not be,
   the log shows who put it there.
3. **Errors name the invariant.** New `StoreError` variants and `Corruption`/`Internal`
   messages say which invariant from proposal §4 was violated and carry the transaction id and
   LSN involved. "Unknown error" is not an acceptable message for any new code.
4. **Everything background is observable.** The maintenance thread publishes its last run
   time, last error, and per-job counters through `Db::stats()`. Nothing runs that cannot be
   seen from `stats()`.
5. **No retry helpers, no silent spinning.** If an operation can fail from a transient cause,
   fix the cause. The one bounded retry left (the maintenance thread re-attempting an abort
   revert) is counted in `stats()` and, past its budget, flips the engine to `Degraded` so the
   failure is loud rather than hidden.
6. **No waiter sets, ever again.** Any "wait until readers are gone" logic goes through the
   horizon. If a new piece of garbage needs retention, it gets a `commit_ts` and joins one of
   the three retention rules in proposal §3.6.
7. **Each phase ships green.** Full `store` and `squeal-sql` suites, both stress backends at
   `--threads 16 --ops 20000`, and (from Phase 0 on) the crash harness at a fixed seed set.
   One commit per phase, or one per sub-step where a phase has them, with the phase number in
   the message.
8. **Docs move with code.** `ARCHITECTURE.md` is updated in the same commit as each phase. The
   audit-era design docs are left as history and get a one-line "superseded by" header when a
   phase replaces what they describe.

## 1. Phase order and why

| phase | what | removes | size | risk |
|---|---|---|---|---|
| 0 | observability + crash harness + WAL dump tool | nothing | M | none |
| 1 | one clock, `TxnId(u64)`, no wall clocks, logged sequences | second generator, `TransactionInner`, reconciliation | M | low |
| 2 | transaction table with `commit_ts`; two visibility/conflict rules | snapshot sets | M | medium |
| 3 | version store + horizon + maintenance thread + one abort path | three deferral mechanisms, drains in `begin()`, `aborting` machinery | L | medium |
| 4 | one write primitive; tombstone-as-version; reverse undo | `insert_at_lsn` cleanup path, `Mod{pre:None}`, own-insert special case | M | medium |
| 5 | enforced lock order, fail-fast timeout, no retries | `retry_on_contention`, `LockContentionError` | M | low |
| 6 | segmented WAL + fuzzy checkpoint | `checkpoint_gate`, quiesce wait, log truncate/rewrite | L | high |
| 7 | caps, stats, `Durability::Async`, SQL-layer follow-ups | T7 gap, per-row COPY fsync | S | low |

Order rationale: 0 first because nothing else can be trusted without it. 1 and 2 are cheap
and each halves the number of things a visibility bug can be. 3 is the center of the mess and
needs 2's `commit_ts` to define the horizon. 4 needs 3's vacuum to own tombstone purging. 5 comes after 4 has removed the multi-step operations that made
retries feel necessary, and after the harness exists to prove that legitimate waits are
microseconds. 6 is the largest
format change and the biggest behavior change under load; it goes last among the core phases
so it lands on a system whose other invariants are already simple. 7 is polish.

If the work has to stop early: after phase 3 the system is already dramatically simpler to
reason about, with the quiesced checkpoint still in place. Phases 4–6 can each be deferred
independently.

## 2. Phase 0 — see before you touch

Goal: be able to answer "what state is the engine in" and "what did the disk actually see" for
the current code, so every later phase is measured against a real baseline.

Changes:
- `Db::stats() -> DbStats`: active/aborting counts, oldest active id, pending undo discards,
  pending tombstone reclaims, version-record count, WAL bytes since checkpoint, pending page
  writes, last checkpoint time. Cheap reads of existing state; no new locking.
- `examples/wal_dump.rs`: prints every record in a `.wal` file (LSN, kind, txn, table, key,
  torn-tail/corruption verdict). Works on today's format; updated in phases 1 and 6.
- **Crash-consistency harness** (`examples/crash/`): `NamedMemFile` gains a "synced length"
  per file (advanced only by `do_sync`). The harness runs a seeded random workload of
  begin/insert/update/remove/commit/rollback across threads for N ops, then "cuts power" by
  cloning only the synced prefix of each file, reopens, and checks: every transaction whose
  `commit()` returned is fully present; every other transaction is fully absent; the tree is
  structurally valid (every index entry resolves; no duplicates). Prints the seed on failure.
  Runs in CI at a fixed seed set and as a `--seed` loop for soak runs.
- **Snapshot-isolation checker** in the stress harness: each reader records `(start_seq,
  key, value seen)`; each writer records `(commit_seq, key, value)`; the checker verifies every
  read equals the latest value committed before the reader began. Reuses the existing per-key
  history.
- Debug-only invariant asserts at the places that today have fallbacks: count how often
  `Visibility::MissingUndoRecord` is hit (it should become zero after phase 3).

Exit: harness passes on current code at all fixed seeds, or the failing seeds are filed with
their minimal sequence (they may exist; the quiesced checkpoint is still in place, so failures
are more likely in the deferral paths).

How this pays off later: every later phase's "did I break it" question is answered by the
same three commands, and the WAL dump turns a recovery failure into a readable list.

## 3. Phase 1 — one clock, one identity, logged sequences

Goal: exactly one source of ordered numbers; nothing wall-clock anywhere in ordering or
identity; sequences survive a crash.

Changes:
- `txn.rs`: `pub struct TxnId(pub u64)` with `Copy, Ord`. `TransactionManager::begin` takes
  `counter.fetch_add(1)` from the `LsnClock`. Delete `TransactionInner`, `TXN_TS_GENERATOR_NAME`,
  `advance_ts_past`, `TransactionId::new/for_test/ts`, the `PartialEq`/`Hash` impls.
- `logger.rs`: delete `Record.timestamp`; `Commit(TxnId)`, `Abort(TxnId)` (rename from
  `Rollback`); new `Sequence { name, high_water }` record. `LsnClock` counter starts at 1,
  `last_written` starts at 0; delete the `u64::MAX` sentinel and `is_durable`'s special case.
- `page.rs`: `set_dirty` stops stamping an LSN. A page that was never stamped has `lsn = 0`
  and is always flushable. `stamp_lsn_at_least` is the only stamp.
- `db.rs`: `Header` gains `counter: u64`, written at checkpoint/close; open seeds the clock
  from `max(header.counter, max LSN in log) + 1`.
- `generator.rs`: chunked allocation (32) with a `Sequence` record appended before the first
  value of a new chunk is handed out; recovery restores `max(persisted, highest high_water)`.
  The transaction-id generator is deleted; `SYSTEM_TABLE_NAME` and user sequences remain.
- `tuple.rs`: `txn_id: Option<TxnId>`. `squeal-sql` stops passing `Some(txn.id())` into
  `Tuple::new_with` (the store overwrites it anyway); this is the only SQL-layer touch.

Tests to add: reopen after crash seeds the counter above every id and LSN in the log; a
sequence never repeats a value across a crash (harness gets a "PK-less table" table kind).

Failure reasoning after this phase: any "wrong order" bug is a bug in one `fetch_add` or in
the seed-on-open formula. Both are three lines.

## 4. Phase 2 — commit timestamps

Goal: visibility and conflict become two one-line rules over one map.

Changes:
- `txn.rs`: `TxnState { Active{first_lsn, policy}, Aborted, Committed{commit_ts} }` in a
  `RwLock<BTreeMap<TxnId, TxnState>>`. `transition()` per rule 0.2. `commit(id, lsn)` moves
  `Active → Committed{lsn}`. `oldest_active()` returns the first `Active` key.
- `db.rs`: `is_visible(writer, reader)` and `conflicts(writer, me)` exactly as proposal §3.3.
  `resolve_visible` takes the reader id and calls `is_visible`; `find_visible_to` loses its
  `reader_snapshot` parameter; `snapshot_of` and `TransactionData` are deleted;
  `check_write_conflict` becomes `conflicts`. `TableCursor`/`RangeCursor` drop their snapshot
  field.
- Retention of `Committed` entries, for this phase only: pruned inside the existing
  `drain_ready_undo_discards` step using `oldest_active()` (entries with `commit_ts <
  oldest_active` are removed). Phase 3 moves this into vacuum. This is the one place a
  temporary bridge is accepted, because the alternative is landing phases 2 and 3 together.

Tests: the existing MVCC suite (`test_find_is_repeatable_*`, phantom insert, T6, T14) must
pass unchanged. Add: first-committer-wins matrix (writer active / committed-before-me /
committed-after-me / aborted, each against update and remove).

Failure reasoning: a row is visible when it shouldn't be ⇒ either `commit_ts` was set wrong
(one call site) or the entry was pruned early (one comparison against `oldest_active`).

## 5. Phase 3 — version store, horizon, maintenance thread, one abort

Goal: all retention decisions are one comparison against the horizon; all background work is
in one thread; a transaction has exactly two exits.

Sub-steps, each its own commit:

3a. **Version store.** Move `records`/`by_txn` out of `Logger` into `VersionStore` (new file
    `version.rs`). `Logger::log` no longer touches them; the write path inserts the `Version`
    before publishing the page and appends to the WAL after. `Logger` becomes append-only.
    Delete `discard_or_defer_undo`, `drain_ready_undo_discards`, `pending_undo_discards`,
    `discard_undo`, the `Rollback` special case in `log()`.

3b. **Maintenance thread + vacuum.** `maintenance.rs`: one thread per `Db`, woken by a
    `Condvar` from commit/abort/end-of-transaction and by a timer. `vacuum()` applies the
    three rules of proposal §3.6 using `oldest_active()`. Tombstone queue
    `Vec<(commit_ts, table, key)>` appended at commit. Delete `pending_tombstone_reclaims`,
    `drain_ready_tombstone_reclaims`, `reclaim_tombstones` (its body becomes vacuum's
    tombstone step). `begin()` becomes: take id, insert `Active`. The log-size checkpoint
    trigger moves into the thread, fed by a byte counter the log runner already has (delete
    the `fstat` in `begin()`). The checkpoint itself is still the quiesced one from Phase 4
    of the audit at this point; it just runs on the maintenance thread.

3c. **One abort path.** `Db::abort(id)` per proposal §3.7. `Transaction` holds
    `Arc<dyn TxnSink>` and its `Drop` calls `sink.abort(id)`. `Db::rollback(txn)` is
    `abort(txn.into_id())`. `AbortOnConflict` calls the same function. Delete
    `rollback_by_id`, `revert_aborted`, `drain_aborting`, `aborting_ids`, `abort_complete`,
    `finish_rolled_back`, `update_checked_with_retry`'s drain-and-retry, and the
    `wait_for_no_in_flight_transactions` drain (it just waits now). A failed revert leaves
    `Aborted` in the table and wakes the thread, which retries and counts.

3d. **Chain rules.** Undo in reverse LSN order in both `abort` and recovery's pass 3. Own-row
    writes carry a real pre-image (the transaction's own prior version). Delete `Mod{pre:
    None}` handling and the `current.pre_lsn.is_none()` branches in `update`/`remove`'s build
    closures. `Visibility::MissingUndoRecord` becomes `StoreError::Corruption` with the LSN;
    `find_last_committed` is called only from the write path's pre-image resolution.

Tests: every audit test in the T6/T7/T8/T9/T12/T13/T14 family, unchanged. Add: vacuum never
reclaims a version a live reader can reach (property test: random readers with random
lifetimes, assert zero `Corruption` from `resolve_visible`); a dropped guard is fully reverted
before `drop` returns; `stats()` reports zero pending work after quiescence.

Failure reasoning: a missing pre-image ⇒ vacuum's comparison or a wrong `commit_ts`. A leaked
row ⇒ `abort` (one function). Something not getting cleaned up ⇒ `stats()` shows which
counter is stuck and the thread's last error.

## 6. Phase 4 — one write primitive

Goal: insert, update, remove are one code path with one lock scope.

Changes:
- `bplustree.rs`: `write_version(key, txn, f)` per proposal §3.5, holding the leaf from
  lookup to log append. `insert_at_lsn` and its cleanup block, `update_checked`, and the
  `build`/`before_write` closure pair are deleted; `insert`/`update`/`remove` in `db.rs` are
  each a ten-line closure over `write_version`.
- Insert onto a visible committed tombstone is a `Mod` with `pre = tombstone`; vacuum's
  tombstone step checks under the leaf lock that the row is still a tombstone by the same
  writer before removing row + entry.
- `Db::insert(table, key, data, txn)`: the store constructs the tuple.
- `insert_if_needed`/`update_if_needed` remain for recovery only.

Tests: the T9 family, the `DuplicateKey`-after-remove tests, and [7]/[11]/[15]'s
regression tests unchanged. Add: reinsert onto a committed tombstone before vacuum runs.

Failure reasoning: a duplicate or orphan row is now impossible without a lock-scope bug, and
the lock scope is one function.

## 7. Phase 5 — enforced lock order, fail-fast timeouts, no retries

Goal: contention is a wait; a *bug* is an immediate, attributed error; nothing ever hangs and
nothing ever retries. The current design gets this backwards: the timeout is short enough to
fire on ordinary scheduling jitter, so it cannot be treated as a bug, so every caller retries,
so a real bug is retried too.

The rule this phase installs: **a lock wait longer than a few hundred microseconds is never
legitimate**. Once that is true, a timeout of one second is a bug detector with a thousandfold
margin, not a tunable.

Changes, in order:

5a. **Never hold a page lock across anything that blocks.** Audit the two known cases and add
    a debug assertion for the general one:
    - `write_locked_page` sends on the writer's bounded channel while the caller still holds
      the page guard. Under backpressure that send blocks for as long as the writer needs to
      drain, which can be a disk-fsync-bound wait. Publish to the cache under the lock,
      release, then send. Safe because the message carries the live `Arc<Page>` and the writer
      snapshots bytes at flush time, so two messages for one page in either order describe the
      same object.
    - `wait_until_durable` in `commit` already runs with no locks held; keep it that way.
    - Debug-only: a thread-local "locks held" counter; `Sender::send`, `Condvar::wait`, and
      `Db::checkpoint` assert it is zero.

5b. **Enforce the lock order at acquisition, not by detecting deadlocks afterwards.** Every
    page-lock acquisition names its level:

    ```
    enum LockLevel { TableGuard = 0, IndexInner = 1, IndexLeaf = 2, DataPage = 3 }
    ```

    A thread-local stack records the levels currently held. `get_page_mut(page, level)`
    checks `level > top()` (or the same page, for reentrancy) before waiting. A violation
    returns `StoreError::LockOrderViolation { held: Vec<(LockLevel, PageId)>, requested:
    (LockLevel, PageId) }` immediately: no wait, no hang, and the error names both sites.
    Cost is one thread-local read and one compare per acquisition. This is on in release
    builds; it is the invariant, not a debugging aid. Crabbing top-down is
    `IndexInner → IndexInner` at the same level, which the check allows only when the
    previous inner page is released first (the stack must pop before the push), which is
    what crabbing already does.

5c. **Timeout as a backstop, never as control flow.** `ArcLock::lock` keeps a timeout, but one
    value, configurable per `Db` (`lock_timeout`, default 1 s), and the error it returns is
    `StoreError::LockTimeout { page, level, holder: ThreadId, held_for: Duration, waited:
    Duration }`. The registry records holder thread and acquire time per key (one `Instant`
    store per acquisition) so the error can say who has it and for how long. The caller does
    not retry. The operation fails, the transaction is aborted through the single abort path,
    `stats().lock_timeouts` increments, and the error is logged at `error` level with the
    holder's information. With 5a and 5b in place this error means one of two things, both
    bugs: a critical section that blocks, or a thread that died holding a guard, and the error
    says which page and which thread.

5d. **Delete `retry_on_contention` and `LockContentionError`.** Every call site goes away; the
    `squeal-sql` error mapping loses one arm and gains `LockOrderViolation`/`LockTimeout`,
    both mapped to `InternalError` because a correct program cannot cause them.

5e. **When the backstop fires inside abort.** The revert of an aborted transaction can hit
    `LockTimeout` like any other operation. The transaction stays `Aborted`; the maintenance
    thread retries it with backoff up to a small fixed count (say 3, over ~10 s), each attempt
    logged with the holder information. If it still fails, the engine does not keep spinning
    silently: `Db` enters `Degraded { reason }`, every new write returns
    `StoreError::EngineDegraded(reason)` naming the stuck transaction and page, reads continue,
    and `stats().degraded` is set. The operator sees a loud, specific failure within seconds
    instead of a hang or an unbounded retry loop. Recovery from `Degraded` is a restart, which
    replays the WAL and undoes the transaction cleanly. This is the one retry loop the design
    allows, and it is bounded and reported.

5f. **`parking_lot` deadlock detection** stays as a test-only feature (stress and crash
    harnesses). In production, 5b makes it redundant: an ordering bug fails before waiting.

Tests: the concurrency stress tests unchanged. Add: a deliberate order violation (data page
then leaf, in a test-only helper) returns `LockOrderViolation` without waiting; a thread
that parks while holding a page guard makes a second thread's acquisition return `LockTimeout`
naming the first thread within `lock_timeout`; a failed abort revert flips the engine to
`Degraded` and a subsequent write returns `EngineDegraded`; a stress soak at
`--tables 1 --private-keys 20 --hot-keys 0` with `lock_timeout` set to 50 ms produces zero
timeouts (proves 5a's claim that legitimate waits are microseconds).

Failure reasoning: a `LockOrderViolation` names both sites; a `LockTimeout` names the holder;
`Degraded` names the transaction. None of them can be masked by a retry, because there are no
retries.

## 8. Phase 6 — segmented WAL, fuzzy checkpoint

Goal: checkpoints never wait for transactions; log retention is one rule.

Sub-steps:

6a. **Segments.** `name.wal.<n>`; runner appends to the newest; `roll()` opens the next.
    Open lists and orders segments; `scan_log` runs per segment. `NamedMemFile` gains
    `list(prefix)`. `wal_dump` learns segments. Nothing else changes; checkpoint still
    truncates (now: rolls and deletes all older segments) under the quiesce.

6b. **Fuzzy checkpoint.** The five steps of proposal §3.9 on the maintenance thread.
    `first_lsn` recorded on a transaction's first logged write. Delete `checkpoint_gate`,
    `wait_for_no_in_flight_transactions`, and the read-side gate in `begin()`. The runner's
    `Checkpoint` arm becomes `Roll`.

6c. **Recovery.** Analysis records `first_lsn`/`max_lsn`; redo unchanged; undo in global
    reverse LSN order. `header.checkpoint_lsn` and `header.counter` are read for seeding and
    for the segment-deletion floor.

Tests: every replay/checkpoint test, migrated to segments. Crash harness with checkpoints
injected at random points and long-lived readers spanning them (the T3 scenario, now
expected to pass without quiescing). Add: a segment is never deleted while an active
transaction's `first_lsn` is inside it.

Failure reasoning: data loss after crash ⇒ a segment was deleted early (one `min()` in one
function) or a page was flushed with `lsn > C` (one comparison in the writer). The WAL dump
plus `header.checkpoint_lsn` tells you which in minutes.

## 9. Phase 7 — caps and follow-ups

- `SnapshotTooOld`: cap on version-store bytes and retained log bytes; the oldest reader is
  aborted with that reason. Exposed in `stats()`.
- `Durability::Async` on commit for bulk loads.
- SQL layer: one statement-scoped transaction for multi-table SELECT; `COPY INTO` batches
  rows per transaction (or uses `Durability::Async`); `LogicalPlan::execute` owns the
  transaction's lifetime rather than each cursor.

## 10. Debugging playbook (what the finished system gives you)

| symptom | first place to look | why it is the only place |
|---|---|---|
| row visible that should not be, or missing that should be | `is_visible` (one function) and the writer's entry in `stats().transactions` | visibility is one comparison against `commit_ts` |
| `Corruption: missing version at lsn L` | vacuum's horizon comparison; `stats().oldest_active` vs the writer's `commit_ts` | only vacuum removes versions |
| duplicate key on reinsert | `write_version`'s leaf scope | only one path writes rows |
| memory grows | `stats().version_bytes`, `stats().oldest_active` | pinned by the oldest reader, nothing else |
| WAL grows | `stats().retained_segments`, the oldest active's `first_lsn` | one retention rule |
| `LockOrderViolation` | the two sites it names | the order is checked at every acquisition |
| `LockTimeout` | the holder thread and page it names | nothing legitimate holds a page lock for more than microseconds |
| writes return `EngineDegraded` | `stats().degraded` reason: the stuck transaction and page | the engine refuses work loudly instead of retrying silently |
| data missing after crash | `wal_dump` + `header.checkpoint_lsn` | segment deletion and page flush are each one comparison |
| transaction stuck | `stats().transactions[id]` shows state and last transition; the log shows who moved it | one transition function |

## 11. What not to do during this work

- Do not keep an old mechanism "just in case" alongside its replacement past the end of a
  phase. The bridge in phase 2 (pruning committed entries in the old drain) is the only one,
  and phase 3 removes it.
- Do not fix an unrelated bug found mid-phase in the same commit. File it against the phase
  where it belongs, or fix it in its own commit first.
- Do not add a new `retry_on_*` helper, a new waiter set, a new `RwLock<()>` guard, or a new
  wall-clock field. Each of those is how the current complexity got here.
- Do not tune performance inside a correctness phase. Record the stress number before and
  after each phase; investigate regressions after the phase lands, in their own commit.
