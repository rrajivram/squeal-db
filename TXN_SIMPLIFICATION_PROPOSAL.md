# Proposal: a simpler transaction and durability design for `store`

Status: **proposal only — no code changes**. Written 2026-09-16 against commit `776af4b`.

Scope: transaction identity, visibility, conflict detection, undo/version retention,
commit/abort, checkpoint, WAL retention, recovery, and the locking/retry behavior that those
paths depend on under concurrent load. The page format, the page cache's eviction and sharding,
overflow chains, and the B+tree's split logic are out of scope except where a transaction-level
rule leans on them (see §10).

Sources: `STORE_AUDIT.md`, `audit-progress.md`, `TXN_HARDENING.md`, the three phase design docs,
`todo.txt` [1]–[17], `store/benches/BASELINE.md`, and a fresh read of `db.rs`, `txn.rs`,
`logger.rs`, `buffer.rs`, `tables/bplustree.rs`, `cursor.rs`, `page.rs`.

---

## 1. Why: the pattern behind the last 40 fixes

Every fix landed in the audit passes was locally correct and test-backed. The problem is what
they add up to. Today the transaction path is roughly 2,300 production lines in `db.rs`, 570 in
`txn.rs`, 880 in `logger.rs`, and it contains:

| thing | count today |
|---|---|
| independent "defer until readers are gone" mechanisms, each with its own waiter set | 3 (`pending_undo_discards`, `pending_tombstone_reclaims`, `aborting` state) |
| maintenance drains run from inside `begin()` | 3 drains + 1 `fstat` + 1 possible inline checkpoint |
| ways a transaction's writes get reverted | 3 (`rollback_by_id`, `revert_aborted`, `process_log` undo pass) |
| numbers that identify a transaction | 2 (`id` + `ts`, two persisted generators, reconciled from the log on open) |
| ad-hoc `RwLock<()>` guards on `Db`/`BPlusTree` | 3 (`checkpoint_gate`, `table_locks`, `relocation_lock`) |
| `retry_on_contention` call sites | ~10, including cleanup paths that must themselves retry |
| special cases in the undo chain | 2 (`Mod { pre: None }`, "own insert keeps `pre_lsn = None`") |
| wall-clock `u128` fields still serialized per record | 3 (`Record.timestamp`, `Commit(_, ts)`, `Rollback(_, ts)`) |

Five root causes explain almost all of it. The design in §3 removes each one rather than
patching its symptoms again.

**R1. There is no commit timestamp.** "Committed" is defined as "absent from the state map", so
a reader cannot ask "did this writer commit before I began?" Instead every transaction captures
a `HashSet` of the active set at `begin()`, and visibility is a three-clause predicate over
start timestamps plus that set. The write-conflict rule is a mirror-image three-clause predicate.
T14, the phantom-insert bug, the `MissingUndoRecord` fallback, and the two `create_transaction`
races all trace back to this.

**R2. Version history lives in the WAL's in-memory shadow, and the default is to discard it at
commit.** Because the row is overwritten in place and the only copy of the pre-image is the
`Logger::records` entry, every reader-safety fix became "don't discard yet, park it with a set of
waiters, drain later from `begin()`". Three such mechanisms exist now (undo trail, tombstones,
abandoned transactions). T7 (reader-pinned garbage is unbounded and invisible) is the same issue
with no fix yet.

**R3. Identity and ordering come from two counters that are persisted only at checkpoint.** Hence
`TransactionId = Arc<(id: u64, ts: u128)>` with custom `Eq`/`Hash`, a second generator just for
`ts`, `advance_ts_past` on every open, and the rule that `ts` must be minted under the same lock
as snapshot registration. The WAL already has a monotonic, crash-recoverable counter (the LSN).

**R4. Lock acquisition can time out and return an error.** So contention is control flow:
`retry_on_contention` wraps every multi-page operation, cleanup-on-failure paths must retry
internally, and a whole family of bugs ([7], [11], [15], T9 follow-up, T6 follow-up) is "a
cleanup step failed from contention and left an orphan row or a stale index entry".

**R5. The checkpoint primitive is "truncate the whole log".** That forces the quiesced design:
a gate that blocks every `begin()`, a wait loop that also drains aborts, a checkpoint that a
long reader or a leaked transaction blocks forever, and an auto-trigger inside `begin()` that
makes the calling thread pay for everyone's checkpoint. Under sustained load this is the one
mechanism that makes latency bimodal.

## 2. What stays

These were hard-won and are kept unchanged in spirit:

- Single framed, checksummed WAL with a `LogHeader`; torn-tail vs corruption scan rule (T4/S2).
- LSN minted before the mutation, stamped on the page, writer flushes only when
  `page.lsn <= durable_lsn` (T2). Commit returns only after its record is fsynced (T1). Group
  commit with adaptive linger (P10). Batch cap ≥ channel capacity.
- Combined pre/post images in one record per operation.
- Conditional physical reverts (`update_if_txn` / `remove_if_txn`) that never clobber a
  concurrent forward write.
- Logical (re-execute) redo and undo on recovery, idempotent by "already applied" checks.
- `Transaction` as a non-`Clone` RAII guard; `ConflictPolicy`.
- B-link high keys, proactive top-down splits, the rightmost-split heuristic, `last_data_page`.
- Header format version + checksum; free-list reconciliation on open (T16); `drop_table` guard
  (T17); write-new-copy-then-repoint relocation (T14).
- CLOCK eviction, sharded cache map, per-page `ReentrantMutex` registry, bounded pending writes.
- The stress harness with per-key history; the test-first discipline.

## 2b. How the SQL layer uses transactions today

Read from `squeal-sql/src/conn/connection.rs`, `stmt.rs`, `schema_ops/{schema,database}.rs`,
`source/table.rs`, `plan/logical.rs`, `error.rs`, and `squeal-cli`. This is what the store's
transaction API actually has to serve, and it is narrower than the store's own surface.

**The model is connection-scoped autocommit with an optional explicit block.** A `Connection`
holds `current_txn: RwLock<Option<Transaction>>`. `BEGIN`/`START TRANSACTION` fills it via
`Db::begin`, `COMMIT`/`ROLLBACK` take it and call `Db::commit`/`Db::rollback`. Nesting and
savepoints are rejected at parse time. With no block open, every statement opens, commits, or
rolls back its own transaction. Dropping the connection drops the guard, which today parks the
transaction in `aborting` until some other `begin()` drains it.

**Only INSERT and SELECT participate.** UPDATE and DELETE are not implemented at the SQL layer
(prepared forms return "not supported"). `Db::update` is reached only by `Schema::flush_metadata`
at close; `Db::remove` is never called from SQL. `WriteConflict`, `ConflictPolicy`, and every
own-row special case in the store are exercised today only by the store's own tests and the
stress harness.

**One SQL INSERT fans out to `1 + indices` store inserts** plus one `Db::find` per foreign key,
all under one transaction (read-your-own-writes is what makes a batch reference an earlier row
of the same batch). A failure on any row rolls back the whole batch in autocommit mode.

**DDL is not transactional at the store level and the SQL layer knows it.** `create_table` and
`create_schema` wrap their catalog-row insert in a transaction but compensate for
`create_table_with_index_entry_size` failures by calling `drop_table` by hand. `ALTER TABLE`
rewrites the catalog row under its own transaction. `Database::close` re-persists every table's
metadata row under one transaction before calling `Db::close`.

**A SELECT holds a reader for as long as the client holds the result.** `TableSource::new` opens
a `TableCursor`; in autocommit mode that cursor *owns* a fresh transaction, and the
`StreamingResultSet` (and therefore the transaction) lives until the client drains or drops it.
The CLI drains eagerly, but any client that pages lazily is a long-lived reader. Under the
current quiesced checkpoint, one such client blocks every checkpoint and, via the auto-trigger,
every `begin()` on every connection. Under §3.9 it costs retained log and version bytes, visible
in `stats()` and bounded by snapshot-too-old.

**A multi-table SELECT in autocommit mode runs under one transaction per table.** Each
`open_source` call goes through `with_current_txn`, which yields `None` outside a `BEGIN` block,
so every `TableCursor` begins its own transaction with its own start timestamp. A join over five
tables reads five different snapshots. Under an explicit `BEGIN` they share one. This is a
consistency gap at the SQL layer, not the store; the fix is for `LogicalPlan::execute` to open one
statement-scoped transaction and pass it to every source. The proposal does not change the store
to accommodate it, but the store's `table_scan_in_txn` already supports the fix.

**COPY INTO commits once per row.** `copy_csv_into` calls `insert_rows(.., None)` per CSV
record so that a bad row is skipped rather than fatal. Since T1, each of those commits waits for
its own fsync, so a single-connection load runs at the disk's fsync rate (~14k/s on the `File`
backend per `ARCHITECTURE.md`) regardless of group commit, which only helps across concurrent
committers. This is the most likely "load" pain point at the SQL level. Two fixes, either at
the SQL layer: batch N rows per transaction and, on a failure, replay that batch row-by-row to
find the bad row; or the store-level `commit_nowait`/`Durability` opt-out that T1's design named
and never built. The proposal adds the latter to §3.7 as a one-line variant of commit.

**Row-id sequences are not crash-safe.** A table without a `PRIMARY KEY` keys rows by
`Generator::gen_key` on a per-table sequence. Sequences are persisted only by
`write_system_tables` (checkpoint, close, table creation). A crash after committed inserts and
before the next checkpoint reopens with the sequence at its last persisted value; the next
insert is handed a row id that is already committed in the tree and fails with `DuplicateKey`.
The same class of bug as T11 for transaction ids, one layer up. §3.11 covers it.

**Nothing in the SQL layer depends on transaction ids being small, dense, or timestamped.** The
only use of `txn.id()` is to stamp `Tuple::new_with(key, data, Some(txn.id()), None)` before
`Db::insert`, which then overwrites the same field via `set_txn_id`. That stamping is redundant
and can go: `Db::insert` should take `(table, key, data)` and construct the tuple itself
(§3.5). This answers open question 7 in the affirmative.

**Store errors the SQL layer distinguishes**: `DuplicateKey`, `KeyNotFound`, `WriteConflict`,
`WriteConflictTransactionAborted`, `TransactionAlreadyFinished`, `TupleTooLarge`, and the
table-name family. Everything else, including `LockContentionError`, collapses to
`InternalError`. Deleting `LockContentionError` (§3.8) removes the one `InternalError` that a
correct program under load could currently produce.

## 3. The design

### 3.1 One clock, one identity

One `AtomicU64` counter per `Db` (the existing `LsnClock::counter`) issues **every** ordered
number in the system:

- `TxnId(u64)`: taken from the counter at `begin()`. It is the transaction's start timestamp.
- `Lsn(u64)`: taken from the counter for every log record, as today.
- `commit_ts`: the LSN of the transaction's `Commit` record.

Consequences: `TxnId` is `Copy`, compared and hashed as a plain integer, and totally ordered by
start time. `TxnId < every LSN of that transaction's records < its commit_ts`, by construction.
No `ts` generator, no `(id, ts)` pair, no `advance_ts_past`, no `TransactionInner`, no `Arc`.

Recovery seeds the counter from `max(highest LSN in the retained log, header.counter) + 1`. A
transaction that wrote nothing before a crash can have its id re-issued; that is harmless because
nothing references it. The header persists the counter at every checkpoint as a floor, so the
counter never regresses even if the log has been trimmed to empty.

Everything wall-clock is removed from records and ordering: `Record.timestamp`, the `u128` in
`Commit`/`Rollback`, `TransactionInner::default()`'s `timestamp()`. `Header.last_checkpoint`
may stay as a human-readable field; nothing orders by it.

### 3.2 The transaction table

`TransactionManager` becomes one map under one lock:

```
enum TxnState {
    Active   { first_lsn: Option<Lsn>, policy: ConflictPolicy },
    Aborted,                       // revert in progress or awaiting retry (rare)
    Committed { commit_ts: Lsn },  // retained until commit_ts < horizon (§3.6)
}
table: RwLock<BTreeMap<TxnId, TxnState>>
```

- Absent from the map ⇒ committed before the horizon ⇒ visible to everyone.
- `first_lsn` is set on the transaction's first logged write; it is what WAL retention needs.
- `BTreeMap` so `oldest_active()` (the horizon) is the first `Active` key.

There is no per-transaction snapshot set. `TransactionData` and `snapshot()` are deleted.

### 3.3 Visibility and conflicts: two one-line rules

Version `v` written by `w`, read by `r`:

```
visible(v, r)  :=  w == r
               ||  match table[w] { None => true,
                                    Committed{commit_ts} => commit_ts < r.id,
                                    _ => false }
```

Writer `m` wants to overwrite a row whose current writer is `w`:

```
conflict(w, m) :=  w != m
               &&  match table[w] { Some(Active{..}) | Some(Aborted) => true,
                                    Some(Committed{commit_ts}) => commit_ts > m.id,
                                    None => false }
```

This is first-committer-wins snapshot isolation. It is the same guarantee the current code
provides ("a writer active when I began stays invisible even after it commits" is exactly
`commit_ts > r.id`), with one lookup and no set membership.

The undo-chain walk (`resolve_visible`) is unchanged in shape but loses its fallback: under the
retention rule in §3.6 a record that a live reader can still need is never gone, so
`MissingUndoRecord` becomes `StoreError::Corruption`, and `find_last_committed` is no longer
called from any read path.

### 3.4 Version store, separate from the log

Today `Logger::records` is two things at once: the WAL's in-memory shadow and the MVCC version
store, and the WAL's lifecycle (discard at commit) wins. Split them:

- **WAL**: append-only, framed, segmented (§3.9). Nothing reads it at runtime.
- **`VersionStore`** (in memory): `records: ShardedMap<Lsn, Version>` where
  `Version { txn: TxnId, table: TableId, pre: Option<Tuple> }`, plus
  `by_txn: ShardedMap<TxnId, Vec<Lsn>>` for abort. This is what `Tuple.pre_lsn` points at.

Retention is a single rule (§3.6), not a decision made at commit time.

Two chain rules change, and they remove both special cases:

1. **Undo replays in reverse LSN order** (per transaction on abort; globally on recovery). With
   that, a transaction updating its own fresh insert logs an ordinary `Mod` whose `pre` is its
   own prior version. Abort restores the prior version first, then the `Add`'s revert removes the
   row. `Mod { pre: None }` and "own insert keeps `pre_lsn = None`" are deleted; the chain is
   uniform. (T13 predicted exactly this.)
2. **A tombstone is just a version.** `Del` writes a tombstoned tuple with `pre_lsn` pointing at
   the pre-image, like `Mod`. Physical removal of the row and its index entry is vacuum's job
   (§3.6), never commit's. An insert onto a visible committed tombstone is a `Mod` whose `pre` is
   the tombstone — so the "reinsert after delete hits `DuplicateKey` because the index entry is
   still there" bug family cannot exist.

### 3.5 One write primitive

`insert`, `update`, `remove` are today two different code paths (`insert_at_lsn` with
cleanup-on-failure; `update_checked` with `build`/`before_write` closures). They become one:

```
write_version(table, key, txn, f: FnOnce(Option<&Tuple>) -> Result<Tuple>) -> Result<()>
```

executed inside one critical section (§3.8 lock order):

1. Route to the index leaf holding `key` (proactive splits on the way down, as now). Hold the
   leaf lock from here to the end.
2. Look the key up in the leaf. If present, read the current tuple from its data page.
3. `conflict(current.writer, txn)` ⇒ `WriteConflict`. Under `AbortOnConflict` the caller aborts
   through the single abort path (§3.7).
4. `f(current)` builds the new version (insert requires `None` or a visible tombstone; update
   requires a live row; remove produces a tombstone). Mint `lsn`, set `new.txn_id = txn`,
   `new.pre_lsn = current.map(lsn_of_version)`.
5. Insert the `Version` into the store, **then** write the page (replace in place, or relocate
   as today, or write a fresh row + leaf entry for a new key), stamping the page with `lsn`.
6. Append the log record. Release the leaf.

Step order matters and differs from today for `update`/`remove`, which append the record
*before* the physical write. What a concurrent reader needs is the in-memory version record
(step 5), not the on-disk record, so the append moves after the publish. §3.9 relies on this.

Because the leaf is held across the data write, a duplicate key is detected before any byte is
written, and there is no cleanup-on-failure path to get wrong. Because the version is in the
store before the page is published, a concurrent reader can always resolve `pre_lsn`.

### 3.6 The horizon and the maintenance thread

**Horizon** `H := min TxnId over Active transactions`, or the current counter if none. Three
retention rules hang off it and nothing else:

| garbage | reclaimable when |
|---|---|
| `Version` record written by `w` | `w` is `Aborted` and reverted, **or** `commit_ts(w) < H` |
| `Committed{commit_ts}` entry in the transaction table | `commit_ts < H` |
| tombstone row + its index entry | its writer's `commit_ts < H` and it is still the row's latest version (checked under the leaf lock) |

Tombstones to purge are found via a plain queue `(commit_ts, table, key)` appended at commit
from `by_txn`'s `Del` entries. No waiter sets anywhere.

**One maintenance thread per `Db`** (started in `setup_needed_modules`, joined in `close`)
owns all background work:

- `vacuum()`: apply the three rules above. Runs when woken (commit, abort, transaction end) with
  a rate limit, and on a timer.
- Checkpoint (§3.9): triggered when the log runner reports bytes-since-last-checkpoint over a
  threshold (the runner already counts what it writes; no `fstat`), or on an explicit
  `Db::checkpoint()` which just asks the thread and waits for the reply.
- Retry of failed aborts (§3.7).
- **Snapshot-too-old**: if version-store bytes or retained log bytes exceed a configured cap,
  mark the oldest `Active` reader `Aborted` with reason `SnapshotTooOld`; its next operation
  returns that error. This is the bound T7 asked for.
- `Db::stats()`: active count, oldest start id, version-store bytes, retained log bytes,
  pending page writes, last checkpoint LSN.

Foreground threads never drain, reclaim, checkpoint, or `fstat`. `begin()` is: take a counter
value, insert `Active`, return.

### 3.7 Lifecycle: exactly one path each

**begin**: `id = counter.fetch_add(1)`; `table.insert(id, Active{..})`. No gate, no drains.

**commit** (`Db::commit`):
1. `require Active`.
2. `lsn = log(Commit(id))` — one append.
3. Under the table lock: `Active → Committed{commit_ts: lsn}`. This is the atomic commit point
   for every other thread; nothing about the version store changes here.
4. Enqueue this transaction's `Del` keys `(lsn, table, key)` for vacuum; wake the maintenance
   thread.
5. `wait_until_durable(lsn)`; return.

The T14 ordering subtlety disappears: versions are never discarded at commit, so there is no
window where a walker sees "not committed" and "pre-image gone" at once.

`Db::commit_with(txn, Durability::Async)` is the same sequence without step 5: the transaction
is committed for every other reader immediately and becomes durable at the next group-commit
sync. Intended for bulk loads (`COPY INTO`, §2b), where the caller re-runs the load on a crash
anyway. Default stays `Durability::Sync`.

**abort** — one function, `Db::abort(id)`, used by explicit `rollback`, by `Transaction::drop`,
by `AbortOnConflict`, and by snapshot-too-old:
1. Under the table lock: `Active → Aborted`. The transaction's versions are invisible from this
   instant (§3.3 treats `Aborted` as not committed).
2. Revert `by_txn[id]` in reverse LSN order with the conditional `*_if_txn` writes.
3. `log(Abort(id))`; drop its versions from the store; remove from the table.
4. If step 2 fails (I/O error, corruption), leave the entry `Aborted` and wake the maintenance
   thread, which retries. With blocking locks (§3.8) contention cannot cause this.

To let `Transaction::drop` run the full abort, the guard holds `Arc<dyn TxnSink>` (implemented
by `Db<F>`; object-safe, so the guard stays non-generic) instead of `Arc<TransactionManager>`.
The `aborting` set, `drain_aborting`, `revert_aborted`, `finish_rolled_back`, `abort_complete`,
`rollback_by_id`, and `update_checked_with_retry`'s drain-and-retry are all deleted.

### 3.8 Locks: ordered, enforced, fail-fast

Contention is a wait. A bug is an immediate, attributed error. Nothing hangs and nothing
retries. `LockContentionError` and `retry_on_contention` are deleted; three things replace them:

1. **A page lock is never held across anything that blocks.** `write_locked_page` publishes to
   the cache under the lock, releases, then sends to the writer's bounded channel (the message
   carries the live `Arc<Page>`, so order between two messages for one page is irrelevant).
   With that, a legitimate hold is microseconds, which is what makes 3 meaningful.
2. **The acquisition order is enforced at every acquisition, in release builds.** Levels
   `TableGuard < IndexInner < IndexLeaf < DataPage`; a thread-local stack of held levels;
   requesting a lock at or below the top of the stack (other than reentrantly on the same
   page) returns `LockOrderViolation { held, requested }` before waiting. At most one data
   page is held at a time: relocation writes the new copy (new page only), repoints the leaf
   entry, then locks the old page to remove the copy. `relocation_lock` is kept, exactly as
   T14 left it.
3. **A single, generous timeout as a backstop, never as control flow.** `lock_timeout` per
   `Db`, default 1 s, a thousandfold margin over any legitimate wait. Expiry returns
   `LockTimeout { page, holder thread, held_for, waited }` (the registry records holder and
   acquire time per key). The operation fails, the transaction is aborted via the single abort
   path, the event is counted and logged. Nobody retries. If the maintenance thread's own
   abort revert hits it more than a small fixed number of times, the engine enters
   `Degraded { reason }`: writes return `EngineDegraded` naming the stuck transaction and page,
   reads continue, and a restart (WAL replay) is the recovery. Loud within seconds, never a
   silent spin.

`parking_lot`'s deadlock detector stays a test-only feature; in production rule 2 makes it
redundant because an ordering bug fails before it waits.

### 3.9 Segmented WAL and fuzzy checkpoint

The log becomes a sequence of files `name.wal.<n>`, each starting with the existing `LogHeader`.
The runner appends to the newest segment and rolls to a new one when told to checkpoint. The
recovery scan reads segments in order and is otherwise the existing `scan_log`.

**Checkpoint** (maintenance thread, never blocks a transaction):
1. Ask the runner to flush and sync what it has queued; read `C = durable_lsn`.
   Every record with `lsn <= C` was appended, and by §3.5's ordering a record is appended only
   after its page was published, so every page dirtied by such a record is already dirty in the
   cache when step 2 starts.
2. Flush every dirty page whose `page.lsn <= C` and `fsync` the data file. Pages with a higher
   LSN stay dirty; their records are retained below.
3. Persist system pages (catalog, generators, free list) and the header with
   `checkpoint_lsn = C` and `counter`, `fsync` (the existing `write_header_synced`).
4. Roll the log to a new segment.
5. Delete every segment whose `max_lsn < min(C, first_lsn of the oldest Active transaction)`.

An uncommitted transaction's pages may reach disk at step 2. That is safe because the records
needed to undo it are, by step 5, still on disk. This is what T3's quiesce was substituting for.

`checkpoint_gate`, `wait_for_no_in_flight_transactions`, and the log runner's
truncate-seek-rewrite-header sequence are deleted. A long reader now costs retained log bytes
(visible in `stats()`, bounded by snapshot-too-old) instead of blocking the world.

### 3.10 Recovery

Same three passes as today over the retained segments, with two changes:

- Analysis also records each transaction's `first_lsn` and the maximum LSN; the counter is
  seeded from `max(max_lsn, header.counter) + 1`.
- Undo runs in **descending** LSN order over every record of every non-committed transaction
  (uncommitted transactions never overlap on a row, so per-transaction reverse is sufficient,
  and global reverse is the simplest statement of it).

Redo re-applies records with `lsn <= checkpoint_lsn` too; that is idempotent and bounded by
segment retention, so no in-flight-mutation bookkeeping is needed to make `C` exact.

### 3.11 Sequences

Named sequences (`Generator`) stay a store feature: the SQL layer uses them for row ids of
tables without a primary key, and `Db::get_generator` is public. Today they are persisted only
at checkpoint, which is a crash-recovery bug (§2b). Fix, borrowed from PostgreSQL: a sequence
hands out values from an in-memory chunk, and whenever it crosses a chunk boundary (say every
32 values) it appends a `Sequence { name, high_water }` WAL record *before* returning the first
value of the new chunk. Recovery's analysis pass restores each sequence to the highest
`high_water` seen, or the persisted value if higher. A crash can skip up to one chunk of values
and never repeats one. The two transaction-related sequences disappear entirely (§3.1), so this
applies only to user-created ones.

## 4. Invariants (the whole design in eight lines)

1. One counter issues every `TxnId`, `Lsn`, and `commit_ts`; `TxnId < its LSNs < its commit_ts`.
2. A version is visible to reader `r` iff its writer is `r` or `commit_ts(writer) < r.id`.
3. A row has at most one uncommitted writer, enforced under the leaf lock before any byte is
   written.
4. A page reaches disk only after every record stamped on it is durable (`page.lsn <= durable`).
5. A version record, a committed-transaction entry, and a tombstone are retained while
   `commit_ts >= H`, where `H` is the oldest active transaction's id.
6. A log segment is retained while `max_lsn >= min(checkpoint_lsn, first_lsn of oldest active)`.
7. Locks are taken in the order table guard → index pages top-down → leaf → one data page; the
   order is checked at every acquisition; a wait past `lock_timeout` is a reported bug, not a
   retry.
8. Foreground threads never perform maintenance; one thread per `Db` does vacuum, checkpoint,
   and abort retries.

## 5. What gets deleted

Types and fields: `TransactionInner`, `TransactionData`, `TxnState::Aborting` (replaced by
`Aborted`), `TXN_GENERATOR_NANE`, `TXN_TS_GENERATOR_NAME`, `pending_undo_discards`,
`pending_tombstone_reclaims`, `checkpoint_gate`, `Record.timestamp`, the `u128` on
`Commit`/`Rollback`, `LsnClock`'s `u64::MAX` sentinel (counter starts at 1, `durable` at 0, an
unlogged page has `lsn = 0` and is always flushable, so `set_dirty` stops stamping anything).

Functions: `advance_ts_past`, `snapshot`, `snapshot_of`, `discard_or_defer_undo`,
`drain_ready_undo_discards`, `drain_ready_tombstone_reclaims`, `reclaim_tombstones` (moves into
vacuum), `drain_aborting`, `revert_aborted`, `abort_complete`, `finish_rolled_back`,
`rollback_by_id`, `update_checked_with_retry`, `wait_for_no_in_flight_transactions`,
`retry_on_contention` and every call site, `find_last_committed` on read paths,
`insert_at_lsn`'s cleanup-on-failure block, `Logger::log`'s `Rollback` discard special case,
the `Checkpoint` arm of `log_runner`.

Special cases: `Mod { pre: None }`, "own insert keeps `pre_lsn = None`", `Visibility::
MissingUndoRecord`'s fallback, `check_write_conflict`'s `was_active_when_txn_began`, the
`ts()`-under-the-same-lock rule.

Errors: `LockContentionError`, `UndoLogError` (unused). Added: `SnapshotTooOld`,
`LockOrderViolation`, `LockTimeout`, `EngineDegraded`.

## 6. Behavior under load: what changes for a caller

| today | proposed |
|---|---|
| `begin()` drains three queues, `fstat`s the log, and may run a full checkpoint inline; blocks while any checkpoint runs | `begin()` is one atomic increment and one map insert |
| a checkpoint waits for every open transaction and every abandoned one; a leaked guard blocks checkpoints forever | checkpoints never wait for transactions; a leaked guard costs retained log until snapshot-too-old ends it |
| contention surfaces as `LockContentionError` after ~120 ms of retries; SQL layer sees `InternalError` | contention is a microsecond wait; an ordering bug fails at the site; a stuck holder is reported by name within `lock_timeout` |
| a long reader pins unbounded memory with no way to see or cap it | pinned bytes are in `stats()`; a cap converts the oldest reader into `SnapshotTooOld` |
| commit does tree work (tombstone reclaim with retries) after the commit point | commit does one append, one map update, one wait |
| every tuple carries an `Arc` id with a `u128`; index entries carry `None` | every tuple carries a varint `u64` |

Expected throughput: neutral to positive. The per-row visibility check drops a `HashSet` lookup;
`begin()` and `commit()` lose their tree and I/O work; the leaf lock is held slightly longer per
write (one data-page write inside it), which the stress harness must confirm is not a hot-key
regression (see §8).

## 7. Phasing

Each phase ships with the full `store` and `squeal-sql` suites green and a stress run at
`--threads 16 --ops 20000` on both backends. Backward compatibility of the on-disk format is
not required (confirmed for T4/S2; §9 asks to re-confirm).

0. **Test infrastructure first.** Build the two audit Phase 0 items that never got built: a
   crash-consistency harness (random workload on `NamedMemFile`, simulated power cut keeps only
   bytes that were `do_sync`'d, reopen, assert every committed transaction present and every
   uncommitted one absent) and a snapshot-isolation checker in the stress harness (each reader
   records what it saw; the checker verifies against the commit order). Every later phase is
   validated against these, not only against the per-finding tests.
1. **Identity and clock** (§3.1). `TxnId(u64)` from the LSN counter; delete the `ts` generator,
   `TransactionInner`, wall-clock fields. Mechanical but wide (`Tuple`, `Operation`,
   `squeal-sql`'s `TransactionId` uses).
2. **Transaction table with commit timestamps** (§3.2, §3.3). Replace snapshot sets with the two
   rules. The existing deferral mechanisms keep working during this phase (they key off the
   active set, not the snapshot), so it is independently shippable.
3. **Version store + horizon + maintenance thread** (§3.4, §3.6, §3.7). Delete the three
   deferral mechanisms, the `begin()` drains, and the multiple abort paths. `TxnSink` on the
   guard. Reverse-order undo and tombstone-as-version land here because vacuum owns purging.
4. **One write primitive** (§3.5). Fold `insert_at_lsn`/`update_checked` into `write_version`.
5. **Blocking, ordered locks** (§3.8). Delete `retry_on_contention` and `LockContentionError`.
   Done after phase 0's harness exists so a lock-order mistake is caught as a detected deadlock.
6. **Segmented WAL and fuzzy checkpoint** (§3.9, §3.10). Delete `checkpoint_gate`.
7. **Caps and stats** (`SnapshotTooOld`, `Db::stats()`).

Phases 1–3 are the core; 6 is the load fix; 4 and 5 remove the orphan/stale-index bug family.
If only one thing is done, do 0 + 2 + 3.

## 8. Risks and trade-offs

- **Lock-order enforcement costs one thread-local check per acquisition** and requires every
  `get_page_mut` site to name its level. The payoff is that an ordering bug fails at the site,
  in production, before waiting; a hang is not a possible outcome.
- **Leaf lock held across the data-page write** lengthens the critical section for hot keys.
  The P2 benchmark already shows same-key contention is the weak spot of the current lock
  registry. Measure with `--tables 1 --private-keys 20 --hot-keys 0` before and after phase 4.
- **Snapshot-too-old is a new failure mode** for long scans. It only fires past a configured cap
  that defaults high; a SQL layer that wants "never" can set it to unlimited and accept the
  memory.
- **Fuzzy checkpoint flushes uncommitted pages.** Correct by invariants 4–6, but it is the one
  place where the design relies on retention being right. Phase 0's crash harness is the guard.
- **Segment files** add a directory listing on open and a delete on checkpoint. `NamedMemFile`
  needs a "list siblings" operation for tests.
- **Committed-transaction map grows** between vacuums under a long reader. It is bounded by the
  same cap as the version store, and each entry is a few dozen bytes.
- **Recovery re-applies already-checkpointed records.** Bounded by segment retention; if it ever
  matters, redo can skip `lsn <= checkpoint_lsn` at the cost of tracking in-flight mints (the
  design deliberately avoids that bookkeeping).

## 9. Open questions

Each has a recommended default; the proposal assumes the default unless told otherwise.

1. **On-disk compatibility.** Still not required? (Default: not required. `TxnId` width, record
   layout, and segment naming all change.)
2. **Fuzzy vs quiesced checkpoint.** Phase 4 chose quiesced for simplicity. Is the load behavior
   in §6 enough reason to revisit? (Default: yes; it is the only item that fixes the
   begin-blocks-on-checkpoint and leaked-transaction-blocks-forever behaviors.)
3. **Fail-fast locking.** Resolved: no hangs. Lock order is enforced at acquisition, one
   generous timeout is a reported bug (never retried), and repeated failure inside abort flips
   the engine to a loud `Degraded` state rather than spinning. Remaining choice: `lock_timeout`
   default of 1 s, and whether `Degraded` should also refuse reads. (Default: 1 s; reads
   continue.)
4. **Snapshot-too-old.** Acceptable for a long reader to receive an error rather than pin
   unbounded memory? (Default: yes, with a high default cap and `stats()` to see it coming.)
5. **`Transaction` guard holds a `Db` handle** (`Arc<dyn TxnSink>`) so drop can revert inline.
   Any objection to the guard depending on `Db` rather than only the manager? (Default: none;
   `squeal-sql` stores the guard in a connection slot and is unaffected.)
6. **`ConflictPolicy::AbortOnConflict`**: keep? `squeal-sql` does not use it yet. (Default: keep;
   it is one branch in the single abort path.)
7. **Generators.** Answered by reading `squeal-sql` (§2b): nothing relies on transaction ids being
   small, dense, or timestamped, and the SQL layer's own row-id sequences are not crash-safe
   today. §3.11 makes them WAL-logged. Remaining question: is one chunk of skipped ids after a
   crash acceptable? (Default: yes; every mainstream engine behaves this way.)
8. **Scope of "simplify".** This proposal stops at the transaction and durability layer. The
   page-flush handshake and overflow-chain writes (§10) are candidates for a second pass; include
   them now or later? (Default: later.)

## 10. Out of scope, noted for a second pass

- **Flush vs mutation race** (`dirty_version`/`flushed_version`, `PageTransientlyInconsistent`,
  bounded retries in the writer). A simpler rule is "the writer `try_lock`s the page to take its
  byte snapshot and defers if busy". It must be `try_lock`, not `lock`: a foreground thread can
  hold a page lock while blocked on the writer's bounded channel, so a blocking writer would
  deadlock against backpressure.
- **Overflow-chain structural writes** are synchronous `pwrite`s (P9 second half); needs an
  in-memory authoritative chain shape before they can be queued.
- **Free list**: reconciliation on open walks every page. A pending-free list promoted at
  checkpoint would make open O(1) again; with fuzzy checkpoints being frequent, that becomes
  attractive.
- **Slotted pages** (P6): the id-only decode path identified in `P6_SLOTTED_PAGE_DESIGN.md`.
- **Catalog on one page** (S6): make the catalog an internal B+tree table.
- **SQL layer, not store**: one statement-scoped transaction for a multi-table SELECT in
  autocommit mode (§2b); `COPY INTO` batching or `Durability::Async`; UPDATE/DELETE, which will
  be the first real consumers of `WriteConflict` and `ConflictPolicy`.
