# `store` audit: transaction management, durability, security, performance

Date: 2026-09-12. Scope: the `store` crate as of commit `779659a` (plus the
uncommitted `squeal-sql` edits in the working tree, which do not touch `store`).
This is a recommendations document, not a patch set. Each item is written so a
separate session can pick it up cold: where the problem is, why it is a problem,
what to change, and how to prove the change with a test.

Method: full read of `db.rs`, `buffer.rs`, `logger.rs`, `txn.rs`, `arclock.rs`,
`page.rs`, `tables/bplustree.rs`, `cursor.rs`, `run.rs`, `valueitem.rs`,
`tuple.rs`, `memfile.rs`, the page-content codecs, `ARCHITECTURE.md`,
`TXN_HARDENING.md`, and both `todo.txt` files. Five findings were then confirmed
empirically with a throwaway example binary against the public API on the
`MemFile` backend (deleted afterward; the exact sequences are given inline so
they can be turned into regression tests). Everything else is from code
reading; each item says which.

Status legend:
`CONFIRMED` reproduced this session · `CODE` established by reading the code
path end to end · `LIKELY` a plausible interleaving not yet reproduced.

Severity legend:
`S1` silent loss or corruption of committed data, or uncommitted data becoming
committed · `S2` violates a documented guarantee (isolation, atomicity) or can
brick a database · `S3` robustness / DoS / footgun · `P` performance.

---

## Part 1. Transaction management, WAL, and recovery

### T1. `commit()` returns before the commit record is durable  — S1, CODE

**Where.** `db.rs:618-629` (`Db::commit`), `logger.rs:326-338` (`log_redo`),
`logger.rs:538-596` (`redo_log_runner`).

**What.** `commit()` does `log_redo(Commit)` and `log_undo(Commit)`, both of
which are `channel.send()` and return immediately. The runner thread lingers
200µs, batches, writes, then `do_sync()`. Nothing in `commit()` waits for that
`do_sync()`. `ARCHITECTURE.md` and the comment on `write_locked_page`
(`buffer.rs:247-250`) both assert that the redo log "is fsynced on every
commit"; that is not true of the call the application sees. A process crash or
power loss in the window after `commit()` returns and before the batch syncs
loses the transaction, and the caller was told it succeeded.

**Why it matters.** This is the D in ACID. Every consumer of `store` (including
`squeal-sql`'s `COMMIT`) currently reports success for something that may not
exist after a crash.

**Recommendation.**
1. Make `log_redo` for a `Commit` record return a completion handle (a
   `crossbeam::channel::bounded(1)` reply sender attached to the message, or a
   shared `(Mutex<u64>, Condvar)` "durable LSN" that the runner bumps after
   `do_sync`). `Db::commit` blocks on it before `tx_mgr.commit`.
2. Keep group commit: the runner already batches; the wait is per-batch, so N
   concurrent committers share one fsync. Do not wake per record.
3. Offer an explicit opt-out (`commit_nowait`, or a `Durability` enum on
   `begin`) for bulk loads where the caller accepts the risk; default to
   durable.
4. Expect the `File`-backend single-threaded commit rate to drop to roughly the
   device's fsync rate. That is the honest number. Multi-threaded throughput
   should hold up because of the shared batch.

**Test.** File backend: `commit()`, then immediately kill the process
(`std::process::abort` in a child process spawned by the test), reopen, assert
the row is present. Without the fix this flakes; with the fix it must be
deterministic. Also assert `commit()` latency ≥ one `do_sync()` on a fake
`DBFile` whose `do_sync` sleeps 5ms.

### T2. Page flush is gated on the wrong LSN, so pages can reach disk before their own redo record  — S1, CODE

**Where.** `page.rs:436-460` (`set_dirty` stamps the page with the *current
watermark*), `buffer.rs:1109` and `buffer.rs:1236` (writer flushes when
`page.lsn < last_written`), `db.rs:791-794` (`insert` mutates the page, *then*
logs redo). `todo.txt [4]` already calls the redesign "worthwhile but no longer
urgent"; it is urgent.

**What.** A page is stamped with `W = last_written` at dirty time. Its own
operation's redo LSN is allocated later and is `> W`. The writer flushes the
page as soon as `last_written > W`, i.e. as soon as *any* later record lands,
not this one. Interleaving (single writer thread suffices):

1. Watermark is 10. Op A's predecessor (LSN 11) is sitting in the redo channel,
   not yet synced.
2. Op A dirties page P; P.lsn = 10.
3. Redo batch containing LSN 11 syncs; watermark = 11.
4. Writer sees P.lsn (10) < 11, flushes P, which contains A's row.
5. A's redo record (LSN 12) has not been written. Crash.

On reopen A's row is on disk with a `txn_id` that appears in neither log.
`is_committed` (`txn.rs:246-249`) is "absent from both sets", so the row is
visible as committed, and nothing can ever revert it (no undo record either,
because `insert` logs undo after the page write too).

**Why it matters.** This is the write-ahead invariant. Everything the recovery
path does assumes it holds.

**Recommendation.** Stamp pages with the LSN that protects them, not the
watermark:
1. Allocate the redo LSN *before* mutating the page (move `clock.next_lsn()`
   out of `log_redo` into the operation; pass it into the closure that runs
   under the page lock).
2. Under the page lock, set `page.lsn = max(page.lsn, this_lsn)`.
3. Writer flushes when `page.lsn <= last_written` (inclusive).
4. Then `log_redo(record_with_that_lsn)`. Redo records may now arrive at the
   runner slightly out of LSN order across threads; `highest_lsn` in the runner
   (`logger.rs:569-572`) must become "highest *contiguous* LSN" or the runner
   must sort the batch before `mark_written`. Simplest: allocate the LSN under
   the same `redo_tx.send` critical section (a small mutex around
   next_lsn+send) so channel order equals LSN order.
5. Delete the `u64::MAX` cold-start sentinel special case once pages carry real
   LSNs.

**Test.** Fake `DBFile` for the redo file whose `do_sync` blocks on a barrier;
insert two rows on two threads; release only the first batch; assert the second
row's page has not been written (inspect the main file bytes) until its own
record is synced.

### T3. `checkpoint()` with an active transaction turns uncommitted writes into committed ones after a crash  — S1, CONFIRMED

**Where.** `db.rs:383-407` (`checkpoint`), `buffer.rs:495-519`
(`flush_dirty_cached_pages` flushes *every* dirty page), `logger.rs:531,591`
(runners truncate both logs on `Checkpoint`). Auto-triggered from `begin()` at
`db.rs:577-581` whenever a log exceeds 16 MiB, so any busy workload hits this
without ever calling `checkpoint()` itself.

**Reproduction (MemFile, public API).**
```
create db, create table, close, reopen with the returned files
t = begin(); insert(7, "uncommitted", t)
checkpoint()
sleep 50ms; mem::forget(t)      // never commits, never rolls back
drop(db)                        // crash: no close()
open_using(same files); begin(); find(7)  -> Some("uncommitted")
```

**What.** Checkpoint flushes the uncommitted row's page, syncs, and truncates
both logs. The undo record that would have reverted it is gone. On reopen the
row's transaction is unknown, so it is visible.

**Recommendation.** Two viable designs; pick one.
- **Fuzzy checkpoint (ARIES-style, recommended).** Do not truncate. Write a
  `Checkpoint { redo_lsn, active_txns: [...] }` record; on recovery, start
  redo at `redo_lsn` and undo every transaction in `active_txns` that has no
  later `Commit`. Truncate/rotate only records older than the *oldest active
  transaction's first LSN*. Requires logging the first LSN per transaction
  (cheap: record it at first write).
- **Quiesced checkpoint (simpler, worse latency).** `checkpoint()` takes a
  write side of an `RwLock` that every `begin()`/write holds the read side of,
  waits for `active_count() == 0`, then flushes and truncates. Long readers
  block checkpoints; combine with T7's reader-pinning limits.

Either way, undo information for still-active transactions must survive the
checkpoint. Today it exists only in `undo_txns` (memory) after truncation.

**Test.** The reproduction above, asserting `find(7) == None`. Plus: checkpoint
between two writes of the same transaction, commit, crash, reopen, assert both
writes present.

### T4. Redo and undo are two independently-synced files; the commit point is not atomic  — S1, CODE

**Where.** `logger.rs:160-187` (two runner threads, two channels),
`db.rs:445-547` (`process_redo` decides "committed" from the redo file;
`process_undo` decides "revert" from the *undo* file).

**What.** `commit()` sends `Commit` to both channels. Each runner syncs on its
own schedule. A crash between the two syncs leaves the files disagreeing:
- Undo has `Commit`, redo does not: redo replays nothing for the transaction,
  undo keeps its ops (`operations.retain(!committed)`), so any of its rows that
  reached disk stay, visible as committed (see T2 for how rows reach disk).
- Redo has `Commit`, undo does not: redo replays the transaction, then
  `process_undo` reverts it. A committed transaction is rolled back.

Additionally, `process_redo` computes `inprogress` and `rollback` sets and never
uses them (`db.rs:448-467`); recovery has no notion of "seen in redo but never
committed, therefore abort" and relies entirely on the undo file.

**Recommendation.** One log. Put the pre-image into the same record as the
post-image (`Operation::Mod { pre, post }`, `Del { pre }`, `Add { post }`), keep
one runner, one fsync per batch, and derive the in-memory undo trail from the
same records. Recovery becomes the standard three passes over one file:
analysis (which txns committed), redo (all records, idempotent), undo (every
transaction without a `Commit`, in reverse LSN order). This also removes the
second fsync per commit (see P1) and the u16 overflow in T10.

If keeping two files is non-negotiable, the commit record must live in exactly
one of them (redo), and `process_undo` must take its "committed" set from the
redo scan, never from the undo file.

**Test.** Fault-inject with a `DBFile` wrapper that drops writes to one of the
two files after N bytes; run commit; reopen; assert consistency under both
orderings.

### T5. The checkpoint sequence is not crash-ordered: log truncation can precede the header write and the header is never synced  — S1, CODE

**Where.** `db.rs:396-405`, `buffer.rs:1257-1263` (`WriteHeader` handler:
`pwrite_all`, no `do_sync`), `logger.rs:531,591` (`truncate`).

**What.** `checkpoint()` does: flush+sync pages (synchronous), `write_header`
(asynchronous, queued, not synced), `logger.checkpoint` (asynchronous, truncates
when dequeued). Three problems:
1. Between `buffer.checkpoint()` returning and the runner dequeuing
   `Checkpoint`, other threads keep logging redo records *ahead of* the
   `Checkpoint` message in the channel and dirtying pages that were not in the
   flush. Those records get written and then truncated; the pages are dirty in
   cache only. Crash: committed writes lost. This is not exotic: `begin()`
   triggers checkpoints under load, while other threads are mid-transaction.
2. The header (`page_count`, `last_checkpoint`) may still be in the channel
   when the logs are truncated. Crash: header says `page_count = N_old`, logs
   are empty, index pages on disk point past `N_old`, `get_page` rejects them
   (`buffer.rs:719-724`), and `open` fails or replay panics.
3. Even when written, the header is never fsynced. `set_len(0)` on the logs is
   also not followed by a sync of the file or its directory.

**Recommendation.** Sequence as: (a) take a checkpoint LSN `C` = current redo
counter; (b) flush all pages dirtied *before* `C` and sync; (c) write and sync
the header; (d) discard only log records with LSN < `C`, never "everything"
(this requires the log runner to know record LSNs; with T4's single log that is
a prefix cut, implemented as a rotate-and-delete of the old segment). Also derive
`page_count` on open from the file length rather than trusting the header
(`(len - first_page_offset) / page_size`), which removes an entire class of
"header stale" failures.

### T6. Reads are not snapshot-isolated against committed deletes  — S2, CONFIRMED

**Where.** `db.rs:652-663` (commit's tombstone reclaim calls `table.remove`, a
physical removal).

**Reproduction.**
```
insert(1,"v0"); commit
reader = begin(); find(1) -> Some("v0")
t1 = begin(); remove(1, t1); commit(t1)
find(1, reader) -> None            // must still be Some("v0")
```

**What.** `discard_or_defer_undo` protects the undo *trail* while a reader might
need it, but the physical row and its index entry are removed at commit
regardless. The reader's snapshot has nothing to walk back to.

**Recommendation.** Defer reclamation exactly like undo discard: at commit,
capture the active set; park `(txn, del_records, waiters)`; reclaim in the
`begin()` drain once the waiters are gone. Equivalent formulation: a vacuum
horizon = min `ts` over active transactions; only tombstones committed before
the horizon may be reclaimed. This is the same horizon T7 needs.

### T7. Reader-pinned garbage has no bound and no back-pressure  — S2/P, CODE

**Where.** `logger.rs:252-286`, `db.rs:570-576`.

**What.** Every commit while any other transaction is active parks its full undo
trail (with cloned tuples) until *all* of those transactions finish. Draining
happens only inside `begin()`. A single long-lived reader (a `TableCursor` held
open by a query, or a session that issued `BEGIN` and walked away; see
`squeal-sql/src/conn/connection.rs:433-439`) pins every commit's write set in
memory indefinitely. There is no way to observe or cap this.

**Recommendation.** Track per-transaction age and pinned bytes; expose them
(`Db::stats()`); add a configurable cap after which the oldest reader is marked
as "snapshot too old" (its next read errors) rather than letting memory grow.
Move the drain out of `begin()` into commit/rollback paths and a periodic tick
so it happens without new transactions.

### T8. `Transaction` is `Clone`; dropping a clone after commit rolls back committed data  — S1, CONFIRMED

**Where.** `txn.rs:82-86` (`#[derive(Clone)]`), `txn.rs:127-133` (`Drop`
calls `rollback` → `abort`), `db.rs:737-742` (`revert_aborted` replays undo
ops that were deferred by T7). `TableCursor` derives `Clone` too
(`cursor.rs:50-57`) and owns a `Transaction`, so cloning a cursor has the same
effect.

**Reproduction.**
```
insert(1,"v0"); commit
long_reader = begin()             // forces t1's undo discard to be deferred
t1 = begin(); update(1,"v1",t1); c = t1.clone(); commit(t1)
find(1) -> Some("v1")
drop(c)                           // Drop -> abort(t1): committed txn enters `aborting`
begin()                           // drain_aborting -> update_if_txn restores "v0"
find(1) -> Some("v0")             // committed write silently reverted
```

**Recommendation.** Remove `Clone` from `Transaction` (share via `Arc` where a
cursor needs it). Independently harden `TransactionManager::abort`: refuse to
move an id into `aborting` unless it is currently in `active` (return
`TransactionAlreadyFinished`). That second change alone closes this hole and
also guards the `AbortOnConflict` path, which today relies on "second rollback
is harmless" reasoning that is only true while undo discard is immediate.

### T9. A transaction cannot update or delete a row it inserted  — S2, CONFIRMED

**Where.** `db.rs:846-849` and `db.rs:889-892` (`find_last_committed` on the
transaction's own fresh insert returns `NoAncestor`, mapped to `KeyNotFound`).

**Reproduction.** `t = begin(); insert(1,"v0",t); update(1,"v1",t)` returns
`KeyNotFound(1)`; `remove(1,t)` likewise. Commit then shows `"v0"`.

**What.** The pre-image resolution insists on a *committed* ancestor. For an
own-insert there is none, but the correct pre-image for undo purposes is
"nothing" (undo = remove the row), and for a second own-update it is the
transaction's own current version chain. `check_write_conflict` already handles
`writer == txn`; the pre-image step does not.

**Recommendation.** In `build`, if `current.txn_id == txn`: for update, keep the
existing undo chain (`updated.undo_id = current.undo_id`, so a rollback still
walks to whatever the first write's pre-image was, or removes the row if the
first write was an insert); for remove of an own-insert, physically remove the
row and its index entry now (no tombstone needed) and log a `Del` whose undo is
a no-op. Add tests for insert→update→commit, insert→remove→commit,
insert→update→rollback, update→update→rollback.

### T10. Undo ids are `u16`; a transaction with more than 65,535 writes corrupts its own undo chain  — S1, CODE

**Where.** `logger.rs:86` (`UndoId(u16)`), `logger.rs:598-602`
(`usize as u16` truncation), `logger.rs:288-295`, `db.rs:1046`
(`find_undo_tuple(txn, undo_id)` indexes the vec by it).

**What.** `next_undo_id` returns `len() as u16`. Past 65,535 operations the id
wraps, so a tuple's `undo_id` points at the wrong pre-image. Rollback restores
the wrong version; MVCC readers walk to the wrong ancestor. `bulk_load` defaults
to 200 rows per transaction so tests never see it; a `squeal-sql` `BEGIN` with a
large import will.

**Recommendation.** `u32` at minimum (`u64` costs one more varint byte only when
large). With T4's single-log design the undo id can become the LSN of the
record holding the pre-image, which is 64-bit already and needs no per-txn
counter. Add a test that performs 70,000 updates of one row in one transaction
and rolls back.

### T11. Wall-clock timestamps are the ordering primitive for isolation and conflict detection  — S2, CODE

**Where.** `constant.rs:15-20` (`SystemTime::now()`), `txn.rs:195`,
`db.rs:1112` (`writer.ts() >= txn.ts()`), `db.rs:1190` (`txn.ts() < reader_ts`).

**What.** `SystemTime` is not monotonic (NTP steps, VM migration, manual
changes). The `create_transaction` critical section guarantees lock order, but
not that `ts()` increases in lock order if the clock steps backward between two
calls. A backward step makes a later transaction look older: it becomes visible
to readers that began before it, and `check_write_conflict` stops detecting the
conflict. The `unwrap()` on `duration_since(UNIX_EPOCH)` also panics if the
clock is before 1970. Every tuple and every index entry also serializes this
`u128` (see P4).

**Recommendation.** Replace `ts` with a per-`Db` monotonically increasing
logical start timestamp (`AtomicU64`, seeded on open from `max(persisted
high-water mark, highest txn id/ts seen in the log) + 1`, persisted at
checkpoint and recoverable from the log). Keep the wall clock out of ordering
decisions entirely. If cross-session ordering must survive a stale persisted
sequence, seed from the log scan (which already happens in `process_redo`)
rather than from time.

### T12. Failure paths leave transactions active forever  — S2, CODE

**Where.** `db.rs:585-597` (`into_id` then `require_active` / `log_redo` `?`),
`db.rs:684-697` (`rollback_by_id`: `revert_txn_writes` `?` before
`finish_rolled_back`).

**What.** `into_id` deliberately disarms the guard so a failed commit does not
roll back. But nothing re-arms anything: if `revert_txn_writes` fails (lock
contention exhausted, table dropped, I/O error), the id stays in `active`
forever. Consequences: every future write to its rows returns `WriteConflict`
permanently; it is in every later reader's snapshot, so T7's deferred discards
never drain; `checkpoint` under a quiesce design (T3) would never run. The
`Transaction` is gone, so the application has no handle to retry with.

**Recommendation.** On any failure after `into_id`, move the id into
`aborting` (not leave it in `active`); `drain_aborting` already retries reverts
opportunistically. Return the error, but with the transaction in a state the
system can recover from. Add a test with a `DBFile` that fails writes once.

### T13. Undo replay order is forward, not reverse  — S3, CODE

**Where.** `db.rs:710-725`.

**What.** Works today only because every pre-image is resolved to the
*committed* ancestor, so any order restores the same value. The moment T9 is
fixed (own-write chains), or a `Mod` pre-image is the transaction's own prior
version, forward order restores an intermediate value. Reverse the iteration
now, while it is still a no-op behaviorally, and add a test that would catch it
(update A→B→C in one txn, rollback, expect A, with a pre-image chain that is
not all "A").

### T14. Row relocation on update is not atomic to readers  — S2, LIKELY

**Where.** `bplustree.rs:465-481` (remove from page, release lock, `write_data`
elsewhere, then repoint index).

**What.** A concurrent `find` between the remove and the index update resolves
the old page id and gets `None`: a committed, existing key reads as missing.
`RangeCursor` explicitly skips such rows (`cursor.rs:223-225`), so a range scan
silently drops the row. The window is small but it is on the hot update path
whenever a row grows past its page's remaining space (adding `undo_id` on first
update grows every row by a few bytes, so this triggers on full pages
routinely).

**Recommendation.** Write the new copy first (new page), repoint the index,
then remove the old copy; readers see either the old or the new. Or hold the
leaf index page lock across the whole relocation (crabbing already exists).

### T15. Recovery does not replay `Add`/`Mod` for transactions that committed but whose page writes were superseded  — S3, CODE (edge)

**Where.** `bplustree.rs:195-205` (`insert_if_needed` = "exists ⇒ skip").

**What.** "Exists" means the key exists, not "this version exists". Sequence:
txn A inserts K (v1), commits; txn B updates K (v2), commits; crash with only
A's page flush durable. Replay: A's `Add` skipped (exists), B's `Mod` applied via
`update_if_needed` which compares data, fine. Now the reverse: B's flush durable
but A's `Commit` record not… cannot happen once T1/T2 hold. Low priority on its
own; listed so the single-log redesign (T4) uses page LSNs (`page.lsn >=
record.lsn ⇒ skip`), which is the correct idempotence test and makes
`insert_if_needed`'s existence heuristic unnecessary.

### T16. Free list and page count are only persisted at checkpoint; crash recovery can double-allocate pages  — S1, CODE

**Where.** `buffer.rs:529-537` (`free_page` in memory only), `db.rs:1435-1453`
(persisted in `write_system_tables`), `buffer.rs:654-665` (`alloc_page` pops
from the in-memory list).

**What.** After a checkpoint the free list on disk is `F`. A transaction takes
page X from `F`, writes data, its page and index pages flush, it commits, the
redo record syncs. Crash. Reopen restores `F` (still containing X); replay sees
the row already present (`insert_if_needed`) and leaves X alone; the next
allocation hands X out again, overwriting committed data.

**Recommendation.** Either log allocation/free events (replay rebuilds the list),
or never reuse a page freed after the last checkpoint until the next checkpoint
has persisted the list (a "pending free" list promoted at checkpoint), *and* on
open reconcile the persisted free list against reachable pages (walk every
table's index and data chains; anything reachable is removed from the free
list). The reconcile pass is cheap insurance regardless.

### T17. `drop_table` frees pages that in-flight operations may still hold  — S3, documented

Documented at `db.rs:1284-1292`. Fine for the current `squeal-sql` usage, but it
should at least be excluded by a table-level `RwLock` (readers take the read
side for the duration of an operation, `drop_table` takes the write side), or
freed pages should go to the pending-free list from T16 so nothing reuses them
until the next checkpoint. Also: `drop_table` is not logged, so replaying a redo
record for a dropped table's id hits `TableNotFound` and aborts `open`.

---

## Part 2. Security, robustness, and operational footguns

The threat model that applies to an embedded engine is: a corrupted or
adversarial database file, a hostile or buggy caller, and concurrent processes.
Code execution is not in scope; availability and integrity are.

### S1. No format version, no header checksum, no header validation  — S3, CODE

**Where.** `db.rs:133-143` (`Header`), `db.rs:238-247` (open).

**What.** The header carries a 2-byte magic and nothing else that identifies the
on-disk format. `PageHeader`, `Tuple`, `TransactionId`, and the log record
enums are all `postcard`-derived; adding or reordering a field changes the wire
layout silently, and an old file opened by new code decodes garbage without an
error. `page_size` and `first_page_offset` are trusted as read: `page_size = 0`
underflows `size - PAGE_OVERHEAD` in `Page::new_with_content`; a huge
`page_size` allocates that much per page read (`read_page` allocates
`vec![0; page_size]`). The header is written in place at offset 0 with no
checksum, so a torn header write at checkpoint is undetectable.

**Recommendation.** Add `format_version: u32`, `header_checksum: u32` (over the
rest), validate `page_size` (power of two, within `[4 KiB, 1 MiB]`,
`> PAGE_OVERHEAD + margin`), `first_page_offset >= header size`, and reject
otherwise with a typed error. Write the header to two alternating slots with a
generation number and pick the newest valid one on open (LMDB/SQLite pattern);
this removes the torn-header case entirely and pairs with T5.

### S2. Log files have no record framing or per-record checksum; a torn tail makes the database unopenable  — S3, CODE

**Where.** `logger.rs:519,573` (raw `postcard` bytes appended back to back),
`db.rs:452,478,515` (`take_from_bytes` with `?`).

**What.** A crash mid-`write_all` leaves a partial record at the tail. Replay
hits a deserialization error and `open_using` returns `Err`. There is no way to
open the database short of hand-truncating the log. Mid-file corruption (bit
flip) is indistinguishable from a torn tail. `process_redo` also `panic!`s on an
unexpected variant instead of erroring.

**Recommendation.** Frame every record as `len: u32, crc32: u32, bytes`. On
replay, stop cleanly at the first record whose length runs past EOF or whose CRC
fails *at the tail*; treat a CRC failure followed by more valid records as
corruption and refuse to open (with a clear error). Replace both `panic!`s with
`StoreError`.

### S3. Hand-rolled key parser panics on malformed input  — S3, CODE

**Where.** `valueitem.rs:311-376` (`from_bytes_many`: unchecked slicing,
`try_into().unwrap()`), `valueitem.rs:106-121` (`IndexKey::from_bytes`).
Callers: `squeal-sql/src/table.rs:197`, `squeal-sql/src/source/run.rs:45`.

**What.** These decode bytes that came from disk. Any truncation or a
`real_len` larger than the remaining buffer panics the thread (index out of
range). A panic inside a page lock's critical section leaves `ArcLock`'s map
entry held forever (the guard is dropped during unwind, so actually released,
but the `std::sync::RwLock` on `PageInner` becomes poisoned and every later
access returns `UnknownError`). Also, `String::from_utf8(...).unwrap_or_default()`
silently turns invalid UTF-8 into an empty string, changing a key's value.

**Recommendation.** Make both functions return `Result`, bounds-check every
slice, reject invalid UTF-8, and cap `len`/`real_len` against the input length.
Consider replacing the custom codec with `postcard` for `IndexKey` too, since
`Tuple` already uses it. Fuzz `IndexKey::from_bytes`, `Tuple::from`,
`Page::from_bytes`, and the log record decoder with `cargo-fuzz`; each should
return `Err`, never panic.

### S4. `Db::create` on an existing path silently destroys the existing database  — S3, CODE

**Where.** `db.rs:1359-1364` (`create(true)` without `truncate` or
`create_new`), `db.rs:1377-1379` (lock taken *after* the header is written).

**What.** Creating over an existing file rewrites the header with
`page_count = 0`; old pages are then overwritten as the new database grows. The
`.undo`/`.redo` files are opened without truncation, so a stale log is replayed
against the fresh database on the next open. The exclusive lock is acquired
after the destructive write, so two `create`s racing both write headers.

**Recommendation.** `create_new(true)` for the main file (fail if it exists,
with a distinct `AlreadyExists` error); truncate the two log files on create;
lock before writing anything. `Db::open` should lock before reading the header
as well (`db.rs:240-246` reads first).

### S5. Advisory locking only; `Db::delete` ignores it  — S3, CODE

**Where.** `memfile.rs:98-100` (`File::try_lock`, advisory `flock`),
`db.rs:1503-1510`.

**What.** `flock` is respected only by cooperating processes and is silently
dropped on some network filesystems. `delete` unlinks the three files with no
lock check, so a second process can delete a live database. The `mmap` in
`load_logs` (`db.rs:420,427`) is `unsafe` precisely because another process
modifying the file is UB; the advisory lock is the only thing standing between
that and a real memory-safety bug.

**Recommendation.** `delete` should try the lock first and refuse if held.
Replace `mmap` with `read_to_end` (the logs are bounded to ~16 MiB by the
auto-checkpoint anyway; this also removes the `memmap` dependency, which is
unmaintained). Document that only local filesystems are supported.

### S6. System tables live on one page; exceeding it bricks the database  — S2, CONFIRMED

**Where.** `db.rs:1435-1453` (`write_system_tables`), `db.rs:1253-1271`
(`create_table` inserts into the map before persisting).

**Reproduction.** Create tables named `table_number_00000…`: the 476th
`create_table` fails with `PageCapacityError`. The table is nevertheless present
in memory (`table_id_by_name` finds it). From then on `checkpoint()` and
`close()` both fail with the same error forever. Nothing since the last
checkpoint can be persisted; the auto-checkpoint in `begin()` fails every call.

**What.** The catalog (page 0), the generator table (page 1), and the free list
(page 2) are each a single 16 KiB page. Page 0 holds one tuple per table so it
has a hard cap; page 2 relies on the overflow-chain mechanism for a single large
tuple, which works but means a big free list rewrites a multi-page chain on
every `create_table`/`drop_table`/checkpoint.

**Recommendation.** Short term: check the serialized catalog size *before*
mutating `tables` and fail `create_table` cleanly; roll back the generator and
freed pages on failure. Medium term: make the catalog an ordinary internal
B+Tree table (the engine already has one) and the free list a page-chain
bitmap. Add a test that creates 2,000 tables and closes/reopens.

### S7. Table-name and key validation gaps  — S3, CODE

- Table names are checked only for length. A name colliding with an internal
  generator name (`__system.transactions`, `__system.core.*`) fails with a
  confusing `DuplicateName`; reserve the `__system.` prefix explicitly.
- `ValueItem::Ord` panics on mixed-type comparison and on any `Blob`
  (`valueitem.rs:433-440`). A `Rec` key whose field types differ from an
  existing key in the same tree (possible: `store` has no schema) panics inside
  `route_to_leaf`, under a page lock. Return `Ordering` based on a type rank
  instead (Null < Boolean < Integer < Double < Datetime < Str < Blob) and order
  blobs bytewise; never panic in `cmp`.
- `DBIdType::Int(u64::MAX)` is the root sentinel (`bplustree.rs:1324`). A user
  inserting key `u64::MAX` collides with it. Reserve it and reject at insert.
- `Ord`/`Eq`/`Hash` disagreement on `Str`/`Blob` reserved capacity is
  documented; it should be fixed by dropping capacity from `Eq` rather than
  documented.

### S8. Panics as control flow  — S3, CODE

`bplustree.rs` has eleven `panic!`s on tree-shape invariants; `page.rs`
`unwrap()`s `std::sync::RwLock` (poisoning turns one panic into permanent
failure of that page); `resolve_visible` panics on a tuple without `txn_id`
(`db.rs:1065`), which any pre-existing or hand-crafted file can produce. For an
embedded library, a panic is a process crash for the host. Convert the
structural ones to `StoreError::Corruption(String)`, switch `PageInner`'s lock
to `parking_lot::RwLock` (no poisoning; already a dependency), and make
`ArcLock` `parking_lot` too (`arclock.rs:111,177` `unwrap()` on a poisoned
global map takes down every page lock at once).

### S9. Resource exhaustion knobs  — S3, CODE

- `MAX_OVERFLOW_PAGES = 1024` bounds one tuple at ~16 MiB; but there is no cap
  on tuple *count* per transaction, undo trail size, or number of active
  transactions. See T7 and T10.
- `retry_on_contention` + `ArcLock`'s hardcoded 60s wait mean a stuck page lock
  turns every touching operation into a 60s stall with no diagnostic. Honor the
  timeout parameter (see P2) and surface which page and which thread holds it.
- `begin()` calls `get_metadata` on two files (two `fstat`) and can run a
  full checkpoint inline; a caller doing `begin()` in a tight loop pays for
  everyone's checkpoint. Move the size check into the log runner (it knows the
  byte count it has written) and signal a background checkpoint instead.

---

## Part 3. Performance

Numbers below reference `ARCHITECTURE.md`'s table (File backend: ~14k inserts/s
flat across threads; Mem: 75k→41k from 1 to 8 threads) and
`store/benches/BASELINE.md`.

### P1. Two fsyncs per commit where one would do  — P

Every write is logged to two files, each synced per batch
(`logger.rs:525-527,578-581`). The undo file is never needed for anything the
redo file could not carry (T4). Merging halves the sync count and the write
amplification. Combined with T1 (commit waits) this is the difference between
"honest durability at N/2 commits/s" and "honest durability at N commits/s".

### P2. `ArcLock` is a global serialization point with busy-polling  — P, CODE

**Where.** `arclock.rs:108-158,160-196`.

Every `get_page_mut` takes the *write* side of one global `RwLock<HashMap>`;
every waiter re-takes that write lock every 100µs while sleeping; the map never
shrinks (`cleanup` is never called), and the caller's timeout is ignored. On the
Mem backend this is the documented reason throughput *drops* with more threads.

**Recommendation.** Put the lock in the cache entry: `PageEntry` gains an
`Arc<parking_lot::ReentrantMutex<()>>` (or a plain `Mutex` plus the existing
`ThreadId` check) created when the page is installed. `get_page_mut` then does
one `buffer.read()` to fetch the `Arc` and `lock_for(timeout)` on it. Waiters
block on a futex, not a sleep loop; timeouts are honored; no global write lock on
the hot path. Keep `ArcLock` as a fallback for pages not yet cached, or install
the entry first.

### P3. Cache bookkeeping does two `SystemTime::now()` calls and a priority-queue update per page access  — P, CODE

**Where.** `buffer.rs:736,773,970-981,849-854`.

`get_page` on a hit calls `update_page_access` (a `timestamp()` plus a sharded
`PriorityQueue::change_priority`, O(log n) under a shard write lock) and
`page.accessed()` (another `timestamp()`). The find path touches 3–4 pages per
lookup. Replace with CLOCK (second-chance): an `AtomicBool referenced` per entry
set on access, a ring pointer for eviction. No timestamps, no heap, no lock on
the hit path. Drop `accessed`/`saved`/`written` `AtomicU128`s from `Page` unless
something reads them (nothing does).

### P4. Every row and every index entry carries a 128-bit timestamp  — P, CODE

**Where.** `tuple.rs:76-91` (`txn_id: Option<TransactionId>` serializes
`u64 + u128`), `bplustree.rs:44-53` (21 of the 64 budgeted bytes per index
entry), `bplustree.rs:258-263` (index entries get a `txn_id` they never use).

Index entries are looked up by key and resolved through the data tuple; their
`txn_id` is dead weight that cuts fanout by roughly a third. Data tuples need a
transaction *identity*, not a wall-clock (`T11`): a `u64` logical start
timestamp is enough and is 1–9 bytes as a varint. Expected effect: ~30% more
entries per index page, one fewer level for large tables, fewer bytes through
`postcard` on every page write.

### P5. Inner-node routing clones and decodes every entry on the page  — P, CODE

**Where.** `bplustree.rs:716-730` (`page.iter()` → `values()` clones every
`Tuple` into a `Vec`, then `from_bytes::<Node>` per row until a match),
`page.rs:479-490`.

With `nodes_per_page = 256`, one lookup at depth 3 clones ~768 tuples and runs
~400 `postcard` decodes. `AnyTuplePage` is a `BTreeMap`; use
`range(..=id).next_back()` / `range(id..).next()` on the map directly (add a
`PageTuple::successor(&id)` method) and store `Node` as a fixed 9-byte encoding
that can be read without `postcard`. Same for `remove_index_entry`,
`update_index_entry`, and `insert_recursive`'s scan.

### P6. Whole-page re-serialization on every flush and a deep clone on every `write_page(&Page)`  — P, CODE

**Where.** `page.rs:635-650` (`to_bytes_snapshot` serializes the whole
`Vec<&Tuple>` with `postcard`), `buffer.rs:216-230` (`write_page` deep-clones),
`todo.txt [3]`.

A one-row change rewrites and re-encodes a 16 KiB page. A slotted page layout
(fixed header, slot array, tuples as raw bytes with an offset table) makes
`add`/`replace`/`remove` byte moves within the page, makes `to_bytes` a memcpy,
and makes `from_bytes` a bounds check instead of a full decode into a
`BTreeMap`. This is the single biggest structural win available but also the
largest change; sequence it after the correctness items above so the new format
can carry per-page LSNs and the checksum from day one (S1/T2).

### P7. Per-row visibility check clones the reader's whole snapshot  — P, CODE

**Where.** `db.rs:1181-1185` in `find_visible_to`, called once per row by
`TableCursor::next` and `RangeCursor::next`.

A scan of M rows with N concurrent transactions allocates M hash sets of N
entries. Resolve the snapshot once per cursor (or once per `find`) and pass
`&HashSet` into `resolve_visible`. `is_committed` also takes two `RwLock` reads
per chain hop; a single `RwLock<HashMap<TxnId, State>>` (Active | Aborting |
Committed(commit_ts)) collapses those and enables the classic "visible iff
commit_ts < reader_start_ts" test, removing the snapshot set from the hot path
entirely.

### P8. Three copies of every pre-image per operation  — P, CODE

**Where.** `logger.rs:195-233` (`op.clone()` into the vec, `op.clone()` into
the message, plus the caller's own), `db.rs:863-864` (redo *and* undo records
each clone the tuple).

`Tuple.data` is an `Arc<[u8]>` so payload copies are refcount bumps, but the
`Operation`/`Record`/`Tuple`/`TransactionId` wrappers allocate each time. With
T4's single log this becomes one `Arc<Record>` shared by the channel message and
the in-memory undo index.

### P9. Overflow chains do synchronous, unbatched header writes  — P, documented

`buffer.rs:466-486` (`write_page_header` is a direct `pwrite`), called several
times per overflow allocation and on every rewrite of an overflow page's owner
(`handle_large_page_size` tears down and rebuilds the chain on every write, even
when the size did not change). Documented in `ARCHITECTURE.md` as the reason
large-value File inserts run at ~600–1000/s. Fix: only rebuild the chain when
the required page count changes; queue continuation-page writes through the
writer thread like every other page.

### P10. Group-commit linger is paid by every isolated write  — P

`LOG_BATCH_LINGER = 200µs` (`logger.rs:480`) is charged after the *first*
message even when no other producer exists. Adaptive linger (skip the wait when
the channel was empty for the previous batch; extend it when the previous batch
was full) recovers the single-writer latency without losing batching under
load. Once T1 makes commit wait, this linger becomes directly visible as commit
latency, so it is worth tuning at the same time.

---

## Suggested sequencing

The correctness items interact; fixing them piecemeal risks re-doing work.
A defensible order, each step independently testable:

1. **Stop the bleeding without a format change.** T8 (remove `Clone`, guard
   `abort`), T9 (own-row update/delete), T6 (defer tombstone reclaim), T12
   (failed rollback → `aborting`), S4 (`create_new`), S6 (fail `create_table`
   cleanly), S3 (bounds-checked key parser). All small, all with the
   reproductions above as regression tests.
2. **Single WAL with framed, checksummed records and pre+post images** (T4, S2,
   T10, P1, P8). Recovery = analysis/redo/undo over one file. This is the
   foundation for the next two steps.
3. **Correct page-LSN gating and durable commit** (T2, T1, P10). Pages carry the
   LSN of their last mutation; writer flushes on `<=`; commit waits on the
   durable-LSN condvar.
4. **Fuzzy checkpoint and header discipline** (T3, T5, T16, S1). Checkpoint
   record with active-txn list, log records discarded by LSN not wholesale,
   double-slot versioned header, page count derived from file length, free-list
   reconciliation on open.
5. **Logical timestamps and slimmer tuples** (T11, P4, P7). Replace `(id, ts:
   u128)` with a `u64` start timestamp; drop `txn_id` from index entries; make
   visibility a commit-timestamp comparison.
6. **Locking and cache** (P2, P3, P5). Per-entry locks with real timeouts,
   CLOCK eviction, `BTreeMap::range` routing.
7. **Slotted pages** (P6, P9) once the format is being changed anyway.

Two pieces of test infrastructure would pay for themselves across all of the
above and are worth building first:

- A **fault-injecting `DBFile`** (drop or delay writes/syncs to a chosen file
  after N calls, optionally return errors) so T1–T5, T16 and S2 can be tested
  deterministically instead of via sleeps.
- A **crash-consistency harness**: run a random workload against `NamedMemFile`,
  snapshot the three buffers at a random point (simulated power loss = keep
  only what `do_sync` has been called on, which the fake file can track), reopen,
  and check every committed transaction is present and every uncommitted one is
  absent. This is the test that would have caught T2, T3, T4 and T5 together.
