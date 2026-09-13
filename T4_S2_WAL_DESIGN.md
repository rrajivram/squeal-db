# T4 + S2 design: one framed, checksummed WAL

Status: **design only, not implemented**. Follows T10 (widened `UndoId`, committed `1deaf31`),
per `audit-progress.md`'s Phase 2 staging decision. This document is the "full design" that
staging deferred — detailed enough to implement from directly, but no code changes have been
made yet. Backward compatibility with existing on-disk databases is explicitly NOT required
(confirmed with the user) — every point below assumes a clean break.

## 1. What's wrong today (recap, grounded in the real code)

- **T4**: `Db<F>` has two independently-managed log files, `undo_file`/`redo_file`
  (`store/src/db.rs:155-156`), each with its own runner thread (`undo_log_runner`/
  `redo_log_runner`, `store/src/logger.rs:482,538`) and its own `fsync`. `Db::update`/
  `Db::remove`/`Db::insert`/`Db::rollback_by_id` each make two separate calls —
  `logger.log_redo(op)` then `logger.log_undo(op)` (or vice versa) — with no ordering guarantee
  between when either lands durably. A crash between the two fsyncs leaves the two files
  disagreeing about whether an operation happened at all.
- **S2**: Every record is written as a bare `to_allocvec(&MsgType)` with no length prefix, no
  checksum (`store/src/logger.rs:519,573`). `Db::load_logs`/`process_redo`/`process_undo`
  (`store/src/db.rs:424-562`) decode with `take_from_bytes::<MsgType>` in a `while !buf.is_empty()`
  loop — a torn tail (crash mid `write_all`) leaves a partial record at the end, and
  `take_from_bytes` returns `Err` on it, which `load_logs` propagates via `?`, failing
  `Db::open` outright. There is no way today to tell "the last record is incomplete, drop it and
  open anyway" apart from "the file is genuinely corrupted, refuse to open."

## 2. On-disk record framing

Each persisted record (everything except the in-memory-only `ShutDown`/`Checkpoint` control
messages, which never reach disk today and won't under this design either) is framed as:

```
[u32 len (LE)] [u32 crc32 (LE)] [len bytes: postcard-encoded LogRecord]
```

- `len` and `crc32` are raw fixed-width LE integers, written directly (`u32::to_le_bytes`), NOT
  postcard-encoded — recovery must be able to parse the frame header without depending on
  postcard's own varint format ever staying compatible with itself, and it keeps the frame
  header a fixed, trivially-skippable 8 bytes.
- `crc32` is computed over exactly the `len` payload bytes (the postcard-encoded `LogRecord`),
  not over the length field. Use the `crc32fast` crate (add as a new `store` dependency — small,
  widely used, no reason to hand-roll this).
- One batch write (`file.write_all(&batch)`) may contain several framed records back to back,
  exactly as today's batching already concatenates several `to_allocvec` blobs — framing changes
  what's inside `batch`, not the batching/fsync structure around it.

### Recovery scan rule (this is S2's actual fix)

Scanning forward from the start of the file, at each position:

1. If fewer than 8 bytes remain → **torn tail at the frame header**. Stop scanning; everything
   before this position is the durable prefix. Not an error.
2. If ≥8 bytes remain but fewer than `len` bytes of payload follow the header → **torn tail at
   the payload**. Stop scanning at the START of this frame (the 8-byte header itself is
   incomplete evidence, not a usable record). Not an error.
3. If a full `len`-byte payload is present: compute `crc32` over it.
   - Match → valid record, decode it, continue scanning from the next frame.
   - Mismatch → check whether a **complete, well-formed frame** (steps 1-3 succeeding) exists
     immediately afterward.
     - If yes → this is **real, mid-file corruption** (there's more valid log after it, so this
       can't be an in-progress tail write) → refuse to open with a new
       `StoreError::LogCorruption` (clear message, includes byte offset), not a silent drop.
     - If no (EOF, or only another torn-tail-shaped fragment follows) → treat as the torn tail:
       this record's bytes were mid-write when the crash happened and just happen to have a
       length prefix that survived with a payload that didn't (or was itself partially
       overwritten). Stop scanning at the start of this frame. Not an error.

This exact positional rule (mismatch + more valid data after ⇒ hard error; mismatch + nothing
usable after ⇒ torn tail) is what makes "torn tail" and "corrupted" distinguishable at all,
which is the concrete thing S2 was missing.

Recovery logically truncates the file to the end of the last successfully-scanned record before
running the analysis/redo/undo passes below — not physically truncating the file. `Logger`'s own
subsequent appends (new writes after a recovering session opens the DB) then start with
`file.seek(SeekFrom::End(0))` the same way they do today, which lands them after the true content
end, not after any torn/corrupt trailing garbage — so the file's own O.S.-reported length can stay
untouched by recovery.

## 3. Unified `Operation` (combined pre/post images)

Replaces the current split (`Operation::Add/Del/Mod` carrying ONE `Record`, with redo getting
the post-image and undo getting the pre-image via two separate log calls):

```rust
enum Operation {
    Add { txn: TransactionId, post: Record },
    Mod { txn: TransactionId, pre: Record, post: Record },
    Del { txn: TransactionId, pre: Record },
    Commit(TransactionId, u128),
    Rollback(TransactionId, u128),
}
```

- `Add` carries only `post` — a fresh insert has no pre-image, nothing to derive.
- `Mod` carries both — `post.tuple.data` is genuinely new content that can't be derived from
  `pre`, so both must be stored (this is the one case that's NOT "free" to combine — it's exactly
  as many bytes as today's two separate records already cost, just in one write instead of two).
- `Del` carries only `pre` — redo derives the tombstoned version deterministically
  (`pre.tuple.clone()` + `.tombstone()`) instead of storing a near-duplicate second image. This
  is a real size win over today (which logs the full tombstoned tuple separately as the redo
  record).

`Record` (`table_id`, `timestamp`, `tuple`, `data_page`) is unchanged.

One call site per operation now: `Db::insert`/`update`/`remove`/`rollback_by_id` each call
`self.logger.log(op)` ONCE (replacing today's paired `log_redo`+`log_undo`), which returns the
single `LsnId` this operation was assigned.

## 4. `LogRecord` / `LogMsg` (replaces `RedoOperation`/`UndoOperation`/`MsgType`)

```rust
struct LogRecord {
    lsn: LsnId,
    operation: Operation,
}

enum LogMsg {
    Record(LogRecord),
    ShutDown,
    Checkpoint(u128),
}
```

`LsnId` is unchanged (`pub(crate) struct LsnId(pub(crate) u64)`, already exists). Only
`LogMsg::Record` variants are ever framed/written to disk; `ShutDown`/`Checkpoint` stay pure
in-memory control signals to the runner thread, exactly as `MsgType::ShutDown`/`Checkpoint` are
today (never appear in `batch`, per `undo_log_runner`/`redo_log_runner`'s existing `match`).

## 5. One runner thread, one channel

`Logger::set_db` (`store/src/logger.rs:160`) takes ONE `file: impl DBFile` instead of
`(undo_file, redo_file)`, creates ONE `bounded(LOG_CHANNEL_CAPACITY)` channel, and spawns ONE
`log_runner(file, rx, clock)` thread — replacing `undo_log_runner`+`redo_log_runner`. `log_runner`
is `redo_log_runner`'s existing batching/linger loop (`store/src/logger.rs:538-596`) almost
unchanged: same `MAX_LOG_BATCH`/`LOG_BATCH_LINGER` batching, same "write the batch, one fsync,
then `mark_written(highest_lsn)`" structure — just framing each `LogRecord` into the batch buffer
(`len`+`crc32`+bytes) instead of `to_allocvec`-ing it bare, and matching on `LogMsg` instead of
`MsgType`.

This incidentally fixes the other half of T4 (independently-synced files ⇒ commit not atomic):
one file, one thread, one `fsync` per batch — a batch either lands completely or the crash caught
it mid-`write_all`, handled by the torn-tail rule in §2. There is no longer a "redo landed, undo
didn't" state to reach, because there's only one artifact.

`Logger::shutdown`/`checkpoint` each send exactly one `ShutDown`/`Checkpoint(ts)` instead of two.

## 6. Undo pointer becomes the record's own LSN — retiring `UndoId`

This is what makes T10's fix (widening `UndoId` to `u64`, already committed) obsolete rather than
just safe: once every record has a real LSN, `Tuple`'s back-pointer to its own pre-image can just
be that LSN directly, instead of a per-transaction positional counter.

- `Tuple.undo_id: Option<UndoId>` → `Tuple.pre_lsn: Option<LsnId>` (rename recommended — `undo_id`
  no longer describes what the field holds).
  - `Option<LsnId>`'s postcard size is identical to `Option<UndoId>` post-T10 (both wrap a
    `u64`), so `bplustree.rs`'s `MAX_ENTRY_BYTES` comment (`store/src/tables/bplustree.rs:48`)
    needs only a label change (`Option<UndoId> → 1 (None)` becomes `Option<LsnId> → 1 (None)`),
    no numeric change — index entries still store `None` there.
- `Logger`'s in-memory bookkeeping (`store/src/logger.rs:126-142`) changes from a single
  per-transaction `Vec<Operation>` map to two maps:
  ```rust
  records: RwLock<HashMap<LsnId, Operation>>,          // global, keyed by LSN
  by_txn: RwLock<HashMap<TransactionId, Vec<LsnId>>>,   // for revert/cleanup, same role as today's undo_txns keys
  ```
  `log(op)` inserts into both (`records[lsn] = op`, `by_txn[txn].push(lsn)`) instead of pushing
  onto one `Vec` and minting a positional `UndoId` from its length.
- `find_undo_tuple(id: TransactionId, undo_id: UndoId)` → `find_record(lsn: LsnId) ->
  Option<Operation>` (or directly `Option<Tuple>`, matching today's return shape) — a flat
  `records.get(&lsn)`, no `TransactionId` needed at all (the record already carries its own txn),
  no positional index, **no wraparound possible ever** (an LSN is minted once, globally, by
  `LsnClock::next_lsn`, and never reused or truncated back to a smaller range within a session).
- `get_undo_operations(id)` (used by `revert_txn_writes`, `store/src/db.rs:824-827`) becomes
  `by_txn[id].iter().map(|lsn| records[lsn].clone())` — same shape callers see today.
- `discard_undo`/`discard_or_defer_undo`/`pending_undo_discards`/`drain_ready_undo_discards`
  (`store/src/logger.rs:238-286`) keep their exact same defer-until-no-active-reader logic,
  just removing a txn's LSNs from both `records` and `by_txn` (via `by_txn.remove(&id)`'s
  returned `Vec<LsnId>`) instead of removing one `Vec<Operation>` from a single map.

**Decision: undo replay stays forward-order, not reverse-LSN.** The audit's own sketch says
"undo... in reverse LSN order," matching classic ARIES. I'm deliberately NOT adopting that here:
`revert_undo_ops` (`store/src/db.rs:829-844`) applies each op via `update_if_txn`/`remove_if_txn`,
which are conditional on "the row still belongs to this txn right now" — each op independently
restores one row to ITS OWN pre-image, and (as T10's investigation confirmed empirically) a
transaction's repeated writes to the same row all carry the SAME pre-image already, because
`build()`'s pre-image resolution (`find_last_committed`) always walks straight to the true
committed ancestor rather than the immediately-preceding in-flight version. Order across
different rows never matters (disjoint keys); order across repeated writes to the same row
doesn't matter either (identical content). Reverse-order undo would be genuinely required only if
a future change made pre-images "immediately preceding version" instead of "true committed
ancestor" — flagging that coupling explicitly here so it isn't silently reintroduced as a latent
bug if that semantic ever changes. Keep forward order; it's simpler and already proven correct.

## 7. Recovery: three passes, one file

Replaces `Db::load_logs`/`process_redo`/`process_undo` (`store/src/db.rs:424-562`) with one
`process_log(buffer)`, reading the ONE log file (mmap'd for `File`, `.data()` for `MemFile`,
exactly as today's dual read does) and running:

1. **Analysis** — single forward scan (applying §2's torn-tail/corruption rule as it goes),
   building:
   - `committed: HashSet<TransactionId>` (saw a `Commit` record for this txn)
   - `by_txn: HashMap<TransactionId, Vec<(LsnId, Operation)>>` (every `Add`/`Mod`/`Del`, in scan
     order, which is LSN order since records are written in the order they were logged)
   - `max_lsn: Option<LsnId>` (highest LSN seen, for clock seeding)

   Drop today's `inprogress`/`rollback` `HashSet`s outright — both `process_redo` and
   `process_undo` already build them and never read them again (confirmed: `inprogress` is
   populated in both functions but never consulted; `rollback` likewise). They're dead
   bookkeeping today; don't carry them into the redesign. A transaction needing undo is fully
   characterized by "not in `committed`" — covers both a genuinely abandoned/in-progress
   transaction AND one that logged an explicit `Rollback` before the crash, identically, exactly
   as `process_undo`'s existing `operations.retain(|k,_v| !committed.contains(k))` already
   treats them.

2. **Redo** — for every `(lsn, op)` across every txn in `by_txn`, in LSN order, where the txn IS
   in `committed`: apply the post-image side —
   - `Add{post,..}` → `insert_if_needed(&post.tuple, txn)`
   - `Mod{post,..}` → `update_if_needed(post.tuple)`
   - `Del{pre,..}` → `remove(pre.tuple.id)` (today's exact behavior — the tombstone's identity
     is enough, no need to reconstruct and re-apply the derived tombstoned tuple just to delete
     it)

   Same idempotent-reapply contract as today's `process_redo` — a page that already reflects
   this write from before the crash (because a checkpoint flushed it) tolerates re-application
   without error, unchanged.

3. **Undo** — for every txn NOT in `committed`, replay its `by_txn` ops in forward LSN order
   (§6's decision) via the existing `revert_undo_ops` (`store/src/db.rs:829-844`), unchanged in
   shape — it already takes `&Vec<Operation>` and a `&TransactionId`, just now sourced from one
   unified `by_txn` map instead of two separately-decoded files agreeing (or not) on which
   records existed.

`max_lsn` seeds `LsnClock` exactly as today's `process_redo` tail does
(`logger.clock().mark_written(lsn)` + `advance_counter_past(lsn)`) — but now unconditionally
(today this only happens inside `process_redo`, so an empty redo file with a non-empty undo file
would silently skip clock seeding; moot with one file, since there's only one scan to seed from).

## 8. `Db<F>` / file-layout changes

- `Db<F>` (`store/src/db.rs:151-177`): `undo_file`/`redo_file` fields collapse to one
  `log_file: F`.
- `create_core_db` (`store/src/db.rs:1553+`): opens ONE file (suggest `name + ".wal"`) instead of
  `.undo`/`.redo` — one `create_new`/`do_lock`/`do_clone` instead of two, same S4/S5 semantics
  (exclusive create, exclusive lock) just applied once.
- `open_using`/`open_using_with_limits` (`store/src/db.rs:234-299`): take one `log_file: F`
  parameter instead of two, one `do_lock`/`do_clone`.
- `close` (`store/src/db.rs:339-392`): returns `(F, F)` (main file + log file) instead of
  `(F, F, F)`.
- `checkpoint`/`Logger::checkpoint`: unchanged in spirit (§5) — one truncate instead of two,
  removing the (latent, never-observed-failing-but-real) risk of the two runner threads
  truncating their respective files at very slightly different moments under load.

## 9. Test infrastructure needed (Phase 0 item, build before writing T4/S2's own tests)

A fault-injecting `DBFile` wrapper, per `audit-progress.md`'s existing Phase 0 note — needed to
actually exercise "crash mid-write" for S2's torn-tail path and T4's atomicity claim, not just
assert on already-complete files.

```rust
struct FaultyFile<F: DBFile> {
    inner: F,
    // Shared (Arc<Mutex<_>>) so a test can flip it AFTER the wrapper's already
    // spawned into the runner thread.
    fault: Arc<Mutex<FaultSchedule>>,
}

enum FaultSchedule {
    None,
    TruncateWritesAfter(u64),      // silently drop every byte past this cumulative offset
                                    // written via write_all — simulates a torn tail
    CorruptByteAt(u64, u8),        // flip one byte in an otherwise-complete write —
                                    // simulates mid-file corruption, distinct from a torn
                                    // tail, needed to exercise S2's "hard error" branch
                                    // specifically (a pure-truncation fault can't produce
                                    // "more valid data after the bad record")
}
```

Wraps `MemFile` for these (unit tests, no real disk needed) — `write_all`/`write` consult the
schedule before delegating; everything else (`read`, `seek`, `do_sync`, `do_lock`, `do_clone`,
...) delegates straight through unchanged. `do_clone` should clone `fault` by `Arc::clone`, not
reset it, so a test can take a "crash clone" (mirroring the existing `crash_clone` helper) that
still carries whatever fault was injected up to that point.

## 10. Blast radius (concrete, from the current tree)

- `store/src/logger.rs` — the whole redo/undo split: `MsgType`/`RedoOperation`/`UndoOperation` →
  `LogMsg`/`LogRecord`; `Operation` gains combined pre/post variants; `undo_log_runner`+
  `redo_log_runner` → one `log_runner`; `UndoId` type deleted; `Logger`'s `undo_txns` field →
  `records`+`by_txn`; `next_undo_id`/`find_undo_tuple` retired in favor of LSN-keyed lookups;
  `log_redo`+`log_undo` → one `log`.
- `store/src/db.rs` — `Db` struct's `undo_file`/`redo_file` → `log_file`; `create_core_db`,
  `open_using`/`open_using_with_limits`, `close`, `checkpoint`, `setup_needed_modules`
  (`Logger::set_db` call site); `load_logs`/`process_redo`/`process_undo` → one `process_log`;
  `insert`/`update`/`remove`/`rollback_by_id`'s logging call sites (one `log()` call each instead
  of paired `log_redo`+`log_undo`); new `StoreError::LogCorruption` variant.
- `store/src/tuple.rs` — `undo_id: Option<UndoId>` → `pre_lsn: Option<LsnId>` (plus
  `set_undo_id`/any other direct references).
- `store/src/tables/bplustree.rs` — `MAX_ENTRY_BYTES` comment label only, no numeric change.
- `store/Cargo.toml` — add `crc32fast` dependency.
- **Tests directly entangled with the two-file assumption** (confirmed via grep for
  `redo_file`/`undo_file`/`crash_clone`/`wait_for_durable_logs`/`count_log_records` across
  `store/src/db.rs`'s test module — 12 test functions, plus 2 in `store/src/logger.rs`'s own
  tests that call `set_db(MemFile::new(), MemFile::new())`):
  - `test_replay_redoes_committed_writes_on_reopen`
  - `test_replay_undoes_uncommitted_abandoned_writes_on_reopen`
  - `test_replay_handles_mixed_add_mod_del_across_committed_and_abandoned_txns`
  - `test_replay_is_idempotent_across_repeated_reopens`
  - `test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file`
  - `test_replay_handles_empty_file_backed_logs_without_panicking`
  - `test_replay_recovers_committed_writes_on_file_backed_db`
  - `test_replay_is_idempotent_across_repeated_reopens_file_backed`
  - `test_replay_seeds_lsn_watermark_from_prior_session`
  - `test_lsn_watermark_does_not_regress_after_new_writes_post_reopen`
  - `test_checkpoint_truncates_redo_and_undo_log_files` (name itself becomes wrong — "one log
    file" — needs renaming, not just updating)
  - `test_checkpoint_keeps_log_bounded_across_many_rounds`
  - Two `logger::tests` calls to `set_db(MemFile::new(), MemFile::new())` (now one `MemFile`
    argument).
  Every one of these needs its helper usage (`crash_clone`, `count_log_records`,
  `wait_for_durable_logs`, all defined in `store/src/db.rs`'s test module,
  `store/src/db.rs:1848-1891`) updated from "two files" to "one file" shape, not just a mechanical
  rename — several assert on redo/undo record counts independently, which no longer makes sense
  once there's one merged stream.

## 11. Explicitly out of scope for this design

- T1 (commit durability timing), T2 (page-LSN gating), T3/T5/T16 (checkpoint/header discipline),
  T11 (wall-clock timestamps), S1 (header versioning/checksum) — separate phases per
  `audit-progress.md`, untouched here even though some (T2 especially) will eventually want to
  read the same per-record LSN this design introduces.
- Any on-disk backward compatibility with pre-T4/S2 database files — confirmed not required.
- Performance work (P1, P8) — the plan already notes P1 (two fsyncs per commit) is incidentally
  fixed by §5's single runner/single fsync, but that's a side effect, not a benchmarked goal of
  this pass.

## 12. Suggested implementation order (for the eventual coding pass, not done here)

1. `FaultyFile` test wrapper (§9) — build first, so the rest of the work can be red/green tested
   against real torn-tail/corruption scenarios from day one, matching this repo's established
   test-first process (`audit-progress.md`'s own stated process for every finding so far).
2. Record framing + checksum (§2) in isolation — encode/decode/scan-rule unit tests, no `Logger`
   or `Db` wiring yet.
3. Unified `Operation`/`LogRecord`/`LogMsg` (§3-4) and the single `log_runner` (§5) — get one
   file/one thread writing framed records, with existing tests still pointed at two files
   temporarily disabled/ignored rather than half-migrated.
4. LSN-keyed `Logger` bookkeeping + retire `UndoId` (§6).
5. `process_log`'s three passes (§7) and `Db<F>`/file-layout changes (§8).
6. Migrate the 12+2 entangled tests (§10) to the new one-file shape; add S2's own new
   torn-tail/corruption tests and T4's own new atomicity tests using `FaultyFile`.
