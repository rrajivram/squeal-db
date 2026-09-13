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

## 2. Log file header (mismatch detection)

Small addition on top of the original sketch: the log file's first bytes are a fixed-size
`LogHeader`, written once when the log file is created and never rewritten with different
content afterward (only re-written verbatim after a checkpoint truncate — see below). This is
what lets `Db::open` refuse cleanly if the WAL sitting next to a database file doesn't actually
belong to it — e.g. someone restores a log file from a different backup generation, a different
database entirely, or a build with an incompatible WAL format — instead of either failing deep
inside recovery with a confusing decode error, or (worse) silently replaying operations that
assume a different page layout.

```rust
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct LogHeader {
    magic: [u8; 4],                              // LOG_MAGIC, distinct from the main file's own 2-byte MAGIC
    #[serde(with = "postcard::fixint::le")]
    version: u16,                                  // WAL format version — bump on any incompatible framing/Operation change
    #[serde(with = "postcard::fixint::le")]
    page_size: DBSizeType,                        // must equal the paired main file's Header.page_size
}

const LOG_MAGIC: [u8; 4] = [0x53, 0x71, 0x57, 0x4c]; // "SqWL" — deliberately a different length AND
                                                       // value from the main file's `MAGIC`, so the two
                                                       // file kinds can never be mistaken for each other
const CURRENT_LOG_VERSION: u16 = 1;
```

Every numeric field uses `postcard::fixint::le`, same as `Header` (`store/src/db.rs:134-144`)
already does — so, exactly like `Header`, `LogHeader`'s postcard-encoded size equals
`size_of::<LogHeader>()`, and reading it is the same `vec![0u8; size_of::<LogHeader>()]` +
`read_exact` + `from_bytes` pattern `open_using` already uses for the main file's header
(`store/src/db.rs:252-259`).

- **Written**: `create_core_db` (`store/src/db.rs:1553+`) writes `LogHeader{ magic: LOG_MAGIC,
  version: CURRENT_LOG_VERSION, page_size }` to the freshly `create_new`'d log file immediately
  after opening it — mirroring exactly how it already writes the main file's `Header` via
  `f.write_all(&bytes)?` right after that file's own `create_new`, before `setup_needed_modules`
  spawns anything. `page_size` here is the same value already passed into `create_core_db` for
  the main `Header`, so there's no new input to thread through, just one more serialize+write.
- **Validated**: `open_using`/`open_using_with_limits` (`store/src/db.rs:234-299`), right after
  the existing main-file magic check (`if header.magic != MAGIC { return Err(...) }`,
  `store/src/db.rs:257-259`) and before any `do_lock()` call — read the log file's `LogHeader`
  the same way, then check, in order: `magic == LOG_MAGIC`, `version == CURRENT_LOG_VERSION`
  (exact match, not `<=` — a version this build doesn't recognize is exactly the "don't guess"
  case S1's own future header-versioning work will want to reuse this reasoning for), then
  `page_size == header.page_size` (the main file's, just read). Any mismatch returns a new
  `StoreError::LogHeaderMismatch` variant whose message names which field disagreed and both
  values — checking all three unconditionally before failing (not stopping at the first
  mismatch) makes for a more useful error message if more than one is wrong at once. Failing
  here, before any lock is taken, means a wrongly-paired log file is rejected with zero side
  effects — nothing gets locked, nothing gets recovered against it.
- **Recovery scanning** (§8) begins at byte offset `size_of::<LogHeader>()`, not byte 0 — the
  header itself is never mistaken for a framed record.
- **Checkpoint's truncate must not erase the header.** Today, `Checkpoint` truncates a log file
  to zero bytes unconditionally (`Opener::truncate` — `MemFile`/`File`/`NamedMemFile` all
  implement it as "set length to 0", `store/src/memfile.rs:73-76,102-105`). Under this design
  that would also delete the `LogHeader`, so the NEXT append (after this checkpoint) would land
  at byte 0 with no header in front of it, and the NEXT `Db::open` would read zeroed/absent
  header bytes and reject the file. Fix: `log_runner` (§6) is given a copy of the exact
  `LogHeader` bytes it should own (computed once, either freshly written by `create_core_db` or
  re-derived from the just-validated on-disk header in `open_using` — byte-identical either
  way), and on `Checkpoint`, immediately after `file.truncate()?`, does
  `file.write_all(&header_bytes)?` before returning to the batching loop. `Db::close`
  (`store/src/db.rs:339-392`) goes through the identical `logger.checkpoint(ts)` call
  (`store/src/db.rs:389`), so this is the ONE place that needs the fix, not two.

## 3. On-disk record framing

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
- The very first framed record in the file starts right after `LogHeader` (§2), not at byte 0.

### Recovery scan rule (this is S2's actual fix)

Scanning forward from the start of the RECORD region (i.e. from `size_of::<LogHeader>()`, after
§2's header has already been read and validated separately), at each position:

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

## 4. Unified `Operation` (combined pre/post images)

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

## 5. `LogRecord` / `LogMsg` (replaces `RedoOperation`/`UndoOperation`/`MsgType`)

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

## 6. One runner thread, one channel

`Logger::set_db` (`store/src/logger.rs:160`) takes ONE `file: impl DBFile` (plus the
`log_header_bytes: Vec<u8>` from §2 — needed for the post-checkpoint-truncate rewrite) instead of
`(undo_file, redo_file)`, creates ONE `bounded(LOG_CHANNEL_CAPACITY)` channel, and spawns ONE
`log_runner(file, rx, clock, log_header_bytes)` thread — replacing `undo_log_runner`+
`redo_log_runner`. `log_runner` is `redo_log_runner`'s existing batching/linger loop
(`store/src/logger.rs:538-596`) almost unchanged: same `MAX_LOG_BATCH`/`LOG_BATCH_LINGER`
batching, same "write the batch, one fsync, then `mark_written(highest_lsn)`" structure — just
framing each `LogRecord` into the batch buffer (`len`+`crc32`+bytes) instead of `to_allocvec`-ing
it bare, matching on `LogMsg` instead of `MsgType`, and — the one new step — on `Checkpoint`,
writing `log_header_bytes` back immediately after `file.truncate()?` (§2).

This incidentally fixes the other half of T4 (independently-synced files ⇒ commit not atomic):
one file, one thread, one `fsync` per batch — a batch either lands completely or the crash caught
it mid-`write_all`, handled by the torn-tail rule in §3. There is no longer a "redo landed, undo
didn't" state to reach, because there's only one artifact.

`Logger::shutdown`/`checkpoint` each send exactly one `ShutDown`/`Checkpoint(ts)` instead of two.

## 7. Undo pointer becomes the record's own LSN — retiring `UndoId`

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

## 8. Recovery: three passes, one file

Replaces `Db::load_logs`/`process_redo`/`process_undo` (`store/src/db.rs:424-562`) with one
`process_log(buffer)`, reading the ONE log file (mmap'd for `File`, `.data()` for `MemFile`,
exactly as today's dual read does) and running:

0. **Skip the header** — `process_log` is handed `&buffer[size_of::<LogHeader>()..]`; the header
   itself was already read and validated separately, before locking, per §2 (so a mismatched
   header never even reaches this function).

1. **Analysis** — single forward scan (applying §3's torn-tail/corruption rule as it goes),
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
   (§7's decision) via the existing `revert_undo_ops` (`store/src/db.rs:829-844`), unchanged in
   shape — it already takes `&Vec<Operation>` and a `&TransactionId`, just now sourced from one
   unified `by_txn` map instead of two separately-decoded files agreeing (or not) on which
   records existed.

`max_lsn` seeds `LsnClock` exactly as today's `process_redo` tail does
(`logger.clock().mark_written(lsn)` + `advance_counter_past(lsn)`) — but now unconditionally
(today this only happens inside `process_redo`, so an empty redo file with a non-empty undo file
would silently skip clock seeding; moot with one file, since there's only one scan to seed from).

## 9. `Db<F>` / file-layout changes

- `Db<F>` (`store/src/db.rs:151-177`): `undo_file`/`redo_file` fields collapse to one
  `log_file: F`.
- `create_core_db` (`store/src/db.rs:1553+`): opens ONE file (suggest `name + ".wal"`) instead of
  `.undo`/`.redo` — one `create_new`/`do_lock`/`do_clone` instead of two, same S4/S5 semantics
  (exclusive create, exclusive lock) just applied once, PLUS writes the `LogHeader` (§2)
  synchronously right after creating it, same as the main `Header`.
- `open_using`/`open_using_with_limits` (`store/src/db.rs:234-299`): take one `log_file: F`
  parameter instead of two; read+validate its `LogHeader` (§2) right after the main file's own
  magic check and before any `do_lock()` call, returning the new `StoreError::LogHeaderMismatch`
  on disagreement; then one `do_lock`/`do_clone` instead of two.
- `close` (`store/src/db.rs:339-392`): returns `(F, F)` (main file + log file) instead of
  `(F, F, F)`.
- `checkpoint`/`Logger::checkpoint`: unchanged in spirit (§6) — one truncate instead of two (now
  immediately followed by the header rewrite, §2), removing the (latent, never-observed-failing-
  but-real) risk of the two runner threads truncating their respective files at very slightly
  different moments under load.
- New `StoreError` variants: `LogHeaderMismatch` (§2) and `LogCorruption` (§3).

## 10. Test infrastructure needed (Phase 0 item, build before writing T4/S2's own tests)

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

Also usable directly (no new fault variant needed) for §2's header-mismatch tests: construct a
log file whose first `size_of::<LogHeader>()` bytes were written by hand with a different
`page_size`/`version`/`magic` than the paired main file expects, then assert `Db::open_using`
returns `StoreError::LogHeaderMismatch` rather than proceeding into recovery.

## 11. Blast radius (concrete, from the current tree)

- `store/src/logger.rs` — the whole redo/undo split: `MsgType`/`RedoOperation`/`UndoOperation` →
  `LogMsg`/`LogRecord`; `Operation` gains combined pre/post variants; `undo_log_runner`+
  `redo_log_runner` → one `log_runner` (now also carrying `log_header_bytes` for the post-
  truncate rewrite); `UndoId` type deleted; `Logger`'s `undo_txns` field → `records`+`by_txn`;
  `next_undo_id`/`find_undo_tuple` retired in favor of LSN-keyed lookups; `log_redo`+`log_undo`
  → one `log`; new `LogHeader`/`LOG_MAGIC`/`CURRENT_LOG_VERSION`.
- `store/src/db.rs` — `Db` struct's `undo_file`/`redo_file` → `log_file`; `create_core_db`
  (writes `LogHeader`), `open_using`/`open_using_with_limits` (validates `LogHeader`), `close`,
  `checkpoint`, `setup_needed_modules` (`Logger::set_db` call site); `load_logs`/`process_redo`/
  `process_undo` → one `process_log` (skips the header region first); `insert`/`update`/
  `remove`/`rollback_by_id`'s logging call sites (one `log()` call each instead of paired
  `log_redo`+`log_undo`); new `StoreError::LogHeaderMismatch`/`LogCorruption` variants.
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
    file" — needs renaming, not just updating; also needs a new assertion that the header
    survives the truncate)
  - `test_checkpoint_keeps_log_bounded_across_many_rounds`
  - Two `logger::tests` calls to `set_db(MemFile::new(), MemFile::new())` (now one `MemFile`
    argument plus header bytes).
  Every one of these needs its helper usage (`crash_clone`, `count_log_records`,
  `wait_for_durable_logs`, all defined in `store/src/db.rs`'s test module,
  `store/src/db.rs:1848-1891`) updated from "two files" to "one file" shape, not just a mechanical
  rename — several assert on redo/undo record counts independently, which no longer makes sense
  once there's one merged stream, and `count_log_records` needs to skip the header before
  counting framed records.
  - New tests (not migrations of existing ones): `LogHeader` mismatch on `page_size`/`version`/
    `magic` individually (§2/§10), header survives a checkpoint truncate, torn-tail-vs-corruption
    scan rule (§3/§10) for each of the three cases in that rule.

## 12. Explicitly out of scope for this design

- T1 (commit durability timing), T2 (page-LSN gating), T3/T5/T16 (checkpoint/header discipline),
  T11 (wall-clock timestamps), S1 (header versioning/checksum) — separate phases per
  `audit-progress.md`, untouched here even though some (T2 especially) will eventually want to
  read the same per-record LSN this design introduces, and S1's own eventual main-file header
  versioning can likely reuse §2's exact validate-before-lock pattern.
- Any on-disk backward compatibility with pre-T4/S2 database files — confirmed not required.
- Performance work (P1, P8) — the plan already notes P1 (two fsyncs per commit) is incidentally
  fixed by §6's single runner/single fsync, but that's a side effect, not a benchmarked goal of
  this pass.

## 13. Suggested implementation order (for the eventual coding pass, not done here)

1. `FaultyFile` test wrapper (§10) — build first, so the rest of the work can be red/green tested
   against real torn-tail/corruption/mismatched-header scenarios from day one, matching this
   repo's established test-first process (`audit-progress.md`'s own stated process for every
   finding so far).
2. `LogHeader` (§2) in isolation — encode/decode/validate unit tests (magic/version/page_size
   mismatch each rejected with the right error), independent of everything else.
3. Record framing + checksum (§3) in isolation — encode/decode/scan-rule unit tests, no `Logger`
   or `Db` wiring yet.
4. Unified `Operation`/`LogRecord`/`LogMsg` (§4-5) and the single `log_runner` (§6, including the
   post-checkpoint-truncate header rewrite) — get one file/one thread writing header-then-framed-
   records, with existing tests still pointed at two files temporarily disabled/ignored rather
   than half-migrated.
5. LSN-keyed `Logger` bookkeeping + retire `UndoId` (§7).
6. `process_log`'s three passes (§8, header-skip included) and `Db<F>`/file-layout changes (§9,
   including `open_using`'s header validation).
7. Migrate the 12+2 entangled tests (§11) to the new one-file shape; add S2's own new
   torn-tail/corruption tests, T4's own new atomicity tests, and §2's header-mismatch tests, all
   using `FaultyFile`.

## 14. Implementation notes — where reality diverged from this design

**Status: implemented.** Everything below is what actually shipped, kept alongside the original
plan above (rather than silently editing it) so the record of what was proposed vs. what building
it surfaced stays intact — matching this repo's own `audit-progress.md` convention.

- **Checksum: `page::fnv1a_32`, not `crc32fast`.** `page.rs` already has a hand-rolled FNV-1a-32
  hash used for exactly this job (physical page checksums — "cheap and good enough to catch
  accidental corruption... nothing cryptographic needed", per its own comment). Reusing it for
  WAL record framing needed zero new dependencies and keeps one checksum algorithm in the codebase
  for the same purpose instead of two. `store/Cargo.toml` was NOT changed.
- **`LogHeader::encoded_len()` is NOT `size_of::<LogHeader>()`.** This was a real bug, caught by a
  failing integration test before it shipped: Rust pads a struct's in-memory layout for its
  widest field's alignment (`page_size: u64` needs 8-byte alignment, padding the 14 meaningful
  bytes up to 16), while postcard's actual encoding just concatenates each field with no padding
  at all. `db.rs`'s own `Header` makes the identical `size_of` assumption and gets away with it
  ONLY because the main file always has real page data immediately after its header — an
  over-sized `read_exact` harmlessly absorbs a few bytes of that (postcard's `from_bytes` ignores
  unconsumed trailing bytes) instead of hitting EOF. The log file has no such guarantee: a fresh
  WAL has nothing at all after its header until the first record lands, so the same over-read hit
  a real `UnexpectedEof`. Fixed by computing the encoded length directly (`to_allocvec` on a throwaway
  instance) instead of trusting `size_of` to match it — `Header`'s matching risk is now flagged
  here for whenever S1's own header-versioning work touches it, not fixed as part of this pass.
- **`log_runner`'s checkpoint-truncate rewrite needs an explicit `seek(Start(0))` first.** Also
  caught by a failing test (`test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file`,
  pre-existing, not new): `Opener::truncate` resets a file's LENGTH but not its seek cursor.
  Without an explicit seek back to the start, `write_all(&log_header_bytes)` right after
  `truncate()` resumed writing at the OLD end-of-file position (now past the truncated length),
  padding the gap with zero bytes instead of landing the header at the actual start of the file —
  every subsequent read of "the header" was actually reading zeros. Confirmed via a targeted
  `eprintln!` before diagnosing it properly; fixed with one `file.seek(SeekFrom::Start(0))?`
  between `truncate()` and the header `write_all`.
- **`Operation::Mod`'s `pre` is `Option<Record>`, not a plain `Record`.** The own-insert-then-
  update case (a transaction revising a row it inserted itself, `Db::update`'s `build` closure)
  needs a record logged for REDO (so replay reconstructs the final value) but must NOT contribute
  a real pre-image to UNDO replay — traced why concretely: `revert_undo_ops` replays a
  transaction's own ops in forward order (§7's decision), so if this Mod carried a real
  pre-image, undoing it would run AFTER the original Add's own revert already removed the row,
  re-materializing something that revert had just deleted. `pre: None` marks exactly this
  "redo-only" case; `revert_undo_ops`/`process_log`'s undo pass both skip it outright, matching
  the pre-unification code's behavior of never logging a second undo record for this case at all.
- **`Del`'s redo replay never needed a derived tombstoned tuple.** §7 originally sketched redo
  deriving `pre.tuple.clone()` + `.tombstone()` to reconstruct the post-image. Turned out
  unnecessary once actually tracing the old `process_redo`'s own Del arm: it only ever called
  `table.remove(r.tuple.id)` — using just the ID, discarding the rest of the record entirely. The
  new `process_log`'s redo pass does the identical `table.remove(pre.tuple.id.clone())`, no
  derivation step needed.
- **`LsnId` is `pub`, not `pub(crate)`, even though it's otherwise an internal WAL/clock concept.**
  Needed once `Tuple::pre_lsn`'s type (appearing in the public `Tuple::new_with`/`set_pre_lsn`
  signatures) had to be nameable from `squeal-sql`, an external crate — a `pub(crate)` type in a
  public function signature compiles as only a lint warning within the SAME crate but is a hard
  error across a crate boundary. Exactly mirrors the retired `UndoId`'s own visibility split
  (`pub struct UndoId(pub(crate) u16)`) for the identical reason: nameable everywhere, but only
  ever constructible with a real value from inside `store` (external code can still write `None`
  freely).
- **No `FaultyFile` wrapper was built.** §10's generic fault-injecting `DBFile` wrapper was
  designed but not implemented — every scenario it was meant to enable (torn tail, mid-file
  corruption, header mismatch) turned out to need only ONE specific, known byte-level change
  applied once to a `MemFile` snapshot, which is exactly what `crash_clone` + direct buffer
  manipulation already does throughout this test suite (e.g.
  `test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file`, pre-existing).
  Added instead: `logger.rs`'s own unit tests exercise `scan_log`/`read_and_validate_log_header`
  directly against hand-built byte buffers (no `Db`/`Logger` wiring at all —
  `test_scan_log_torn_tail_at_payload_is_dropped_not_errored`,
  `test_scan_log_checksum_mismatch_with_a_valid_record_after_is_real_corruption`, etc.), and
  `db.rs` gained three end-to-end integration tests exercising the identical scenarios through
  the real `Db::open_using` path (`test_open_using_refuses_a_log_file_from_a_different_page_size_database`,
  `test_open_using_tolerates_a_torn_tail_and_recovers_everything_before_it`,
  `test_open_using_refuses_a_log_file_with_mid_file_corruption`). A generic wrapper remains
  available as future infrastructure (T1/T2/T5/T16 per `audit-progress.md`'s Phase 0 note) if a
  later fix genuinely needs to inject a fault mid-flight across MULTIPLE writes rather than at one
  known point in an otherwise-complete snapshot — not needed for T4/S2 itself.
- **Log file naming: `<name>.wal`, replacing BOTH `.undo` and `.redo`.** Also updated
  `store/src/named_memfile.rs`'s `NamedMemFile::delete` (its sibling-cleanup logic hardcoded the
  old `.undo`/`.redo` suffixes) — missed on the first pass, caught by grepping the whole
  workspace for `undo_file`/`redo_file`/`.undo`/`.redo` after the main implementation compiled,
  not by a failing test (there wasn't one exercising `NamedMemFile::delete`'s sibling cleanup
  specifically pre-existing, so this was a static-scan catch, not a red/green one — flagged here
  rather than presented as more rigorously verified than it was).
- **Blast radius was larger than estimated.** §11 originally counted "12+2" entangled tests.
  Actual count of `open_using`/`close`/`crash_clone` call sites needing the 3-tuple→2-tuple
  mechanical change: 32 `open_using(...)` calls and 20 `.close()` calls across `db.rs`'s test
  module (not all distinct tests — several tests call more than one). Handled as a scripted,
  whole-file substring replacement (`"f, u, r)"` → `"f, l)"`, etc. — verified safe first by
  grepping that literally every occurrence of that substring in the file was this exact
  destructure/call pattern and nothing else) rather than by hand, then let the compiler's own
  type errors catch anything the script missed (which is exactly what a 3-tuple→2-tuple signature
  change is good at catching — every leftover site is a hard type error, not a silent bug).
- **Verification**: full `store --lib` suite green — **396 passed, 0 failed**
  (`cargo test -p store --lib -- --test-threads=1`; 381 baseline-after-T10 + 15 new tests from
  this pass: 3 `db.rs` integration tests + 12 `logger.rs` unit tests, confirmed by exact count
  across two independent runs). `squeal-sql --lib` unaffected (346 passed, 0 failed) aside from
  one signature ripple (`Database::close`'s own return type, `store/schema_ops/database.rs`) and
  one stale comment (`squeal-sql/src/table.rs`). Whole workspace (`cargo build --workspace
  --tests`) builds clean.
