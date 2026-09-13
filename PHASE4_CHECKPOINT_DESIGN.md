# STORE_AUDIT.md Phase 4: T3, T5, T16, S1, T17

Companion to `T4_S2_WAL_DESIGN.md` and `T2_T1_P10_DURABILITY_DESIGN.md`. Same process: design
first, test-first per finding, `audit-progress.md` updated as each lands, one commit per
finding.

**Decision confirmed with the user**: T3 uses the audit's simpler **quiesced checkpoint**
design (`checkpoint()` blocks new transactions and waits for in-flight ones to finish), not
the full ARIES-style fuzzy checkpoint. This substantially simplifies T5 and T16 too, since
quiescing means nothing can be mutating state (or logging new records) during the flush+
truncate window — see each section below for exactly what that removes.

## 1. T3 — checkpoint() can turn an uncommitted write into a committed one

### Current bug (confirmed in code)

`Db::checkpoint()` (`db.rs`) calls `buffer.checkpoint()` (`flush_dirty_cached_pages` — flushes
**every** dirty page, including one written by a transaction that's still active or merely
abandoned-but-not-yet-reverted) and then `logger.checkpoint(ts)` (truncates the WAL to
nothing). Nothing prevents a transaction from being mid-write when this runs, and `begin()`'s
own auto-checkpoint trigger (log > 16 MiB) means this isn't a rare, explicitly-invoked edge
case — it fires under ordinary sustained load.

### Fix

A `checkpoint_gate: RwLock<()>` on `Db`:
- `checkpoint()` takes the **write** side for its entire run.
- `begin_with_conflict_policy` takes the **read** side, but *only* around the final
  `tx_mgr.begin(policy)` call — not around its own auto-checkpoint trigger a few lines above,
  which would self-deadlock (a thread can't hold the read side and then have `checkpoint()`
  take the write side on the same call stack). The auto-checkpoint call happens first, fully
  outside the guard's scope; the guard is acquired fresh, immediately before registering the
  new transaction.

Holding the write side blocks all *new* `begin()` calls (they block on the read side), so the
set of in-flight transactions can only shrink from the moment `checkpoint()` acquires the
gate. `checkpoint()` then waits for that set to reach empty before touching pages or the log.

**Two sets matter, not one**: `TransactionManager::active_transactions` (currently open,
mid-work) AND `aborting_transactions` (abandoned via `Transaction::drop`, not yet physically
reverted). A transaction sitting in `aborting` is already correctly invisible
(`is_committed` = false), but if its page write survives a flush+truncate un-reverted, a fresh
session after a crash has *no record of it at all* (log truncated, in-memory sets reset on
reopen) — `is_committed` on reopen sees it in neither set and wrongly reports it committed.
Exactly T3's bug again, reached through the abandoned-transaction path instead of the
still-open one. So the wait condition is `active_transactions.is_empty() &&
aborting_transactions.is_empty()`.

**The wait loop must itself drain `aborting`**: `drain_aborting()` (the only thing that moves
transactions out of `aborting`) normally runs from `begin()` — which is now blocked by the
gate `checkpoint()` itself holds. Without checkpoint's wait loop calling `drain_aborting()`
itself, an abandoned transaction could never be reclaimed while a checkpoint is waiting,
livelocking. So the loop is: `drain_aborting(); check both sets; sleep briefly; repeat`.

No timeout: an indefinite wait is the accepted tradeoff of this design (matches the audit's
own framing — "long readers block checkpoints; combine with T7's reader-pinning limits", and
T7 stays out of scope, same as Phase 1-3).

### Test

`test_audit_t3_checkpoint_never_commits_an_uncommitted_write`: the audit's own reproduction —
insert a row under a transaction that's abandoned (`mem::forget`, never committed or rolled
back), checkpoint, "crash" (reopen from a `crash_clone` without a clean close), assert the row
is absent. Plus a concurrency-shaped variant: spawn a thread that begins a transaction and
holds it briefly (simulating in-flight work) while the main thread calls `checkpoint()` on
another thread, asserting `checkpoint()` doesn't return until the transaction finishes and
that its write survives correctly (rather than testing the buggy case) — proves the gate
actually blocks/waits rather than just happening to pass the abandoned-transaction case.

## 2. T5 — checkpoint sequence isn't crash-ordered

### What quiescing already removes

The audit's problem #1 ("other threads keep logging redo records ahead of the Checkpoint
message, dirtying pages not in the flush") is structurally eliminated by T3's fix: logging a
new record requires an active transaction, and no new transaction can become active while
`checkpoint()` holds the gate. Nothing can log or dirty a page during the flush+truncate
window.

### What still needs fixing

Problems #2 and #3 are independent of concurrency — they're about **ordering and durability
between two separate async channels** (the page buffer's writer thread and the log runner
thread), not about a concurrent mutator:
- The header write (`PageBuffer::write_header`, a fire-and-forget `send()`) is not waited on
  or synced before `Logger::checkpoint` truncates the log. A crash in that window can leave
  the header stale relative to an empty log.
- The header write itself is never `do_sync()`'d at all.

**Fix**: a new `PageBuffer::write_header_synced(header) -> Result<(), StoreError>`, mirroring
the existing `checkpoint()` method's own reply-channel pattern (`bounded(1)` reply sender) —
send the header write with a reply channel, have the writer thread `pwrite_all` it and then
`do_sync()` the file before replying, and have the caller block on that reply. `Db::checkpoint`
calls this instead of the current `write_header`, and only calls `logger.checkpoint(ts)`
(the truncate) after it returns `Ok`. `Db::close` can keep using the existing fire-and-forget
`write_header` — its own `buffer.shutdown()` call right after already flushes and syncs the
whole file, which covers the header too (same-channel FIFO ordering already guarantees the
header write is processed before the shutdown message that syncs).

**Page count derivation — tried, reverted, do not retry without re-reading this note.** The
audit's recommendation (derive `page_count` from file length on open instead of trusting the
header field) was implemented and immediately caught its own failure, panicking with "Unknown
page PageId(3)" — worse than the bug it was meant to fix.

**Correction, found during a later re-investigation**: the FIRST version of this note blamed
`write_locked_page`'s general deferral ("the file's length reflects whichever pages happened to
be evicted so far — sparse and out of allocation order"). That's imprecise — a *new* page's own
initial write is always synchronous and eager (`alloc_page` → `init_page` → `write_page`, a
completely different, direct-to-`self_file` path from `write_locked_page`), so file length DOES
reliably track how many page slots have ever been carved out, contiguously, in allocation
order. The real, narrower mechanism: some call sites — concretely `BPlusTree::new`, setting up
a table's first index page — allocate a page synchronously as a generic, unflagged placeholder
(`Page::new_data`, `alloc_page(false)`) and then *separately* build its real, intended content
(`Page::new_indexed` with `LEAF_NODE` set) and write *that* via the deferred, cache-only
`write_locked_page`. A crash-clone taken before that second write ever got flushed (no
eviction/checkpoint/shutdown yet) leaves the page's on-disk bytes as the generic placeholder —
neither `LEAF_NODE` nor `INNER_NODE` flagged. The panic actually came from
`BPlusTree::insert_recursive`'s `panic!("Unknown page {:?}", start)` (a content/flag mismatch on
an in-bounds page), not from `PageBuffer::get_page`'s bounds check (worded "Invalid page
number") — confirming the page genuinely existed (file length was right about that); its
*content* just hadn't caught up. A file-length-derived `page_count` makes replay treat "this
slot exists" as license to read and use it; the header-derived count avoids the failure only as
a side effect, by keeping such pages out of the visible set until a checkpoint has actually
flushed everything.

Reverted; `page_count` stays sourced from the header — same decision as before, just for this
more precise reason, not a general claim that file length is unreliable. Not a live gap either
way: the specific race T5 actually describes (a stale header paired with an already-truncated
log) is fully closed by the header-sync fix above on its own: by the time a header is ever
paired with an empty log, `write_header_synced` already guarantees it reflects everything
`buffer.checkpoint()` just flushed.

### Test

`test_audit_t5_checkpoint_header_write_is_durable_before_log_truncation`: fake/instrumented
check — after `checkpoint()` returns, assert the header bytes on disk reflect the new
`page_count`/`last_checkpoint` (not just "eventually" — synchronously, no polling, mirroring
T1's own `count_log_records`-no-polling test pattern). Plus
`test_audit_t5_page_count_is_derived_from_file_length_not_a_stale_header`: write more pages
than the header's `page_count` field claims (simulating a stale/rolled-back header value),
reopen, assert the real page count is recognized (via file length) rather than the stale
smaller value.

## 3. T16 — free list and page count only persisted at checkpoint

### What quiescing changes here

The audit's exact reproduction (a transaction takes a free page, writes, commits, its redo
syncs, crash, reopen restores the *stale* on-disk free list which still contains that page,
next allocation hands it out again, clobbering committed data) is about the free list's
*persisted* snapshot going stale relative to *committed* allocations made since the last
checkpoint — this is independent of T3's concurrency concern (the allocation there is fully
committed, not in-flight) and needs its own fix regardless of the checkpoint concurrency
model chosen.

### Fix (the audit's "reconcile on open" option — simpler than logging every alloc/free)

On `Db::open_using` (after loading the persisted free list and tables), walk every table's
index pages and data chains, collect every *reachable* page id, and remove any of those from
the loaded free list before it's used for allocation. A page that's reachable (part of a
live table's structure) is by definition not actually free, regardless of what the
stale-checkpoint snapshot says.

This is a startup-time cost (proportional to total page count), acceptable for a correctness
fix in the same spirit as the audit's own "cheap insurance regardless" framing. Does not
address the *pending-free-list* half of the audit's recommendation (deferring reuse of
pages freed since the last checkpoint until the next one persists) — the reconciliation walk
alone is sufficient to fix the *reproduction* (a reachable page is never handed out), and
adding a second bookkeeping structure (pending-free) for pages that are freed-but-unreachable-
either-way is not needed to close the actual bug. Noted as a scope decision, not an oversight.

### Test

`test_audit_t16_reopen_reconciles_the_free_list_against_reachable_pages`: reproduce the
audit's exact sequence (allocate + write + commit, force the free list to disk in a stale
state that still lists the now-live page as free — e.g. via a crash-clone snapshot taken at
the right moment, or by directly manipulating the persisted free-list bytes in a `MemFile`
test), reopen, insert enough rows to force an allocation, assert the reused page is *not* the
one still live with committed data (or more directly: assert the stale entry is gone from
`buffer.get_free_pages()` right after open, before any allocation happens).

## 4. S1 — no format version, no header checksum, no header validation

### Scope decision

Implementing the audit's *full* recommendation (format_version + header_checksum + validated
page_size/first_page_offset + double-buffered alternating header slots with a generation
number) — the double-slot part is a real format change on par with T4+S2. Given T5's fix
above already closes the specific "torn header write" risk that double-buffering primarily
exists to survive (the header write is now sync'd and its completion is waited on before
anything truncates the log), the double-slot mechanism becomes a "belt and suspenders"
improvement rather than the thing actually closing a live bug. **Scope for this pass**:
`format_version: u32` + `header_checksum: u32` (over the rest of the header, same `fnv1a_32`
already reused for page/WAL checksums) + validation of `page_size` (power of two, within
`[4 KiB, 1 MiB]`) and `first_page_offset` (`>=` header's own encoded size) on open, rejecting
with a typed error otherwise. Double-buffered slots deferred, not forgotten — noted as a
follow-up in `audit-progress.md`, not silently dropped.

### Test

`test_audit_s1_open_rejects_a_corrupted_header_checksum`, `test_audit_s1_open_rejects_an_invalid_page_size`,
`test_audit_s1_open_rejects_a_first_page_offset_smaller_than_the_header_itself` — each
constructs a byte-level-tampered header (via `FileDB`/`NamedMemFile`, whichever makes
byte-level tampering easiest) and asserts `Db::open`/`open_using` returns a clear,
typed `Err` instead of decoding garbage or panicking downstream (e.g. a huge `page_size`
attempting a giant allocation).

## 5. T17 — drop_table frees pages in-flight operations may still hold

### Fix

The audit's own text calls this "fine for the current squeal-sql usage" and suggests either a
table-level `RwLock` or routing freed pages through T16's pending-free mechanism, *plus*
logging `drop_table` so replay doesn't hit `TableNotFound` for a dropped table's id.

Given T16 above doesn't build a pending-free list (scope decision), the RwLock approach is the
one in scope here: a `table_locks: RwLock<HashMap<TableIdType, Arc<RwLock<()>>>>`-style
per-table lock (or simpler: reuse `checkpoint_gate`'s *shape* but per-table) where ordinary
operations (`insert`/`update`/`remove`/`find`) take the read side for their duration and
`drop_table` takes the write side before freeng any pages. Given the size this could add
across every read/write call site, confirm the actual minimal blast radius by reading
`Db::insert`/`update`/`remove`/`find`'s current structure before committing to exactly where
the guard is acquired — this needs its own investigation pass once T3/T5/T16/S1 are done,
not designed blind here.

The redo-replay logging half (so a crash between `drop_table` and its next checkpoint doesn't
leave replay trying to redo an op against a table id that no longer exists) is more
self-contained: log an `Operation`-level marker (or reuse the existing log plumbing) for
`drop_table`, and have `process_log`'s redo pass skip (not error on) a record whose table id
is unknown — the table being gone is, by definition, the *correct* outcome to converge to
either way.

## Sequencing for this pass

T3 first (biggest, and T5/T16 are each simpler once quiescing is in place). Then T5. Then
T16. Then S1. Then T17 last (needs its own investigation once the dust settles on the other
four, per its own section above).

Each gets its own commit once its test is green and the full suite shows 0 regressions,
exactly like every prior finding this session.
