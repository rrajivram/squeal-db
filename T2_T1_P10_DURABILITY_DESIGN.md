# STORE_AUDIT.md Phase 3: T2 (page-LSN gating), T1 (durable commit), P10 (adaptive linger)

Companion to `T4_S2_WAL_DESIGN.md`. Same process: design first, test-first per finding,
`audit-progress.md` updated as each lands.

## 1. T2 — pages are flushed gated on the wrong LSN

### Current bug (confirmed in code, not just from the audit)

`page.rs::set_dirty(true)` stamps `page.lsn` from `clock.last_written()` — the current
**flush watermark** ("whatever's already durable") — not from the LSN of the mutation that's
dirtying it right now. The mutation's own redo LSN doesn't exist yet at that point: e.g.
`Db::insert` (`db.rs:911-928`) calls `table.insert(...)` (which mutates + dirties the page)
*before* `self.logger.log_new(op)` (which mints the LSN and logs the record). Same shape in
`update`/`remove`/`update_checked`/`relocate_tuple`.

The writer thread's flush gate (`buffer.rs:1109`, `:1236`) is `page.lsn_id() < clock.last_written()`
— "flush once anything *newer* than this page's stamped LSN is durable." Since the stamped
LSN is the watermark *at dirty time*, not the operation's own LSN, the page can satisfy this
gate and get flushed as soon as *any later, unrelated* record becomes durable — before its
own change's redo record has even been logged, let alone synced. Confirmed exploitable with a
single writer thread (no concurrency needed): watermark advances from an unrelated concurrent
commit while this operation's own record is still sitting in the channel.

### Fix

1. **Mint the LSN before mutating**, not after logging. `Db::insert`/`update`/`remove` call
   `let lsn = self.logger.next_lsn();` first, thread it down into the table-level call, and
   after the physical write succeeds, log with `self.logger.log(lsn, op)` (the existing
   two-step mint/log API — already used for `pre_lsn` stamping, see `logger.rs:489-499`).
2. **Stamp pages with that LSN, not the watermark.** New `Page::stamp_lsn_at_least(lsn)`:
   `*self.lsn.write()? = max(current, lsn)` — monotonic, so if a page is touched twice in one
   critical section (e.g. a split writes to both a page and its new sibling) it always ends up
   carrying the *highest* LSN of anything it currently holds. `set_dirty` itself is untouched
   (still self-stamps from the watermark) — it's still correct for non-logged internal
   mutations, and changing its signature would ripple everywhere for no benefit. Instead, a
   new `PageBuffer::write_locked_page_with_lsn(handle, lsn)` calls
   `handle.page.stamp_lsn_at_least(lsn)?` then delegates to the existing
   `write_locked_page(handle)` — called at exactly the write sites that have an
   operation-level LSN to give it. This overrides `set_dirty`'s watermark stamp with the
   correct value before the page is ever published to the cache (still under the page's
   exclusive lock, so no reader/writer can observe the wrong value in between).
3. **Thread `lsn: LsnId` through every `BPlusTree` function that can reach a
   `write_locked_page` call**, alongside the `txn: TransactionId` parameter these functions
   already carry (a direct, mechanical extension of an existing pattern, not a new one):
   `insert`, `insert_recursive`, `insert_index`, `update`, `update_checked`, `remove`,
   `relocate_tuple`, `update_index_entry`, `remove_index_entry`, `write_data`/`write_page`
   (the bplustree-internal one), and split-handling helpers. Swap their internal
   `write_locked_page(...)` calls for `write_locked_page_with_lsn(..., lsn)`.
4. **Writer's gate becomes `<=`, not `<`.** `page.lsn_id()? <= clock.last_written()` — once
   `page.lsn` correctly equals the exact LSN it needs durable (not something guaranteed
   strictly less), a page whose LSN exactly equals the current watermark is safe to flush
   *now*, not stuck waiting for something even newer.
5. **Delete the `u64::MAX` cold-start sentinel special case** in `set_dirty` (page.rs:447-456)
   — it existed only because `set_dirty` needed to defensively avoid stamping a page with
   "nothing is durable yet" and having it wait forever. Once mutation call sites stamp their
   own real LSN via `stamp_lsn_at_least` right after `set_dirty` runs, `set_dirty`'s own
   watermark stamp is always immediately overwritten for anything that matters, making the
   cold-start special case dead weight. Confirm via a fresh-page-write test that nothing
   regresses before deleting.

### Blast radius

Every `write_locked_page(...)` call site in `bplustree.rs` (18, per a `grep`) whose write is
part of a `Db`-level insert/update/remove needs to become `write_locked_page_with_lsn`, and
every function on the call path down to it needs the `lsn` parameter threaded through. A few
of those 18 are NOT part of a logged operation (e.g. table-creation-time index page
formatting in `BPlusTree::new`) — those stay on plain `write_locked_page`, since nothing logs
a redo record for them and nothing needs to gate their flush on anything.

### Test (write first, confirm failing, then fix)

Matches the audit's own suggested repro almost exactly, adapted to this codebase's existing
fault-injection idiom (a `DBFile` wrapper whose `do_sync` can be held open — check if one
already exists from the T4/S2 work before building a new one; `T4_S2_WAL_DESIGN.md` §10 notes
none was needed there, so this may be the first real use case for one).

`test_audit_t2_a_page_is_never_flushed_before_its_own_redo_record_is_durable`:
- Needs a way to observe "page P flushed" vs "record for P's mutation became durable" as two
  independently-checkable events with the sync deliberately held open in between — i.e. a
  `DBFile` wrapping the WAL file whose `do_sync` blocks on a signal (channel/barrier) until the
  test releases it.
- Sequence: block the WAL's `do_sync`; perform a write (dirty a page, page now stamped with
  its own not-yet-durable LSN); force/wait for a flush attempt (e.g. drop the page from cache,
  or call checkpoint's flush path) and assert it did NOT happen (data on disk still reflects
  the pre-write state); release the `do_sync` block; assert the flush now proceeds.
- Confirm this fails against the current (unfixed) gate before applying the fix.

### Verification

- `cargo test -p store --lib test_audit_t2 -- --test-threads=1` green.
- Full `cargo test -p store --lib -- --test-threads=1` — 0 regressions against the 396+1
  baseline (396 from Phase 2, +1 from the process_log ordering fix already committed as
  `b9c1437`, so 397).
- `cargo build --workspace --tests` clean.

## 2. T1 — `commit()` returns before the commit record is durable

### Fix

1. `LsnClock` (or `Logger`) gains a durability-completion signal: a `Mutex<LsnId> +
   Condvar` pair (or a `crossbeam::channel` reply-per-batch, per the audit's two suggested
   shapes) tracking the highest LSN the log runner has actually `do_sync`'d — this is
   *exactly* `last_written`, already tracked, just not currently anything callers can block
   on. Add a `wait_until_durable(&self, lsn: LsnId)` that blocks (condvar wait, re-checking
   `last_written() >= lsn` each wake) until the runner's `mark_written` call has passed it.
2. `log_runner` (logger.rs) calls the condvar's `notify_all()` (or equivalent) right after
   `mark_written`, once per batch — not per record, preserving group commit exactly as it
   works today (T1's own recommendation #2).
3. `Db::commit` captures the LSN `self.logger.log_new(Operation::Commit(...))` returns, then
   calls `self.logger.wait_until_durable(that_lsn)` before proceeding to `tx_mgr.commit`.
4. Explicit opt-out: `Db::commit_nowait` (or a `Durability` choice threaded through
   `begin`/`commit`) for bulk-load callers who accept the risk — default stays durable. Given
   this is a new API surface, confirm with a quick look at whether `squeal-sql` already has a
   bulk-load path that would want this before deciding the exact shape (`bulk_load`'s default
   of 200 rows/txn, mentioned in T10's writeup, suggests one might already exist).

### Test

`test_audit_t1_commit_does_not_return_before_its_record_is_durable`: fake `DBFile` whose
`do_sync` sleeps a measurable amount (e.g. 20ms) or blocks on a signal; call `commit()`;
assert it did not return before that sync completed (either by timing — commit() latency ≥
the sleep — or, more robustly, by blocking `do_sync` on a barrier the test controls and
asserting `commit()` is still blocked until the test releases it, then completes promptly
after).

### Verification

Same shape as T2's: dedicated test green, full suite 0 regressions, workspace builds clean.

## 3. P10 — group-commit linger is paid by every isolated write

### Fix

`LOG_BATCH_LINGER` (currently a flat 200µs charged after the first message even with no other
producer) becomes adaptive: skip the linger wait entirely when the previous batch was *not*
full (a signal nothing else was contending for a batch slot), extend/keep it when the previous
batch *was* full (real concurrent load, worth waiting for more to arrive). Track "was the last
batch full" as a small piece of runner-local state (no shared/atomic needed — the runner is a
single thread).

Since T1 makes `commit()` actually wait on this, this linger becomes real, visible commit
latency for the first time — worth tuning at the same time, per the audit's own note.

### Test

A throughput/latency-shaped check is inherently fuzzier than a correctness test. Minimum bar:
a test asserting a single isolated `commit()` (no concurrent writers) does NOT pay the full
`LOG_BATCH_LINGER` — e.g. assert wall-clock latency for one commit is well under
`LOG_BATCH_LINGER`, using a fast (`MemFile`) backend so `do_sync` itself isn't the bottleneck.
Not a strict red/green test in the same sense as the correctness fixes above (performance
tests are inherently about magnitude, not a boolean), but should still fail meaningfully
against the current flat-linger behavior and pass once adaptive.

## Sequencing for this pass

T2 first (bigger, and T1's own durability guarantee is only meaningful once T2 ensures pages
never race ahead of their own redo record — a commit that's "durable" per T1 but whose page
already reached disk out of order per T2 isn't actually safe). Then T1. Then P10 last (small,
independent, pure perf).

Each gets its own commit once its test is green and the full suite shows 0 regressions,
exactly like every Phase 1/Phase 2 finding.
