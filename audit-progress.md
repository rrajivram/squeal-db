# STORE_AUDIT.md progress tracker

Catalogs all 36 findings from `STORE_AUDIT.md`. Process (per user instruction): write a test
that FAILS for each finding before touching any fix; the fix is done once that test passes and
nothing else regresses.

Status legend: `[ ]` not started · `[t-red]` red test written, confirmed failing · `[t-green]`
red test written, confirmed failing, now fixed and green · `[skip]` deliberately deferred (with
reason)

Scope decision (this pass): **Phase 1 only** (the "stop the bleeding" items — no format/WAL
change needed). Everything else stays catalogued below as `[ ]` until Phase 1 is done.
Performance findings (P1-P10) are deferred entirely — no tests written yet; revisit when
actually implementing Phase 6/7 of the fix plan, since a benchmark written before the fix mostly
just documents current (slow) behavior rather than proving anything.

**Current status: Phase 1 is DONE; Phase 2's T10 is DONE (T4+S2 staged, not yet started).** All
12 Phase 1 findings (T8, T9, T6, T12, T14, S3, S4, S5, S6, S7 — S7 counted once, covering both its
prefix and Ord sub-issues) plus Phase 2's T10 are fixed and `[t-green]`. Full `store` suite:
**381 passed, 0 failed** (`cargo test -p store --lib -- --test-threads=1`). `squeal-sql --lib`:
346 passed, 0 failed. Whole workspace builds clean. One test
(`page::tests::test_separate_header_and_data_calls_can_observe_a_mismatched_pair`) is a known
pre-existing, unrelated flaky/timing-sensitive test (confirmed via repeated standalone runs
during T12's work) — not part of this audit's scope. 3 sub-findings deliberately have no test
(T7, S7's `u64::MAX` sentinel, S7's `Eq`-capacity question) — see their own entries for why; not
blocking, since nothing regressed them. T4+S2 (the rest of Phase 2 — single WAL redesign) remain
staged, not started, per the user's decision to scope T10 as a standalone fix first. Phases 3-7
remain untouched, catalogued below as `[ ]`.

## Phase 1 — ALL FIXED

All tests below live in `store/src/db.rs`'s `mod tests` unless noted. Every finding here is
`[t-green]`; the full suite (`cargo test -p store --lib`) is 380 passed, 0 failed.

- [t-green] **T8** — FIXED. Removed `#[derive(Clone)]` from `Transaction` (and, transitively,
  from `TableCursor`/`RangeCursor`/`ScanTxn`, which owned one but had no real call site cloning
  a cursor either). Hardened `TransactionManager::abort` to return `TransactionAlreadyFinished`
  instead of silently moving an already-finished id into `aborting`. The original reproduction
  is no longer expressible at all (compile-time proof); replaced the test with
  `test_audit_t8_abort_refuses_to_move_an_already_finished_transaction_into_aborting`, covering
  the independent `abort()` hardening. Whole workspace builds clean — nothing else depended on
  `Transaction`/cursor `Clone`. Full `store` suite: 368 passed, 13 failed (the remaining
  Phase 1 items), 0 regressions.
- [t-green] **T9** — FIXED. `Db::update`/`Db::remove`'s `build` closures now special-case a row
  the SAME transaction inserted (`current.txn_id == txn && current.undo_id.is_none()`): revise/
  tombstone it in place instead of calling `find_last_committed` (which has nothing to find and
  would fail with `KeyNotFound`), keeping `undo_id` at `None` so a later own-update in the same
  chain is recognized the same way and concurrent readers' `find_visible_to` still correctly
  treats the row as having no ancestor. `update()`'s `before_write` skips logging a redundant Mod
  undo record for this case (the original insert's own `Add` undo already fully reverts it).
  `remove()`'s `before_write` does NOT skip its Del undo record, even though it's similarly
  redundant for rollback — found via a follow-up test (`test_audit_t9_insert_then_remove_then_commit_allows_reinserting_the_same_key`,
  from the audit's own recommended test list) that `Db::commit`'s tombstone-reclaim pass finds
  rows to physically clean up by scanning the undo log specifically for `Operation::Del` records,
  not by inspecting `undo_id` — skipping the log entry left the tombstone's index entry
  permanently orphaned, so reinserting the same key after commit failed with a real, permanent
  `DuplicateKey`. Fixed by always logging the Del undo record in `remove()` (traced that replaying
  it on rollback is safe in either order relative to the original insert's own `Add` undo, since
  both `update_if_txn`/`remove_if_txn` tolerate the row already being in the expected end state).
  Tests: `test_audit_t9_a_transaction_can_update_a_row_it_inserted_itself`,
  `test_audit_t9_a_transaction_can_remove_a_row_it_inserted_itself`,
  `test_audit_t9_rollback_after_insert_then_update_leaves_no_trace_of_the_row`,
  `test_audit_t9_insert_then_remove_then_commit_allows_reinserting_the_same_key` — all 4 pass.
  Full `store` suite: 372 passed, 11 failed (exactly the remaining unfixed Phase 1 items),
  0 regressions.
- [t-green] **T13** — `test_audit_t13_rollback_after_two_updates_in_one_txn_restores_the_original_not_an_intermediate_value`.
  Written and PASSES already — confirms the audit's own claim that this is a no-op today (traced
  why: both of a transaction's own updates resolve their pre-image via `find_last_committed`,
  which always walks past the txn's own uncommitted layer straight to the true committed
  ancestor, so both undo ops end up storing the identical pre-image regardless of replay order).
  Kept as a green regression guard; revisit once T9 is fixed, since T9's fix is likely to
  introduce genuine multi-hop own-chains where forward-vs-reverse order would start to matter —
  a stronger test may be needed then.
- [t-green] **T6** — FIXED. Root cause confirmed: `Db::commit`'s tombstone reclaim physically
  removed a tombstoned row from the tree unconditionally, right after commit — even though the
  in-memory undo trail needed to resolve it for an older reader's snapshot was correctly deferred
  already (`Logger::discard_or_defer_undo`). Once the physical tuple was gone, `find_visible_to`
  had nothing to start its undo-chain walk from, so a reader whose snapshot predated the delete
  saw a flat "missing" instead of falling back to the pre-delete version. Fixed by adding
  `Db::pending_tombstone_reclaims` (mirrors `Logger::pending_undo_discards`/
  `discard_or_defer_undo` exactly, just gating the tree-level physical removal instead of the
  undo trail): `commit()` now reclaims immediately only if no other transaction is active at that
  exact commit point, otherwise defers each del record with the same waiter set; a new
  `Db::drain_ready_tombstone_reclaims`, called opportunistically from `begin()` alongside the
  existing `drain_aborting`/`drain_ready_undo_discards`, finishes the reclaim once every waiter
  has finished. Test: `test_audit_t6_a_reader_snapshot_survives_a_concurrent_committed_delete` —
  passes, plus T9/T13 re-verified with no regressions.
  Follow-up bug found and fixed via the existing stress test
  `test_concurrent_insert_remove_reinsert_does_not_resurrect_stale_value` (16 threads, tight
  insert/remove/commit cycles, no artificial delay — pre-existing, not one of the audit's own
  tests): my first pass derived the reclaim's "owner" transaction from `Record::tuple.txn_id`,
  but for a `Del` undo record `tuple` is the *pre-image* (whoever owned the row before the
  remove), not the remover — so `reclaim_tombstones`'s `is_same_txn` check silently failed for
  every deferred reclaim, permanently orphaning the index entry and turning any later reinsert of
  the same key into a real `DuplicateKey`, 100% reproducible under load. Fixed by carrying the
  remover's `TransactionId` explicitly alongside each deferred `(waiters, record)` pair instead of
  trying to infer it. Stress test now passes consistently (3 repeated runs, previously failed
  100% of the time within the first cycle).
- [ ] **T7** — deliberately deferred, no test written. Reader-pinned undo garbage has no bound,
  no stats, no back-pressure — there's no current *wrong* behavior to assert against (it's a
  missing cap/API), so a red test needs the stats API shape to exist first. Pair with T6's fix
  (same deferred-reclaim horizon).
- [t-green] **T12** — FIXED. `Db::rollback_by_id` now catches a failed `revert_txn_writes` and
  moves the transaction into `aborting` (via `tx_mgr.abort`) instead of propagating the error
  with `?` and leaving it stuck in `active` forever. `aborting` is the same recoverable state an
  abandoned (dropped-without-explicit-rollback) transaction already lands in, so
  `drain_aborting`'s existing opportunistic retry (via `revert_aborted`, called from every
  `begin()`) picks it back up later and finishes the revert once whatever failed stops failing.
  The original explicit `Operation::Rollback` log entry is skipped on this path — matching
  `revert_aborted`'s own existing behavior for abandoned transactions, which never logs one
  either. Test: `test_audit_t12_a_failed_rollback_must_not_leave_the_transaction_stuck_active_forever`
  — passes. Full suite: 371 passed, 12 failed — the 10 remaining unfixed Phase 1 items plus 2
  extra (`test_checkpoint_keeps_log_bounded_across_many_rounds`,
  `test_concurrent_insert_remove_under_splits_does_not_resurrect_stale_value`) and one unrelated
  (`page::tests::test_separate_header_and_data_calls_can_observe_a_mismatched_pair`); all 3
  confirmed pre-existing flaky/timing-sensitive tests, not caused by this fix — each passes
  reliably (3/3) run standalone.
- [t-green] **T14** — FIXED, but the real root cause turned out to be different from (and
  narrower than) the audit's own diagnosis. `LIKELY` in the audit, `CONFIRMED` empirically first
  (real threads, no artificial delay: ~1140-4067 false "missing" reads per 20,000 iterations).
  Applied the audit's own recommended fix first — `BPlusTree::relocate_tuple` (extracted from
  `update`/`update_checked`'s relocation branch) now writes the new copy and repoints the index
  *before* removing the old copy, instead of remove-then-write-then-repoint, so the index never
  points at a page missing the tuple; also added `BPlusTree::relocation_lock` (a table-wide
  `RwLock<()>`) so `find()`'s "read index, then read the data page it points to" (two separate
  lock acquisitions) is atomic with respect to a concurrent relocation, closing a second,
  narrower TOCTOU an early retry-based mitigation only reduced (~1140-4067 → ~150-200) rather
  than eliminated. Both are real, kept fixes — but doing them dropped the failure to a **still
  nonzero** ~130-180, proving there was a THIRD, unrelated bug underneath. Root-caused via direct
  instrumentation (traced exactly which check inside `find_visible_to` was failing, not just
  guessing): `Db::commit` computed `still_active` / called `Logger::discard_or_defer_undo` —
  which can discard a just-committing transaction's *entire* undo trail immediately, if no one
  else happens to be active — **before** calling `tx_mgr.commit(id)`, which is what actually
  flips `is_committed(id)` to true. That left a real window where a concurrent
  `find_visible_to` walk saw `is_committed(id) == false` (so tried to walk *past* id for an
  older pre-image) but id's undo trail was *already gone* (discarded a few instructions
  earlier) — hitting `Visibility::MissingUndoRecord`. Its documented fallback
  (`find_last_committed(tuple)`) doesn't rescue this specific case either: it re-walks the exact
  same, already-discarded chain from the same starting tuple and hits the identical dead end.
  Fixed by reordering `Db::commit` to call `tx_mgr.commit(id)` *before* deciding whether to
  discard or defer the undo trail — safe because `still_active`'s computation (get the active
  set, then explicitly remove `id`) gives the identical result regardless of whether
  `tx_mgr.commit` has already removed `id` from that set or not. Once ordered this way, by the
  time any walker could observe the undo trail gone, `is_committed(id)` is already true, so it
  never needs to walk past `id` at all. Test: `test_audit_t14_...` — 5/5 clean runs (0 missing
  each) after this fix, vs. consistently nonzero after the first two fixes alone.
- [t-green] **S3** — FIXED. `ValueItem::from_bytes_many`/`from_bytes_single` and
  `IndexKey::from_bytes` now return `Result<_, StoreError>` (new variant
  `StoreError::TruncatedValueItem`) instead of panicking on truncated/malformed input. Replaced
  every raw slice index and `try_into().unwrap()` with a bounds-checked `take()` helper
  (`bytes.get(index..index+len)`, with `checked_add`/`saturating_add` guarding the offset
  arithmetic itself against overflow on adversarial length prefixes) that returns `Err` instead
  of panicking; `IndexKey::from_bytes`'s per-field loop also switched from `bytes[index..]` to
  `bytes.get(index..)`, since a prior field's truncated declared length could otherwise push
  `index` past `bytes.len()` and panic on the slice itself before `from_bytes_many` ever got a
  chance to report it. Signature change (anticipated by the audit's own note) rippled to all
  callers: ~15 in-crate test call sites (`.unwrap()`ed, matching existing test style) plus two
  real external callers in `squeal-sql` — `source/run.rs`'s `RunSource::next` (propagates via
  `?`/`SchemaError`'s existing `From<StoreError>`) and `table.rs`'s `VersionedRowVisitor::visit_seq`
  (a serde `Deserialize` impl, mapped via `de::Error::custom`) — plus one new match arm in
  `squeal-sql`'s `From<StoreError> for SchemaError` (bucketed with the other data-corruption-style
  errors as `InternalError`, alongside `PageChecksumMismatch`/`InvalidPageMagic`). Whole workspace
  builds clean. Tests: both S3 tests pass (the `catch_unwind` wrapper is now vacuous — the
  functions return `Err` rather than needing to be caught — but left in place since it's still
  correct and matches the audit's own reasoning). `squeal-sql --lib`: 346 passed, 0 failed — no
  regressions from the signature change.
- [t-green] **S4** — FIXED. `Db::create_core_db` opened all three files (main/`.undo`/`.redo`)
  with `OpenOptions::create(true)` (create-or-open), so `Db::create` on an already-existing path
  silently reopened it and overwrote its header with `page_count=0`, destroying any existing
  data with no warning. Switched all three to `create_new(true)`, which fails with an
  `AlreadyExists` io error if any of the three paths already exists — `Db::open` (the "load an
  existing database" entry point) is unaffected, it has its own separate file-opening path.
  Confirmed zero blast radius on the rest of the suite before running it: `MemFile::open` and
  `NamedMemFile::open` (the two in-memory `DBFile` backends every other test — and all of
  `squeal-sql`'s tests — actually use) both explicitly ignore their `OpenOptions` argument
  entirely, so `create_new` has no effect on them either way; only a real `std::fs::File`-backed
  `FileDB` (used only by S4/S5's own tests) observes the new behavior. Test:
  `test_audit_s4_create_on_an_existing_path_does_not_silently_destroy_it` — passes.
- [t-green] **S5** — FIXED. `Db::delete` had no file handle of its own to check (a static,
  path-based API) — it now opens each of the three files fresh and attempts the same exclusive,
  non-blocking `do_lock()` create()/open() already use: success means no one else holds it (drop
  immediately, proceed to unlink); failure means some other open handle owns it right now, so
  delete refuses with an error instead of unlinking a live database's files out from under it.
  Fixing this surfaced (via the existing suite, not a new test) that 4 pre-existing crash-replay
  tests (`test_drop_table_persists_across_close_and_reopen`,
  `test_replay_handles_empty_file_backed_logs_without_panicking`,
  `test_replay_recovers_committed_writes_on_file_backed_db`,
  `test_replay_is_idempotent_across_repeated_reopens_file_backed`) called `FileDB::delete` for
  cleanup at the end while still holding one or more live `db`/`db2`/`db3` handles open on
  purpose (simulating a crash without a clean close) — previously harmless since delete() never
  checked locks at all, but now silently no-ops (via `.unwrap_or_default()`) instead of actually
  cleaning up, since those handles still hold the lock. Fixed by adding explicit `drop(...)` calls
  for every live handle immediately before each test's final delete, restoring real cleanup.
  Tests: `test_audit_s5_delete_refuses_to_remove_a_locked_live_database` plus all 4 previously-
  passing tests above — all pass.
- [t-green] **S6** — FIXED. `create_table_with_index_entry_size` registered the new table in
  `self.tables` and the name generator BEFORE calling `write_system_tables()` — so once the
  single-page catalog filled up (~1000th table) and that call failed with `PageCapacityError`,
  the failed table stayed fully registered in memory despite `create_table` returning `Err`:
  `table_id_by_name` found it, and — worse — every LATER `write_system_tables` call (including
  `checkpoint()`'s own) tried to serialize it too and hit the identical `PageCapacityError`,
  permanently breaking checkpointing on an otherwise-healthy database. Fixed by rolling back all
  three steps on failure: remove the table from `self.tables`, deregister its generator entry,
  and free the index/data pages `BPlusTree::new` had already allocated for it (same pattern
  `drop_table` already uses) — so a rejected `create_table` leaves the database exactly as if it
  had never been called. Test:
  `test_audit_s6_create_table_fails_cleanly_once_the_catalog_page_is_full` — passes.
- [t-green] **S7 (prefix)** — FIXED. Added `constant::RESERVED_TABLE_NAME_PREFIX` (`"__system."`)
  and a check in `Db::validate_table_name` rejecting any user-chosen name under it (new
  `StoreError::ReservedTableName`, bucketed as `BadTableName` in `squeal-sql`'s
  `From<StoreError> for SchemaError`). System tables themselves (catalog/generator/free-page)
  never go through `validate_table_name` at all — they're raw fixed pages 0/1/2, not entries in
  `self.tables` — so nothing internal needed an exemption. Test:
  `test_audit_s7_the_system_prefix_is_reserved_as_a_whole_namespace` — passes.
- [t-green] **S7 (Ord panics)** — FIXED. Replaced `ValueItem::Ord`'s panicking catch-all arms
  with a fixed `type_rank()` (Null < Boolean < Integer < Double < Datetime < Str < Blob, arbitrary
  but total) used only when comparing two different variants; same-variant comparisons keep their
  existing natural ordering, plus a new Blob-vs-Blob arm that compares content (byte slice via
  `Arc<[u8]>`'s own `Ord`), matching how Str already ignores its reserved-capacity field. This also
  fixed a genuine asymmetry the old code had: `Blob.cmp(&Null)` panicked but `Null.cmp(&Blob)`
  didn't (Null had its own special-cased catch-all checked first) — every pair now agrees with
  itself in both directions. Updated the tests that used to pin the panicking behavior via
  `#[should_panic]` (`test_partial_ord_blob_vs_*_panics`, `test_partial_ord_*_panics` for
  Boolean/Integer/Str cross-type pairs, `IndexKey`'s `test_partial_ord_panics_when_a_field_is_blob`)
  to instead assert the new, well-defined ordering — they were pinning the exact opposite of the
  desired end state, so leaving them alongside wasn't an option. Tests:
  `test_audit_s7_value_item_ord_does_not_panic_on_mixed_types`,
  `test_audit_s7_value_item_ord_does_not_panic_on_blob` — pass, plus all 51
  `valueitem_tests`/`indexkey_tests` (including the rewritten ones) green. `squeal-sql --lib`:
  346 passed, 0 failed.
- [ ] **S7 (`u64::MAX` sentinel)** — attempted empirical confirmation, could NOT reproduce.
  Inserted a single row at `u64::MAX`, and separately forced a 3-level-deep tree (3000 rows)
  then inserted/found `u64::MAX` and verified all 3000 other rows were still present — no
  corruption or misrouting observed either way. The routing code's `while tuple.id >= row_id.id`
  loop combined with its `None => break` fallthrough appears to handle the sentinel value
  correctly for insert+find, at least for straightforward ascending insertion. Not disproven
  entirely — `remove_index_entry`/`update_index_entry`'s own equality-based paths weren't
  specifically stress-tested — but no red test written since I can't currently show it fails.
  Flagged for a deeper look if picked up later, not blocking Phase 1.
- [ ] **S7 (`Eq` drops reserved capacity)** — deliberately deferred. `ValueItem::Str`/`Blob`
  equality includes the declared/reserved capacity, not just content, and this is already
  pinned as *intentional* current behavior by an existing test
  (`test_str_eq_and_partial_cmp_disagree_on_reserved_capacity` in `valueitem_tests`). Writing a
  red test now would mean immediately also deciding how to handle that existing test — closer
  to starting the fix than just pinning the bug. Left for the fix step itself.

## Phase 2 — T10 FIXED; T4/S2 staged as a separate follow-up

- [t-green] **T10** — FIXED. Root cause confirmed via `grep -rn "UndoId"` across `store/src`:
  `Logger::log_undo` minted `UndoId(id.len() as u16)` (`id.len()` being the transaction's own
  TOTAL undo-op count so far, across every row it touches, not per-row) — wraps at 65,536 ops,
  not saturates or errors. `Logger::next_undo_id`/`impl From<usize> for UndoId` did the identical
  `as u16` truncation. A wrapped id then silently resolved, via `find_undo_tuple`'s
  `v.get(undo_id.0 as usize)`, to whatever op happens to sit at that (much lower) index in the
  SAME transaction's op list — if that index belongs to a DIFFERENT row, the walk hands back that
  other row's pre-image entirely. Fixed by widening `UndoId`'s inner type from `u16` to `u64`
  (not the audit's stated minimum of `u32` — chosen because the later T4+S2 redesign will very
  likely make the undo pointer *be* the record's own `LsnId`, already `u64`, avoiding a second
  width change later) across all three call sites in `store/src/logger.rs`
  (`UndoId(pub(crate) u64)`, `log_undo`'s `UndoId(id.len() as u64)`,
  `From<usize>`'s `Self(value as u64)`). `find_undo_tuple`'s `undo_id.0 as usize` cast needed no
  change (widening `u64`→`usize` is lossless on any real target). `bplustree.rs`'s
  `MAX_ENTRY_BYTES` comment needed no change either — it already accounts for `Option<UndoId>` as
  1 byte on the assumption it's `None` for index entries, which postcard's `None` discriminant is
  regardless of the inner type's width.
  Test: `test_audit_t10_a_wrapped_undo_id_must_not_alias_a_different_rows_undo_entry`. IMPORTANT
  process note — the audit's own suggested repro ("70,000 updates of one row in one transaction,
  then rolled back") was tried FIRST and confirmed to NOT reproduce anything (it passed cleanly
  against the unfixed `u16` code). Traced why, directly, before redesigning the test: (1)
  `Db::rollback`'s `revert_txn_writes` replays the raw undo `Vec<Operation>` directly and never
  goes through `UndoId`/`find_undo_tuple` at all, so a rollback-shaped test can't touch this bug
  by construction; (2) `Db::update`'s `build` resolves every one of a transaction's own repeated
  updates to the SAME row via `find_last_committed`, which always walks straight past the txn's
  own in-flight chain to the true committed ancestor — so every undo entry logged for repeated
  updates to one row has IDENTICAL content regardless of index, making even a genuinely wrong
  slot within that row's own entries indistinguishable. Rebuilt the test around the audit's
  OTHER named consequence instead — a concurrent MVCC read (`Db::find_visible_to`) walking a
  wrapped `undo_id` into a different row's undo entry — by giving a second row (touched once,
  first) the low slot the wraparound aliases into, then driving 65,536 updates against the row
  under test so its last update's minted id wraps to that same low slot. Confirmed red first
  (failed with `left: Int(1), right: Int(2)` — row 2's lookup resolved to row 1's identity) against
  the unfixed `u16` code, then green after the widen. Full `store` suite: 381 passed (380 baseline
  + this test), 0 failed, 0 regressions (`cargo test -p store --lib -- --test-threads=1`).
  `squeal-sql --lib`: 346 passed, 0 failed. Whole workspace (`cargo build --workspace --tests`)
  builds clean.
- [ ] **T4** — redo and undo are two independently-synced files; commit point isn't atomic.
  Design done — see `T4_S2_WAL_DESIGN.md` (one framed/checksummed WAL, one runner thread, LSN
  becomes the undo pointer, three-pass recovery, fault-injecting `DBFile` test wrapper, exact
  blast radius). Implementation not started.
- [ ] **S2** — log records have no framing/checksum; a torn tail makes the DB unopenable. Shares
  T4's design doc (`T4_S2_WAL_DESIGN.md`, §2's record-framing/recovery-scan rule) — implementation
  not started.
- [ ] **P1** — two fsyncs per commit where one would do. *(deferred — performance)*
- [ ] **P8** — three clones of every pre-image per operation. *(deferred — performance)*

### Phase 3 — page-LSN gating + durable commit
- [ ] **T2** — pages are flushed gated on the wrong LSN; a page can reach disk before its own
  redo record.
- [ ] **T1** — `commit()` returns before the commit record is actually durable (fsynced).
- [ ] **P10** — group-commit linger is paid by every isolated write. *(deferred — performance)*

### Phase 4 — fuzzy checkpoint + header discipline
- [ ] **T3** — `checkpoint()` with an active transaction turns uncommitted writes into
  committed ones after a crash. `CONFIRMED`.
- [ ] **T5** — checkpoint sequence isn't crash-ordered; log truncation can precede the header
  write, and the header is never synced.
- [ ] **T16** — free list / page count only persisted at checkpoint; crash recovery can
  double-allocate a page still holding committed data.
- [ ] **S1** — no format version, no header checksum, no header validation.
- [ ] **T17** — `drop_table` frees pages in-flight operations may still hold; not logged either.

### Phase 5 — logical timestamps, slimmer tuples
- [ ] **T11** — wall-clock (`SystemTime`) timestamps are the ordering primitive for isolation
  and conflict detection; not monotonic, can panic pre-1970, can invert conflict detection on a
  backward clock step.
- [ ] **P4** — every row and index entry carries a 128-bit timestamp. *(deferred — performance)*
- [ ] **P7** — per-row visibility check clones the reader's whole snapshot set.
  *(deferred — performance)*

### Phase 6 — locking and cache
- [ ] **P2** — `ArcLock` is a global serialization point with busy-polling.
  *(deferred — performance)*
- [ ] **P3** — cache bookkeeping does two `SystemTime::now()` calls + a heap update per page
  access. *(deferred — performance)*
- [ ] **P5** — inner-node routing clones and decodes every entry on the page.
  *(deferred — performance)*
- [ ] **S9** — resource exhaustion knobs: no cap on tuple count/undo trail/active txns;
  `retry_on_contention`'s timeout isn't honored (hardcoded 60s `ArcLock` wait);
  `begin()`'s inline checkpoint check costs two `fstat`s per call.

### Phase 7 — slotted pages
- [ ] **P6** — whole-page re-serialization on every flush; deep clone on every `write_page`.
  *(deferred — performance)*
- [ ] **P9** — overflow chains do synchronous, unbatched header writes.
  *(deferred — performance)*

### Not yet slotted into a phase by the audit itself
- [ ] **T15** — recovery doesn't replay `Add`/`Mod` for a committed txn whose page write was
  superseded. Edge case; audit notes it's naturally resolved once T4's page-LSN idempotence
  check exists — verify once T4 lands, no separate fix needed.
- [ ] **S8** — panics as control flow (11 `panic!`s in `bplustree.rs`, `std::sync::RwLock`
  poisoning turns one panic into permanent failure of that page/lock). Recommend doing the
  `parking_lot` swap *before* Phase 1's testing work, since a poisoned lock from an unrelated
  panic could cause misleading cross-test failures during fault-injection testing later.

## Test infrastructure (Phase 0, build as needed)

- [ ] Fault-injecting `DBFile` wrapper (drop/delay writes or syncs to a chosen file after N
  calls, optionally return errors) — needed now for T12; needed later for T1, T2, T4, T5, T16.
- [ ] Crash-consistency harness (random workload, snapshot only what's been `do_sync`'d, reopen,
  verify committed-present/uncommitted-absent) — needed for Phase 2-4, not Phase 1.
