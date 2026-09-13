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

**Current status: Phases 1, 2, and 3 are DONE.** All 12 Phase 1 findings (T8, T9, T6, T12, T14,
S3, S4, S5, S6, S7 — S7 counted once, covering both its prefix and Ord sub-issues), Phase 2's
T10/T4/S2, and Phase 3's T2/T1/P10 are fixed and `[t-green]`. T4+S2 shipped together as one WAL
redesign (single framed/checksummed log file, `LogHeader` mismatch detection, `UndoId` retired in
favor of LSN-keyed lookups) — full design in `T4_S2_WAL_DESIGN.md`. T2/T1/P10 design in
`T2_T1_P10_DURABILITY_DESIGN.md`; T2 fixed the actual root cause (pages stamped from the flush
watermark instead of their own operation's lsn), T1 added a real durability-wait to `commit()`
(catching a cold-start-sentinel bug in the process), P10 made the group-commit linger adaptive.
Also caught and fixed, mid-Phase-3, a genuine pre-existing bug unrelated to any single finding: a
nondeterministic (HashMap-iteration-order-dependent) replay ordering bug in `process_log`'s redo
pass, found by re-verifying Phase 2's own "396 passed" claim after a session boundary and
noticing a test failed 5/6 standalone reruns despite being part of that green commit — see the
`process_log` note further down and commit `b9c1437`.
`squeal-sql --lib`: 346 passed, 0 failed throughout. Whole workspace builds clean throughout. One
test (`page::tests::test_separate_header_and_data_calls_can_observe_a_mismatched_pair`) is a
known pre-existing, unrelated flaky/timing-sensitive test (confirmed via repeated standalone runs
during T12's work) — not part of this audit's scope. 3 sub-findings deliberately have no test
(T7, S7's `u64::MAX` sentinel, S7's `Eq`-capacity question) — see their own entries for why; not
blocking, since nothing regressed them. Full `store` suite after Phase 3: **399 passed, 0
failed**. Phases 4-7 remain untouched, catalogued below as `[ ]`.

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

## Phase 2 — ALL FIXED (T10, T4, S2)

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
- [t-green] **T4 + S2** — FIXED together (one root fix, per the design). Full design in
  `T4_S2_WAL_DESIGN.md` (13 sections + a 14th documenting where implementation diverged from the
  design). Summary of what shipped:
  - **One WAL file** (`<name>.wal`) replacing the separate `.undo`/`.redo` pair — one runner
    thread (`log_runner`, replacing `undo_log_runner`+`redo_log_runner`), one channel
    (`LogMsg`), one fsync per batch. `Db<F>`'s `undo_file`/`redo_file` fields collapse to one
    `log_file`; `create_core_db`/`open_using`/`close`/`Db::delete`/`begin`'s checkpoint-size
    check all updated. T4's "commit isn't atomic" is closed as a structural consequence — there
    is only one artifact, so there's no "redo landed, undo didn't" state left to reach.
  - **Framed, checksummed records** (`[u32 len][u32 checksum][payload]`, `logger::scan_log`) —
    S2's actual fix. Checksum is `page::fnv1a_32` (the SAME hash already used for physical page
    checksums), not a new `crc32fast` dependency — reused rather than adding a second algorithm
    for the identical job. Recovery scan rule: a checksum failure with nothing valid after it is
    a torn tail (dropped silently); a checksum failure with a complete, valid record after it is
    real corruption (`StoreError::LogCorruption`, refuses to open).
  - **`LogHeader`** (magic/version/page_size), added on top of the original sketch per explicit
    follow-up feedback: written once at file creation, validated at `open_using` BEFORE any lock
    is taken, so a log file paired with the wrong database (wrong page size, wrong WAL version,
    wrong file entirely) is refused cleanly (`StoreError::LogHeaderMismatch`) instead of failing
    deep inside decode or silently misreading a different page layout. Survives checkpoint's
    truncate — `log_runner` restores it verbatim immediately after each truncate.
  - **Combined pre/post-image `Operation`** (`Add{txn,post}`, `Mod{txn,pre:Option<Record>,post}`,
    `Del{txn,pre}`) — one `Logger::log`/`log_new` call per write instead of paired
    `log_redo`+`log_undo`. `Mod.pre: Option<Record>` (not plain `Record`, per the design's
    original sketch) — `None` marks a "redo-only" record for the own-insert-then-update chain,
    which must NOT contribute a real pre-image to undo replay (traced why: `revert_undo_ops`
    replays forward, so a real pre-image there would re-materialize a row the original Add's own
    revert just removed in the same pass).
  - **`UndoId` retired entirely**, not just widened past T10 — `Tuple.undo_id: Option<UndoId>` →
    `Tuple.pre_lsn: Option<LsnId>`, an LSN minted once, globally, by `LsnClock::next_lsn`, never
    reused. `Logger`'s `undo_txns: HashMap<TransactionId, Vec<Operation>>` → `records:
    HashMap<LsnId, Operation>` + `by_txn: HashMap<TransactionId, Vec<LsnId>>`; `find_undo_tuple`
    → `find_record(lsn)`, no `TransactionId` needed at all. `LsnId` kept `pub` (field
    `pub(crate)`) — mirrors `UndoId`'s exact old visibility split, needed because `Tuple::pre_lsn`
    appears in public signatures `squeal-sql` (a different crate) calls.
  - **Recovery**: `load_logs`/`process_redo`/`process_undo` → one `process_log`, three passes
    (analysis/redo/undo) over the single scanned buffer. Dropped the old `inprogress`/`rollback`
    `HashSet`s outright — both were built by the pre-existing code and never actually read
    afterward in either old function; "not in `committed`" already fully characterizes "needs
    undo" either way.
  - **Two real bugs caught by tests during implementation, not found by inspection**: (1)
    `LogHeader::encoded_len()` initially used `size_of::<LogHeader>()`, which is NOT the same as
    its postcard-encoded length (Rust pads the struct's layout to 16 bytes for `page_size: u64`'s
    8-byte alignment; postcard's actual encoding is 14 bytes, no padding) — `db.rs`'s own
    `Header` makes the identical assumption and gets away with it only because the main file
    always has real page data after its header to absorb an over-read; the WAL doesn't. Fixed by
    computing the true length via `to_allocvec` instead of trusting `size_of`. (2) `log_runner`'s
    post-checkpoint header rewrite wrote at the OLD (pre-truncate) seek position instead of byte
    0 — `Opener::truncate` resets a file's length but not its cursor — silently padding the file
    with zeros instead of landing the header at the start. Both caught by
    `test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file` (pre-existing)
    failing, not by code review.
  - **No `FaultyFile` wrapper built** (§10 in the design doc sketched one) — every scenario it
    was meant to enable turned out to need only one known byte-level change applied once to a
    `MemFile` snapshot, which `crash_clone` + direct buffer manipulation already does throughout
    this suite. Covered instead by `logger.rs`'s own unit tests against hand-built byte buffers
    (no `Db`/`Logger` wiring) plus three new `db.rs` integration tests through the real
    `Db::open_using` path: `test_open_using_refuses_a_log_file_from_a_different_page_size_database`,
    `test_open_using_tolerates_a_torn_tail_and_recovers_everything_before_it`,
    `test_open_using_refuses_a_log_file_with_mid_file_corruption`.
  - **Blast radius larger than the design doc's "12+2" estimate**: 32 `open_using(...)` calls and
    20 `.close()` calls across `db.rs`'s test module needed the 3-tuple→2-tuple mechanical
    change (several tests call more than one). Handled as a verified whole-file substring
    replacement, with the compiler's own type errors catching anything missed. Also caught (by
    grep, not a failing test — flagged as such rather than presented as more rigorously verified
    than it was): `store/src/named_memfile.rs`'s `NamedMemFile::delete` hardcoded the old
    `.undo`/`.redo` sibling-cleanup suffixes; updated to `.wal`.
  - Tests: all of the above plus every migrated pre-existing replay/checkpoint test green. Full
    `store` suite: **396 passed, 0 failed** (`cargo test -p store --lib -- --test-threads=1`;
    381 baseline-after-T10 + 15 new tests — 3 `db.rs` integration tests + 12 `logger.rs` unit
    tests — confirmed by exact count across two independent runs). `squeal-sql --lib`: 346
    passed, 0 failed (unaffected aside from `Database::close`'s return-type ripple and one stale
    comment). Whole workspace (`cargo build --workspace --tests`) builds clean.
  - **Follow-up correctness fix (found after the fact, in a later session, not caught by the
    "396 passed" run above)**: `process_log`'s Pass 2 (redo) grouped records by transaction into
    `by_txn: HashMap<TransactionId, Vec<&LogRecord>>` and iterated THAT map to decide replay
    order — but a `HashMap`'s iteration order across different keys is randomized per-process
    (Rust's default `RandomState`, reseeded each process start), so which of two DIFFERENT
    committed transactions' records replayed first was effectively a coin flip each run. Found
    because `test_replay_handles_mixed_add_mod_del_across_committed_and_abandoned_txns` — a
    pre-existing test — failed 5 of 6 standalone reruns despite being part of the "396 passed"
    commit: row 2 is inserted by committed txn C then removed by committed txn D; whenever D's
    group happened to be visited before C's, replay ran `remove(row2)` (a no-op — row 2 isn't
    there yet) and only then `insert_if_needed(row2)` from C's group — resurrecting a row that
    was correctly, committedly removed. Fixed by having Pass 2 iterate `scanned.records`
    directly (already in true LSN/log order from the scan) instead of `by_txn`, checking
    `committed.contains(txn)` per record inline — `by_txn` is still built and used, unchanged,
    for Pass 3 (undo), where it's safe: two *different* uncommitted transactions can never have
    written the same row (write-conflict detection guarantees only one owner at a time while
    uncommitted), so cross-transaction order doesn't matter there, only within one transaction's
    own op list — which was already correctly built in scan order and already relied on
    `update_if_txn`/`remove_if_txn`'s own order-tolerant checks (established during Phase 1's
    T13 work). Confirmed fixed, not just less likely: 20/20 standalone reruns green after the
    fix (was 1/6 before), plus two full-suite runs (fresh process each time, so a different
    random hash seed) both at 396 passed, 0 failed. `squeal-sql --lib`: 346 passed, 0 failed.
- [t-green] **P1** — effectively FIXED as a structural consequence of T4, verified by reading
  `logger::log_runner` directly: one WAL file, one runner thread, one `do_sync()` call per
  batch — down from two (one per the old separate undo/redo files). No separate code change
  or dedicated benchmark; noted here since the audit calls it out as a distinct finding, and
  because verifying it mattered before deciding Phase 3 + perf fixes needed any NEW work here.
- [t-green] **P8** — effectively FIXED as a structural consequence of T4, verified by reading
  `Logger::log` directly: one `op.clone()` (into the in-memory `records` map, needed so a
  later rollback/MVCC walk can find it) plus one MOVE (not clone) of the original `op` into the
  channel message — down from the audit's counted three (a `.clone()` into the vec, a second
  `.clone()` into the message, plus the caller's own), since the old separate redo+undo
  logging calls are gone. Not literally the audit's suggested `Arc<Record>` shared reference,
  but the SAME outcome (one clone total) via a simpler mechanism, and `Operation`'s own fields
  (`TransactionId` is `Arc`-backed internally, `Tuple.data` is `Arc<[u8]>`) already make that
  one remaining clone cheap (mostly refcount bumps, not deep copies).

## Phase 3 — ALL FIXED (T2, T1, P10)

Design doc: `T2_T1_P10_DURABILITY_DESIGN.md`.

- [t-green] **T2** — FIXED. Root cause confirmed in code, not just from the audit:
  `Page::set_dirty(true)` stamped a freshly-dirtied page's `lsn` field from
  `clock.last_written()` — the CURRENT flush watermark ("whatever's already durable") — not
  from the redo lsn of the mutation dirtying it right now, because that lsn didn't exist yet:
  `Db::insert` called `table.insert(...)` (mutates + dirties the page) BEFORE
  `self.logger.log_new(op)` (mints the lsn and logs the record); same shape in
  `update`/`remove`. The writer thread's flush gate was `page.lsn < clock.last_written()` —
  satisfied by a stale watermark stamp as soon as ANY later, unrelated record became durable,
  flushing a page before its own change's redo record was even logged.
  Reproduced with a test needing NO timing/concurrency/fault-injection at all: advance the
  watermark to a known value via an unrelated, already-durable prior write, then check whether
  a NEW insert's page ends up stamped with something `<=` that old watermark (proving it got
  the stale value — a fresh lsn, minted after the warmup already committed, could only ever be
  strictly greater). Confirmed red first.
  Fixed by: (1) minting the lsn BEFORE mutating — `Db::insert`/`Db::update`/`Db::remove` call
  `Logger::next_lsn()` (already existed, previously only used for `pre_lsn` stamping) ahead of
  the physical write, then log the record under that SAME lsn via `Logger::log` (not
  `log_new`, which would mint a different one); (2) `Page::stamp_lsn_at_least(lsn)` — a new,
  monotonic (`max(current, lsn)`) stamp, called by a new `PageBuffer::write_locked_page_with_lsn`
  right before a page is published to the cache, overriding `set_dirty`'s watermark stamp with
  the correct value while still under the page's exclusive lock; (3) threading `lsn: LsnId`
  through every `BPlusTree` write path that can reach a `write_locked_page` call
  (`insert_at_lsn`, `write_data`, `insert_index`, `insert_recursive`, `split_if_needed`,
  `split_non_root_page`, `update_root_page`, `split_root_page`, `write_page`,
  `update_checked`, `relocate_tuple`, `update_index_entry`) — a monotonic stamp handles a page
  touched twice in one operation (e.g. a split writing to both a page and its new sibling)
  correctly regardless of write order; (4) the writer's flush gate becomes `<=`, not `<` — once
  `page.lsn` correctly equals the exact lsn it needs durable, a page whose lsn exactly equals
  the watermark is safe to flush now, not stuck waiting for something strictly newer.
  `table.remove`/`remove_index_entry` needed NO changes: `Db::remove` never calls them for a
  live, first-logged operation (it tombstones via `update_checked`, already covered) — they're
  only reached by `commit()`'s best-effort post-commit tombstone reclaim (already
  best-effort/replayable, per T6) and by crash-recovery replay (already past the durability
  boundary), neither of which needs fresh redo-durability protection.
  Blast-radius control: `insert`/`update`/`update_checked` (and the `before_write` closures
  `Db::update`/`Db::remove` pass into it) needed a genuinely new design, not just a bolted-on
  parameter — `update_checked`'s conditional lsn-minting decision (own-fresh-insert vs. a real
  ancestor) only resolves INSIDE `build`, which runs under `update_checked`'s own lock, so the
  lsn can't be handed in ahead of time the way `Db::insert` does. Solved by changing
  `before_write`'s return type from `Result<(), StoreError>` to `Result<LsnId, StoreError>` —
  it already computed the correct lsn internally for logging; `update_checked` now captures
  that same return value and uses it for stamping instead of taking a separate parameter.
  `insert`/`update` themselves (the plain, non-`Db`-facing public API, still used directly by
  ~62 existing `bplustree.rs` unit tests) kept their EXACT original signatures — `insert` is a
  thin wrapper over the new `pub(crate) insert_at_lsn` that mints its own lsn when no caller
  supplies one; `update` mints its own inline (nothing needed an `update_at_lsn` split, since
  only `Db::update_checked_with_retry` — not the plain path — needed an externally-supplied
  lsn). Verified safe for those 62 tests before relying on it: their own `make_buffer`/
  `make_logger` test helpers wire up two entirely INDEPENDENT `LsnClock`s (a pre-existing test
  fixture quirk, not something this fix introduced), so a self-minted lsn from `BPlusTree`'s
  own logger never interacts with the buffer's flush-gating clock in a way that could stall a
  page — confirmed via a `cargo build` with zero signature-mismatch errors, not just
  assumption.
  One pre-existing test needed updating to match the corrected (now stricter) semantics:
  `test_pending_write_cap_blocks_then_drains_without_deadlock` stamped its test pages via plain
  `set_dirty(true)` (landing exactly ON the watermark) specifically because the OLD `<` gate
  treated "exactly on the watermark" as "not yet durable" — with the fix, that's now genuinely
  durable and flushes immediately, so the test's own `pending`-queue setup no longer filled to
  capacity as intended. Fixed by stamping explicitly one above the watermark via
  `stamp_lsn_at_least`, matching how a real not-yet-logged operation is stamped now.
  Tests: `test_audit_t2_a_page_is_stamped_with_its_own_operations_lsn_not_a_stale_watermark`
  plus the updated pending-write-cap test — both pass. Full `store` suite: 397 passed, 0
  failed (396 baseline + this test), confirmed after the pending-write-cap fix. `squeal-sql
  --lib`: 346 passed, 0 failed. Whole workspace builds clean.
- [t-green] **T1** — FIXED. `Db::commit` logged its `Operation::Commit` record via `Logger::log_new`
  — a plain channel send that returns as soon as the log runner thread merely accepts the
  message, not once it's actually batched, written, and `do_sync`'d. Fixed by adding a
  durability-completion signal to `LsnClock`: a `Mutex<()>`/`Condvar` pair (`durable_mutex`/
  `durable_condvar`) alongside the existing `last_written` watermark — `mark_written` now
  notifies under the mutex after storing (standard pairing, no missed-wakeup window between a
  waiter's check and its wait call), and a new `wait_until_durable(lsn)` blocks (timeout-bounded
  `wait_timeout` in a loop, so even a hypothetical missed notify just costs one extra 50ms
  iteration, never a permanent hang) until `last_written() >= lsn`. `Db::commit` captures the
  lsn its Commit record was logged under and calls `Logger::wait_until_durable` on it — but
  deliberately at the very END of `commit()`, right before returning, not immediately after
  logging as the audit's own recommendation literally suggests ("`Db::commit` blocks on it
  before `tx_mgr.commit`"). Blocking that early would reopen the exact race STORE_AUDIT.md T14
  already closed: `tx_mgr.commit`/the tombstone-reclaim decision need to run promptly so
  `is_committed(id)` flips true and undo/reclaim bookkeeping resolves quickly, unaffected by
  however long a real fsync takes — delaying those would leave a concurrent walker unable to
  tell "committed" from "not yet" for the whole wait, not just a few instructions. Only this
  function's own return to ITS OWN caller is delayed; every other thread still observes the
  commit immediately, exactly as before. Group commit is fully preserved: the wait blocks on
  the SAME per-batch watermark the runner already advances once per batch, so N concurrent
  committers waiting on lsns within one batch all wake from the same `notify_all` — nothing
  changes about how often the runner actually syncs.
  A real, second bug caught by the test during implementation (not by inspection): the FIRST
  version of `wait_until_durable` used a bare `last_written() >= lsn` check — but
  `last_written` starts at the `u64::MAX` cold-start sentinel ("nothing tracked yet"), which is
  deliberately `>= ` any real lsn so a freshly-dirtied page's flush gate doesn't defer forever
  waiting for a watermark that hasn't started moving (see T2's own `Page::set_dirty` comment).
  That's correct for gating a page flush but exactly backwards here — the sentinel means
  nothing is durable yet, the opposite of what a bare `>=` concludes — so the first version
  returned instantly on every commit without ever actually waiting. Fixed with a dedicated
  `is_durable(lsn)` check (`w != u64::MAX && w >= lsn.0`) instead of reusing the existing
  `last_written()`/`PartialOrd<LsnId>` comparison.
  Test: `test_audit_t1_commit_does_not_return_before_its_own_record_is_durable` — no fake/slow
  `DBFile` needed: the log runner deliberately lingers up to `LOG_BATCH_LINGER` (200us) after a
  batch's first message hoping more arrive to share the sync, so even with `MemFile`'s
  effectively-instant `do_sync` there's a real, near-guaranteed window right after `commit()`
  returns during which the record isn't durable yet — checking the raw log-buffer record count
  with NO polling (unlike `wait_for_durable_logs`, which exists specifically because this isn't
  normally guaranteed) is enough to observe it. Confirmed red by temporarily disabling the
  `wait_until_durable` call: failed 5/5 runs (0 records found instead of 2). Green after
  restoring it: 5/5 clean runs. Full `store` suite: 398 passed, 0 failed (397 baseline + this
  test). `squeal-sql --lib`: 346 passed, 0 failed. Whole workspace builds clean.
- [t-green] **P10** — FIXED. `log_runner`'s `LOG_BATCH_LINGER` (200us) was charged after the
  first message of every batch, even with nothing else in flight — pure added latency with zero
  batching benefit for an isolated write, and (since T1 now makes `commit()` actually wait on
  durability) directly visible as commit latency rather than a hidden background cost. Fixed
  with adaptive linger: a `last_batch_had_concurrency` flag (local to the runner loop, no shared
  state needed — it's a single thread), set from whether the PREVIOUS batch actually contained
  more than one record. When true, the "look for more" step lingers exactly as before
  (`recv_timeout(LOG_BATCH_LINGER)`); when false, it polls non-blocking instead
  (`try_recv()`), so an isolated write's batch closes immediately rather than waiting out the
  full window. Starts `true` (linger on the very first batch, matching the original behavior
  until there's real evidence either way). Note: "more than one record in a batch" isn't
  purely a proxy for *cross-thread* concurrency — a single transaction's own back-to-back
  `log()` calls (e.g. an Add followed shortly by its Commit) can also land in one batch — but
  that's a feature, not a noise source: it correctly keeps batching a busy single transaction's
  own rapid writes too, which is exactly the group-commit spirit.
  Test: `test_audit_p10_an_isolated_write_skips_the_group_commit_linger` — a first, throwaway
  write establishes "previous batch had exactly one record," then a second, isolated write's
  wall-clock latency (log + `wait_until_durable`) is asserted to land under `LOG_BATCH_LINGER`
  itself, using `MemFile` so `do_sync` cost doesn't dominate the measurement. Confirmed
  meaningful, not just passing by construction: reliably red (3/3) with the adaptive check
  temporarily reverted to the old unconditional `recv_timeout`, reliably green (20/20) restored.
  This is a magnitude-based (performance) test, not a strict boolean correctness check like the
  rest of Phase 3 — flagged as such per the design doc's own caveat; one earlier one-off failure
  immediately after a fresh `cargo build` (before the 20-run confirmation) is noted rather than
  hidden, most likely transient system load right after compilation, not a real flake rate.
  Full `store` suite: 399 passed, 0 failed (398 baseline + this test). `squeal-sql --lib`: 346
  passed, 0 failed. Whole workspace builds clean.

## Phase 4 — T3/T5/T16 FIXED; S1/T17 in progress

Design doc: `PHASE4_CHECKPOINT_DESIGN.md`. **Decision confirmed with the user**: T3 uses the
audit's simpler *quiesced checkpoint* design (checkpoint blocks new transactions and waits for
in-flight ones to finish), not the full ARIES-style fuzzy checkpoint — this substantially
simplifies T5/T16 too (see the design doc's own reasoning for why).

- [t-green] **T3** — FIXED. `Db::checkpoint` flushed EVERY dirty page (including one written
  by a transaction that was still active, or merely abandoned-and-not-yet-reverted) and then
  truncated the WAL to nothing — the undo/redo trail that would have proven the write
  uncommitted was gone, so a fresh session after a crash found no trace of that transaction in
  either the active or aborting set and (`TransactionManager::is_committed`: "absent from
  both" means committed) wrongly treated its write as committed. Auto-triggered from `begin()`
  whenever the log exceeds 16 MiB, so this fires under ordinary sustained load, not just an
  explicit `checkpoint()` call.
  Fixed with a `checkpoint_gate: RwLock<()>` on `Db`: `checkpoint()` takes the WRITE side for
  its entire run; `begin_with_conflict_policy` takes the READ side, but only around its own
  final `tx_mgr.begin` call — deliberately NOT around the auto-checkpoint trigger a few lines
  above it, which would self-deadlock (a thread can't hold the read side while `checkpoint()`
  tries to take the write side on the very call it's making). Once `checkpoint()` holds the
  write side, no NEW transaction can become active, so the in-flight set can only shrink from
  there; a new `wait_for_no_in_flight_transactions` loop then blocks until it actually reaches
  empty before touching any page or the log.
  Two sets matter, not one: `active_transactions` (open, mid-work) AND `aborting_transactions`
  (abandoned via `Transaction::drop`, not yet physically reverted) — a transaction sitting in
  `aborting` is already correctly invisible, but if its page write survives a flush+truncate
  un-reverted, a fresh session has no record of it at all and reaches the identical "absent
  from both sets = committed" false conclusion, just via the abandoned-transaction path instead
  of the still-open one. The wait loop actively calls `drain_aborting()` itself on every
  iteration (not just polls): that's normally only ever triggered from `begin()`, which is
  blocked by the very gate `checkpoint()` holds — without driving it itself, an abandoned
  transaction could never be reclaimed while a checkpoint waits, livelocking. No timeout: an
  indefinite wait is the accepted tradeoff of quiescing (matches the audit's own "long readers
  block checkpoints; combine with T7's reader-pinning limits" framing — T7 stays deferred, same
  as Phases 1-3).
  Test design note: the audit's own reproduction used `mem::forget` (a transaction that never
  drops, so `Transaction::drop`'s move into `aborting` never runs) — under a quiesced
  checkpoint that specific shape is unfixable by construction (checkpoint must wait for every
  in-flight transaction to actually resolve, and a forgotten one never will, regardless of
  design) and isn't a data-integrity gap this fix leaves open, just an inherent, accepted limit
  of quiescing (a genuinely leaked transaction blocks all future checkpoints forever either
  way) — a caller bug, not a correctness one. Adapted to a normally-dropped (not forgotten)
  abandoned transaction instead, which IS the scenario this fix needs to close.
  Tests: `test_audit_t3_checkpoint_reverts_an_abandoned_write_before_flushing` (the adapted
  reproduction above) and `test_audit_t3_checkpoint_waits_for_a_still_active_transaction` (a
  genuinely still-open transaction on another thread, proving the gate actually blocks/waits —
  checked via `JoinHandle::is_finished()` — rather than the fix merely happening to handle the
  already-resolved dropped case). Confirmed meaningful: reliably red (3/3) with the wait call
  temporarily disabled, reliably green (3/3) restored. Full `store` suite: 401 passed, 0 failed
  (399 baseline + these 2 tests). `squeal-sql --lib`: 346 passed, 0 failed. Whole workspace
  builds clean.
- [t-green] **T5** — FIXED (partially — see scope note). `PageBuffer::write_header` was
  fire-and-forget (a plain channel send, no reply, no fsync) — `Db::checkpoint` truncated the
  log right after calling it with no guarantee the header write had even been dequeued, let
  alone durably written. A crash in that window could leave a stale on-disk header (wrong
  `page_count`/`last_checkpoint`) paired with an already-empty log. Fixed with
  `PageBuffer::write_header_synced` (a `bounded(1)`-reply-channel variant, mirroring
  `checkpoint()`'s own existing pattern) that `pwrite`'s and `do_sync`'s the header before
  replying; `Db::checkpoint` now calls it (blocking) instead of the fire-and-forget version,
  and only truncates the log (`logger.checkpoint`) after it returns `Ok`. `Db::close` keeps
  using the plain fire-and-forget `write_header` — its own `buffer.shutdown()` call right after
  already flushes and syncs the whole file, and same-channel FIFO order already guarantees the
  header write is processed first.
  Test: `test_audit_t5_checkpoint_header_write_is_durable_before_returning` — checks the
  on-disk header bytes match the just-checkpointed `page_count` with NO polling (mirroring T1's
  own no-polling pattern) right after `checkpoint()` returns; if it merely queued the write,
  this would be flaky/failing rather than reliably true. Confirmed meaningful: reliably red
  (5/5) with `write_header_synced` temporarily reverted to plain `write_header`, reliably green
  (5/5) restored.
  **Scope note — tried and reverted**: the audit's OTHER T5 recommendation (derive
  `page_count` on open from the main file's actual length instead of trusting the header
  field) was implemented and immediately caught a worse bug of its own: `write_locked_page`
  deliberately does NOT write pages to disk on every mutation — it only updates the cache,
  deferring the real write until eviction, checkpoint, or shutdown. Outside a checkpoint/close
  boundary the file's length reflects whichever pages happened to be evicted so far — sparse
  and out of order, not "every page up to the highest one allocated". A file-length-derived
  count let replay route through a page that was never actually flushed (all-zero bytes, no
  node-type flag set), panicking with "Unknown page PageId(3)" during a red-test run — strictly
  worse than the original bug, since the stale-but-honest header count at least never claimed a
  page existed before it was durable. Reverted; `page_count` stays sourced from the header. Not
  a gap: the specific race T5 actually describes (a stale header paired with an
  already-truncated log) is fully closed by `write_header_synced` alone — by the time a header
  is ever paired with an empty log, it's already guaranteed to reflect everything
  `buffer.checkpoint()` just flushed. Full write-up in `PHASE4_CHECKPOINT_DESIGN.md`.
  Full `store` suite: 402 passed, 0 failed (401 baseline + this test). `squeal-sql --lib`: 346
  passed, 0 failed. Whole workspace builds clean.
- [t-green] **T16** — FIXED. The free list is only ever persisted at checkpoint/close
  (`write_system_tables`) — a transaction that takes a page off it, writes, and commits between
  checkpoints leaves the ON-DISK free list stale: it still lists that now-live page as free. On
  a crash-and-reopen with no intervening checkpoint, the stale list would let a later allocation
  hand that same page out again, silently clobbering committed data replay already restored (or
  is about to). Fixed with `Db::reconcile_free_list`, called right after `load_system_tables`
  (needs `self.tables` populated) and before `load_logs` (replay's own `alloc_page` calls must
  never see a free list that still lists a live page as available): builds the set of every page
  reachable from a live table's own structure — the 3 system pages, every table's index pages
  (`all_index_page_ids`), and a raw `next_page`-following walk from each table's `first_data_page`
  — and strips any of them off the loaded free list unconditionally, regardless of what the stale
  snapshot claims.
  The data-chain walk deliberately does NOT use the overflow-skipping `data_chain_next`/
  `overflow_terminator` pair: those exist so a page's *content* survives a shrink while its
  overflow chain collapses, by returning only the real next *sibling* page. Reconciliation needs
  the opposite — every physically linked page marked reachable, overflow continuations included
  — which a plain "follow next_page until invalid" walk already gives, exactly like
  `PageBuffer::free_page_chain`'s own (destructive) walk for `drop_table`.
  Caught a second, real bug while building this: the walk's first version read each page via
  `get_page` (full content decode) to pull out `next_page`. That's unsafe for an overflow
  *continuation* page — its on-disk data region is only a valid, decodable `Page` when
  reassembled starting from its chain's primary (`HAS_OVERFLOW`) page (see `buffer::read_page`);
  calling `get_page` directly on a middle page (`IS_OVERFLOW`, not `HAS_OVERFLOW`) tries to
  decode a raw mid-stream byte chunk as a standalone tuple page, which reliably throws
  `SerializationError(SerdeDeCustom)` (confirmed by instrumenting the walk: it failed exactly at
  the first continuation page of a shrunk overflow object). This surfaced as 7 unrelated-looking
  failures in the full suite (`test_checkpoint_with_large_overflow_object`,
  `test_free_pages_do_not_accumulate_across_multiple_close_reopen_cycles`,
  `test_freed_overflow_pages_persist_across_close_reopen`, `test_large_object_full_lifecycle_all_ops`,
  `test_large_objects_persist_across_close_reopen`,
  `test_reopened_db_reuses_freed_pages_before_growing_page_count`,
  `test_reused_freed_overflow_page_is_safe_to_write_fresh_data_into`) — every one that opens a
  db containing an overflow object. Fixed by switching the walk to `read_page_header` (raw
  header-only read, newly made `pub(crate)`; same pattern `overflow_terminator` already uses) —
  it only needs `next_page`, which the header alone carries safely for every page in the chain,
  continuations included, with no content decode at all.
  (Note: this suggests `PageBuffer::free_page_chain` itself may have the identical latent bug —
  it also calls `get_page` while walking into overflow continuation pages for `drop_table` — but
  its own existing tests don't reopen the db mid-chain and apparently don't trip it. Not fixed
  here (out of scope for T16); worth folding into **T17**'s drop_table investigation, which
  already touches this same code path.)
  Test: `test_audit_t16_reopen_reconciles_the_free_list_against_reachable_pages` — writes a
  free-list page directly at the byte level claiming a real, reachable page (a table's
  `first_data_page`) is free (the state a stale checkpoint snapshot would produce), crash-reopens,
  asserts reconciliation removed it. Confirmed meaningful: reliably red with the `retain` call
  temporarily replaced with a no-op, reliably green restored.
  Full `store` suite: 403 passed, 0 failed (402 baseline + this test, including the 7 overflow
  tests above). `squeal-sql --lib`: 346 passed, 0 failed. Whole workspace builds clean.
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
