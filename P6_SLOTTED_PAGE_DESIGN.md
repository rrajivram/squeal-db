# STORE_AUDIT.md P6 — Slotted pages (scoping/design pass)

Companion to `T4_S2_WAL_DESIGN.md`, `T2_T1_P10_DURABILITY_DESIGN.md`, and
`PHASE4_CHECKPOINT_DESIGN.md`. Same process: design first, `audit-progress.md` updated once
implemented, one commit per landed piece — except this document is **scoping/design only, per
explicit request**. No code changes accompany it. It exists so implementation (a separate,
future pass) starts from an already-verified plan instead of re-deriving one from scratch.

This continues the session that has already fixed and benchmarked P7, P9 (first half), P4, and
P5 — cumulative **+39.5%** end-to-end throughput on the stress harness (`store/benches/
BASELINE.md`). P6 is the audit's own "single biggest structural win available but also the
largest change" (STORE_AUDIT.md:676-687): every page flush re-serializes the WHOLE page from
scratch (`AnyTuplePage::to_bytes` postcard-encodes every tuple every time, even for a one-row
change to a 16 KiB page), and every page load does a full eager decode of every tuple into a
`BTreeMap` before any query can run. The fix is a slotted-page layout — fixed header, slot/
offset array, raw tuple bytes — where `add`/`replace`/`remove` are byte moves within an
already-resident buffer, `to_bytes` is close to a memcpy, and `from_bytes` is a bounds check,
not a decode.

## The Parquet question, answered first

The user is planning to eventually add Parquet/columnar support (shape undecided) and asked
that P6 not foreclose it. It doesn't, for a specific reason worth being explicit about:
`PageTuple` (`store/src/pages/mod.rs:14-60`, the trait `AnyTuplePage`/`FixedTuplePage`/
`RunPage` all implement) is fundamentally row-oriented — `add(one tuple)`, `get(id) -> one
tuple`, id-keyed. That's the right shape for OLTP-style point lookups/updates, and it's also
the wrong shape to force Parquet-style columnar storage through even in principle — column
chunks are bulk-loaded, dictionary/RLE/delta-encoded, and read in whole-column sweeps, not
touched one row at a time. So a future columnar feature was never going to be "a new
`PageTuple` impl" regardless of what P6 does; it'll be its own subsystem — most likely a new
`TableType` variant (`store/src/table.rs:7-10` currently has `BtreeTable`/`Index` as a plain,
open `enum`) with its own storage/read path, quite possibly bypassing `PageBuffer`'s
fixed-page-size/WAL/dirty-tracking model entirely, since Parquet's own footer/row-group/
column-chunk layout doesn't naturally fit a fixed page size anyway. This is how real hybrid
engines do it — a distinct storage layer, not a variant of the row-page format.

The concrete takeaway, confirmed by reading the code rather than assumed: P6 stays entirely
inside the existing `PageContentRegistry`/`PageContentKind`/`PageTuple` extension seam.
`Page::new` (`store/src/page.rs:314-323`) is the *only* place content-kind gets selected for a
new page (`new_with_content`, `store/src/page.rs:326+`, is the generic entry point everything
— including `new_run`'s `RunPage` — actually funnels through); every caller in `bplustree.rs`/
`buffer.rs` just calls `Page::new_data`/`new_indexed`, oblivious to the concrete type. P6
touches zero lines in `TableType`, `buffer.rs`'s I/O/checksum/overflow-chain machinery, or the
WAL. That keeps the door fully open for a columnar `TableType` later, designed on its own
terms, whenever its shape becomes clearer.

## Design

### New `PageTuple` impl: `SlottedPage` (`store/src/pages/slotted.rs`, new file)

Data region layout (the `page_data_size`-byte area `PageTuple::to_bytes()`/`from_bytes()`
already treat as one opaque, self-contained buffer — confirmed safe to blind-byte-split across
physical pages for the overflow-chain case, since `buffer.rs`'s page write path reassembles one
contiguous buffer before ever calling `from_bytes`, exactly like it does for `AnyTuplePage`
today):

- **Slot directory**, growing from the front: `slot_count: u32`, then `slot_count` fixed-size
  entries `{offset: u32, len: u32, tombstone: bool}`, kept in ascending `DBIdType::cmp` order.
  No sort key is stored redundantly in the slot — `DBIdType::Rec`'s `Ord` is structural (not
  hash-based, per a prior fix that made range queries over multi-key ids work — see
  `anytuple.rs:16-28`'s own header comment, confirmed by reading it), so there's no universal
  fixed-size digest that preserves true order for every key shape. Binary search instead
  decodes the *candidate* tuple's `id` at each comparison step — O(log N) decodes per lookup,
  not the O(1) `AnyTuplePage`/`BTreeMap` gives once in-memory, but nowhere near the O(N)
  `page.iter()` P5 already removed from the hot routing path, and it avoids the fixed-vs-
  variable-key-width problem entirely.
- **Tuple bytes**, growing from the back, packed contiguously — reuses `Tuple`'s existing
  postcard wire format unchanged (`to_allocvec(&tuple)`/`from_bytes::<Tuple>`, same calls
  `anytuple.rs` already makes); only the *container* layout changes, not the per-tuple
  encoding.
- Free space in the middle.

Operation shapes:
- `from_bytes`: parses only the slot directory (fixed-size records — cheap, O(N) in the
  trivial "read N small headers" sense, not O(N) tuple decodes). Tuple bytes stay raw/
  undecoded until actually requested.
- `to_bytes`: the page keeps one resident byte buffer, mutated in place by `add`/`replace`/
  `remove`; `to_bytes()` returns/clones that buffer directly. For the common "nothing changed
  since load" case this is a plain copy of already-correct bytes — no per-tuple work at all,
  which is the actual fix for "one-row change rewrites a 16 KiB page."
- `get`/`successor` (the method P5 added — `store/src/pages/mod.rs:47-58` — exercised by
  `route_to_leaf`/`remove_index_entry`/`update_index_entry`/`insert_recursive` in
  `bplustree.rs`): binary search over the sorted slot directory.
- `add`/`replace`/`remove`: binary-search the insertion/target point, `memmove` the (small,
  fixed-size) slot directory to make room/close a gap, write/tombstone tuple bytes at the
  current heap pointer. `replace` overwrites in place when the new tuple fits in the old slot's
  span, else falls back to remove+add. Compaction (reclaiming tombstoned tuple-byte space) is
  lazy — only run when a following `add`/`replace` can't otherwise fit, not on every mutation.

Two correctness requirements confirmed load-bearing by reading the code (not just "contains the
right tuples" — actual behavioral contracts other code depends on):
1. `values()`/`keys()` must yield strictly ascending `DBIdType::cmp` order — `bplustree.rs`'s
   navigation/split logic treats this as canonical sort order (`anytuple.rs:16-20`'s own
   comment: "this map's iteration order is what the B+ tree's navigation/split logic treats as
   'sorted by DBIdType::cmp'"; enforced today by `BTreeMap`'s iteration order).
2. `DBIdType::cmp`-tied-but-`PartialEq`-distinct ids (Int hash collisions; `Rec`'s documented
   structural ties) must both stay independently reachable — `AnyTuplePage` handles this via a
   `Vec<Tuple>` bucket per map key (`anytuple.rs:29-31`); `SlottedPage` handles it as a short
   run of adjacent, cmp-equal slots, linearly disambiguated by `PartialEq` within that run.

### Capacity-accounting fix needed alongside this

`Page::can_store`/`usable_data_size` (`store/src/page.rs:561-562`, `575-583`) are agnostic to
`PageTuple`'s internal encoding today — `page_used_size` is a `Page`-level running sum of
`Tuple::size()` (the *logical* tuple size), compared against `page_data_size -
USABLE_DATA_MARGIN` (a flat 16-byte slack constant, `page.rs:163`). That's fine for
`AnyTuplePage`/`FixedTuplePage`, whose postcard container overhead is already close to what
`Tuple::size()` implies. `SlottedPage` adds a real per-tuple fixed cost (the slot directory
entry, ~9 bytes) that `page_used_size` never sees — invisible to `can_store`, and bigger than
the existing 16-byte *whole-page* margin once more than one or two tuples are on a page.

Recommended fix, mirroring the existing precedent (`FixedTuplePage::add`/`replace` already do
their own precise `tuple.size() > self.tuple_size` check *beyond* `Page::can_store`'s coarse
one): keep `Page::can_store` as the cheap, coarse pre-filter unchanged, and make `SlottedPage`'s
own `add`/`replace` the authoritative check — attempt the write against its actual resident
buffer's free space, returning `StoreError::PageCapacityError` if it genuinely doesn't fit, the
same way a real "page is full" condition is already surfaced elsewhere. No changes needed to
`Page`'s public capacity API.

### `Page::from_bytes` has two dispatch paths — resolved, not just flagged

Initial concern: `page.rs`'s `impl From<PageDto> for Page` might be a *second*,
registry-bypassing decode path, separate from the registry-aware `Page::from_bytes(bytes,
&registry)` that `buffer.rs`'s real page-read path actually calls. Read both directly
(`page.rs:886-926`): `From<PageDto>` carries its own comment explaining exactly this — it
exists *only* to satisfy `#[serde(from = "PageDto")]` on `Page`'s derive, nothing in the crate
actually calls Page's generic `Deserialize` path, and it already `panic!()`s on any content
kind other than the two original built-ins, **including `RUN_TUPLE`** (added later, alongside
`crate::run::Run`, and never given an arm here either). So this is pre-existing, confirmed
legacy/dead-for-real-loads plumbing, not a live second path — `SLOTTED_TUPLE` doesn't need an
arm here any more than `RUN_TUPLE` currently has one. No action item; noted so implementation
doesn't waste time re-verifying it.

## Rollout plan (staged, test-first — same methodology as P2–P5 this session)

1. Implement `SlottedPage` in isolation (`store/src/pages/slotted.rs`) with its own unit test
   suite mirroring `anytuple.rs`'s file-for-file, including the two ordering/tie-break tests
   above. No wiring into `Page` yet.
2. Register it as a new, additive `PageContentKind` (e.g. `SLOTTED_TUPLE = PageContentKind(3)`)
   in `PageContentRegistry::builtin()` (`store/src/pages/content.rs:60-83`) — zero risk to
   `ANY_TUPLE`/`FIXED_TUPLE`/`RUN_TUPLE`.
3. Confirm meaningful the cheap way: temporarily swap `Page::new`'s default branch
   (`page.rs:314-323`, the `else` arm that currently builds `AnyTuplePage`) to construct
   `SlottedPage`, and run the full existing `store`/`squeal-sql` suites unmodified — this
   exercises it through real B+tree inserts/splits/scans, overflow chains, checkpoint/recovery,
   and the stress harness for free, exactly like every fix this session.
4. Benchmark pre/post: (a) a direct microbenchmark of `to_bytes`/`from_bytes` cost for a page
   with many tuples and one single-tuple mutation — the audit's own "one-row change rewrites a
   16 KiB page" scenario, expected to go from O(page size) to ~O(1); (b) the full
   `examples/stress` E2E harness, same convention as every prior fix (`store/benches/
   BASELINE.md`).
5. Once proven, make the `Page::new` swap permanent.
6. Decide `AnyTuplePage`/`FixedTuplePage`'s fate last: likely keep them registered (cheap,
   already correct and tested, no forcing function to delete working code) but no longer
   constructed by default — mirrors how `RunPage` already coexists as a third kind.

## Verification (once implemented — not part of this scoping pass)

- `cargo test -p store --lib -- --test-threads=1` and `cargo test -p squeal-sql --lib` green
  throughout, at every stage above (isolated `SlottedPage` tests first, then the full suite
  once wired in as the default).
- `cargo build --workspace --tests` clean.
- New `#[ignore]`d throwaway benchmarks (same convention as this session's other perf fixes) for
  the to_bytes/from_bytes cost claim specifically, plus the standard `examples/stress
  --threads 16 --ops 20000 --backend mem` E2E comparison recorded in `BASELINE.md`.
- Update `audit-progress.md`'s P6 entry (currently `[ ]`, Phase 7) and `STORE_AUDIT.md`
  cross-references once implemented.

## Status — implemented, benchmarked, wiring reverted (design itself missed one axis)

This *was* implemented (`store/src/pages/slotted.rs`, full test suite, registered in
`PageContentRegistry` as `SLOTTED_TUPLE`) and briefly made `Page::new`'s default for data
pages, in a later pass than this document's own scoping-only status originally described.

Integration testing (the full `store`/`squeal-sql` suites — this session's standard "confirm
meaningful through real usage" gate) caught a real bug: `SlottedPage::replace`'s remove-then-
add fallback deleted the old slot *before* confirming the new (bigger) tuple would fit, so a
capacity failure silently dropped the row forever while reporting "nothing happened." Fixed
(see `slotted.rs`'s own comment on the fix, and `audit-progress.md`'s P6 entry for the full
story) and covered by a dedicated regression test.

Then benchmarked — and reverted. This design document reasoned carefully about **load/flush**
cost (the audit's own framing: "a one-row change rewrites and re-encodes a 16 KiB page") but
never weighed **repeated in-memory access** cost for an already-cached page, which turned out
to be the dominant axis: `AnyTuplePage` decodes once, on load, into a live `BTreeMap`, so every
later access is a free comparison; `SlottedPage` decodes nothing on load, but its O(log N)
binary search fully decodes a whole candidate `Tuple` on *every* comparison of *every* access,
paying that cost over and over rather than once. Measured ~22x slower repeated `get()` on an
already-loaded page, and a ~45-50% end-to-end stress-harness regression (107K → 54-60K ops/s)
— that gap alone, not any remaining correctness issue, is why `Page::new` was reverted to
building `AnyTuplePage`/`FixedTuplePage` as before.

Disposition, per explicit direction: `SlottedPage` is kept, not deleted — a real, working,
fully-tested design exploration, documented (its own top comment, `content.rs`'s
`SLOTTED_TUPLE` doc comment) with exactly why it's dormant and the one identified-but-
unattempted path back (decode only the `id` field during binary search — `Tuple`'s first
declared struct field, self-delimiting under postcard's declaration-order serialization —
instead of the whole `Tuple`). `bplustree.rs`'s `write_data`/`update`/`update_checked`, which
had gained `PageCapacityError`-fallback handling for `SlottedPage`'s stricter capacity
semantics, were reverted to their pre-P6 form along with `page.rs`'s `Page::new`.

`audit-progress.md`'s P6 line reflects this outcome; `STORE_AUDIT.md`'s own P6 item stays
open/deferred, since the underlying finding (whole-page re-serialization on flush) remains
real and unaddressed by anything currently active.
