// STORE_AUDIT.md P6 — slotted pages. Full design writeup:
// `/P6_SLOTTED_PAGE_DESIGN.md` (repo root). Summary of what this buys over
// `AnyTuplePage`: AnyTuplePage's `to_bytes()` postcard-re-encodes EVERY tuple
// on the page on every flush, even for a one-row change, and its
// `from_bytes()` decodes every tuple into a `BTreeMap` before any query can
// run. A slotted page instead keeps one resident byte buffer that `add`/
// `replace`/`remove` mutate in place (a slot-directory memmove plus a raw
// byte write), so `to_bytes()` is a clone of already-correct bytes and
// `from_bytes()` only ever parses the (small, fixed-size) slot directory —
// tuple bytes stay raw/undecoded until actually requested.
//
// ## Why this isn't the default (kept, but not wired into `Page::new`)
//
// This type is fully implemented, tested (including the two load-bearing
// ordering/tie-break contracts `AnyTuplePage` also has to satisfy — see
// below), and was briefly wired in as `Page::new`'s default. It was reverted
// after measuring a real, reproducible ~45-50% END-TO-END throughput
// regression on the stress harness (107K → 54-60K ops/s) — the opposite of
// what P6 set out to do. Root cause, confirmed by a direct microbenchmark
// (`bench_repeated_get_on_an_already_loaded_page`, this file and
// `anytuple.rs`'s matching one): `AnyTuplePage` decodes every tuple once, on
// `from_bytes` (page load), into a live `BTreeMap<DBIdType, Vec<Tuple>>` —
// every subsequent `get`/`add`/`replace`/`successor` for as long as that page
// stays cache-resident is then a free, no-decode in-memory comparison.
// `SlottedPage` inverts that trade: `from_bytes` decodes nothing (just the
// slot directory), but its O(log N) binary search fully `postcard`-decodes a
// *whole* candidate `Tuple` (id, txn_id, pre_lsn, data, flags — not just the
// id it actually needs to compare) at every single comparison, on *every*
// access, for as long as the page is in memory — never amortized the way
// AnyTuplePage's one-time decode is. Measured: ~23.3M gets/s (AnyTuplePage)
// vs. ~1.07M gets/s (SlottedPage) on an identical 200-tuple page — ~22x.
// For a page that gets touched many times while resident (the common case,
// especially given this session's own P2/P3 caching work, which keeps hot
// pages cached far more effectively than the audit's original "one-row
// change re-serializes a 16 KiB page" framing accounted for), that repeated-
// access cost dominates completely and swamps the real, genuine win on
// load/flush cost this type does deliver.
//
// A viable path back to making this the default would be decoding only the
// `id` field during binary search (it's `Tuple`'s first declared struct
// field, and postcard serializes struct fields in declaration order, so it's
// self-delimiting at the front of each candidate's bytes) instead of the
// whole `Tuple` — not attempted here. Kept in the tree, registered in
// `PageContentRegistry` (`content.rs`'s `SLOTTED_TUPLE` kind), and covered by
// its own full test suite below as a preserved, working design exploration —
// just not the active choice `Page::new` reaches for.
//
// ## On-disk layout (the buffer this struct owns and returns verbatim from
// `to_bytes()`)
//
// ```text
// [ slot_count: u32 ][ declared_capacity: u32 ][ slot 0 ][ slot 1 ] ... [ free space ] ... [ tuple bytes (packed from the end) ]
// ```
//
// - `slot_count`/`declared_capacity` are a fixed 8-byte header (see
//   `HEADER_BYTES`). `declared_capacity` is explained below.
// - The slot directory grows from the front, one fixed-size `{offset: u32,
//   len: u32}` entry per tuple (see `SlotEntry`/`SLOT_ENTRY_BYTES`), kept in
//   ascending `DBIdType::cmp` order at all times — `values()`/`keys()`
//   iterate it directly, and `bplustree.rs`'s navigation/split logic treats
//   that order as canonical (see `AnyTuplePage`'s own doc comment on `data`,
//   which this must match).
// - Tuple bytes grow from the back, packed contiguously, in `Tuple`'s
//   existing postcard wire format — only the *container* changes here, not
//   the per-tuple encoding.
// - The `declared_capacity: u32` header field is what makes `Page::new`'s
//   escape hatch ("an empty page always accepts a tuple bigger than a whole
//   page" — see `Page::can_store`) round-trip correctly through eviction.
//   That's the ONLY way a page here is ever allowed to hold more bytes than
//   a normal single page's worth (`handle_large_page_size` in buffer.rs
//   refuses to build an overflow chain for a multi-tuple page — see its own
//   comment — so this only ever happens for a lone oversized tuple on an
//   otherwise-empty page). `grow_to_fit`/`shrink_to_normal` below resize the
//   *actual* buffer to fit that one tuple and back again, while
//   `declared_capacity` (the original, normal single-page size) stays fixed
//   in the header the whole time — so a page that grew, got serialized,
//   evicted, and reloaded still knows what size to shrink back to once the
//   oversized tuple is removed. Without this, a page that briefly grew and
//   was then emptied would stay oversized forever, and buffer.rs's plain
//   (non-overflow) write path would reject it on the next flush
//   (`bytes.len() > page_size` → `PageTransientlyInconsistent`).
//
// ## Two deliberate deviations from this feature's original design sketch
//
// 1. **No per-slot tombstone flag.** The original sketch had slot entries
//    carry a `tombstone: bool` so `remove` could mark-and-defer. Implemented
//    simpler: `remove` always immediately closes the gap in the (cheap,
//    fixed-width) slot directory via a memmove — there is never a
//    tombstoned entry sitting in the directory to skip over during a binary
//    search or a `values()` scan. What IS lazy, per the design, is
//    reclaiming the dead HEAP bytes a removed (or shrunk-in-place replaced)
//    tuple leaves behind (`dead_heap_bytes`) — `compact()` only runs when a
//    following `add`/`replace` can't otherwise fit.
// 2. **Ordering (tie-break) semantics**: `DBIdType::cmp` can say `Equal` for
//    ids that are `PartialEq`-distinct (Int hash collisions; `Rec`'s
//    documented structural ties — see `tuple.rs`'s own comment on
//    `DBIdType::cmp`). Unlike `AnyTuplePage`'s `Vec<Tuple>` bucket per exact
//    map key, this stores tied entries as an ordinary run of adjacent slots
//    (no special bucketing) and disambiguates within that run by decoding
//    and comparing via `PartialEq` — see `find_exact`.
//
// ## Complexity
//
// `add`/`replace`/`remove`/`get`/`contains`/`successor` are all O(log N)
// *tuple decodes* (binary search over the slot directory, decoding the
// candidate at each comparison step — there's no way to avoid this without
// a fixed-width, universally order-preserving key encoding, which
// `DBIdType::Rec`'s structural `Ord` rules out, see the design doc) plus an
// O(N) but cheap *byte* memmove of the slot directory (not tuple decodes) to
// open/close the gap. `to_bytes()` for the common "nothing changed since
// load" case is a plain `Vec<u8>` clone — no per-tuple work at all.

use postcard::{from_bytes, to_allocvec};

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

const SLOT_COUNT_BYTES: usize = 4;
const DECLARED_CAPACITY_BYTES: usize = 4;
// See this file's own top comment on declared_capacity.
const HEADER_BYTES: usize = SLOT_COUNT_BYTES + DECLARED_CAPACITY_BYTES;
// offset: u32 LE + len: u32 LE. No tombstone bit — see this file's own
// top comment on why (deviation #1).
const SLOT_ENTRY_BYTES: usize = 8;

#[derive(Debug, Clone, Copy)]
struct SlotEntry {
    offset: u32,
    len: u32,
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().unwrap())
}

fn write_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn dir_offset(idx: usize) -> usize {
    HEADER_BYTES + idx * SLOT_ENTRY_BYTES
}

fn slot_entry_at(buf: &[u8], idx: usize) -> SlotEntry {
    let at = dir_offset(idx);
    SlotEntry {
        offset: read_u32(buf, at),
        len: read_u32(buf, at + 4),
    }
}

fn write_slot_entry_at(buf: &mut [u8], idx: usize, e: SlotEntry) {
    let at = dir_offset(idx);
    write_u32(buf, at, e.offset);
    write_u32(buf, at + 4, e.len);
}

// Builds a fresh, empty buffer of the given physical length, stamped with
// slot_count=0 and the given declared_capacity. Shared by `new`, `clear`,
// and `shrink_to_normal` (all three want "a blank page of some length").
fn blank_buffer(len: usize, declared_capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len.max(HEADER_BYTES)];
    write_u32(&mut buf, 0, 0);
    write_u32(&mut buf, SLOT_COUNT_BYTES, declared_capacity as u32);
    buf
}

#[derive(Debug)]
pub struct SlottedPage {
    buf: Vec<u8>,
    // Lowest byte offset currently occupied by any live tuple's bytes (the
    // heap grows downward from buf.len()); buf.len() itself when empty.
    // Cached rather than recomputed per-op so add/remove/replace stay
    // O(log N) decodes + O(1) arithmetic, not an O(N) directory rescan each
    // time. Kept in sync by every mutating method below.
    heap_start: usize,
    // Bytes orphaned within [heap_start, buf.len()) by a remove or a
    // shrunk-in-place replace — real, allocated heap space that's no
    // longer pointed to by any slot, only reclaimed by compact(). See this
    // file's own top comment, deviation #1.
    dead_heap_bytes: usize,
}

impl Clone for SlottedPage {
    fn clone(&self) -> Self {
        Self {
            buf: self.buf.clone(),
            heap_start: self.heap_start,
            dead_heap_bytes: self.dead_heap_bytes,
        }
    }
}

// Structural equality would compare raw bytes, which differ across two
// pages holding identical tuples but different insert/remove history
// (fragmentation, heap packing order) — compare by decoded content instead,
// the same semantic AnyTuplePage's PartialEq gets for free from BTreeMap.
impl PartialEq for SlottedPage {
    fn eq(&self, other: &Self) -> bool {
        self.values().unwrap_or_default() == other.values().unwrap_or_default()
    }
}

impl SlottedPage {
    pub(crate) fn new(capacity: usize) -> Self {
        let buf = blank_buffer(capacity, capacity);
        let heap_start = buf.len();
        Self {
            buf,
            heap_start,
            dead_heap_bytes: 0,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < HEADER_BYTES {
            return Err(StoreError::UnknownError(format!(
                "SlottedPage::from_bytes: buffer too small ({} bytes, need at least {HEADER_BYTES})",
                bytes.len()
            )));
        }
        let buf = bytes.to_vec();
        let slot_count = read_u32(&buf, 0) as usize;
        let dir_end = dir_offset(slot_count);
        if dir_end > buf.len() {
            return Err(StoreError::UnknownError(format!(
                "SlottedPage::from_bytes: slot directory ({slot_count} entries, {dir_end} bytes) \
                 exceeds buffer ({} bytes)",
                buf.len()
            )));
        }
        let mut heap_start = buf.len();
        let mut live_bytes: usize = 0;
        for idx in 0..slot_count {
            let e = slot_entry_at(&buf, idx);
            let off = e.offset as usize;
            let len = e.len as usize;
            if off < dir_end || off.saturating_add(len) > buf.len() {
                return Err(StoreError::UnknownError(format!(
                    "SlottedPage::from_bytes: slot {idx} out of bounds \
                     (offset={off}, len={len}, buffer={} bytes, dir_end={dir_end})",
                    buf.len()
                )));
            }
            heap_start = heap_start.min(off);
            live_bytes += len;
        }
        let dead_heap_bytes = (buf.len() - heap_start).saturating_sub(live_bytes);
        Ok(Self {
            buf,
            heap_start,
            dead_heap_bytes,
        })
    }

    fn slot_count(&self) -> usize {
        read_u32(&self.buf, 0) as usize
    }

    fn set_slot_count(&mut self, n: usize) {
        write_u32(&mut self.buf, 0, n as u32);
    }

    fn declared_capacity(&self) -> usize {
        read_u32(&self.buf, SLOT_COUNT_BYTES) as usize
    }

    fn dir_end(&self) -> usize {
        dir_offset(self.slot_count())
    }

    fn slot(&self, idx: usize) -> SlotEntry {
        slot_entry_at(&self.buf, idx)
    }

    fn set_slot(&mut self, idx: usize, e: SlotEntry) {
        write_slot_entry_at(&mut self.buf, idx, e);
    }

    fn decode_at(&self, idx: usize) -> Result<Tuple, StoreError> {
        let e = self.slot(idx);
        let bytes = &self.buf[e.offset as usize..e.offset as usize + e.len as usize];
        Ok(from_bytes::<Tuple>(bytes)?)
    }

    // First index in [0, slot_count) whose decoded id is NOT Less than
    // `id` (i.e. >= id), or slot_count if every entry is < id. O(log N)
    // tuple decodes, one per comparison step — see this file's own top
    // comment on why a decode is unavoidable here.
    fn lower_bound(&self, id: &DBIdType) -> Result<usize, StoreError> {
        self.bound(id, false)
    }

    // First index whose decoded id is strictly Greater than `id` — used
    // directly by `successor`.
    fn upper_bound(&self, id: &DBIdType) -> Result<usize, StoreError> {
        self.bound(id, true)
    }

    fn bound(&self, id: &DBIdType, strict_greater: bool) -> Result<usize, StoreError> {
        let mut lo = 0usize;
        let mut hi = self.slot_count();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cand = self.decode_at(mid)?;
            let ord = cand.id.cmp(id);
            let go_right = if strict_greater {
                ord != std::cmp::Ordering::Greater
            } else {
                ord == std::cmp::Ordering::Less
            };
            if go_right {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    // The exact slot for `id`, disambiguating a same-cmp run via PartialEq
    // (see this file's own top comment, deviation #2). Returns the run
    // bounds too so callers that also need them (add's duplicate check,
    // replace/remove's target) don't pay for a second pair of binary
    // searches.
    fn find_exact_in(&self, id: &DBIdType, lo: usize, hi: usize) -> Result<Option<usize>, StoreError> {
        for idx in lo..hi {
            if self.decode_at(idx)?.id == *id {
                return Ok(Some(idx));
            }
        }
        Ok(None)
    }

    fn find_exact(&self, id: &DBIdType) -> Result<Option<usize>, StoreError> {
        let lo = self.lower_bound(id)?;
        let hi = self.upper_bound(id)?;
        self.find_exact_in(id, lo, hi)
    }

    fn contiguous_free(&self) -> usize {
        self.heap_start.saturating_sub(self.dir_end())
    }

    fn fits_without_compaction(&self, tuple_len: usize) -> bool {
        self.contiguous_free() >= SLOT_ENTRY_BYTES + tuple_len
    }

    // Repacks every live tuple's bytes tightly against the end of the
    // buffer, discarding all dead_heap_bytes fragmentation in one pass.
    // Slot count/order/ids are untouched — only each slot's `offset`
    // changes — so any index computed before calling this (e.g. an
    // insertion point from lower_bound) stays valid after it.
    fn compact(&mut self) {
        let n = self.slot_count();
        let mut entries: Vec<(usize, Vec<u8>)> = Vec::with_capacity(n);
        for idx in 0..n {
            let e = self.slot(idx);
            entries.push((
                idx,
                self.buf[e.offset as usize..e.offset as usize + e.len as usize].to_vec(),
            ));
        }
        let mut cursor = self.buf.len();
        for (idx, bytes) in entries {
            let len = bytes.len();
            cursor -= len;
            self.buf[cursor..cursor + len].copy_from_slice(&bytes);
            self.set_slot(
                idx,
                SlotEntry {
                    offset: cursor as u32,
                    len: len as u32,
                },
            );
        }
        self.heap_start = cursor;
        self.dead_heap_bytes = 0;
    }

    // Only valid to call when slot_count() == 0 — see this file's own top
    // comment on declared_capacity for why this is the ONLY legitimate way
    // a page grows past a single page's worth: Page::can_store's "an empty
    // page always accepts" escape hatch, exercised for exactly one
    // oversized tuple (handle_large_page_size in buffer.rs refuses to
    // build an overflow chain for a multi-tuple page).
    fn grow_to_fit(&mut self, tuple_len: usize) {
        debug_assert_eq!(self.slot_count(), 0, "grow_to_fit is only for an empty page");
        let declared_capacity = self.declared_capacity();
        let new_len = HEADER_BYTES + SLOT_ENTRY_BYTES + tuple_len;
        self.buf = blank_buffer(new_len, declared_capacity);
        self.heap_start = self.buf.len();
        self.dead_heap_bytes = 0;
    }

    // Undoes grow_to_fit once the page empties out again, so a page that
    // briefly held one oversized tuple doesn't stay oversized forever (see
    // this file's own top comment). Only called from remove(), and only
    // when slot_count just reached 0.
    fn shrink_to_normal(&mut self) {
        let declared_capacity = self.declared_capacity();
        if self.buf.len() == declared_capacity {
            return;
        }
        self.buf = blank_buffer(declared_capacity, declared_capacity);
        self.heap_start = self.buf.len();
        self.dead_heap_bytes = 0;
    }

    // Ensures room for one more slot entry + `tuple_len` heap bytes,
    // trying (in order): the contiguous gap as-is, compacting away dead
    // heap bytes, and — only for an empty page — growing the buffer
    // outright. Returns PageCapacityError only when none of those apply:
    // a genuinely full, non-empty page. This is the authoritative
    // capacity check for this page kind — see the design doc's "capacity
    // accounting fix" section on why Page::can_store's coarse, tuple-size-
    // only check can't see this format's per-slot directory overhead.
    fn ensure_room(&mut self, tuple_len: usize) -> Result<(), StoreError> {
        if self.fits_without_compaction(tuple_len) {
            return Ok(());
        }
        if self.slot_count() > 0 {
            self.compact();
            if self.fits_without_compaction(tuple_len) {
                return Ok(());
            }
            return Err(StoreError::PageCapacityError);
        }
        self.grow_to_fit(tuple_len);
        Ok(())
    }

    // Inserts already-encoded tuple bytes at directory index `idx`,
    // shifting later entries right by one slot width and allocating heap
    // space at the current frontier. Caller must have already called
    // ensure_room(encoded.len()) — this does no capacity checking itself.
    fn insert_at(&mut self, idx: usize, encoded: &[u8]) {
        let n = self.slot_count();
        let len = encoded.len();
        let shift_start = dir_offset(idx);
        let shift_len = (n - idx) * SLOT_ENTRY_BYTES;
        if shift_len > 0 {
            self.buf
                .copy_within(shift_start..shift_start + shift_len, shift_start + SLOT_ENTRY_BYTES);
        }
        let new_offset = self.heap_start - len;
        self.buf[new_offset..new_offset + len].copy_from_slice(encoded);
        self.heap_start = new_offset;
        self.set_slot(
            idx,
            SlotEntry {
                offset: new_offset as u32,
                len: len as u32,
            },
        );
        self.set_slot_count(n + 1);
    }

    // Deletes the directory entry at `idx` (closing the gap via a
    // memmove — see this file's own top comment, deviation #1) and marks
    // its heap bytes dead. Does NOT shrink the buffer even if this empties
    // the page — callers that care (remove()) do that separately, since
    // replace()'s remove+add fallback never wants to shrink mid-operation.
    fn delete_slot(&mut self, idx: usize) -> Result<Tuple, StoreError> {
        let removed = self.decode_at(idx)?;
        let e = self.slot(idx);
        self.dead_heap_bytes += e.len as usize;
        let n = self.slot_count();
        let dst = dir_offset(idx);
        let src = dir_offset(idx + 1);
        let len = (n - idx - 1) * SLOT_ENTRY_BYTES;
        if len > 0 {
            self.buf.copy_within(src..src + len, dst);
        }
        self.set_slot_count(n - 1);
        Ok(removed)
    }
}

impl PageTuple for SlottedPage {
    fn deep_clone(&self) -> Box<dyn PageTuple> {
        Box::new(self.clone())
    }

    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.slot_count())
    }

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        let lo = self.lower_bound(&tuple.id)?;
        let hi = self.upper_bound(&tuple.id)?;
        if self.find_exact_in(&tuple.id, lo, hi)?.is_some() {
            return Err(StoreError::DuplicateKey(tuple.id));
        }
        let encoded = to_allocvec(&tuple)?;
        self.ensure_room(encoded.len())?;
        // ensure_room may have compacted (offsets change, order/count/ids
        // don't) but never grown a non-empty page and never reorders
        // existing entries, so `lo` — computed against the pre-compaction
        // directory — is still the correct insertion index.
        self.insert_at(lo, &encoded);
        Ok(())
    }

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        Ok(self.find_exact(id)?.is_some())
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        match self.find_exact(id)? {
            Some(idx) => Ok(Some(self.decode_at(idx)?)),
            None => Ok(None),
        }
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        let idx = self
            .find_exact(id)?
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        let old_slot = self.slot(idx);
        let encoded = to_allocvec(&tuple)?;
        if encoded.len() <= old_slot.len as usize {
            // Fits in the old span: overwrite in place. Any leftover bytes
            // become reclaimable fragmentation, same as a remove's.
            let old = self.decode_at(idx)?;
            let off = old_slot.offset as usize;
            self.buf[off..off + encoded.len()].copy_from_slice(&encoded);
            self.dead_heap_bytes += old_slot.len as usize - encoded.len();
            self.set_slot(
                idx,
                SlotEntry {
                    offset: old_slot.offset,
                    len: encoded.len() as u32,
                },
            );
            Ok(old)
        } else {
            // Doesn't fit: remove (frees the old span) + add (finds fresh
            // room, compacting/growing if needed) — see the design doc.
            //
            // Bug fixed here: an earlier version called delete_slot() FIRST
            // and only then found out via ensure_room() whether the new
            // tuple actually fits. If it didn't, ensure_room's Err
            // propagated straight out via `?` — but delete_slot had
            // already removed the old entry, with nothing putting it
            // back. The caller (bplustree.rs's update/update_checked)
            // treats PageCapacityError as "nothing happened here, try a
            // relocate instead" — so the old entry was silently gone,
            // forever, while every caller believed this page was
            // untouched. Confirmed via direct repro:
            // test_table_scan_correct_after_updating_every_row_across_multiple_data_pages
            // (db.rs) — a same-value update just barely grows every
            // tuple (an old pre_lsn:None becomes Some(lsn)), which
            // eventually exhausts a page's slack and hits exactly this
            // path; the row then vanishes.
            //
            // Fix: compute, WITHOUT mutating anything, the max space a
            // full compaction could ever free once this slot's own bytes
            // are also counted as dead (contiguous_free + dead_heap_bytes
            // + this slot's own len) — if the new tuple can't possibly
            // fit even then, fail now, before touching the slot
            // directory at all, so a failure truly means "this page is
            // unchanged." Skipped when this is the page's only entry:
            // that path exists precisely to fall through to
            // grow_to_fit's unconditional-success escape hatch (see its
            // own doc comment — Page::can_store's "an empty page always
            // accepts" rule), which this pre-check would otherwise wrongly
            // reject for any tuple bigger than a normal page.
            if self.slot_count() > 1 {
                let max_reclaimable =
                    self.contiguous_free() + self.dead_heap_bytes + old_slot.len as usize;
                if max_reclaimable < encoded.len() {
                    return Err(StoreError::PageCapacityError);
                }
            }
            let old = self.delete_slot(idx)?;
            // Guaranteed to succeed now: either the pre-check above already
            // confirmed compaction would free enough, or slot_count was 1
            // and ensure_room's grow_to_fit path (slot_count()==0 after the
            // delete above) always succeeds.
            self.ensure_room(encoded.len())?;
            // id is unchanged, but the delete shifted indices and possibly
            // compacted — recompute the insertion point fresh rather than
            // reusing idx.
            let insert_idx = self.lower_bound(id)?;
            self.insert_at(insert_idx, &encoded);
            Ok(old)
        }
    }

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        let idx = self
            .find_exact(&id)?
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        let removed = self.delete_slot(idx)?;
        if self.slot_count() == 0 {
            self.shrink_to_normal();
        }
        Ok(removed)
    }

    fn values(&self) -> Result<Vec<Tuple>, StoreError> {
        (0..self.slot_count()).map(|idx| self.decode_at(idx)).collect()
    }

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
        // Unused externally — see AnyTuplePage::keys' own matching note.
        (0..self.slot_count())
            .map(|idx| Ok(self.decode_at(idx)?.id.hashed()))
            .collect()
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        // The whole point of this page kind: for the common "nothing
        // changed since load" case this is a plain Vec clone of
        // already-correct bytes, not a re-encode of every tuple.
        Ok(self.buf.clone())
    }

    fn clear(&mut self) -> Result<(), StoreError> {
        let declared_capacity = self.declared_capacity();
        self.buf = blank_buffer(declared_capacity, declared_capacity);
        self.heap_start = self.buf.len();
        self.dead_heap_bytes = 0;
        Ok(())
    }

    fn first(&self) -> Result<Option<Tuple>, StoreError> {
        if self.slot_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(self.decode_at(0)?))
        }
    }

    fn last(&self) -> Result<Option<Tuple>, StoreError> {
        let n = self.slot_count();
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some(self.decode_at(n - 1)?))
        }
    }

    // STORE_AUDIT.md P5: see PageTuple::successor's own comment.
    fn successor(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        let idx = self.upper_bound(id)?;
        if idx < self.slot_count() {
            Ok(Some(self.decode_at(idx)?))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::StoreError,
        pages::{PageTuple, slotted::SlottedPage},
        tuple::{DBIdType, Tuple},
    };

    // Comfortably larger than anything these tests store, but small enough
    // that "ran out of room" tests can actually exhaust it on purpose.
    const CAP: usize = 4096;

    fn make_page() -> SlottedPage {
        SlottedPage::new(CAP)
    }

    #[test]
    fn test_add_and_count() {
        let mut p = make_page();
        assert_eq!(p.count().unwrap(), 0);
        p.add(Tuple::new(1, b"hello")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        p.add(Tuple::new(2, b"world")).unwrap();
        assert_eq!(p.count().unwrap(), 2);
    }

    #[test]
    fn test_add_duplicate_returns_err() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        assert!(matches!(p.add(Tuple::new(1, b"b")), Err(StoreError::DuplicateKey(_))));
    }

    #[test]
    fn test_contains() {
        let mut p = make_page();
        p.add(Tuple::new(5, b"data")).unwrap();
        assert!(p.contains(&DBIdType::Int(5)).unwrap());
        assert!(!p.contains(&DBIdType::Int(99)).unwrap());
    }

    #[test]
    fn test_get_hit_and_miss() {
        let mut p = make_page();
        p.add(Tuple::new(10, b"value")).unwrap();
        let found = p.get(&DBIdType::Int(10)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().data.to_vec(), b"value");
        assert!(p.get(&DBIdType::Int(999)).unwrap().is_none());
    }

    #[test]
    fn test_successor_returns_first_key_greater_than_id() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(5, b"b")).unwrap();
        p.add(Tuple::new(10, b"c")).unwrap();
        let s = p.successor(&DBIdType::Int(3)).unwrap().unwrap();
        assert_eq!(s.data.to_vec(), b"b", "successor of 3 must be the entry keyed 5");
    }

    #[test]
    fn test_successor_of_an_existing_key_skips_past_it_not_returns_it() {
        let mut p = make_page();
        p.add(Tuple::new(5, b"exact")).unwrap();
        p.add(Tuple::new(10, b"next")).unwrap();
        let s = p.successor(&DBIdType::Int(5)).unwrap().unwrap();
        assert_eq!(
            s.data.to_vec(),
            b"next",
            "successor must be strictly greater, never the exact match itself"
        );
    }

    #[test]
    fn test_successor_returns_none_when_id_is_at_or_past_every_key() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(5, b"b")).unwrap();
        assert!(p.successor(&DBIdType::Int(5)).unwrap().is_none());
        assert!(p.successor(&DBIdType::Int(99)).unwrap().is_none());
    }

    #[test]
    fn test_successor_on_an_empty_page_returns_none() {
        let p = make_page();
        assert!(p.successor(&DBIdType::Int(1)).unwrap().is_none());
    }

    #[test]
    fn test_replace_updates_existing() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"old")).unwrap();
        let updated = Tuple::new(1, b"new");
        p.replace(&DBIdType::Int(1), updated).unwrap();
        let got = p.get(&DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(got.data.to_vec(), b"new");
    }

    #[test]
    fn test_replace_missing_returns_err() {
        let mut p = make_page();
        assert!(matches!(
            p.replace(&42.into(), Tuple::new(42, b"x")),
            Err(StoreError::KeyNotFound(_))
        ));
    }

    #[test]
    fn test_replace_with_a_shorter_tuple_reuses_the_span_in_place() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a very long original value")).unwrap();
        p.replace(&DBIdType::Int(1), Tuple::new(1, b"short")).unwrap();
        assert_eq!(p.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), b"short");
        assert_eq!(p.count().unwrap(), 1);
    }

    #[test]
    fn test_replace_with_a_longer_tuple_falls_back_to_remove_and_add() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"short")).unwrap();
        p.add(Tuple::new(2, b"also short")).unwrap();
        p.replace(&DBIdType::Int(1), Tuple::new(1, b"a much, much longer replacement value"))
            .unwrap();
        assert_eq!(
            p.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"a much, much longer replacement value"
        );
        // The other entry must survive the fallback untouched.
        assert_eq!(p.get(&DBIdType::Int(2)).unwrap().unwrap().data.to_vec(), b"also short");
        assert_eq!(p.count().unwrap(), 2);
    }

    // Regression test for a real bug found via STORE_AUDIT.md P6's own
    // integration testing (db.rs's
    // test_table_scan_correct_after_updating_every_row_across_multiple_data_pages):
    // the remove-then-add fallback used to call delete_slot() before
    // confirming the new (bigger) tuple would actually fit, so a capacity
    // failure left the OLD entry permanently gone — deleted, never
    // re-added — while reporting PageCapacityError, which every caller
    // (bplustree.rs) treats as "this page is untouched, try elsewhere."
    // The old entry must survive a replace() that genuinely can't fit.
    #[test]
    fn test_replace_that_cannot_possibly_fit_leaves_the_old_entry_intact() {
        let mut p = SlottedPage::new(200);
        p.add(Tuple::new(1, b"short-one")).unwrap();
        p.add(Tuple::new(2, b"short-two")).unwrap();
        let too_big = vec![b'x'; 10_000]; // can never fit on a 200-byte page
        let err = p
            .replace(&DBIdType::Int(1), Tuple::new(1, &too_big))
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PageCapacityError),
            "expected PageCapacityError, got {err:?}"
        );
        assert_eq!(
            p.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"short-one",
            "the old entry must still be present and unchanged after a failed replace \
             — a capacity error must mean the page is untouched, not that the old \
             value silently vanished"
        );
        assert_eq!(p.get(&DBIdType::Int(2)).unwrap().unwrap().data.to_vec(), b"short-two");
        assert_eq!(p.count().unwrap(), 2);
    }

    #[test]
    fn test_remove_existing() {
        let mut p = make_page();
        p.add(Tuple::new(3, b"bye")).unwrap();
        let removed = p.remove(DBIdType::Int(3));
        assert!(removed.is_ok());
        assert_eq!(removed.unwrap().data.to_vec(), b"bye");
        assert!(!p.contains(&DBIdType::Int(3)).unwrap());
    }

    #[test]
    fn test_remove_missing_returns_err() {
        let mut p = make_page();
        assert!(matches!(p.remove(DBIdType::Int(7)), Err(StoreError::KeyNotFound(_))));
    }

    #[test]
    fn test_remove_then_add_reuses_reclaimed_space_via_compaction() {
        // Fill the page to capacity with same-size tuples, then remove
        // every other one but leave at least one survivor (slot_count
        // stays > 0 throughout) — this deliberately rules out the separate
        // "empty page grows to fit" escape hatch (grow_to_fit) from being
        // what makes the re-adds below succeed; only compact() can, since
        // the page was already full and an 8-byte slot-directory entry
        // alone can't account for a whole same-size tuple's worth of room.
        let mut p = SlottedPage::new(300);
        let payload = vec![b'x'; 30];
        let mut ids = vec![];
        for i in 0..20u64 {
            if p.add(Tuple::new(i, &payload)).is_err() {
                break;
            }
            ids.push(i);
        }
        assert!(ids.len() >= 4, "test setup needs room for several same-size tuples");
        let removed_ids: Vec<u64> = ids.iter().copied().step_by(2).collect();
        for &i in &removed_ids {
            p.remove(DBIdType::Int(i)).unwrap();
        }
        assert!(
            p.count().unwrap() >= 1,
            "at least one tuple must survive, or grow_to_fit (not compact) would explain a re-add succeeding"
        );
        // Re-adding EXACTLY as many same-size tuples as were removed must
        // succeed: the page was already at capacity before any removes, so
        // this is only possible if compact() reclaimed the removed
        // tuples' heap bytes, not just their small directory entries.
        for (n, id) in removed_ids.iter().enumerate() {
            p.add(Tuple::new(1000 + *id, &payload)).unwrap_or_else(|e| {
                panic!(
                    "re-add {n} of {} failed: {e:?} — compact() should have reclaimed enough space",
                    removed_ids.len()
                )
            });
        }
    }

    #[test]
    fn test_full_page_returns_capacity_error_not_panic() {
        let mut p = SlottedPage::new(64);
        let mut last_err = None;
        for i in 0..1000u64 {
            if let Err(e) = p.add(Tuple::new(i, b"0123456789")) {
                last_err = Some(e);
                break;
            }
        }
        assert!(matches!(last_err, Some(StoreError::PageCapacityError)));
    }

    #[test]
    fn test_one_oversized_tuple_on_an_empty_page_grows_the_buffer() {
        // Mirrors AnyTuplePage's unconstrained accept for this exact case —
        // Page::can_store's "an empty page always accepts" escape hatch
        // (see this file's own top comment) is the only legitimate way a
        // single tuple bigger than the declared capacity gets stored.
        let mut p = SlottedPage::new(64);
        let big = vec![b'z'; 1000];
        p.add(Tuple::new(1, &big)).unwrap();
        assert_eq!(p.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), big);
        let bytes = p.to_bytes().unwrap();
        assert!(bytes.len() > 64, "buffer must have grown past the declared capacity");
    }

    #[test]
    fn test_removing_the_oversized_tuple_shrinks_the_buffer_back() {
        let mut p = SlottedPage::new(64);
        let big = vec![b'z'; 1000];
        p.add(Tuple::new(1, &big)).unwrap();
        assert!(p.to_bytes().unwrap().len() > 64);
        p.remove(DBIdType::Int(1)).unwrap();
        // Must shrink back to the ORIGINAL declared capacity (64), not stay
        // oversized — see this file's own top comment: buffer.rs's
        // non-overflow write path rejects any page whose to_bytes() is
        // bigger than one physical page slot.
        assert_eq!(p.to_bytes().unwrap().len(), 64);
        assert_eq!(p.count().unwrap(), 0);
    }

    #[test]
    fn test_grow_then_shrink_then_regrow_round_trips_through_bytes() {
        let mut p = SlottedPage::new(64);
        let big = vec![b'z'; 500];
        p.add(Tuple::new(1, &big)).unwrap();
        let grown = p.to_bytes().unwrap();
        let mut reloaded = SlottedPage::from_bytes(&grown).unwrap();
        assert_eq!(reloaded.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), big);
        reloaded.remove(DBIdType::Int(1)).unwrap();
        let shrunk = reloaded.to_bytes().unwrap();
        assert_eq!(shrunk.len(), 64, "declared_capacity must survive a from_bytes round trip");
        // And it must still accept a fresh oversized tuple after shrinking.
        let mut reloaded2 = SlottedPage::from_bytes(&shrunk).unwrap();
        reloaded2.add(Tuple::new(2, &big)).unwrap();
        assert_eq!(reloaded2.get(&DBIdType::Int(2)).unwrap().unwrap().data.to_vec(), big);
    }

    #[test]
    fn test_values_returns_all_in_ascending_order() {
        let mut p = make_page();
        p.add(Tuple::new(3, b"c")).unwrap();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(2, b"b")).unwrap();
        let vals: Vec<String> = p
            .values()
            .unwrap()
            .into_iter()
            .map(|t| String::from_utf8(t.data.to_vec()).unwrap())
            .collect();
        assert_eq!(vals, vec!["a", "b", "c"], "values() must yield ascending DBIdType::cmp order");
    }

    #[test]
    fn test_first_and_last() {
        let mut p = make_page();
        assert!(p.first().unwrap().is_none());
        assert!(p.last().unwrap().is_none());
        p.add(Tuple::new(5, b"middle")).unwrap();
        p.add(Tuple::new(1, b"first")).unwrap();
        p.add(Tuple::new(9, b"last")).unwrap();
        assert_eq!(p.first().unwrap().unwrap().data.to_vec(), b"first");
        assert_eq!(p.last().unwrap().unwrap().data.to_vec(), b"last");
    }

    #[test]
    fn test_clear() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(2, b"b")).unwrap();
        p.clear().unwrap();
        assert_eq!(p.count().unwrap(), 0);
        assert!(p.values().unwrap().is_empty());
        // Must still be usable afterward.
        p.add(Tuple::new(3, b"c")).unwrap();
        assert_eq!(p.get(&DBIdType::Int(3)).unwrap().unwrap().data.to_vec(), b"c");
    }

    #[test]
    fn test_roundtrip_serialization_is_a_plain_byte_clone() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"foo")).unwrap();
        p.add(Tuple::new(2, b"bar")).unwrap();
        let bytes = p.to_bytes().unwrap();
        let p2 = SlottedPage::from_bytes(&bytes).unwrap();
        assert_eq!(p2.count().unwrap(), 2);
        assert_eq!(p2.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), b"foo");
        assert_eq!(p2.get(&DBIdType::Int(2)).unwrap().unwrap().data.to_vec(), b"bar");
        // And re-serializing an untouched reload must reproduce the exact
        // same bytes — the whole point of this page kind.
        assert_eq!(p2.to_bytes().unwrap(), bytes);
    }

    #[test]
    fn test_clone_is_independent() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"original")).unwrap();
        let mut q = p.clone();
        q.add(Tuple::new(2, b"extra")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        assert_eq!(q.count().unwrap(), 2);
    }

    #[test]
    fn test_partial_eq_is_by_content_not_raw_bytes() {
        let mut p = make_page();
        let mut q = make_page();
        assert_eq!(p, q);
        p.add(Tuple::new(1, b"x")).unwrap();
        assert_ne!(p, q);
        q.add(Tuple::new(1, b"x")).unwrap();
        assert_eq!(p, q);
        // Different physical history (insert-then-remove-then-reinsert
        // leaves different heap layout/fragmentation) but same final
        // content must still compare equal.
        p.add(Tuple::new(2, b"y")).unwrap();
        p.remove(DBIdType::Int(2)).unwrap();
        p.add(Tuple::new(2, b"y")).unwrap();
        q.add(Tuple::new(2, b"y")).unwrap();
        assert_eq!(p, q);
    }

    #[test]
    fn test_string_id() {
        let mut p = make_page();
        let id = DBIdType::from("my_key".to_string());
        p.add(Tuple::new_with(id.clone(), b"payload", None, None)).unwrap();
        assert!(p.contains(&id.clone()).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data.to_vec(), b"payload");
    }

    fn rec_id(a: i64, b: i64) -> DBIdType {
        DBIdType::Rec(
            crate::valueitem::IndexKey::new_from(&[
                crate::valueitem::ValueItem::Integer(a),
                crate::valueitem::ValueItem::Integer(b),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn test_rec_id_add_get_remove() {
        let mut p = make_page();
        let id = rec_id(1, 2);
        p.add(Tuple::new_with(id.clone(), b"payload", None, None)).unwrap();
        assert!(p.contains(&id).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data.to_vec(), b"payload");
        let removed = p.remove(id.clone()).unwrap();
        assert_eq!(removed.data.to_vec(), b"payload");
        assert!(!p.contains(&id).unwrap());
    }

    #[test]
    fn test_rec_id_iterates_in_structural_order() {
        let mut p = make_page();
        for (a, b) in [(3, 1), (1, 2), (2, 1), (1, 1)] {
            p.add(Tuple::new_with(rec_id(a, b), format!("{a}-{b}").as_bytes(), None, None))
                .unwrap();
        }
        let vals: Vec<String> = p
            .values()
            .unwrap()
            .into_iter()
            .map(|t| String::from_utf8(t.data.to_vec()).unwrap())
            .collect();
        assert_eq!(vals, vec!["1-1", "1-2", "2-1", "3-1"]);
    }

    // DBIdType::cmp for Rec can say Equal for ids that are `!=` under
    // PartialEq (see IndexKey::partial_cmp's own documented ties). Unlike
    // AnyTuplePage's Vec-bucket, this stores both as adjacent slots in the
    // same cmp-tied run — confirm both stay independently reachable via
    // PartialEq disambiguation within that run (see find_exact_in), rather
    // than one silently shadowing the other or `add`'s duplicate check
    // wrongly rejecting the second as a dup of the first.
    #[test]
    fn test_rec_ids_that_tie_under_ord_but_differ_under_partial_eq_both_survive() {
        use crate::valueitem::{IndexKey, ValueItem};

        let mut p = make_page();
        let short = DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(1)]).unwrap());
        let long =
            DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap());
        assert_eq!(
            short.cmp(&long),
            std::cmp::Ordering::Equal,
            "sanity: a prefix key ties under Ord with the longer key it prefixes"
        );
        assert_ne!(short, long, "but they are NOT the same id under PartialEq");

        p.add(Tuple::new_with(short.clone(), b"short", None, None)).unwrap();
        p.add(Tuple::new_with(long.clone(), b"long", None, None)).unwrap();

        assert_eq!(p.count().unwrap(), 2, "both must be independently stored, not bucketed into one");
        assert_eq!(p.get(&short).unwrap().unwrap().data.to_vec(), b"short");
        assert_eq!(p.get(&long).unwrap().unwrap().data.to_vec(), b"long");

        let removed_short = p.remove(short.clone()).unwrap();
        assert_eq!(removed_short.data.to_vec(), b"short");
        assert!(!p.contains(&short).unwrap());
        assert!(p.contains(&long).unwrap(), "removing one tied id must not remove the other");
    }

    #[test]
    fn test_from_bytes_rejects_a_too_small_buffer() {
        let err = SlottedPage::from_bytes(&[0u8; 3]).unwrap_err();
        assert!(matches!(err, StoreError::UnknownError(_)));
    }

    #[test]
    fn test_from_bytes_rejects_a_slot_directory_longer_than_the_buffer() {
        // slot_count = 5, but no actual entries/heap bytes follow.
        let mut bytes = vec![0u8; 8];
        bytes[0..4].copy_from_slice(&5u32.to_le_bytes());
        let err = SlottedPage::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, StoreError::UnknownError(_)));
    }

    // STORE_AUDIT.md P6 — the benchmark that explains why this type is kept
    // but NOT wired in as Page::new's default. See this file's own top
    // comment ("Why this isn't the default") for the full story; in short:
    // AnyTuplePage decodes once on load, then every get/add/replace on an
    // already-cached page is a free in-memory BTreeMap comparison.
    // SlottedPage decodes nothing on load, but every comparison during its
    // O(log N) binary search fully postcard-decodes a candidate Tuple (all
    // fields, not just id) — so a page touched many times while resident
    // (the common case, especially given this session's own P2/P3 caching
    // work) pays that cost over and over. Measured: AnyTuplePage ~23.3M
    // gets/s vs SlottedPage ~1.07M gets/s on an identical 200-tuple page —
    // ~22x. That single number, not any correctness issue, is why
    // Page::new still builds AnyTuplePage/FixedTuplePage. Throwaway (not a
    // committed criterion bench), #[ignore]d — compare against
    // anytuple.rs's identically-shaped bench_repeated_get_on_an_already_
    // loaded_page.
    #[test]
    #[ignore]
    fn bench_repeated_get_on_an_already_loaded_page() {
        let mut p = SlottedPage::new(16 * 1024);
        for i in 0..200u64 {
            p.add(Tuple::new(i, b"0123456789012345678901234567890123456789")).unwrap();
        }
        let mut state: u64 = 0x243F_6A88_85A3_08D3;
        const ITERS: u64 = 2_000_000;
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let id = DBIdType::Int(state % 200);
            std::hint::black_box(p.get(&id).unwrap());
        }
        let elapsed = start.elapsed();
        eprintln!(
            "SlottedPage bench_repeated_get_on_an_already_loaded_page: {ITERS} gets in \
             {elapsed:?} ({:.0} gets/s)",
            ITERS as f64 / elapsed.as_secs_f64()
        );
    }
}
