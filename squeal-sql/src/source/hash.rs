use crate::source::{planinfo::PlanNode};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    run::Run,
    table::TableIdType,
    valueitem::IndexKey,
};

use crate::{
    ds::bitvec::BitVec,
    error::SchemaError,
    optim::table_stats::TableStat,
    plan::memory::{MemReservation, QueryMemory},
    source::{
        ComputedTableStat, ProjectableField, QueryStats, Source, join::JoinType,
        joinmatch::JoinMatcher, merge_stats,
    },
};

#[allow(unused)]
pub(crate) struct HashedSource<F: DBFile + 'static> {
    sources: Vec<Box<dyn Source>>,
    db: Arc<Db<F>>,
    // The build (left, post-swap) side's table: in memory (no Run, no
    // postcard) until it outgrows `mem`'s budget, at which point it
    // migrates to the Run-backed open-addressing table once and for all
    // (see migrate_to_run) — the shape every build used to be, and still
    // is once a build genuinely doesn't fit. See BuildTable's own doc
    // comment.
    table: BuildTable<F>,
    record_size: usize,
    // Sizing hint for the build side, computed once in new() from its
    // table_stats() — used only if/when this ever migrates to Run (see
    // migrate_to_run); a Mem table needs no such hint, it just grows.
    initial_capacity_hint: usize,
    left_fields: Vec<usize>,
    right_fields: Vec<usize>,
    fields: Arc<[ProjectableField]>,
    count: usize,
    mem: Arc<QueryMemory>,
    join_type: JoinType,
    // True when new() swapped the two sources (build on the smaller side).
    // Everything ABOVE this source — `fields`, JoinSource, the ON/SELECT
    // positions built against them — still sees the ORIGINAL left ++ right
    // column order, so `emit` has to put the rows back in that order, and
    // `join_type` (see new()) already describes the swapped, physical sides.
    swapped: bool,
    // The match rules shared with every other join algorithm (key
    // equality, which unmatched side the join type keeps, the NULL-padding
    // rows, output assembly) — built from the PHYSICAL (post-swap) sides.
    matcher: JoinMatcher,
    // Whether the left source has been fully drained into the table yet
    // — done lazily on the first next() call rather than in new(), so
    // constructing a HashedSource stays cheap even if it's never
    // actually iterated. The right side is never eagerly drained at
    // all: next() streams it one row at a time (see pending_matches'
    // own doc comment for why).
    built: bool,
    // Every left row matching `current_right`'s key, found by
    // probe_matches and not yet emitted — next() pops one per call,
    // refilling this (by pulling and probing the next right row) once
    // it runs dry. A right row can produce zero, one, or many output
    // rows depending on how many left rows share its key — this, plus
    // probe_matches walking the FULL matching chain instead of
    // stopping at the first hit, is what makes multi-match joins work
    // correctly in both directions (multiple right rows sharing a left
    // key used to overwrite a single stored match; multiple left rows
    // sharing a key used to only ever be checked one at a time).
    pending_matches: VecDeque<IndexKey>,
    current_right: Option<IndexKey>,
    // Set once sources[1] (right) has been fully drained — next() then
    // switches from "pull + probe a right row" to (for LEFT/FULL only)
    // the unmatched-left sweep below; for INNER/RIGHT it just means done.
    right_exhausted: bool,
    // Unmatched-left sweep position (LEFT/FULL only, after
    // right_exhausted): which page/slot next_unmatched_left is
    // currently scanning, and that page's cached decoded content.
    sweep_page: usize,
    // Index into sweep_page_data (a compacted list of only the OCCUPIED
    // slots on sweep_page, in ascending slot-id order — see
    // Run::slots_at), not the raw slot id itself. STORE_AUDIT.md P6: the
    // slot id each entry actually lives at is carried alongside it (the
    // `u64` in the tuple below), since it's no longer implied by
    // position the way indexing into a full Vec<Option<HashValue>> used
    // to make it.
    sweep_row: usize,
    sweep_page_data: Option<Vec<(u64, HashValue)>>,
    // next_unmatched_left's resume position for a Mem table — an index
    // into MemTable::entries/matched. Trivial compared to the Run sweep
    // above (no pages to walk), since everything's already one flat Vec.
    mem_sweep_pos: usize,
    left_time: u128,
    next_time: u128,
    probe_time: u128,
    rehash_time: u128,
    insert_left: u128,
}

// STORE_AUDIT.md P6: `left_value` used to be `Option<IndexKey>` even
// though every real write path only ever stored `Some(item)` — a slot's
// mere PRESENCE on the page already meant "occupied" (see `bitmask`),
// so the `Option` inside the payload was redundant. Now that occupancy
// is answered by whether `Run::get_slot_at` returns anything at all
// (see `insert_with_hash`/`probe_matches`), there's nothing left for it
// to disambiguate.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashValue {
    hash: u64,
    left_value: IndexKey,
}

// Per-entry memory-accounting overhead charged against QueryMemory for a
// Mem-backed entry: the Vec<HashValue> slot, the HashMap bucket, and the
// Vec<usize> index entry it lives in. Deliberately generous, like
// SLOTTED_OVERHEAD_BYTES below is for a Run page — getting it a little
// wrong only means migrating to Run a bit earlier or later than the true
// byte count would, never a correctness problem.
const MEM_ENTRY_OVERHEAD_BYTES: usize = 48;

// The build side kept as plain owned values — no Run, no SlottedPage, no
// postcard. Every left row lives twice: once in `entries` (insertion
// order, needed for the LEFT/FULL unmatched sweep) and once via `index`
// (hash -> positions in `entries`, needed for probing). `matched` is
// parallel to `entries`, same purpose as RunTable's `matched` bitmask.
struct MemTable {
    entries: Vec<HashValue>,
    matched: Vec<bool>,
    index: HashMap<u64, Vec<usize>>,
    mem: Vec<MemReservation>,
}

impl MemTable {
    fn new() -> Self {
        Self {
            entries: vec![],
            matched: vec![],
            index: HashMap::new(),
            mem: vec![],
        }
    }

    // On success, `value` is absorbed into the table. On failure (the
    // budget is exhausted), `value` comes back unowned so the caller can
    // hand it to whatever the table migrates to instead of re-cloning it.
    fn try_insert(
        &mut self,
        budget: &Arc<QueryMemory>,
        value: HashValue,
    ) -> Result<(), (SchemaError, HashValue)> {
        let bytes = value.left_value.size() + MEM_ENTRY_OVERHEAD_BYTES;
        match budget.try_reserve(bytes) {
            Ok(reservation) => {
                self.mem.push(reservation);
                self.index.entry(value.hash).or_default().push(self.entries.len());
                self.entries.push(value);
                self.matched.push(false);
                Ok(())
            }
            Err(e) => Err((e, value)),
        }
    }

    // Same matching semantics as RunTable::probe (see its own doc
    // comment) — every entry sharing `hash`, checked with the real
    // equality (a hash match is necessary, not sufficient), each marked
    // matched as found.
    fn probe(&mut self, matcher: &JoinMatcher, right: &IndexKey, hash: u64) -> VecDeque<IndexKey> {
        let mut matches = VecDeque::new();
        let Some(positions) = self.index.get(&hash) else {
            return matches;
        };
        for &i in positions {
            if matcher.keys_match(&self.entries[i].left_value, right) {
                matches.push_back(self.entries[i].left_value.clone());
                self.matched[i] = true;
            }
        }
        matches
    }
}

// The build side once it has spilled: today's open-addressing table over a
// Run of SlottedPages, unchanged in every particular from before this
// module also had a Mem-backed option — same probe-chain, same rehash-at-
// 100%-load policy (see insert_with_hash's own comment), same raw-bytes
// rehash replay (see HashedSource::rehash). Bundles exactly the fields
// HashedSource itself used to hold directly.
struct RunTable<F: DBFile + 'static> {
    run: Run<F>,
    capacity: usize,
    bitmask: BitVec,
    // Parallel to `bitmask` (same size, recreated alongside it): has this
    // OCCUPIED slot ever been matched by a probing right row? Only
    // meaningful for LEFT/FULL, which need to emit every left row that
    // never matched anything once the right source is exhausted — see
    // HashedSource::next_unmatched_left.
    matched: BitVec,
    records_per_page: usize,
}

impl<F: DBFile + 'static> RunTable<F> {
    #[inline(always)]
    fn slot_available(&self, index: usize) -> bool {
        !self.bitmask.is_set(index)
    }

    fn find_next_slot(&self, index: usize) -> Result<usize, SchemaError> {
        self.bitmask
            .first_available(index)
            .ok_or(SchemaError::UnknownError("Could not find any slots".into()))
    }

    fn claim_slot(&mut self, index: usize) -> Result<(), SchemaError> {
        self.bitmask.set(index);
        assert!(!self.slot_available(index));
        Ok(())
    }

    fn insert_with_hash(&mut self, item: IndexKey, hash: u64) -> Result<(), SchemaError> {
        let mut index = (hash % self.capacity as u64) as usize;
        if !self.slot_available(index) {
            index = self.find_next_slot(index)?;
        }
        self.claim_slot(index)?;
        let page_index = index / self.records_per_page;
        let row_index = (index % self.records_per_page) as u64;
        let value = HashValue {
            hash,
            left_value: item,
        };
        self.run
            .set_slot_at(page_index, row_index, &to_allocvec(&value)?)?;
        Ok(())
    }

    // Rehash-replay fast path — see HashedSource::rehash's own comment for
    // why this exists apart from insert_with_hash: `raw` is a prior
    // entry's already-encoded bytes (hash prefix included), placed at its
    // new slot verbatim, no IndexKey decode/re-encode needed.
    fn insert_raw(&mut self, hash: u64, raw: &[u8]) -> Result<(), SchemaError> {
        let mut index = (hash % self.capacity as u64) as usize;
        if !self.slot_available(index) {
            index = self.find_next_slot(index)?;
        }
        self.claim_slot(index)?;
        let page_index = index / self.records_per_page;
        let row_index = (index % self.records_per_page) as u64;
        self.run.set_slot_at(page_index, row_index, raw)?;
        Ok(())
    }

    // Finds every left row matching `right`'s join key — see
    // HashedSource::probe_matches's own (former) doc comment for why this
    // walks the FULL probe chain instead of stopping at the first hit,
    // and why an empty slot is a reliable chain-end signal.
    fn probe(
        &mut self,
        matcher: &JoinMatcher,
        right: &IndexKey,
        hash: u64,
    ) -> Result<VecDeque<IndexKey>, SchemaError> {
        let start = (hash % self.capacity as u64) as usize;
        let mut matches = VecDeque::new();
        let mut index = start;
        loop {
            let page_index = index / self.records_per_page;
            let row_index = (index % self.records_per_page) as u64;
            match self.run.get_slot_at(page_index, row_index)? {
                Some(bytes) => {
                    let v: HashValue = from_bytes(&bytes)?;
                    if matcher.keys_match(&v.left_value, right) {
                        matches.push_back(v.left_value);
                        self.matched.set(index);
                    }
                }
                None => break,
            }
            index = (index + 1) % self.capacity;
            if index == start {
                break;
            }
        }
        Ok(matches)
    }
}

// The build side's storage: in memory until it doesn't fit, then the
// Run-backed table — a one-way transition (see HashedSource::migrate_to_run),
// same as SortJoinSource::GroupBuf's own Vec-then-Run spill. Every method
// on HashedSource that needs the table dispatches on this once, at the top
// (see e.g. probe_matches) — RunTable/MemTable know nothing about each
// other or about HashedSource itself.
enum BuildTable<F: DBFile + 'static> {
    Mem(MemTable),
    Run(RunTable<F>),
}

impl<F: DBFile + 'static> HashedSource<F> {
    pub(crate) fn new(
        left_source: Box<dyn Source>,
        right_source: Box<dyn Source>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
        left_fields: &[usize],
        right_fields: &[usize],
        join_type: JoinType,
    ) -> Result<Self, SchemaError> {
        let record_size = left_source
            .fields()
            .iter()
            .chain(right_source.fields().iter())
            .map(|f| f.field.datatype.size())
            .sum();
        let fields = Arc::from(
            left_source
                .fields()
                .iter()
                .chain(right_source.fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        );
        // Put the side with MORE rows on the left, i.e. make it the build
        // (hash-table) side. Deliberate, and the opposite of the textbook
        // "build the smaller side": measured on the retail 4-way join it is
        // ~3.5x faster (~330 ms vs ~1140 ms with the comparison flipped), so
        // don't "correct" it without re-measuring. Inner/Full joins are
        // symmetric, so swapping is free; Left/Right are not — "preserve
        // every LEFT row" must keep meaning the ORIGINAL left table, which
        // after a swap is the physical right (probe) side, so the type is
        // mirrored along with the sources.
        let (left_source, left_fields, right_source, right_fields, join_type, swapped) = {
            if let Some(left_stat) = left_source.table_stats()
                && let Some(right_stat) = right_source.table_stats()
                && right_stat.table_stat.row_count > left_stat.table_stat.row_count
            {
                let mirrored = match join_type {
                    JoinType::Left => JoinType::Right,
                    JoinType::Right => JoinType::Left,
                    other => other,
                };
                (
                    right_source,
                    right_fields,
                    left_source,
                    left_fields,
                    mirrored,
                    true,
                )
            } else {
                (
                    left_source,
                    left_fields,
                    right_source,
                    right_fields,
                    join_type,
                    false,
                )
            }
        };
        // Sized from the BUILD side — `left_source` here, whichever branch
        // above produced it — but only USED if this build ever migrates to
        // Run (see migrate_to_run); a Mem table needs no upfront sizing at
        // all. No stats for the build side means no basis for a guess: one
        // page's worth, same fallback as before this table could be Mem.
        let initial_capacity_hint = left_source
            .table_stats()
            .map(|s| (s.table_stat.row_count as f64 * Self::INITIAL_CAPACITY_FACTOR) as usize)
            .unwrap_or(1);
        let matcher = JoinMatcher::new(
            join_type,
            left_fields,
            right_fields,
            left_source.fields().len(),
            right_source.fields().len(),
        )?;
        Ok(Self {
            sources: vec![left_source, right_source],
            fields,
            // No Run, no page, allocated yet — see BuildTable's own doc
            // comment. The overwhelmingly common case (a join whose build
            // side fits the query's memory budget) never allocates one at
            // all.
            table: BuildTable::Mem(MemTable::new()),
            initial_capacity_hint,
            record_size,
            db,
            left_fields: left_fields.to_vec(),
            right_fields: right_fields.to_vec(),
            count: 0,
            mem,
            join_type,
            swapped,
            matcher,
            built: false,
            pending_matches: VecDeque::new(),
            current_right: None,
            right_exhausted: false,
            sweep_page: 0,
            sweep_row: 0,
            sweep_page_data: None,
            mem_sweep_pos: 0,
            next_time: 0,
            left_time: 0,
            probe_time: 0,
            rehash_time: 0,
            insert_left: 0,
        })
    }

    // STORE_AUDIT.md P6: per-slot overhead for a SlottedPage-backed run
    // page — a fixed slot-directory entry (kept in sync by hand with
    // store::pages::slotted's own SLOT_ENTRY_BYTES, which is private to
    // that crate) plus Tuple/postcard framing for a HashValue payload
    // (an id varint, None txn_id/pre_lsn, a flags byte, a data-length
    // prefix, and the `hash: u64` field itself — usually close to its
    // 10-byte varint worst case, since a real hash is close to uniform).
    // Deliberately generous rather than tight: getting this wrong just
    // means `records_per_page` under-packs a little (an early rehash),
    // never a hard capacity-mismatch error, since SlottedPage's own
    // add() is the real, authoritative check either way.
    const SLOTTED_OVERHEAD_BYTES: usize = 40;

    // Initial slots per expected build-side row. Rehash fires only at 100%
    // load (see insert_left), so this is not about avoiding a rehash — any
    // factor >= 1.0 does that — but about keeping linear-probe chains short:
    // 1.9 ends a full build at ~0.53 load.
    const INITIAL_CAPACITY_FACTOR: f64 = 1.9;

    // Creates a Run (backed by SlottedPage — STORE_AUDIT.md P6) with
    // enough pages to hold at least `min_capacity` slots. Unlike the
    // prior plain-blob-per-page design, pages need no upfront
    // initialization — an empty SlottedPage slot simply doesn't exist
    // yet (see insert_with_hash/probe_matches, which test for that via
    // `Run::get_slot_at` returning `None`), rather than needing a
    // pre-written `Vec<Option<HashValue>>` placeholder to decode later.
    // `records_per_page` is a function of record_size and the DB's fixed
    // page size only, so it comes back out unchanged across calls
    // (new()'s initial single-page allocation and rehash()'s later
    // multi-page growth both compute the exact same value) — callers
    // that need it again (rehash, to size its growth request) get it
    // back rather than recomputing it separately.
    //
    // Because capacity is always set to records_per_page * num_pages
    // (never an arbitrary requested number), every slot index in
    // `0..capacity` maps to a page index in `0..num_pages` — never out
    // of range — which is what lets probe_matches/insert_with_hash use
    // plain division without any bounds juggling.
    // Slots per page for a run of this record size; the run only supplies
    // its page data size. Asserts at least one fits.
    fn slots_per_page(run: &Run<F>, record_size: usize) -> usize {
        let records_per_page =
            run.data_size() as usize / (record_size + Self::SLOTTED_OVERHEAD_BYTES);
        assert!(
            records_per_page > 0,
            "a single page must be able to hold at least one hash slot"
        );
        records_per_page
    }

    // Grows `run` (which already has its first page — create_slotted_run
    // allocates one) to hold at least `min_capacity` slots; returns the
    // resulting page count.
    fn grow_run(
        run: &mut Run<F>,
        records_per_page: usize,
        min_capacity: usize,
    ) -> Result<usize, SchemaError> {
        let num_pages = min_capacity.max(1).div_ceil(records_per_page).max(1);
        for _ in 1..num_pages {
            run.new_slotted_page()?;
        }
        Ok(num_pages)
    }

    fn allocate_run(
        db: &Arc<Db<F>>,
        record_size: usize,
        min_capacity: usize,
    ) -> Result<(Run<F>, usize, usize), SchemaError> {
        let mut run = db.create_slotted_run()?;
        let records_per_page = Self::slots_per_page(&run, record_size);
        let num_pages = Self::grow_run(&mut run, records_per_page, min_capacity)?;
        Ok((run, records_per_page, num_pages))
    }

    // Rehashes at 100% load, deliberately, despite RunTable::find_next_slot
    // (BitVec::first_available) being a plain linear probe with no
    // clustering mitigation — Knuth's classic linear-probing analysis
    // makes filling all the way to 100% load a textbook Theta(n^1.5)
    // operation, so a lower (e.g. 70%) threshold looks like the obvious
    // fix on paper. Tried twice (independently, in two separate sessions)
    // and reverted both times: a 70% threshold means one extra doubling
    // cycle to reach the same final capacity, and each rehash does real
    // per-entry I/O (a full decode/encode replay of every entry through
    // the page buffer) — that extra pass costs more than the shorter
    // probe chains save. Measured at n=800,000 with a real, separate
    // store-level data-loss bug already fixed (see rehash()'s own
    // comment, so this isn't that bug muddying the numbers): 70% pushed
    // rehash time from 806ms to 1438ms (+78%) and total build_left time
    // from 18.1s to 19.4s (+7%). See
    // bench_insert_left_scaling_isolates_load_factor_effects to
    // reproduce either way. (All Run-specific — a Mem table has no
    // capacity/load-factor concept at all; std HashMap grows on its own.)
    fn insert_left(&mut self, item: IndexKey) -> Result<(), SchemaError> {
        let start = Instant::now();
        let hash = self.get_hash(&item, &self.left_fields);
        match &mut self.table {
            BuildTable::Mem(mt) => match mt.try_insert(&self.mem, HashValue { hash, left_value: item }) {
                Ok(()) => self.count += 1,
                // Doesn't fit the budget: migrate every already-accumulated
                // entry to a Run-backed table (see migrate_to_run), then
                // insert the row that didn't fit through that table instead
                // — from here on this HashedSource behaves exactly as it
                // did before it could ever be Mem-backed.
                Err((SchemaError::QueryMemoryExceeded { .. }, value)) => {
                    self.migrate_to_run()?;
                    self.insert_into_run(value.left_value, value.hash)?;
                }
                Err((e, _)) => return Err(e),
            },
            BuildTable::Run(_) => self.insert_into_run(item, hash)?,
        }
        self.insert_left += start.elapsed().as_nanos();
        Ok(())
    }

    // The Run-backed insert path: rehash first if this insert would land on
    // a full table (see insert_left's own comment on the 100%-load
    // trigger), then place it. Shared by insert_left's already-Run branch
    // and migrate_to_run's replay loop — both need the exact same
    // rehash-before-inserting behavior, so `self.count` (bumped here, the
    // one thing both callers rely on) stays accurate throughout either.
    fn insert_into_run(&mut self, item: IndexKey, hash: u64) -> Result<(), SchemaError> {
        let capacity = match &self.table {
            BuildTable::Run(rt) => rt.capacity,
            BuildTable::Mem(_) => unreachable!("insert_into_run requires a Run table"),
        };
        if self.count == capacity {
            self.rehash(capacity * 2)?;
        }
        let BuildTable::Run(rt) = &mut self.table else {
            unreachable!("insert_into_run requires a Run table")
        };
        rt.insert_with_hash(item, hash)?;
        self.count += 1;
        Ok(())
    }

    // Moves every entry MemTable has accumulated so far into a fresh
    // Run-backed table, sized from `initial_capacity_hint` (the same
    // stats-based guess new() always used to size the Run up front with —
    // see its own comment) or the entry count, whichever asks for more.
    // Only ever called from insert_left, strictly before any right-side
    // probing starts, so (like rehash) nothing here needs to replay a
    // `matched` bit — nothing has been matched yet. A one-way trip: once
    // Run, always Run for the rest of this HashedSource's life (until
    // reset() — see its own comment).
    fn migrate_to_run(&mut self) -> Result<(), SchemaError> {
        let BuildTable::Mem(mt) = std::mem::replace(&mut self.table, BuildTable::Mem(MemTable::new()))
        else {
            unreachable!("migrate_to_run requires a Mem table")
        };
        let (run, records_per_page, num_pages) = Self::allocate_run(
            &self.db,
            self.record_size,
            self.initial_capacity_hint.max(mt.entries.len()),
        )?;
        let capacity = records_per_page * num_pages;
        self.table = BuildTable::Run(RunTable {
            run,
            capacity,
            bitmask: BitVec::with_capacity(capacity),
            matched: BitVec::with_capacity(capacity),
            records_per_page,
        });
        self.count = 0;
        // mt.mem (this table's QueryMemory reservations) drops here,
        // giving the budget back — the same rows now live in Run pages
        // instead, which draw from the database's own temp pool, not
        // QueryMemory.
        for value in mt.entries {
            self.insert_into_run(value.left_value, value.hash)?;
        }
        Ok(())
    }

    // Finds every left row matching `right`'s join key — dispatches to
    // whichever table backs the build side; see MemTable::probe/
    // RunTable::probe for the matching itself (identical semantics either
    // way — every match found, not just the first, each marked as matched
    // for LEFT/FULL's later unmatched-left sweep).
    fn probe_matches(&mut self, right: &IndexKey) -> Result<VecDeque<IndexKey>, SchemaError> {
        let start_time = Instant::now();
        let hash = self.get_hash(right, &self.right_fields);
        let matches = match &mut self.table {
            BuildTable::Mem(mt) => mt.probe(&self.matcher, right, hash),
            BuildTable::Run(rt) => rt.probe(&self.matcher, right, hash)?,
        };
        self.probe_time += start_time.elapsed().as_nanos();
        Ok(matches)
    }

    fn get_hash(&self, key: &IndexKey, fields: &[usize]) -> u64 {
        IndexKey::hash_fields(fields.iter().map(|f| &key.values()[*f]))
    }

    // STORE_AUDIT.md P6: `run_cursor` (a plain sequential walk over every
    // tuple in the old run, page by page) now yields exactly one
    // HashValue per step, not a whole page's Vec to flatten — a
    // SlottedPage only ever stores tuples for slots that are actually
    // occupied (see RunTable::insert_with_hash), so there's no `None`
    // filler to skip over the way the old `Vec<Option<HashValue>>` scheme
    // needed.
    //
    // FIXED (was a KNOWN BUG in this loop): "Did not expect empty run
    // tuple" — self.count claiming more entries than the old run
    // actually had — reproduced reliably at large scale (e.g. the
    // 272384 -> 544768 rehash boundary), independent of this hash
    // table's own load-factor/replay logic (confirmed via a 70%-load
    // trigger and a raw-bytes replay path, both since reverted, still
    // reproducing identically). Root cause was in store's PageBuffer,
    // not here: the async writer thread cleared a page's dirty flag
    // unconditionally after flushing it, racing a concurrent mutation of
    // that same shared page (via a Weak-upgrade) that landed between the
    // flush's byte snapshot and the dirty-clear — the mutation was then
    // silently lost the next time that page was evicted while wrongly
    // believed clean. Fixed in store/src/page.rs (Page::dirty_version /
    // mark_flushed_up_to) and store/src/buffer.rs (write_page).
    // See store/src/buffer.rs's MiniHashTable test for a fast, direct
    // repro of the underlying store-level bug, and
    // bug_repro_rehash_past_272384_rows_loses_entries (this file) for
    // the original real-code-level repro, now passing reliably.
    fn rehash(&mut self, new_capacity: usize) -> Result<(), SchemaError> {
        let start = Instant::now();
        let count = self.count;
        let mut run_cursor = {
            let BuildTable::Run(rt) = &self.table else {
                unreachable!("rehash requires a Run table")
            };
            rt.run.cursor()?
        };
        let (run, records_per_page, num_pages) =
            Self::allocate_run(&self.db, self.record_size, new_capacity)?;
        let capacity = records_per_page * num_pages;
        let mut new_table = RunTable {
            run,
            capacity,
            bitmask: BitVec::with_capacity(capacity),
            matched: BitVec::with_capacity(capacity),
            records_per_page,
        };
        self.count = 0;
        let mut added = 0;
        while added < count {
            let data = run_cursor.next()?.ok_or(SchemaError::UnknownError(format!(
                "Did not expect empty run tuple: added={added} count={count} \
                 new_capacity={new_capacity} capacity={capacity}, records_per_page={records_per_page},\
                 record_size={}",
                self.record_size
            )))?;
            let raw = data.data();
            // Only the hash prefix needs decoding — see RunTable::
            // insert_raw's own comment. take_from_bytes decodes just that
            // one leading field and hands back the (unexamined,
            // unmodified) remaining bytes.
            let (hash, _rest): (u64, &[u8]) = postcard::take_from_bytes(raw)?;
            new_table.insert_raw(hash, raw)?;
            self.count += 1;
            added += 1;
        }
        // The old RunTable (still owned by self.table until this line)
        // drops here, freeing its pages.
        self.table = BuildTable::Run(new_table);
        self.rehash_time = start.elapsed().as_nanos();
        Ok(())
    }

    // Drains the left source into the table — done lazily on the first
    // next() call (see `built`'s own doc comment), not eagerly in
    // new(). rehash only ever runs from in here (via insert_left), i.e.
    // strictly before any right-side probing starts, so `matched` never
    // needs replaying across a rehash — nothing has been matched yet.
    fn build_left(&mut self) -> Result<(), SchemaError> {
        let mut row = self.sources[0].next()?;
        let start = Instant::now();
        while let Some(r) = row {
            self.insert_left(r)?;
            row = self.sources[0].next()?;
        }
        self.left_time += start.elapsed().as_nanos();
        Ok(())
    }

    // LEFT/FULL only: once the right source is exhausted, every
    // occupied-but-never-matched slot (per `matched`) still owes an
    // output row, paired with right_null. Scans the table page by page,
    // resuming across calls via sweep_page/sweep_row.
    //
    // STORE_AUDIT.md P6: `Run::slots_at` returns only the OCCUPIED slots
    // on a page (each tagged with its own real slot id), so this only
    // ever decodes real entries — no `None` placeholders to skip past
    // the way indexing through a full `Vec<Option<HashValue>>` used to
    // require, which matters most right after a rehash (the fresh
    // capacity is ~2x the live count, so close to half of every page
    // used to be wasted decode work here).
    fn next_unmatched_left(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        // Finds the next never-matched entry, if any, without calling back
        // into `self.emit` (a &self method — would conflict with the
        // in-progress mutable borrow of self.table below) until after the
        // table borrow has ended.
        let found = match &mut self.table {
            BuildTable::Mem(mt) => {
                let mut found = None;
                while self.mem_sweep_pos < mt.entries.len() {
                    let i = self.mem_sweep_pos;
                    self.mem_sweep_pos += 1;
                    if !mt.matched[i] {
                        found = Some(mt.entries[i].left_value.clone());
                        break;
                    }
                }
                found
            }
            BuildTable::Run(rt) => {
                let mut found = None;
                'sweep: loop {
                    if self.sweep_page >= rt.run.page_count() {
                        break;
                    }
                    if self.sweep_page_data.is_none() {
                        let slots = rt.run.slots_at(self.sweep_page)?;
                        let mut decoded = Vec::with_capacity(slots.len());
                        for (slot, bytes) in slots {
                            decoded.push((slot, from_bytes::<HashValue>(&bytes)?));
                        }
                        self.sweep_page_data = Some(decoded);
                        self.sweep_row = 0;
                    }
                    let page_data = self.sweep_page_data.as_ref().unwrap();
                    if self.sweep_row >= page_data.len() {
                        self.sweep_page += 1;
                        self.sweep_row = 0;
                        self.sweep_page_data = None;
                        continue;
                    }
                    let (slot, value) = &page_data[self.sweep_row];
                    let slot_index = self.sweep_page * rt.records_per_page + *slot as usize;
                    self.sweep_row += 1;
                    if !rt.matched.is_set(slot_index) {
                        found = Some(value.left_value.clone());
                        break 'sweep;
                    }
                }
                found
            }
        };
        self.next_time += start.elapsed().as_nanos();
        match found {
            Some(left) => {
                let right_null = self.matcher.right_null().clone();
                Ok(Some(self.emit(&left, &right_null)?))
            }
            None => Ok(None),
        }
    }

    // `build` is a row from the physical left (hash-table) side, `probe`
    // from the physical right (streamed) side. The output is always in the
    // ORIGINAL left ++ right order the rest of the plan was built against:
    // if the sources were swapped, the probe side is the original left.
    fn emit(&self, build: &IndexKey, probe: &IndexKey) -> Result<IndexKey, SchemaError> {
        let (first, second) = if self.swapped {
            (probe, build)
        } else {
            (build, probe)
        };
        self.matcher.combine(first, second)
    }
}

// Temporary stub — Source requires Debug, but Run<F>/Db<F> don't
// implement it (and deriving would also force F: Debug on every
// HashedSource<F> regardless). Real impl can replace this once the
// fields settle; matches the same finish_non_exhaustive() pattern
// RunSource/TableRef already use for the same reason.
impl<F: DBFile + 'static> std::fmt::Debug for HashedSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashedSource").finish_non_exhaustive()
    }
}

impl<F: DBFile + 'static> Source for HashedSource<F> {
    fn plan(&self) -> PlanNode {
        let (left, right) = (self.sources[0].fields(), self.sources[1].fields());
        let name = |fields: &[ProjectableField], i: &usize| {
            fields
                .get(*i)
                .map(|f| f.display_name.clone())
                .unwrap_or_else(|| format!("#{i}"))
        };
        let keys = self
            .left_fields
            .iter()
            .zip(&self.right_fields)
            .map(|(l, r)| format!("build({}) = probe({})", name(&left, l), name(&right, r)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // `join_type` already describes the physical sides (see new()); say
        // so when they were swapped relative to the query text.
        let swapped = if self.swapped { ", sides swapped" } else { "" };
        PlanNode::new("HashJoin")
            .detail(format!("{:?} on {keys}{swapped}", self.join_type))
            .child(self.sources[0].plan().with_role("build"))
            .child(self.sources[1].plan().with_role("probe"))
    }


    fn fields(&self) -> Arc<[super::ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        if !self.built {
            self.build_left()?;
            self.built = true;
        }
        loop {
            if let Some(left) = self.pending_matches.pop_front() {
                let right = self
                    .current_right
                    .as_ref()
                    .expect("pending_matches is only ever populated alongside current_right");
                return Ok(Some(self.emit(&left, right)?));
            }

            if self.right_exhausted {
                return self.next_unmatched_left();
            }

            match self.sources[1].next()? {
                Some(right) => {
                    let matches = self.probe_matches(&right)?;
                    if matches.is_empty() {
                        if self.matcher.keeps_unmatched_right() {
                            return Ok(Some(self.emit(self.matcher.left_null(), &right)?));
                        }
                        // INNER/LEFT: an unmatched right row contributes
                        // nothing — try the next right row.
                        continue;
                    }
                    self.pending_matches = matches;
                    self.current_right = Some(right);
                    // loop back around to pop the first pending match
                }
                None => {
                    self.right_exhausted = true;
                    if !self.matcher.keeps_unmatched_left() {
                        return Ok(None);
                    }
                    // loop back around; next_unmatched_left takes over
                }
            }
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.sources[0].reset()?;
        self.sources[1].reset()?;
        // Back to Mem — the same starting state new() itself builds, so a
        // reset join gets the same in-memory-first chance the first build
        // did, rather than staying Run-backed (with its now-empty, already
        // appropriately-sized table) just because the first pass happened
        // to spill.
        self.table = BuildTable::Mem(MemTable::new());
        self.count = 0;
        self.built = false;
        self.pending_matches = VecDeque::new();
        self.current_right = None;
        self.right_exhausted = false;
        self.sweep_page = 0;
        self.sweep_row = 0;
        self.sweep_page_data = None;
        self.mem_sweep_pos = 0;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        let mut stats = HashMap::new();
        stats.insert("probe_ns".to_string(), self.probe_time as f64);
        stats.insert("rehash_ns".into(), self.rehash_time as f64);
        stats.insert("next_ns".into(), self.next_time as f64);
        stats.insert("build_left_ns".into(), self.left_time as f64);
        stats.insert("insert_left_ns".into(), self.insert_left as f64);
        let this_stats = QueryStats { stats, level: 0 };
        let name = format!("HashJoin:({:?})", self.join_type);
        let mut res = vec![(name, this_stats)];
        for s in &self.sources {
            res = merge_stats(res, s.query_stats())
        }
        Some(res)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        Some(ComputedTableStat {
            table_stat: TableStat {
                col_stats: HashMap::new(),
                id: TableIdType::none(),
                name: "".into(),
                row_count: self.count,
            },
            indices: None,
            self_index: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use store::{db::Db, memfile::MemFile, valueitem::ValueItem};

    use super::*;
    use crate::{
        plan::memory::QueryMemory,
        source::test_support::{VecSource, drain},
    };

    fn make_db() -> Arc<Db<MemFile>> {
        Db::<MemFile>::create("hash_join_test.db").unwrap()
    }

    fn left_rows() -> Vec<Vec<ValueItem>> {
        vec![
            vec![ValueItem::Integer(1), ValueItem::Integer(100)],
            vec![ValueItem::Integer(2), ValueItem::Integer(200)],
            vec![ValueItem::Integer(3), ValueItem::Integer(300)],
        ]
    }

    fn right_rows() -> Vec<Vec<ValueItem>> {
        vec![
            vec![ValueItem::Integer(2), ValueItem::Integer(9002)], // matches left id=2
            vec![ValueItem::Integer(3), ValueItem::Integer(9003)], // matches left id=3
            vec![ValueItem::Integer(99), ValueItem::Integer(9099)], // no match
        ]
    }

    fn left_source() -> Box<dyn Source> {
        Box::new(VecSource::new(&["id", "val"], left_rows()))
    }

    fn right_source() -> Box<dyn Source> {
        Box::new(VecSource::new(&["id", "val"], right_rows()))
    }

    fn make_source_with(join_type: JoinType) -> HashedSource<MemFile> {
        HashedSource::new(
            left_source(),
            right_source(),
            make_db(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            join_type,
        )
        .unwrap()
    }

    fn make_source() -> HashedSource<MemFile> {
        make_source_with(JoinType::Inner)
    }

    // Test-only access into whichever table is currently backing a
    // HashedSource — panics with a clear message if a test that expects
    // (and wants to exercise) the Run-backed table's own sizing/rehash
    // behavior got a Mem table instead, e.g. because it forgot to force a
    // spill (see QueryMemory::new(0) below).
    impl<F: DBFile + 'static> HashedSource<F> {
        fn run_table(&self) -> &RunTable<F> {
            match &self.table {
                BuildTable::Run(rt) => rt,
                BuildTable::Mem(_) => panic!(
                    "expected a Run-backed table — force one with QueryMemory::new(0) \
                     (see spill()) before asserting on Run-specific sizing"
                ),
            }
        }
        fn is_mem(&self) -> bool {
            matches!(self.table, BuildTable::Mem(_))
        }
    }

    // Drives HashedSource::build_left directly (bypassing next()'s probe
    // phase) with a QueryMemory budget of 0 — any Mem reservation attempt
    // fails immediately, so the very first left row forces a migration to
    // Run. For tests that specifically want the Run-backed table's own
    // sizing/rehash behavior, the same thing every HashedSource always did
    // before it could be Mem-backed at all.
    fn spill(mut source: HashedSource<MemFile>) -> HashedSource<MemFile> {
        source.build_left().unwrap();
        source
    }

    // Reads every slot across every page of the run — mirrors the exact
    // per-page decode RunTable::insert_with_hash/probe uses internally,
    // just exposed here so a test can inspect the whole table's final
    // state directly.
    // STORE_AUDIT.md P6: `slots_at` returns only occupied slots directly
    // (no `Option` wrapper needed — see `HashValue`'s own doc comment),
    // so this is a plain flatten now instead of decoding a whole
    // `Vec<Option<HashValue>>` per page.
    fn dump_slots(source: &HashedSource<MemFile>) -> Vec<HashValue> {
        let rt = source.run_table();
        let mut out = vec![];
        for page in 0..rt.run.page_count() {
            for (_, bytes) in rt.run.slots_at(page).unwrap() {
                out.push(from_bytes(&bytes).unwrap());
            }
        }
        out
    }

    #[test]
    fn test_probe_matches_finds_every_left_row_sharing_a_key_not_just_the_first() {
        // Three left rows share id=1, one shares id=2. A right row
        // probing id=1 must get back all three, in some order — not
        // stop at whichever one the linear probe happens to land on
        // first.
        let mut source = make_source();
        for (id, val) in [(1, 10), (1, 20), (1, 30), (2, 200)] {
            let row =
                IndexKey::new_from_owned(vec![ValueItem::Integer(id), ValueItem::Integer(val)])
                    .unwrap();
            source.insert_left(row).unwrap();
        }

        let probe_key =
            IndexKey::new_from_owned(vec![ValueItem::Integer(1), ValueItem::Integer(-1)]).unwrap();
        let matches = source.probe_matches(&probe_key).unwrap();
        let mut vals: Vec<i64> = matches
            .iter()
            .map(|k| match k.values()[1] {
                ValueItem::Integer(v) => v,
                _ => panic!("expected an integer"),
            })
            .collect();
        vals.sort();
        assert_eq!(vals, vec![10, 20, 30], "matches: {matches:?}");
    }

    #[test]
    fn test_probe_matches_returns_empty_for_a_key_with_no_matches() {
        let mut source = make_source();
        source
            .insert_left(
                IndexKey::new_from_owned(vec![ValueItem::Integer(1), ValueItem::Integer(10)])
                    .unwrap(),
            )
            .unwrap();
        let probe_key =
            IndexKey::new_from_owned(vec![ValueItem::Integer(99), ValueItem::Integer(-1)]).unwrap();
        assert!(source.probe_matches(&probe_key).unwrap().is_empty());
    }

    #[test]
    fn test_insert_left_triggers_a_rehash_once_capacity_is_reached() {
        // Small page size so `capacity` is small enough to actually fill
        // in a test, instead of needing hundreds of rows. QueryMemory::
        // new(0) forces this build onto the Run-backed table (see
        // run_table()'s own comment) — this specifically tests THAT
        // table's rehash-at-100%-load behavior.
        let db = Db::<MemFile>::create_with_page_size("hash_rehash_test.db", 1024).unwrap();
        let mem = QueryMemory::new(0);
        let mut source = HashedSource::new(
            Box::new(VecSource::new(&["id", "val"], vec![])),
            Box::new(VecSource::new(&["id", "val"], vec![])),
            db,
            mem,
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();

        // The first insert forces the Mem->Run migration (any reservation
        // fails immediately against a 0 budget); capacity is only known
        // once that has happened.
        source
            .insert_left(IndexKey::new_from_owned(vec![ValueItem::Integer(0), ValueItem::Integer(0)]).unwrap())
            .unwrap();
        let initial_capacity = source.run_table().capacity;
        // The count==capacity check runs at the START of insert_left, so
        // it only fires on the (capacity+1)th call — insert one more than
        // capacity in total, not exactly capacity (one row already went
        // in above, so this loop covers the rest), to actually trigger it.
        for i in 1..=initial_capacity as i64 {
            let row = IndexKey::new_from_owned(vec![ValueItem::Integer(i), ValueItem::Integer(0)])
                .unwrap();
            source.insert_left(row).unwrap();
        }

        assert!(
            source.run_table().capacity > initial_capacity,
            "inserting capacity+1 distinct rows should have triggered a rehash that grows \
             capacity — it stayed at {initial_capacity}, meaning self.count never actually \
             reached capacity"
        );
    }

    // Beyond just "capacity grows": every left row inserted before the
    // rehash must still be present afterward (rehash's own replay loop
    // reinserts them), spanning multiple pages — the exact case that
    // used to panic on `page_index < self.run.page_count()` before
    // allocate_run existed to actually grow the run's page count to
    // match a grown capacity. Matching correctness across a grown,
    // multi-page table is covered separately by
    // test_next_finds_every_match_after_growing_across_multiple_pages,
    // which goes through the full next() pipeline.
    #[test]
    fn test_rehash_preserves_every_left_row_across_multiple_pages() {
        let db = Db::<MemFile>::create_with_page_size("hash_rehash_test2.db", 1024).unwrap();
        let mem = QueryMemory::new(0);
        let mut source = HashedSource::new(
            Box::new(VecSource::new(&["id", "val"], vec![])),
            Box::new(VecSource::new(&["id", "val"], vec![])),
            db,
            mem,
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();

        // id=0 forces the Mem->Run migration; capacity is only known once
        // that has happened. The rest of the sequence (1..n below) picks
        // up where it left off, so the final present-ids set is still
        // exactly 0..n.
        source
            .insert_left(IndexKey::new_from_owned(vec![ValueItem::Integer(0), ValueItem::Integer(0)]).unwrap())
            .unwrap();
        let initial_capacity = source.run_table().capacity;
        // Enough distinct rows to force at least one rehash, landing the
        // table on more than one page.
        let n = initial_capacity as i64 * 3 + 1;
        for i in 1..n {
            let row =
                IndexKey::new_from_owned(vec![ValueItem::Integer(i), ValueItem::Integer(i * 10)])
                    .unwrap();
            source.insert_left(row).unwrap();
        }
        assert!(
            source.run_table().run.page_count() > 1,
            "expected growth to span multiple pages; page_count={}",
            source.run_table().run.page_count()
        );

        let slots = dump_slots(&source);
        let present_ids: std::collections::HashSet<i64> = slots
            .iter()
            .map(|v| match &v.left_value.values()[0] {
                ValueItem::Integer(i) => *i,
                other => panic!("unexpected key type: {other:?}"),
            })
            .collect();
        assert_eq!(
            present_ids,
            (0..n).collect(),
            "every inserted row must survive the rehash, exactly once each"
        );
    }

    #[test]
    fn test_next_yields_only_matched_pairs_as_combined_left_then_right_rows() {
        let mut source = make_source();
        let rows = drain(&mut source);
        assert_eq!(
            rows.len(),
            2,
            "only the 2 matching pairs should be emitted: {rows:?}"
        );
        assert!(rows.contains(&vec![
            ValueItem::Integer(2),
            ValueItem::Integer(200),
            ValueItem::Integer(2),
            ValueItem::Integer(9002),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(3),
            ValueItem::Integer(300),
            ValueItem::Integer(3),
            ValueItem::Integer(9003),
        ]));
    }

    #[test]
    fn test_next_yields_nothing_when_no_rows_match() {
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(1)]],
        ));
        let right = Box::new(VecSource::new(
            &["id", "val"],
            vec![vec![ValueItem::Integer(2), ValueItem::Integer(2)]],
        ));
        let mut source = HashedSource::new(
            left,
            right,
            make_db(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(drain(&mut source), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_reset_allows_a_full_rescan_with_identical_results() {
        let mut source = make_source();
        let first_pass = drain(&mut source);
        assert_eq!(first_pass.len(), 2);

        source.reset().unwrap();
        let second_pass = drain(&mut source);
        assert_eq!(
            second_pass, first_pass,
            "a reset source must reproduce the exact same output on a second scan"
        );
    }

    #[test]
    fn test_next_finds_every_match_after_growing_across_multiple_pages() {
        let db =
            Db::<MemFile>::create_with_page_size("hash_next_multi_page_test.db", 1024).unwrap();
        let n: i64 = 30; // enough distinct keys to force a rehash at 1024-byte pages
        let left_rows: Vec<Vec<ValueItem>> = (0..n)
            .map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(i * 10)])
            .collect();
        let right_rows: Vec<Vec<ValueItem>> = (0..n)
            .step_by(2)
            .map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(-i)])
            .collect();
        let left = Box::new(VecSource::new(&["id", "val"], left_rows));
        let right = Box::new(VecSource::new(&["id", "val"], right_rows));
        let mut source = HashedSource::new(
            left,
            right,
            db,
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();

        let rows = drain(&mut source);
        assert_eq!(rows.len(), (0..n).step_by(2).count());
        for row in &rows {
            let ValueItem::Integer(left_id) = row[0] else {
                panic!("expected an integer id, got {:?}", row[0])
            };
            let ValueItem::Integer(right_id) = row[2] else {
                panic!("expected an integer id, got {:?}", row[2])
            };
            assert_eq!(
                left_id, right_id,
                "a joined row's left/right ids must match"
            );
            assert_eq!(
                left_id % 2,
                0,
                "only even ids were ever inserted on the right side"
            );
        }
    }

    #[test]
    fn test_next_emits_one_row_per_match_when_multiple_right_rows_share_a_left_key() {
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(100)]],
        ));
        let right = Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(9001)],
                vec![ValueItem::Integer(1), ValueItem::Integer(9002)],
                vec![ValueItem::Integer(1), ValueItem::Integer(9003)],
            ],
        ));
        let mut source = HashedSource::new(
            left,
            right,
            make_db(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        let rows = drain(&mut source);
        let amounts: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| match r[3] {
                ValueItem::Integer(v) => v,
                _ => panic!("expected an integer"),
            })
            .collect();
        assert_eq!(
            amounts,
            [9001, 9002, 9003].into_iter().collect(),
            "every right row matching the same left key must produce its own output row: \
             {rows:?}"
        );
    }

    #[test]
    fn test_next_emits_one_row_per_match_when_multiple_left_rows_share_a_key() {
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(10)],
                vec![ValueItem::Integer(1), ValueItem::Integer(20)],
                vec![ValueItem::Integer(1), ValueItem::Integer(30)],
            ],
        ));
        let right = Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(9001)]],
        ));
        let mut source = HashedSource::new(
            left,
            right,
            make_db(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        let rows = drain(&mut source);
        let vals: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| match r[1] {
                ValueItem::Integer(v) => v,
                _ => panic!("expected an integer"),
            })
            .collect();
        assert_eq!(
            vals,
            [10, 20, 30].into_iter().collect(),
            "every left row sharing the matched key must produce its own output row: {rows:?}"
        );
    }

    #[test]
    fn test_left_join_emits_unmatched_left_rows_paired_with_nulls() {
        let mut source = make_source_with(JoinType::Left);
        let rows = drain(&mut source);
        assert_eq!(rows.len(), 3, "{rows:?}"); // ids 1,2,3 — 1 unmatched, 2&3 matched
        assert!(rows.contains(&vec![
            ValueItem::Integer(1),
            ValueItem::Integer(100),
            ValueItem::Null,
            ValueItem::Null,
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(2),
            ValueItem::Integer(200),
            ValueItem::Integer(2),
            ValueItem::Integer(9002),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(3),
            ValueItem::Integer(300),
            ValueItem::Integer(3),
            ValueItem::Integer(9003),
        ]));
    }

    #[test]
    fn test_right_join_emits_unmatched_right_rows_paired_with_nulls() {
        let mut source = make_source_with(JoinType::Right);
        let rows = drain(&mut source);
        assert_eq!(rows.len(), 3, "{rows:?}"); // right rows 2,3,99 — 99 unmatched
        assert!(rows.contains(&vec![
            ValueItem::Integer(2),
            ValueItem::Integer(200),
            ValueItem::Integer(2),
            ValueItem::Integer(9002),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(3),
            ValueItem::Integer(300),
            ValueItem::Integer(3),
            ValueItem::Integer(9003),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Integer(99),
            ValueItem::Integer(9099),
        ]));
    }

    #[test]
    fn test_full_join_emits_unmatched_rows_from_both_sides() {
        let mut source = make_source_with(JoinType::Full);
        let rows = drain(&mut source);
        // matched: id=2, id=3 (2 rows); unmatched left: id=1 (1 row);
        // unmatched right: user_id=99 (1 row) = 4 total.
        assert_eq!(rows.len(), 4, "{rows:?}");
        assert!(rows.contains(&vec![
            ValueItem::Integer(1),
            ValueItem::Integer(100),
            ValueItem::Null,
            ValueItem::Null,
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(2),
            ValueItem::Integer(200),
            ValueItem::Integer(2),
            ValueItem::Integer(9002),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(3),
            ValueItem::Integer(300),
            ValueItem::Integer(3),
            ValueItem::Integer(9003),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Integer(99),
            ValueItem::Integer(9099),
        ]));
    }

    #[test]
    fn test_left_join_reset_allows_a_full_rescan_with_identical_results() {
        let mut source = make_source_with(JoinType::Left);
        let first_pass = drain(&mut source);
        assert_eq!(first_pass.len(), 3);

        source.reset().unwrap();
        let second_pass = drain(&mut source);
        assert_eq!(second_pass, first_pass);
    }

    // STORE_AUDIT.md P6 — throwaway (not a committed criterion bench,
    // same convention as this session's other direct microbenchmarks) an
    // end-to-end measurement of a non-trivial hash join: 50,000 distinct
    // left rows (several fields each, forcing multiple rehash-driven
    // page-chain growths along the way) 1:1-joined against 50,000
    // probing right rows — every right row finds exactly one match, the
    // common real-world FK-join shape (e.g. orders 1:1-joined against
    // one order_detail apiece). Exercises build_left's insert-heavy path
    // and next()'s probe-heavy path in roughly equal measure. Run with:
    //   cargo test -p squeal-sql --lib --release -- --ignored --nocapture \
    //     source::hash::tests::bench_hash_join_50k_rows
    #[test]
    #[ignore]
    fn bench_hash_join_50k_rows() {
        const N: i64 = 50_000;
        let left_rows: Vec<Vec<ValueItem>> = (0..N)
            .map(|i| {
                vec![
                    ValueItem::Integer(i),
                    ValueItem::Integer(i * 7),
                    ValueItem::Str((format!("left-row-{i}"), 32)),
                ]
            })
            .collect();
        let right_rows: Vec<Vec<ValueItem>> = (0..N)
            .map(|i| {
                vec![
                    ValueItem::Integer(i),
                    ValueItem::Str((format!("right-row-{i}"), 32)),
                ]
            })
            .collect();
        let left = Box::new(VecSource::new(&["id", "val", "name"], left_rows));
        let right = Box::new(VecSource::new(&["id", "name"], right_rows));
        let mut source = HashedSource::new(
            left,
            right,
            make_db(),
            QueryMemory::new(64 * 1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();

        let start = std::time::Instant::now();
        let rows = drain(&mut source);
        let elapsed = start.elapsed();
        assert_eq!(
            rows.len(),
            N as usize,
            "every right row must find its one match"
        );
        eprintln!(
            "bench_hash_join_50k_rows: {N} rows each side, {} output rows in {elapsed:?} \
             ({:.0} joined rows/s) — build_left={}ms probe={}ms next={}ms rehash={}ms",
            rows.len(),
            rows.len() as f64 / elapsed.as_secs_f64(),
            source.left_time / 1_000_000,
            source.probe_time / 1_000_000,
            source.next_time / 1_000_000,
            source.rehash_time / 1_000_000,
        );
    }

    // Scratch investigation, not a permanent benchmark: insert_left only
    // rehashes at `self.count == self.capacity` (100% load factor —
    // deliberately, see its own comment for why a lower threshold was
    // tried and reverted), and find_next_slot (ds::bitvec::BitVec::
    // first_available) is a plain linear probe with no clustering
    // mitigation (no double hashing/quadratic step). Filling a
    // linearly-probed open-addressed table all the way to 100% load is a
    // textbook Theta(n^1.5) operation
    // (Knuth's classic linear-probing analysis), not Theta(n) — probe
    // length blows up as load factor approaches 1. This measures ns/row
    // inserted at several scales to see whether that's actually showing
    // up here (flat ns/row = healthy amortized O(1); growing ns/row = the
    // theory confirmed). One next() call is enough to trigger the full
    // (lazy) build_left phase without spending time probing.
    // QueryMemory::new(0) forces the Run-backed table from the start (this
    // benchmark is about ITS load-factor scaling specifically — at these
    // row counts a generous budget would just stay in memory and never
    // touch it at all). Run with:
    //   cargo test -p squeal-sql --lib --release -- --ignored --nocapture \
    //     source::hash::tests::bench_insert_left_scaling_isolates_load_factor_effects
    #[test]
    #[ignore]
    fn bench_insert_left_scaling_isolates_load_factor_effects() {
        for n in [10_000i64, 50_000, 200_000, 800_000] {
            let left_rows: Vec<Vec<ValueItem>> =
                (0..n).map(|i| vec![ValueItem::Integer(i)]).collect();
            let right_rows: Vec<Vec<ValueItem>> = vec![vec![ValueItem::Integer(0)]];
            let left = Box::new(VecSource::new(&["id"], left_rows));
            let right = Box::new(VecSource::new(&["id"], right_rows));
            let mut source = HashedSource::new(
                left,
                right,
                make_db(),
                QueryMemory::new(0),
                &[0],
                &[0],
                JoinType::Inner,
            )
            .unwrap();
            let _ = source.next().unwrap();
            let ns_per_row = source.insert_left as f64 / n as f64;
            eprintln!(
                "n={n:>8}  insert_left={:>9.2}ms  {:>6.1} ns/row  rehash={:>7.2}ms  \
                 build_left_total={:>9.2}ms",
                source.insert_left as f64 / 1_000_000.0,
                ns_per_row,
                source.rehash_time as f64 / 1_000_000.0,
                source.left_time as f64 / 1_000_000.0,
            );
        }
    }

    // Regression test for the data-loss bug documented on rehash()'s own
    // doc comment: building a HashedSource's left table past 272,384 rows
    // used to reliably panic ("Did not expect empty run tuple") once
    // count crossed that boundary and triggered a rehash to 544,768 —
    // that exact boundary was specific to the old 100%-load-factor
    // trigger (272384 = 2048 pages * the old records_per_page of 133);
    // now that rehash() fires at 70% load instead, the trigger points
    // have shifted, but 800K rows still drives many rehash cycles well
    // past the scale the original bug needed to surface. Passes reliably
    // now (see rehash()'s own comment for the fix).
    #[test]
    #[ignore]
    fn bug_repro_rehash_past_272384_rows_loses_entries() {
        const N: i64 = 800_000;
        let left_rows: Vec<Vec<ValueItem>> = (0..N).map(|i| vec![ValueItem::Integer(i)]).collect();
        let right_rows: Vec<Vec<ValueItem>> = vec![vec![ValueItem::Integer(0)]];
        let left = Box::new(VecSource::new(&["id"], left_rows));
        let right = Box::new(VecSource::new(&["id"], right_rows));
        // QueryMemory::new(0) forces the Run-backed table this regression
        // is about — 800K rows would otherwise just stay in memory.
        let mut source = HashedSource::new(
            left,
            right,
            make_db(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        // Triggers the lazy build_left phase in full (see next()'s own
        // doc comment).
        let _ = source.next().unwrap();
    }

    // --- build-side selection by table stats ---

    // Delegates to a VecSource but reports a chosen row_count, standing in
    // for a TableSource whose table has that many rows.
    #[derive(Debug)]
    struct WithRowCount(Box<dyn Source>, usize);

    impl Source for WithRowCount {
        fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
            self.0.next()
        }
        fn fields(&self) -> Arc<[ProjectableField]> {
            self.0.fields()
        }
        fn reset(&mut self) -> Result<(), SchemaError> {
            self.0.reset()
        }
        fn table_stats(&self) -> Option<ComputedTableStat> {
            Some(ComputedTableStat {
                table_stat: crate::optim::table_stats::TableStat {
                    id: store::table::TableIdType::none(),
                    name: "t".into(),
                    row_count: self.1,
                    col_stats: HashMap::new(),
                },
                indices: None,
                self_index: None,
            })
        }
    }

    fn drain_sorted(mut source: HashedSource<MemFile>) -> Vec<Vec<ValueItem>> {
        let mut rows = vec![];
        while let Some(r) = source.next().unwrap() {
            rows.push(r.values().to_vec());
        }
        rows.sort();
        rows
    }

    // left_rows() has 3 rows and right_rows() has 3; report the RIGHT as far
    // bigger so new() swaps, or the reverse so it must not.
    fn join_with_counts(join_type: JoinType, left: usize, right: usize) -> HashedSource<MemFile> {
        HashedSource::new(
            Box::new(WithRowCount(left_source(), left)),
            Box::new(WithRowCount(right_source(), right)),
            make_db(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            join_type,
        )
        .unwrap()
    }

    // Like join_with_counts, but forced onto the Run-backed table (budget
    // 0) and built — for tests checking that table's own sizing, which
    // (unlike swapped/join-result tests) only exists once something has
    // actually been inserted into it.
    fn spilled_join_with_counts(join_type: JoinType, left: usize, right: usize) -> HashedSource<MemFile> {
        spill(HashedSource::new(
            Box::new(WithRowCount(left_source(), left)),
            Box::new(WithRowCount(right_source(), right)),
            make_db(),
            QueryMemory::new(0),
            &[0],
            &[0],
            join_type,
        )
        .unwrap())
    }

    #[test]
    fn test_a_larger_right_side_is_swapped_and_a_larger_left_is_not() {
        assert!(join_with_counts(JoinType::Inner, 1, 1000).swapped);
        assert!(!join_with_counts(JoinType::Inner, 1000, 1).swapped);
        assert!(
            !join_with_counts(JoinType::Inner, 5, 5).swapped,
            "ties keep the order"
        );
        // Without stats on both sides there is nothing to decide with.
        assert!(!make_source().swapped);
    }

    // The point of the whole thing: swapping must not change the RESULT —
    // same rows, same column order (original left ++ original right), and the
    // same side preserved by an outer join — for every join type.
    #[test]
    fn test_swapping_sides_never_changes_the_join_result() {
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
        ] {
            let baseline = drain_sorted(make_source_with(join_type));
            let swapped_source = join_with_counts(join_type, 1, 1000);
            assert!(swapped_source.swapped);
            assert_eq!(
                drain_sorted(swapped_source),
                baseline,
                "{join_type:?}: a swapped join must return exactly what the unswapped one does"
            );
        }
    }

    #[test]
    fn test_a_swapped_join_keeps_declared_column_order_in_its_rows() {
        // left = (id, val) 1..3 with val 100s; right = (id, val) with val
        // 9000s. Whatever side was built, columns 0-1 are LEFT's, 2-3 RIGHT's.
        let rows = drain_sorted(join_with_counts(JoinType::Inner, 1, 1000));
        assert_eq!(
            rows,
            vec![
                vec![
                    ValueItem::Integer(2),
                    ValueItem::Integer(200),
                    ValueItem::Integer(2),
                    ValueItem::Integer(9002)
                ],
                vec![
                    ValueItem::Integer(3),
                    ValueItem::Integer(300),
                    ValueItem::Integer(3),
                    ValueItem::Integer(9003)
                ],
            ]
        );
    }

    #[test]
    fn test_a_swapped_left_join_still_preserves_the_original_left_side() {
        // Original LEFT JOIN: left id=1 has no right match and must appear
        // with a NULL right half; right id=99 must NOT appear at all.
        let rows = drain_sorted(join_with_counts(JoinType::Left, 1, 1000));
        assert!(rows.contains(&vec![
            ValueItem::Integer(1),
            ValueItem::Integer(100),
            ValueItem::Null,
            ValueItem::Null
        ]));
        assert!(!rows.iter().any(|r| r[2] == ValueItem::Integer(99)));
        assert_eq!(rows.len(), 3);
    }

    // --- initial sizing from the build side's stats ---

    fn wanted_slots(rows: usize) -> usize {
        (rows as f64 * HashedSource::<MemFile>::INITIAL_CAPACITY_FACTOR) as usize
    }

    #[test]
    fn test_the_table_is_sized_from_the_build_side_when_the_sources_are_not_swapped() {
        // Left (build) is the big table: 30,271 rows vs 10,000 — the ordinary
        // order-details-joins-orders shape. This used to stay at one page.
        // Forced onto the Run-backed table (QueryMemory::new(0)) — this is
        // specifically testing THAT table's own sizing, which today's
        // in-memory-first build only reaches once it has spilled.
        let s = spilled_join_with_counts(JoinType::Inner, 30271, 10000);
        assert!(!s.swapped);
        let rt = s.run_table();
        assert!(rt.capacity >= wanted_slots(30271), "capacity {}", rt.capacity);
        assert_eq!(rt.capacity % rt.records_per_page, 0, "whole pages of slots");
        // ...and not wildly more than asked for: at most one extra page.
        assert!(rt.capacity < wanted_slots(30271) + rt.records_per_page);
    }

    #[test]
    fn test_the_table_is_sized_from_the_build_side_when_the_sources_are_swapped() {
        // Right is bigger, so it becomes the build side: same table, same size.
        let swapped = spilled_join_with_counts(JoinType::Inner, 10000, 30271);
        assert!(swapped.swapped);
        let plain = spilled_join_with_counts(JoinType::Inner, 30271, 10000);
        assert_eq!(swapped.run_table().capacity, plain.run_table().capacity);
    }

    #[test]
    fn test_a_build_side_without_stats_starts_at_one_page() {
        // A generous budget never spills a table this small (confirmed by
        // test_construction_allocates_only_the_pages_the_run_needs) — force
        // it via 0 to see what the Run-backed table would have started at.
        let s = spill(HashedSource::new(
            left_source(),
            right_source(),
            make_db(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap());
        let rt = s.run_table();
        assert_eq!(rt.capacity, rt.records_per_page);
        // Only the (post-swap) build side's stats matter; a probe side that
        // reports none must not stop it being sized.
        let build_only = spill(HashedSource::new(
            Box::new(WithRowCount(left_source(), 5000)),
            right_source(),
            make_db(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap());
        assert!(build_only.run_table().capacity >= wanted_slots(5000));
    }

    #[test]
    fn test_construction_allocates_only_the_pages_the_run_needs() {
        // A generous budget never touches a Run at all — construction, and
        // even the whole build, stays in memory (see BuildTable's own doc
        // comment).
        let db = make_db();
        let main_before = db.page_count();
        let before = db.stats().temp.live_pages;
        let mut s = HashedSource::new(
            left_source(),
            right_source(),
            db.clone(),
            QueryMemory::new(1024 * 1024),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        s.build_left().unwrap();
        assert!(s.is_mem(), "a small build under a generous budget must never spill");
        assert_eq!(
            db.stats().temp.live_pages,
            before,
            "and so must not allocate a single Run page"
        );

        // Forced to spill (budget 0): used to create a throwaway one-page
        // run alongside the real one, so every join construction allocated
        // (at least) one page too many. Now nothing is allocated until (and
        // unless) a build actually needs it, so this checks the page count
        // right where that now happens.
        let before = db.stats().temp.live_pages;
        let small = spill(HashedSource::new(
            left_source(),
            right_source(),
            db.clone(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap());
        let rt = small.run_table();
        assert_eq!(
            db.stats().temp.live_pages - before,
            (rt.capacity / rt.records_per_page) as u64
        );

        let before = db.stats().temp.live_pages;
        let big = spill(HashedSource::new(
            Box::new(WithRowCount(left_source(), 5000)),
            Box::new(WithRowCount(right_source(), 10)),
            db.clone(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap());
        let rt = big.run_table();
        let pages = rt.capacity / rt.records_per_page;
        assert!(pages > 1);
        assert_eq!(db.stats().temp.live_pages - before, pages as u64);
        // The join's pages are scratch: they never touch the database file.
        assert_eq!(db.page_count(), main_before);
    }

    // The behavior the sizing exists for: a build whose row count was
    // reported up front never rehashes, while the same build with no stats
    // has to grow by doubling. Forced onto the Run-backed table throughout
    // (QueryMemory::new(0)) — this specifically tests THAT table's sizing.
    #[test]
    fn test_a_build_sized_from_stats_does_not_rehash_but_an_unsized_one_does() {
        let n = 1000usize;
        let rows = |n: usize| -> Vec<Vec<ValueItem>> {
            (0..n as i64)
                .map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(i * 10)])
                .collect()
        };
        let build = |with_stats: bool| -> HashedSource<MemFile> {
            let left: Box<dyn Source> = Box::new(VecSource::new(&["id", "val"], rows(n)));
            let left: Box<dyn Source> = if with_stats {
                Box::new(WithRowCount(left, n))
            } else {
                left
            };
            HashedSource::new(
                left,
                Box::new(VecSource::new(&["id", "val"], rows(1))),
                make_db(),
                QueryMemory::new(0),
                &[0],
                &[0],
                JoinType::Inner,
            )
            .unwrap()
        };

        // next()'s own lazy build (not the spill() helper, which would
        // build a second time on top and double-count nothing meaningful
        // but is unnecessary here) drains the build side in full.
        let mut sized = build(true);
        while sized.next().unwrap().is_some() {}
        assert_eq!(sized.count, n);
        let rt = sized.run_table();
        assert!(
            rt.capacity >= wanted_slots(n) && rt.capacity < wanted_slots(n) + rt.records_per_page,
            "a correctly pre-sized build must never rehash past its initial stats-based size: \
             capacity={}",
            rt.capacity
        );

        let mut unsized_ = build(false);
        while unsized_.next().unwrap().is_some() {}
        assert_eq!(unsized_.count, n);
        let rt = unsized_.run_table();
        assert!(
            rt.capacity > rt.records_per_page,
            "with no stats the build has to grow past its one-page start by rehashing"
        );
    }

    // The hash table lives in the database's scratch pool, which spills to its
    // own file under memory pressure: a join whose table is many times the
    // cache must still be correct, and must never touch the main file.
    #[test]
    fn test_a_join_larger_than_the_temp_cache_spills_and_stays_correct() {
        let n = 4_000usize;
        let db = make_db();
        db.set_temp_cache_bytes(4 * 4096);
        let main_before = db.page_count();
        let rows = |n: usize| -> Vec<Vec<ValueItem>> {
            (0..n as i64)
                .map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(i * 10)])
                .collect()
        };
        // Forced onto the Run-backed table (budget 0) — this test is about
        // THAT table's own pages spilling to the temp file under the pool's
        // cache pressure, a different mechanism from (and downstream of)
        // QueryMemory: a build this size would otherwise just stay in
        // memory and never touch the temp pool at all.
        let mut join = HashedSource::new(
            Box::new(VecSource::new(&["id", "val"], rows(n))),
            Box::new(VecSource::new(&["id", "val"], rows(n))),
            db.clone(),
            QueryMemory::new(0),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        let mut matched = 0;
        while join.next().unwrap().is_some() {
            matched += 1;
        }
        assert_eq!(matched, n);
        let temp = db.stats().temp;
        assert!(temp.spills > 0 && temp.cached_pages <= 4, "{temp:?}");
        assert_eq!(db.page_count(), main_before);
        drop(join);
        assert_eq!(db.stats().temp.live_pages, 0);
        assert_eq!(
            db.stats().temp.file_bytes,
            0,
            "the file is given back once idle"
        );
    }
}

