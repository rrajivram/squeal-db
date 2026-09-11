use std::{collections::VecDeque, sync::Arc};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    run::Run,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    ds::bitvec::BitVec,
    error::SchemaError,
    plan::memory::QueryMemory,
    source::{ProjectableField, Source, join::JoinType},
};

pub(crate) struct HashedSource<F: DBFile + 'static> {
    sources: Vec<Box<dyn Source>>,
    capacity: usize,
    db: Arc<Db<F>>,
    run: Run<F>,
    record_size: usize,
    left_fields: Vec<usize>,
    right_fields: Vec<usize>,
    fields: Arc<[ProjectableField]>,
    count: usize,
    bitmask: BitVec,
    // Parallel to `bitmask` (same size, recreated alongside it in
    // new()/rehash()/reset()): has this OCCUPIED slot ever been matched
    // by a probing right row? Only meaningful for LEFT/FULL, which need
    // to emit every left row that never matched anything once the right
    // source is exhausted — see `next_unmatched_left`.
    matched: BitVec,
    records_per_page: usize,
    mem: Arc<QueryMemory>,
    join_type: JoinType,
    // All-NULL rows shaped like the left/right side's own columns —
    // what LEFT/RIGHT/FULL pair an unmatched row from the OTHER side
    // with (e.g. RIGHT JOIN: a right row with no matching left row is
    // still emitted, paired with `left_null`).
    left_null: IndexKey,
    right_null: IndexKey,
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
    sweep_row: usize,
    sweep_page_data: Option<Vec<Option<HashValue>>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct HashValue {
    hash: u64,
    left_value: Option<IndexKey>,
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
        let left_null = IndexKey::new_from_owned(vec![ValueItem::Null; left_source.fields().len()])?;
        let right_null =
            IndexKey::new_from_owned(vec![ValueItem::Null; right_source.fields().len()])?;
        let (run, records_per_page, num_pages) = Self::allocate_run(&db, record_size, 1)?;
        let capacity = records_per_page * num_pages;
        Ok(Self {
            sources: vec![left_source, right_source],
            fields,
            capacity,
            run,
            record_size,
            db,
            left_fields: left_fields.to_vec(),
            right_fields: right_fields.to_vec(),
            count: 0,
            bitmask: BitVec::with_capacity(capacity),
            matched: BitVec::with_capacity(capacity),
            records_per_page,
            mem,
            join_type,
            left_null,
            right_null,
            built: false,
            pending_matches: VecDeque::new(),
            current_right: None,
            right_exhausted: false,
            sweep_page: 0,
            sweep_row: 0,
            sweep_page_data: None,
        })
    }

    // Creates a Run with enough pages to hold at least `min_capacity`
    // slots, every page pre-initialized to `records_per_page` empty
    // (`None`) slots — mirrors what a prior version of this used a
    // dedicated Run::new_from for (creating and initializing every page
    // up front); reimplemented here via new_page() + set_content_at()
    // now that Run's own API only ever creates a single page up front.
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
    fn allocate_run(
        db: &Arc<Db<F>>,
        record_size: usize,
        min_capacity: usize,
    ) -> Result<(Run<F>, usize, usize), SchemaError> {
        let mut run = db.create_run()?;
        let records_per_page = run.data_size() as usize / (record_size + size_of::<Option<u64>>());
        assert!(
            records_per_page > 0,
            "a single page must be able to hold at least one hash slot"
        );
        let num_pages = min_capacity.max(1).div_ceil(records_per_page).max(1);
        let empty_page = to_allocvec(&vec![Option::<HashValue>::None; records_per_page])?;
        // create() already allocated page 0 — only need num_pages - 1 more.
        for _ in 1..num_pages {
            run.new_page()?;
        }
        for i in 0..num_pages {
            run.set_content_at(i, &empty_page)?;
        }
        Ok((run, records_per_page, num_pages))
    }

    // Decodes one page's slots — the common step behind every read of
    // the table (insert, probe, the unmatched-left sweep). Panics if
    // the page was never initialized, which should be impossible:
    // every page this struct ever reads from was allocated (and
    // pre-filled with empty slots) by allocate_run.
    fn load_page(&self, page_index: usize) -> Result<Vec<Option<HashValue>>, SchemaError> {
        let data = self
            .run
            .get_content_at(page_index)?
            .expect("every page allocated by allocate_run was initialized with content");
        Ok(from_bytes(&data)?)
    }

    #[inline(always)]
    fn slot_available(&self, index: usize) -> Result<bool, SchemaError> {
        Ok(!self.bitmask.is_set(index))
    }

    fn find_next_slot(&self, index: usize) -> Result<usize, SchemaError> {
        self.bitmask
            .first_available(index)
            .ok_or(SchemaError::UnknownError("Could not find any slots".into()))
    }

    fn claim_slot(&mut self, index: usize) -> Result<(), SchemaError> {
        self.bitmask.set(index);
        assert!(!self.slot_available(index)?);
        Ok(())
    }

    fn insert_left(&mut self, item: IndexKey) -> Result<(), SchemaError> {
        if self.count == self.capacity {
            self.rehash(self.capacity * 2)?;
        }
        let hash = self.get_hash(&item, &self.left_fields);

        self.insert_with_hash(item, hash)?;
        self.count += 1;

        Ok(())
    }

    fn are_keys_equal(
        &self,
        lhs: &IndexKey,
        rhs: &IndexKey,
        left_fields: &[usize],
        right_fields: &[usize],
    ) -> bool {
        left_fields
            .iter()
            .zip(right_fields.iter())
            .all(|(l, r)| lhs.values()[*l] == rhs.values()[*r])
    }

    // Finds every left row matching `right`'s join key, walking the
    // FULL open-addressing probe chain starting at its hash's natural
    // slot — not stopping at the first hit, since more than one left
    // row can legitimately share the same key. The chain ends the
    // moment an empty slot is reached: this table never deletes
    // entries, so (standard open-addressing property) if a slot were
    // ever empty, any insert whose probe sequence passes through it
    // would have claimed it directly rather than skipping past — an
    // empty slot is therefore a reliable "nothing further on this
    // chain" signal, not just "nothing here." The `index == start`
    // check guards the degenerate case where the table is 100% full
    // (right before insert_left's next call would rehash) and would
    // otherwise wrap forever.
    //
    // Marks every match found in `matched` as it goes — LEFT/FULL's
    // final unmatched-left sweep (next_unmatched_left) relies on this
    // to know which occupied slots were never claimed by any right row.
    fn probe_matches(&mut self, right: &IndexKey) -> Result<VecDeque<IndexKey>, SchemaError> {
        let hash = self.get_hash(right, &self.right_fields);
        let start = (hash % self.capacity as u64) as usize;
        let mut matches = VecDeque::new();
        let mut index = start;
        let mut page_index = index / self.records_per_page;
        let mut page_data = self.load_page(page_index)?;
        loop {
            let row_index = index % self.records_per_page;
            if let Some(v) = &page_data[row_index] {
                if let Some(left) = &v.left_value
                    && self.are_keys_equal(left, right, &self.left_fields, &self.right_fields)
                {
                    matches.push_back(left.clone());
                    self.matched.set(index);
                }
            } else {
                break;
            }
            index = (index + 1) % self.capacity;
            if index == start {
                break;
            }
            let new_page_index = index / self.records_per_page;
            if new_page_index != page_index {
                page_index = new_page_index;
                page_data = self.load_page(page_index)?;
            }
        }
        Ok(matches)
    }

    fn insert_with_hash(&mut self, item: IndexKey, hash: u64) -> Result<(), SchemaError> {
        let mut index = (hash % self.capacity as u64) as usize;
        if !self.slot_available(index)? {
            index = self.find_next_slot(index)?;
        }
        self.claim_slot(index)?;
        let page_index = index / self.records_per_page;
        let mut vec_data = self.load_page(page_index)?;
        let row_index = index % self.records_per_page;
        assert!(vec_data[row_index].is_none());
        vec_data[row_index] = Some(HashValue {
            hash,
            left_value: Some(item),
        });
        self.run
            .set_content_at(page_index, &to_allocvec(&vec_data)?)?;
        Ok(())
    }

    fn get_hash(&self, key: &IndexKey, fields: &[usize]) -> u64 {
        IndexKey::hash_fields(fields.iter().map(|f| &key.values()[*f]))
    }

    fn rehash(&mut self, new_capacity: usize) -> Result<(), SchemaError> {
        let count = self.count;
        let mut run_cursor = self.run.cursor()?;
        let (run, records_per_page, num_pages) =
            Self::allocate_run(&self.db, self.record_size, new_capacity)?;
        self.run = run;
        self.records_per_page = records_per_page;
        self.capacity = records_per_page * num_pages;
        self.bitmask = BitVec::with_capacity(self.capacity);
        self.matched = BitVec::with_capacity(self.capacity);
        self.count = 0;
        let mut added = 0;
        while added < count {
            let data = run_cursor.next()?.ok_or(SchemaError::UnknownError(
                "Did not expect empty run tuple".into(),
            ))?;
            let data = data.data();
            let vec_data = from_bytes::<Vec<Option<HashValue>>>(&data)?;
            for v in vec_data {
                if let Some(v) = v
                    && let Some(l) = v.left_value
                {
                    self.insert_left(l)?;
                    added += 1;
                }
            }
        }
        Ok(())
    }

    // Drains the left source into the table — done lazily on the first
    // next() call (see `built`'s own doc comment), not eagerly in
    // new(). rehash only ever runs from in here (via insert_left), i.e.
    // strictly before any right-side probing starts, so `matched` never
    // needs replaying across a rehash — nothing has been matched yet.
    fn build_left(&mut self) -> Result<(), SchemaError> {
        while let Some(row) = self.sources[0].next()? {
            self.insert_left(row)?;
        }
        Ok(())
    }

    // LEFT/FULL only: once the right source is exhausted, every
    // occupied-but-never-matched slot (per `matched`) still owes an
    // output row, paired with right_null. Scans the table page by page,
    // resuming across calls via sweep_page/sweep_row.
    fn next_unmatched_left(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        loop {
            if self.sweep_page >= self.run.page_count() {
                return Ok(None);
            }
            if self.sweep_page_data.is_none() {
                self.sweep_page_data = Some(self.load_page(self.sweep_page)?);
            }
            let page_data = self.sweep_page_data.as_ref().unwrap();
            if self.sweep_row >= page_data.len() {
                self.sweep_page += 1;
                self.sweep_row = 0;
                self.sweep_page_data = None;
                continue;
            }
            let slot_index = self.sweep_page * self.records_per_page + self.sweep_row;
            let slot = &page_data[self.sweep_row];
            self.sweep_row += 1;
            if let Some(v) = slot
                && let Some(left) = &v.left_value
                && !self.matched.is_set(slot_index)
            {
                return Ok(Some(Self::combine(left, &self.right_null)?));
            }
        }
    }

    fn combine(left: &IndexKey, right: &IndexKey) -> Result<IndexKey, SchemaError> {
        let mut values = left.values().to_vec();
        values.extend_from_slice(right.values());
        Ok(IndexKey::new_from_owned(values)?)
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
                return Ok(Some(Self::combine(&left, right)?));
            }

            if self.right_exhausted {
                return self.next_unmatched_left();
            }

            match self.sources[1].next()? {
                Some(right) => {
                    let matches = self.probe_matches(&right)?;
                    if matches.is_empty() {
                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            let left_null = self.left_null.clone();
                            return Ok(Some(Self::combine(&left_null, &right)?));
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
                    if !matches!(self.join_type, JoinType::Left | JoinType::Full) {
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
        let (run, records_per_page, num_pages) =
            Self::allocate_run(&self.db, self.record_size, 1)?;
        self.run = run;
        self.records_per_page = records_per_page;
        self.capacity = records_per_page * num_pages;
        self.bitmask = BitVec::with_capacity(self.capacity);
        self.matched = BitVec::with_capacity(self.capacity);
        self.count = 0;
        self.built = false;
        self.pending_matches = VecDeque::new();
        self.current_right = None;
        self.right_exhausted = false;
        self.sweep_page = 0;
        self.sweep_row = 0;
        self.sweep_page_data = None;
        Ok(())
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

    // Reads every slot across every page of the run — mirrors the exact
    // per-page decode insert_with_hash/probe_matches use internally,
    // just exposed here so a test can inspect the whole table's final
    // state directly.
    fn dump_slots(source: &HashedSource<MemFile>) -> Vec<Option<HashValue>> {
        let mut out = vec![];
        for page in 0..source.run.page_count() {
            let data = source.run.get_content_at(page).unwrap().unwrap();
            let vec_data: Vec<Option<HashValue>> = from_bytes(&data).unwrap();
            out.extend(vec_data);
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
            IndexKey::new_from_owned(vec![ValueItem::Integer(99), ValueItem::Integer(-1)])
                .unwrap();
        assert!(source.probe_matches(&probe_key).unwrap().is_empty());
    }

    #[test]
    fn test_insert_left_triggers_a_rehash_once_capacity_is_reached() {
        // Small page size so `capacity` is small enough to actually fill
        // in a test, instead of needing hundreds of rows.
        let db = Db::<MemFile>::create_with_page_size("hash_rehash_test.db", 1024).unwrap();
        let mem = QueryMemory::new(1024 * 1024);
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

        let initial_capacity = source.capacity;
        // The count==capacity check runs at the START of insert_left, so
        // it only fires on the (capacity+1)th call — insert one more than
        // capacity, not exactly capacity, to actually trigger it.
        for i in 0..=initial_capacity as i64 {
            let row =
                IndexKey::new_from_owned(vec![ValueItem::Integer(i), ValueItem::Integer(0)])
                    .unwrap();
            source.insert_left(row).unwrap();
        }

        assert!(
            source.capacity > initial_capacity,
            "inserting capacity+1 distinct rows should have triggered a rehash that grows \
             capacity — it stayed at {initial_capacity}, meaning self.count never actually \
             reached self.capacity"
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
        let mem = QueryMemory::new(1024 * 1024);
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

        let initial_capacity = source.capacity;
        // Enough distinct rows to force at least one rehash, landing the
        // table on more than one page.
        let n = initial_capacity as i64 * 3 + 1;
        for i in 0..n {
            let row =
                IndexKey::new_from_owned(vec![ValueItem::Integer(i), ValueItem::Integer(i * 10)])
                    .unwrap();
            source.insert_left(row).unwrap();
        }
        assert!(
            source.run.page_count() > 1,
            "expected growth to span multiple pages; page_count={}",
            source.run.page_count()
        );

        let slots = dump_slots(&source);
        let present_ids: std::collections::HashSet<i64> = slots
            .iter()
            .flatten()
            .filter_map(|v| v.left_value.as_ref())
            .map(|k| match &k.values()[0] {
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
        assert_eq!(rows.len(), 2, "only the 2 matching pairs should be emitted: {rows:?}");
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
        let db = Db::<MemFile>::create_with_page_size("hash_next_multi_page_test.db", 1024).unwrap();
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
            assert_eq!(left_id, right_id, "a joined row's left/right ids must match");
            assert_eq!(left_id % 2, 0, "only even ids were ever inserted on the right side");
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
}
