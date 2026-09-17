use std::sync::Arc;

use log::error;

use crate::{
    buffer::LockLevel,
    buffer::PageBuffer,
    cursor::Cursor,
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{Page, PageId, PageTupleIterator, USABLE_DATA_MARGIN},
    tuple::{DBIdType, Tuple},
};

/// The actual owner of a run's on-disk page chain — held behind an `Arc`
/// shared between the writing `Run` and every `RunCursor` cloned from it
/// (see `Run::cursor`), and freed automatically, exactly once, on
/// whichever of those drops last.
///
/// This indirection is what makes cleanup automatic without `RunCursor`
/// having to *borrow* `&Run`: a borrow would force every cursor to live
/// no longer than the `Run` that produced it, but this crate's cursors
/// are deliberately built to detach and outlive their producer (e.g.
/// `TempTable::open_source` releases its read lock and returns a
/// cursor-backed `Source` that keeps running long after). An `Arc` clone
/// gets the same "can't free out from under a live reader" safety via
/// reference counting instead of a lifetime, without that constraint.
pub(crate) struct RunPages<F: DBFile + 'static> {
    buffer: Arc<PageBuffer<F>>,
    head: PageId,
}

impl<F: DBFile + 'static> Drop for RunPages<F> {
    fn drop(&mut self) {
        // Can't propagate a failure out of Drop::drop (it returns ()),
        // and panicking mid-drop — especially during an unrelated panic's
        // unwind — risks aborting the whole process over what would
        // otherwise just be a leak. Freeing a run's pages failing at all
        // should be rare (I/O error, corrupted state); log it and move
        // on rather than lose the failure silently.
        if let Err(e) = self.buffer.free_page_chain(self.head) {
            error!(
                "failed to free run page chain starting at {:?}: {e:?}",
                self.head
            );
        }
    }
}

/// An append-only, unkeyed chain of pages holding raw byte records in the
/// order they were written — the building block for query-execution
/// scratch space (sort runs, hash-join/aggregation spill partitions, ...),
/// as opposed to a table's B+Tree-indexed, MVCC-visible row storage.
///
/// Deliberately not transactional: nothing written through `append` is
/// undo/redo-logged. Its pages are freed automatically (see `RunPages`)
/// once nothing references them anymore, rather than through an explicit
/// call — not tracked across close/reopen or replayed by crash recovery
/// either way. A Run is scratch space for whichever query is building
/// it; it was never meant to survive a restart, so it doesn't try to.
pub struct Run<F: DBFile + 'static> {
    pages: Arc<RunPages<F>>,
    tail: PageId,
    pg_count: usize,
    // Every page id this run has ever allocated, in chain order (index 0
    // is always `head`, last is always `tail`) — lets a caller address a
    // specific page by position (`set_content_at`/`get_content_at`)
    // instead of only ever reading/writing the current tail or walking
    // the whole chain sequentially via `cursor()`. Needed for anything
    // that wants true random access over a run's pages, e.g. a hash
    // index mapping bucket number -> page directly.
    page_ids: Vec<PageId>,
}

impl<F> Run<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn create(buffer: Arc<PageBuffer<F>>) -> Result<Self, StoreError> {
        let head = buffer.alloc_run_page()?;
        Ok(Self {
            pages: Arc::new(RunPages { buffer, head }),
            tail: head,
            pg_count: 1,
            page_ids: vec![head],
        })
    }

    // STORE_AUDIT.md P6 — like `create`, but every page this run ever
    // allocates (this head page, and any grown via `new_slotted_page`)
    // is backed by `SlottedPage` instead of `RunPage` — see
    // `new_slotted_page`'s own comment for the access pattern this is
    // for. A caller must pick one page kind for a whole run up front:
    // mixing `new_page` (RunPage) and `new_slotted_page` (SlottedPage)
    // pages within the same run works mechanically (each page tracks its
    // own content kind) but isn't a scenario anything here is designed
    // or tested for.
    pub(crate) fn create_slotted(buffer: Arc<PageBuffer<F>>) -> Result<Self, StoreError> {
        let head = buffer.alloc_slotted_page()?;
        Ok(Self {
            pages: Arc::new(RunPages { buffer, head }),
            tail: head,
            pg_count: 1,
            page_ids: vec![head],
        })
    }

    /// Every page id this run has allocated, in chain order — index `i`
    /// is the `(i+1)`th page allocated (`head` is index 0, `tail` is the
    /// last entry). Use with `set_content_at`/`get_content_at` for direct
    /// access to a specific page by position, e.g. a hash index mapping
    /// bucket number straight to a page instead of scanning for it.
    pub fn page_ids(&self) -> &[PageId] {
        &self.page_ids
    }

    /// The chain's first page.
    pub fn head(&self) -> PageId {
        self.pages.head
    }

    /// The chain's current last page — where `append` is currently
    /// writing, and what `set_content`/`get_content` operate against (see
    /// their own docs). Exposed for callers doing their own page-by-page
    /// writes via `new_page`/`set_content` who want to record which page
    /// each chunk landed on (e.g. to build an index over a run's pages),
    /// the same way `head` lets a reader locate the start of the chain.
    pub fn tail(&self) -> PageId {
        self.tail
    }

    /// Appends one record, in the order given. Every tuple a Run stores
    /// carries the same placeholder id (0) — a Run has no per-record key
    /// at all (see RunPage), so there's nothing meaningful to put there.
    pub fn append(&mut self, data: &[u8]) -> Result<(), StoreError> {
        let tuple = Tuple::new(0, data);
        loop {
            let handle = self.pages.buffer.get_page_mut(self.tail, LockLevel::Data)?;
            if handle.page.can_store(&tuple) {
                handle.page.add_tuple(tuple)?;
                self.pages.buffer.write_locked_page(handle)?;
                return Ok(());
            }
            drop(handle);
            let new_id = self.pages.buffer.alloc_run_page()?;
            self.pg_count += 1;
            self.pages.buffer.set_data_chain_next(self.tail, new_id)?;
            self.tail = new_id;
            self.page_ids.push(new_id);
        }
    }

    pub fn available_size(&self) -> DBSizeType {
        self.pages.buffer.page_size() - Page::get_overhead()
    }

    /// The largest single blob `set_content` will accept without erroring.
    /// Same margin `Page::usable_data_size` reserves below `available_size`
    /// for page-serialization framing that no individual tuple's own
    /// `size()` accounts for (see USABLE_DATA_MARGIN) — kept in sync with
    /// it here since a Page's own version is private to `page.rs`.
    pub fn data_size(&self) -> DBSizeType {
        self.available_size().saturating_sub(USABLE_DATA_MARGIN)
    }

    /// Grows the chain by one page and moves onto it: allocates a fresh
    /// page, links the current tail to it, and makes it the new tail —
    /// the same two steps `append`'s internal retry loop takes when the
    /// current tail is full, just exposed directly so a caller writing
    /// through `set_content` (which, unlike `append`, never allocates a
    /// page on its own) can control exactly when the chain grows. Returns
    /// the new page's id, e.g. to remember alongside whatever chunk gets
    /// written to it next.
    pub fn new_page(&mut self) -> Result<PageId, StoreError> {
        let new_id = self.pages.buffer.alloc_run_page()?;
        self.pages.buffer.set_data_chain_next(self.tail, new_id)?;
        self.tail = new_id;
        self.pg_count += 1;
        self.page_ids.push(new_id);
        Ok(self.tail)
    }

    /// Like `new_page`, but the new page is backed by `SlottedPage`
    /// (STORE_AUDIT.md P6) instead of `RunPage` — individually
    /// addressable, in-place-mutable slots (`get_slot_at`/`set_slot_at`/
    /// `slots_at`) instead of one opaque append-only blob per page
    /// (`set_content_at`/`get_content_at`). For a caller with a fixed,
    /// page-per-bucket-range layout (e.g. a hash index mapping bucket
    /// number to a specific page and slot within it) that mutates
    /// individual slots repeatedly, rather than rewriting a whole page's
    /// content atomically on every change. See `slotted.rs`'s own doc
    /// comment for the cost this avoids: `set_content_at`/`get_content_at`
    /// round-trip the WHOLE page's content through the caller's own
    /// encoding on every touch — fine for "write it once, read it back
    /// whole later," expensive for "mutate one small piece of it, over
    /// and over."
    pub fn new_slotted_page(&mut self) -> Result<PageId, StoreError> {
        let new_id = self.pages.buffer.alloc_slotted_page()?;
        self.pages.buffer.set_data_chain_next(self.tail, new_id)?;
        self.tail = new_id;
        self.pg_count += 1;
        self.page_ids.push(new_id);
        Ok(self.tail)
    }

    pub fn page_count(&self) -> usize {
        self.pg_count
    }

    /// Directly stores `data` as the current tail page's entire content,
    /// replacing whatever was there — skipping append's per-record
    /// tuple-then-retry-on-a-new-page loop, cheaper than `append` when the
    /// caller already holds one contiguous blob known to fit on a single
    /// page. Unlike `append`, oversized content here is a caller mistake
    /// to surface immediately, not something to transparently spill
    /// across an overflow chain — so this checks against `data_size` up
    /// front and errors instead.
    ///
    /// A Run never grows its chain on its own outside of `append`, so
    /// writing more content than one page holds is on the caller: call
    /// `new_page` to advance the tail, then `set_content` again for the
    /// next chunk. Reading a multi-page run back is just `cursor()` —
    /// every page `set_content` ever wrote holds exactly one Tuple, so
    /// the existing tuple-by-tuple RunCursor already walks chunk-by-chunk,
    /// page-by-page, in write order; nothing about it is specific to
    /// `append`.
    pub fn set_content(&mut self, data: &[u8]) -> Result<(), StoreError> {
        self.set_content_at(self.page_ids.len() - 1, data)
    }

    /// Reads back whatever `set_content` last wrote to the current tail
    /// page — `None` if nothing has been written to it yet. For anything
    /// written before the most recent `new_page` (i.e. earlier pages in
    /// the chain), use `cursor()` instead.
    pub fn get_content(&self) -> Result<Option<Vec<u8>>, StoreError> {
        self.get_content_at(self.page_ids.len() - 1)
    }

    /// `set_content`, but addressable by page position instead of always
    /// the current tail — lets a caller with a fixed, page-per-bucket
    /// layout (e.g. a hash index) write straight to a specific page
    /// without disturbing the run's own tail/append state. `index` is
    /// into `page_ids()` (0 is always the head); out of range errors
    /// rather than panicking, same as any other caller-suppliable index
    /// into this crate's storage (see e.g. `BadRowNumber`).
    pub fn set_content_at(&mut self, index: usize, data: &[u8]) -> Result<(), StoreError> {
        let page_id = *self
            .page_ids
            .get(index)
            .ok_or(StoreError::RunPageIndexOutOfRange(index, self.page_ids.len()))?;
        let tuple = Tuple::new(0, data);
        let max = self.data_size();
        if tuple.size() > max {
            return Err(StoreError::TupleTooLarge(tuple.size(), max as usize));
        }
        let handle = self.pages.buffer.get_page_mut(page_id, LockLevel::Data)?;
        handle.page.clear()?;
        handle.page.add_tuple(tuple)?;
        self.pages.buffer.write_locked_page(handle)?;
        Ok(())
    }

    /// `get_content`, but addressable by page position instead of always
    /// the current tail — the read-side counterpart to `set_content_at`.
    pub fn get_content_at(&self, index: usize) -> Result<Option<Vec<u8>>, StoreError> {
        let page_id = *self
            .page_ids
            .get(index)
            .ok_or(StoreError::RunPageIndexOutOfRange(index, self.page_ids.len()))?;
        let page = self.pages.buffer.get_page(page_id)?;
        Ok(page.iter().next().map(|t| t.data().to_vec()))
    }

    /// Reads back one slot's raw bytes from a page allocated via
    /// `new_slotted_page`/`create_slotted` — `None` if that exact slot id
    /// has never been written (a slotted page's own way of representing
    /// "empty," no `Option` wrapper needed in the caller's own encoding
    /// the way `set_content_at`'s one-blob-per-page model required). Only
    /// ever decodes the ONE slot asked for, not the whole page — the
    /// point of this over `get_content_at`.
    pub fn get_slot_at(&self, page_index: usize, slot: u64) -> Result<Option<Vec<u8>>, StoreError> {
        let page_id = *self
            .page_ids
            .get(page_index)
            .ok_or(StoreError::RunPageIndexOutOfRange(page_index, self.page_ids.len()))?;
        let page = self.pages.buffer.get_page(page_id)?;
        Ok(page.get(DBIdType::Int(slot))?.map(|t| t.data().to_vec()))
    }

    /// Writes `data` as slot `slot`'s content on a page allocated via
    /// `new_slotted_page`/`create_slotted` — write-once: errors (via
    /// `StoreError::DuplicateKey`) if this exact slot already holds
    /// something, since every known caller (a hash index's open-
    /// addressing scheme, which only ever claims a slot once) treats a
    /// slot as immutable once set. Only touches this one slot's own
    /// bytes, not the whole page — the point of this over
    /// `set_content_at`.
    pub fn set_slot_at(&self, page_index: usize, slot: u64, data: &[u8]) -> Result<(), StoreError> {
        let page_id = *self
            .page_ids
            .get(page_index)
            .ok_or(StoreError::RunPageIndexOutOfRange(page_index, self.page_ids.len()))?;
        let handle = self.pages.buffer.get_page_mut(page_id, LockLevel::Data)?;
        handle.page.add_tuple(Tuple::new(slot, data))?;
        self.pages.buffer.write_locked_page(handle)?;
        Ok(())
    }

    /// Every occupied slot's `(slot id, raw bytes)` on a page allocated
    /// via `new_slotted_page`/`create_slotted`, in ascending slot-id
    /// order — the batch-read counterpart to `get_slot_at`, for a caller
    /// sweeping a whole page's live entries at once (e.g. a hash index's
    /// unmatched-left scan) rather than probing one slot at a time.
    /// Empty slots cost nothing to skip — unlike `get_content_at`'s
    /// one-blob model, an unoccupied slot was never written at all, not
    /// an `Option::None` that still has to be decoded to find out.
    pub fn slots_at(&self, page_index: usize) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let page_id = *self
            .page_ids
            .get(page_index)
            .ok_or(StoreError::RunPageIndexOutOfRange(page_index, self.page_ids.len()))?;
        let page = self.pages.buffer.get_page(page_id)?;
        Ok(page
            .iter()
            .map(|t| {
                let DBIdType::Int(slot) = t.id else {
                    panic!("slotted Run pages are always Int-keyed by slot number")
                };
                (slot, t.data().to_vec())
            })
            .collect())
    }

    /// A fresh cursor over this run's own pages, starting from its head.
    /// Holds its own `Arc` clone onto the same underlying page chain this
    /// `Run` owns (see `RunPages`) — so those pages stay alive for as
    /// long as either this `Run` or the returned cursor (or any further
    /// clone taken from either) still exists, independent of this
    /// particular `Run` value's own lifetime. There's no separate way to
    /// read a run by page id alone; a cursor always comes from a live
    /// `Run`.
    pub fn cursor(&self) -> Result<RunCursor<F>, StoreError> {
        RunCursor::new(self.pages.clone())
    }
}

/// Sequential reader over a Run's page chain, yielding each record's raw
/// bytes in the order they were appended. No visibility filtering at
/// all — unlike TableCursor, a Run isn't MVCC-shared state, so every
/// record physically present is unconditionally returned.
///
/// Holds its own `Arc<RunPages<F>>` clone rather than borrowing `&Run` —
/// deliberately, so it can be boxed and handed off past whatever produced
/// it (e.g. `TempTable::open_source` releases its read lock and returns
/// a cursor-backed `Source` that keeps running afterward). That same Arc
/// clone is what keeps the underlying pages alive for as long as this
/// cursor exists, even after the `Run` it came from is gone.
pub struct RunCursor<F: DBFile + 'static> {
    pages: Arc<RunPages<F>>,
    current_page: Arc<Page>,
    current_iter: PageTupleIterator,
}

impl<F> RunCursor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(pages: Arc<RunPages<F>>) -> Result<Self, StoreError> {
        let current_page = pages.buffer.get_page(pages.head)?;
        let current_iter = current_page.iter();
        Ok(Self {
            pages,
            current_page,
            current_iter,
        })
    }

    fn next_tuple(&mut self) -> Result<Option<Tuple>, StoreError> {
        if let Some(t) = self.current_iter.next() {
            return Ok(Some(t));
        }
        // Raw next_page, not the overflow-aware data_chain_next: a page
        // read through PageBuffer::get_page already has any overflow
        // chain transparently reassembled into it (see buffer.rs's
        // read_page), so by the time we see it here, next_page is
        // already the real next sibling — the same reasoning
        // TableCursor's own next_tuple relies on.
        let next = self.current_page.get_next_page();
        if next.is_valid_next_page() {
            self.current_page = self.pages.buffer.get_page(next)?;
            self.current_iter = self.current_page.iter();
            Ok(self.current_iter.next())
        } else {
            Ok(None)
        }
    }
}

impl<F: DBFile> Cursor for RunCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    type Item = Tuple;

    fn next(&mut self) -> Result<Option<Self::Item>, StoreError> {
        self.next_tuple()
    }

    // Same lookup `new()` did against this run's own head page.
    fn reset(&mut self) -> Result<(), StoreError> {
        let current_page = self.pages.buffer.get_page(self.pages.head)?;
        self.current_iter = current_page.iter();
        self.current_page = current_page;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{cursor::Cursor, db::Db, memfile::MemFile};

    #[test]
    fn test_run_cursor_reset_rescans_from_the_head() {
        let db = Db::<MemFile>::create("run_cursor_reset.db").unwrap();
        let mut run = db.create_run().unwrap();
        for i in 0..10u8 {
            run.append(&[i]).unwrap();
        }

        let mut cursor = run.cursor().unwrap();
        let first_pass: Vec<u8> = std::iter::from_fn(|| cursor.next().unwrap())
            .map(|t| t.data()[0])
            .collect();
        assert_eq!(
            first_pass,
            (0..10).collect::<Vec<_>>(),
            "sanity: first pass"
        );
        assert!(
            cursor.next().unwrap().is_none(),
            "sanity: cursor is actually exhausted before reset"
        );

        cursor.reset().unwrap();
        let second_pass: Vec<u8> = std::iter::from_fn(|| cursor.next().unwrap())
            .map(|t| t.data()[0])
            .collect();
        assert_eq!(
            second_pass, first_pass,
            "reset must let the same cursor re-read every record again, in the same order"
        );
    }

    #[test]
    fn test_set_content_get_content_round_trip() {
        let db = Db::<MemFile>::create("run_set_content_round_trip.db").unwrap();
        let mut run = db.create_run().unwrap();

        assert_eq!(
            run.get_content().unwrap(),
            None,
            "a fresh run has no content yet"
        );

        run.set_content(b"hello").unwrap();
        assert_eq!(run.get_content().unwrap(), Some(b"hello".to_vec()));

        // A second call replaces the first, rather than appending to it.
        run.set_content(b"goodbye").unwrap();
        assert_eq!(run.get_content().unwrap(), Some(b"goodbye".to_vec()));
    }

    #[test]
    fn test_set_content_rejects_data_larger_than_data_size() {
        let db = Db::<MemFile>::create("run_set_content_too_large.db").unwrap();
        let mut run = db.create_run().unwrap();

        let too_big = vec![0u8; run.data_size() as usize + 1];
        let err = run.set_content(&too_big).unwrap_err();
        assert!(
            matches!(err, crate::error::StoreError::TupleTooLarge(_, _)),
            "expected TupleTooLarge, got {err:?}"
        );

        // The failed set_content must not have left a half-written page.
        assert_eq!(run.get_content().unwrap(), None);
    }

    #[test]
    fn test_set_content_accepts_data_up_to_data_size() {
        let db = Db::<MemFile>::create("run_set_content_near_cap.db").unwrap();
        let mut run = db.create_run().unwrap();

        // Comfortably under data_size() to leave room for the constructed
        // Tuple's own serialization framing (id/flags/length-prefix) on
        // top of the raw bytes, which data_size() itself doesn't subtract
        // for — this is checking "a large payload near the cap still
        // works", not pinning the exact byte-for-byte boundary.
        let almost_max = vec![0u8; run.data_size() as usize - 64];
        run.set_content(&almost_max).unwrap();
        assert_eq!(run.get_content().unwrap(), Some(almost_max));
    }

    #[test]
    fn test_new_page_lets_set_content_span_multiple_pages() {
        let db = Db::<MemFile>::create("run_set_content_multi_page.db").unwrap();
        let mut run = db.create_run().unwrap();

        let head = run.head();
        assert_eq!(run.tail(), head, "a fresh run's tail starts at its head");

        run.set_content(b"chunk one").unwrap();

        let second_page = run.new_page().unwrap();
        assert_eq!(run.tail(), second_page);
        assert_ne!(second_page, head, "new_page must actually grow the chain");
        assert_eq!(
            run.get_content().unwrap(),
            None,
            "a freshly allocated page has no content until set_content writes to it"
        );
        run.set_content(b"chunk two").unwrap();

        let third_page = run.new_page().unwrap();
        run.set_content(b"chunk three").unwrap();
        assert_eq!(run.tail(), third_page);

        // The write side used new_page/set_content exclusively (no
        // append) — reading back through the ordinary cursor should walk
        // all three pages' single tuples in write order regardless, since
        // nothing about RunCursor's traversal is specific to append.
        let mut cursor = run.cursor().unwrap();
        let chunks: Vec<Vec<u8>> = std::iter::from_fn(|| cursor.next().unwrap())
            .map(|t| t.data().to_vec())
            .collect();
        assert_eq!(
            chunks,
            vec![
                b"chunk one".to_vec(),
                b"chunk two".to_vec(),
                b"chunk three".to_vec(),
            ]
        );
    }

    #[test]
    fn test_page_ids_tracks_every_allocated_page_in_chain_order() {
        let db = Db::<MemFile>::create("run_page_ids_tracking.db").unwrap();
        let mut run = db.create_run().unwrap();

        assert_eq!(
            run.page_ids(),
            &[run.head()],
            "a fresh run has exactly its own head page"
        );

        let second = run.new_page().unwrap();
        assert_eq!(run.page_ids(), &[run.head(), second]);

        let third = run.new_page().unwrap();
        assert_eq!(run.page_ids(), &[run.head(), second, third]);
        assert_eq!(run.page_ids().last(), Some(&run.tail()));
    }

    #[test]
    fn test_page_ids_tracks_pages_new_page_and_append_both_allocate() {
        // append()'s own internal page-growth loop must also register
        // into page_ids, not just the explicit new_page() path.
        let db: Arc<Db<MemFile>> = Db::create_with_page_size("run_page_ids_append.db", 512).unwrap();
        let mut run = db.create_run().unwrap();
        for i in 0..100u32 {
            run.append(&i.to_be_bytes()).unwrap();
        }
        assert!(
            run.page_ids().len() > 1,
            "100 records at a 512-byte page size must span more than one page"
        );
        assert_eq!(
            run.page_ids().len(),
            run.page_count(),
            "page_ids and page_count must always agree"
        );
        assert_eq!(run.page_ids()[0], run.head());
        assert_eq!(*run.page_ids().last().unwrap(), run.tail());
    }

    #[test]
    fn test_get_content_at_and_set_content_at_random_access_by_index() {
        let db = Db::<MemFile>::create("run_content_at.db").unwrap();
        let mut run = db.create_run().unwrap();

        // Build a run with 3 pages, each independently addressable —
        // the shape a hash index (bucket i -> page i) would rely on.
        run.set_content(b"bucket 0").unwrap();
        run.new_page().unwrap();
        run.set_content(b"bucket 1").unwrap();
        run.new_page().unwrap();
        run.set_content(b"bucket 2").unwrap();

        // Random access, not just sequential: read them back out of order.
        assert_eq!(
            run.get_content_at(2).unwrap(),
            Some(b"bucket 2".to_vec())
        );
        assert_eq!(
            run.get_content_at(0).unwrap(),
            Some(b"bucket 0".to_vec())
        );
        assert_eq!(
            run.get_content_at(1).unwrap(),
            Some(b"bucket 1".to_vec())
        );

        // Overwriting an earlier page directly (not the current tail)
        // must not disturb the others.
        run.set_content_at(0, b"rehashed bucket 0").unwrap();
        assert_eq!(
            run.get_content_at(0).unwrap(),
            Some(b"rehashed bucket 0".to_vec())
        );
        assert_eq!(run.get_content_at(1).unwrap(), Some(b"bucket 1".to_vec()));
        assert_eq!(run.get_content_at(2).unwrap(), Some(b"bucket 2".to_vec()));
    }

    #[test]
    fn test_content_at_out_of_range_index_errors_instead_of_panicking() {
        let db = Db::<MemFile>::create("run_content_at_oob.db").unwrap();
        let mut run = db.create_run().unwrap();

        let err = run.get_content_at(5).unwrap_err();
        assert!(
            matches!(err, crate::error::StoreError::RunPageIndexOutOfRange(5, 1)),
            "expected RunPageIndexOutOfRange(5, 1), got {err:?}"
        );

        let err = run.set_content_at(5, b"x").unwrap_err();
        assert!(
            matches!(err, crate::error::StoreError::RunPageIndexOutOfRange(5, 1)),
            "expected RunPageIndexOutOfRange(5, 1), got {err:?}"
        );
    }

    // STORE_AUDIT.md P6 — sanity coverage for the new slotted-page Run
    // API before wiring anything real (squeal-sql's HashedSource) on top
    // of it.
    #[test]
    fn test_slot_at_round_trip_and_empty_slot_is_none() {
        let db = Db::<MemFile>::create("run_slot_round_trip.db").unwrap();
        let run = db.create_slotted_run().unwrap();

        assert_eq!(
            run.get_slot_at(0, 3).unwrap(),
            None,
            "an unwritten slot must read back as None, not an error"
        );

        run.set_slot_at(0, 3, b"hello").unwrap();
        assert_eq!(run.get_slot_at(0, 3).unwrap(), Some(b"hello".to_vec()));
        // A different slot on the same page stays untouched.
        assert_eq!(run.get_slot_at(0, 4).unwrap(), None);
    }

    #[test]
    fn test_set_slot_at_twice_on_the_same_slot_errors() {
        let db = Db::<MemFile>::create("run_slot_duplicate.db").unwrap();
        let run = db.create_slotted_run().unwrap();
        run.set_slot_at(0, 1, b"first").unwrap();
        let err = run.set_slot_at(0, 1, b"second").unwrap_err();
        assert!(
            matches!(err, crate::error::StoreError::DuplicateKey(_)),
            "expected DuplicateKey, got {err:?}"
        );
        // The original value must survive the failed overwrite attempt.
        assert_eq!(run.get_slot_at(0, 1).unwrap(), Some(b"first".to_vec()));
    }

    #[test]
    fn test_slots_at_returns_only_occupied_slots_in_ascending_order() {
        let db = Db::<MemFile>::create("run_slots_at.db").unwrap();
        let run = db.create_slotted_run().unwrap();
        // Written out of order — slots_at must still come back sorted.
        run.set_slot_at(0, 5, b"five").unwrap();
        run.set_slot_at(0, 1, b"one").unwrap();
        run.set_slot_at(0, 3, b"three").unwrap();

        let got = run.slots_at(0).unwrap();
        assert_eq!(
            got,
            vec![
                (1, b"one".to_vec()),
                (3, b"three".to_vec()),
                (5, b"five".to_vec()),
            ]
        );
    }

    #[test]
    fn test_new_slotted_page_grows_the_run_and_keeps_pages_independently_addressable() {
        let db = Db::<MemFile>::create("run_new_slotted_page.db").unwrap();
        let mut run = db.create_slotted_run().unwrap();
        assert_eq!(run.page_count(), 1);

        run.new_slotted_page().unwrap();
        assert_eq!(run.page_count(), 2);

        run.set_slot_at(0, 0, b"page zero slot zero").unwrap();
        run.set_slot_at(1, 0, b"page one slot zero").unwrap();
        assert_eq!(
            run.get_slot_at(0, 0).unwrap(),
            Some(b"page zero slot zero".to_vec())
        );
        assert_eq!(
            run.get_slot_at(1, 0).unwrap(),
            Some(b"page one slot zero".to_vec())
        );
    }

    // Isolation test for a squeal-sql-level bug (HashedSource's
    // bug_repro_rehash_past_272384_rows_loses_entries, in source/hash.rs):
    // a hash table with 133 records/page loses entries once it grows to
    // exactly 272384 = 133 * 2048 slots (2048 pages) — a suspiciously
    // round page count to be where a hashing/capacity bug would show up.
    // This checks Run/RunCursor alone, no hashing or capacity-doubling
    // involved: can a plain sequential fill across a page chain that
    // crosses exactly 2048 pages be read back completely, via the same
    // cursor.next() page-chain walk (RunCursor::next_tuple) rehash's
    // replay depends on?
    #[test]
    fn test_slotted_run_cursor_reads_back_every_slot_across_2048_pages() {
        const RECORDS_PER_PAGE: u64 = 133;
        const NUM_PAGES: usize = 2100;
        let db = Db::<MemFile>::create("run_cursor_many_pages.db").unwrap();
        let mut run = db.create_slotted_run().unwrap();
        for page in 0..NUM_PAGES {
            if page > 0 {
                run.new_slotted_page().unwrap();
            }
            for slot in 0..RECORDS_PER_PAGE {
                let value = (page as u64) * RECORDS_PER_PAGE + slot;
                run.set_slot_at(page, slot, &value.to_le_bytes()).unwrap();
            }
        }
        assert_eq!(run.page_count(), NUM_PAGES);

        let mut cursor = run.cursor().unwrap();
        let mut seen = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen.push(u64::from_le_bytes(bytes));
        }
        let expected_total = NUM_PAGES as u64 * RECORDS_PER_PAGE;
        assert_eq!(
            seen.len() as u64,
            expected_total,
            "expected {expected_total} tuples, cursor yielded {}",
            seen.len()
        );
        let expected: Vec<u64> = (0..expected_total).collect();
        assert_eq!(
            seen, expected,
            "tuples must come back in write order, none lost or duplicated"
        );
    }

    // Same as the sequential-fill version above, but writes in
    // hash-scattered order instead of page-by-page — HashedSource's own
    // insert_with_hash writes to `hash % capacity` (then linear-probes
    // on collision), so pages fill in a scattered, non-sequential
    // pattern, not front-to-back. Uses a multiplicative permutation
    // (coprime multiplier over the full slot space, a full-period Weyl
    // sequence) so every slot still gets exactly one write, just in
    // scrambled order — closer to what the real bug's access pattern
    // looks like than the sequential version.
    #[test]
    fn test_slotted_run_cursor_reads_back_every_slot_written_in_scattered_order() {
        const RECORDS_PER_PAGE: u64 = 133;
        const NUM_PAGES: usize = 2100;
        const TOTAL_SLOTS: u64 = RECORDS_PER_PAGE * NUM_PAGES as u64;
        // Coprime with TOTAL_SLOTS (279300 = 2^2*3*5^2*7^2*19; this is
        // prime and shares no factor with it) so `(i * MULT) %
        // TOTAL_SLOTS` visits every slot exactly once as i ranges over
        // 0..TOTAL_SLOTS.
        const MULT: u64 = 104729;

        let db = Db::<MemFile>::create("run_cursor_scattered.db").unwrap();
        let mut run = db.create_slotted_run().unwrap();
        for _ in 1..NUM_PAGES {
            run.new_slotted_page().unwrap();
        }
        assert_eq!(run.page_count(), NUM_PAGES);

        for i in 0..TOTAL_SLOTS {
            let target = (i * MULT) % TOTAL_SLOTS;
            let page = (target / RECORDS_PER_PAGE) as usize;
            let slot = target % RECORDS_PER_PAGE;
            // Value is the slot's own target index, not `i` — lets the
            // read-back check assert against a plain 0..TOTAL_SLOTS
            // range regardless of write order.
            run.set_slot_at(page, slot, &target.to_le_bytes()).unwrap();
        }

        let mut cursor = run.cursor().unwrap();
        let mut seen = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen.push(u64::from_le_bytes(bytes));
        }
        assert_eq!(
            seen.len() as u64,
            TOTAL_SLOTS,
            "expected {TOTAL_SLOTS} tuples, cursor yielded {}",
            seen.len()
        );
        seen.sort_unstable();
        let expected: Vec<u64> = (0..TOTAL_SLOTS).collect();
        assert_eq!(
            seen, expected,
            "every slot's value must be read back exactly once, none lost or duplicated"
        );
    }
}
