use std::sync::Arc;

use crate::{
    buffer::PageBuffer,
    cursor::Cursor,
    db::DBFile,
    error::StoreError,
    page::{Page, PageId, PageTupleIterator},
    tuple::Tuple,
};

/// An append-only, unkeyed chain of pages holding raw byte records in the
/// order they were written — the building block for query-execution
/// scratch space (sort runs, hash-join/aggregation spill partitions, ...),
/// as opposed to a table's B+Tree-indexed, MVCC-visible row storage.
///
/// Deliberately not transactional: nothing written through `append` is
/// undo/redo-logged, and a Run's pages are freed only by an explicit call
/// to `free` — not tracked across close/reopen or replayed by crash
/// recovery. A Run is scratch space owned by whichever query is building
/// it, for exactly as long as that query runs; it was never meant to
/// survive a restart, so it doesn't try to.
pub struct Run<F: DBFile + 'static> {
    buffer: Arc<PageBuffer<F>>,
    head: PageId,
    tail: PageId,
}

impl<F> Run<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn create(buffer: Arc<PageBuffer<F>>) -> Result<Self, StoreError> {
        let head = buffer.alloc_run_page()?;
        Ok(Self {
            buffer,
            head,
            tail: head,
        })
    }

    /// The chain's first page — hand this to `Db::open_run` to read this
    /// run back, including from a different owner than whoever wrote it
    /// (e.g. a merge step reading several already-written input runs).
    pub fn head(&self) -> PageId {
        self.head
    }

    /// Appends one record, in the order given. Every tuple a Run stores
    /// carries the same placeholder id (0) — a Run has no per-record key
    /// at all (see RunPage), so there's nothing meaningful to put there.
    pub fn append(&mut self, data: &[u8]) -> Result<(), StoreError> {
        let tuple = Tuple::new(0, data);
        loop {
            let handle = self.buffer.get_page_mut(self.tail)?;
            if handle.page.can_store(&tuple) {
                handle.page.add_tuple(tuple)?;
                self.buffer.write_locked_page(handle)?;
                return Ok(());
            }
            drop(handle);
            let new_id = self.buffer.alloc_run_page()?;
            self.buffer.set_data_chain_next(self.tail, new_id)?;
            self.tail = new_id;
        }
    }

    /// Frees every page in this run. Call once its contents are no
    /// longer needed (e.g. after a merge step has consumed it) — nothing
    /// else reclaims a Run's pages on its own.
    pub fn free(self) -> Result<(), StoreError> {
        self.buffer.free_page_chain(self.head)
    }

    /// A fresh cursor over this run's own pages, starting from its head.
    pub fn cursor(&self) -> Result<RunCursor<F>, StoreError> {
        RunCursor::new(self.buffer.clone(), self.head)
    }
}

/// Sequential reader over a Run's page chain, yielding each record's raw
/// bytes in the order they were appended. No visibility filtering at
/// all — unlike TableCursor, a Run isn't MVCC-shared state, so every
/// record physically present is unconditionally returned.
pub struct RunCursor<F: DBFile + 'static> {
    buffer: Arc<PageBuffer<F>>,
    current_page: Arc<Page>,
    current_iter: PageTupleIterator,
    head: PageId,
}

impl<F> RunCursor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(buffer: Arc<PageBuffer<F>>, head: PageId) -> Result<Self, StoreError> {
        let current_page = buffer.get_page(head)?;
        let current_iter = current_page.iter();
        Ok(Self {
            buffer,
            current_page,
            current_iter,
            head,
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
            self.current_page = self.buffer.get_page(next)?;
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

    // Same lookup `new()` did against this run's own head page — `buffer`
    // and `head` are already stored as fields, unused after construction
    // until now.
    fn reset(&mut self) -> Result<(), StoreError> {
        let current_page = self.buffer.get_page(self.head)?;
        self.current_iter = current_page.iter();
        self.current_page = current_page;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{cursor::Cursor, db::Db, memfile::MemFile};

    #[test]
    fn test_run_cursor_reset_rescans_from_the_head() {
        let db = Db::<MemFile>::create("run_cursor_reset.db").unwrap();
        let mut run = db.create_run().unwrap();
        for i in 0..10u8 {
            run.append(&[i]).unwrap();
        }

        let mut cursor = db.open_run(run.head()).unwrap();
        let first_pass: Vec<u8> = std::iter::from_fn(|| cursor.next().unwrap())
            .map(|t| t.data()[0])
            .collect();
        assert_eq!(first_pass, (0..10).collect::<Vec<_>>(), "sanity: first pass");
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
}
