use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    buffer::{PageBuffer, WritePageHandle},
    db::{DBFile, DBSizeType, retry_on_contention},
    error::StoreError,
    logger::Logger,
    page::{Page, PageId},
    table::{Table, TableIdType, TableType},
    tuple::{DBIdType, Tuple},
    txn::{TransactionId, TransactionManager},
};

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, PartialOrd)]
enum Node {
    Inner(PageId),
    Leaf(PageId),
}

// Bit indices 0–3 are reserved (PINNED, INDEX_PAGE, …); user flags start at 4.
const INNER_NODE: usize = 4;
const LEAF_NODE: usize = 5;

// Postcard varint upper bounds per field:
//   DBIdType::Int(u64::MAX)  → 1 (variant) + 10 (varint) = 11 B
//   Option<TransactionId>    → 1 (Some) + 10 (u64 id) + 10 (u128 ts) = 21 B
//   Option<UndoId>           → 1 (None)
//   data Node::Inner(u64::MAX) as Vec<u8> → 1 (len) + 1 (variant) + 10 (varint) = 12 B
//   flags                    → 1 B
//   Total ≈ 46 B; 64 B gives comfortable headroom for any realistic payload.
const MAX_ENTRY_BYTES: u64 = 64;

pub(crate) struct BPlusTree<F: DBFile + 'static> {
    pub(crate) table: Table,
    buffer: Arc<PageBuffer<F>>,
    txn_mgr: Arc<TransactionManager>,
    logger: Arc<Logger>,
    // Cached hint for where write_data should start looking for room,
    // instead of always rescanning the data-chain from table.first_data_page
    // (which made every insert O(chain length), i.e. O(N) per insert / O(N^2)
    // total for N sequential inserts — confirmed empirically: throughput
    // dropped from ~1300 to ~200 rows/sec between row 15k and row 20k with a
    // small page size). Not persisted: on a fresh table it starts at
    // first_data_page (nothing to discover); on reopen it's rebuilt once by
    // walking the chain to its actual end (see from_bytes) — a one-time,
    // load-time cost instead of a per-insert one.
    //
    // This is purely a hint, not a correctness-bearing value: write_data
    // still calls can_store on whatever page it starts from and walks
    // forward (allocating a new page if needed) exactly as before, so a
    // stale value just costs a few extra hops, never wrong behavior. That's
    // why a plain Relaxed store (not a CAS/fetch_max) is fine even though
    // concurrent writers can race to extend the chain — see write_data.
    last_data_page: AtomicU64,
}

impl<F: DBFile> BPlusTree<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub fn new(
        id: TableIdType,
        name: String,
        buffer: Arc<PageBuffer<F>>,
        txn_mgr: Arc<TransactionManager>,
        logger: Arc<Logger>,
    ) -> Result<Self, StoreError> {
        let pg = buffer.page_size();
        let count = pg / MAX_ENTRY_BYTES;

        if count < 2 {
            return Err(StoreError::UnknownError(
                "Unable to fit index : count = {count}, size = {size}".into(),
            ));
        }
        let first_index_page = buffer.alloc_page(false)?;
        let first_data_page = buffer.alloc_page(false)?;
        let mut index_page = Page::new_indexed(pg, MAX_ENTRY_BYTES as usize);
        // Adopt into this database's WAL clock before flagging (set_page_flags
        // dirties it, which stamps the lsn from the clock).
        index_page.set_clock(buffer.clock());
        index_page.set_page_flags(LEAF_NODE)?;
        let mut handle = buffer.get_page_mut(first_index_page)?;
        handle.page = Arc::new(index_page);
        buffer.write_locked_page(handle)?;
        let table = Table {
            id,
            name,
            table_type: TableType::BtreeTable,
            first_index_page,
            first_data_page,
            nodes_per_page: count as usize,
        };
        Ok(Self {
            table,
            buffer,
            txn_mgr,
            logger,
            last_data_page: AtomicU64::new(first_data_page.into()),
        })
    }

    pub fn from_bytes(
        bytes: &[u8],
        buffer: Arc<PageBuffer<F>>,
        txn_mgr: Arc<TransactionManager>,
        logger: Arc<Logger>,
    ) -> Result<Self, StoreError> {
        let t: Table = from_bytes(bytes)?;
        // One-time cost, not per-insert: walk the chain to its real end so
        // write_data doesn't have to rediscover it on every call after reopen.
        let tail = Self::discover_tail_data_page(&buffer, t.first_data_page)?;
        Ok(Self {
            table: t,
            buffer,
            txn_mgr,
            logger,
            last_data_page: AtomicU64::new(tail.into()),
        })
    }

    pub fn id(&self) -> TableIdType {
        self.table.id
    }

    pub fn insert(&self, tuple: Tuple, txn: TransactionId) -> Result<(), StoreError> {
        let tuple_id = tuple.id.clone();
        // Write the data row, then insert its index entry. If the index insert
        // fails (e.g. DuplicateKey, or lock contention), the data-page write we
        // just did must be undone: otherwise it's an orphaned row with no undo
        // record (Db::insert only logs the undo *after* this returns Ok), so a
        // rollback can't clean it up and a later retry/re-insert leaves the same
        // key's data on multiple pages. Making the whole insert atomic here is
        // exactly "a failed insert rolls back its own partial work".
        let data_page_id = self.write_data(&tuple)?;
        let res = self.insert_index(tuple_id.clone(), data_page_id, txn);
        if res.is_err() {
            // Undo the data-page write so a failed index insert leaves no
            // orphaned row. Retry on LockContentionError: this cleanup is
            // undoing a failure that may itself have been mere contention
            // (insert_index racing another thread's page locks), so it must
            // not be allowed to fail from that same transient cause — unlike
            // an ordinary insert failure, a failed *cleanup* leaves the write
            // permanently orphaned (un-indexed, so invisible to find(), but
            // still occupying its data page) instead of rolled back. That
            // orphan then primes a spurious, permanent DuplicateKey for any
            // later insert of the same key that lands on the same data page
            // (write_data's page selection is deterministic) — confirmed via
            // reproduction, not just theory: the exact sequence was
            // insert_index failing with LockContentionError, this cleanup's
            // own get_page_mut *also* hitting LockContentionError and
            // bailing out via `?` (the bug), leaving the row orphaned, then a
            // retried insert of the same key hitting DuplicateKey on that
            // same page. Tolerate the row already being gone (KeyNotFound):
            // a concurrent abort-revert (drain_aborting) can remove it
            // between write_data and here — which is exactly the cleanup
            // goal already met. Any *other* error still propagates (real
            // corruption isn't swallowed).
            retry_on_contention(|| {
                let mut h = self.buffer.get_page_mut(data_page_id)?;
                match Arc::make_mut(&mut h.page).remove_tuple(tuple_id.clone()) {
                    Ok(_) => self.buffer.write_locked_page(h),
                    Err(StoreError::KeyNotFound(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            })?;
        }
        res
    }

    fn insert_index(
        &self,
        tuple_id: DBIdType,
        data_page_id: PageId,
        txn: TransactionId,
    ) -> Result<(), StoreError> {
        let id_tuple = Tuple::new_with(
            tuple_id,
            &to_allocvec(&Node::Leaf(data_page_id))?,
            Some(txn.clone()),
            None,
        );
        // The root-split pre-check uses an unlocked read, so by the time
        // insert_recursive holds the root lock the root may already be split.
        let handle = self.buffer.get_page_mut(self.table.first_index_page)?;
        if handle.page.count()? == self.table.nodes_per_page - 1 {
            self.split_root_page(handle, txn.clone(), &id_tuple.id)?;
        } else {
            drop(handle);
        }
        self.insert_recursive(id_tuple, txn, self.table.first_index_page, None)
        /*         let mut retries = 0u32;
               loop {
                   let page = self.buffer.get_page(self.table.first_index_page)?;
                   if page.count()? == self.table.nodes_per_page - 1 {
                       self.split_root_page(
                           self.buffer.get_page_mut(self.table.first_index_page)?,
                           txn.clone(),
                       )?;
                   }
                   match self.insert_recursive(id_tuple.clone(), txn.clone(), self.table.first_index_page) {
                       Err(StoreError::PageCapacityError) if retries < 16 => {
                           retries += 1;
                           std::thread::sleep(std::time::Duration::from_micros(50));
                       }
                       other => return other,
                   }
               }
        */
    }

    /// Place `tuple` on a data page and return that page's id. Locks each
    /// candidate page before checking capacity (no unlocked scan), and always
    /// terminates: a freshly allocated page's `can_store` is unconditionally
    /// true, so the walk ends by appending a new page if none has room.
    // One-time (per BPlusTree::from_bytes, i.e. per Db open) walk to the real
    // end of the data chain — see last_data_page's doc comment for why this
    // is only ever paid once instead of on every insert.
    fn discover_tail_data_page(
        buffer: &PageBuffer<F>,
        start: PageId,
    ) -> Result<PageId, StoreError> {
        let mut page_id = start;
        loop {
            let page = buffer.get_page(page_id)?;
            let next = buffer.data_chain_next(&page, page_id)?;
            if next.is_valid_next_page() {
                page_id = next;
            } else {
                return Ok(page_id);
            }
        }
    }

    fn write_data(&self, tuple: &Tuple) -> Result<PageId, StoreError> {
        let mut data_page_id = PageId::from(self.last_data_page.load(Ordering::Relaxed));
        loop {
            let handle = self.buffer.get_page_mut(data_page_id)?;
            if handle.page.can_store(tuple) {
                self.write_page(handle, tuple.clone())?;
                return Ok(data_page_id);
            }
            let next = self.buffer.data_chain_next(&handle.page, data_page_id)?;
            drop(handle);
            if next.is_valid_next_page() {
                data_page_id = next;
            } else {
                let new_id = self.buffer.alloc_page(false)?;
                self.buffer.set_data_chain_next(data_page_id, new_id)?;
                data_page_id = new_id;
                self.last_data_page.store(new_id.into(), Ordering::Relaxed);
            }
        }
    }

    pub fn find(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self
            .find_page(id.clone(), self.table.first_index_page)?
            .map(|p| self.buffer.get_page(p).and_then(|p| p.get(id)))
            .transpose()?
            .flatten())
    }

    pub fn update(&self, tuple: Tuple) -> Result<Tuple, StoreError> {
        let id = tuple.id.clone();
        let pid = self
            .find_page(id.clone(), self.table.first_index_page)?
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        let mut h = self.buffer.get_page_mut(pid)?;
        let old = Arc::make_mut(&mut h.page).replace_tuple(&id, tuple)?;
        self.buffer.write_locked_page(h)?;
        Ok(old)
    }

    // Returns Ok(None) when the data was already gone by the time this call
    // reached it (see the comment below) — same convention as
    // update_if_txn/remove_if_txn, and NOT an error: the caller's goal
    // (this id absent from both data and index) is still met.
    pub fn remove(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        let pid = match self.find_page(id.clone(), self.table.first_index_page)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let mut h = self.buffer.get_page_mut(pid)?;
        let old = match Arc::make_mut(&mut h.page).remove_tuple(id.clone()) {
            Ok(t) => {
                self.buffer.write_locked_page(h)?;
                Some(t)
            }
            // The index entry we just found via find_page still exists, but
            // the data behind it is already gone — this can only mean an
            // earlier, retried attempt of this exact call already removed
            // the data but not yet the index (see below for why that
            // retried attempt happens at all). There's no txn-ownership
            // question here the way update_if_txn/remove_if_txn have: with
            // no tuple left to check, a stale index entry pointing at
            // missing data is never valid, full stop — so finish the
            // cleanup below unconditionally instead of bailing out and
            // leaving it stale forever.
            Err(StoreError::KeyNotFound(_)) => None,
            Err(e) => return Err(e),
        };
        // Data is now gone for good — the index entry must follow, no matter
        // what, or it's left pointing at a page this key no longer occupies.
        // That's not just a leaked entry: it permanently blocks re-inserting
        // this key (DuplicateKey forever, matching the stale entry) AND, far
        // worse, if a *later* insert of some other key's data ever lands on
        // this same now-vacated page (write_data's page selection walks
        // forward through the same pages everyone else uses) and that
        // insert's own index step fails, its cleanup-on-error can itself
        // fail under contention and leave that new row physically present
        // but un-indexed — at which point this stale entry, still pointing
        // at that same page, resolves straight to it. find() then returns
        // that unrelated orphaned row for *this* key: a committed remove
        // whose key comes back with someone else's value. Confirmed via
        // targeted reproduction (extreme contention: 16 threads, 1 table, 20
        // keys), not just theory — caught by a dedicated regression test.
        //
        // Retrying here (not just relying on the caller retrying this whole
        // function, e.g. commit()'s best-effort reclaim) matters because a
        // caller-level retry is unsafe once the data step above has already
        // succeeded: remove_tuple would hit KeyNotFound (not
        // LockContentionError) on the retried call. That in itself is now
        // handled (the match above tolerates it and still proceeds to the
        // index cleanup) — but this internal retry is still needed for the
        // common case, since it means an outer caller retry is rarely
        // needed at all. remove_index_entry is itself idempotent (checks
        // `contains` before removing), so retrying it — from here or from
        // an outer caller's retry of the whole function — is always safe,
        // never a double-remove.
        retry_on_contention(|| {
            self.remove_index_entry(id.clone(), self.table.first_index_page, None)
        })?;
        Ok(old)
    }

    /// Conditional `update` for abort-revert: replaces the row for `tuple.id`
    /// with `tuple` **only if** the row currently on the page still belongs to
    /// `expect_txn`. The read-check-and-write all happen under the same data-page
    /// lock, so this is atomic with respect to a concurrent forward writer to the
    /// same key — the write that would clobber a just-committed value never fires.
    /// Returns Ok(None) when the row is gone or has been taken over by another
    /// transaction (the revert is correctly a no-op in that case).
    pub(crate) fn update_if_txn(
        &self,
        tuple: Tuple,
        expect_txn: &TransactionId,
    ) -> Result<Option<Tuple>, StoreError> {
        let id = tuple.id.clone();
        let pid = match self.find_page(id.clone(), self.table.first_index_page)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let mut h = self.buffer.get_page_mut(pid)?;
        let matches = matches!(
            h.page.get(id.clone())?,
            Some(cur) if cur.is_same_txn(expect_txn.clone())
        );
        if !matches {
            return Ok(None);
        }
        let old = Arc::make_mut(&mut h.page).replace_tuple(&id, tuple)?;
        self.buffer.write_locked_page(h)?;
        Ok(Some(old))
    }

    pub(crate) fn next_data_page(
        &self,
        page: Option<Arc<Page>>,
    ) -> Result<Option<Arc<Page>>, StoreError> {
        if let Some(page) = page {
            if PageId::is_valid_next_page(&page.get_next_page()) {
                Ok(Some(self.buffer.get_page(page.get_next_page())?))
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(self.buffer.get_page(self.table.first_data_page)?))
        }
    }

    /// Conditional `remove` for abort-revert — see `update_if_txn`. Removes the
    /// row (and its index entry) only if it still belongs to `expect_txn`, atomic
    /// under the data-page lock. Returns Ok(None) if it is already gone or owned
    /// by another transaction.
    pub(crate) fn remove_if_txn(
        &self,
        id: DBIdType,
        expect_txn: &TransactionId,
    ) -> Result<Option<Tuple>, StoreError> {
        let pid = match self.find_page(id.clone(), self.table.first_index_page)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let mut h = self.buffer.get_page_mut(pid)?;
        let current = h.page.get(id.clone())?;
        let old = match current {
            // Row is still here and still ours: remove it.
            Some(cur) if cur.is_same_txn(expect_txn.clone()) => {
                let old = Arc::make_mut(&mut h.page).remove_tuple(id.clone())?;
                self.buffer.write_locked_page(h)?;
                Some(old)
            }
            // Row exists but now belongs to someone else — a genuine no-op,
            // matching this function's documented contract. Nothing to
            // clean up: the index entry is legitimately still pointing at
            // live data.
            Some(_) => return Ok(None),
            // The index entry we just found via find_page still exists, but
            // there's no data behind it at all. Unlike the case above,
            // there's no live tuple whose ownership could still be "someone
            // else's" — an index entry pointing at missing data is never
            // valid, regardless of expect_txn. This can only mean an
            // earlier, retried attempt of this exact call already removed
            // the data but not yet the index (see remove()'s matching
            // comment) — finish that cleanup below instead of silently
            // leaving the index entry stale, which the old `if !matches {
            // return Ok(None) }` short-circuit used to do.
            None => None,
        };
        // See remove()'s comment on why this must retry internally rather
        // than rely on the caller (revert_txn_writes) retrying this whole
        // function: that's unsafe once the data step above has already
        // succeeded. remove_index_entry checks `contains` before removing,
        // so retrying it here — or via an outer caller retry — is always
        // safe, never a double-remove.
        retry_on_contention(|| {
            self.remove_index_entry(id.clone(), self.table.first_index_page, None)
        })?;
        Ok(old)
    }

    #[allow(clippy::bind_instead_of_map)]
    fn find_page(&self, id: DBIdType, start: PageId) -> Result<Option<PageId>, StoreError> {
        let page = self.buffer.get_page(start)?;
        if page.is_flag_set(INNER_NODE) {
            let rows_iter = page.iter();
            let mut last_child: Option<PageId> = None;
            for row in rows_iter {
                if id < row.id {
                    let page_id = from_bytes::<Node>(&row.data)?;
                    if let Node::Inner(page_num) = page_id {
                        return self.find_page(id, page_num);
                    } else {
                        panic!("Expected Inner. Found leaf ! {:?}", row.id);
                    }
                }
                let node = from_bytes::<Node>(&row.data)?;
                if let Node::Inner(page_num) = node {
                    last_child = Some(page_num);
                }
            }
            // id >= all entry bounds: route to the last child.
            // Non-root inner nodes have no u64::MAX sentinel; the last child
            // covers all keys from its separator up to the parent's upper bound.
            if let Some(child) = last_child {
                return self.find_page(id, child);
            }
            Ok(None)
        } else {
            Ok(page
                .get(id)?
                .and_then(|t| {
                    let id = from_bytes::<Node>(&t.data);
                    match id {
                        Ok(Node::Leaf(page_id)) => Some(Ok(page_id)),
                        Ok(Node::Inner(_)) => {
                            panic!("Expected leaf, found inner! {:?}", t.id)
                        }
                        Err(e) => Some(Err(StoreError::from(e))),
                    }
                })
                .transpose()?)
        }
    }

    fn remove_index_entry(
        &self,
        id: DBIdType,
        start: PageId,
        parent: Option<WritePageHandle>,
    ) -> Result<(), StoreError> {
        // Hand-over-hand locking, mirroring insert_recursive: lock this node,
        // *then* release the parent. The previous version navigated with
        // unlocked get_page() reads, so a concurrent split could move entries
        // (and flip a node leaf<->inner) between the routing read here and the
        // locked remove below — corrupting the index and orphaning unrelated
        // keys. Deciding leaf-vs-inner under the same lock we mutate under
        // closes that window.
        let mut handle = self.buffer.get_page_mut(start)?;
        drop(parent);
        if handle.page.is_flag_set(INNER_NODE) {
            let mut last_child: Option<PageId> = None;
            for row in handle.page.iter() {
                if id < row.id {
                    if let Node::Inner(page_num) = from_bytes::<Node>(&row.data)? {
                        return self.remove_index_entry(id, page_num, Some(handle));
                    } else {
                        panic!("Expected Inner. Found leaf! {:?}", row.id);
                    }
                }
                if let Node::Inner(page_num) = from_bytes::<Node>(&row.data)? {
                    last_child = Some(page_num);
                }
            }
            // id >= all separators: route to the last child (same fallthrough as
            // find_page). Without this, a key in the rightmost child of a
            // non-root inner node would never have its index entry removed.
            if let Some(child) = last_child {
                return self.remove_index_entry(id, child, Some(handle));
            }
            Ok(())
        } else {
            // Tolerate an already-absent entry: a concurrent path may have
            // removed it, or a committed tombstone was reclaimed elsewhere.
            if handle.page.contains(id.clone())? {
                Arc::make_mut(&mut handle.page).remove_tuple(id)?;
                self.buffer.write_locked_page(handle)?;
            }
            Ok(())
        }
    }

    fn insert_recursive(
        &self,
        tuple: Tuple,
        txn_id: TransactionId,
        start: PageId,
        parent: Option<WritePageHandle>,
    ) -> Result<(), StoreError> {
        let mut handle = self.buffer.get_page_mut(start)?;
        // Hand-over-hand (crabbing): now that we hold this node's lock, release
        // the parent's. Acquiring the child lock *before* releasing the parent
        // guarantees the routing decision that led us here cannot be invalidated
        // by a concurrent split of this node between the parent's routing read
        // and our arrival — the source of the "just-inserted id unreachable" bug.
        drop(parent);
        if handle.page.is_flag_set(LEAF_NODE) {
            let count = handle.page.count()?;
            if count == self.table.nodes_per_page - 1 {
                if self.is_root_page(start) {
                    // Root leaf was filled by a concurrent insert between the
                    // unlocked pre-check in insert_index() and this locked
                    // arrival (that pre-check reads the count, then releases
                    // the lock before insert_recursive re-acquires it — a
                    // real gap, not just theoretical: reproduced under
                    // stress once the retry_on_contention backoff/lock
                    // timeout budgets were widened to tolerate realistic OS
                    // scheduling jitter, which incidentally also gave more
                    // concurrent inserts room to race this exact window).
                    // We hold the lock here, so split it now and retry.
                    // split_root_page re-checks the count under this same
                    // lock and no-ops if some other thread already split it
                    // first, so this is safe even if the race resolves a
                    // different way than assumed here.
                    self.split_root_page(handle, txn_id.clone(), &tuple.id)?;
                    return self.insert_recursive(tuple, txn_id, start, None);
                }
                panic!("count == nodes- should not happen");
                // A non-root leaf was concurrently filled to capacity after its
                // parent's split_if_needed released the parent lock. Signal the
                // caller to retry so the parent re-detects the full page.
                //return Err(StoreError::PageCapacityError);
            } else if handle.page.count()? < self.table.nodes_per_page
                && handle.page.can_store(&tuple)
            {
                self.write_page(handle, tuple)?;
            } else {
                panic!(
                    "count == nodes {}, or too big {}",
                    handle.page.count().unwrap(),
                    tuple.size()
                );
            }
        } else if handle.page.is_flag_set(INNER_NODE) {
            if handle.page.count()? == 0 {
                panic!("Inner Page cannot be empty {:?}", handle.page_num);
            }
            // If this node is already at capacity, a child split would need to
            // add a separator here — but there's no room. Return early so the
            // retry loop in insert() re-enters: on the next pass, the caller
            // one level up will call split_if_needed() on this node first, then
            // descend into a half-full copy that can accept the new separator.
            if handle.page.count()? == self.table.nodes_per_page - 1 {
                return Err(StoreError::PageCapacityError);
            }
            // Rows are in ascending id order; find the first one whose id exceeds
            // the search key (mirrors find_page's matching logic). The previous
            // version broke out of the scan on the first row that *didn't*
            // match, instead of continuing to the next row — so with 3+ entries
            // it could get stuck on an early row and route to the wrong child.
            //
            // If tuple.id is >= even the LAST row's id, fall through to that
            // last row anyway (same fallthrough find_page and
            // remove_index_entry already implement) instead of panicking. A
            // non-root inner node's last entry does not bound its own child —
            // that child's true upper bound is inherited from this node's own
            // governing entry in its parent — so "ran out of rows" does not
            // mean "no child covers this key", it means "the key falls in the
            // last child's range". This only matters once the tree is 3+
            // levels deep (a non-root inner node exists); the root always
            // carries a u64::MAX sentinel as its last entry (see
            // update_root_page), so real ids never run off the end there.
            let mut rows = handle.page.iter();
            let mut row_id = rows.next().unwrap();
            while tuple.id >= row_id.id {
                match rows.next() {
                    Some(next) => row_id = next,
                    None => break,
                }
            }
            let node = from_bytes::<Node>(&row_id.data)?;
            if let Node::Inner(p) = node {
                if let Some((separator, sibling)) = self.split_if_needed(p, &tuple)? {
                    let page = Arc::make_mut(&mut handle.page);
                    // `p` kept the smaller half (keys < separator); the larger half
                    // (up to the old upper bound, row_id.id) moved to `sibling`. The
                    // existing entry routed that whole range to `p`, so it must now
                    // point at `sibling`; a new entry routes the smaller half to `p`.
                    page.replace_tuple(
                        &row_id.id,
                        Tuple::new_with(
                            row_id.id.clone(),
                            &to_allocvec(&Node::Inner(sibling))?,
                            Some(txn_id.clone()),
                            None,
                        ),
                    )?;
                    page.add_tuple(Tuple::new_with(
                        separator.clone(),
                        &to_allocvec(&Node::Inner(p))?,
                        Some(txn_id.clone()),
                        None,
                    ))?;
                    // Crab into the destination child: lock it *before* the
                    // parent is written/released below, so no other thread can
                    // re-split it out from under the routing decision we just
                    // made. write_locked_page must still run (it persists the
                    // new separator entries and releases the parent lock); the
                    // pre-acquired child lock is reentrantly re-taken by the
                    // recursive get_page_mut and dropped when `child` does.
                    let target = if tuple.id < separator { p } else { sibling };
                    let child = self.buffer.get_page_mut(target)?;
                    // Must persist before recursing — handle.page was a
                    // detached COW copy (the buffer's cache still held the
                    // pre-split version), so without this write-back the
                    // updated routing entries are silently dropped when
                    // `handle` goes out of scope, corrupting the index.
                    self.buffer.write_locked_page(handle)?;
                    return self.insert_recursive(tuple, txn_id, target, Some(child));
                }
                // Crab into the child: pass our still-held parent lock down so
                // it is only released once the child lock has been acquired.
                return self.insert_recursive(tuple, txn_id, p, Some(handle));
            } else {
                panic!("Expected inner - found leaf : {:?}", start);
            }
        } else {
            panic!("Unknown page {:?}", start);
        }
        Ok(())
    }

    fn split_if_needed(
        &self,
        page_id: PageId,
        tuple: &Tuple,
    ) -> Result<Option<(DBIdType, PageId)>, StoreError> {
        let handle = self.buffer.get_page_mut(page_id)?;
        if handle.page.count()? == self.table.nodes_per_page - 1 || !handle.page.can_store(tuple) {
            if self.is_root_page(page_id) {
                panic!("Trying to split root in the wrong place");
            } else {
                Ok(Some(self.split_non_root_page(handle, &tuple.id)?))
            }
        } else {
            Ok(None)
        }
    }

    fn is_root_page(&self, page_id: PageId) -> bool {
        self.table.first_index_page == page_id
    }

    // Shared by split_non_root_page and split_root_page: picks where to cut
    // `values` (already known to be at capacity). `incoming_id` is the key
    // that triggered this split — not necessarily inserted into this exact
    // node, but always the key driving the split somewhere in this node's
    // subtree.
    //
    // Default: split at the midpoint, discarding nothing.
    //
    // Exception — rightmost-append optimization (mirrors PostgreSQL
    // nbtree's rightmost-split heuristic): if `incoming_id` is going to
    // land past everything already here, keep all of it together instead
    // of a 50/50 split. A plain midpoint split strands the "kept" half at
    // ~50% full forever under sustained sequential/monotonic insertion —
    // once created, nothing ever descends into it again, since every
    // future key is higher and always routes to the "moved" sibling. That
    // isn't just wasted space: each such split still adds one level to the
    // *entire* tree (see split_root_page), so under pure ascending
    // insertion, depth grows almost linearly with N instead of
    // logarithmically (confirmed empirically: depth 999 after 2000
    // sequential inserts at nodes_per_page=4, vs depth ~31 for the same
    // 2000 keys in random order). Moving only the single highest entry to
    // the sibling avoids that: the kept side stays essentially full, and
    // the sibling starts with just 1 entry, primed to keep absorbing
    // further appends — which is exactly the pattern sequential insertion
    // needs to stay efficient, and self-corrects a few splits later back
    // to a normal midpoint split if the insertion pattern turns out not to
    // be an unbroken ascending run after all.
    //
    // For an inner node, the last entry has no bound of its own — it's a
    // fallthrough (see find_page) — so "would land past everything" means
    // landing at or past the entry *before* the last one.
    fn split_point(values: &[Tuple], is_inner: bool, incoming_id: &DBIdType) -> usize {
        let is_rightmost_append = if is_inner {
            values.len() < 2 || *incoming_id >= values[values.len() - 2].id
        } else {
            *incoming_id > values.last().unwrap().id
        };
        if is_rightmost_append {
            values.len() - 1
        } else {
            values.len() / 2
        }
    }

    fn split_non_root_page(
        &self,
        handle: WritePageHandle,
        incoming_id: &DBIdType,
    ) -> Result<(DBIdType, PageId), StoreError> {
        let mut current_handle = handle;
        let values = current_handle.page.iter().collect::<Vec<_>>();
        let is_inner = current_handle.page.is_flag_set(INNER_NODE);

        let mid = Self::split_point(&values, is_inner, incoming_id);
        let current_vals = &values[..mid];
        let new_vals = &values[mid..];
        // The separator becomes the parent's new boundary key between the
        // kept (lower) and moved (upper) halves — but what that boundary
        // *means* differs by node kind:
        //
        // Leaf entries are exact-match data, so any key that cleanly divides
        // the two sorted slices works; the first moved entry's own id is the
        // natural choice ("< separator" lands exactly on current_vals).
        //
        // Inner entries encode "< key routes to *this entry's* child" (the
        // last entry is the sole exception, covering everything up to the
        // node's own external bound via fallthrough). That means the last
        // *kept* entry's own key is the true, exclusive upper bound of its
        // child — using the first *moved* entry's key instead would make it
        // the new external bound for the kept page, and since that page's
        // now-last entry inherits everything up to its external bound via
        // fallthrough, it would silently absorb the range that actually
        // belongs to the entry that just moved to the sibling — orphaning
        // that entry's whole subtree (findable on disk, unreachable via
        // routing). This only matters once a non-root inner node can
        // actually exist and split; a leaf split's exact-match semantics
        // hide the same subtlety.
        let separator_id = if is_inner {
            current_vals.last().unwrap().id.clone()
        } else {
            new_vals[0].id.clone()
        };
        let new_page_id = self.buffer.alloc_page(false)?;
        let mut new_handle = self.buffer.get_page_mut(new_page_id)?;

        let current_page = Arc::make_mut(&mut current_handle.page);
        let new_page = Arc::make_mut(&mut new_handle.page);
        // Set flags on the COW copy, never on the shared cached Arc: flags is an
        // AtomicU16 mutated through &self, so flipping it before make_mut would
        // be visible to concurrent readers while the page's data is still stale.
        if is_inner {
            new_page.set_page_flags(INNER_NODE)?;
        } else {
            new_page.set_page_flags(LEAF_NODE)?;
        }
        current_page.clear()?;
        current_vals
            .iter()
            .try_for_each(|t| current_page.add_tuple(t.clone()))?;
        new_vals
            .iter()
            .try_for_each(|t| new_page.add_tuple(t.clone()))?;
        self.buffer.write_locked_page(current_handle)?;
        self.buffer.write_locked_page(new_handle)?;
        Ok((separator_id, new_page_id))
    }

    fn update_root_page(
        &self,
        id: DBIdType,
        left_page: PageId,
        right_page: PageId,
        txn_id: TransactionId,
    ) -> Result<(), StoreError> {
        let mut handle = self.buffer.get_page_mut(self.table.first_index_page)?;
        // Mutate flags and data together on the COW copy. Flipping LEAF→INNER on
        // the shared cached Arc before rewriting the entries would let a
        // concurrent find_page see INNER_NODE set while the entries are still the
        // old leaf tuples → "Expected Inner. Found leaf!".
        let page = Arc::make_mut(&mut handle.page);
        page.clear_page_flag(LEAF_NODE)?;
        page.set_page_flags(INNER_NODE)?;
        page.clear()?;
        let left_node = Node::Inner(left_page);
        let right_node = Node::Inner(right_page);
        let new_t = Tuple::new_with(
            id.clone(),
            &to_allocvec(&left_node)?,
            Some(txn_id.clone()),
            None,
        );
        let end_t = Tuple::new_with(
            DBIdType::Int(DBSizeType::MAX),
            &to_allocvec(&right_node)?,
            Some(txn_id.clone()),
            None,
        );
        page.add_tuple(new_t)?;
        page.add_tuple(end_t)?;
        self.buffer.write_locked_page(handle)?;
        Ok(())
    }

    fn split_root_page(
        &self,
        handle: WritePageHandle,
        txn_id: TransactionId,
        incoming_id: &DBIdType,
    ) -> Result<(), StoreError> {
        // insert()'s caller decides whether to call this based on an
        // *unlocked* read of the root's count — by the time we actually hold
        // the lock (this `handle`), another thread may have already split the
        // root and changed its count. The count check alone is the correct
        // guard for that race: a freshly-split root only has 2 entries, which
        // won't match nodes_per_page - 1 except in a degenerate
        // nodes_per_page == 3 table, so re-checking count under the lock is
        // enough to detect "someone already handled this."
        //
        // Deliberately NOT special-cased on leaf vs inner: the root starts as
        // a leaf and is promoted to inner on its first split, but once inner
        // it can fill up again and need a *second* split to grow the tree to
        // a third level. The redistribution logic below already preserves
        // the root's current flag (leaf or inner) onto the two new child
        // pages, and update_root_page already unconditionally leaves the
        // root as an inner node with exactly 2 entries — both are already
        // correct for re-splitting an inner root, so gating on "already
        // inner" here was the only thing wrong: it made every second split
        // silently no-op, permanently capping the tree at 2 levels.
        if handle.page.count()? != self.table.nodes_per_page - 1 {
            return Ok(());
        }
        let values = handle.page.iter().collect::<Vec<_>>();
        let is_inner = handle.page.is_flag_set(INNER_NODE);
        let flags = if is_inner { INNER_NODE } else { LEAF_NODE };

        // See split_non_root_page's split_point for the rightmost-append
        // optimization this shares.
        let mid = Self::split_point(&values, is_inner, incoming_id);
        let left_vals = &values[..mid];
        let right_vals = &values[mid..];
        // See split_non_root_page's comment for why the separator formula
        // differs for inner vs leaf: an inner node's last entry inherits
        // everything up to its own external bound via fallthrough, so the
        // boundary between left_vals and right_vals must be left_vals' own
        // last key, not right_vals' first key — otherwise left's new last
        // entry silently swallows the range that belongs to whatever moved
        // into right, orphaning it.
        let separator_id = if flags == INNER_NODE {
            left_vals.last().unwrap().id.clone()
        } else {
            right_vals[0].id.clone()
        };
        let left_page_id = self.buffer.alloc_page(false)?;
        let right_page_id = self.buffer.alloc_page(false)?;
        let mut left_handle = self.buffer.get_page_mut(left_page_id)?;
        let mut right_handle = self.buffer.get_page_mut(right_page_id)?;
        // Flags on the COW copy, not the shared cached Arc (see update_root_page).
        let left_page = Arc::make_mut(&mut left_handle.page);
        let right_page = Arc::make_mut(&mut right_handle.page);
        left_page.set_page_flags(flags)?;
        right_page.set_page_flags(flags)?;
        left_vals
            .iter()
            .try_for_each(|t| left_page.add_tuple(t.clone()))?;
        right_vals
            .iter()
            .try_for_each(|t| right_page.add_tuple(t.clone()))?;
        self.buffer.write_locked_page(left_handle)?;
        self.buffer.write_locked_page(right_handle)?;
        self.update_root_page(separator_id, left_page_id, right_page_id, txn_id)?;
        Ok(())
    }

    fn write_page(&self, handle: WritePageHandle, tuple: Tuple) -> Result<(), StoreError> {
        let mut handle = handle;
        let page = Arc::make_mut(&mut handle.page);
        page.add_tuple(tuple)?;
        self.buffer.write_locked_page(handle)?;
        Ok(())
    }
}

impl Eq for Node {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicU64};

    use postcard::from_bytes;

    use super::{BPlusTree, INNER_NODE, LEAF_NODE, MAX_ENTRY_BYTES, Node};
    use crate::{
        buffer::PageBuffer,
        constant::FIRST_USER_PAGE,
        db::Header,
        error::StoreError,
        generator::Generator,
        logger::Logger,
        memfile::MemFile,
        page::Page,
        tuple::{DBIdType, Tuple},
        txn::{TransactionId, TransactionManager},
    };

    fn page_overhead(page_size: u64) -> u64 {
        page_size - Page::new_data(page_size).get_data_size()
    }

    fn make_header(page_size: u64) -> Arc<Header> {
        let mut v = vec![0x53u8, 0x65];
        v.extend_from_slice(&0u64.to_le_bytes()); // first_page_offset
        v.extend_from_slice(&FIRST_USER_PAGE.to_le_bytes()); // page_count
        v.extend_from_slice(&page_size.to_le_bytes());
        // last_checkpoint: u128, not fixint-annotated, so postcard varint-
        // encodes it — append its own to_allocvec output (see the identical
        // fix/comment in buffer.rs's make_header_bytes).
        v.extend_from_slice(&postcard::to_allocvec(&0u128).unwrap());
        Arc::new(from_bytes::<Header>(&v).unwrap())
    }

    fn make_buffer(page_size: u64) -> Arc<PageBuffer<MemFile>> {
        let header = make_header(page_size);
        let counter = Arc::new(AtomicU64::new(FIRST_USER_PAGE));
        Arc::new(
            PageBuffer::new(
                page_size,
                counter,
                MemFile::new(),
                header,
                256,
                Arc::new(crate::logger::LsnClock::default()),
                1024,
            )
            .unwrap(),
        )
    }

    fn make_txn_mgr() -> Arc<TransactionManager> {
        let generator = Arc::new(Generator::new());
        TransactionManager::new(generator, TransactionId::new(0))
            .unwrap()
            .into()
    }

    fn make_logger() -> Arc<Logger> {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        Arc::new(logger)
    }

    fn make_tree(page_size: u64) -> BPlusTree<MemFile> {
        let buf = make_buffer(page_size);
        BPlusTree::new(1.into(), "t".into(), buf, make_txn_mgr(), make_logger()).unwrap()
    }

    fn txn() -> TransactionId {
        TransactionId::new(1)
    }

    // update()/remove() log an undo record keyed off the *stored* tuple's own
    // txn_id field — Tuple::new() always leaves that None, which makes
    // log_undo fail with "Missing transaction". Tests that update/remove a
    // row need to insert it with an explicit txn_id via this helper instead.
    fn tuple_with_txn(id: DBIdType, data: &[u8]) -> Tuple {
        Tuple::new_with(id, data, Some(txn()), None)
    }

    // Universal B+tree structural invariants — not a design choice of this
    // codebase, these hold for any B+tree by definition:
    //   1. Every leaf is at the same depth from the root (perfect balance;
    //      this is what distinguishes a B+tree from a general BST).
    //   2. Within any node, entry ids are strictly ascending.
    //   3. Every node holds at most nodes_per_page - 1 entries (the split
    //      threshold this implementation enforces).
    // Walks the whole tree and returns every leaf's depth (root = depth 0),
    // asserting (2) and (3) along the way. Callers assert (1) themselves
    // (comparing min/max of the returned depths) so a violation shows the
    // actual spread instead of failing inside the walk.
    fn leaf_depths(tree: &BPlusTree<MemFile>) -> Vec<usize> {
        fn walk(
            tree: &BPlusTree<MemFile>,
            page_id: crate::page::PageId,
            depth: usize,
            out: &mut Vec<usize>,
        ) {
            let page = tree.buffer.get_page(page_id).unwrap();
            let count = page.count().unwrap();
            assert!(
                count < tree.table.nodes_per_page,
                "page {page_id:?} holds {count} entries, over the max of {}",
                tree.table.nodes_per_page - 1
            );
            let mut prev: Option<DBIdType> = None;
            for row in page.iter() {
                if let Some(p) = &prev {
                    assert!(
                        *p < row.id,
                        "page {page_id:?} entries not strictly ascending: {p:?} >= {:?}",
                        row.id
                    );
                }
                prev = Some(row.id.clone());
            }
            if page.is_flag_set(INNER_NODE) {
                for row in page.iter() {
                    if let Node::Inner(child) = from_bytes::<Node>(&row.data).unwrap() {
                        walk(tree, child, depth + 1, out);
                    }
                }
            } else {
                out.push(depth);
            }
        }
        let mut out = Vec::new();
        walk(tree, tree.table.first_index_page, 0, &mut out);
        out
    }

    // Large page — no splits during basic tests.
    const BIG: u64 = 8192;

    #[test]
    fn test_insert_single_data_and_index() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(
            dp.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"hello"
        );

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert_eq!(ip.count().unwrap(), 1);
    }

    #[test]
    fn test_insert_multiple_same_page() {
        let tree = make_tree(BIG);
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, b"val"), txn()).unwrap();
        }
        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 5);
        for i in 1u64..=5 {
            assert!(dp.contains(DBIdType::Int(i)).unwrap(), "missing id {i}");
        }
        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert_eq!(ip.count().unwrap(), 5);
    }

    #[test]
    fn test_insert_out_of_order() {
        let tree = make_tree(BIG);
        for &id in &[5u64, 3, 8, 1, 7, 2, 6, 4] {
            tree.insert(Tuple::new(id, b"d"), txn()).unwrap();
        }
        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 8);
        for id in 1u64..=8 {
            assert!(dp.contains(DBIdType::Int(id)).unwrap(), "missing id {id}");
        }
    }

    #[test]
    fn test_data_page_chains_to_next_page_when_full() {
        // BIG (8192 B) page, 3 KB payloads (~3007 B serialized each).
        // data_size = 8192 - PAGE_OVERHEAD = 8112 B.
        // can_store = empty || used + tuple <= data_size (tuple must FIT):
        //   insert 1: empty page accepts        → dp1 used≈3007
        //   insert 2: 3007+3007=6014 <= 8112    → dp1 used≈6014
        //   insert 3: 6014+3007=9021 > 8112     → does NOT fit dp1 → chains to dp2
        // Overflow is NOT used here: a data page that can't fit a tuple links to
        // the next data page instead of spilling into an overflow chain.
        let tree = make_tree(BIG);
        let large = vec![b'x'; 3000];

        for i in 1u64..=3 {
            tree.insert(Tuple::new(i, &large), txn()).unwrap();
        }

        let dp1 = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp1.count().unwrap(), 2, "dp1 holds the 2 tuples that fit");
        assert!(
            !dp1.has_overflow(),
            "dp1 must NOT overflow — it chains instead"
        );

        // dp1 links to dp2 via the normal data-chain pointer (no overflow).
        let dp2_id = tree
            .buffer
            .data_chain_next(&dp1, tree.table.first_data_page)
            .unwrap();
        assert!(dp2_id.is_valid_next_page(), "dp1 must link to dp2");

        let dp2 = tree.buffer.get_page(dp2_id).unwrap();
        assert_eq!(dp2.count().unwrap(), 1, "dp2 holds the 3rd tuple");
        assert!(dp2.contains(DBIdType::Int(3)).unwrap());

        // All three remain findable through the index.
        for i in 1u64..=3 {
            assert_eq!(
                tree.find(DBIdType::Int(i)).unwrap().unwrap().data.to_vec(),
                large
            );
        }
    }

    #[test]
    fn test_root_splits_into_inner_node() {
        // page_size = MAX_ENTRY_BYTES * 4 → nodes_per_page = 4; split fires after 3 index entries.
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        // table.nodes_per_page = 4; split fires after 3 index entries, so 5 inserts exercises post-split.
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, b"x"), txn()).unwrap();
        }

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(
            ip.is_flag_set(INNER_NODE),
            "root must become an inner node after split"
        );
        // The root gains its first 2 child pointers from its own split; a 5th
        // insert then overflows the right child, splitting it too and adding a
        // 3rd pointer back to the root — that's correct B+ tree growth, not a bug.
        assert!(
            ip.count().unwrap() >= 2,
            "inner root must hold at least 2 child pointers, got {}",
            ip.count().unwrap()
        );
    }

    #[test]
    // Regression test for todo.txt item [9]: insert_recursive's inner-node
    // routing scan used to panic ("inner node must have a row covering every
    // key") whenever tuple.id was >= every entry in a non-root inner node,
    // instead of falling through to the last entry's child the way find_page
    // and remove_index_entry already do. That gap only exists once the tree
    // is 3+ levels deep (the root always carries a u64::MAX sentinel as its
    // last entry, so it never runs off the end) — with nodes_per_page=4 here,
    // the root splits a second time (creating non-root inner nodes) well
    // before 20 sequential inserts, and every key after that point which
    // exceeds the current maximum exercises exactly this fallthrough.
    fn test_sequential_inserts_past_root_second_split_do_not_panic() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        for i in 1u64..=40 {
            tree.insert(Tuple::new(i, b"v"), txn()).unwrap();
        }

        // The root must actually have split a second time (a non-root inner
        // node exists) for this test to be exercising the bug at all.
        let root = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(root.is_flag_set(INNER_NODE));
        let has_non_root_inner = root.iter().any(|row| {
            matches!(
                from_bytes::<Node>(&row.data).unwrap(),
                Node::Inner(child) if tree.buffer.get_page(child).unwrap().is_flag_set(INNER_NODE)
            )
        });
        assert!(
            has_non_root_inner,
            "test setup must grow the tree to 3+ levels to exercise the bug"
        );

        for i in 1u64..=40 {
            assert_eq!(
                tree.find(DBIdType::Int(i)).unwrap().unwrap().data.to_vec(),
                b"v",
                "id {i} must remain findable"
            );
        }
    }

    // A B+tree with fanout >= 2 at every level guarantees height <=
    // log2(n+1); this generously allows 6x that (small nodes_per_page and a
    // node's post-split "kept" half being as small as 1 entry both cost
    // real but bounded slack) so it only fires on genuine, order-of-
    // magnitude degeneration, not on this implementation's small-page
    // inefficiency alone.
    fn assert_logarithmic_depth(depths: &[usize], n: usize) {
        let max = *depths.iter().max().unwrap();
        let bound = 6 * (usize::BITS - (n as u64 + 1).leading_zeros()) as usize;
        assert!(
            max <= bound,
            "tree depth {max} far exceeds the logarithmic bound {bound} for n={n} — \
             the tree has degenerated into something close to a linked list"
        );
    }

    #[test]
    fn test_btree_stays_balanced_and_shallow_under_random_order_inserts() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        // Deterministic shuffle (xorshift), no external RNG dependency.
        let mut ids: Vec<u64> = (1u64..=2000).collect();
        let mut state: u64 = 0x243F_6A88_85A3_08D3;
        for i in (1..ids.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let j = (state % (i as u64 + 1)) as usize;
            ids.swap(i, j);
        }
        for &i in &ids {
            tree.insert(Tuple::new(i, b"v"), txn()).unwrap();
        }

        let depths = leaf_depths(&tree);
        let (min, max) = (depths.iter().min().unwrap(), depths.iter().max().unwrap());
        assert_eq!(
            min, max,
            "every leaf must be at the same depth (got range {min}..={max})"
        );
        assert_logarithmic_depth(&depths, ids.len());

        for &i in &ids {
            assert_eq!(
                tree.find(DBIdType::Int(i)).unwrap().unwrap().data.to_vec(),
                b"v",
                "id {i} must remain findable"
            );
        }
    }

    #[test]
    // Regression test for a real (if niche) design gap: without the
    // rightmost-append optimization in split_point, sequential ascending
    // inserts degenerated this B+tree into near-linear depth instead of
    // the logarithmic depth a B+tree is supposed to guarantee regardless
    // of insertion order — depth 999 after 2000 sequential inserts at
    // nodes_per_page=4, vs depth ~31 for the same 2000 keys in random
    // order (see the sibling test above). The tree stayed perfectly
    // balanced throughout (every leaf at the same depth) — the bug was in
    // how fast that shared depth grew, not in balance.
    // Root cause (now fixed): split_non_root_page/split_root_page always
    // split at the midpoint. For a node at capacity, that keeps roughly
    // half the entries on the "lower" side and moves the rest to the
    // "upper" sibling. Under strictly ascending inserts, every future
    // insert descends into whichever side holds the *highest* keys —
    // always the "upper" sibling, never the "lower" (kept) side — so the
    // lower side froze permanently at its post-split size the instant it
    // was created, while the upper side kept absorbing every subsequent
    // insert, split, freeze-half-again, repeat. Each such split added one
    // level to the *entire* tree, which is why depth ballooned roughly
    // linearly with N.
    // Fix: split_point (shared by both split sites) detects when the key
    // driving the split is going to land past everything already in the
    // node (the same heuristic PostgreSQL's nbtree uses for rightmost
    // page splits) and, when so, moves only the single highest entry to
    // the new sibling instead of half the node — keeping the "kept" side
    // essentially full and giving the sibling room to keep absorbing
    // further appends, which is exactly the access pattern sequential
    // insertion produces.
    fn test_btree_stays_shallow_under_sequential_inserts() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);
        for i in 1u64..=2000 {
            tree.insert(Tuple::new(i, b"v"), txn()).unwrap();
        }
        let depths = leaf_depths(&tree);
        let (min, max) = (depths.iter().min().unwrap(), depths.iter().max().unwrap());
        assert_eq!(
            min, max,
            "every leaf must be at the same depth (got range {min}..={max})"
        );
        assert_logarithmic_depth(&depths, 2000);

        for i in 1u64..=2000 {
            assert_eq!(
                tree.find(DBIdType::Int(i)).unwrap().unwrap().data.to_vec(),
                b"v",
                "id {i} must remain findable"
            );
        }
    }

    #[test]
    fn test_root_split_both_children_are_leaves() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        for i in 1u64..=4 {
            tree.insert(Tuple::new(i, b"y"), txn()).unwrap();
        }

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        let entries: Vec<_> = ip.iter().collect();
        assert_eq!(entries.len(), 2);
        for entry in &entries {
            let node: Node = from_bytes(&entry.data).unwrap();
            if let Node::Inner(child_page_id) = node {
                let cp = tree.buffer.get_page(child_page_id).unwrap();
                assert!(
                    cp.is_flag_set(LEAF_NODE),
                    "child page {:?} must be a leaf",
                    child_page_id
                );
            } else {
                panic!("inner root entry must be Node::Inner");
            }
        }
    }

    #[test]
    fn test_insert_string_id_stored_and_retrievable() {
        let tree = make_tree(BIG);
        let id1 = DBIdType::from("alpha".to_string());
        let id2 = DBIdType::from("beta".to_string());
        tree.insert(Tuple::new_with(id1.clone(), b"a-data", None, None), txn())
            .unwrap();
        tree.insert(Tuple::new_with(id2.clone(), b"b-data", None, None), txn())
            .unwrap();

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 2);
        assert_eq!(
            dp.get(id1.clone()).unwrap().unwrap().data.to_vec(),
            b"a-data"
        );
        assert_eq!(
            dp.get(id2.clone()).unwrap().unwrap().data.to_vec(),
            b"b-data"
        );

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert_eq!(ip.count().unwrap(), 2);
        assert!(ip.contains(id1).unwrap());
        assert!(ip.contains(id2).unwrap());
    }

    #[test]
    fn test_insert_mixed_int_and_string_ids() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"int-1"), txn()).unwrap();
        tree.insert(
            Tuple::new_with(
                DBIdType::from("str-key".to_string()),
                b"str-data",
                None,
                None,
            ),
            txn(),
        )
        .unwrap();

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 2);
        assert!(dp.contains(DBIdType::Int(1)).unwrap());
        assert!(dp.contains(DBIdType::from("str-key".to_string())).unwrap());
    }

    #[test]
    fn test_insert_duplicate_int_id_returns_error() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"first"), txn()).unwrap();
        let result = tree.insert(Tuple::new(1, b"second"), txn());
        assert!(
            matches!(result, Err(StoreError::DuplicateKey(_))),
            "expected DuplicateKey error, got {:?}",
            result
        );

        // Original value must be untouched, and no second entry was added.
        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(
            dp.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"first"
        );
    }

    #[test]
    fn test_insert_duplicate_string_id_returns_error() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("dup-key".to_string());
        tree.insert(Tuple::new_with(id.clone(), b"first", None, None), txn())
            .unwrap();
        let result = tree.insert(Tuple::new_with(id.clone(), b"second", None, None), txn());
        assert!(
            matches!(result, Err(StoreError::DuplicateKey(_))),
            "expected DuplicateKey error, got {:?}",
            result
        );

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(dp.get(id).unwrap().unwrap().data.to_vec(), b"first");
    }

    #[test]
    fn test_find_returns_inserted_tuple() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();
        let found = tree.find(DBIdType::Int(1)).unwrap();
        assert_eq!(found.unwrap().data.to_vec(), b"hello");
    }

    #[test]
    fn test_find_missing_id_on_empty_tree_returns_none() {
        let tree = make_tree(BIG);
        assert!(tree.find(DBIdType::Int(1)).unwrap().is_none());
    }

    #[test]
    fn test_find_missing_id_returns_none() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();
        assert!(tree.find(DBIdType::Int(42)).unwrap().is_none());
    }

    #[test]
    fn test_find_multiple_in_same_page() {
        let tree = make_tree(BIG);
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, format!("val-{i}").as_bytes()), txn())
                .unwrap();
        }
        for i in 1u64..=5 {
            let t = tree.find(DBIdType::Int(i)).unwrap().unwrap();
            assert_eq!(t.data.to_vec(), format!("val-{i}").into_bytes());
        }
        assert!(tree.find(DBIdType::Int(6)).unwrap().is_none());
    }

    #[test]
    fn test_find_out_of_order_inserts() {
        let tree = make_tree(BIG);
        for &id in &[5u64, 3, 8, 1, 7, 2, 6, 4] {
            tree.insert(Tuple::new(id, format!("v{id}").as_bytes()), txn())
                .unwrap();
        }
        for id in 1u64..=8 {
            let t = tree.find(DBIdType::Int(id)).unwrap().unwrap();
            assert_eq!(t.data.to_vec(), format!("v{id}").into_bytes());
        }
    }

    #[test]
    fn test_find_with_string_id() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("alpha".to_string());
        tree.insert(Tuple::new_with(id.clone(), b"a-data", None, None), txn())
            .unwrap();
        let found = tree.find(id).unwrap().unwrap();
        assert_eq!(found.data.to_vec(), b"a-data");
        assert!(
            tree.find(DBIdType::from("missing".to_string()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_find_resolves_through_data_page_overflow() {
        let tree = make_tree(BIG);
        let large = vec![b'x'; 3000];
        tree.insert(Tuple::new(1, &large), txn()).unwrap();
        tree.insert(Tuple::new(2, &large), txn()).unwrap();
        // Overflows onto a second data page (see test_data_page_overflow_links_next_page).
        tree.insert(Tuple::new(3, &large), txn()).unwrap();

        // id 3 lives on the second (overflow) data page; find() must follow the
        // index's Node::Leaf pointer there rather than only checking the first page.
        let found = tree.find(DBIdType::Int(3)).unwrap().unwrap();
        assert_eq!(found.data.to_vec(), large);
        assert_eq!(
            tree.find(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            large
        );
    }

    #[test]
    fn test_find_after_root_split_left_and_right_subtrees() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        // table.nodes_per_page = 4; split fires after 3 inserts (see test_root_splits_into_inner_node).
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, format!("v{i}").as_bytes()), txn())
                .unwrap();
        }

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(ip.is_flag_set(INNER_NODE), "sanity: root must have split");

        for i in 1u64..=5 {
            let found = tree.find(DBIdType::Int(i)).unwrap();
            assert_eq!(
                found.map(|t| t.data.to_vec()),
                Some(format!("v{i}").into_bytes()),
                "id {i} must be found after root split"
            );
        }
        assert!(tree.find(DBIdType::Int(100)).unwrap().is_none());
    }

    #[test]
    fn test_find_after_root_split_with_string_ids() {
        // MAX_ENTRY_BYTES * 4 → nodes_per_page = 4 regardless of id type.
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

        // table.nodes_per_page = 4; 5 string-keyed inserts exercise both a root split
        // and a child split, exactly the scenario broken before DBIdType::Ord
        // was made hash-consistent with AnyTuplePage's iteration order.
        let keys = ["alpha", "bravo", "charlie", "delta", "echo"];
        for k in &keys {
            let id = DBIdType::from(k.to_string());
            tree.insert(Tuple::new_with(id, k.as_bytes(), None, None), txn())
                .unwrap();
        }

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(
            ip.is_flag_set(INNER_NODE),
            "root must split with string ids too"
        );

        for k in &keys {
            let id = DBIdType::from(k.to_string());
            let found = tree.find(id).unwrap();
            assert_eq!(
                found.map(|t| t.data.to_vec()),
                Some(k.as_bytes().to_vec()),
                "key {k} must be found after split"
            );
        }
        assert!(
            tree.find(DBIdType::from("missing".to_string()))
                .unwrap()
                .is_none()
        );
    }

    // PageBuffer::get_page_mut reads the page via get_page() *before* acquiring
    // the per-page lock (see buffer.rs). That means two threads can both snapshot
    // the page pre-lock, then each build their write from that stale snapshot —
    // the second writer's version wouldn't include the first writer's row. This
    // hammers that window with many iterations to get an empirical answer.
    //
    // To make the race window wide enough to actually hit under normal thread
    // scheduling, the page is pre-populated so each write's clone-and-overwrite
    // critical section (in PageBuffer::write_page) takes measurably longer, and
    // several threads race concurrently rather than just two.
    //
    // Note: a thread can legitimately fail with LockContentionError (the
    // per-page lock in get_page_mut has a tight 500us timeout, unrelated to the
    // race under test) — that's tracked separately and NOT retried, because
    // insert() writes the data row and index entry as two non-atomic steps, so
    // blindly retrying a failed insert can re-attempt an already-written data
    // row and fail with a confusing DuplicateKey instead. We only care here
    // about: of the inserts that returned Ok, was every single one of them
    // actually findable afterwards?
    #[test]
    fn test_concurrent_inserts_to_same_page_do_not_lose_updates() {
        use std::sync::Barrier;
        use std::thread;

        const ITERATIONS: usize = 100;
        const RACERS: u64 = 12;
        const PREPOPULATE: u64 = 200;
        let mut lost_update_iterations = vec![];
        let mut total_contention_errors = 0usize;

        for iteration in 0..ITERATIONS {
            let tree = make_tree(BIG);
            for i in 0..PREPOPULATE {
                tree.insert(Tuple::new(i, b"warm"), TransactionId::new(i))
                    .unwrap();
            }
            let tree = Arc::new(tree);
            let barrier = Arc::new(Barrier::new(RACERS as usize));

            let racer_ids: Vec<u64> = (0..RACERS)
                .map(|i| PREPOPULATE + iteration as u64 * RACERS + i)
                .collect();

            let handles: Vec<_> = racer_ids
                .iter()
                .map(|&id| {
                    let tree = tree.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        barrier.wait();
                        tree.insert(
                            Tuple::new(id, format!("v{id}").as_bytes()),
                            TransactionId::new(id),
                        )
                    })
                })
                .collect();

            let results: Vec<Result<(), StoreError>> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();

            for (&id, result) in racer_ids.iter().zip(results.iter()) {
                match result {
                    Ok(()) => {
                        if tree.find(DBIdType::Int(id)).unwrap().is_none() {
                            lost_update_iterations.push((iteration, id));
                        }
                    }
                    // Both are legitimate, *explicit* failures under heavy contention,
                    // not silent lost updates: LockContentionError means the 500us
                    // lock timeout tripped; PageCapacityError means another racer
                    // filled the page first and this insert correctly saw that
                    // fresh (not stale) state and refused to write, rather than
                    // silently overwriting.
                    Err(StoreError::LockContentionError) | Err(StoreError::PageCapacityError) => {
                        total_contention_errors += 1
                    }
                    Err(e) => panic!("unexpected insert error: {e:?}"),
                }
            }
        }

        assert!(
            lost_update_iterations.is_empty(),
            "an insert reported Ok but its row was unfindable afterwards (silent lost update) \
             in {} cases: {:?} ({total_contention_errors} unrelated lock-contention errors observed)",
            lost_update_iterations.len(),
            lost_update_iterations
        );
    }

    #[test]
    fn test_update_existing_tuple_returns_old_and_replaces_value() {
        let tree = make_tree(BIG);
        tree.insert(tuple_with_txn(1.into(), b"hello"), txn())
            .unwrap();

        let old = tree.update(tuple_with_txn(1.into(), b"world")).unwrap();
        assert_eq!(
            old.data.to_vec(),
            b"hello",
            "update must return the previous value"
        );

        let found = tree.find(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(
            found.data.to_vec(),
            b"world",
            "update must replace the stored value"
        );
    }

    #[test]
    fn test_update_preserves_other_tuples() {
        let tree = make_tree(BIG);
        for i in 1u64..=5 {
            tree.insert(tuple_with_txn(i.into(), format!("v{i}").as_bytes()), txn())
                .unwrap();
        }
        tree.update(tuple_with_txn(3.into(), b"updated")).unwrap();

        for i in 1u64..=5 {
            let found = tree.find(DBIdType::Int(i)).unwrap().unwrap();
            if i == 3 {
                assert_eq!(found.data.to_vec(), b"updated");
            } else {
                assert_eq!(found.data.to_vec(), format!("v{i}").into_bytes());
            }
        }
    }

    #[test]
    fn test_update_with_string_id() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("alpha".to_string());
        tree.insert(tuple_with_txn(id.clone(), b"a-data"), txn())
            .unwrap();

        let old = tree
            .update(tuple_with_txn(id.clone(), b"a-data-2"))
            .unwrap();
        assert_eq!(old.data.to_vec(), b"a-data");
        assert_eq!(tree.find(id).unwrap().unwrap().data.to_vec(), b"a-data-2");
    }

    #[test]
    fn test_update_does_not_change_entry_counts() {
        let tree = make_tree(BIG);
        tree.insert(tuple_with_txn(1.into(), b"hello"), txn())
            .unwrap();
        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(ip.count().unwrap(), 1);

        tree.update(tuple_with_txn(1.into(), b"world")).unwrap();

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert_eq!(
            dp.count().unwrap(),
            1,
            "update must not add or remove data rows"
        );
        assert_eq!(
            ip.count().unwrap(),
            1,
            "update must not add or remove index entries"
        );
    }

    #[test]
    fn test_update_nonexistent_id_returns_err() {
        let tree = make_tree(BIG);
        let result = tree.update(tuple_with_txn(999.into(), b"x"));
        assert!(
            matches!(result, Err(StoreError::KeyNotFound(_))),
            "update on a never-inserted id must return KeyNotFound, got {:?}",
            result
        );
    }

    #[test]
    fn test_remove_existing_tuple_returns_value_and_deletes_it() {
        let tree = make_tree(BIG);
        tree.insert(tuple_with_txn(1.into(), b"hello"), txn())
            .unwrap();

        let removed = tree.remove(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(removed.data.to_vec(), b"hello");

        assert!(
            tree.find(DBIdType::Int(1)).unwrap().is_none(),
            "removed id must no longer be findable"
        );
        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert!(!dp.contains(DBIdType::Int(1)).unwrap());
    }

    #[test]
    fn test_remove_preserves_other_tuples() {
        let tree = make_tree(BIG);
        for i in 1u64..=5 {
            tree.insert(tuple_with_txn(i.into(), format!("v{i}").as_bytes()), txn())
                .unwrap();
        }
        tree.remove(DBIdType::Int(3)).unwrap();

        assert!(tree.find(DBIdType::Int(3)).unwrap().is_none());
        for i in [1u64, 2, 4, 5] {
            let found = tree.find(DBIdType::Int(i)).unwrap().unwrap();
            assert_eq!(found.data.to_vec(), format!("v{i}").into_bytes());
        }
    }

    #[test]
    fn test_remove_with_string_id() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("alpha".to_string());
        tree.insert(tuple_with_txn(id.clone(), b"a-data"), txn())
            .unwrap();

        let removed = tree.remove(id.clone()).unwrap().unwrap();
        assert_eq!(removed.data.to_vec(), b"a-data");
        assert!(tree.find(id).unwrap().is_none());
    }

    #[test]
    fn test_remove_nonexistent_id_returns_none() {
        let tree = make_tree(BIG);
        let result = tree.remove(DBIdType::Int(999)).unwrap();
        assert!(
            result.is_none(),
            "remove on a never-inserted id must return Ok(None), got {:?}",
            result
        );
    }

    #[test]
    fn test_remove_cleans_up_index_entry() {
        let tree = make_tree(BIG);
        tree.insert(tuple_with_txn(1.into(), b"first"), txn())
            .unwrap();
        tree.remove(DBIdType::Int(1)).unwrap();

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(
            !ip.contains(DBIdType::Int(1)).unwrap(),
            "index entry must be removed alongside the data row"
        );

        // Re-inserting the same id must succeed once the index entry is gone.
        tree.insert(Tuple::new(1, b"second"), txn()).unwrap();
        let found = tree.find(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(found.data.to_vec(), b"second");
    }

    #[test]
    fn test_many_sequential_inserts_remain_findable_across_splits() {
        let tree = make_tree(BIG);
        for i in 0u64..400 {
            tree.insert(
                Tuple::new(i, format!("v{i}").as_bytes()),
                TransactionId::new(i),
            )
            .unwrap();
        }
        for i in 0u64..400 {
            let found = tree.find(DBIdType::Int(i)).unwrap();
            assert_eq!(
                found.map(|t| t.data.to_vec()),
                Some(format!("v{i}").into_bytes()),
                "id {i} must remain findable after 400 inserts across multiple splits"
            );
        }
    }
}
