use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    buffer::{PageBuffer, WritePageHandle},
    db::{DBFile, DBSizeType},
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
        let index_page = Page::new_indexed(pg, MAX_ENTRY_BYTES as usize);
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
        })
    }

    pub fn from_bytes(
        bytes: &[u8],
        buffer: Arc<PageBuffer<F>>,
        txn_mgr: Arc<TransactionManager>,
        logger: Arc<Logger>,
    ) -> Result<Self, StoreError> {
        let t: Table = from_bytes(bytes)?;
        Ok(Self {
            table: t,
            buffer,
            txn_mgr,
            logger,
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
        if let Err(orig) = &res {
            // Undo the data-page write. This MUST succeed — we just wrote the row
            // to this exact page and hold no belief that anyone else touched it.
            // Surface (don't swallow) any failure: a failing remove_tuple here
            // means the row isn't where we wrote it, which is itself the kind of
            // corruption we're hunting, and silently ignoring it would hide it.
            let mut h = self.buffer.get_page_mut(data_page_id)?;
            Arc::make_mut(&mut h.page)
                .remove_tuple(tuple_id.clone())
                .unwrap_or_else(|e| {
                    panic!(
                        "insert cleanup: remove_tuple({:?}) on page {} failed after insert_index err {:?}: {:?}",
                        tuple_id, data_page_id.0, orig, e
                    )
                });
            self.buffer.write_locked_page(h)?;
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
            self.split_root_page(handle, txn.clone())?;
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
    fn write_data(&self, tuple: &Tuple) -> Result<PageId, StoreError> {
        let mut data_page_id = self.table.first_data_page;
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

    pub fn remove(&self, id: DBIdType) -> Result<Tuple, StoreError> {
        let pid = self
            .find_page(id.clone(), self.table.first_index_page)?
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        let mut h = self.buffer.get_page_mut(pid)?;
        let old = Arc::make_mut(&mut h.page).remove_tuple(id.clone())?;
        self.buffer.write_locked_page(h)?;
        self.remove_index_entry(id, self.table.first_index_page, None)?;
        Ok(old)
    }

    fn find_page(&self, id: DBIdType, start: PageId) -> Result<Option<PageId>, StoreError> {
        let page = self.buffer.get_page(start)?;
        if page.is_flag_set(INNER_NODE) {
            let rows_iter = page.iter();
            let mut last_child: Option<PageId> = None;
            for row in rows_iter {
                if id < row.id {
                    let page_id = from_bytes::<Node>(&row.data)?;
                    if let Node::Inner(page_num) = page_id {
                        return Ok(self.find_page(id, page_num)?);
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
                return Ok(self.find_page(id, child)?);
            }
            return Ok(None);
        } else {
            Ok(page
                .get(id)?
                .map(|t| {
                    let id = from_bytes::<Node>(&t.data);
                    match id {
                        Ok(Node::Leaf(page_id)) => Some(Ok(page_id)),
                        Ok(Node::Inner(_)) => {
                            panic!("Expected leaf, found inner! {:?}", t.id)
                        }
                        Err(e) => Some(Err(StoreError::from(e))),
                    }
                })
                .flatten()
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
                    // unlocked pre-check in insert() and this locked arrival.
                    // We hold the lock here, so split it now and retry.
                    panic!("Root page split should not happen here");
                    /*                     self.split_root_page(handle, txn_id.clone())?;
                                       return self.insert_recursive(tuple, txn_id, start);
                    */
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
            let mut rows = handle.page.iter();
            let mut row_id = rows.next().unwrap();
            while tuple.id >= row_id.id {
                row_id = rows
                    .next()
                    .expect("inner node must have a row covering every key");
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
                Ok(Some(self.split_non_root_page(handle)?))
            }
        } else {
            Ok(None)
        }
    }

    fn is_root_page(&self, page_id: PageId) -> bool {
        self.table.first_index_page == page_id
    }

    fn split_non_root_page(
        &self,
        handle: WritePageHandle,
    ) -> Result<(DBIdType, PageId), StoreError> {
        let mut current_handle = handle;
        let values = current_handle.page.iter().collect::<Vec<_>>();

        // Split at the midpoint without discarding any entry: the right half
        // (including the entry at `mid`) keeps its full payload, and its first
        // entry's id doubles as the separator key for the parent.
        let mid = values.len() / 2;
        let current_vals = &values[..mid];
        let new_vals = &values[mid..];
        let separator_id = new_vals[0].id.clone();
        let is_inner = current_handle.page.is_flag_set(INNER_NODE);
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
    ) -> Result<(), StoreError> {
        // insert()'s caller decides whether to call this based on an
        // *unlocked* read of the root's count — by the time we actually hold
        // the lock (this `handle`), another thread may have already split the
        // root (it's now an inner node) or otherwise changed its count. Treat
        // that as "nothing to do" rather than blindly re-splitting, which
        // would corrupt the index (double-wrapping an already-split root).
        if handle.page.is_flag_set(INNER_NODE)
            || handle.page.count()? != self.table.nodes_per_page - 1
        {
            return Ok(());
        }
        let values = handle.page.iter().collect::<Vec<_>>();
        let flags = if handle.page.is_flag_set(INNER_NODE) {
            INNER_NODE
        } else {
            LEAF_NODE
        };

        // Split at the midpoint without discarding any entry (see split_non_root_page).
        let mid = values.len() / 2;
        let left_vals = &values[..mid];
        let right_vals = &values[mid..];
        let separator_id = right_vals[0].id.clone();
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
        Arc::new(from_bytes::<Header>(&v).unwrap())
    }

    fn make_buffer(page_size: u64) -> Arc<PageBuffer<MemFile>> {
        let header = make_header(page_size);
        let counter = Arc::new(AtomicU64::new(FIRST_USER_PAGE));
        Arc::new(PageBuffer::new(page_size, counter, MemFile::new(), header, 256).unwrap())
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

    // Large page — no splits during basic tests.
    const BIG: u64 = 8192;

    #[test]
    fn test_insert_single_data_and_index() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();

        let dp = tree.buffer.get_page(tree.table.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(dp.get(DBIdType::Int(1)).unwrap().unwrap().data, b"hello");

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
        assert!(!dp1.has_overflow(), "dp1 must NOT overflow — it chains instead");

        // dp1 links to dp2 via the normal data-chain pointer (no overflow).
        let dp2_id = tree.buffer.data_chain_next(&dp1, tree.table.first_data_page).unwrap();
        assert!(dp2_id.is_valid_next_page(), "dp1 must link to dp2");

        let dp2 = tree.buffer.get_page(dp2_id).unwrap();
        assert_eq!(dp2.count().unwrap(), 1, "dp2 holds the 3rd tuple");
        assert!(dp2.contains(DBIdType::Int(3)).unwrap());

        // All three remain findable through the index.
        for i in 1u64..=3 {
            assert_eq!(tree.find(DBIdType::Int(i)).unwrap().unwrap().data, large);
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
        assert_eq!(dp.get(id1.clone()).unwrap().unwrap().data, b"a-data");
        assert_eq!(dp.get(id2.clone()).unwrap().unwrap().data, b"b-data");

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
        assert_eq!(dp.get(DBIdType::Int(1)).unwrap().unwrap().data, b"first");
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
        assert_eq!(dp.get(id).unwrap().unwrap().data, b"first");
    }

    #[test]
    fn test_find_returns_inserted_tuple() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();
        let found = tree.find(DBIdType::Int(1)).unwrap();
        assert_eq!(found.unwrap().data, b"hello");
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
            assert_eq!(t.data, format!("val-{i}").into_bytes());
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
            assert_eq!(t.data, format!("v{id}").into_bytes());
        }
    }

    #[test]
    fn test_find_with_string_id() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("alpha".to_string());
        tree.insert(Tuple::new_with(id.clone(), b"a-data", None, None), txn())
            .unwrap();
        let found = tree.find(id).unwrap().unwrap();
        assert_eq!(found.data, b"a-data");
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
        assert_eq!(found.data, large);
        assert_eq!(tree.find(DBIdType::Int(1)).unwrap().unwrap().data, large);
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
                found.map(|t| t.data),
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
                found.map(|t| t.data),
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
        assert_eq!(old.data, b"hello", "update must return the previous value");

        let found = tree.find(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(found.data, b"world", "update must replace the stored value");
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
                assert_eq!(found.data, b"updated");
            } else {
                assert_eq!(found.data, format!("v{i}").into_bytes());
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
        assert_eq!(old.data, b"a-data");
        assert_eq!(tree.find(id).unwrap().unwrap().data, b"a-data-2");
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

        let removed = tree.remove(DBIdType::Int(1)).unwrap();
        assert_eq!(removed.data, b"hello");

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
            assert_eq!(found.data, format!("v{i}").into_bytes());
        }
    }

    #[test]
    fn test_remove_with_string_id() {
        let tree = make_tree(BIG);
        let id = DBIdType::from("alpha".to_string());
        tree.insert(tuple_with_txn(id.clone(), b"a-data"), txn())
            .unwrap();

        let removed = tree.remove(id.clone()).unwrap();
        assert_eq!(removed.data, b"a-data");
        assert!(tree.find(id).unwrap().is_none());
    }

    #[test]
    fn test_remove_nonexistent_id_returns_err() {
        let tree = make_tree(BIG);
        let result = tree.remove(DBIdType::Int(999));
        assert!(
            matches!(result, Err(StoreError::KeyNotFound(_))),
            "remove on a never-inserted id must return KeyNotFound, got {:?}",
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
        assert_eq!(found.data, b"second");
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
                found.map(|t| t.data),
                Some(format!("v{i}").into_bytes()),
                "id {i} must remain findable after 400 inserts across multiple splits"
            );
        }
    }
}
