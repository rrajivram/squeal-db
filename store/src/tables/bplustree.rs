use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};

use crate::{
    buffer::{LockLevel, PageBuffer, WritePageHandle},
    db::{DBFile, DBSizeType},
    error::StoreError,
    logger::{LsnId, Logger},
    page::{Page, PageId},
    table::{Table, TableIdType, TableType},
    tuple::{DBIdType, Tuple},
    txn::{TransactionId, TransactionManager},
};

// Hand-rolled codec, not derived: see table.rs's TableType for why. Node is
// embedded in on-disk B+tree routing entries, so its wire tag must never
// depend on Rust declaration order. Inner=0/Leaf=1 are fixed forever; a
// future variant picks an unused tag rather than reordering these.
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
enum Node {
    Inner(PageId),
    Leaf(PageId),
}

impl Serialize for Node {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Node::Inner(id) => (0u8, id).serialize(serializer),
            Node::Leaf(id) => (1u8, id).serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (tag, id): (u8, PageId) = Deserialize::deserialize(deserializer)?;
        match tag {
            0 => Ok(Node::Inner(id)),
            1 => Ok(Node::Leaf(id)),
            other => Err(DeError::custom(format!("unknown Node tag {other}"))),
        }
    }
}

/// What a `write_version` caller wants done with the row for a key, decided
/// under the leaf lock against what is currently there
/// (TXN_SIMPLIFICATION_PLAN.md phase 4).
pub(crate) enum Decision {
    /// A fresh row; the key must currently be absent.
    Insert(Tuple),
    /// Replace the current row with this version (same key).
    Replace(Tuple),
    /// Physically remove the row and its index entry.
    Delete,
    Skip,
}

/// What `write_version` did.
pub(crate) enum Written {
    Inserted(PageId),
    Replaced(Tuple),
    Deleted(Option<Tuple>),
    Skipped,
}

// split_if_needed's result — see its own comment on why the no-split case
// carries the already-held lock forward instead of dropping it.
enum SplitOutcome {
    Split(DBIdType, PageId),
    NoSplitNeeded(WritePageHandle),
}

// Bit indices 0–3 are reserved (PINNED, INDEX_PAGE, …); user flags start at 4.
// Phase 4: route writes with unlocked reads and lock only the leaf (see
// leaf_for_write); false forces the locked crabbing descent for every write.
const OPTIMISTIC_DESCENT: bool = true;

const INNER_NODE: usize = 4;
const LEAF_NODE: usize = 5;

// NOT used inside BPlusTree::new anymore — index_entry_size is now a real
// parameter callers must supply deliberately (see BPlusTree::new's own doc
// comment), not an assumption baked into the tree's own construction. This
// constant only sizes `Int`-keyed tables (`Db::create_table`'s convenience
// default) and is used freely by this file's own tests, per the estimate
// below. Anything else — in particular a composite `Rec(IndexKey)` key with
// `Str`/`Blob` fields (including a plain string id, which is now also
// `Rec`-backed — see `DBIdType`) — should compute its own bound instead of
// assuming this is enough; it easily isn't.
//
// Postcard varint upper bounds per field, for an Int-keyed entry:
//   DBIdType::Int(u64::MAX)  → 1 (variant) + 10 (varint) = 11 B
//   Option<TransactionId>    → 1 (None) — STORE_AUDIT.md P4: index/routing
//                              entries never carry a real txn_id anymore
//                              (see insert_index's own comment), so this is
//                              always the 1-byte None discriminant, not the
//                              21 B a live Some(TransactionId) used to cost.
//   Option<LsnId>            → 1 (None)
//   data Node::Inner(u64::MAX) as Vec<u8> → 1 (len) + 1 (variant) + 10 (varint) = 12 B
//   flags                    → 1 B
//   Total ≈ 26 B; 48 B gives comfortable headroom for any realistic payload
//   of *that* shape — not a general bound for every key shape. Directly
//   controls fanout: BPlusTree::new sets nodes_per_page = page_size /
//   index_entry_size, so shrinking this (64 → 48, following P4's fix)
//   proportionally raises nodes_per_page for the same page_size — e.g. a
//   33% fanout increase for any large, realistic page_size. Kept well above
//   the computed 26 B floor (not shrunk further to it) because several
//   bplustree.rs tests derive a deliberately tiny page_size as `MAX_ENTRY_
//   BYTES * nodes_per_page` with no separate PAGE_OVERHEAD term (PAGE_
//   OVERHEAD is a fixed ~112 B tax per page regardless of page_size) — at
//   nodes_per_page=4 that tax alone can swallow most of a page_size sized
//   too close to the true per-entry floor, confirmed empirically: 40 broke
//   6 of those tests (capacity/ordering failures at nodes_per_page=4), 48
//   passes all of them with real margin to spare.
pub(crate) const MAX_ENTRY_BYTES: u64 = 48;

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
    // STORE_AUDIT.md T14: find()'s "read the index, then read the data page
    // it points to" is two separate lock acquisitions, not one atomic step
    // — see find()'s and relocate_tuple's own comments. A concurrent
    // relocation landing entirely between those two reads could make
    // find() observe a page the row had *already* moved off of, reporting
    // a still-existing, committed row as missing. Retrying find()'s lookup
    // once against a fresh index read narrows the window a lot but can't
    // close it (a tight enough writer can still race two lookups in a
    // row) — confirmed empirically: retrying cut a ~1140-4067-per-20,000
    // failure rate to ~150-200, not zero. This lock makes the two truly
    // mutually exclusive instead: find() holds the read side across its
    // whole index-then-data lookup; relocate_tuple holds the write side
    // across its whole write-then-repoint-then-remove sequence. Global to
    // the table (not per-id) — coarser than necessary, but correctness,
    // not throughput, is Phase 1's goal here (see STORE_AUDIT.md's P-item
    // performance findings, deliberately deferred).
    relocation_lock: std::sync::RwLock<()>,
}

impl<F: DBFile> BPlusTree<F>
where
    F: DBFile<Item = F> + 'static,
{
    /// `index_entry_size` is the fixed per-entry byte budget for this
    /// table's index pages (`FixedTuplePage`-encoded) — every index entry
    /// for this table, for its whole lifetime, must serialize to no more
    /// than this many bytes. Callers must size this deliberately for their
    /// actual key shape rather than relying on a guessed default: a plain
    /// `Int` key comfortably fits the historical default (`MAX_ENTRY_BYTES`,
    /// still available for tests and as `Db::create_table`'s own convenience
    /// default), but a composite `Rec(IndexKey)` key with several
    /// `Str`/`Blob` fields — including a plain string id, which is also a
    /// `Rec` under the hood (see `DBIdType`) — can easily need more; see
    /// `IndexKey`/`ValueItem`'s own reserved-capacity fields for how to
    /// compute an exact upper bound for a given key shape. Passing too small
    /// a value doesn't fail here; it fails later, at insert time, with a
    /// clear `TupleTooLarge(actual, budget)`. This holds for the whole
    /// table's lifetime, not just before its first split:
    /// `alloc_sibling_index_page` carries this same budget forward onto
    /// every page a split creates, rather than letting it silently lapse
    /// into an unbounded page the way `PageBuffer::alloc_page` would.
    pub fn new(
        id: TableIdType,
        name: String,
        buffer: Arc<PageBuffer<F>>,
        txn_mgr: Arc<TransactionManager>,
        logger: Arc<Logger>,
        index_entry_size: DBSizeType,
    ) -> Result<Self, StoreError> {
        let pg = buffer.page_size();
        if index_entry_size == 0 {
            return Err(StoreError::UnknownError(
                "index_entry_size must be greater than 0".into(),
            ));
        }
        let count = pg / index_entry_size;

        if count < 2 {
            return Err(StoreError::UnknownError(format!(
                "Unable to fit index: page_size = {pg}, index_entry_size = {index_entry_size} \
                 gives count = {count}, need at least 2"
            )));
        }
        let first_index_page = buffer.alloc_page(false)?;
        let first_data_page = buffer.alloc_page(false)?;
        let index_page = Page::new_indexed(pg, index_entry_size as usize);
        // Adopt into this database's WAL clock before flagging (set_page_flags
        // dirties it, which stamps the lsn from the clock).
        index_page.set_page_flags(LEAF_NODE)?;
        let mut handle = buffer.get_page_mut(first_index_page, LockLevel::Index)?;
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
            relocation_lock: std::sync::RwLock::new(()),
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
            relocation_lock: std::sync::RwLock::new(()),
        })
    }

    pub fn id(&self) -> TableIdType {
        self.table.id
    }
    // Redo-replay counterpart to insert(): tolerates the row already being
    // there, since a committed Add's row may or may not have made it to
    // disk before a crash (that's the whole reason it needs replaying at
    // all). Used to compare `page.lsn_id()` (the page's own LSN) against
    // this record's LSN to decide "already applied, skip" vs "must
    // reapply" — but page.lsn_id() is stamped by set_dirty() from the
    // *flush watermark at dirty-time*, not this operation's own LSN, and
    // dirtying always happens before this operation's redo record is even
    // logged (see Db::insert: table.insert() writes the page, *then*
    // logger.log_redo() allocates the LSN) — so page.lsn_id() is
    // structurally always less than this record's own lsn, and the
    // "already applied" branch could never actually trigger. In practice
    // that meant insert_if_needed always fell into re-adding a row that
    // was already there, hitting DuplicateKey on the most ordinary
    // "commit, then reopen without a checkpoint in between" case.
    //
    // Existence is sufficient evidence on its own: redo records are
    // replayed strictly in their original log order, so the only way this
    // id could already be present when its own Add is processed is that
    // the very write it's replaying already landed (pre-crash or an
    // earlier pass of this same replay) — a fresh, unrelated row can't
    // occupy the same id without a Del in between, which would already
    // have removed it. No LSN comparison needed.
    // ------------------------------------------------------------------
    // TXN_SIMPLIFICATION_PLAN.md phase 4: THE write primitive. insert,
    // update, remove, the conditional reverts, recovery's redo, and vacuum's
    // purge are all one call each through `write_version`.
    // ------------------------------------------------------------------

    /// Recovery's redo for an `Add`: apply this exact version — insert if
    /// the key is absent, overwrite if a different occupant is there (a
    /// checkpointed tombstone whose purge was not logged, or a stale log
    /// older than the checkpoint), skip if it is already there.
    pub(crate) fn insert_if_needed(&self, tuple: &Tuple) -> Result<PageId, StoreError> {
        let t = tuple.clone();
        match self.write_version(tuple.id.clone(), self.logger.next_lsn(), |cur| {
            Ok(match cur {
                None => Decision::Insert(t),
                Some(c) if c.data == t.data && c.is_tombstoned() == t.is_tombstoned() => Decision::Skip,
                Some(_) => Decision::Replace(t),
            })
        })? {
            Written::Inserted(p) => Ok(p),
            _ => self
                .find_page(tuple.id.clone(), self.table.first_index_page)?
                .ok_or_else(|| StoreError::KeyNotFound(tuple.id.clone())),
        }
    }

    /// Plain insert (tests): a fresh row, DuplicateKey if present.
    pub fn insert(&self, tuple: Tuple, txn: TransactionId) -> Result<PageId, StoreError> {
        self.insert_at_lsn(tuple, txn, self.logger.next_lsn())
    }

    /// Insert under a caller-minted lsn (STORE_AUDIT.md T2: the pages this
    /// write touches are stamped with the same lsn the caller logs under).
    pub(crate) fn insert_at_lsn(
        &self,
        tuple: Tuple,
        _txn: TransactionId,
        lsn: LsnId,
    ) -> Result<PageId, StoreError> {
        let id = tuple.id.clone();
        match self.write_version(id.clone(), lsn, |cur| {
            Ok(match cur {
                None => Decision::Insert(tuple),
                Some(_) => return Err(StoreError::DuplicateKey(id.clone())),
            })
        })? {
            Written::Inserted(p) => Ok(p),
            _ => Err(StoreError::DuplicateKey(id)),
        }
    }

    /// The one write path. Descends to the leaf for `id` (splitting
    /// proactively on the way, so an insert always has room), holds that
    /// leaf from the lookup through the data-page write and the leaf entry
    /// change, and calls `decide` exactly once with the row currently under
    /// `id` (None if there is no entry, or an entry with no row behind it).
    /// Whatever `decide` does before returning — conflict checks, building
    /// the new version, recording it in the version store, appending to the
    /// WAL — happens under that lock, before any byte moves. Lock order:
    /// index pages top-down, then the leaf, then at most one data page at a
    /// time (proposal §3.8).
    pub(crate) fn write_version(
        &self,
        id: DBIdType,
        lsn: LsnId,
        decide: impl FnOnce(Option<&Tuple>) -> Result<Decision, StoreError>,
    ) -> Result<Written, StoreError> {
        // Phase 6: held for the whole write so a checkpoint's capture never
        // sees a split or a chain extension half done.
        let _writer = self.buffer.writer_permit();
        // A stand-in for the leaf entry this write may add, for capacity and
        // split decisions. Its data-page id is the next id that could be
        // allocated — an upper bound on any real one — so its size is
        // never smaller than the real entry's.
        let probe = Tuple::new_with(
            id.clone(),
            &to_allocvec(&Node::Leaf(PageId::from(self.buffer.page_count_val())))?,
            None,
            None,
        );
        let leaf = self.leaf_for_write(&probe, lsn)?;
        let entry = leaf.page.get(id.clone())?;
        let data_page = match &entry {
            Some(e) => match from_bytes::<Node>(&e.data)? {
                Node::Leaf(p) => Some(p),
                Node::Inner(_) => {
                    return Err(StoreError::Corruption(format!(
                        "expected a leaf entry, found an inner routing entry at {:?}",
                        id
                    )));
                }
            },
            None => None,
        };
        let current = match data_page {
            Some(dp) => self.buffer.get_page(dp)?.get(id.clone())?,
            None => None,
        };
        match decide(current.as_ref())? {
            Decision::Skip => Ok(Written::Skipped),
            Decision::Insert(new) => {
                if entry.is_some() {
                    return Err(StoreError::DuplicateKey(id));
                }
                // The leaf has room by count (proactive splits); check the
                // per-entry budget and aggregate bytes before any write.
                if let Some(max) = leaf.page.record_size()
                    && probe.size() as usize > max
                {
                    return Err(StoreError::TupleTooLarge(probe.size(), max));
                }
                if !leaf.page.can_store(&probe) {
                    return Err(StoreError::PageCapacityError);
                }
                let dp = self.write_data(&new, lsn)?;
                let entry = Tuple::new_with(id.clone(), &to_allocvec(&Node::Leaf(dp))?, None, None);
                if let Err(e) = leaf.page.add_tuple(entry) {
                    // Cannot happen after the checks above; if it does, the
                    // row must not be left orphaned on its data page.
                    let h = self.buffer.get_page_mut(dp, LockLevel::Data)?;
                    let _ = h.page.remove_tuple(id);
                    self.buffer.write_locked_page_with_lsn(h, lsn)?;
                    return Err(e);
                }
                self.buffer.write_locked_page_with_lsn(leaf, lsn)?;
                Ok(Written::Inserted(dp))
            }
            Decision::Replace(new) => {
                let (dp, cur) = match (data_page, current) {
                    (Some(dp), Some(cur)) => (dp, cur),
                    _ => return Err(StoreError::KeyNotFound(id)),
                };
                let h = self.buffer.get_page_mut(dp, LockLevel::Data)?;
                // An ordinary multi-tuple page must never exceed capacity via
                // an in-place replace (handle_large_page_size's overflow chain
                // is only for a single oversized tuple — see Page::can_store).
                let header = h.page.header();
                let fits_in_place = h.page.count()? <= 1
                    || header.used_size().saturating_sub(cur.size()) + new.size()
                        <= header.usable_data_size();
                if fits_in_place {
                    let old = h.page.replace_tuple(&id, new)?;
                    self.buffer.write_locked_page_with_lsn(h, lsn)?;
                    drop(leaf);
                    Ok(Written::Replaced(old))
                } else {
                    drop(h);
                    self.relocate(leaf, dp, id, new, lsn)?;
                    Ok(Written::Replaced(cur))
                }
            }
            Decision::Delete => {
                let Some(dp) = data_page else {
                    return Ok(Written::Deleted(None));
                };
                let h = self.buffer.get_page_mut(dp, LockLevel::Data)?;
                let old = match h.page.remove_tuple(id.clone()) {
                    Ok(t) => {
                        self.buffer.write_locked_page_with_lsn(h, lsn)?;
                        Some(t)
                    }
                    // Entry with no row behind it: just finish the index side.
                    Err(StoreError::KeyNotFound(_)) => None,
                    Err(e) => return Err(e),
                };
                leaf.page.remove_tuple(id)?;
                self.buffer.write_locked_page_with_lsn(leaf, lsn)?;
                Ok(Written::Deleted(old))
            }
        }
    }

    // The locked leaf for `probe.id`, with the root split pre-check and the
    // retry-from-root that a full inner node asks for (it can't take the
    // separator a child split needs; the level above splits it first on the
    // next pass).
    fn leaf_for_write(&self, probe: &Tuple, lsn: LsnId) -> Result<WritePageHandle, StoreError> {
        // Fast path (B-link): route with unlocked reads, lock only the leaf,
        // and revalidate under that lock — the page is still a leaf (a root
        // split turns the root leaf into an inner node) and still covers the
        // key (a split moves the upper part of a leaf's range to a linked
        // sibling and sets high_key; follow next_page like find does). If
        // the leaf has room, done: no ancestor was ever locked. Measured
        // (stress, 16 threads): locking the root on every write for the
        // crabbing descent below cost ~20% throughput.
        'optimistic: for _ in 0..(if OPTIMISTIC_DESCENT { 4 } else { 0 }) {
            let (leaf_id, _) = self.route_to_leaf_id(&probe.id, self.table.first_index_page)?;
            let mut handle = self.buffer.get_page_mut(leaf_id, LockLevel::Index)?;
            loop {
                if !handle.page.is_flag_set(LEAF_NODE) {
                    continue 'optimistic;
                }
                match handle.page.high_key() {
                    Some(hk) if probe.id >= hk => {
                        let next = handle.page.get_next_page();
                        if !next.is_valid_next_page() {
                            break;
                        }
                        drop(handle);
                        handle = self.buffer.get_page_mut(next, LockLevel::Index)?;
                    }
                    _ => break,
                }
            }
            if handle.page.count()? < self.table.nodes_per_page - 1
                && handle.page.can_store(probe)
            {
                return Ok(handle);
            }
            // Needs a split: only the locked top-down descent can do that.
            break;
        }
        let mut retries = 0u32;
        loop {
            let handle = self.buffer.get_page_mut(self.table.first_index_page, LockLevel::Index)?;
            if handle.page.count()? == self.table.nodes_per_page - 1 {
                self.split_root_page(handle, &probe.id, lsn)?;
            } else {
                drop(handle);
            }
            match self.descend_for_write(probe, self.table.first_index_page, None, lsn) {
                Err(StoreError::PageCapacityError) if retries < 16 => {
                    retries += 1;
                    std::thread::sleep(std::time::Duration::from_micros(50 * retries as u64));
                }
                other => return other,
            }
        }
    }

    // A row that no longer fits alongside its siblings moves to wherever
    // write_data lands. With the leaf held: write the new copy (other data
    // pages, one at a time), repoint the leaf entry, remove the old copy,
    // then publish the leaf. STORE_AUDIT.md T14: relocation_lock keeps
    // find()'s unlocked index-then-data read from landing astride this.
    fn relocate(
        &self,
        leaf: WritePageHandle,
        old_dp: PageId,
        id: DBIdType,
        new: Tuple,
        lsn: LsnId,
    ) -> Result<(), StoreError> {
        let _guard = self
            .relocation_lock
            .write()
            .map_err(|_| StoreError::UnknownError("relocation_lock poisoned".into()))?;
        let new_dp = match self.write_data(&new, lsn) {
            Ok(p) => p,
            // write_data landed back on old_dp (the old copy is still there):
            // remove the old copy first, then write.
            Err(StoreError::DuplicateKey(d)) if d == id => {
                let h = self.buffer.get_page_mut(old_dp, LockLevel::Data)?;
                h.page.remove_tuple(id.clone())?;
                self.buffer.write_locked_page_with_lsn(h, lsn)?;
                let p = self.write_data(&new, lsn)?;
                if p != old_dp {
                    let entry = Tuple::new_with(id.clone(), &to_allocvec(&Node::Leaf(p))?, None, None);
                    leaf.page.replace_tuple(&id, entry)?;
                }
                self.buffer.write_locked_page_with_lsn(leaf, lsn)?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let entry = Tuple::new_with(id.clone(), &to_allocvec(&Node::Leaf(new_dp))?, None, None);
        leaf.page.replace_tuple(&id, entry)?;
        let h = self.buffer.get_page_mut(old_dp, LockLevel::Data)?;
        h.page.remove_tuple(id)?;
        self.buffer.write_locked_page_with_lsn(h, lsn)?;
        self.buffer.write_locked_page_with_lsn(leaf, lsn)?;
        Ok(())
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

    fn write_data(&self, tuple: &Tuple, lsn: LsnId) -> Result<PageId, StoreError> {
        let mut data_page_id = PageId::from(self.last_data_page.load(Ordering::Relaxed));
        loop {
            let handle = self.buffer.get_page_mut(data_page_id, LockLevel::Data)?;
            if handle.page.can_store(tuple) {
                self.write_page(handle, tuple.clone(), lsn)?;
                return Ok(data_page_id);
            }
            let next = self.buffer.data_chain_next(&handle.page, data_page_id)?;
            if next.is_valid_next_page() {
                drop(handle);
                data_page_id = next;
                continue;
            }
            // Extending the chain. Two rules meet here: the tail's "no
            // successor" check and the link must happen under one hold of
            // the tail's lock (phase 0: two writers that both saw the same
            // tail and both linked orphaned a page full of rows), and no
            // page lock may be held across page allocation (phase 5:
            // alloc_page can block on the writer's channel under
            // backpressure). So: allocate with nothing held, re-lock the
            // tail, re-check, and either link or give the page back.
            drop(handle);
            let new_id = self.buffer.alloc_page(false)?;
            let handle = self.buffer.get_page_mut(data_page_id, LockLevel::Data)?;
            let next = self.buffer.data_chain_next(&handle.page, data_page_id)?;
            if next.is_valid_next_page() {
                drop(handle);
                self.buffer.free_page(new_id)?;
                data_page_id = next;
                continue;
            }
            self.buffer.set_data_chain_next(data_page_id, new_id)?;
            drop(handle);
            data_page_id = new_id;
            self.last_data_page.store(new_id.into(), Ordering::Relaxed);
        }
    }

    pub fn find(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        // STORE_AUDIT.md T14: reading the index (find_page) and then the
        // data page it points to are two separate lock acquisitions, not
        // one atomic step. Holding relocation_lock's read side across both
        // makes them atomic with respect to relocate_tuple (which holds
        // the write side across its whole write-then-repoint-then-remove
        // sequence): either this runs fully before a given relocation (and
        // sees the old page, which still has the row) or fully after (and
        // sees the new index entry and the new page) — never astride it,
        // which is what let a concurrent find() land on a page the row had
        // *already* moved off of and report a still-existing, committed
        // row as missing. A single retry-on-miss against a fresh index
        // read (an earlier attempt at this fix) narrowed the window a lot
        // but couldn't close it — confirmed empirically: it cut a
        // ~1140-4067-per-20,000 failure rate to ~150-200, not zero.
        let _guard = self
            .relocation_lock
            .read()
            .map_err(|_| StoreError::UnknownError("relocation_lock poisoned".into()))?;
        let Some(page_id) = self.find_page(id.clone(), self.table.first_index_page)? else {
            return Ok(None);
        };
        self.buffer.get_page(page_id)?.get(id)
    }

    /// Replace the row for `tuple.id` with `tuple` (tests and recovery).
    pub fn update(&self, tuple: Tuple) -> Result<Tuple, StoreError> {
        let id = tuple.id.clone();
        match self.write_version(id.clone(), self.logger.next_lsn(), |cur| {
            Ok(match cur {
                Some(_) => Decision::Replace(tuple),
                None => return Err(StoreError::KeyNotFound(id.clone())),
            })
        })? {
            Written::Replaced(old) => Ok(old),
            _ => Err(StoreError::KeyNotFound(id)),
        }
    }

    // Redo-replay counterpart to update(): tolerates the row already
    // reflecting this exact version (same bytes AND same tombstone state —
    // a delete's redo changes only the flag), and a missing row (a log
    // older than the checkpoint: the data file is ahead of this record).
    pub(crate) fn update_if_needed(&self, tuple: Tuple) -> Result<(), StoreError> {
        self.write_version(tuple.id.clone(), self.logger.next_lsn(), |cur| {
            Ok(match cur {
                Some(c) if c.data == tuple.data && c.is_tombstoned() == tuple.is_tombstoned() => {
                    Decision::Skip
                }
                Some(_) => Decision::Replace(tuple),
                None => Decision::Skip,
            })
        })?;
        Ok(())
    }

    /// Physical removal of the row and its index entry, both under the
    /// leaf lock. Ok(None) when the data was already gone (a dangling index
    /// entry is still removed) or the key is unknown.
    pub fn remove(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        match self.write_version(id, self.logger.next_lsn(), |_| Ok(Decision::Delete))? {
            Written::Deleted(old) => Ok(old),
            _ => Ok(None),
        }
    }

    /// Conditional `update` for abort-revert: replaces the row only if it
    /// still belongs to `expect_txn`. Ok(None) when the row is gone or has
    /// been taken over by another transaction (the revert is correctly a
    /// no-op then).
    pub(crate) fn update_if_txn(
        &self,
        tuple: Tuple,
        expect_txn: &TransactionId,
    ) -> Result<Option<Tuple>, StoreError> {
        let expect = *expect_txn;
        match self.write_version(tuple.id.clone(), self.logger.next_lsn(), |cur| {
            Ok(match cur {
                Some(c) if c.is_same_txn(expect) => Decision::Replace(tuple),
                _ => Decision::Skip,
            })
        })? {
            Written::Replaced(old) => Ok(Some(old)),
            _ => Ok(None),
        }
    }

    // `page_id` is the CURRENT page's own id, needed (not derivable from
    // `page` alone — Page doesn't carry its own id) to resolve the actual
    // next DATA page rather than a raw `next_page` read: when `page` holds
    // an oversized tuple, `next_page` points at its first overflow
    // continuation page instead (see PageBuffer::data_chain_next's own
    // doc comment), and decoding a continuation page's raw byte chunk as
    // a standalone tuple page is exactly the corruption this used to hit
    // — content-dependent, so it looked like a nondeterministic race
    // rather than the deterministic bug it actually was.
    pub(crate) fn next_data_page(
        &self,
        current: Option<(PageId, Arc<Page>)>,
    ) -> Result<Option<(PageId, Arc<Page>)>, StoreError> {
        match current {
            Some((page_id, page)) => {
                let next = self.buffer.data_chain_next(&page, page_id)?;
                if next.is_valid_next_page() {
                    Ok(Some((next, self.buffer.get_page(next)?)))
                } else {
                    Ok(None)
                }
            }
            None => {
                let first = self.table.first_data_page;
                Ok(Some((first, self.buffer.get_page(first)?)))
            }
        }
    }

    /// Conditional physical removal for abort-revert of an insert: only if
    /// the row still belongs to `expect_txn` (or the index entry is
    /// dangling with no row behind it). Returns Ok(None) when there was
    /// nothing to do.
    pub(crate) fn remove_if_txn(
        &self,
        id: DBIdType,
        expect_txn: &TransactionId,
    ) -> Result<Option<Tuple>, StoreError> {
        let expect = *expect_txn;
        match self.write_version(id, self.logger.next_lsn(), |cur| {
            Ok(match cur {
                Some(c) if c.is_same_txn(expect) => Decision::Delete,
                Some(_) => Decision::Skip,
                None => Decision::Delete,
            })
        })? {
            Written::Deleted(old) => Ok(old),
            _ => Ok(None),
        }
    }

    // Routes through inner nodes to find the LEAF index page that contains
    // (or, if `id` isn't an existing key, would contain) `id` — shared by
    // find_page (which then does an exact lookup within that leaf and
    // resolves its Node::Leaf pointer to a data page) and find_leaf_page
    // (which returns the leaf itself, for a range scan's positional
    // "first key >= id" starting point — no exact match required).
    //
    // B-link tree traversal: at every page visited (inner or leaf), before
    // trusting its own entries/fallthrough, check whether `id` still falls
    // within its high_key. A concurrent split can move part of a page's
    // range to a new right sibling after some ancestor already decided to
    // route here — without this check, that leaves a lookup landing on a
    // now-too-narrow page and missing a key that genuinely exists (silent
    // KeyNotFound), or panicking on a routing invariant a split briefly
    // invalidated. If `id >= high_key`, follow next_page and re-check there
    // instead of using this page's own entries — repeating as needed
    // through however many splits raced with this lookup.
    //
    // Returns the leaf's already-fetched Arc<Page> directly, not just its
    // PageId: a caller re-fetching by id afterward would reopen the exact
    // same window (a split landing between this function's own read and
    // the caller's separate one), defeating the whole point of the check
    // above. Returning the same reference the check already validated
    // closes that gap entirely.
    fn route_to_leaf(&self, id: &DBIdType, start: PageId) -> Result<Arc<Page>, StoreError> {
        self.route_to_leaf_id(id, start).map(|(_, page)| page)
    }

    // route_to_leaf, also returning the leaf's own id — what an optimistic
    // writer needs in order to lock the leaf it was routed to.
    fn route_to_leaf_id(
        &self,
        id: &DBIdType,
        start: PageId,
    ) -> Result<(PageId, Arc<Page>), StoreError> {
        let mut current = start;
        loop {
            let page = self.buffer.get_page(current)?;
            if let Some(high_key) = page.high_key()
                && *id >= high_key
            {
                let next = page.get_next_page();
                if next.is_valid_next_page() {
                    current = next;
                    continue;
                }
                // No sibling despite a bound that says there should be one —
                // structurally shouldn't happen (splits always wire up
                // next_page and high_key together). Fall through and use
                // this page's own entries as a last resort rather than
                // getting stuck.
            }
            if !page.is_flag_set(INNER_NODE) {
                return Ok((current, page));
            }
            // STORE_AUDIT.md P5: successor(id) is the first entry whose key
            // exceeds id — an O(log N) B-tree range lookup replacing the old
            // O(N) clone-every-tuple-then-linear-decode-scan (`page.iter()`
            // → `values()`). Falls back to last() (still O(log N), no
            // clone) when id >= every entry, matching the old loop's
            // last_child fallthrough: non-root inner nodes have no
            // u64::MAX sentinel, so the last child covers everything from
            // its own separator up to the parent's upper bound.
            let entry = match page.successor(id)? {
                Some(entry) => entry,
                None => match page.last()? {
                    Some(entry) => entry,
                    // An INNER_NODE page always has at least one Node::Inner
                    // entry (routing pages are never created empty) — this
                    // is here so the match is exhaustive, not because it's
                    // expected to happen.
                    None => return Ok((current, page)),
                },
            };
            current = match from_bytes::<Node>(&entry.data)? {
                Node::Inner(page_num) => page_num,
                // STORE_AUDIT.md S8: this page's own INNER_NODE flag
                // disagrees with this entry's actual content — every real
                // write path keeps these in sync, so this can only mean a
                // corrupted or hand-crafted on-disk file. A panic here
                // would be a process crash for the host; surface it as a
                // typed error.
                Node::Leaf(_) => {
                    return Err(StoreError::Corruption(format!(
                        "expected an inner routing entry, found a leaf entry at {:?}",
                        entry.id
                    )));
                }
            };
        }
    }

    // Positional lookup for RangeCursor: the leaf that would hold `id`,
    // whether or not `id` actually exists there. Unlike find_page/find,
    // this never requires an exact match — the caller (a range scan) wants
    // "start here and walk forward via the leaf chain," not "this exact
    // key must exist."
    pub(crate) fn find_leaf_page(&self, id: &DBIdType) -> Result<Arc<Page>, StoreError> {
        self.route_to_leaf(id, self.table.first_index_page)
    }

    // Given a leaf index page, returns its sibling in the leaf chain (see
    // split_non_root_page/split_root_page, which wire up next_page on
    // every leaf split), or None once the walk reaches the last leaf.
    // Mirrors next_data_page's pattern, but for index leaves rather than
    // data pages — leaves never have overflow, so (unlike
    // PageBuffer::data_chain_next) a plain get_next_page() is enough.
    pub(crate) fn next_leaf_page(&self, page: &Page) -> Result<Option<Arc<Page>>, StoreError> {
        let next = page.get_next_page();
        if next.is_valid_next_page() {
            Ok(Some(self.buffer.get_page(next)?))
        } else {
            Ok(None)
        }
    }

    // Every index page this table's tree currently owns (root, every inner
    // node, every leaf) — used by Db::drop_table to know exactly which
    // pages to free. A full structural traversal, not a walk of the leaf
    // sibling chain: inner nodes aren't reachable that way at all (that
    // chain only links leaves; an inner node is only reachable via its
    // parent's own Node::Inner routing entry), so visiting every page
    // means following those entries recursively from the root, same as
    // route_to_leaf's descent but branching into every child instead of
    // just the one a specific key would route to.
    /// TXN_SIMPLIFICATION_PLAN.md phase 0: a human-readable structural dump —
    /// every index leaf's (key -> data page) entries in chain order, then every
    /// data page in chain order with the raw tuples it holds (key, writer
    /// txn, tombstone flag). For diagnosing "find sees it, scan doesn't"
    /// class disagreements without a debugger.
    /// Every page this table owns: the index (root down through inner
    /// children and along leaf/inner B-links) and the data chain. Used by
    /// tests to prove page accounting closes (no orphans, no leaks).
    #[cfg(test)]
    pub(crate) fn reachable_pages(&self) -> Result<std::collections::HashSet<PageId>, StoreError> {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![self.table.first_index_page];
        while let Some(pid) = stack.pop() {
            if !seen.insert(pid) {
                continue;
            }
            let page = self.buffer.get_page(pid)?;
            if page.is_flag_set(INNER_NODE) {
                for t in page.iter() {
                    if let Ok(Node::Inner(child)) = from_bytes::<Node>(&t.data) {
                        stack.push(child);
                    }
                }
            }
            let next = page.get_next_page();
            if next.is_valid_next_page() {
                stack.push(next);
            }
        }
        let mut pid = self.table.first_data_page;
        loop {
            seen.insert(pid);
            let page = self.buffer.get_page(pid)?;
            let next = self.buffer.data_chain_next(&page, pid)?;
            if !next.is_valid_next_page() {
                break;
            }
            pid = next;
        }
        Ok(seen)
    }

    pub(crate) fn debug_dump(&self) -> Result<Vec<String>, StoreError> {
        let mut out = vec![format!(
            "table {} (id {}): first_index_page={:?} first_data_page={:?} last_data_page={}",
            self.table.name,
            self.table.id,
            self.table.first_index_page,
            self.table.first_data_page,
            self.last_data_page.load(Ordering::Relaxed)
        )];
        // Leaves in chain order, starting from the leftmost leaf.
        let mut leaf = {
            let mut cur = self.buffer.get_page(self.table.first_index_page)?;
            let mut cur_id = self.table.first_index_page;
            while cur.is_flag_set(INNER_NODE) {
                let first = cur.iter().next();
                match first.map(|t| from_bytes::<Node>(&t.data)) {
                    Some(Ok(Node::Inner(child))) => {
                        cur_id = child;
                        cur = self.buffer.get_page(child)?;
                    }
                    _ => break,
                }
            }
            Some((cur_id, cur))
        };
        while let Some((pid, page)) = leaf {
            let entries: Vec<String> = page
                .iter()
                .map(|t| match from_bytes::<Node>(&t.data) {
                    Ok(Node::Leaf(dp)) => format!("{}->{:?}", t.id, dp),
                    Ok(Node::Inner(ip)) => format!("{}->INNER{:?}", t.id, ip),
                    Err(_) => format!("{}->?", t.id),
                })
                .collect();
            out.push(format!(
                "  leaf {:?} next={:?} high_key={:?}: {}",
                pid,
                page.get_next_page(),
                page.high_key(),
                entries.join(" ")
            ));
            leaf = self
                .next_leaf_page(&page)?
                .map(|n| (page.get_next_page(), n));
        }
        // Data pages in chain order.
        let mut cur = self.table.first_data_page;
        let mut guard = 0;
        loop {
            let page = self.buffer.get_page(cur)?;
            let rows: Vec<String> = page
                .iter()
                .map(|t| {
                    format!(
                        "{}(txn={} {}{})",
                        t.id,
                        t.txn_id.as_ref().map(|x| x.id_num()).unwrap_or(0),
                        if t.is_tombstoned() { "TOMB " } else { "" },
                        String::from_utf8_lossy(&t.data)
                    )
                })
                .collect();
            out.push(format!(
                "  data {:?} next={:?} overflow={} : {}",
                cur,
                page.get_next_page(),
                page.has_overflow(),
                rows.join(" ")
            ));
            let next = self.buffer.data_chain_next(&page, cur)?;
            if !next.is_valid_next_page() {
                break;
            }
            cur = next;
            guard += 1;
            if guard > 100_000 {
                out.push("  data chain: cycle?".into());
                break;
            }
        }
        Ok(out)
    }

    pub(crate) fn all_index_page_ids(&self) -> Result<Vec<PageId>, StoreError> {
        let mut out = vec![];
        let mut stack = vec![self.table.first_index_page];
        while let Some(pid) = stack.pop() {
            out.push(pid);
            let page = self.buffer.get_page(pid)?;
            if page.is_flag_set(INNER_NODE) {
                for row in page.iter() {
                    if let Node::Inner(child) = from_bytes::<Node>(&row.data)? {
                        stack.push(child);
                    }
                }
            }
        }
        Ok(out)
    }

    // Resolves an index leaf entry (a Tuple whose `.data` is a serialized
    // Node::Leaf pointer, not real row content — see insert_index) to the
    // actual row it points to.
    pub(crate) fn resolve_index_entry(&self, entry: &Tuple) -> Result<Option<Tuple>, StoreError> {
        match from_bytes::<Node>(&entry.data)? {
            Node::Leaf(data_page_id) => self.buffer.get_page(data_page_id)?.get(entry.id.clone()),
            // STORE_AUDIT.md S8: see route_to_leaf's identical comment —
            // a leaf page holding an inner routing entry instead of a
            // real leaf pointer is corrupted on-disk data, not a
            // reachable outcome of any real write path.
            Node::Inner(_) => Err(StoreError::Corruption(format!(
                "expected a leaf entry, found an inner routing entry at {:?}",
                entry.id
            ))),
        }
    }

    #[allow(clippy::bind_instead_of_map)]
    fn find_page(&self, id: DBIdType, start: PageId) -> Result<Option<PageId>, StoreError> {
        let page = self.route_to_leaf(&id, start)?;
        page.get(id)?
            .and_then(|t| {
                let id = from_bytes::<Node>(&t.data);
                match id {
                    Ok(Node::Leaf(page_id)) => Some(Ok(page_id)),
                    // STORE_AUDIT.md S8: see route_to_leaf's identical
                    // comment on why this is corrupted data, not a
                    // reachable outcome of any real write path.
                    Ok(Node::Inner(_)) => Some(Err(StoreError::Corruption(format!(
                        "expected a leaf entry, found an inner routing entry at {:?}",
                        t.id
                    )))),
                    Err(e) => Some(Err(StoreError::from(e))),
                }
            })
            .transpose()
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 4: the top-down, hand-over-hand
    // descent that every write goes through (write_version), ending with the
    // leaf that covers `probe.id` LOCKED and — because inner nodes and
    // children are split proactively on the way down — guaranteed to have
    // room for one more entry of `probe`'s size. Returning the held lock is
    // the whole point: the caller does its data-page write and its leaf
    // entry change under this one lock, so there is no window for a
    // duplicate to slip in and no cleanup path for a half-done insert.
    fn descend_for_write(
        &self,
        probe: &Tuple,
        start: PageId,
        parent: Option<WritePageHandle>,
        lsn: LsnId,
    ) -> Result<WritePageHandle, StoreError> {
        let mut handle = self.buffer.get_page_mut(start, LockLevel::Index)?;
        // Crabbing: now that we hold this node's lock, release the parent's.
        drop(parent);
        if handle.page.is_flag_set(LEAF_NODE) {
            let count = handle.page.count()?;
            if count == self.table.nodes_per_page - 1 {
                if self.is_root_page(start) {
                    // Root leaf filled between the unlocked pre-check in
                    // leaf_for_write and this locked arrival: split it now
                    // (split_root_page re-checks under this lock) and retry.
                    self.split_root_page(handle, &probe.id, lsn)?;
                    return self.descend_for_write(probe, start, None, lsn);
                }
                // A non-root leaf full on arrival. Under the pure crabbing
                // descent this could not happen (split_if_needed carried the
                // child's lock forward). With the optimistic fast path it can,
                // legitimately: a split publishes its new sibling — reachable
                // through the old leaf's B-link `next`/high_key — and
                // releases the sibling's lock before the splitter descends
                // into it, so fast-path writers can fill the sibling in that
                // window. Retry from the root: the parent now knows the
                // sibling and split_if_needed splits it on the way down.
                return Err(StoreError::PageCapacityError);
            }
            return Ok(handle);
        }
        if !handle.page.is_flag_set(INNER_NODE) {
            // STORE_AUDIT.md S8: neither flag — corrupted or hand-crafted.
            return Err(StoreError::Corruption(format!(
                "page {:?} is flagged neither a leaf nor an inner node",
                start
            )));
        }
        if handle.page.count()? == 0 {
            return Err(StoreError::Corruption(format!(
                "inner page {:?} has no routing entries",
                handle.page_num
            )));
        }
        // A full inner node can't take the separator a child split would
        // add: back out so leaf_for_write retries from the root, where the
        // level above will split this node first (split_if_needed).
        if handle.page.count()? == self.table.nodes_per_page - 1 {
            return Err(StoreError::PageCapacityError);
        }
        // STORE_AUDIT.md P5: successor(id), falling back to last() when the
        // key is >= every separator (non-root inner nodes have no sentinel).
        let row_id = match handle.page.successor(&probe.id)? {
            Some(row) => row,
            None => handle
                .page
                .last()?
                .expect("INNER_NODE page must have at least one entry"),
        };
        let Node::Inner(p) = from_bytes::<Node>(&row_id.data)? else {
            return Err(StoreError::Corruption(format!(
                "expected an inner routing entry, found a leaf entry at {:?}",
                start
            )));
        };
        match self.split_if_needed(p, probe, lsn)? {
            SplitOutcome::Split(separator, sibling) => {
                let page = Arc::make_mut(&mut handle.page);
                // `p` kept the smaller half; the larger half moved to
                // `sibling`, which the existing entry must now route to.
                page.replace_tuple(
                    &row_id.id,
                    Tuple::new_with(
                        row_id.id.clone(),
                        &to_allocvec(&Node::Inner(sibling))?,
                        None,
                        None,
                    ),
                )?;
                page.add_tuple(Tuple::new_with(
                    separator.clone(),
                    &to_allocvec(&Node::Inner(p))?,
                    None,
                    None,
                ))?;
                // Crab into the destination child before the parent is
                // written/released, so no other thread can re-split it out
                // from under this routing decision.
                let target = if probe.id < separator { p } else { sibling };
                let child = self.buffer.get_page_mut(target, LockLevel::Index)?;
                self.buffer.write_locked_page_with_lsn(handle, lsn)?;
                self.descend_for_write(probe, target, Some(child), lsn)
            }
            SplitOutcome::NoSplitNeeded(child) => {
                drop(handle);
                self.descend_for_write(probe, p, Some(child), lsn)
            }
        }
    }

    fn split_if_needed(
        &self,
        page_id: PageId,
        tuple: &Tuple,
        lsn: LsnId,
    ) -> Result<SplitOutcome, StoreError> {
        let handle = self.buffer.get_page_mut(page_id, LockLevel::Index)?;
        if handle.page.count()? == self.table.nodes_per_page - 1 || !handle.page.can_store(tuple) {
            if self.is_root_page(page_id) {
                // STORE_AUDIT.md S8: a caller-discipline invariant (the
                // root is always split via split_root_page, from
                // insert_recursive, before ever reaching here) — not
                // corrupted on-disk data. See the non-root-leaf-at-
                // capacity comment in insert_recursive for the same
                // reasoning on why this is still a typed error, not a
                // panic.
                Err(StoreError::UnknownError(
                    "Trying to split root in the wrong place".into(),
                ))
            } else {
                let (separator, sibling) = self.split_non_root_page(handle, &tuple.id, lsn)?;
                Ok(SplitOutcome::Split(separator, sibling))
            }
        } else {
            // Not full: return the already-held lock instead of dropping it
            // here. The caller (insert_recursive) used to let this handle
            // go out of scope and re-acquire `page_id`'s lock fresh, later,
            // via its own get_page_mut(start) — leaving a real window where
            // nothing holds this page's lock at all, during which a
            // concurrent insert targeting the same (non-root) page could
            // fill it to exactly its capacity threshold. When that
            // happened, insert_recursive's leaf-handling branch had no way
            // to recover: splitting a non-root node needs to rewrite the
            // *parent's* routing entry, and by the time this thread was
            // back inside the child with nothing but the child's own lock,
            // the parent context (and lock) was already gone — hence the
            // `panic!("count == nodes- should not happen")` this used to
            // hit, confirmed reachable under concurrent load (~1/300 in a
            // targeted stress run). Carrying the lock forward instead of
            // dropping it means the capacity check and the actual insert
            // happen under one continuous hold — no other thread can ever
            // observe this page as "not full" and then find it full by the
            // time it gets a turn, because the turn never comes until this
            // one is done with it.
            Ok(SplitOutcome::NoSplitNeeded(handle))
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

    // Splits must carry a fixed-record (index) page's per-entry budget
    // forward instead of silently dropping to a variable-size AnyTuplePage
    // (see BPlusTree::new's doc comment) — otherwise the TupleTooLarge
    // protection in Page::add_tuple only ever covers a table before its
    // first split. `sibling_of` is always the page actually being split
    // (never yet mutated), so its own record_size is the exact same budget
    // this table's index has used from the start; falls back to a plain
    // AnyTuplePage only if that page has already lost its own record_size —
    // on-disk data written before this fix existed. Every split from here on
    // keeps propagating a real record_size forward, since the root is
    // always FixedTuplePage-backed (BPlusTree::new) and every subsequent
    // split's `sibling_of` was itself created by this same function.
    fn alloc_sibling_index_page(&self, sibling_of: &Page) -> Result<PageId, StoreError> {
        match sibling_of.record_size() {
            Some(record_size) => self.buffer.alloc_indexed_page(record_size),
            None => self.buffer.alloc_page(false),
        }
    }

    fn split_non_root_page(
        &self,
        handle: WritePageHandle,
        incoming_id: &DBIdType,
        lsn: LsnId,
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
        let new_page_id = self.alloc_sibling_index_page(&current_handle.page)?;
        let mut new_handle = self.buffer.get_page_mut(new_page_id, LockLevel::Index)?;

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
        // B-link tree sibling chain + high key, maintained the same way for
        // both leaf and inner splits (this used to be leaf-only — see
        // route_to_leaf's own comment on why that left inner-node splits
        // racy against concurrent lookups). Captured before either page's
        // own fields are overwritten below.
        //
        // separator_id is exactly the right new high_key for current_page
        // in both cases: for a leaf it's new_vals[0].id, the first key now
        // exclusively in new_page; for an inner node it's
        // current_vals.last().id — the same value already established
        // above as the true external bound of current_page's own routing
        // (see this function's earlier comment on why that's the correct
        // separator, not new_vals[0].id). Either way, "current_page's
        // fallthrough/entries are only trustworthy below separator_id, must
        // follow next_page beyond that" is exactly what a high key means.
        // new_page inherits whatever bound current_page had before this
        // split — it now owns everything from separator_id up to that.
        let old_next_page = current_page.get_next_page();
        let old_high_key = current_page.high_key();
        new_page.set_next_page(old_next_page)?;
        current_page.set_next_page(new_page_id)?;
        new_page.set_high_key(old_high_key)?;
        current_page.set_high_key(Some(separator_id.clone()))?;
        current_page.clear()?;
        current_vals
            .iter()
            .try_for_each(|t| current_page.add_tuple(t.clone()))?;
        new_vals
            .iter()
            .try_for_each(|t| new_page.add_tuple(t.clone()))?;
        self.buffer.write_locked_page_with_lsn(current_handle, lsn)?;
        self.buffer.write_locked_page_with_lsn(new_handle, lsn)?;
        Ok((separator_id, new_page_id))
    }

    fn update_root_page(
        &self,
        id: DBIdType,
        left_page: PageId,
        right_page: PageId,
        lsn: LsnId,
    ) -> Result<(), StoreError> {
        let mut handle = self.buffer.get_page_mut(self.table.first_index_page, LockLevel::Index)?;
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
        // STORE_AUDIT.md P4: see insert_index's identical comment.
        let new_t = Tuple::new_with(id.clone(), &to_allocvec(&left_node)?, None, None);
        let end_t = Tuple::new_with(
            DBIdType::Int(DBSizeType::MAX),
            &to_allocvec(&right_node)?,
            None,
            None,
        );
        page.add_tuple(new_t)?;
        page.add_tuple(end_t)?;
        self.buffer.write_locked_page_with_lsn(handle, lsn)?;
        Ok(())
    }

    fn split_root_page(
        &self,
        handle: WritePageHandle,
        incoming_id: &DBIdType,
        lsn: LsnId,
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
        let left_page_id = self.alloc_sibling_index_page(&handle.page)?;
        let right_page_id = self.alloc_sibling_index_page(&handle.page)?;
        let mut left_handle = self.buffer.get_page_mut(left_page_id, LockLevel::Index)?;
        let mut right_handle = self.buffer.get_page_mut(right_page_id, LockLevel::Index)?;
        // Flags on the COW copy, not the shared cached Arc (see update_root_page).
        let left_page = Arc::make_mut(&mut left_handle.page);
        let right_page = Arc::make_mut(&mut right_handle.page);
        left_page.set_page_flags(flags)?;
        right_page.set_page_flags(flags)?;
        // See split_non_root_page's matching comment: maintain the B-link
        // sibling chain and high key for both leaf and inner roots. The old
        // root (about to become an inner routing page via update_root_page)
        // was itself the only page at its level so far, so right inherits
        // whatever it pointed to / was bounded by (normally nothing/None,
        // but preserved for consistency regardless) and left now points at
        // right, bounded by separator_id.
        right_page.set_next_page(handle.page.get_next_page())?;
        left_page.set_next_page(right_page_id)?;
        right_page.set_high_key(handle.page.high_key())?;
        left_page.set_high_key(Some(separator_id.clone()))?;
        left_vals
            .iter()
            .try_for_each(|t| left_page.add_tuple(t.clone()))?;
        right_vals
            .iter()
            .try_for_each(|t| right_page.add_tuple(t.clone()))?;
        self.buffer.write_locked_page_with_lsn(left_handle, lsn)?;
        self.buffer.write_locked_page_with_lsn(right_handle, lsn)?;
        self.update_root_page(separator_id, left_page_id, right_page_id, lsn)?;
        Ok(())
    }

    fn write_page(&self, handle: WritePageHandle, tuple: Tuple, lsn: LsnId) -> Result<(), StoreError> {
        handle.page.add_tuple(tuple)?;
        self.buffer.write_locked_page_with_lsn(handle, lsn)?;
        Ok(())
    }
}

impl Eq for Node {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicU64};

    use postcard::{from_bytes, to_allocvec};

    use super::{BPlusTree, INNER_NODE, LEAF_NODE, MAX_ENTRY_BYTES, Node};
    use crate::{
        buffer::PageBuffer,
        constant::FIRST_USER_PAGE,
        db::Header,
        error::StoreError,
        logger::Logger,
        memfile::MemFile,
        page::{Page, PageId},
        tuple::{DBIdType, Tuple},
        txn::{TransactionId, TransactionManager},
        valueitem::{IndexKey, ValueItem},
    };

    #[test]
    fn test_node_round_trip() {
        for v in [Node::Inner(PageId::from(3u64)), Node::Leaf(PageId::from(9u64))] {
            let bytes = to_allocvec(&v).unwrap();
            let back: Node = from_bytes(&bytes).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn test_node_unknown_tag_errors() {
        assert!(from_bytes::<Node>(&[99, 3]).is_err());
    }

    // Fixture captured from the pre-Stage-1 `#[derive(Serialize,
    // Deserialize)]` encoding (commit bfbc240), before Node grew a
    // hand-rolled codec.
    #[test]
    fn test_node_decodes_pre_stage1_derived_fixture() {
        const INNER_BYTES: &[u8] = &[0, 3];
        const LEAF_BYTES: &[u8] = &[1, 9];
        assert_eq!(from_bytes::<Node>(INNER_BYTES).unwrap(), Node::Inner(PageId::from(3u64)));
        assert_eq!(from_bytes::<Node>(LEAF_BYTES).unwrap(), Node::Leaf(PageId::from(9u64)));
    }

    fn page_overhead(page_size: u64) -> u64 {
        page_size - Page::new_data(page_size).get_data_size()
    }

    fn make_header(page_size: u64) -> Arc<Header> {
        let mut v = vec![0x53u8, 0x65];
        v.extend_from_slice(&3u32.to_le_bytes()); // format_version
        v.extend_from_slice(&0u64.to_le_bytes()); // first_page_offset
        v.extend_from_slice(&FIRST_USER_PAGE.to_le_bytes()); // page_count
        v.extend_from_slice(&page_size.to_le_bytes());
        // last_checkpoint: u128, not fixint-annotated, so postcard varint-
        // encodes it — append its own to_allocvec output (see the identical
        // fix/comment in buffer.rs's make_header_bytes).
        v.extend_from_slice(&postcard::to_allocvec(&0u128).unwrap());
        v.extend_from_slice(&1u64.to_le_bytes()); // counter (phase 1)
        v.extend_from_slice(&0u64.to_le_bytes()); // checkpoint_lsn (phase 6)
        // header_checksum (STORE_AUDIT.md S1) — never validated on this
        // direct PageBuffer-construction path (only Db::open_using calls
        // Header::validate), so a placeholder value is fine here.
        v.extend_from_slice(&0u32.to_le_bytes());
        Arc::new(from_bytes::<Header>(&v).unwrap())
    }

    // One clock shared by buffer, transaction manager, and logger — as
    // production wires them (TXN_SIMPLIFICATION_PLAN.md phase 1). With
    // separate clocks the buffer's flush gate (`page.lsn <= durable`) never
    // opens, every stamped page is deferred, and backpressure eventually
    // blocks the test forever.
    fn make_buffer(page_size: u64) -> Arc<PageBuffer<MemFile>> {
        let clock = Arc::new(crate::logger::LsnClock::default());
        // These tests drive the tree directly, with no Db-level operation
        // ever logging a record, so nothing would advance the durable
        // watermark — and the writer gates every stamped page on it.
        // Declare everything durable up front: there is no WAL to wait for.
        clock.mark_written(crate::logger::LsnId(u64::MAX));
        make_buffer_with_clock(page_size, clock)
    }

    fn make_buffer_with_clock(
        page_size: u64,
        clock: Arc<crate::logger::LsnClock>,
    ) -> Arc<PageBuffer<MemFile>> {
        let header = make_header(page_size);
        let counter = Arc::new(AtomicU64::new(FIRST_USER_PAGE));
        Arc::new(
            PageBuffer::new(
                page_size,
                counter,
                MemFile::new(),
                header,
                256,
                clock,
                Arc::new(crate::pages::content::PageContentRegistry::builtin()),
            )
            .unwrap(),
        )
    }

    fn make_txn_mgr(clock: Arc<crate::logger::LsnClock>) -> Arc<TransactionManager> {
        TransactionManager::new(clock).into()
    }

    fn make_logger(clock: Arc<crate::logger::LsnClock>) -> Arc<Logger> {
        let mut logger = Logger::with_clock(clock);
        logger.set_db_for_test(MemFile::new()).unwrap();
        Arc::new(logger)
    }

    fn make_tree(page_size: u64) -> BPlusTree<MemFile> {
        make_tree_with_entry_size(page_size, MAX_ENTRY_BYTES)
    }

    fn make_tree_with_entry_size(page_size: u64, index_entry_size: u64) -> BPlusTree<MemFile> {
        let buf = make_buffer(page_size);
        let clock = buf.clock();
        BPlusTree::new(
            1.into(),
            "t".into(),
            buf,
            make_txn_mgr(clock.clone()),
            make_logger(clock),
            index_entry_size,
        )
        .unwrap()
    }

    fn txn() -> TransactionId {
        TransactionId::from(1u64)
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

    // Regression test for RangeCursor's leaf-to-leaf walk (cursor.rs):
    // index LEAF pages must be chained via next_page on every split, the
    // same way data pages already are, so a range scan can walk them in
    // ascending key order without ever going through the index's inner
    // nodes. Forces enough sequential inserts to produce several leaf
    // splits (both the root's own first split and at least one
    // non-root leaf split), then walks the chain purely via
    // find_leaf_page/next_leaf_page and checks it visits every key
    // exactly once, in order — not through find()/range_scan, so this
    // isolates the chain-wiring itself from the rest of RangeCursor.
    #[test]
    fn test_leaf_pages_chain_across_multiple_splits_in_ascending_order() {
        let page_size = MAX_ENTRY_BYTES * 4; // nodes_per_page = 4
        let tree = make_tree(page_size);

        for i in 1u64..=40 {
            tree.insert(Tuple::new(i, b"v"), txn()).unwrap();
        }

        let mut leaf = tree.find_leaf_page(&DBIdType::Int(1)).unwrap();
        let mut seen: Vec<u64> = vec![];
        loop {
            for row in leaf.iter() {
                match row.id {
                    DBIdType::Int(i) => seen.push(i),
                    other => panic!("unexpected id type {other:?}"),
                }
            }
            match tree.next_leaf_page(&leaf).unwrap() {
                Some(next) => leaf = next,
                None => break,
            }
        }

        assert_eq!(
            seen,
            (1u64..=40).collect::<Vec<_>>(),
            "walking the leaf chain from the first leaf must visit every \
             key exactly once, in ascending order, regardless of how many \
             splits happened along the way"
        );
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

    // STORE_AUDIT.md P4: direct behavioral confirmation that a real,
    // post-split index page's routing entries carry no live TransactionId
    // — not just that insert_index/update_index_entry/update_root_page's
    // construction sites were edited to pass None, but that the actual
    // cached/on-disk entries reflect it. Reads the root's own entries
    // after it's been forced to split (so this exercises update_root_page,
    // not just insert_index's leaf-level path).
    #[test]
    fn test_index_routing_entries_carry_no_live_txn_id() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, b"x"), txn()).unwrap();
        }
        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        assert!(ip.is_flag_set(INNER_NODE), "sanity: root must have split");
        let entries: Vec<_> = ip.iter().collect();
        assert!(!entries.is_empty(), "sanity: root must have routing entries");
        for entry in &entries {
            assert!(
                entry.txn_id.is_none(),
                "index routing entry {:?} carries a live txn_id — dead weight \
                 P4 removed, since visibility is resolved through the data \
                 tuple, never the routing entry",
                entry.id
            );
        }
    }

    // STORE_AUDIT.md P4 — quantifies the actual per-entry byte savings from
    // no longer stamping a live TransactionId on an index/routing entry,
    // using the exact same Tuple::size() (postcard serialized size) the
    // page-capacity accounting itself relies on.
    #[test]
    fn test_dropping_txn_id_from_a_routing_entry_shrinks_its_serialized_size() {
        let id = DBIdType::Int(12345);
        let data = postcard::to_allocvec(&Node::Leaf(PageId::from(6789u64))).unwrap();
        let without_txn = Tuple::new_with(id.clone(), &data, None, None);
        // Small ids (a freshly-opened test db's txn() helper: id=1, ts=1) vs a
        // "mature database" magnitude, since postcard's varint cost scales
        // with the VALUE, not the field's declared width — a small live
        // TransactionId costs little over None; a large one costs much more,
        // and only None ever costs the same 1 byte regardless.
        let with_small_txn = Tuple::new_with(id.clone(), &data, Some(txn()), None);
        let with_large_txn = Tuple::new_with(
            id,
            &data,
            Some(TransactionId::from(50_000_000u64)),
            None,
        );
        assert!(
            without_txn.size() < with_small_txn.size(),
            "dropping the dead txn_id must shrink the entry even for a small id: \
             with={} without={}",
            with_small_txn.size(),
            without_txn.size()
        );
        assert!(
            with_small_txn.size() < with_large_txn.size(),
            "sanity: a larger TransactionId must cost more bytes than a small one"
        );
        eprintln!(
            "routing entry size: without txn_id = {} B, with a small live one = {} B \
             ({} B saved), with a large one = {} B ({} B saved)",
            without_txn.size(),
            with_small_txn.size(),
            with_small_txn.size() - without_txn.size(),
            with_large_txn.size(),
            with_large_txn.size() - without_txn.size(),
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

    // Same idea as test_leaf_pages_chain_across_multiple_splits_in_
    // ascending_order, but under random insertion order — sequential
    // inserts always split at the rightmost edge (see split_point's
    // rightmost-append optimization), which is a narrower code path than
    // splits triggered by inserts landing in the middle of a page. Random
    // order exercises both, so this is the stronger check that the
    // leaf-chain wiring in split_non_root_page/split_root_page is correct
    // regardless of where in a page the split point falls.
    #[test]
    fn test_leaf_chain_remains_complete_and_ordered_under_random_inserts() {
        let page_size = MAX_ENTRY_BYTES * 4;
        let tree = make_tree(page_size);

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

        let mut leaf = tree.find_leaf_page(&DBIdType::Int(1)).unwrap();
        let mut seen: Vec<u64> = vec![];
        loop {
            for row in leaf.iter() {
                match row.id {
                    DBIdType::Int(i) => seen.push(i),
                    other => panic!("unexpected id type {other:?}"),
                }
            }
            match tree.next_leaf_page(&leaf).unwrap() {
                Some(next) => leaf = next,
                None => break,
            }
        }
        assert_eq!(
            seen,
            (1u64..=2000).collect::<Vec<_>>(),
            "leaf chain must cover every inserted key exactly once, in \
             ascending order, no matter what order they were inserted in"
        );
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
    fn test_split_child_index_pages_preserve_record_size() {
        // The whole point of alloc_sibling_index_page: a split-created index
        // page must keep the same fixed per-entry budget as the page it
        // split from (via the root's own record_size, propagated forward),
        // not silently become an unbounded AnyTuplePage the way alloc_page
        // used to hand back.
        let index_entry_size = MAX_ENTRY_BYTES;
        let page_size = index_entry_size * 4;
        let tree = make_tree_with_entry_size(page_size, index_entry_size);

        for i in 1u64..=4 {
            tree.insert(Tuple::new(i, b"y"), txn()).unwrap();
        }

        let ip = tree.buffer.get_page(tree.table.first_index_page).unwrap();
        let entries: Vec<_> = ip.iter().collect();
        assert_eq!(
            entries.len(),
            2,
            "root must have split into 2 routing entries"
        );
        for entry in &entries {
            let node: Node = from_bytes(&entry.data).unwrap();
            if let Node::Inner(child_page_id) = node {
                let cp = tree.buffer.get_page(child_page_id).unwrap();
                assert_eq!(
                    cp.record_size(),
                    Some(index_entry_size as usize),
                    "split-created child page {:?} must keep the table's own \
                     index_entry_size, not silently drop to a variable-size page",
                    child_page_id
                );
            } else {
                panic!("inner root entry must be Node::Inner");
            }
        }
    }

    #[test]
    fn test_oversized_insert_after_split_still_returns_tuple_too_large() {
        // Regression test: before alloc_sibling_index_page, a split sibling
        // was always allocated as a variable-size AnyTuplePage with no
        // per-entry budget at all — an oversized entry that routed there
        // would bypass TupleTooLarge entirely, since that protection only
        // ever covered a table's very first, pre-split index page. Force a
        // split first, then confirm the budget still holds no matter which
        // leaf the next insert actually lands on.
        let index_entry_size = MAX_ENTRY_BYTES;
        let page_size = index_entry_size * 4;
        let tree = make_tree_with_entry_size(page_size, index_entry_size);

        for i in 1u64..=4 {
            tree.insert(Tuple::new(i, b"y"), txn()).unwrap();
        }

        let big_key =
            DBIdType::Rec(IndexKey::new_from(&[ValueItem::Str(("z".repeat(90), 90))]).unwrap());
        match tree.insert(Tuple::new_with(big_key, b"v", None, None), txn()) {
            Err(StoreError::TupleTooLarge(actual, budget)) => {
                assert_eq!(budget, index_entry_size as usize);
                assert!(
                    actual > budget as u64,
                    "reported actual size must exceed the budget it failed against"
                );
            }
            other => panic!("expected TupleTooLarge, got {other:?}"),
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
    // Note: a thread can legitimately fail with PageCapacityError (another
    // racer filled the page first) — that's tracked separately and NOT
    // retried. We only care here about: of the inserts that returned Ok, was
    // every single one of them actually findable afterwards?
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
                tree.insert(Tuple::new(i, b"warm"), TransactionId::from(i))
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
                            TransactionId::from(id),
                        )
                    })
                })
                .collect();

            let results: Vec<Result<PageId, StoreError>> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();

            for (&id, result) in racer_ids.iter().zip(results.iter()) {
                match result {
                    Ok(_p) => {
                        if tree.find(DBIdType::Int(id)).unwrap().is_none() {
                            lost_update_iterations.push((iteration, id));
                        }
                    }
                    // A legitimate, *explicit* failure under heavy contention, not
                    // a silent lost update: another racer filled the page first and
                    // this insert correctly saw that fresh (not stale) state and
                    // refused to write, rather than silently overwriting.
                    Err(StoreError::PageCapacityError) => {
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
                TransactionId::from(i),
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

    // --- index_entry_size: BPlusTree::new takes it as a real parameter now,
    // not an assumption baked in via MAX_ENTRY_BYTES ---

    #[test]
    fn test_new_rejects_zero_index_entry_size() {
        let buf = make_buffer(BIG);
        let clock = buf.clock();
        let result = BPlusTree::new(
            1.into(),
            "t".into(),
            buf,
            make_txn_mgr(clock.clone()),
            make_logger(clock),
            0,
        );
        let err = match result {
            Ok(_) => panic!("index_entry_size = 0 must be rejected, not silently divide by zero"),
            Err(e) => e,
        };
        assert!(matches!(err, StoreError::UnknownError(_)), "got {err:?}");
    }

    #[test]
    fn test_new_rejects_index_entry_size_too_large_to_fit_two_entries() {
        let page_size = 1000u64;
        let index_entry_size = 600u64; // page_size / index_entry_size == 1 < 2
        let buf = make_buffer(page_size);
        let clock = buf.clock();
        let result = BPlusTree::new(
            1.into(),
            "t".into(),
            buf,
            make_txn_mgr(clock.clone()),
            make_logger(clock),
            index_entry_size,
        );
        let err = match result {
            Ok(_) => {
                panic!("an index_entry_size that can't fit at least 2 entries per page must fail")
            }
            Err(e) => e,
        };
        let StoreError::UnknownError(msg) = err else {
            panic!("expected UnknownError, got {err:?}");
        };
        // The error used to be a string literal with unescaped {count}/{size}
        // placeholders — never actually interpolated, so it printed that
        // literal text instead of the real numbers. Assert the real values
        // actually appear now.
        assert!(
            msg.contains(&page_size.to_string()) && msg.contains(&index_entry_size.to_string()),
            "error message must include the actual page_size/index_entry_size \
             that failed, not a template with unfilled placeholders: {msg:?}"
        );
    }

    #[test]
    fn test_nodes_per_page_is_computed_from_the_supplied_index_entry_size() {
        // Same page_size, two different index_entry_size choices, must
        // produce different nodes_per_page — proving the parameter is what
        // actually drives capacity, not a hardcoded constant.
        let page_size = 1024u64;
        let small_entries = make_tree_with_entry_size(page_size, 32);
        let large_entries = make_tree_with_entry_size(page_size, 128);
        assert_eq!(
            small_entries.table.nodes_per_page,
            (page_size / 32) as usize
        );
        assert_eq!(
            large_entries.table.nodes_per_page,
            (page_size / 128) as usize
        );
        assert!(
            small_entries.table.nodes_per_page > large_entries.table.nodes_per_page,
            "a smaller per-entry budget must pack strictly more entries per page"
        );
    }

    #[test]
    fn test_custom_index_entry_size_supports_normal_insert_find_remove_roundtrip() {
        // Deliberately NOT MAX_ENTRY_BYTES-derived, and small enough to force
        // several splits over the course of the test — exercises ordinary
        // operation with a caller-chosen size end to end, not just table
        // construction.
        let tree = make_tree_with_entry_size(512, 40);
        for i in 0u64..50 {
            tree.insert(Tuple::new(i, format!("v{i}").as_bytes()), txn())
                .unwrap();
        }
        for i in 0u64..50 {
            assert_eq!(
                tree.find(DBIdType::Int(i))
                    .unwrap()
                    .map(|t| t.data.to_vec()),
                Some(format!("v{i}").into_bytes())
            );
        }
        let removed = tree.remove(DBIdType::Int(10)).unwrap();
        assert!(removed.is_some());
        assert!(tree.find(DBIdType::Int(10)).unwrap().is_none());
    }

    // The concrete motivation for this whole change: a composite Rec(IndexKey)
    // key with Str fields easily exceeds MAX_ENTRY_BYTES (64) — the default
    // guess sized for a plain Int key — and the failure mode without a
    // deliberate size is a clear insert-time TupleTooLarge. A caller who
    // computes their key's real upper bound and passes it as
    // index_entry_size avoids that entirely.
    #[test]
    fn test_composite_key_too_big_for_default_entry_size_succeeds_with_a_larger_one() {
        // Two Str fields reserving 50 bytes each: comfortably over
        // MAX_ENTRY_BYTES (64) once IndexKey/Tuple framing is added, and
        // comfortably under a 300-byte budget.
        let big_key = || -> DBIdType {
            DBIdType::Rec(
                IndexKey::new_from(&[
                    ValueItem::Str(("alpha".repeat(9), 50)),
                    ValueItem::Str(("beta".repeat(9), 50)),
                ])
                .unwrap(),
            )
        };

        // Default-sized index (MAX_ENTRY_BYTES): the same insert must fail,
        // not silently corrupt something — confirming the failure mode this
        // change is meant to let callers avoid via an explicit size instead.
        // FixedTuplePage::add already checks the serialized entry against
        // tuple_size and reports TupleTooLarge(actual, budget) — a much
        // clearer failure than a generic PageCapacityError.
        let default_sized = make_tree(BIG);
        let err = default_sized
            .insert(Tuple::new_with(big_key(), b"v", None, None), txn())
            .expect_err(
                "a composite key exceeding MAX_ENTRY_BYTES must fail on a \
                 default-sized index, not silently succeed",
            );
        match err {
            StoreError::TupleTooLarge(actual, budget) => {
                assert_eq!(budget, MAX_ENTRY_BYTES as usize);
                assert!(
                    actual > budget as u64,
                    "reported actual size must exceed the budget it failed against"
                );
            }
            other => panic!("expected TupleTooLarge, got {other:?}"),
        }

        // Deliberately sized index: the identical insert now succeeds.
        let deliberately_sized = make_tree_with_entry_size(BIG, 300);
        let key = big_key();
        deliberately_sized
            .insert(Tuple::new_with(key.clone(), b"v", None, None), txn())
            .unwrap();
        assert_eq!(
            deliberately_sized
                .find(key)
                .unwrap()
                .map(|t| t.data.to_vec()),
            Some(b"v".to_vec())
        );
    }

    // Scratch investigation, not a permanent benchmark: measures how much of
    // a hot-path point lookup's allocation/time is attributable to
    // ValueItem/IndexKey decode specifically, and how that scales with the
    // number of Str fields in a composite key. Every SlottedPage::bound
    // comparison step during descent calls decode_id_at, which for a Rec
    // key means postcard-decoding the whole IndexKey — an Arc<[ValueItem]>
    // allocation plus, per Str field, a fresh String — just to compare one
    // field; DBIdType::Int decode is a fixed-width integer read with no
    // allocation at all, so the int row is the zero-allocation baseline.
    // The per-field delta is what a zero-copy/borrowed comparison path
    // (compare against the raw on-disk bytes directly, never materializing
    // a ValueItem/IndexKey/String) could plausibly recover, and this is
    // meant to be re-run with the identical text against that change once
    // it exists, the same way alloc_proxy_table_scan_snapshot (cursor.rs)
    // and this file's own composite-key tests were used for their fixes'
    // before/after numbers. Run with:
    //   cargo test -p store --lib tables::bplustree::tests::\
    //     alloc_proxy_find_scaling_by_composite_key_str_field_count \
    //     -- --ignored --nocapture --test-threads=1
    #[test]
    #[ignore]
    fn alloc_proxy_find_scaling_by_composite_key_str_field_count() {
        const ROWS: u64 = 20_000;
        // Comfortably fits up to 3 ~14-byte Str fields plus IndexKey/Tuple
        // framing; bumped from the single-field version's 300.
        const ENTRY_SIZE: u64 = 500;

        struct FindBenchResult {
            label: String,
            ns_per_find: f64,
            allocs_per_find: f64,
            bytes_per_find: f64,
        }

        fn measure(
            label: String,
            rows: u64,
            entry_size: u64,
            key_for: impl Fn(u64) -> DBIdType,
        ) -> FindBenchResult {
            let tree = make_tree_with_entry_size(BIG, entry_size);
            for i in 0..rows {
                tree.insert(Tuple::new_with(key_for(i), b"v", None, None), txn())
                    .unwrap();
            }
            let before = crate::alloc::stats();
            let start = std::time::Instant::now();
            for i in 0..rows {
                assert!(tree.find(key_for(i)).unwrap().is_some());
            }
            let elapsed = start.elapsed();
            let after = crate::alloc::stats();
            let allocs: usize = after
                .size_histogram
                .iter()
                .zip(before.size_histogram.iter())
                .map(|(a, b)| a - b)
                .sum();
            let bytes = after.total_allocated - before.total_allocated;
            FindBenchResult {
                label,
                ns_per_find: elapsed.as_nanos() as f64 / rows as f64,
                allocs_per_find: allocs as f64 / rows as f64,
                bytes_per_find: bytes as f64 / rows as f64,
            }
        }

        let mut results = vec![measure(
            "int (0 Str fields)".into(),
            ROWS,
            ENTRY_SIZE,
            DBIdType::Int,
        )];
        for fields in 1..=3usize {
            let key_for = move |i: u64| -> DBIdType {
                let items: Vec<ValueItem> = (0..fields)
                    .map(|f| {
                        let s = format!("key{f}-{i:08}");
                        let len = s.len() as u32;
                        ValueItem::Str((s, len))
                    })
                    .collect();
                DBIdType::Rec(IndexKey::new_from(&items).unwrap())
            };
            let label = format!(
                "rec ({fields} Str field{})",
                if fields == 1 { "" } else { "s" }
            );
            results.push(measure(label, ROWS, ENTRY_SIZE, key_for));
        }

        let baseline_ns = results[0].ns_per_find;
        println!(
            "{:<24} {:>10} {:>13} {:>12} {:>12}",
            "key shape", "ns/find", "allocs/find", "bytes/find", "delta vs int"
        );
        for r in &results {
            println!(
                "{:<24} {:>10.0} {:>13.2} {:>12.1} {:>+12.0}",
                r.label,
                r.ns_per_find,
                r.allocs_per_find,
                r.bytes_per_find,
                r.ns_per_find - baseline_ns
            );
        }
    }

    #[test]
    fn test_oversized_insert_on_nonempty_leaf_returns_tuple_too_large_not_panic() {
        // Regression test for the masking bug fixed alongside this: once a
        // fixed-record leaf page already holds other entries,
        // insert_recursive's own can_store precheck (an aggregate-bytes
        // fullness check, unrelated to any one tuple's size) could go false
        // for an oversized tuple *before* write_page/add_tuple was ever
        // called — falling into the "count == nodes, or too big" branch,
        // which used to unconditionally panic. That branch now checks the
        // page's record_size first and returns TupleTooLarge when that's the
        // real cause, so this must return a clean error, not panic. Fill the
        // leaf directly (bypassing insert(), which would trigger a root
        // split long before the page is byte-full, moving traffic onto a
        // split sibling — a variable-size AnyTuplePage with no record_size
        // at all, see BPlusTree::new's doc comment) so nodes_per_page-based
        // count stays low while the page's real aggregate bytes fill up.
        // 30 comfortably fits a real Int-keyed leaf entry (28 bytes) but,
        // combined with a small page, leaves an aggregate byte budget that
        // runs out at a lower count than nodes_per_page assumes — the exact
        // mismatch the fix guards against.
        let index_entry_size = 30u64;
        let tree = make_tree_with_entry_size(180, index_entry_size);
        {
            let mut handle = tree
                .buffer
                .get_page_mut(tree.table.first_index_page, crate::buffer::LockLevel::Index)
                .unwrap();
            let page = Arc::make_mut(&mut handle.page);
            let mut i = 0u64;
            // Payload sized so the page's aggregate byte budget runs out well
            // before nodes_per_page does (a TransactionId is one varint since
            // phase 1, so a 3-byte payload no longer gets there).
            let filler =
                |i: u64| Tuple::new_with(DBIdType::Int(i), b"payload-padding-", Some(txn()), None);
            while page.can_store(&filler(i)) {
                page.add_tuple(filler(i)).unwrap();
                i += 1;
            }
            assert!(
                i < tree.table.nodes_per_page as u64 - 1,
                "test setup assumption broken: the page filled by count, not aggregate \
                 bytes (i={i}, nodes_per_page={}) — this no longer exercises the \
                 aggregate-vs-record_size masking case",
                tree.table.nodes_per_page
            );
            tree.buffer.write_locked_page(handle).unwrap();
        }
        let big_key =
            DBIdType::Rec(IndexKey::new_from(&[ValueItem::Str(("z".repeat(60), 60))]).unwrap());
        match tree.insert(Tuple::new_with(big_key, b"v", None, None), txn()) {
            Err(StoreError::TupleTooLarge(actual, budget)) => {
                assert_eq!(budget, index_entry_size as usize);
                assert!(
                    actual > budget as u64,
                    "reported actual size must exceed the budget it failed against"
                );
            }
            other => panic!("expected TupleTooLarge, got {other:?}"),
        }
    }

    // STORE_AUDIT.md S8: route_to_leaf/find_page/resolve_index_entry/
    // remove_index_entry/update_index_entry/insert_recursive all panic
    // when a page's own INNER_NODE/LEAF_NODE flag disagrees with what an
    // entry (or the page itself) actually contains — e.g. an INNER_NODE
    // page holding a Node::Leaf entry where a Node::Inner routing pointer
    // was expected. Every real write path keeps these in sync by
    // construction, but a corrupted or hand-crafted on-disk file could
    // easily disagree, and for an embedded library a panic there is a
    // process crash for the host. Reproduces one representative case
    // (route_to_leaf, reached via the public find()) by directly replacing
    // the root page's content with exactly that mismatch — the other
    // panics in this file share the identical pattern (flag says one
    // node type, content says another) and were converted the same way,
    // verified by the full suite rather than one red test each.
    #[test]
    fn test_audit_s8_find_returns_corruption_error_for_an_inner_page_holding_a_leaf_entry() {
        let page_size = 4096;
        let tree = make_tree(page_size);

        let corrupt_root = Page::new_indexed(page_size, MAX_ENTRY_BYTES as usize);
        corrupt_root.set_page_flags(INNER_NODE).unwrap();
        corrupt_root
            .add_tuple(Tuple::new_with(
                DBIdType::Int(100),
                &postcard::to_allocvec(&Node::Leaf(tree.table.first_data_page)).unwrap(),
                None,
                None,
            ))
            .unwrap();
        let mut handle = tree.buffer.get_page_mut(tree.table.first_index_page, crate::buffer::LockLevel::Index).unwrap();
        handle.page = Arc::new(corrupt_root);
        tree.buffer.write_locked_page(handle).unwrap();

        // 1 < 100, so find() routes into the corrupt entry expecting
        // Node::Inner and finds Node::Leaf instead.
        let result = tree.find(DBIdType::Int(1));
        assert!(
            matches!(result, Err(StoreError::Corruption(_))),
            "expected StoreError::Corruption, got {result:?}"
        );
    }

    // STORE_AUDIT.md P5 — throwaway (not a committed criterion bench, same
    // call as this session's other direct microbenchmarks) wall-clock
    // measurement of route_to_leaf's traversal cost. A big page_size (to
    // approach the audit's own nodes_per_page ~256 reference case) with
    // many rows forces real multi-level depth, so this exercises repeated
    // full root-to-leaf walks, not a single-level page. Run the identical
    // test text against this revision and against `git show <pre-fix>:
    // store/src/tables/bplustree.rs` (patched with this same fn) to get a
    // before/after comparison — see BASELINE.md.
    #[test]
    #[ignore]
    fn bench_find_traversal_cost() {
        const ROWS: u64 = 20_000;
        const LOOKUPS: u64 = 20_000;
        let page_size = 16 * 1024;
        let tree = make_tree(page_size);
        for i in 1..=ROWS {
            tree.insert(Tuple::new(i, b"v"), txn()).unwrap();
        }
        let start = std::time::Instant::now();
        // Scattered, not sequential, so this doesn't just retrace the same
        // cached root-to-leaf path every time.
        let mut state: u64 = 0x243F_6A88_85A3_08D3;
        for _ in 0..LOOKUPS {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let id = 1 + (state % ROWS);
            tree.find(DBIdType::Int(id)).unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!(
            "bench_find_traversal_cost: {LOOKUPS} lookups over {ROWS} rows (page_size={page_size}) \
             in {elapsed:?} ({:.0} lookups/s)",
            LOOKUPS as f64 / elapsed.as_secs_f64()
        );
    }
}
