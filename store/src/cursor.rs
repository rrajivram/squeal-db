use std::{ops::Bound, sync::Arc};

use crate::{
    db::{DBFile, Db},
    error::StoreError,
    page::{Page, PageId, PageTupleIterator},
    table::TableIdType,
    tables::bplustree::BPlusTree,
    tuple::{DBIdType, Tuple},
    txn::{Transaction, TransactionId},
    valueitem::IndexKey,
};

pub trait Cursor {
    type Item;
    fn next(&mut self) -> Result<Option<Self::Item>, StoreError>;
    // Rewinds back to this cursor's own starting position — same
    // transaction/snapshot, same (start, end) range where applicable —
    // so a caller (a nested-loop join's inner side, say) can re-scan
    // from the top without re-opening a fresh cursor or transaction.
    fn reset(&mut self) -> Result<(), StoreError>;
}

// A scan needs a reader TransactionId for find_visible_to, but who keeps
// that transaction registered as "active" (so a concurrent commit's
// discard_or_defer_undo still retains what the scan might still need to
// walk back to) differs by caller:
//   - Owned: no transaction was supplied, so the cursor began one of its
//     own and must hold onto the RAII guard itself for its whole
//     lifetime — dropping it early would move it to `aborting`
//     prematurely, before the scan is done needing its snapshot honored.
//   - Borrowed: the caller supplied an already-open transaction (e.g. an
//     explicit BEGIN) that they keep alive themselves for at least as
//     long as the scan runs — the cursor only needs its id, not
//     ownership, since the caller's own guard is what keeps it active.
// Not Clone: it owns a Transaction in the Owned case, and Transaction is
// deliberately not Clone (see STORE_AUDIT.md T8). Nothing actually cloned
// a cursor before this either — the derive was just following
// Transaction's own (since removed) Clone, not serving a real call site.
enum ScanTxn {
    Owned(Transaction),
    Borrowed(TransactionId),
}

impl ScanTxn {
    fn id(&self) -> TransactionId {
        match self {
            ScanTxn::Owned(t) => t.id(),
            ScanTxn::Borrowed(id) => *id,
        }
    }
}

pub struct TableCursor<F: DBFile + 'static> {
    db: Arc<Db<F>>,
    table: TableIdType,
    // The CURRENT page's own id — next_data_page needs it (not derivable
    // from `current_page` alone) to resolve the real next data page
    // rather than a raw, possibly-overflow-continuation `next_page` read.
    // See BPlusTree::next_data_page's own doc comment.
    current_page_id: PageId,
    current_page: Arc<Page>,
    current_iter: PageTupleIterator,
    transaction: ScanTxn,
}

pub struct RangeCursor<F: DBFile + 'static> {
    db: Arc<Db<F>>,
    table: TableIdType,
    // A leaf of the INDEX tree (see BPlusTree::find_leaf_page/
    // next_leaf_page), not a data page like TableCursor's current_page —
    // its entries are (id, Node::Leaf(data_page_id)) routing tuples that
    // next() resolves to the real row via resolve_index_entry.
    current_leaf: Arc<Page>,
    current_iter: PageTupleIterator,
    transaction: Transaction,
    start: Bound<DBIdType>,
    end: Bound<DBIdType>,
    // Some for prefix scans (see Db::prefix_scan): the scan ends at the
    // first entry whose leading fields differ from this key's.
    prefix: Option<IndexKey>,
    // Set once an index entry is past end is seen: ascending leaf-chain order
    // guarantees everything after that point is also >= end, so next()
    // can stop instead of walking the rest of the tree.
    done: bool,
}

impl<F: DBFile> TableCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub(crate) fn new(
        db: Arc<Db<F>>,
        table: TableIdType,
        transaction: Option<TransactionId>,
    ) -> Result<Self, StoreError> {
        let transaction = match transaction {
            Some(id) => ScanTxn::Borrowed(id),
            None => ScanTxn::Owned(db.begin()?),
        };
        let (current_page_id, current_page) = db
            .table_by_id(table)?
            .next_data_page(None)?
            .ok_or(StoreError::UnknownError("No data page found".into()))?;
        let current_iter = current_page.iter();
        Ok(Self {
            db,
            table,
            current_iter,
            current_page_id,
            current_page,
            transaction,
        })
    }

    fn next_tuple(&mut self) -> Result<Option<Tuple>, StoreError> {
        // Loop, not a single step: a data page can be EMPTY (every row on it
        // removed and reclaimed, or relocated away to the tail) while still
        // sitting in the chain. Advancing exactly one page and returning
        // whatever it yields ended the whole scan on the second consecutive
        // empty page — a scan that returned zero rows from a table with
        // hundreds, found by the crash harness's scan check
        // (TXN_SIMPLIFICATION_PLAN.md phase 0).
        loop {
            if let Some(t) = self.current_iter.next() {
                return Ok(Some(t));
            }
            let new_page = self.db.table_by_id(self.table)?.next_data_page(Some((
                self.current_page_id,
                Arc::clone(&self.current_page),
            )))?;
            match new_page {
                Some((new_page_id, new_page)) => {
                    self.current_page_id = new_page_id;
                    self.current_page = new_page;
                    self.current_iter = self.current_page.iter();
                }
                None => return Ok(None),
            }
        }
    }
}

impl<F: DBFile> RangeCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub(crate) fn new(
        db: Arc<Db<F>>,
        table: TableIdType,
        transaction: Option<Transaction>,
        start: Bound<DBIdType>,
        end: Bound<DBIdType>,
    ) -> Result<Self, StoreError> {
        let transaction = transaction.unwrap_or(db.begin()?);
        // Positional: finds the leaf that would hold `start` whether or
        // not `start` actually exists as a key (unlike the old
        // find_first_page, which did an exact index lookup and errored
        // with KeyNotFound if `start` wasn't a real row).
        let current_leaf = Self::start_leaf(&db.table_by_id(table)?, &start)?;
        let current_iter = current_leaf.iter();
        Ok(Self {
            db,
            table,
            current_iter,
            current_leaf,
            transaction,
            start,
            end,
            prefix: None,
            done: false,
        })
    }

    // Every entry whose leading fields equal `prefix`, in key order.
    //
    // Deliberately NOT a range over the short key itself: IndexKey's
    // ordering calls a short key Equal to every longer key that starts with
    // it, but the tree routes with strict successor() lookups, so a short
    // start key can land past some of the prefix's entries. Instead the
    // start is a full-length key — the prefix followed by each remaining
    // field's lower_bound(), the shape read off an existing entry — and the
    // end is a leading-field check in next().
    pub(crate) fn new_prefix(
        db: Arc<Db<F>>,
        table: TableIdType,
        prefix: IndexKey,
    ) -> Result<Self, StoreError> {
        let transaction = db.begin()?;
        let tree = db.table_by_id(table)?;
        let start = Self::prefix_start(&tree, &prefix)?;
        let current_leaf = Self::start_leaf(&tree, &start)?;
        let current_iter = current_leaf.iter();
        Ok(Self {
            db,
            table,
            current_iter,
            current_leaf,
            transaction,
            start,
            end: Bound::Unbounded,
            prefix: Some(prefix),
            done: false,
        })
    }

    fn prefix_start(
        tree: &Arc<BPlusTree<F>>,
        prefix: &IndexKey,
    ) -> Result<Bound<DBIdType>, StoreError> {
        // Any entry shows the shape (all keys of an index share it). An
        // empty index has no entries to scan: start at the beginning.
        let Some(sample) = tree.first_leaf_page()?.iter().next() else {
            return Ok(Bound::Unbounded);
        };
        let DBIdType::Rec(sample) = sample.id else {
            return Err(StoreError::UnknownError(
                "prefix_scan needs a table keyed by IndexKey".into(),
            ));
        };
        let n = prefix.values().len();
        if n > sample.values().len() {
            return Err(StoreError::UnknownError(format!(
                "prefix has {n} fields but the key has {}",
                sample.values().len()
            )));
        }
        let mut full: Vec<_> = prefix.values().to_vec();
        full.extend(sample.values()[n..].iter().map(|v| v.lower_bound()));
        Ok(Bound::Included(DBIdType::Rec(IndexKey::new_from(&full)?)))
    }

    fn start_leaf(table: &Arc<BPlusTree<F>>, start: &Bound<DBIdType>) -> Result<Arc<Page>, StoreError> {
        match start {
            Bound::Included(k) | Bound::Excluded(k) => table.find_leaf_page(k),
            Bound::Unbounded => table.first_leaf_page(),
        }
    }

    // Advances to the next INDEX entry (an (id, Node::Leaf(data_page_id))
    // routing tuple, not a real row) in ascending key order, walking
    // leaf-to-leaf via the leaf sibling chain once the current leaf is
    // exhausted. Mirrors TableCursor::next_tuple's pattern, but over index
    // leaves instead of data pages.
    fn next_index_entry(&mut self, table: &BPlusTree<F>) -> Result<Option<Tuple>, StoreError> {
        // Same loop as TableCursor::next_tuple, for the same reason: an
        // index leaf whose every entry was removed is still in the leaf
        // chain, and must be skipped rather than end the scan.
        loop {
            if let Some(t) = self.current_iter.next() {
                return Ok(Some(t));
            }
            match table.next_leaf_page(&self.current_leaf)? {
                Some(next_leaf) => {
                    self.current_leaf = next_leaf;
                    self.current_iter = self.current_leaf.iter();
                }
                None => return Ok(None),
            }
        }
    }
}

impl<F: DBFile> Cursor for RangeCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    type Item = Tuple;
    // Loops rather than resolving just one raw row per call: a single
    // physical row can be invisible for two different reasons, and either
    // one must make the cursor move on to the next row instead of
    // stopping or surfacing something the caller shouldn't see.
    //   - find_last_committed returns None when the row's writer isn't
    //     committed and there's no committed ancestor to walk back to (an
    //     in-flight insert with nothing before it) — the old `.unwrap()`
    //     here would panic on exactly this, which a concurrent writer
    //     racing the scan makes entirely reachable, not just theoretical.
    //   - a resolved-but-tombstoned tuple means the key was removed (a
    //     committed remove's tombstone, matching Db::find's own check) —
    //     it must be treated as absent, the same way Db::find does.
    fn next(&mut self) -> Result<Option<Self::Item>, StoreError> {
        if self.done {
            return Ok(None);
        }
        let table = self.db.table_by_id(self.table)?;
        let reader = self.transaction.id();
        // See Db::find: a finished reader may not keep scanning.
        self.db.require_active(&reader)?;
        loop {
            match self.next_index_entry(&table)? {
                Some(entry) => {
                    // The leaf containing `start` generally holds entries
                    // both below and at/above it — skip the ones below.
                    let before_start = match &self.start {
                        Bound::Included(k) => entry.id < *k,
                        Bound::Excluded(k) => entry.id <= *k,
                        Bound::Unbounded => false,
                    };
                    if before_start {
                        continue;
                    }
                    // Ascending leaf-chain order guarantees everything
                    // from here on is also >= end, so this is a real
                    // early-termination, not just a filter.
                    let past_end = match &self.end {
                        Bound::Included(k) => entry.id > *k,
                        Bound::Excluded(k) => entry.id >= *k,
                        Bound::Unbounded => false,
                    };
                    let off_prefix = match (&self.prefix, &entry.id) {
                        (Some(p), DBIdType::Rec(k)) => !p
                            .values()
                            .iter()
                            .zip(k.values())
                            .all(|(a, b)| a.cmp(b) == std::cmp::Ordering::Equal),
                        _ => false,
                    };
                    if past_end || off_prefix {
                        self.done = true;
                        return Ok(None);
                    }
                    // Resolve the index entry's Node::Leaf pointer to the
                    // real row. None here would mean the index still has
                    // an entry for a row that's already gone from its data
                    // page — the same kind of transient inconsistency
                    // remove()'s own retry logic exists to close, not a
                    // new failure mode this cursor needs to invent
                    // handling for; skip and move on rather than erroring
                    // the whole scan over one stale entry.
                    let Some(tuple) = table.resolve_index_entry(&entry)? else {
                        continue;
                    };
                    match self.db.find_visible_to(&tuple, &reader)? {
                        Some(committed) if !committed.is_tombstoned() => {
                            return Ok(Some(committed.into_owned()));
                        }
                        _ => continue,
                    }
                }
                None => return Ok(None),
            }
        }
    }

    // Same lookup `new()` did to find the leaf holding `start` — keeps
    // the existing transaction (so a reset scan still reads a consistent
    // snapshot rather than picking up concurrent writes) and start/end,
    // which are already stored as fields rather than only consumed once
    // at construction time.
    fn reset(&mut self) -> Result<(), StoreError> {
        let tree = self.db.table_by_id(self.table)?;
        if let Some(prefix) = &self.prefix {
            // The index may have been empty when the scan was built.
            self.start = Self::prefix_start(&tree, prefix)?;
        }
        let current_leaf = Self::start_leaf(&tree, &self.start)?;
        self.current_iter = current_leaf.iter();
        self.current_leaf = current_leaf;
        self.done = false;
        Ok(())
    }
}

impl<F: DBFile> Cursor for TableCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    type Item = Tuple;
    // Loops rather than resolving just one raw row per call: a single
    // physical row can be invisible for two different reasons, and either
    // one must make the cursor move on to the next row instead of
    // stopping or surfacing something the caller shouldn't see.
    //   - find_last_committed returns None when the row's writer isn't
    //     committed and there's no committed ancestor to walk back to (an
    //     in-flight insert with nothing before it) — the old `.unwrap()`
    //     here would panic on exactly this, which a concurrent writer
    //     racing the scan makes entirely reachable, not just theoretical.
    //   - a resolved-but-tombstoned tuple means the key was removed (a
    //     committed remove's tombstone, matching Db::find's own check) —
    //     it must be treated as absent, the same way Db::find does.
    fn next(&mut self) -> Result<Option<Self::Item>, StoreError> {
        let reader = self.transaction.id();
        // See Db::find: a finished reader may not keep scanning.
        self.db.require_active(&reader)?;
        loop {
            match self.next_tuple()? {
                Some(t) => match self.db.find_visible_to(&t, &reader)? {
                    Some(committed) if !committed.is_tombstoned() => {
                        return Ok(Some(committed.into_owned()));
                    }
                    _ => continue,
                },
                None => return Ok(None),
            }
        }
    }

    // Same lookup `new()` did to find the table's first data page — keeps
    // the existing transaction, so a reset scan still reads a consistent
    // snapshot rather than picking up concurrent writes made between the
    // original scan and this reset.
    fn reset(&mut self) -> Result<(), StoreError> {
        let (current_page_id, current_page) = self
            .db
            .table_by_id(self.table)?
            .next_data_page(None)?
            .ok_or(StoreError::UnknownError("No data page found".into()))?;
        self.current_iter = current_page.iter();
        self.current_page_id = current_page_id;
        self.current_page = current_page;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memfile::MemFile;
    use crate::tuple::DBIdType;
    use crate::valueitem::{IndexKey, ValueItem};

    fn scan_all(db: &Arc<Db<MemFile>>, tid: TableIdType) -> Vec<Tuple> {
        let mut cursor = db.table_scan(tid).unwrap();
        let mut out = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            out.push(t);
        }
        out
    }

    fn scan_range(
        db: &Arc<Db<MemFile>>,
        tid: TableIdType,
        start: DBIdType,
        end: DBIdType,
    ) -> Vec<Tuple> {
        let mut cursor = db.range_scan(tid, start, end).unwrap();
        let mut out = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            out.push(t);
        }
        out
    }

    fn int_ids(tuples: &[Tuple]) -> Vec<u64> {
        let mut ids: Vec<u64> = tuples
            .iter()
            .map(|t| match t.id {
                DBIdType::Int(i) => i,
                _ => panic!("unexpected id type"),
            })
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn test_range_scan_basic_inclusive_start_exclusive_end() {
        let db = Db::<MemFile>::create("range_basic.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for i in 1u64..=10 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        let found = int_ids(&scan_range(&db, tid, DBIdType::Int(3), DBIdType::Int(7)));
        assert_eq!(
            found,
            vec![3, 4, 5, 6],
            "start must be inclusive and end exclusive"
        );
    }

    // Small page size (nodes_per_page = 256/64 = 4) forces several INDEX
    // leaf splits over 60 sequential inserts, so this exercises
    // RangeCursor's leaf-to-leaf walk across multiple leaves end to end
    // (not just within a single starting leaf, like the basic test above).
    #[test]
    fn test_range_scan_spans_multiple_leaf_splits() {
        let db = Db::<MemFile>::create_with_page_size_and_max_index_key_size("range_multi_leaf.db", 256, 8).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for i in 1u64..=60 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        let found = int_ids(&scan_range(&db, tid, DBIdType::Int(10), DBIdType::Int(50)));
        assert_eq!(found, (10u64..50).collect::<Vec<_>>());
    }

    // Regression test for the fix to the bug this test used to document:
    // range_scan used to require the range's start id to be an EXACT,
    // existing key (find_first_page did an exact index lookup and errored
    // with KeyNotFound otherwise). find_leaf_page now does a positional
    // "leaf that would hold this key" search instead, so a start id that
    // doesn't exist works fine — exactly what a normal "everything from 10
    // to 20" range query needs.
    #[test]
    fn test_range_scan_start_id_need_not_exist() {
        let db = Db::<MemFile>::create("range_start_neednt_exist.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        db.insert(tid, Tuple::new(5, b"v5"), &t).unwrap();
        db.insert(tid, Tuple::new(10, b"v10"), &t).unwrap();
        db.commit(t).unwrap();

        // No row with id exactly 7 — the range must still start from
        // whatever real row exists at or after it (here, 10).
        let found = int_ids(&scan_range(&db, tid, DBIdType::Int(7), DBIdType::Int(20)));
        assert_eq!(found, vec![10]);
    }

    // Regression test for the fix to the bug this test used to document:
    // range_scan used to walk the DATA page chain forward starting from
    // whichever data page the start id's index entry pointed to. Data
    // pages are populated in roughly insertion order (write_data always
    // tries the current tail first, extending the chain forward when
    // full), which has no relationship to key order — so a row that
    // qualifies for the range but was inserted onto an EARLIER data page
    // than the start id's own page was never visited (the old cursor only
    // ever walked forward from its starting page). Fixed by driving the
    // scan off the INDEX tree's own leaf-to-leaf chain (see
    // BPlusTree::find_leaf_page/next_leaf_page) instead of the data-page
    // chain — leaf order IS key order, so there's no "earlier page" to
    // miss.
    #[test]
    fn test_range_scan_finds_matching_rows_regardless_of_data_page_layout() {
        // BIG (8192 B) page, ~3000 B payloads: exactly 2 tuples fit per
        // data page (same recipe as bplustree.rs's own
        // test_data_page_chains_to_next_page_when_full).
        let db = Db::<MemFile>::create_with_page_size("range_earlier_pages.db", 8192).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let large = vec![b'x'; 3000];

        let t = db.begin().unwrap();
        // ids 100 and 101 fill data page 1 completely.
        db.insert(tid, Tuple::new(100, &large), &t).unwrap();
        db.insert(tid, Tuple::new(101, &large), &t).unwrap();
        // id 1 no longer fits on page 1, so it lands on page 2 even though
        // it is numerically far smaller than everything already stored.
        db.insert(tid, Tuple::new(1, &large), &t).unwrap();
        db.commit(t).unwrap();

        // A range covering all three ids, with the lower bound (id 1)
        // happening to live on the same (later) data page the scan starts
        // from.
        let found = int_ids(&scan_range(&db, tid, DBIdType::Int(1), DBIdType::Int(200)));
        assert_eq!(
            found,
            vec![1, 100, 101],
            "range scan must find every row in range regardless of which \
             data page it physically landed on — ids 100/101 live on an \
             EARLIER page than the scan's own starting page (id 1's page), \
             so they're missed if the walk only ever goes forward"
        );
    }

    // Mirrors TableCursor's identically-named test: RangeCursor's next()
    // shares the same find_last_committed + is_tombstoned handling, so it
    // must skip the same two invisible-row cases a plain table scan does.
    #[test]
    fn test_range_scan_skips_tombstoned_and_uncommitted_rows() {
        let db = Db::<MemFile>::create("range_tombstone_test.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for i in 1u64..=3 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        let table = db.table_by_id(tid).unwrap();
        let mut tombstoned = table.find(DBIdType::Int(2)).unwrap().unwrap();
        tombstoned.tombstone();
        table.update(tombstoned).unwrap();

        let uncommitted_txn = db.begin().unwrap();
        db.insert(tid, Tuple::new(4, b"v4"), &uncommitted_txn)
            .unwrap();
        std::mem::forget(uncommitted_txn);

        let found = int_ids(&scan_range(&db, tid, DBIdType::Int(1), DBIdType::Int(5)));
        assert_eq!(
            found,
            vec![1, 3],
            "range scan must skip the tombstoned key (2) and the uncommitted key (4)"
        );
    }

    fn rec_key(a: i64, b: i64) -> DBIdType {
        DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(a), ValueItem::Integer(b)]).unwrap())
    }

    // Basic sanity that multi-key (DBIdType::Rec) ids work at all through
    // the normal insert/find path, decoupled from range_scan's ordering
    // question (see the next test): a row keyed by a multi-field IndexKey
    // round-trips correctly through insert + find.
    #[test]
    fn test_multi_key_ids_insert_and_find() {
        let db = Db::<MemFile>::create("multikey_find_basic.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for (a, b) in [(0, 1), (0, 2), (1, 1)] {
            db.insert(
                tid,
                Tuple::new_with(rec_key(a, b), format!("v{a}-{b}").as_bytes(), None, None),
                &t,
            )
            .unwrap();
        }
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        for (a, b) in [(0, 1), (0, 2), (1, 1)] {
            let found = db.find(tid, rec_key(a, b), &t).unwrap().unwrap();
            assert_eq!(found.data().to_vec(), format!("v{a}-{b}").into_bytes());
        }
    }

    // Regression test for the fix to the bug this test used to document:
    // DBIdType::cmp used to order DBIdType::Rec purely by IndexKey::hash()
    // (a scrambling hash, unrelated to field values), so a range_scan over
    // multi-key ids didn't reliably return "every row whose value falls
    // between start and end". DBIdType::cmp now compares Rec ids
    // structurally (field-by-field, via IndexKey::partial_cmp), so a range
    // over a single-field IndexKey behaves exactly like the equivalent
    // DBIdType::Int range would.
    #[test]
    fn test_range_scan_with_multi_key_ids_follows_structural_order() {
        let db = Db::<MemFile>::create("range_multikey_structural_order.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let key = |a: i64| DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(a)]).unwrap());

        let t = db.begin().unwrap();
        for a in 0i64..=4 {
            db.insert(
                tid,
                Tuple::new_with(key(a), format!("v{a}").as_bytes(), None, None),
                &t,
            )
            .unwrap();
        }
        db.commit(t).unwrap();

        let mut cursor = db.range_scan(tid, key(0), key(3)).unwrap();
        let mut found = vec![];
        while let Some(tup) = cursor.next().unwrap() {
            found.push(String::from_utf8(tup.data().to_vec()).unwrap());
        }
        found.sort();
        assert_eq!(
            found,
            vec!["v0", "v1", "v2"],
            "range [0,3) over single-field IndexKey ids must return exactly \
             the rows whose value structurally falls in that range"
        );
    }

    // Same idea with a real multi-column key: the second field must act as
    // a tie-breaker within a fixed first field, exactly like a lexicographic
    // (customer_id, order_seq) index would need to for a range query over
    // "all of customer X's orders".
    #[test]
    fn test_range_scan_with_multi_key_ids_multi_field_lexicographic_order() {
        let db = Db::<MemFile>::create("range_multikey_lexicographic.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for (a, b) in [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)] {
            db.insert(
                tid,
                Tuple::new_with(rec_key(a, b), format!("v{a}-{b}").as_bytes(), None, None),
                &t,
            )
            .unwrap();
        }
        db.commit(t).unwrap();

        // "All of customer 1's orders": (1,1) through (1,3) inclusive, i.e.
        // up to but not including (2,1) — the first row of the next group.
        let mut cursor = db.range_scan(tid, rec_key(1, 1), rec_key(2, 1)).unwrap();
        let mut found = vec![];
        while let Some(tup) = cursor.next().unwrap() {
            found.push(String::from_utf8(tup.data().to_vec()).unwrap());
        }
        found.sort();
        assert_eq!(found, vec!["v1-1", "v1-2", "v1-3"]);
    }

    // Regression test for the exact bug this session's Arc<Db> change was
    // meant to unblock fixing: TableCursor::next used to call
    // find_last_committed(&t).unwrap(), which (a) panics outright on an
    // in-flight (uncommitted) row with no committed ancestor, and (b) even
    // when it didn't panic, never checked is_tombstoned() — so a
    // committed remove's tombstone would come back out of the cursor as a
    // live row, contradicting Db::find's own "tombstoned == absent" rule.
    //
    // Both scenarios are constructed directly rather than via Db::remove /
    // Transaction::drop: going through the normal API, Db::commit's
    // best-effort tombstone reclaim and Db::begin's drain_aborting both
    // physically clean up the row before a single-threaded test's scan
    // ever runs, so the cursor would never actually see the stale state a
    // real concurrent scan can observe mid-race. Bypassing them (writing
    // the tombstone straight via BPlusTree::update with no undo record,
    // and leaking an active transaction with mem::forget) reproduces
    // exactly what's left behind once commit/reclaim's best-effort step
    // hasn't run yet — deterministically, without needing to race a
    // second thread's timing.
    #[test]
    fn test_scan_skips_tombstoned_and_uncommitted_rows() {
        let db = Db::<MemFile>::create("cursor_test.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        // Three committed rows.
        let t = db.begin().unwrap();
        for i in 1u64..=3 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        // Directly overwrite row 2 with a committed-but-tombstoned tuple
        // (same txn_id it already had, which is already committed) — no
        // Db::remove, no undo record, so nothing will ever reclaim it.
        // This is exactly "a committed remove whose tombstone hasn't been
        // physically reclaimed yet", the state Db::find's own tombstone
        // check exists for.
        let table = db.table_by_id(tid).unwrap();
        let mut tombstoned = table.find(DBIdType::Int(2)).unwrap().unwrap();
        tombstoned.tombstone();
        table.update(tombstoned).unwrap();

        // A fourth row inserted but never committed or rolled back — must
        // not be visible to a scan either (find_last_committed resolves an
        // in-flight write with no committed ancestor to nothing). Leaked
        // (not dropped): dropping a Transaction triggers an implicit
        // rollback that Db::begin's drain_aborting would physically clean
        // up before the scan's own begin() call, defeating the point.
        let uncommitted_txn = db.begin().unwrap();
        db.insert(tid, Tuple::new(4, b"v4"), &uncommitted_txn)
            .unwrap();
        std::mem::forget(uncommitted_txn);

        let mut found: Vec<u64> = scan_all(&db, tid)
            .into_iter()
            .map(|t| match t.id {
                DBIdType::Int(i) => i,
                _ => panic!("unexpected id type"),
            })
            .collect();
        found.sort();
        assert_eq!(
            found,
            vec![1, 3],
            "scan must skip the tombstoned key (2) and the uncommitted key (4)"
        );
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 0: consecutive empty data pages in
    // the chain (every row on them removed and reclaimed) used to end a
    // table scan early. Small page so a handful of wide rows fill a page.
    #[test]
    fn test_table_scan_skips_consecutive_empty_data_pages() {
        let db = Db::<MemFile>::create_with_page_size("scan_empty_pages", 4096).unwrap();
        let tid = db.create_table("t".into()).unwrap();
        let t = db.begin().unwrap();
        for k in 0..60u64 {
            db.insert(tid, Tuple::new(k, &[1u8; 300]), &t).unwrap();
        }
        db.commit(t).unwrap();
        assert!(
            db.page_count() > 6,
            "expected several data pages, got page_count {}",
            db.page_count()
        );
        // Remove the first 40 rows (they fill the first few pages in
        // insertion order), commit, and let the next begin() reclaim them.
        let t = db.begin().unwrap();
        for k in 0..40u64 {
            db.remove(tid, DBIdType::Int(k), &t).unwrap();
        }
        db.commit(t).unwrap();
        drop(db.begin().unwrap());
        let got = int_ids(&scan_all(&db, tid));
        assert_eq!(got, (40..60u64).collect::<Vec<_>>());
        // Range scans walk index leaves the same way.
        let got = int_ids(&scan_range(&db, tid, DBIdType::Int(0), DBIdType::Int(100)));
        assert_eq!(got, (40..60u64).collect::<Vec<_>>());
    }

    #[test]
    fn test_table_cursor_reset_rescans_from_the_start() {
        let db = Db::<MemFile>::create("cursor_reset_table.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for i in 1u64..=10 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        let mut cursor = db.table_scan(tid).unwrap();
        let first_pass = std::iter::from_fn(|| cursor.next().unwrap()).count();
        assert_eq!(first_pass, 10, "sanity: exhausted the cursor once");
        assert!(
            cursor.next().unwrap().is_none(),
            "sanity: cursor is actually exhausted before reset"
        );

        cursor.reset().unwrap();
        let second_pass = std::iter::from_fn(|| cursor.next().unwrap()).count();
        assert_eq!(
            second_pass, 10,
            "reset must let the same cursor re-scan every row again"
        );
    }

    #[test]
    fn test_range_cursor_reset_rescans_the_same_range() {
        let db = Db::<MemFile>::create("cursor_reset_range.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t = db.begin().unwrap();
        for i in 1u64..=10 {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        let mut cursor = db
            .range_scan(tid, DBIdType::Int(3), DBIdType::Int(7))
            .unwrap();
        let first_pass = int_ids(&std::iter::from_fn(|| cursor.next().unwrap()).collect::<Vec<_>>());
        assert_eq!(first_pass, vec![3, 4, 5, 6], "sanity: first pass");

        cursor.reset().unwrap();
        let second_pass =
            int_ids(&std::iter::from_fn(|| cursor.next().unwrap()).collect::<Vec<_>>());
        assert_eq!(
            second_pass, first_pass,
            "reset must let the same cursor re-scan the identical (start, end) range"
        );
    }

    // STORE_AUDIT.md P7 — allocation-count proxy, complementing wall-clock
    // intuition with a hard number. `#[ignore]`d (run explicitly, alone)
    // because `crate::alloc::stats()` reads this whole PROCESS's global
    // allocator counters — any other test allocating concurrently would
    // pollute the delta. Run with:
    //   cargo test -p store --lib cursor::tests::alloc_proxy_table_scan_snapshot \
    //     -- --ignored --nocapture --test-threads=1
    //
    // Old code (Db::find_visible_to) re-resolved AND cloned the reader's
    // whole snapshot HashSet on every row candidate — for a scan of ROWS
    // rows with NOISE_TXNS other transactions concurrently active (so N =
    // NOISE_TXNS entries per clone), that's ROWS clones of an N-entry
    // HashSet to answer a question whose answer never changes for the
    // life of the scan. New code (TableCursor::new) resolves it exactly
    // once. This test's own body is intentionally caller-level only (just
    // `table_scan` + a `next()` loop) — the fix is entirely internal to
    // cursor.rs/db.rs, so the identical test text was run against both the
    // pre-fix and post-fix revisions to get the before/after numbers
    // recorded in BASELINE.md.
    #[test]
    #[ignore]
    fn alloc_proxy_table_scan_snapshot() {
        const ROWS: u64 = 2_000;
        const NOISE_TXNS: u64 = 100;

        let db = Db::<MemFile>::create("cursor_alloc_proxy.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        for i in 1..=ROWS {
            db.insert(tid, Tuple::new(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
        }
        db.commit(t).unwrap();

        // Keep NOISE_TXNS transactions active so the reader's own snapshot
        // (captured at its begin()) is non-trivially sized.
        let noise: Vec<_> = (0..NOISE_TXNS).map(|_| db.begin().unwrap()).collect();

        let before = crate::alloc::stats();
        let mut cursor = db.table_scan(tid).unwrap();
        let mut count = 0u64;
        while cursor.next().unwrap().is_some() {
            count += 1;
        }
        let after = crate::alloc::stats();
        assert_eq!(count, ROWS);

        let alloc_events: usize = after
            .size_histogram
            .iter()
            .zip(before.size_histogram.iter())
            .map(|(a, b)| a - b)
            .sum();
        let bytes = after.total_allocated - before.total_allocated;
        println!(
            "table_scan of {ROWS} rows, {NOISE_TXNS} concurrent noise txns: \
             {alloc_events} allocation events ({:.4}/row), {bytes} bytes ({:.2}/row)",
            alloc_events as f64 / ROWS as f64,
            bytes as f64 / ROWS as f64,
        );

        drop(noise);
    }

    // --- ValueItem/IndexKey lower_bound()/upper_bound() as range-scan bounds ---

    use std::ops::Bound;

    fn mm_key(a: i64, b: i64) -> IndexKey {
        IndexKey::new_from(&[ValueItem::Integer(a), ValueItem::Integer(b)]).unwrap()
    }

    fn mm_db(name: &str, rows: &[(i64, i64)]) -> (Arc<Db<MemFile>>, TableIdType) {
        let db = Db::<MemFile>::create(name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        for (a, b) in rows {
            db.insert(
                tid,
                Tuple::new_with(DBIdType::Rec(mm_key(*a, *b)), b"v", None, None),
                &t,
            )
            .unwrap();
        }
        db.commit(t).unwrap();
        (db, tid)
    }

    fn bounded(
        db: &Arc<Db<MemFile>>,
        tid: TableIdType,
        start: Bound<IndexKey>,
        end: Bound<IndexKey>,
    ) -> Vec<Tuple> {
        let wrap = |b: Bound<IndexKey>| match b {
            Bound::Included(k) => Bound::Included(DBIdType::Rec(k)),
            Bound::Excluded(k) => Bound::Excluded(DBIdType::Rec(k)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut cursor = db.range_scan_bounds(tid, wrap(start), wrap(end)).unwrap();
        let mut out = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            out.push(t);
        }
        out
    }

    fn scanned_pairs(tuples: &[Tuple]) -> Vec<(i64, i64)> {
        let mut out: Vec<(i64, i64)> = tuples
            .iter()
            .map(|t| match &t.id {
                DBIdType::Rec(k) => match (&k.values()[0], &k.values()[1]) {
                    (ValueItem::Integer(a), ValueItem::Integer(b)) => (*a, *b),
                    other => panic!("unexpected key {other:?}"),
                },
                other => panic!("unexpected id {other:?}"),
            })
            .collect();
        out.sort();
        out
    }

    fn extreme_grid() -> Vec<(i64, i64)> {
        let vals = [i64::MIN, -1, 0, 1, i64::MAX - 1, i64::MAX];
        vals.iter().flat_map(|a| vals.iter().map(move |b| (*a, *b))).collect()
    }

    // A whole-index scan lower_bound()..=upper_bound() returns EVERY key,
    // including the ones equal to either bound.
    #[test]
    fn test_full_range_from_lower_bound_to_upper_bound_returns_every_key() {
        let rows = extreme_grid();
        let (db, tid) = mm_db("bounds_scan_full.db", &rows);
        let template = mm_key(0, 0);
        let got = bounded(
            &db,
            tid,
            Bound::Included(template.lower_bound()),
            Bound::Included(template.upper_bound().unwrap()),
        );
        let mut want = rows.clone();
        want.sort();
        assert_eq!(scanned_pairs(&got), want);
    }

    // Unbounded on both sides is the same whole-index scan, with no sentinel.
    #[test]
    fn test_unbounded_range_returns_every_key_across_many_leaves() {
        let rows: Vec<(i64, i64)> = (0..400).flat_map(|a| (0..5).map(move |b| (a, b))).collect();
        let (db, tid) = mm_db("bounds_scan_unbounded.db", &rows);
        let got = bounded(&db, tid, Bound::Unbounded, Bound::Unbounded);
        assert_eq!(scanned_pairs(&got), rows);
    }

    // Excluded/Included on both ends, against an ordinary middle range.
    #[test]
    fn test_all_four_inclusivity_combinations() {
        let rows: Vec<(i64, i64)> = (0..10).map(|a| (a, 0)).collect();
        let (db, tid) = mm_db("bounds_scan_incl.db", &rows);
        let run = |s: Bound<IndexKey>, e: Bound<IndexKey>| -> Vec<i64> {
            scanned_pairs(&bounded(&db, tid, s, e)).into_iter().map(|p| p.0).collect()
        };
        let (lo, hi) = (mm_key(3, 0), mm_key(6, 0));
        assert_eq!(run(Bound::Included(lo.clone()), Bound::Included(hi.clone())), vec![3, 4, 5, 6]);
        assert_eq!(run(Bound::Included(lo.clone()), Bound::Excluded(hi.clone())), vec![3, 4, 5]);
        assert_eq!(run(Bound::Excluded(lo.clone()), Bound::Included(hi.clone())), vec![4, 5, 6]);
        assert_eq!(run(Bound::Excluded(lo.clone()), Bound::Excluded(hi.clone())), vec![4, 5]);
        assert_eq!(run(Bound::Unbounded, Bound::Excluded(lo)), vec![0, 1, 2]);
        assert_eq!(run(Bound::Excluded(hi), Bound::Unbounded), vec![7, 8, 9]);
    }

    // A prefix scan: first field fixed, second field spanning its full range,
    // including rows equal to either bound. Other prefixes stay out.
    #[test]
    fn test_prefix_scan_between_lower_and_upper_bound_covers_the_prefix_and_nothing_else() {
        let rows: Vec<(i64, i64)> = [4, 5, 6]
            .iter()
            .flat_map(|a| [i64::MIN, -3, 0, 3, i64::MAX].map(move |b| (*a, b)))
            .collect();
        let (db, tid) = mm_db("bounds_scan_prefix.db", &rows);
        let b = ValueItem::Integer(0);
        let start = IndexKey::new_from(&[ValueItem::Integer(5), b.lower_bound()]).unwrap();
        let end = IndexKey::new_from(&[ValueItem::Integer(5), b.upper_bound().unwrap()]).unwrap();
        let got = scanned_pairs(&bounded(&db, tid, Bound::Included(start), Bound::Included(end)));
        let want: Vec<(i64, i64)> = rows.iter().copied().filter(|(a, _)| *a == 5).collect();
        assert_eq!(want.len(), 5);
        assert_eq!(got, want);
    }

    // The old range_scan contract is unchanged: start inclusive, end exclusive.
    #[test]
    fn test_range_scan_is_still_start_inclusive_end_exclusive() {
        let rows = vec![(1, 0), (2, 0), (3, 0)];
        let (db, tid) = mm_db("bounds_scan_legacy.db", &rows);
        let got = scan_range(&db, tid, DBIdType::Rec(mm_key(1, 0)), DBIdType::Rec(mm_key(3, 0)));
        assert_eq!(scanned_pairs(&got), vec![(1, 0), (2, 0)]);
    }

    // A Str field has no upper bound: the scan uses Unbounded for the end
    // and still returns every string, including "" and ones above char::MAX.
    #[test]
    fn test_scan_over_a_string_field_with_lower_bound_and_unbounded_end_returns_every_string() {
        let strs = ["", "\0", "a", "zzz", "\u{10FFFF}", "\u{10FFFF}z"];
        let db = Db::<MemFile>::create("bounds_scan_str.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let mk = |x: &str| {
            IndexKey::new_from(&[ValueItem::Integer(5), ValueItem::Str((x.to_string(), 16))]).unwrap()
        };
        let t = db.begin().unwrap();
        for x in strs {
            db.insert(tid, Tuple::new_with(DBIdType::Rec(mk(x)), b"v", None, None), &t).unwrap();
        }
        db.commit(t).unwrap();
        let probe = mk("q");
        assert_eq!(probe.upper_bound(), None);
        let got = bounded(&db, tid, Bound::Included(probe.lower_bound()), Bound::Unbounded);
        assert_eq!(got.len(), strs.len());
    }

    #[test]
    fn test_range_cursor_reset_replays_the_same_bounds() {
        let rows: Vec<(i64, i64)> = (0..10).map(|a| (a, 0)).collect();
        let (db, tid) = mm_db("bounds_scan_reset.db", &rows);
        let mut cursor = db
            .range_scan_bounds(
                tid,
                Bound::Excluded(DBIdType::Rec(mm_key(2, 0))),
                Bound::Included(DBIdType::Rec(mm_key(5, 0))),
            )
            .unwrap();
        let mut drain = |c: &mut RangeCursor<MemFile>| {
            let mut n = 0;
            while c.next().unwrap().is_some() {
                n += 1;
            }
            n
        };
        assert_eq!(drain(&mut cursor), 3);
        cursor.reset().unwrap();
        assert_eq!(drain(&mut cursor), 3);
    }

    // --- prefix_scan ---

    fn sk(a: i64, x: &str, c: i64) -> IndexKey {
        IndexKey::new_from(&[
            ValueItem::Integer(a),
            ValueItem::Str((x.to_string(), 16)),
            ValueItem::Integer(c),
        ])
        .unwrap()
    }

    fn prefix_db(name: &str, keys: &[IndexKey]) -> (Arc<Db<MemFile>>, TableIdType) {
        let db = Db::<MemFile>::create(name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        for k in keys {
            db.insert(tid, Tuple::new_with(DBIdType::Rec(k.clone()), b"v", None, None), &t).unwrap();
        }
        db.commit(t).unwrap();
        (db, tid)
    }

    fn drain_keys(c: &mut RangeCursor<MemFile>) -> Vec<IndexKey> {
        let mut out = vec![];
        while let Some(t) = c.next().unwrap() {
            match t.id {
                DBIdType::Rec(k) => out.push(k),
                other => panic!("{other:?}"),
            }
        }
        out
    }

    // Exactly the rows with the leading field(s) equal to the prefix — for
    // every prefix length, over a table big enough to span many index
    // leaves, with Str fields (no upper bound) and extreme ints.
    #[test]
    fn test_prefix_scan_returns_exactly_the_rows_under_the_prefix() {
        let strs = ["", "\0", "a", "b", "zz", "\u{10FFFF}", "\u{10FFFF}z"];
        let mut keys = vec![];
        // 1500 leading values (~31k keys): with far fewer, a short-key start
        // happens to work; at this size it provably drops rows (verified by
        // mutating the start to the bare prefix).
        for a in (0..1500).chain([i64::MIN, i64::MAX]) {
            for x in strs {
                for c in [i64::MIN, 0, i64::MAX] {
                    keys.push(sk(a, x, c));
                }
            }
        }
        let (db, tid) = prefix_db("prefix_scan_exact.db", &keys);
        let one = |a: i64| IndexKey::new_from(&[ValueItem::Integer(a)]).unwrap();
        for a in [0, 1, 30, 59, 1000, 1499, i64::MIN, i64::MAX, 5000] {
            let got = drain_keys(&mut db.prefix_scan(tid, one(a)).unwrap());
            let want: Vec<_> = keys.iter().filter(|k| k.values()[0] == ValueItem::Integer(a)).cloned().collect();
            let mut want = want;
            want.sort_by(|x, y| x.partial_cmp(y).unwrap());
            assert_eq!(got, want, "prefix ({a})");
        }
        // Two-field prefixes, including the empty string and the strings
        // above char::MAX that a sentinel end bound would have dropped.
        for x in strs {
            let p = IndexKey::new_from(&[ValueItem::Integer(30), ValueItem::Str((x.to_string(), 16))]).unwrap();
            let got = drain_keys(&mut db.prefix_scan(tid, p).unwrap());
            assert_eq!(got.len(), 3, "prefix (30, {x:?})");
            assert!(got.iter().all(|k| k.values()[0] == ValueItem::Integer(30) && k.values()[1].cmp(&ValueItem::Str((x.to_string(), 0))).is_eq()));
        }
        // The full-length prefix is an exact-key lookup.
        let got = drain_keys(&mut db.prefix_scan(tid, sk(30, "a", 0)).unwrap());
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn test_prefix_scan_with_no_matches_and_the_empty_prefix() {
        let keys: Vec<_> = (0..20).map(|a| sk(a, "x", 0)).collect();
        let (db, tid) = prefix_db("prefix_scan_edges.db", &keys);
        let none = IndexKey::new_from(&[ValueItem::Integer(99)]).unwrap();
        assert!(drain_keys(&mut db.prefix_scan(tid, none).unwrap()).is_empty());
        let all = drain_keys(&mut db.prefix_scan(tid, IndexKey::default()).unwrap());
        assert_eq!(all.len(), 20);
    }

    #[test]
    fn test_prefix_scan_rejects_a_prefix_longer_than_the_key_and_non_record_keys() {
        let (db, tid) = prefix_db("prefix_scan_err.db", &[sk(1, "x", 0)]);
        let long = IndexKey::new_from(&[1, 2, 3, 4].map(ValueItem::Integer)).unwrap();
        assert!(db.prefix_scan(tid, long).is_err());
        let db2 = Db::<MemFile>::create("prefix_scan_int.db").unwrap();
        let tid2 = db2.create_table("rows".to_string()).unwrap();
        let t = db2.begin().unwrap();
        db2.insert(tid2, Tuple::new_with(DBIdType::Int(1), b"v", None, None), &t).unwrap();
        db2.commit(t).unwrap();
        assert!(db2.prefix_scan(tid2, IndexKey::default()).is_err());
    }

    #[test]
    fn test_prefix_scan_on_an_empty_table_is_empty_and_reset_sees_later_rows() {
        let (db, tid) = prefix_db("prefix_scan_empty.db", &[]);
        let p = IndexKey::new_from(&[ValueItem::Integer(5)]).unwrap();
        let mut c = db.prefix_scan(tid, p).unwrap();
        assert!(drain_keys(&mut c).is_empty());
        let t = db.begin().unwrap();
        for (a, x) in [(4, "x"), (5, "x"), (5, "y"), (6, "x")] {
            db.insert(tid, Tuple::new_with(DBIdType::Rec(sk(a, x, 0)), b"v", None, None), &t).unwrap();
        }
        db.commit(t).unwrap();
        // reset keeps the cursor's own (older) snapshot, so it must not
        // error and must re-derive its start from the now non-empty index;
        // a fresh scan sees the two rows.
        c.reset().unwrap();
        let p = IndexKey::new_from(&[ValueItem::Integer(5)]).unwrap();
        assert_eq!(drain_keys(&mut db.prefix_scan(tid, p).unwrap()).len(), 2);
    }
}
