use std::sync::Arc;

use crate::{
    db::{DBFile, Db},
    error::StoreError,
    page::{Page, PageTupleIterator},
    table::TableIdType,
    tables::bplustree::BPlusTree,
    tuple::{DBIdType, Tuple},
    txn::Transaction,
};

pub trait Cursor {
    type Item;
    fn next(&mut self) -> Result<Option<Self::Item>, StoreError>;
}

pub struct TableCursor<F: DBFile + 'static> {
    db: Arc<Db<F>>,
    table: TableIdType,
    current_page: Arc<Page>,
    current_iter: PageTupleIterator,
    transaction: Transaction,
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
    start: DBIdType,
    end: DBIdType,
    // Set once an index entry >= end is seen: ascending leaf-chain order
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
        transaction: Option<Transaction>,
    ) -> Result<Self, StoreError> {
        let transaction = transaction.unwrap_or(db.begin()?);
        let current_page = db
            .table_by_id(table)?
            .next_data_page(None)?
            .ok_or(StoreError::UnknownError("No data page found".into()))?;
        let current_iter = current_page.iter();
        Ok(Self {
            db,
            table,
            current_iter,
            current_page,
            transaction,
        })
    }

    fn next_tuple(&mut self) -> Result<Option<Tuple>, StoreError> {
        let n = self.current_iter.next();
        if n.is_some() {
            Ok(n)
        } else {
            let new_page = self
                .db
                .table_by_id(self.table)?
                .next_data_page(Some(Arc::clone(&self.current_page)))?;
            if let Some(new_page) = new_page {
                self.current_page = new_page;
                self.current_iter = self.current_page.iter();
                Ok(self.current_iter.next())
            } else {
                Ok(None)
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
        start: DBIdType,
        end: DBIdType,
    ) -> Result<Self, StoreError> {
        let transaction = transaction.unwrap_or(db.begin()?);
        // Positional: finds the leaf that would hold `start` whether or
        // not `start` actually exists as a key (unlike the old
        // find_first_page, which did an exact index lookup and errored
        // with KeyNotFound if `start` wasn't a real row).
        let current_leaf = db.table_by_id(table)?.find_leaf_page(&start)?;
        let current_iter = current_leaf.iter();
        Ok(Self {
            db,
            table,
            current_iter,
            current_leaf,
            transaction,
            start,
            end,
            done: false,
        })
    }

    // Advances to the next INDEX entry (an (id, Node::Leaf(data_page_id))
    // routing tuple, not a real row) in ascending key order, walking
    // leaf-to-leaf via the leaf sibling chain once the current leaf is
    // exhausted. Mirrors TableCursor::next_tuple's pattern, but over index
    // leaves instead of data pages.
    fn next_index_entry(&mut self, table: &BPlusTree<F>) -> Result<Option<Tuple>, StoreError> {
        let n = self.current_iter.next();
        if n.is_some() {
            Ok(n)
        } else {
            let next_leaf = table.next_leaf_page(&self.current_leaf)?;
            if let Some(next_leaf) = next_leaf {
                self.current_leaf = next_leaf;
                self.current_iter = self.current_leaf.iter();
                Ok(self.current_iter.next())
            } else {
                Ok(None)
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
        loop {
            match self.next_index_entry(&table)? {
                Some(entry) => {
                    // The leaf containing `start` generally holds entries
                    // both below and at/above it — skip the ones below.
                    if entry.id < self.start {
                        continue;
                    }
                    // Ascending leaf-chain order guarantees everything
                    // from here on is also >= end, so this is a real
                    // early-termination, not just a filter.
                    if entry.id >= self.end {
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
                    match self.db.find_visible_to(&tuple, &reader) {
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
        loop {
            match self.next_tuple()? {
                Some(t) => match self.db.find_visible_to(&t, &reader) {
                    Some(committed) if !committed.is_tombstoned() => {
                        return Ok(Some(committed.into_owned()));
                    }
                    _ => continue,
                },
                None => return Ok(None),
            }
        }
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
        let db = Db::<MemFile>::create_with_page_size("range_multi_leaf.db", 256).unwrap();
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
    // second thread against retry_on_contention's timing.
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
}
