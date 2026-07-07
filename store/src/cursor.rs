use std::sync::Arc;

use crate::{
    db::{DBFile, Db},
    error::StoreError,
    page::{Page, PageTupleIterator},
    table::TableIdType,
    tuple::Tuple,
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
        loop {
            match self.next_tuple()? {
                Some(t) => match self.db.find_last_committed(&t) {
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

    fn scan_all(db: &Arc<Db<MemFile>>, tid: TableIdType) -> Vec<Tuple> {
        let mut cursor = db.table_scan(tid).unwrap();
        let mut out = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            out.push(t);
        }
        out
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
