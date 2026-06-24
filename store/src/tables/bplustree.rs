use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    buffer::{PageBuffer, WritePageHandle},
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{Page, PageId},
    table::Table,
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

struct BPlusTree<F: DBFile + 'static> {
    table: Arc<Table>,
    buffer: Arc<PageBuffer<F>>,
    txn_mgr: Arc<TransactionManager>,
    first_index_page: PageId,
    first_data_page: PageId,
    nodes_per_page: usize,
}

impl<F: DBFile> BPlusTree<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub fn new(
        table: Arc<Table>,
        buffer: Arc<PageBuffer<F>>,
        txn_mgr: Arc<TransactionManager>,
    ) -> Result<Self, StoreError> {
        let pg = buffer.page_size();
        let size = Tuple::new_with(
            0.into(),
            &to_allocvec(&Node::Inner(0.into()))?,
            Some(0.into()),
            None,
        )
        .size();
        let count = pg / size;

        if count < 2 {
            return Err(StoreError::UnknownError(
                "Unable to fit index : count = {count}, size = {size}".into(),
            ));
        }
        let first_index_page = buffer.alloc_page(false)?;
        let first_data_page = buffer.alloc_page(false)?;
        let index_page = Page::new_indexed(pg, size as usize);
        index_page.set_page_flags(LEAF_NODE)?;
        let mut handle = buffer.get_page_mut(first_index_page)?;
        handle.page = Arc::new(index_page);
        buffer.write_locked_page(handle)?;
        Ok(Self {
            table,
            buffer,
            txn_mgr,
            first_data_page,
            first_index_page,
            nodes_per_page: count as usize,
        })
    }

    pub fn insert(&self, tuple: Tuple, txn: TransactionId) -> Result<(), StoreError> {
        let mut page = self.buffer.get_page(self.first_data_page)?;
        let mut data_page_id = self.first_data_page;
        let tuple_id = tuple.id.clone();
        loop {
            if page.can_store(&tuple) {
                self.write_page(self.buffer.get_page_mut(data_page_id)?, tuple)?;
                break;
            }
            let next_page_id = page.get_next_page();
            if next_page_id.is_valid_next_page() {
                data_page_id = next_page_id;
                page = self.buffer.get_page(data_page_id)?;
            } else {
                let current_page_id = data_page_id;
                data_page_id = self.buffer.alloc_page(false)?;
                let handle = self.buffer.get_page_mut(current_page_id)?;
                handle.page.set_next_page(data_page_id)?;
                self.buffer.write_locked_page(handle)?;
                page = self.buffer.get_page(data_page_id)?;
            }
        }
        let id_tuple = Tuple::new_with(
            tuple_id,
            &to_allocvec(&Node::Leaf(data_page_id))?,
            Some(txn.clone()),
            None,
        );
        let page = self.buffer.get_page(self.first_index_page)?;
        if page.count()? == self.nodes_per_page - 1 {
            self.split_root_page(
                self.buffer.get_page_mut(self.first_index_page)?,
                txn.clone(),
            )?;
        }

        self.insert_recursive(id_tuple, txn, self.first_index_page)?;
        Ok(())
    }

    pub fn find(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self
            .find_page(id.clone(), self.first_index_page)?
            .map(|p| self.buffer.get_page(p).and_then(|p| p.get(id)))
            .transpose()?
            .flatten())
    }

    fn find_page(&self, id: DBIdType, start: PageId) -> Result<Option<PageId>, StoreError> {
        let page = self.buffer.get_page(start)?;
        if page.is_flag_set(INNER_NODE) {
            let rows_iter = page.iter();
            for row in rows_iter {
                if id < row.id {
                    let page_id = from_bytes::<Node>(&row.data)?;
                    if let Node::Inner(page_num) = page_id {
                        return Ok(self.find_page(id, page_num)?);
                    } else {
                        panic!("Expected Inner. Found leaf ! {:?}", row.id);
                    }
                }
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

    fn insert_recursive(
        &self,
        tuple: Tuple,
        txn_id: TransactionId,
        start: PageId,
    ) -> Result<(), StoreError> {
        let mut handle = self.buffer.get_page_mut(start)?;
        if handle.page.is_flag_set(LEAF_NODE) {
            let count = handle.page.count()?;
            if count == self.nodes_per_page - 1 {
                panic!("Count means split not done! {count}, {:?}", start);
            } else if handle.page.count()? < self.nodes_per_page && handle.page.can_store(&tuple) {
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
            let mut rows = handle.page.iter();
            let mut row_id = rows.next().unwrap();
            if tuple.id >= row_id.id {
                for row in rows {
                    if tuple.id < row.id {
                        row_id = row;
                    } else {
                        break;
                    }
                }
            }
            let node = from_bytes::<Node>(&row_id.data)?;
            if let Node::Inner(p) = node {
                if let Some((separator, sibling)) = self.split_if_needed(p, txn_id.clone())? {
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
                    if tuple.id < separator {
                        return Ok(self.insert_recursive(tuple, txn_id, p)?);
                    }
                    return Ok(self.insert_recursive(tuple, txn_id, sibling)?);
                }
                return Ok(self.insert_recursive(tuple, txn_id, p)?);
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
        txn_id: TransactionId,
    ) -> Result<Option<(DBIdType, PageId)>, StoreError> {
        let handle = self.buffer.get_page_mut(page_id)?;
        if handle.page.count()? == self.nodes_per_page - 1 {
            if self.is_root_page(page_id) {
                panic!("Trying to split root in the wrong place");
            } else {
                Ok(Some(self.split_non_root_page(handle, txn_id)?))
            }
        } else {
            Ok(None)
        }
    }

    fn is_root_page(&self, page_id: PageId) -> bool {
        self.first_index_page == page_id
    }

    fn split_non_root_page(
        &self,
        handle: WritePageHandle,
        _txn_id: TransactionId,
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
        let new_page_id = self.buffer.alloc_page(false)?;
        let mut new_handle = self.buffer.get_page_mut(new_page_id)?;
        if current_handle.page.is_flag_set(INNER_NODE) {
            new_handle.page.set_page_flags(INNER_NODE)?;
        } else {
            new_handle.page.set_page_flags(LEAF_NODE)?;
        }

        let current_page = Arc::make_mut(&mut current_handle.page);
        let new_page = Arc::make_mut(&mut new_handle.page);
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
        let mut handle = self.buffer.get_page_mut(self.first_index_page)?;
        handle.page.clear_page_flag(LEAF_NODE)?;
        handle.page.set_page_flags(INNER_NODE)?;
        let page = Arc::make_mut(&mut handle.page);
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
    ) -> Result<(DBIdType, PageId, PageId), StoreError> {
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
        left_handle.page.set_page_flags(flags)?;
        right_handle.page.set_page_flags(flags)?;
        let left_page = Arc::make_mut(&mut left_handle.page);
        let right_page = Arc::make_mut(&mut right_handle.page);
        left_vals
            .iter()
            .try_for_each(|t| left_page.add_tuple(t.clone()))?;
        right_vals
            .iter()
            .try_for_each(|t| right_page.add_tuple(t.clone()))?;
        self.buffer.write_locked_page(left_handle)?;
        self.buffer.write_locked_page(right_handle)?;
        self.update_root_page(separator_id.clone(), left_page_id, right_page_id, txn_id)?;
        Ok((separator_id, left_page_id, right_page_id))
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

    use super::{BPlusTree, INNER_NODE, LEAF_NODE, Node};
    use crate::{
        buffer::PageBuffer,
        constant::FIRST_USER_PAGE,
        db::Header,
        error::StoreError,
        generator::Generator,
        memfile::MemFile,
        page::Page,
        table::{Table, TableType},
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

    fn make_table() -> Arc<Table> {
        Arc::new(Table::new_with_id(1, "t".into(), TableType::Table, None).unwrap())
    }

    fn make_tree(page_size: u64) -> BPlusTree<MemFile> {
        let buf = make_buffer(page_size);
        BPlusTree::new(make_table(), buf, make_txn_mgr()).unwrap()
    }

    fn txn() -> TransactionId {
        TransactionId::new(1)
    }

    // Large page — no splits during basic tests.
    const BIG: u64 = 8192;

    #[test]
    fn test_insert_single_data_and_index() {
        let tree = make_tree(BIG);
        tree.insert(Tuple::new(1, b"hello"), txn()).unwrap();

        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 1);
        assert_eq!(dp.get(DBIdType::Int(1)).unwrap().unwrap().data, b"hello");

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
        assert_eq!(ip.count().unwrap(), 1);
    }

    #[test]
    fn test_insert_multiple_same_page() {
        let tree = make_tree(BIG);
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, b"val"), txn()).unwrap();
        }
        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 5);
        for i in 1u64..=5 {
            assert!(dp.contains(DBIdType::Int(i)).unwrap(), "missing id {i}");
        }
        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
        assert_eq!(ip.count().unwrap(), 5);
    }

    #[test]
    fn test_insert_out_of_order() {
        let tree = make_tree(BIG);
        for &id in &[5u64, 3, 8, 1, 7, 2, 6, 4] {
            tree.insert(Tuple::new(id, b"d"), txn()).unwrap();
        }
        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 8);
        for id in 1u64..=8 {
            assert!(dp.contains(DBIdType::Int(id)).unwrap(), "missing id {id}");
        }
    }

    #[test]
    fn test_data_page_overflow_links_next_page() {
        // BIG (8192 B) page gives nodes_per_page >> 3, so no index split fires.
        // Use 3 KB data payloads: each tuple occupies ~3100 B in the data page.
        // data_size ≈ 8192 - overhead (~56) ≈ 8136 B → fits 2 before overflow.
        let tree = make_tree(BIG);
        let large = vec![b'x'; 3000];

        tree.insert(Tuple::new(1, &large), txn()).unwrap();
        tree.insert(Tuple::new(2, &large), txn()).unwrap();

        let dp1 = tree.buffer.get_page(tree.first_data_page).unwrap();
        assert_eq!(
            dp1.count().unwrap(),
            2,
            "first data page should hold 2 large tuples"
        );

        tree.insert(Tuple::new(3, &large), txn()).unwrap();

        let dp1 = tree.buffer.get_page(tree.first_data_page).unwrap();
        let next = dp1.get_next_page();
        assert!(
            next.is_valid_next_page(),
            "first data page must link to a second page"
        );

        let dp2 = tree.buffer.get_page(next).unwrap();
        assert_eq!(dp2.count().unwrap(), 1);
        assert!(dp2.contains(DBIdType::Int(3)).unwrap());
    }

    #[test]
    fn test_root_splits_into_inner_node() {
        use postcard::to_allocvec;
        // Compute the size of one index entry (same formula as BPlusTree::new).
        let node_bytes = to_allocvec(&Node::Inner(FIRST_USER_PAGE.into())).unwrap();
        let node_tuple_sz = Tuple::new_with(0.into(), &node_bytes, Some(txn()), None).size();
        // nodes_per_page = page_size / node_tuple_sz.
        // Choose page_size so nodes_per_page = 4; split fires after 3 inserts.
        let probe = 4096u64;
        let page_size = node_tuple_sz * 4 + page_overhead(probe) + 1;
        let tree = make_tree(page_size);

        // nodes_per_page = 4; split fires after 3 index entries, so 5 inserts exercises post-split.
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, b"x"), txn()).unwrap();
        }

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
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
        use postcard::to_allocvec;
        let node_bytes = to_allocvec(&Node::Inner(FIRST_USER_PAGE.into())).unwrap();
        let node_tuple_sz = Tuple::new_with(0.into(), &node_bytes, Some(txn()), None).size();
        let page_size = node_tuple_sz * 4 + page_overhead(4096) + 1;
        let tree = make_tree(page_size);

        for i in 1u64..=4 {
            tree.insert(Tuple::new(i, b"y"), txn()).unwrap();
        }

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
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

        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
        assert_eq!(dp.count().unwrap(), 2);
        assert_eq!(dp.get(id1.clone()).unwrap().unwrap().data, b"a-data");
        assert_eq!(dp.get(id2.clone()).unwrap().unwrap().data, b"b-data");

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
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

        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
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
        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
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

        let dp = tree.buffer.get_page(tree.first_data_page).unwrap();
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
        use postcard::to_allocvec;
        let node_bytes = to_allocvec(&Node::Inner(FIRST_USER_PAGE.into())).unwrap();
        let node_tuple_sz = Tuple::new_with(0.into(), &node_bytes, Some(txn()), None).size();
        let page_size = node_tuple_sz * 4 + page_overhead(4096) + 1;
        let tree = make_tree(page_size);

        // nodes_per_page = 4; split fires after 3 inserts (see test_root_splits_into_inner_node).
        for i in 1u64..=5 {
            tree.insert(Tuple::new(i, format!("v{i}").as_bytes()), txn())
                .unwrap();
        }

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
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
        use postcard::to_allocvec;
        // Same sizing as test_find_after_root_split_left_and_right_subtrees: the
        // template tuple uses an Int id, but Tuple::size() only counts the `data`
        // payload length plus the fixed in-memory Tuple size, not the id's own
        // heap bytes — so this sizing is valid for Vec/string ids too.
        let node_bytes = to_allocvec(&Node::Inner(FIRST_USER_PAGE.into())).unwrap();
        let node_tuple_sz = Tuple::new_with(0.into(), &node_bytes, Some(txn()), None).size();
        let page_size = node_tuple_sz * 4 + page_overhead(4096) + 1;
        let tree = make_tree(page_size);

        // nodes_per_page = 4; 5 string-keyed inserts exercise both a root split
        // and a child split, exactly the scenario broken before DBIdType::Ord
        // was made hash-consistent with AnyTuplePage's iteration order.
        let keys = ["alpha", "bravo", "charlie", "delta", "echo"];
        for k in &keys {
            let id = DBIdType::from(k.to_string());
            tree.insert(Tuple::new_with(id, k.as_bytes(), None, None), txn())
                .unwrap();
        }

        let ip = tree.buffer.get_page(tree.first_index_page).unwrap();
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
}
