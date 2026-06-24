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

const INNER_NODE: usize = 0b100000000;
const LEAF_NODE: usize = INNER_NODE << 1;

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
            data_page_id = page.get_next_page();
            if data_page_id.is_valid_next_page() {
                page = self.buffer.get_page(data_page_id)?;
            } else {
                let last_page = data_page_id;
                data_page_id = self.buffer.alloc_page(false)?;
                let handle = self.buffer.get_page_mut(last_page)?;
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
        if page.count()? == self.nodes_per_page {
            self.split_root_page(
                self.buffer.get_page_mut(self.first_index_page)?,
                txn.clone(),
            )?;
        }

        self.insert_recursive(id_tuple, txn, self.first_index_page)?;
        Ok(())
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
                if let Some((id, page_id)) = self.split_if_needed(p, txn_id.clone())? {
                    let page = Arc::make_mut(&mut handle.page);
                    page.add_tuple(Tuple::new_with(
                        id.clone(),
                        &to_allocvec(&Node::Inner(page_id))?,
                        Some(txn_id.clone()),
                        None,
                    ))?;
                    if tuple.id < id {
                        return Ok(self.insert_recursive(tuple, txn_id, page_id)?);
                    }
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
        let mut values = current_handle.page.iter().collect::<Vec<_>>();

        let mid_val = values.swap_remove(values.len() / 2);
        let new_page_id = self.buffer.alloc_page(false)?;
        let mut new_handle = self.buffer.get_page_mut(new_page_id)?;
        if current_handle.page.is_flag_set(INNER_NODE) {
            new_handle.page.set_page_flags(INNER_NODE)?;
        } else {
            new_handle.page.set_page_flags(LEAF_NODE)?;
        }

        let mut iter = values.chunks(values.len() / 2);
        let current_vals = iter.next().map(|s| Vec::from_iter(s)).unwrap();
        let new_vals = iter.next().map(|s| Vec::from_iter(s)).unwrap();
        let current_page = Arc::make_mut(&mut current_handle.page);
        let new_page = Arc::make_mut(&mut new_handle.page);
        current_page.clear()?;
        current_vals
            .iter()
            .try_for_each(|t| current_page.add_tuple((*t).clone()))?;
        new_vals
            .iter()
            .try_for_each(|t| new_page.add_tuple((*t).clone()))?;
        self.buffer.write_locked_page(current_handle)?;
        self.buffer.write_locked_page(new_handle)?;
        Ok((mid_val.id, new_page_id))
    }

    fn update_root_page(
        &self,
        id: DBIdType,
        left_page: PageId,
        right_page: PageId,
        txn_id: TransactionId,
    ) -> Result<(), StoreError> {
        let mut handle = self.buffer.get_page_mut(self.first_index_page)?;
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
        let mut values = handle.page.iter().collect::<Vec<_>>();
        let flags = if handle.page.is_flag_set(INNER_NODE) {
            INNER_NODE
        } else {
            LEAF_NODE
        };

        let mid_val = values.swap_remove(values.len() / 2);
        let left_page_id = self.buffer.alloc_page(false)?;
        let right_page_id = self.buffer.alloc_page(false)?;
        let mut left_handle = self.buffer.get_page_mut(left_page_id)?;
        let mut right_handle = self.buffer.get_page_mut(right_page_id)?;
        left_handle.page.set_page_flags(flags)?;
        right_handle.page.set_page_flags(flags)?;
        let mut iter = values.chunks(values.len() / 2);
        let left_vals = iter.next().map(|s| Vec::from_iter(s)).unwrap();
        let right_vals = iter.next().map(|s| Vec::from_iter(s)).unwrap();
        let left_page = Arc::make_mut(&mut left_handle.page);
        let right_page = Arc::make_mut(&mut right_handle.page);
        left_vals
            .iter()
            .try_for_each(|t| left_page.add_tuple((*t).clone()))?;
        right_vals
            .iter()
            .try_for_each(|t| right_page.add_tuple((*t).clone()))?;
        self.buffer.write_locked_page(left_handle)?;
        self.buffer.write_locked_page(right_handle)?;
        self.update_root_page(mid_val.id.clone(), left_page_id, right_page_id, txn_id)?;
        Ok((mid_val.id, left_page_id, right_page_id))
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
