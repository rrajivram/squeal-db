use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    buffer::PageBuffer,
    db::DBFile,
    error::StoreError,
    page::PageId,
    table::Table,
    tuple::{DBIdType, Tuple},
    txn::{TransactionId, TransactionManager},
};

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, PartialOrd)]
enum Node {
    Key(u64),
    Leaf(PageId),
}

struct BPlusTree<F: DBFile + 'static> {
    table: Arc<Table>,
    buffer: Arc<PageBuffer<F>>,
    txn_mgr: Arc<TransactionManager>,
    first_index_page: PageId,
    first_data_page: PageId,
    key_nodes_per_page: usize,
}

impl<F: DBFile> BPlusTree<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub fn new(table: Arc<Table>, buffer: Arc<PageBuffer<F>>) -> Result<Self, StoreError> {
        //let pg = buffer.get_page(table.first_page)?;
        //let pg = buffer.page_size();
        //let
        // let bt = Self { table, buffer };

        // Ok(bt)
        todo!()
    }

    pub fn insert(&self, tuple: Tuple, txn: TransactionId) -> Result<(), StoreError> {
        let start = self.buffer.get_page(self.first_index_page)?;

        todo!()
    }

    fn insert_recursive(
        &self,
        tuple: Tuple,
        txn: TransactionId,
        start: PageId,
    ) -> Result<(), StoreError> {
        let p = self.buffer.get_page(start)?;
        if p.count()? < self.key_nodes_per_page {
        } else {
            panic!("This should not happen")
        }
        Ok(())
    }
}

impl Eq for Node {}
