use std::{collections::HashMap, sync::RwLock};

use crate::{
    db::{DBSizeType, Db},
    page::Page,
};

#[derive(Debug)]
pub(crate) struct PageBuffer<'a> {
    db: Option<&'a Db>,
    buffer: RwLock<HashMap<DBSizeType, Page>>,
    page_size: DBSizeType,
    max_entries: usize,
}

impl<'a> PageBuffer<'a> {
    pub(crate) fn new(page_size: DBSizeType, max_entries: usize) -> Self {
        Self {
            page_size,
            max_entries,
            db: None,
            buffer: RwLock::new(HashMap::new()),
        }
    }
}
