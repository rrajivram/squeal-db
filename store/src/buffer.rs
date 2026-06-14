use std::{collections::HashMap, sync::RwLock};

use crate::{
    db::{DBFile, DBSizeType, Db},
    page::Page,
};

#[derive(Debug)]
pub(crate) struct PageBuffer<'a, F: DBFile> {
    db: Option<&'a Db<F>>,
    buffer: RwLock<HashMap<DBSizeType, Page>>,
    page_size: DBSizeType,
    max_entries: usize,
}

impl<'a, F: DBFile> PageBuffer<'a, F> {
    pub(crate) fn new(page_size: DBSizeType, max_entries: usize) -> Self {
        Self {
            page_size,
            max_entries,
            db: None,
            buffer: RwLock::new(HashMap::new()),
        }
    }
}
