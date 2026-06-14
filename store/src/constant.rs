use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::DBSizeType;

pub(crate) const MAX_TABLE_NAME_LEN: usize = 128;
pub(crate) const SYSTEM_TABLE_NAME: &str = "__system.core.tables__";
pub(crate) const SYSTEM_TABLE_PAGE: DBSizeType = 0;
pub(crate) const GENERATOR_TABLE_PAGE: DBSizeType = 1;
pub(crate) const EMPTY_PAGE_TABLE_PAGE: DBSizeType = 2;

#[inline(always)]
pub(crate) fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
