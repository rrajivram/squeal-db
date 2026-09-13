use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::DBSizeType;

pub(crate) const MAX_TABLE_NAME_LEN: usize = 128;
// STORE_AUDIT.md S7: reserves the whole `__system.` namespace for internal
// use, not just the specific names already in use (SYSTEM_TABLE_NAME etc.
// below) — a user-chosen name under this prefix that doesn't happen to
// collide with an actual internal table name used to succeed by accident.
pub(crate) const RESERVED_TABLE_NAME_PREFIX: &str = "__system.";
pub(crate) const SYSTEM_TABLE_NAME: &str = "__system.core.tables__";
pub(crate) const SYSTEM_TABLE_PAGE: DBSizeType = 0;
pub(crate) const GENERATOR_TABLE_PAGE: DBSizeType = 1;
pub(crate) const GENERATOR_TABLE_NAME: &str = "__system.core.generator__";
pub(crate) const FREE_PAGE_TABLE_PAGE: DBSizeType = 2;
pub(crate) const FREE_PAGE_TABLE_NAME: &str = "__system.core.empty_pages__";
pub(crate) const FIRST_USER_PAGE: DBSizeType = 3;

#[inline(always)]
pub(crate) fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
