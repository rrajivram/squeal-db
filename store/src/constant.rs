use crate::db::DBSizeType;

pub(crate) const MAX_TABLE_NAME_LEN: usize = 128;
pub(crate) const SYSTEM_TABLE_NAME: &str = "__system.core.table__";
pub(crate) const SYSTEM_TABLE_PAGE: DBSizeType = 0;
