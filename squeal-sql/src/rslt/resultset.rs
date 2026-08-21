use std::sync::Arc;

use store::db::DBFile;

use crate::stmt::Statement;

pub struct ResultSet<F: DBFile> {
    stmt: Arc<Statement<F>>,
}

pub enum ResultType<F: DBFile> {
    Count(usize),
    Result(ResultSet<F>),
}

impl<F> ResultSet<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(stmt: Arc<Statement<F>>) -> Self {
        Self { stmt }
    }
}
