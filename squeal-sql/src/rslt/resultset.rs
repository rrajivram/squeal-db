use std::sync::Arc;

use store::db::DBFile;

use crate::stmt::Statement;

pub struct ResultSet<F: DBFile> {
    stmt: Arc<Statement<F>>,
}

// #[derive(Clone)] would add a spurious `F: Clone` bound — Arc<T> is
// always Clone regardless of T, so this doesn't actually need one.
impl<F: DBFile> Clone for ResultSet<F> {
    fn clone(&self) -> Self {
        Self {
            stmt: self.stmt.clone(),
        }
    }
}

pub enum ResultType<F: DBFile> {
    // Rows affected — for future DML (INSERT/UPDATE/DELETE); nothing
    // produces this yet.
    Count(usize),
    // A query result set — for future SELECT support; nothing produces
    // this yet.
    Result(ResultSet<F>),
    // A human-readable outcome for statements with no row count or rows
    // of their own (CREATE TABLE/DATABASE/SCHEMA, USE DATABASE/SCHEMA,
    // ...) — e.g. "Table 'users' created", "Using database 'test'".
    ResultString(String),
}

// Same reasoning as ResultSet's manual Clone impl above.
impl<F: DBFile> Clone for ResultType<F> {
    fn clone(&self) -> Self {
        match self {
            Self::Count(n) => Self::Count(*n),
            Self::Result(r) => Self::Result(r.clone()),
            Self::ResultString(s) => Self::ResultString(s.clone()),
        }
    }
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
