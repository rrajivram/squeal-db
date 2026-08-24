//! The top-level [`Statement`] type: one parsed SQL statement.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use macros::SQLParser;

use crate::{
    ddl::{AlterTable, CreateTable, DropTable},
    dml::{Delete, Insert, Update},
    keyword as kw,
    parser::{SQLParser, SqlCtx, TokenInput},
    query::Query,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum Statement {
    Select(Box<Query>),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable),
    StartTransaction(StartTransaction),
    Commit(kw::Commit),
    Rollback(kw::Rollback),
}

// Convenience: a self-contained statement parser that builds its own
// recursion context. The derived impl above (taking `SqlCtx`) is what other
// parsers compose with.
impl<'src, I, E> SQLParser<'src, I, E> for Statement
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        <Statement as SQLParser<'src, I, E, SqlCtx<'src, I, E>>>::parser(SqlCtx::build())
    }
}

/// `BEGIN [TRANSACTION]` or `START TRANSACTION`.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum StartTransaction {
    Begin(kw::Begin, Option<kw::Transaction>),
    Start(kw::Start, kw::Transaction),
}
