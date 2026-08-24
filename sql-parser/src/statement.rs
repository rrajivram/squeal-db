//! The top-level [`Statement`] type: one parsed SQL statement.
//!
//! `Statement` is a recursion root (with `Expr` and `Query`) because
//! `EXPLAIN <statement>` nests statements. Its derived parser is emitted as
//! `Statement::body_parser` (`#[sql_parser(body_only)]`) and fed into the
//! `Recursive` handle by `SqlCtx::build`; the trait impl hands out the
//! shared handle.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use macros::SQLParser;

use crate::{
    ddl::{
        AlterTable, CreateDatabase, CreateIndex, CreateTable, DropDatabase, DropIndex,
        DropTable, Truncate, UseStatement,
    },
    dml::{Delete, Insert, Update},
    keyword as kw,
    parser::{SQLParser, SqlCtx, TokenInput},
    query::Query,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
#[sql_parser(body_only)]
pub enum Statement {
    Select(Box<Query>),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    CreateDatabase(CreateDatabase),
    DropTable(DropTable),
    DropIndex(DropIndex),
    DropDatabase(DropDatabase),
    AlterTable(AlterTable),
    Truncate(Truncate),
    Use(UseStatement),
    Explain(kw::Explain, Box<Statement>),
    StartTransaction(StartTransaction),
    Commit(kw::Commit),
    Rollback(kw::Rollback),
}

impl<'src, I, E> SQLParser<'src, I, E, SqlCtx<'src, I, E>> for Statement
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(args: SqlCtx<'src, I, E>) -> impl Parser<'src, I, Self, E> + Clone {
        args.stmt
    }
}

// Convenience: a self-contained statement parser that builds its own
// recursion context.
impl<'src, I, E> SQLParser<'src, I, E> for Statement
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        SqlCtx::build().stmt
    }
}

/// `BEGIN [TRANSACTION]` or `START TRANSACTION`.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum StartTransaction {
    Begin(kw::Begin, Option<kw::Transaction>),
    Start(kw::Start, kw::Transaction),
}
