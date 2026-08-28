//! The top-level [`Statement`] type: one parsed SQL statement.
//!
//! `Statement` is a recursion root (with `Expr` and `Query`) because
//! `EXPLAIN <statement>` nests statements. Its derived parser is emitted as
//! `Statement::body_parser` (`#[sql_parser(body_only)]`) and fed into the
//! `Recursive` handle by `SqlCtx::build`; the trait impl hands out the
//! shared handle.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use either::Either;
use macros::SQLParser;

use crate::{
    datatype::DataType,
    ddl::{
        AlterTable, CopyInto, CreateDatabase, CreateIndex, CreateTable, DropDatabase, DropIndex,
        DropTable, ShowSchemas, ShowTables, Truncate, UseStatement,
    },
    dml::{Delete, Insert, Update},
    expr::Expr,
    ident::Ident,
    keyword as kw,
    parser::{SQLParser, SqlCtx, TokenInput},
    query::Query,
    token::{Comma, LeftParenthesis, RightParenthesis},
    utils::Seq,
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
    CopyInto(CopyInto),
    Use(UseStatement),
    Prepare(Prepare),
    Execute(Execute),
    Deallocate(Deallocate),
    Explain(kw::Explain, Box<Statement>),
    StartTransaction(StartTransaction),
    Commit(kw::Commit),
    Rollback(kw::Rollback),
    ShowTables(ShowTables),
    ShowSchemas(ShowSchemas),
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

/// `PREPARE name [(datatypes)] AS <statement>` — the statement body carries
/// the placeholders (`?`, `$n`, `:name`); enumerate them with
/// [`Statement::placeholders`].
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Prepare {
    pub prepare: kw::Prepare,
    pub name: Ident,
    pub datatypes: Option<(LeftParenthesis, Seq<DataType, Comma>, RightParenthesis)>,
    pub as_token: kw::As,
    pub statement: Box<Statement>,
}

/// `EXECUTE name [(args)]`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Execute {
    pub execute: kw::Execute,
    pub name: Ident,
    pub params: Option<(LeftParenthesis, Seq<Expr, Comma>, RightParenthesis)>,
}

/// `DEALLOCATE [PREPARE] name | ALL`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Deallocate {
    pub deallocate: kw::Deallocate,
    pub prepare: Option<kw::Prepare>,
    pub name: Either<kw::All, Ident>,
}
