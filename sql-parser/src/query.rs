//! Queries. [`Query`] is the full form — optional `WITH` CTEs, a
//! [`SelectCore`], any number of `UNION`/`INTERSECT`/`EXCEPT` compounds, and
//! trailing `ORDER BY`/`LIMIT`/`OFFSET`. Almost everything here is a
//! `#[derive(SQLParser)]` type: a struct parses as its fields in order, an
//! enum as its variants in order, `Option` means the clause may be absent.
//!
//! `Query` is one of the two recursion roots (with `Expr`): subqueries,
//! derived tables, and CTE bodies all refer back to it. Its derived parser is
//! therefore emitted as `Query::body_parser` (`#[sql_parser(body_only)]`) and
//! fed into the `Recursive` handle by [`SqlCtx::build`]; the `SQLParser`
//! trait impl below just hands out that shared handle.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use either::Either;
use macros::SQLParser;

use crate::{
    expr::Expr,
    ident::{Ident, ObjectName},
    keyword as kw,
    parser::{SQLParser, SqlCtx, TokenInput},
    token::{Asterisk, Comma, LeftParenthesis, Period, RightParenthesis},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
#[sql_parser(body_only)]
pub struct Query {
    pub with: Option<With>,
    pub body: SetOperand,
    pub compounds: Vec<CompoundSelect>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub offset: Option<OffsetClause>,
}

impl Query {
    /// The leading `SELECT` block, looking through parenthesized operands —
    /// the common case for consumers of simple, non-compound queries.
    pub fn core(&self) -> &SelectCore {
        self.body.core()
    }
}

impl<'src, I, E> SQLParser<'src, I, E, SqlCtx<'src, I, E>> for Query
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(args: SqlCtx<'src, I, E>) -> impl Parser<'src, I, Self, E> + Clone {
        args.query
    }
}

impl<'src, I, E> SQLParser<'src, I, E> for Query
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        SqlCtx::build().query
    }
}

/// `WITH [RECURSIVE] name [(cols)] AS (query), ...`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct With {
    pub with: kw::With,
    pub recursive: Option<kw::Recursive>,
    pub ctes: Seq<Cte, Comma>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Cte {
    pub name: Ident,
    pub columns: Option<(LeftParenthesis, Seq<Ident, Comma>, RightParenthesis)>,
    pub as_token: kw::As,
    pub lparen: LeftParenthesis,
    pub query: Box<Query>,
    pub rparen: RightParenthesis,
}

/// One operand of a set operation: either a bare `SELECT` block or a
/// parenthesized query (`(SELECT ...) UNION (SELECT ...)`).
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum SetOperand {
    Paren(LeftParenthesis, Box<Query>, RightParenthesis),
    Select(Box<SelectCore>),
}

impl SetOperand {
    pub fn core(&self) -> &SelectCore {
        match self {
            SetOperand::Select(core) => core,
            SetOperand::Paren(_, query, _) => query.core(),
        }
    }
}

/// One `UNION [ALL | DISTINCT] / INTERSECT / EXCEPT` arm.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct CompoundSelect {
    pub op: SetOperator,
    pub operand: SetOperand,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum SetOperator {
    Union(kw::Union, Option<Either<kw::All, kw::Distinct>>),
    Intersect(kw::Intersect),
    Except(kw::Except),
}

/// One `SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ...` block —
/// everything a set operation combines. `ORDER BY`/`LIMIT`/`OFFSET` live on
/// [`Query`], applying to the compound result.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct SelectCore {
    pub select: kw::Select,
    pub distinct: Option<kw::Distinct>,
    pub projection: Seq<SelectItem, Comma>,
    pub from: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<HavingClause>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum SelectItem {
    /// `t.*` — must be tried before the expression branch, which would stop
    /// at the qualifier and leave `.*` unconsumed.
    QualifiedWildcard(ObjectName, Period, Asterisk),
    /// `*`
    Wildcard(Asterisk),
    Expr {
        expr: Expr,
        alias: Option<Alias>,
    },
}

/// `[AS] name`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Alias {
    pub as_token: Option<kw::As>,
    pub name: Ident,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct FromClause {
    pub from: kw::From,
    pub tables: Seq<TableWithJoins, Comma>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum TableFactor {
    /// `(SELECT ...) [AS] alias` — a derived table.
    Derived {
        lparen: LeftParenthesis,
        query: Box<Query>,
        rparen: RightParenthesis,
        alias: Option<Alias>,
    },
    Table {
        name: ObjectName,
        alias: Option<Alias>,
    },
}

impl TableFactor {
    pub fn alias(&self) -> Option<&Alias> {
        match self {
            TableFactor::Derived { alias, .. } | TableFactor::Table { alias, .. } => {
                alias.as_ref()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Join {
    pub operator: JoinOperator,
    pub relation: TableFactor,
    pub constraint: Option<JoinConstraint>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum JoinOperator {
    Inner(kw::Inner, kw::Join),
    LeftOuter(kw::Left, Option<kw::Outer>, kw::Join),
    RightOuter(kw::Right, Option<kw::Outer>, kw::Join),
    FullOuter(kw::Full, Option<kw::Outer>, kw::Join),
    Cross(kw::Cross, kw::Join),
    Plain(kw::Join),
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum JoinConstraint {
    On(kw::On, Expr),
    Using(
        kw::Using,
        LeftParenthesis,
        Seq<Ident, Comma>,
        RightParenthesis,
    ),
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct WhereClause {
    pub where_token: kw::Where,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct GroupByClause {
    pub group: kw::Group,
    pub by: kw::By,
    pub exprs: Seq<Expr, Comma>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct HavingClause {
    pub having: kw::Having,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct OrderByClause {
    pub order: kw::Order,
    pub by: kw::By,
    pub items: Seq<OrderByItem, Comma>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: Option<Either<kw::Asc, kw::Desc>>,
    /// `NULLS FIRST` / `NULLS LAST`
    pub nulls: Option<(kw::Nulls, Either<kw::First, kw::Last>)>,
}

/// `LIMIT <expr>` — an expression, not just a literal, so prepared
/// statements can write `LIMIT ?`.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct LimitClause {
    pub limit: kw::Limit,
    pub count: Expr,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct OffsetClause {
    pub offset: kw::Offset,
    pub count: Expr,
}

impl LimitClause {
    /// The count when it is a plain integer literal.
    pub fn count_i64(&self) -> Option<i64> {
        literal_i64(&self.count)
    }
}

impl OffsetClause {
    /// The count when it is a plain integer literal.
    pub fn count_i64(&self) -> Option<i64> {
        literal_i64(&self.count)
    }
}

fn literal_i64(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(crate::literal::Literal::Number(n)) => n.as_i64(),
        _ => None,
    }
}
