//! `SELECT` and its clauses. Almost everything here is a `#[derive(SQLParser)]`
//! type: a struct parses as its fields in order, an enum as its variants in
//! order, `Option` means the clause may be absent.

use either::Either;
use macros::SQLParser;

use crate::{
    expr::Expr,
    ident::{Ident, ObjectName},
    keyword as kw,
    literal::NumberLiteral,
    token::{Asterisk, Comma, LeftParenthesis, Period, RightParenthesis},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct SelectStatement {
    pub select: kw::Select,
    pub distinct: Option<kw::Distinct>,
    pub projection: Seq<SelectItem, Comma>,
    pub from: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<HavingClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub offset: Option<OffsetClause>,
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
pub struct TableFactor {
    pub name: ObjectName,
    pub alias: Option<Alias>,
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
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct LimitClause {
    pub limit: kw::Limit,
    pub count: NumberLiteral,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct OffsetClause {
    pub offset: kw::Offset,
    pub count: NumberLiteral,
}
