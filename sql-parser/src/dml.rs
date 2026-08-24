//! DML statements: `INSERT`, `UPDATE`, `DELETE`.

use macros::SQLParser;

use crate::{
    expr::Expr,
    ident::{Ident, ObjectName},
    keyword as kw,
    query::{SelectStatement, WhereClause},
    token::{Comma, Equals, LeftParenthesis, RightParenthesis},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Insert {
    pub insert: kw::Insert,
    pub into: kw::Into,
    pub table: ObjectName,
    pub columns: Option<(LeftParenthesis, Seq<Ident, Comma>, RightParenthesis)>,
    pub source: InsertSource,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum InsertSource {
    /// `VALUES (1, 'a'), (2, 'b')`
    Values(kw::Values, Seq<ValuesRow, Comma>),
    /// `INSERT INTO t SELECT ...`
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ValuesRow(
    pub LeftParenthesis,
    pub Seq<Expr, Comma>,
    pub RightParenthesis,
);

impl ValuesRow {
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> {
        self.1.items()
    }
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Update {
    pub update: kw::Update,
    pub table: ObjectName,
    pub set: kw::Set,
    pub assignments: Seq<Assignment, Comma>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Assignment {
    pub column: ObjectName,
    pub eq: Equals,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Delete {
    pub delete: kw::Delete,
    pub from: kw::From,
    pub table: ObjectName,
    pub where_clause: Option<WhereClause>,
}
