//! DDL statements: `CREATE`/`DROP`/`ALTER TABLE`, `CREATE`/`DROP`
//! `DATABASE`/`SCHEMA`, `CREATE`/`DROP INDEX`, `USE`, `TRUNCATE`.

use either::Either;
use macros::SQLParser;

use crate::{
    datatype::DataType,
    expr::Expr,
    ident::{Ident, ObjectName},
    keyword as kw,
    query::OrderByItem,
    token::{Comma, LeftParenthesis, RightParenthesis},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct CreateTable {
    pub create: kw::Create,
    pub table: kw::Table,
    pub if_not_exists: Option<(kw::If, kw::Not, kw::Exists)>,
    pub name: ObjectName,
    pub lparen: LeftParenthesis,
    pub elements: Seq<TableElement, Comma>,
    pub rparen: RightParenthesis,
}

impl CreateTable {
    pub fn columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.elements.items().filter_map(|e| match e {
            TableElement::Column(c) => Some(c),
            TableElement::Constraint(_) => None,
        })
    }

    pub fn constraints(&self) -> impl Iterator<Item = &TableConstraint> {
        self.elements.items().filter_map(|e| match e {
            TableElement::Constraint(c) => Some(c),
            TableElement::Column(_) => None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum TableElement {
    Constraint(TableConstraint),
    Column(ColumnDef),
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    pub options: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum ColumnOption {
    NotNull(kw::Not, kw::Null),
    Null(kw::Null),
    PrimaryKey(kw::Primary, kw::Key),
    Unique(kw::Unique),
    Default(kw::Default, Expr),
    References(ForeignKeyReference),
    Check(kw::Check, LeftParenthesis, Expr, RightParenthesis),
}

/// `REFERENCES table [(column)]`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ForeignKeyReference {
    pub references: kw::References,
    pub table: ObjectName,
    pub column: Option<(LeftParenthesis, Ident, RightParenthesis)>,
}

/// `[CONSTRAINT name] <kind>`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct TableConstraint {
    pub name: Option<(kw::Constraint, Ident)>,
    pub kind: TableConstraintKind,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum TableConstraintKind {
    PrimaryKey(
        kw::Primary,
        kw::Key,
        LeftParenthesis,
        Seq<Ident, Comma>,
        RightParenthesis,
    ),
    Unique(
        kw::Unique,
        LeftParenthesis,
        Seq<Ident, Comma>,
        RightParenthesis,
    ),
    ForeignKey(
        kw::Foreign,
        kw::Key,
        LeftParenthesis,
        Seq<Ident, Comma>,
        RightParenthesis,
        ForeignKeyReference,
    ),
    Check(kw::Check, LeftParenthesis, Expr, RightParenthesis),
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct DropTable {
    pub drop: kw::Drop,
    pub table: kw::Table,
    pub if_exists: Option<(kw::If, kw::Exists)>,
    pub names: Seq<ObjectName, Comma>,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct AlterTable {
    pub alter: kw::Alter,
    pub table: kw::Table,
    pub name: ObjectName,
    pub operation: AlterTableOp,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum AlterTableOp {
    AddColumn(kw::Add, Option<kw::Column>, ColumnDef),
    DropColumn(kw::Drop, Option<kw::Column>, Ident),
    RenameTo(kw::Rename, kw::To, ObjectName),
    RenameColumn(kw::Rename, Option<kw::Column>, Ident, kw::To, Ident),
}

/// `CREATE DATABASE|SCHEMA [IF NOT EXISTS] name`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct CreateDatabase {
    pub create: kw::Create,
    pub kind: Either<kw::Database, kw::Schema>,
    pub if_not_exists: Option<(kw::If, kw::Not, kw::Exists)>,
    pub name: Ident,
}

/// `DROP DATABASE|SCHEMA [IF EXISTS] name [CASCADE | RESTRICT]`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct DropDatabase {
    pub drop: kw::Drop,
    pub kind: Either<kw::Database, kw::Schema>,
    pub if_exists: Option<(kw::If, kw::Exists)>,
    pub name: Ident,
    pub behavior: Option<Either<kw::Cascade, kw::Restrict>>,
}

/// `USE name` / `USE db.schema`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct UseStatement {
    pub use_token: kw::Use,
    pub name: ObjectName,
}

/// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (col [ASC|DESC], ...)`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct CreateIndex {
    pub create: kw::Create,
    pub unique: Option<kw::Unique>,
    pub index: kw::Index,
    pub if_not_exists: Option<(kw::If, kw::Not, kw::Exists)>,
    pub name: Ident,
    pub on: kw::On,
    pub table: ObjectName,
    pub lparen: LeftParenthesis,
    pub columns: Seq<OrderByItem, Comma>,
    pub rparen: RightParenthesis,
}

/// `DROP INDEX [IF EXISTS] name`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct DropIndex {
    pub drop: kw::Drop,
    pub index: kw::Index,
    pub if_exists: Option<(kw::If, kw::Exists)>,
    pub name: ObjectName,
}

/// `TRUNCATE [TABLE] name`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Truncate {
    pub truncate: kw::Truncate,
    pub table: Option<kw::Table>,
    pub name: ObjectName,
}
