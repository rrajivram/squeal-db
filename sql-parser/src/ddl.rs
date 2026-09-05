//! DDL statements: `CREATE`/`DROP`/`ALTER TABLE`, `CREATE`/`DROP`
//! `DATABASE`/`SCHEMA`, `CREATE`/`DROP INDEX`, `USE`, `TRUNCATE`,
//! `COPY INTO`.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use either::Either;
use macros::SQLParser;

use crate::{
    datatype::DataType,
    expr::Expr,
    ident::{Ident, ObjectName},
    keyword as kw,
    parser::{SQLParser, TokenInput, token},
    query::OrderByItem,
    span::TokenSpan,
    token::{Comma, LeftParenthesis, RightParenthesis, StringStyle, Token},
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
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ShowTables {
    pub show: kw::Show,
    pub table: kw::Tables,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ShowSchemas {
    pub show: kw::Show,
    pub schema: kw::Schemas,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct DescribeTable {
    pub describe: kw::Describe,
    pub table: kw::Table,
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ShowTableIndex {
    pub show: kw::Show,
    pub table: kw::Table,
    pub index: kw::Index,
    pub name: ObjectName,
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

/// `REFERENCES table [(column, ...)]` — a column list, not a single
/// column, symmetric with FOREIGN KEY's own local column list
/// (TableConstraintKind::ForeignKey); a consumer that only supports
/// single-column references (as of this writing, the only kind) rejects
/// a multi-column one itself, the same way it already rejects a
/// multi-column local list.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct ForeignKeyReference {
    pub references: kw::References,
    pub table: ObjectName,
    pub column: Option<(LeftParenthesis, Seq<Ident, Comma>, RightParenthesis)>,
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
    /// `ADD [CONSTRAINT name] <kind>` — reuses TableConstraint/
    /// TableConstraintKind from CREATE TABLE unchanged; a caller only
    /// meaning to support foreign keys (as of this writing, the only kind
    /// ALTER TABLE ADD CONSTRAINT is used for) matches
    /// TableConstraintKind::ForeignKey and rejects the rest itself, the
    /// same way it already has to reject every other AlterTableOp variant
    /// it doesn't handle.
    AddConstraint(kw::Add, TableConstraint),
    DropConstraint(kw::Drop, kw::Constraint, Ident),
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

/// `USE [DATABASE | SCHEMA] name` / `USE db.schema`. `kind` is `None` for
/// the bare form (no equivalent concept in this engine — a caller that only
/// understands DATABASE/SCHEMA is expected to treat `None` as a no-op, the
/// same way sqlparser's other USE targets like CATALOG/WAREHOUSE/ROLE were
/// always silently ignored).
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct UseStatement {
    pub use_token: kw::Use,
    pub kind: Option<Either<kw::Database, kw::Schema>>,
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

/// `COPY INTO <table> FROM @<path>` — loads a CSV file at a literal local
/// filesystem path into `table`. Deliberately minimal, not a real Snowflake
/// `COPY INTO`: no stage credentials/URL/storage-integration resolution, no
/// target column list, no FILES/PATTERN/FILE_FORMAT/COPY options, no
/// unloading (`COPY INTO <location>`), no loading from a query. A consumer
/// wanting the full Snowflake grammar would need to extend this — this
/// shape is only as wide as squeal-sql's own `copy_csv_into` needs.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct CopyInto {
    pub copy: kw::Copy,
    pub into: kw::Into,
    pub table: ObjectName,
    pub from: kw::From,
    pub path: StagePath,
}

/// The `@<path>` half of `COPY INTO ... FROM @<path>` — a literal local
/// filesystem path, `@` already stripped by the lexer (see lexer::stage_path).
#[derive(Debug, Clone, PartialEq)]
pub struct StagePath {
    pub span: TokenSpan,
    pub path: String,
}

impl<'src, I, E, A> SQLParser<'src, I, E, A> for StagePath
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: A) -> impl Parser<'src, I, Self, E> + Clone {
        token("a stage path (@<path>)", |t| match &t.token {
            Token::String {
                raw,
                kind: StringStyle::Unquoted,
            } => Some(StagePath {
                span: t.span,
                path: (*raw).to_string(),
            }),
            _ => None,
        })
    }
}

/// `TRUNCATE [TABLE] name`
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct Truncate {
    pub truncate: kw::Truncate,
    pub table: Option<kw::Table>,
    pub name: ObjectName,
}
