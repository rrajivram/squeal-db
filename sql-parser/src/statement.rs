//! The top-level [`Statement`] type: one parsed SQL statement.

use macros::SQLParser;

use crate::{
    ddl::{AlterTable, CreateTable, DropTable},
    dml::{Delete, Insert, Update},
    keyword as kw,
    query::SelectStatement,
};

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum Statement {
    Select(Box<SelectStatement>),
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

/// `BEGIN [TRANSACTION]` or `START TRANSACTION`.
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum StartTransaction {
    Begin(kw::Begin, Option<kw::Transaction>),
    Start(kw::Start, kw::Transaction),
}
