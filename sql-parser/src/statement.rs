extern crate macros;
use macros::SQLParser;

use crate::{
    keyword::{Create, Double, Int, Null, Table, Uint64},
    literal::StringLiteral,
    token::Punctuation,
};

#[derive(Debug, Clone, SQLParser)]
pub enum Statement {
    CreateTable {
        create: Create,
        table: Table,
        name: Option<StringLiteral>,
        columns: Option<ColumnDefList>,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnDefList {
    //    pub left: Punctuation
    pub colums: Vec<ColumnDef>,
}

#[derive(Debug, Clone, SQLParser)]
pub struct ColumnDef {
    name: StringLiteral,
    datatype: DataType,
}

#[derive(Debug, Clone, SQLParser)]
enum DataType {
    Null(Null),
    Int(Int),
    Double(Double),
    Datetime(Uint64),
    Varchar(usize),
}
