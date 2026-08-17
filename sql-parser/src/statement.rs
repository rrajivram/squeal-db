extern crate macros;
use macros::SQLParser;

use crate::{
    datatype::DataType,
    keyword::{Create, Table},
    literal::StringLiteral,
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
