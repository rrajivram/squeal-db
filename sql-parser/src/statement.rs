use crate::{
    keyword::{Create, Double, Int, Null, Table, Uint64},
    literal::StringLiteral,
};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        create: Create,
        table: Table,
        name: Option<StringLiteral>,
        columns: Option<ColumnDefList>,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnDefList(pub Vec<ColumnDef>);

#[derive(Debug, Clone)]
pub struct ColumnDef {
    name: StringLiteral,
    datatype: DataType,
}

#[derive(Debug, Clone)]
enum DataType {
    Null(Null),
    Int(Int),
    Double(Double),
    Datetime(Uint64),
    Varchar(usize),
}
