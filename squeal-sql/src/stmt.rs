use std::{fmt::Display, sync::Arc};

use sqlparser::dialect::GenericDialect;
use store::db::DBFile;
use uuid::Uuid;

use crate::{conn::connection::Connection, error::SchemaError, rslt::resultset::ResultType};

pub struct Statement<F: DBFile> {
    id: uuid::Uuid,
    sql: String,
    stmts: Vec<sqlparser::ast::Statement>,
    conn: Arc<Connection<F>>,
    results: Vec<ResultType<F>>,
    current_result: Option<usize>,
}

mod tests;

impl<F> Statement<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(sql: &str, conn: Arc<Connection<F>>) -> Result<Self, SchemaError> {
        let stmts = sqlparser::parser::Parser::parse_sql(&GenericDialect, sql)?;
        Ok(Self {
            id: Uuid::new_v4(),
            sql: sql.to_string(),
            conn,
            stmts,
            results: vec![],
            current_result: None,
        })
    }

    pub fn execute(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }
}

impl<F> Display for Statement<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Statement: id: {}, sql: {}", self.id, self.sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod dummy_tests {

    use super::*;

    fn exec(sql: &str) {
        let stmt = sqlparser::parser::Parser::parse_sql(&GenericDialect, sql).unwrap();
        for s in stmt {
            println!("{:?}", s);
        }
    }

    #[test]
    fn test_cd() {
        exec("create database if not exists test ");
    }

    #[test]
    fn test_ct() {
        exec("create table if not exists test (a int)");
    }
}
