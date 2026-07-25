#![allow(dead_code)]

extern crate macros;

use either::Either;
use macros::SQLParser;
pub(crate) mod constant;
pub mod datatype;
pub mod error;
pub mod schema;
pub mod table;

struct Hey {
    a: String,
    b: Option<String>,
    c: Option<(i32, i32)>,
    d: (i32, i32, i32),
    e: Either<u32, u32>,
    f: Option<Either<char, char>>,
    g: Vec<f64>,
    h: Vec<(i32, i32)>,
}

#[derive(SQLParser)]
enum Hello {
    Nothing,
    Something(u64),
    Structy { id: i32, name: String },
}

#[cfg(test)]
mod tests {
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    #[test]
    fn it_works() {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(
            &dialect,
            "create table t1 (id integer  primary key autoincrement, name varchar(10))",
        )
        .unwrap();
        println!("{:?}", ast);
    }
}
