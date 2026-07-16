#![allow(dead_code)]
pub mod datatype;
pub mod error;
pub mod schema;
pub mod table;

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
