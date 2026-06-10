#![allow(dead_code)]
mod constant;
pub mod db;
pub mod error;
///
/// Core storage library.
/// Does the following:
/// 1. Create a db
/// 2. Open a db
/// Db can have 1 to N tables.
/// tables are blob stores.Indexed by internal 64 bit id
/// P1 indexes can be built on tables - but index values have to be programatically provided.
/// operations :
///  opendb
///  closedb
///  createdb
///  create table
///  drop table
///  insert into table ([id][blob])
///  select from table [id[]]
///  delete from table [id[]]
mod page;
pub mod table;
pub mod tuple;
