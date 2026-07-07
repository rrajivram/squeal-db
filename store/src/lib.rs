#![allow(dead_code)]
mod arclock;
mod buffer;
mod constant;
pub mod db;
pub mod error;
mod generator;
mod logger;
pub mod memfile;

//

// Core storage library.

// Does the following:

// 1. Create a db

// 2. Open a db

// Db can have 1 to N tables.
// tables are blob stores.Indexed by internal 64 bit id
// P1 indexes can be built on tables - but index values have to be programatically provided.
// operations :
//  opendb
//  closedb
//  createdb
//  create table
//  drop table
//  insert into table ([id][blob])
//  select from table [id[]]
//  delete from table [id[]]
pub mod cursor;
mod page;
pub mod pages;
pub mod table;
pub mod tables;
pub mod tuple;
pub mod txn;
mod utils;
