#![allow(dead_code)]

// Page mutation is dominated by copy-on-write clones/drops of BTreeMap-backed
// pages (see the perf investigation this came out of), which is heavy on the
// allocator specifically, not just on total bytes moved. mimalloc measurably
// cuts that cost over the platform default with no logic changes elsewhere —
// set globally (not feature-gated) since any binary linking this crate pays
// the same allocation pattern.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod arclock;
mod buffer;
mod constant;
pub mod db;
pub mod error;
pub mod generator;
mod logger;
pub mod memfile;
pub mod named_memfile;

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
pub mod valueitem;
