#![allow(dead_code)]

// Page mutation is dominated by copy-on-write clones/drops of BTreeMap-backed
// pages (see the perf investigation this came out of), which is heavy on the
// allocator specifically, not just on total bytes moved. mimalloc measurably
// cuts that cost over the platform default with no logic changes elsewhere —
// set globally (not feature-gated) since any binary linking this crate pays
// the same allocation pattern.
//
// TrackingAllocator (see alloc.rs), not a bare mimalloc::MiMalloc: it
// delegates every call straight to mimalloc (so the perf rationale
// above is unaffected) while also counting bytes/peak/allocation sizes,
// exposed via alloc::stats() — the only place that data can be read
// from, since a process may have at most one #[global_allocator], and
// this crate is the lowest-level one nearly everything else in the
// workspace depends on.
//
// Under the dhat-heap feature, this becomes dhat::Alloc instead — real
// call-site-attributed heap profiling (which allocation call site, not
// just which size bucket) rather than our own counters, at the cost of
// alloc::stats() (which needs a TrackingAllocator specifically) not
// being available in that build. Still exactly one #[global_allocator]
// either way — squeal-cli's own dhat-heap feature enables this one
// rather than declaring a second, conflicting allocator itself.
#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL: alloc::TrackingAllocator = alloc::TrackingAllocator::new();

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

pub mod alloc;
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
pub mod run;
pub mod table;
pub mod tables;
pub mod tuple;
pub mod txn;
mod utils;
pub mod valueitem;
