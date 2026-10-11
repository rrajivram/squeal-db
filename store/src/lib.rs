#![allow(dead_code)]

// TrackingAllocator (see alloc.rs) delegates every call straight to
// std's System allocator while also counting bytes/peak/allocation
// sizes, exposed via alloc::stats() — the only place that data can be
// read from, since a process may have at most one #[global_allocator],
// and this crate is the lowest-level one nearly everything else in the
// workspace depends on. Plain System (not mimalloc, not dhat, both
// tried and removed) so this also works unmodified under
// `cargo +nightly miri test` — Miri can only interpret actual Rust, not
// arbitrary FFI, and System is the one allocator it knows how to
// intercept directly.
//
// Installed only with the `alloc-tracking` feature (squeal-cli turns it
// on, to print the stats): its counters are shared atomics updated on
// every allocation and free, by every thread, and that bookkeeping alone
// made an aggregate over a million rows 4x slower on four threads than on
// plain System. Without the feature, System allocates and alloc::stats()
// reads zeros — the alloc_proxy_* measurement tests want
// `--features alloc-tracking`.
#[cfg_attr(feature = "alloc-tracking", global_allocator)]
static GLOBAL: alloc::TrackingAllocator = alloc::TrackingAllocator::new();

pub mod alloc;
// STORE_AUDIT.md P2: pub, not just pub(crate), so store/benches/arclock.rs
// (a separate compilation unit, like every criterion bench) can reach it
// directly for contention benchmarking — see that bench's own comment.
pub mod arclock;
mod buffer;
pub mod clock;
pub mod config;
mod constant;
pub mod db;
pub mod error;
pub mod generator;
mod maintenance;
pub mod temppool;
mod systempages;
pub mod logger;
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
pub mod crash_harness;
pub mod cursor;
mod page;
pub mod pages;
pub mod run;
pub mod table;
pub mod tables;
pub mod tuple;
pub mod txn;
mod version;
mod versioned;
mod utils;
pub mod valueitem;
mod wire;
