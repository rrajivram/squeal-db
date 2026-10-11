//! A MongoDB-style document database on `store`.
pub(crate) mod aggregate;
pub mod client;
pub mod collection;
pub mod error;
pub mod shell;
pub(crate) mod filter;
pub(crate) mod keys;
pub(crate) mod plan;
pub(crate) mod query;
pub(crate) mod update;
pub mod value;

pub use client::{Client, Database, Session};
pub use collection::{Collection, FindOneAndOptions, FindOptions, IndexInfo, IndexOptions, UpdateResult};
pub use error::{Error, Result};
pub use value::{Document, ObjectId, Value};

/// What a database is created and opened with: see [`store::config`].
pub use store::config::{CreateConfig, OpenConfig};

/// What a `Client` (and its databases, collections, sessions) stores to by
/// default: a file, or — on wasm32, which has no files — memory.
#[cfg(not(target_arch = "wasm32"))]
pub type DefaultFile = std::fs::File;
#[cfg(target_arch = "wasm32")]
pub type DefaultFile = store::memfile::MemFile;
