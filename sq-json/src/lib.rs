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
