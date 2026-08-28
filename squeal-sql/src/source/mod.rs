use std::{fmt::Debug, sync::Arc};

use store::valueitem::IndexKey;

use crate::{error::SchemaError, table::Field};

pub mod constvalue;
pub mod proj;
pub(crate) mod run;
pub mod table;

pub trait Source: Debug {
    // Takes ownership of `depends`, not a shared reference: a streaming
    // pull cascade means this step's own `next()` calls `depends.next()`
    // (directly, or through whatever transform this step applies) every
    // time it's asked for a row, so it needs exclusive, mutable access —
    // Arc can't give that once anything else is holding a clone, and
    // nothing here actually needs a second owner. A leaf source (no
    // upstream, e.g. a table scan) just ignores `depends`/expects None.
    fn chain(&mut self, depends: Option<Box<dyn Source>>);
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError>;
    fn fields(&self) -> Arc<[Arc<Field>]>;
}
