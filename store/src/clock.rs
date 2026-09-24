//! `std::time::Instant`/`SystemTime` type-check on every target, including
//! `wasm32-unknown-unknown`, but their `now()` on that target panics at
//! runtime ("time not implemented on this platform") — there is no OS
//! clock in a browser sandbox, only whatever the JS host exposes
//! (`Performance.now()`/`Date.now()`). `web-time` is a drop-in
//! replacement with the identical API: on every other target it just
//! re-exports std's own types (zero cost, zero behavior change), and on
//! `wasm32-unknown-unknown` it backs them with those JS calls instead of
//! panicking.
//!
//! This module is the ONE place that needs to know that distinction —
//! everywhere else in the workspace (query-stats timing, the WAL clock,
//! the maintenance thread's own interval, ...) imports `Instant`/
//! `SystemTime`/`UNIX_EPOCH` from here instead of `std::time`, and
//! neither knows nor cares which target it's running on.
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};
