//! A pool of the byte buffers pages are read into and held in.
//!
//! A cache bigger than memory allows evicts a page and loads another all
//! the time: a page-sized buffer freed, another allocated. The system
//! allocator keeps much of what is freed rather than returning it, so a
//! store whose page cache was bounded at 128 MiB grew to over 200 MiB of
//! footprint under that churn, with 130 MiB live. Buffers given back here
//! are handed out again for the next page instead, so a steady cache
//! allocates nothing.
//!
//! Process-wide (pages of every database share it), keyed by capacity, and
//! bounded: what is given back past POOL_BYTES is freed.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

// Room for a few hundred pages of the default size: enough to absorb the
// eviction/load churn, small beside any page cache.
const POOL_BYTES: usize = 8 * 1024 * 1024;
// Buffers smaller than this aren't worth pooling.
const MIN_POOLED: usize = 1024;

struct Pool {
    by_capacity: HashMap<usize, Vec<Vec<u8>>>,
    bytes: usize,
}

static POOL: LazyLock<Mutex<Pool>> = LazyLock::new(|| {
    Mutex::new(Pool {
        by_capacity: HashMap::new(),
        bytes: 0,
    })
});

fn take_raw(capacity: usize) -> Option<Vec<u8>> {
    if capacity < MIN_POOLED {
        return None;
    }
    let mut pool = POOL.lock().unwrap_or_else(|e| e.into_inner());
    let buf = pool.by_capacity.get_mut(&capacity)?.pop()?;
    pool.bytes -= buf.capacity();
    Some(buf)
}

/// A zeroed buffer of `len` bytes (to read into), pooled if one is free.
pub(crate) fn zeroed(len: usize) -> Vec<u8> {
    match take_raw(len) {
        Some(mut buf) => {
            buf.clear();
            buf.resize(len, 0);
            buf
        }
        None => vec![0u8; len],
    }
}

/// A buffer holding a copy of `bytes`, pooled if one is free.
pub(crate) fn copy_of(bytes: &[u8]) -> Vec<u8> {
    match take_raw(bytes.len()) {
        Some(mut buf) => {
            buf.clear();
            buf.extend_from_slice(bytes);
            buf
        }
        None => bytes.to_vec(),
    }
}

/// Gives a buffer back for reuse (or frees it, the pool being full).
pub(crate) fn give(buf: Vec<u8>) {
    let capacity = buf.capacity();
    if capacity < MIN_POOLED {
        return;
    }
    let mut pool = POOL.lock().unwrap_or_else(|e| e.into_inner());
    if pool.bytes + capacity > POOL_BYTES {
        return;
    }
    pool.bytes += capacity;
    pool.by_capacity.entry(capacity).or_default().push(buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_given_buffer_is_handed_out_again() {
        let mut a = zeroed(5000);
        a[0] = 7;
        let ptr = a.as_ptr();
        give(a);
        let b = copy_of(&[1u8; 5000]);
        // Another test may have taken it first; when it is ours, it is the
        // same allocation, reused.
        if b.as_ptr() == ptr {
            assert_eq!(b[0], 1);
        }
        assert_eq!(b.len(), 5000);
        let c = zeroed(5000);
        assert!(c.iter().all(|&x| x == 0));
    }
}
