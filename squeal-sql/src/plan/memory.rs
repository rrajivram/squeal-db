use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::error::SchemaError;

// Per-query memory accounting — deliberately separate from store's
// PageBuffer, which is a shared, whole-database page cache (bounded by
// its own max_entries) that every query reads through. This tracks the
// OTHER kind of memory a query engine spends: whatever a blocking
// operator (hash join build side, sort, GROUP BY hash table, DISTINCT
// set, ...) has to materialize in-process rather than stream row by
// row. A pull-based Source (like today's TableSource) never needs this
// at all — it only ever holds O(1) rows live — this exists for the
// operators that will eventually need to buffer more than that.
//
// One QueryMemory is created per query execution (see
// LogicalPlan::memory) and shared (via Arc) with every step that
// buffers; each such step calls try_reserve before growing its own
// in-memory state, and holds onto the returned MemReservation for as
// long as that memory is actually in use.
#[derive(Debug)]
pub struct QueryMemory {
    used: AtomicUsize,
    limit: usize,
}

// RAII guard for a successful reservation — releases its share of
// `used` on drop, so a step that errors out or unwinds partway through
// building up its buffered state can't leak the reservation the way a
// manual reserve()/release() pair could (mismatched calls on an early
// return, a panic mid-build, ...).
#[must_use = "dropping this immediately releases the reservation"]
#[derive(Debug)]
pub struct MemReservation {
    mem: Arc<QueryMemory>,
    bytes: usize,
}

impl QueryMemory {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    // Compare-exchange loop, not a plain fetch_add-then-check: a query
    // engine can't assume single-threaded access to one QueryMemory the
    // way Source's own &mut self methods can — e.g. a future hash
    // join's build and probe sides, or parallel scan partitions, could
    // reserve concurrently against the same budget. fetch_add-then-undo
    // would let two concurrent reservations both observe "under limit"
    // and overshoot it before either one's correction lands; this
    // never lets `used` cross `limit` even transiently.
    pub fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<MemReservation, SchemaError> {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let new = current
                .checked_add(bytes)
                .filter(|&n| n <= self.limit)
                .ok_or(SchemaError::QueryMemoryExceeded {
                    requested: bytes,
                    used: current,
                    limit: self.limit,
                })?;
            match self.used.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(MemReservation {
                        mem: self.clone(),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl MemReservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for MemReservation {
    fn drop(&mut self) {
        self.mem.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserve_within_limit_succeeds_and_tracks_used() {
        let mem = QueryMemory::new(100);
        let r = mem.try_reserve(40).unwrap();
        assert_eq!(mem.used(), 40);
        assert_eq!(r.bytes(), 40);
    }

    #[test]
    fn test_reserve_past_limit_fails_without_changing_used() {
        let mem = QueryMemory::new(100);
        let _r = mem.try_reserve(90).unwrap();
        let err = mem.try_reserve(20).unwrap_err();
        assert!(matches!(
            err,
            SchemaError::QueryMemoryExceeded {
                requested: 20,
                used: 90,
                limit: 100
            }
        ));
        assert_eq!(mem.used(), 90);
    }

    #[test]
    fn test_dropping_a_reservation_releases_its_share() {
        let mem = QueryMemory::new(100);
        {
            let _r = mem.try_reserve(60).unwrap();
            assert_eq!(mem.used(), 60);
        }
        assert_eq!(mem.used(), 0);
        // The released memory must actually be reusable, not just
        // reported as free.
        let _r2 = mem.try_reserve(100).unwrap();
    }

    #[test]
    fn test_reserve_exactly_at_the_limit_succeeds() {
        let mem = QueryMemory::new(50);
        let _r = mem.try_reserve(50).unwrap();
        assert_eq!(mem.used(), 50);
    }

    #[test]
    fn test_concurrent_reservations_never_overshoot_the_limit() {
        use std::thread;

        let mem = QueryMemory::new(1000);
        let handles: Vec<_> = (0..20)
            .map(|_| {
                let mem = mem.clone();
                thread::spawn(move || {
                    let mut held = Vec::new();
                    for _ in 0..50 {
                        if let Ok(r) = mem.try_reserve(7) {
                            held.push(r);
                        }
                    }
                    held
                })
            })
            .collect();

        let mut all_reservations = Vec::new();
        for h in handles {
            all_reservations.extend(h.join().unwrap());
        }
        assert!(mem.used() <= 1000);
        let expected: usize = all_reservations.iter().map(|r| r.bytes()).sum();
        assert_eq!(mem.used(), expected);
    }
}
