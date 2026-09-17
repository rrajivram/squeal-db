// This crate's #[global_allocator] (see lib.rs) — a global allocation
// tracker (total/peak/current bytes, a coarse allocation-size
// histogram), delegating actual allocation work to std's System
// allocator. There can be exactly one #[global_allocator] per binary,
// and this crate is the lowest-level one nearly everything else in the
// workspace depends on, so this is the only place that declaration can
// live if any consumer (e.g. squeal-cli) wants to read allocator stats
// — see `stats()`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub const SIZE_PER_BUCKET: usize = 10;
pub const BUCKET_COUNT: usize = 100;

pub struct TrackingAllocator {
    inner: System,
    allocated: AtomicUsize,
    deallocated: AtomicUsize,
    peak: AtomicUsize,
    allocations: [AtomicUsize; BUCKET_COUNT],
    // Distinct from `allocations`/alloc()'s own bookkeeping — see
    // realloc's own doc comment for why a resize needs to be counted
    // separately from an unrelated fresh alloc+dealloc pair, even
    // though it also updates allocated/deallocated/peak/allocations the
    // same way those would.
    realloc_count: AtomicUsize,
    realloc_grew: AtomicUsize,
    realloc_shrank: AtomicUsize,
}

impl TrackingAllocator {
    pub const fn new() -> Self {
        TrackingAllocator {
            inner: System,
            allocated: AtomicUsize::new(0),
            deallocated: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            // An inline-const array-repeat, not `.collect::<[_; N]>()`
            // (no std `FromIterator` impl for arrays) and not a plain
            // `[AtomicUsize::new(0); N]` repeat expression (that form
            // requires the element type to be Copy, which atomics
            // aren't) — this also needs to stay a `const fn` to back a
            // `static`, which rules out any allocator-touching
            // construction anyway.
            allocations: [const { AtomicUsize::new(0) }; BUCKET_COUNT],
            realloc_count: AtomicUsize::new(0),
            realloc_grew: AtomicUsize::new(0),
            realloc_shrank: AtomicUsize::new(0),
        }
    }

    fn current_usage(&self) -> usize {
        let allocated = self.allocated.load(Ordering::Relaxed);
        let deallocated = self.deallocated.load(Ordering::Relaxed);
        allocated.saturating_sub(deallocated)
    }

    fn get_allocations(&self) -> [usize; BUCKET_COUNT] {
        std::array::from_fn(|i| self.allocations[i].load(Ordering::Relaxed))
    }

    fn peak_usage(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn total_allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    fn realloc_count(&self) -> usize {
        self.realloc_count.load(Ordering::Relaxed)
    }

    fn realloc_grew(&self) -> usize {
        self.realloc_grew.load(Ordering::Relaxed)
    }

    fn realloc_shrank(&self) -> usize {
        self.realloc_shrank.load(Ordering::Relaxed)
    }

    // Shared by alloc() and realloc()'s growth side: records `size`
    // bytes landing, buckets it into the size histogram, and updates
    // peak if this pushed current usage past it.
    fn record_alloc(&self, size: usize) {
        let prev = self.allocated.fetch_add(size, Ordering::Relaxed);
        // Saturating, not a plain subtraction: `reset()` can zero
        // `allocated`/`deallocated` mid-process, and a later dealloc()
        // of memory that was allocated *before* that reset still bumps
        // `deallocated` with nothing matching in the (now zeroed)
        // `allocated` — without saturating, that transient
        // deallocated > allocated state underflows this usize
        // subtraction (silently wraps to near-usize::MAX in a release
        // build) and corrupts `peak` permanently, since the bogus huge
        // value then wins every future `current > peak` comparison.
        let current = (prev + size).saturating_sub(self.deallocated.load(Ordering::Relaxed));
        let bucket = (size / SIZE_PER_BUCKET).min(BUCKET_COUNT - 1);
        self.allocations[bucket].fetch_add(1, Ordering::Relaxed);

        let mut peak = self.peak.load(Ordering::Relaxed);
        while current > peak {
            match self.peak.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            self.record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocated.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe {
            self.inner.dealloc(ptr, layout);
        }
    }

    // Overridden rather than relying on GlobalAlloc's default impl
    // (alloc(new) + memcpy + dealloc(old)): System's own realloc can
    // often grow/shrink a block in place, no copy at all, and leaving
    // this unoverridden would silently discard that and force every
    // Vec/String growth in the process through a full allocate-and-copy
    // it might not have needed. Bookkeeping-wise this still needs to
    // account for the resize as if it were a dealloc(old) + alloc(new)
    // pair (see record_alloc) — the actual work just goes straight to
    // System's own realloc instead of our own alloc()/dealloc(), which
    // is what makes the in-place case possible again.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            self.realloc_count.fetch_add(1, Ordering::Relaxed);
            match new_size.cmp(&old_size) {
                std::cmp::Ordering::Greater => {
                    self.realloc_grew.fetch_add(1, Ordering::Relaxed);
                }
                std::cmp::Ordering::Less => {
                    self.realloc_shrank.fetch_add(1, Ordering::Relaxed);
                }
                std::cmp::Ordering::Equal => {}
            }
            self.deallocated.fetch_add(old_size, Ordering::Relaxed);
            self.record_alloc(new_size);
        }
        new_ptr
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AllocStats {
    pub total_allocated: usize,
    pub peak_usage: usize,
    pub current_usage: usize,
    // Count of allocations whose size fell in
    // [i*SIZE_PER_BUCKET, (i+1)*SIZE_PER_BUCKET) bytes, for
    // i in 0..BUCKET_COUNT-1; the last index catches everything
    // >= (BUCKET_COUNT-1)*SIZE_PER_BUCKET bytes.
    pub size_histogram: [usize; BUCKET_COUNT],
    // Total GlobalAlloc::realloc calls — e.g. a Vec/String outgrowing
    // its current capacity and needing to resize, in either direction.
    pub realloc_count: usize,
    // Of realloc_count, how many actually grew (new_size > old_size,
    // the "Vec capacity increase" case) vs shrank (e.g. shrink_to_fit).
    // Their sum can be less than realloc_count: a realloc requesting
    // the same size it already had counts toward neither.
    pub realloc_grew: usize,
    pub realloc_shrank: usize,
}

// Zeroes every counter (see `stats()`'s own doc comment on what they
// track), so a subsequent `stats()` call reports only what's allocated/
// deallocated *after* this point rather than since process startup —
// e.g. resetting after loading test data so a following query's own
// allocation footprint isn't mixed in with however much the load itself
// cost. Note this makes `current_usage`/`peak_usage` a *delta* from the
// reset point, not the process's true absolute heap size: zeroing
// `allocated`/`deallocated` both to 0 means already-live data (loaded
// pages, cached tables, etc.) is no longer reflected in either — that's
// the intended tradeoff for a clean before/after window, not a bug.
pub fn reset() {
    crate::GLOBAL.allocated.store(0, Ordering::Relaxed);
    crate::GLOBAL.deallocated.store(0, Ordering::Relaxed);
    crate::GLOBAL.peak.store(0, Ordering::Relaxed);
    for bucket in &crate::GLOBAL.allocations {
        bucket.store(0, Ordering::Relaxed);
    }
    crate::GLOBAL.realloc_count.store(0, Ordering::Relaxed);
    crate::GLOBAL.realloc_grew.store(0, Ordering::Relaxed);
    crate::GLOBAL.realloc_shrank.store(0, Ordering::Relaxed);
}

// Snapshot of this process's allocator stats since startup (or since the
// last `reset()` call) — reads directly off the installed
// #[global_allocator] (see lib.rs), so this reflects every allocation
// any crate in the process made, not just store's own.
pub fn stats() -> AllocStats {
    AllocStats {
        total_allocated: crate::GLOBAL.total_allocated(),
        peak_usage: crate::GLOBAL.peak_usage(),
        current_usage: crate::GLOBAL.current_usage(),
        size_histogram: crate::GLOBAL.get_allocations(),
        realloc_count: crate::GLOBAL.realloc_count(),
        realloc_grew: crate::GLOBAL.realloc_grew(),
        realloc_shrank: crate::GLOBAL.realloc_shrank(),
    }
}
