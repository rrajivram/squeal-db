// This crate's #[global_allocator] (see lib.rs) — a global allocation
// tracker (total/peak/current bytes, a coarse allocation-size
// histogram), delegating actual allocation work to mimalloc rather than
// std's System so the perf characteristics lib.rs's own comment
// documents (mimalloc measurably cutting the cost of the copy-on-write
// page clone/drop pattern) don't change just because something's now
// also counting bytes. There can be exactly one #[global_allocator] per
// binary, and this crate is the lowest-level one nearly everything else
// in the workspace depends on, so this is the only place that
// declaration can live if any consumer (e.g. squeal-cli) wants to read
// allocator stats — see `stats()`.
//
// This whole module is gated out under dhat-heap: GLOBAL (see lib.rs)
// becomes dhat::Alloc in that build, which none of TrackingAllocator's
// own accessor methods apply to.
#![cfg(not(feature = "dhat-heap"))]

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

use mimalloc::MiMalloc;

pub const SIZE_PER_BUCKET: usize = 10;
pub const BUCKET_COUNT: usize = 100;

pub struct TrackingAllocator {
    inner: MiMalloc,
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
            inner: MiMalloc,
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
        let current = prev + size - self.deallocated.load(Ordering::Relaxed);
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
    // (alloc(new) + memcpy + dealloc(old)): mimalloc has its own real
    // realloc (can often grow/shrink a block in place, no copy at all —
    // see libmimalloc-sys's mi_realloc_aligned), and leaving this
    // unoverridden would silently discard that and force every Vec/
    // String growth in the process through a full allocate-and-copy it
    // might not have needed. Bookkeeping-wise this still needs to
    // account for the resize as if it were a dealloc(old) + alloc(new)
    // pair (see record_alloc) — the actual work just goes straight to
    // mimalloc's own realloc instead of our own alloc()/dealloc(),
    // which is what makes the in-place case possible again.
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

// Snapshot of this process's allocator stats since startup — reads
// directly off the installed #[global_allocator] (see lib.rs), so this
// reflects every allocation any crate in the process made, not just
// store's own.
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
