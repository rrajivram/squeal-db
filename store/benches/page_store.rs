//! Baseline microbenchmarks for the page tuple store (`AnyTuplePage`).
//!
//! This is the type that currently guards its `BTreeMap<_, Vec<Tuple>>` behind
//! an internal `RwLock`. These benches isolate the per-operation cost of that
//! store so a change to its locking / cloning strategy can be compared against a
//! recorded baseline:
//!
//!     cargo bench -p store --bench page_store        # records / compares
//!
//! Criterion persists results under `target/criterion`, so after taking a
//! baseline you can re-run post-change and it prints the % change per bench.
//! `PAGE_TUPLES` approximates a well-filled index page (many small entries).

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

use store::pages::PageTuple;
use store::pages::anytuple::AnyTuplePage;
use store::tuple::{DBIdType, Tuple};

const PAGE_TUPLES: u64 = 200;

/// A page filled with `PAGE_TUPLES` small tuples, keyed 0..N.
fn populated() -> AnyTuplePage {
    let mut p = AnyTuplePage::default();
    for i in 0..PAGE_TUPLES {
        p.add(Tuple::new(i, format!("value-for-key-{i}").as_bytes()))
            .unwrap();
    }
    p
}

fn bench_reads(c: &mut Criterion) {
    let page = populated();
    let mid = DBIdType::Int(PAGE_TUPLES / 2);

    // Point lookup — one read-lock acquisition + BTreeMap lookup per call.
    c.bench_function("get_hit", |b| {
        b.iter(|| black_box(page.get(black_box(&mid)).unwrap()))
    });

    c.bench_function("contains_hit", |b| {
        b.iter(|| black_box(page.contains(black_box(&mid)).unwrap()))
    });

    // Full scan that clones every tuple — the path `Page::iter`/`values` drives.
    c.bench_function("values_scan_clone", |b| {
        b.iter(|| black_box(page.values().unwrap()))
    });

    c.bench_function("keys_scan", |b| {
        b.iter(|| black_box(page.keys().unwrap()))
    });

    // Serialization — the writer-thread path. Small (200×18 B) and large
    // (16×4 KB) pages so both the refcount-bump cost and the payload-copy cost
    // of the intermediate clone show up.
    c.bench_function("to_bytes_small", |b| {
        b.iter(|| black_box(page.to_bytes().unwrap()))
    });

    let mut large = AnyTuplePage::default();
    for i in 0..16u64 {
        large.add(Tuple::new(i, &vec![7u8; 4 * 1024])).unwrap();
    }
    c.bench_function("to_bytes_large", |b| {
        b.iter(|| black_box(large.to_bytes().unwrap()))
    });
}

fn bench_writes(c: &mut Criterion) {
    // add: clone a full base page, then insert one more key (write-lock path).
    let base = populated();
    c.bench_function("add_one", |b| {
        b.iter_batched(
            || base.clone(),
            |mut p| {
                p.add(Tuple::new(PAGE_TUPLES, b"new-value")).unwrap();
                black_box(&p);
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("replace_one", |b| {
        b.iter_batched(
            || base.clone(),
            |mut p| {
                black_box(
                    p.replace(&DBIdType::Int(PAGE_TUPLES / 2), Tuple::new(PAGE_TUPLES / 2, b"z"))
                        .unwrap(),
                );
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("remove_one", |b| {
        b.iter_batched(
            || base.clone(),
            |mut p| {
                black_box(p.remove(DBIdType::Int(PAGE_TUPLES / 2)).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

/// The payoff of `Tuple.data: Arc<[u8]>`: a Tuple clone no longer copies the
/// payload. `vec_clone_64k` is the cost that used to be paid on every Tuple
/// clone (the `Vec<u8>` memcpy); `tuple_clone_64k` is what it costs now (an Arc
/// refcount bump). `values_scan_clone_large` shows the same effect on a page
/// scan of large rows.
fn bench_large_payload(c: &mut Criterion) {
    let payload = vec![7u8; 64 * 1024];

    // Reference point: cloning the raw Vec<u8> — the copy the old Tuple paid.
    c.bench_function("vec_clone_64k", |b| {
        b.iter(|| black_box(payload.clone()))
    });

    // Now: cloning a Tuple with a 64 KB payload is a refcount bump.
    let t = Tuple::new(1, &payload);
    c.bench_function("tuple_clone_64k", |b| b.iter(|| black_box(t.clone())));

    // Page scan cloning 16 large (4 KB) rows.
    let mut page = AnyTuplePage::default();
    for i in 0..16u64 {
        page.add(Tuple::new(i, &vec![7u8; 4 * 1024])).unwrap();
    }
    c.bench_function("values_scan_clone_large", |b| {
        b.iter(|| black_box(page.values().unwrap()))
    });
}

criterion_group!(benches, bench_reads, bench_writes, bench_large_payload);
criterion_main!(benches);
