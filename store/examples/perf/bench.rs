use std::time::Duration;

/// Deterministic xorshift64* — no external RNG dependency, same algorithm
/// used by the `stress` and `bulk_load` examples.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn next_range(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.next_range(i as u64 + 1) as usize;
            v.swap(i, j);
        }
    }
}

/// Per-op latency samples for one phase, converted to a Summary at the end.
#[derive(Default)]
pub struct Latencies {
    samples_ns: Vec<u64>,
}

impl Latencies {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            samples_ns: Vec::with_capacity(n),
        }
    }

    pub fn record(&mut self, d: Duration) {
        self.samples_ns.push(d.as_nanos() as u64);
    }

    pub fn summary(mut self) -> Summary {
        self.samples_ns.sort_unstable();
        let n = self.samples_ns.len();
        let pct = |p: f64| -> u64 {
            if n == 0 {
                return 0;
            }
            let idx = ((p * (n as f64 - 1.0)).round() as usize).min(n - 1);
            self.samples_ns[idx]
        };
        let sum: u128 = self.samples_ns.iter().map(|&v| v as u128).sum();
        Summary {
            count: n,
            mean_ns: if n > 0 { (sum / n as u128) as u64 } else { 0 },
            p50_ns: pct(0.50),
            p95_ns: pct(0.95),
            p99_ns: pct(0.99),
            max_ns: self.samples_ns.last().copied().unwrap_or(0),
        }
    }
}

pub struct Summary {
    pub count: usize,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

pub fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

/// Prints one report row: phase name, throughput (count / wall time), and
/// latency percentiles from per-op samples taken during that same phase.
pub fn report_phase(name: &str, wall: Duration, lat: Summary) {
    let ops_per_sec = if wall.as_secs_f64() > 0.0 {
        lat.count as f64 / wall.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "{name:<42} n={:<8} wall={:<10} {ops_per_sec:>10.0} ops/s   mean={:<9} p50={:<9} p95={:<9} p99={:<9} max={:<9}",
        lat.count,
        format!("{:.2?}", wall),
        fmt_ns(lat.mean_ns),
        fmt_ns(lat.p50_ns),
        fmt_ns(lat.p95_ns),
        fmt_ns(lat.p99_ns),
        fmt_ns(lat.max_ns),
    );
}

/// Prints a single-duration report row (no per-op latency breakdown) — used
/// for whole-phase operations like checkpoint/close/reopen.
pub fn report_duration(name: &str, wall: Duration) {
    println!("{name:<42} {:>60}", format!("{:.2?}", wall));
}
