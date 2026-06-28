use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::config::Config;
use crate::stats::{Stats, bucket_lower_bound_us};

/// Samples this process's resident set size in KB by shelling out to `ps`.
/// Works unmodified on macOS and Linux; avoids pulling in `libc` for one
/// best-effort diagnostic number.
pub fn sample_rss_kb() -> Option<u64> {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().ok()
}

/// Watches global progress; if no op completes anywhere for
/// `watchdog_timeout_secs`, dumps per-thread last-activity ages (pinpointing
/// which threads are stuck) and aborts the process — this is the deadlock
/// detector. Also periodically prints live throughput and samples memory.
/// Stops cleanly once `stop` is set by the main thread after joining workers.
pub fn spawn_watchdog(
    cfg: Arc<Config>,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut last_completed = 0u64;
        let mut stalled_for_secs = 0u64;
        loop {
            std::thread::sleep(Duration::from_secs(cfg.report_interval_secs.max(1)));
            if stop.load(Ordering::Relaxed) {
                return;
            }

            let completed = stats.completed_ops.load(Ordering::Relaxed);
            let elapsed = stats.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 { completed as f64 / elapsed } else { 0.0 };

            if let Some(rss) = sample_rss_kb() {
                stats.peak_rss_kb.fetch_max(rss, Ordering::Relaxed);
                println!(
                    "[{elapsed:7.1}s] completed={completed} ops, rate={rate:.0} ops/s, rss={rss} KB"
                );
            } else {
                println!("[{elapsed:7.1}s] completed={completed} ops, rate={rate:.0} ops/s");
            }

            if completed == last_completed {
                stalled_for_secs += cfg.report_interval_secs.max(1);
                if stalled_for_secs >= cfg.watchdog_timeout_secs {
                    eprintln!(
                        "\n*** POSSIBLE DEADLOCK: no progress for {stalled_for_secs}s (timeout={}) ***",
                        cfg.watchdog_timeout_secs
                    );
                    dump_thread_states(&stats);
                    std::process::exit(1);
                }
            } else {
                stalled_for_secs = 0;
            }
            last_completed = completed;
        }
    })
}

fn dump_thread_states(stats: &Stats) {
    let now_ms = stats.elapsed().as_millis() as u64;
    for (i, (last, done)) in stats
        .thread_last_activity_ms
        .iter()
        .zip(stats.thread_ops_done.iter())
        .enumerate()
    {
        let last_ms = last.load(Ordering::Relaxed);
        let age_ms = now_ms.saturating_sub(last_ms);
        let ops = done.load(Ordering::Relaxed);
        eprintln!("  thread {i}: ops_done={ops} last_activity={age_ms}ms ago");
    }
    eprintln!("Threads with the largest `last_activity` age are the ones most likely stuck.");
}

pub struct CorrectnessReport {
    pub private_checked: u64,
    pub private_mismatches: Vec<String>,
    pub hot_checked: u64,
    pub hot_corrupt: Vec<String>,
}

impl CorrectnessReport {
    pub fn passed(&self) -> bool {
        self.private_mismatches.is_empty() && self.hot_corrupt.is_empty()
    }
}

const MAX_REPORTED_MISMATCHES: usize = 25;

pub fn record_mismatch(list: &mut Vec<String>, msg: String) {
    if list.len() < MAX_REPORTED_MISMATCHES {
        list.push(msg);
    }
}

pub fn print_final_report(cfg: &Config, stats: &Stats, correctness: &CorrectnessReport) {
    let elapsed = stats.elapsed();
    let total_completed = stats.completed_ops.load(Ordering::Relaxed);
    let rate = total_completed as f64 / elapsed.as_secs_f64().max(0.001);

    println!("\n==================== STRESS REPORT ====================");
    println!(
        "backend={:?} threads={} tables={} ops/thread={} hot_keys={} private_keys={} seed={}",
        cfg.backend, cfg.threads, cfg.tables, cfg.ops_per_thread, cfg.hot_keys, cfg.private_keys, cfg.seed
    );
    println!("elapsed={:.2}s total_ops={total_completed} throughput={rate:.0} ops/s", elapsed.as_secs_f64());
    println!(
        "txn_committed={} txn_rolled_back={} dropped_after_retry_exhaustion={}",
        stats.txn_committed.load(Ordering::Relaxed),
        stats.txn_rolled_back.load(Ordering::Relaxed),
        stats.dropped_after_retry_exhaustion.load(Ordering::Relaxed),
    );
    let peak_rss = stats.peak_rss_kb.load(Ordering::Relaxed);
    if peak_rss > 0 {
        println!("peak_rss={peak_rss} KB");
    }

    println!("\n-- per-op-kind outcomes --");
    for (name, c) in [
        ("insert", &stats.insert),
        ("update", &stats.update),
        ("remove", &stats.remove),
        ("find", &stats.find),
    ] {
        println!(
            "{name:7} total={:<8} ok={:<8} key_not_found={:<7} duplicate_key={:<6} lock_contention={:<6} other_err={}",
            c.total(),
            c.success.load(Ordering::Relaxed),
            c.key_not_found.load(Ordering::Relaxed),
            c.duplicate_key.load(Ordering::Relaxed),
            c.lock_contention.load(Ordering::Relaxed),
            c.other_error.load(Ordering::Relaxed),
        );
    }

    println!("\n-- latency histogram (microseconds) --");
    for (i, b) in stats.latency_buckets.iter().enumerate() {
        let count = b.load(Ordering::Relaxed);
        if count > 0 {
            println!("  [{:>8}us .. {:>8}us): {}", bucket_lower_bound_us(i), bucket_lower_bound_us(i + 1), count);
        }
    }

    println!("\n-- correctness: private (single-writer) keys --");
    println!("checked={} mismatches={}", correctness.private_checked, correctness.private_mismatches.len());
    for m in &correctness.private_mismatches {
        println!("  MISMATCH: {m}");
    }

    println!("\n-- correctness: hot (contended) keys --");
    println!("checked={} corrupt={}", correctness.hot_checked, correctness.hot_corrupt.len());
    for m in &correctness.hot_corrupt {
        println!("  CORRUPT: {m}");
    }

    println!("\n========================================================");
    if correctness.passed() {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL");
    }
    println!("========================================================");
}
