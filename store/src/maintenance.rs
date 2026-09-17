//! The one background thread per `Db` (TXN_SIMPLIFICATION_PLAN.md phase 3,
//! proposal §3.6). It owns every piece of work that is not a foreground
//! transaction's own: retrying failed aborts, vacuum (forgetting committed
//! transactions and their records past the horizon, purging their
//! tombstones), and triggering checkpoints by log growth. Foreground threads
//! never do maintenance; `begin()` is an increment and a map insert.
//!
//! Everything it does is observable through `MaintenanceStats` (surfaced in
//! `Db::stats()`), including the last error it hit.

use std::{
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::db::{DBFile, Db};

/// Counters published by the maintenance thread.
#[derive(Debug, Default)]
pub(crate) struct MaintenanceStats {
    pub(crate) passes: AtomicU64,
    pub(crate) vacuums_with_work: AtomicU64,
    pub(crate) transactions_forgotten: AtomicU64,
    pub(crate) records_discarded: AtomicU64,
    pub(crate) tombstones_purged: AtomicU64,
    pub(crate) abort_retries: AtomicU64,
    pub(crate) checkpoints: AtomicU64,
    pub(crate) errors: AtomicU64,
    pub(crate) last_error: Mutex<Option<String>>,
}

impl MaintenanceStats {
    pub(crate) fn record_error(&self, e: &dyn std::fmt::Display) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string());
    }
}

pub(crate) struct Maintenance {
    stop: AtomicBool,
    /// Test-only: while set, passes do nothing, so a test can hold the
    /// engine in a chosen state (e.g. an unpurged tombstone) deterministically.
    paused: AtomicBool,
    wake: Mutex<bool>,
    cv: Condvar,
    handle: Mutex<Option<JoinHandle<()>>>,
    pub(crate) stats: MaintenanceStats,
    /// How long the thread sleeps with nothing to do before running a pass
    /// anyway (the timer that catches work no wake-up announced).
    interval: Duration,
}

impl std::fmt::Debug for Maintenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Maintenance")
            .field("stopped", &self.stop.load(Ordering::Relaxed))
            .finish()
    }
}

impl Maintenance {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            stop: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            wake: Mutex::new(false),
            cv: Condvar::new(),
            handle: Mutex::new(None),
            stats: MaintenanceStats::default(),
            interval,
        }
    }

    /// Start the thread. Holds only a `Weak<Db>` so a `Db` that is dropped
    /// without `close()` still lets the thread exit on its next wake.
    pub(crate) fn start<F>(&self, db: &Arc<Db<F>>)
    where
        F: DBFile<Item = F> + 'static,
    {
        let weak: Weak<Db<F>> = Arc::downgrade(db);
        let interval = self.interval;
        let handle = std::thread::Builder::new()
            .name(format!("squeal-maintenance-{}", db.name()))
            .spawn(move || {
                loop {
                    let Some(db) = weak.upgrade() else { break };
                    let m = &db.maintenance;
                    if m.stop.load(Ordering::Acquire) {
                        break;
                    }
                    let started = Instant::now();
                    if !m.paused.load(Ordering::Acquire) {
                        if let Err(e) = db.maintenance_pass() {
                            m.stats.record_error(&e);
                        }
                        m.stats.passes.fetch_add(1, Ordering::Relaxed);
                    }
                    // Sleep until woken or the interval elapses; drop the
                    // Arc first so close()'s try_unwrap can succeed while we
                    // sleep.
                    let (wake_lock, cv) = (&m.wake, &m.cv);
                    let mut woke = wake_lock.lock().unwrap_or_else(|e| e.into_inner());
                    let elapsed = started.elapsed();
                    let remaining = interval.saturating_sub(elapsed);
                    if !*woke && !m.stop.load(Ordering::Acquire) {
                        let (g, _) = cv
                            .wait_timeout(woke, remaining.max(Duration::from_millis(1)))
                            .unwrap_or_else(|e| e.into_inner());
                        woke = g;
                    }
                    *woke = false;
                    drop(woke);
                    drop(db);
                }
            })
            .expect("spawn maintenance thread");
        *self.handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
    }

    /// Test-only: suspend/resume passes.
    #[cfg(test)]
    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
        if !paused {
            self.wake();
        }
    }

    /// Ask for a pass soon (commit, abort, transaction end).
    pub(crate) fn wake(&self) {
        let mut w = self.wake.lock().unwrap_or_else(|e| e.into_inner());
        *w = true;
        self.cv.notify_one();
    }

    /// Stop and join. Safe to call more than once.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.wake();
        if let Some(h) = self.handle.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = h.join();
        }
    }
}
