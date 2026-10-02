//! Test-only pause points, for forcing two threads into one particular
//! interleaving instead of hoping a race happens.
//!
//! Code under test calls [`pause`] at a named point (compiled in under
//! `cfg(test)` only). Normally that does nothing. A test that [`arm`]s the
//! point first makes the next thread to reach it stop there until the test
//! releases it — so the test can run the other half of the race while the
//! first is held mid-way.
//!
//! A point is armed for one `key` (the table the operation is on): tests run
//! in parallel in one process, and each race test uses a table name of its
//! own, so it holds only its own threads.

use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Default)]
struct State {
    // A thread is stopped at the point (only the first to reach it is).
    reached: bool,
    released: bool,
}

#[derive(Default)]
struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}

fn gates() -> &'static Mutex<HashMap<String, Arc<Gate>>> {
    static GATES: OnceLock<Mutex<HashMap<String, Arc<Gate>>>> = OnceLock::new();
    GATES.get_or_init(Default::default)
}

fn name(point: &str, key: &str) -> String {
    format!("{point}:{key}")
}

/// Called by the code under test. Stops here if a test armed `point` for
/// `key` and no thread has stopped at it yet; returns once released.
pub(crate) fn pause(point: &str, key: &str) {
    let Some(gate) = gates().lock().unwrap().get(&name(point, key)).cloned() else {
        return;
    };
    let mut state = gate.state.lock().unwrap();
    if state.reached {
        return;
    }
    state.reached = true;
    gate.changed.notify_all();
    while !state.released {
        state = gate.changed.wait(state).unwrap();
    }
}

/// An armed pause point. Dropping it releases whoever is stopped there.
pub(crate) struct Armed {
    name: String,
    gate: Arc<Gate>,
}

/// Arms `point` for `key`: the next thread to reach it stops there.
pub(crate) fn arm(point: &str, key: &str) -> Armed {
    let gate = Arc::new(Gate::default());
    let name = name(point, key);
    gates().lock().unwrap().insert(name.clone(), gate.clone());
    Armed { name, gate }
}

impl Armed {
    /// Waits until a thread is stopped at the point. Panics if none gets
    /// there: the test's premise (that the operation passes this point) is
    /// wrong.
    pub(crate) fn wait_reached(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.gate.state.lock().unwrap();
        while !state.reached {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(
                !left.is_zero(),
                "no thread reached pause point {}",
                self.name
            );
            state = self.gate.changed.wait_timeout(state, left).unwrap().0;
        }
    }

    /// Lets the stopped thread go on.
    pub(crate) fn release(self) {}
}

impl Drop for Armed {
    fn drop(&mut self) {
        gates().lock().unwrap().remove(&self.name);
        self.gate.state.lock().unwrap().released = true;
        self.gate.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_an_armed_point_holds_the_first_thread_until_released() {
        pause("hook.self_test", "unarmed"); // not armed: returns at once
        let armed = arm("hook.self_test", "k");
        let done = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                pause("hook.self_test", "k");
                done.store(true, Ordering::SeqCst);
            });
            armed.wait_reached();
            // Only the first thread is held.
            pause("hook.self_test", "k");
            assert!(!done.load(Ordering::SeqCst));
            armed.release();
        });
        assert!(done.load(Ordering::SeqCst));
    }
}
