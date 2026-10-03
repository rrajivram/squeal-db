//! Per-row timing for query statistics (Source::query_stats), measured on
//! one call in SAMPLE and scaled up: reading the clock twice a row cost
//! more than some steps' own work (a fifth of a 100k-row range count).
//! Each source keeps its own counter, so a step whose calls alternate with
//! another's is still sampled.

use store::clock::Instant;

pub(crate) const SAMPLE: u32 = 16;

#[derive(Debug, Default)]
pub(crate) struct RowTimer {
    calls: u32,
}

impl RowTimer {
    /// The clock, on one call in SAMPLE; else nothing to stop.
    #[inline]
    pub(crate) fn start(&mut self) -> Option<Instant> {
        let call = self.calls;
        self.calls = call.wrapping_add(1);
        call.is_multiple_of(SAMPLE).then(Instant::now)
    }
}

/// Adds a sampled call's time, scaled to stand for the calls not timed.
#[inline]
pub(crate) fn add(total: &mut u128, started: Option<Instant>) {
    if let Some(s) = started {
        *total += s.elapsed().as_nanos() * SAMPLE as u128;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_call_in_sample_is_timed_and_scaled() {
        let mut t = RowTimer::default();
        let timed = (0..SAMPLE * 3).filter(|_| t.start().is_some()).count();
        assert_eq!(timed, 3);
        let mut total = 0;
        add(&mut total, None);
        assert_eq!(total, 0);
        add(&mut total, Some(Instant::now()));
        assert_eq!(total % SAMPLE as u128, 0);
    }
}
