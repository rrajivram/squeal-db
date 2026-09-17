use std::{
    collections::HashMap,
    sync::{Arc, RwLock, atomic::AtomicU64},
};

use crate::{
    db::DBSizeType,
    error::StoreError,
    logger::{Logger, Operation},
};

/// How many values a sequence hands out before it must log a new high-water
/// mark. A crash skips at most one chunk of values and never repeats one.
const SEQUENCE_CHUNK: u64 = 32;

#[derive(Debug)]
struct Seq {
    /// Next value to hand out.
    next: AtomicU64,
    /// Every value below this has a durable-or-queued `Sequence` record
    /// covering it (see `gen_key`).
    logged_high: AtomicU64,
}

/// Named monotonic sequences (table ids, the SQL layer's row ids, ...).
///
/// TXN_SIMPLIFICATION_PLAN.md phase 1 (proposal §3.11): values are handed out
/// from an in-memory chunk; crossing into a new chunk appends a
/// `Operation::Sequence` record BEFORE the first value of that chunk is
/// returned, so the record always precedes, in log order, any transaction
/// record that uses the value. Creation and removal are logged the same
/// way. Recovery restores each sequence to `max(persisted value, highest
/// logged high-water)`. Before this, sequences were persisted only at
/// checkpoint, and a crash after committed inserts reopened with a stale
/// sequence that reissued already-used row ids.
#[derive(Debug, Default, Clone)]
pub struct Generator {
    gens: Arc<RwLock<HashMap<String, Seq>>>,
    logger: Arc<RwLock<Option<Arc<Logger>>>>,
}

impl Generator {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Wire the WAL in. Until this is called (or if it never is, e.g. in
    /// unit tests), sequences work but nothing is logged.
    pub(crate) fn attach_logger(&self, logger: Arc<Logger>) {
        *self.logger.write().unwrap_or_else(|e| e.into_inner()) = Some(logger);
    }

    /// Drop the WAL reference (Db::close needs sole ownership of the logger
    /// to shut it down; the generator outlives it in the SQL layer's hands).
    pub(crate) fn detach_logger(&self) {
        *self.logger.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    fn log(&self, name: &str, high_water: u64, dropped: bool) -> Result<(), StoreError> {
        let guard = self.logger.read()?;
        if let Some(logger) = guard.as_ref() {
            logger.log_new(Operation::Sequence {
                name: name.to_string(),
                high_water,
                dropped,
            })?;
        }
        Ok(())
    }

    pub fn create_generator<S: AsRef<str>>(
        &self,
        name: S,
        start: Option<DBSizeType>,
    ) -> Result<(), StoreError> {
        let name = name.as_ref().to_string();
        let start = start.unwrap_or_default();
        {
            if self.gens.read()?.contains_key(&name) {
                return Err(StoreError::DuplicateName(name.to_owned()));
            }
        }
        // Logged before it becomes usable, so a crash after this call
        // reopens with the sequence present.
        self.log(&name, start, false)?;
        self.gens.write()?.insert(
            name,
            Seq {
                next: AtomicU64::new(start),
                logged_high: AtomicU64::new(start),
            },
        );
        Ok(())
    }

    pub fn remove_generator<S: AsRef<str>>(&self, name: S) -> Result<(), StoreError> {
        let name = name.as_ref();
        if self.gens.write()?.remove(name).is_some() {
            self.log(name, 0, true)?;
        }
        Ok(())
    }

    /// Ensure `name` exists and will never hand out a value below
    /// `at_least`. Used by recovery for every `Sequence` record it replays
    /// and by `set_values` for the checkpoint-persisted snapshot. Never
    /// lowers a sequence.
    pub(crate) fn ensure_at_least<S: AsRef<str>>(
        &self,
        name: S,
        at_least: DBSizeType,
    ) -> Result<(), StoreError> {
        let name = name.as_ref();
        let mut gens = self.gens.write()?;
        match gens.get(name) {
            Some(seq) => {
                seq.next
                    .fetch_max(at_least, std::sync::atomic::Ordering::AcqRel);
                seq.logged_high
                    .fetch_max(at_least, std::sync::atomic::Ordering::AcqRel);
            }
            None => {
                gens.insert(
                    name.to_string(),
                    Seq {
                        next: AtomicU64::new(at_least),
                        logged_high: AtomicU64::new(at_least),
                    },
                );
            }
        }
        Ok(())
    }

    /// Recovery saw a `Sequence { dropped: true }` for `name`.
    pub(crate) fn remove_unlogged<S: AsRef<str>>(&self, name: S) -> Result<(), StoreError> {
        self.gens.write()?.remove(name.as_ref());
        Ok(())
    }

    pub fn gen_key<S: AsRef<str>>(&self, name: S) -> Result<DBSizeType, StoreError> {
        let name = name.as_ref();
        let (v, high) = {
            let gens = self.gens.read()?;
            let seq = gens
                .get(name)
                .ok_or_else(|| StoreError::MissingKey(name.to_string()))?;
            (
                seq.next.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
                seq.logged_high.load(std::sync::atomic::Ordering::Acquire),
            )
        };
        if v >= high {
            // This value is past everything a durable record covers: log a
            // new high-water mark BEFORE handing it out. Send first, then
            // raise `logged_high`: a concurrent caller that also observes the
            // old mark logs its own (duplicate, harmless) record before
            // proceeding, so every value ever returned is preceded in log
            // order by a record that covers it, whichever thread wins.
            let new_high = v + SEQUENCE_CHUNK;
            self.log(name, new_high, false)?;
            let gens = self.gens.read()?;
            if let Some(seq) = gens.get(name) {
                seq.logged_high
                    .fetch_max(new_high, std::sync::atomic::Ordering::AcqRel);
            }
        }
        Ok(v)
    }

    pub(crate) fn get_values(&self) -> Result<Vec<(String, DBSizeType)>, StoreError> {
        Ok(self
            .gens
            .read()?
            .iter()
            .map(|(k, v)| (k.to_string(), v.next.load(std::sync::atomic::Ordering::Relaxed)))
            .collect::<Vec<_>>())
    }

    /// Restore the checkpoint-persisted snapshot (never lowers anything the
    /// log has already raised — see `ensure_at_least`).
    pub(crate) fn set_values(&self, values: Vec<(String, DBSizeType)>) -> Result<(), StoreError> {
        for (k, v) in values {
            self.ensure_at_least(k, v)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::Generator;

    #[test]
    fn test_gen_1() {
        let g = Generator::new();
        assert!(g.create_generator("test", None).is_ok());
        assert!(g.create_generator("test", None).is_err());
        assert!(g.gen_key("test").is_ok());
        assert!(g.gen_key("test1").is_err());
    }

    #[test]
    fn test_remove_generator_allows_recreating_the_same_name() {
        let g = Generator::new();
        g.create_generator("test", None).unwrap();
        g.remove_generator("test").unwrap();
        assert!(g.gen_key("test").is_err());
        g.create_generator("test", None).unwrap();
        assert_eq!(g.gen_key("test").unwrap(), 0);
    }

    #[test]
    fn test_remove_generator_missing_name_is_a_no_op() {
        let g = Generator::new();
        assert!(g.remove_generator("nope").is_ok());
    }

    #[test]
    fn test_gen_2() {
        let g = Generator::new();
        g.create_generator("test", None).unwrap();
        assert_eq!(g.gen_key("test").unwrap(), 0);
        assert_eq!(g.gen_key("test").unwrap(), 1);
        assert_eq!(g.gen_key("test").unwrap(), 2);
    }

    #[test]
    fn test_gen_3() {
        let g = Generator::new();
        g.create_generator("test", Some(100)).unwrap();
        assert_eq!(g.gen_key("test").unwrap(), 100);
        let g = Generator::new();
        g.create_generator("test1", None).unwrap();
        g.create_generator("test2", None).unwrap();
        for _ in 0..100 {
            let _ = g.gen_key("test1").unwrap();
        }
        for _ in 0..50 {
            let _ = g.gen_key("test2").unwrap();
        }
        let a = g.gen_key("test1").unwrap();
        assert_eq!(a, 100);
        let a = g.gen_key("test2").unwrap();
        assert_eq!(a, 50);
    }

    #[test]
    fn test_ensure_at_least_never_lowers_and_creates_when_missing() {
        let g = Generator::new();
        g.create_generator("s", Some(10)).unwrap();
        g.ensure_at_least("s", 5).unwrap();
        assert_eq!(g.gen_key("s").unwrap(), 10);
        g.ensure_at_least("s", 40).unwrap();
        assert_eq!(g.gen_key("s").unwrap(), 40);
        g.ensure_at_least("fresh", 7).unwrap();
        assert_eq!(g.gen_key("fresh").unwrap(), 7);
    }
}
