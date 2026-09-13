use std::{
    collections::HashMap,
    sync::{Arc, RwLock, atomic::AtomicU64},
};

use crate::{db::DBSizeType, error::StoreError};

#[derive(Debug, Default, Clone)]
pub struct Generator {
    gens: Arc<RwLock<HashMap<String, AtomicU64>>>,
}

impl Generator {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn create_generator<S: AsRef<str>>(
        &self,
        name: S,
        start: Option<DBSizeType>,
    ) -> Result<(), StoreError> {
        let name = name.as_ref().to_string();
        {
            if self.gens.read()?.contains_key(&name) {
                return Err(StoreError::DuplicateName(name.to_owned()));
            }
        }
        self.gens
            .write()?
            .insert(name.to_owned(), AtomicU64::new(start.unwrap_or_default()));
        Ok(())
    }

    pub fn remove_generator<S: AsRef<str>>(&self, name: S) -> Result<(), StoreError> {
        self.gens.write()?.remove(name.as_ref());
        Ok(())
    }

    // STORE_AUDIT.md T11: raises a named generator's stored value to at
    // least `min_value`, never lowering it — used to reconcile an
    // in-memory sequence against a value recovered by scanning the log,
    // which can be higher than whatever was last persisted (generators
    // are only flushed to disk at checkpoint/close/table-creation, not on
    // every mint).
    pub(crate) fn advance_past<S: AsRef<str>>(
        &self,
        name: S,
        min_value: DBSizeType,
    ) -> Result<(), StoreError> {
        let name = name.as_ref().to_string();
        self.gens
            .read()?
            .get(&name)
            .ok_or(StoreError::MissingKey(name))?
            .fetch_max(min_value, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    pub fn gen_key<S: AsRef<str>>(&self, name: S) -> Result<DBSizeType, StoreError> {
        let name = name.as_ref().to_string();
        Ok(self
            .gens
            .read()?
            .get(&name)
            .ok_or(StoreError::MissingKey(name))?
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    pub(crate) fn get_values(&self) -> Result<Vec<(String, DBSizeType)>, StoreError> {
        Ok(self
            .gens
            .read()?
            .iter()
            .map(|(k, v)| (k.to_string(), v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect::<Vec<_>>())
    }

    pub(crate) fn set_values(&self, values: Vec<(String, DBSizeType)>) -> Result<(), StoreError> {
        self.gens.write()?.extend(
            values
                .iter()
                .map(|(k, v)| (k.to_string(), AtomicU64::new(*v))),
        );
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
        // Gone: no longer usable...
        assert!(g.gen_key("test").is_err());
        // ...and the name is free again, starting fresh (not resuming the
        // old sequence).
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
        for _ in 0..500 {
            let _ = g.gen_key("test2").unwrap();
        }
        let v = g.get_values().unwrap();
        assert!(
            v == vec![("test1".to_string(), 100), ("test2".to_string(), 500)]
                || v == vec![("test2".to_string(), 500), ("test1".to_string(), 100)]
        );
        let g = Generator::new();
        g.set_values(v).unwrap();
        let a = g.gen_key("test1").unwrap();
        assert_eq!(a, 100);
        let a = g.gen_key("test2").unwrap();
        assert_eq!(a, 500);
    }

    #[test]
    fn test_advance_past_raises_the_value_when_min_value_is_higher() {
        let g = Generator::new();
        g.create_generator("test", Some(5)).unwrap();
        g.advance_past("test", 100).unwrap();
        assert_eq!(g.gen_key("test").unwrap(), 100);
    }

    #[test]
    fn test_advance_past_never_lowers_the_value() {
        let g = Generator::new();
        g.create_generator("test", Some(100)).unwrap();
        g.advance_past("test", 5).unwrap();
        assert_eq!(
            g.gen_key("test").unwrap(),
            100,
            "advance_past must never lower an already-higher value"
        );
    }

    #[test]
    fn test_advance_past_missing_name_is_an_error() {
        let g = Generator::new();
        assert!(g.advance_past("nope", 1).is_err());
    }
}
