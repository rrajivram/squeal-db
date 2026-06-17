use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AnyTuplePage {
    data: RwLock<HashMap<DBIdType, Arc<Tuple>>>,
}

impl PageTuple for AnyTuplePage {
    type Item = AnyTuplePage;
    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.read()?.len())
    }

    fn add(&self, tuple: Tuple) -> Result<(), StoreError> {
        if self.data.read()?.contains_key(&tuple.id) {
            return Err(StoreError::DuplicateKey(tuple.id));
        }
        self.data.write()?.insert(tuple.id.clone(), Arc::new(tuple));
        Ok(())
    }

    fn contains(&self, id: DBIdType) -> Result<bool, StoreError> {
        Ok(self.data.read()?.contains_key(&id))
    }

    fn get(&self, id: DBIdType) -> Result<Option<Arc<Tuple>>, StoreError> {
        Ok(self.data.read()?.get(&id).map(|t| (*t).clone()))
    }

    fn set(&self, tuple: Tuple) -> Result<(), StoreError> {
        let id = tuple.id.clone();
        self.data
            .write()?
            .get_mut(&id)
            .map(|t| *t = Arc::new(tuple))
            .ok_or(Err(StoreError::KeyNotFound(id))?)
    }

    fn remove(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        self.data
            .write()?
            .remove(&id)
            .map(|t| Arc::into_inner(t))
            .ok_or(Err(StoreError::KeyNotFound(id))?)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        let data = self.data.read()?;
        let data = data.values().collect::<Vec<_>>();
        Ok(to_allocvec(&data)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<AnyTuplePage, StoreError> {
        let mut vec: Vec<Arc<Tuple>> = from_bytes(&bytes)?;
        let map: HashMap<DBIdType, Arc<Tuple>> = vec.drain(..).map(|t| (t.id.clone(), t)).collect();
        Ok(Self {
            data: RwLock::new(map),
        })
    }
}
