use std::fmt::Display;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, error::StoreError, logger::UndoId, txn::TransactionId};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash, Eq, Ord, PartialOrd)]
pub enum DBIdType {
    Int(u64),
    Vec(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub(crate) struct Tuple {
    pub(crate) id: DBIdType,
    pub(crate) txn_id: Option<TransactionId>,
    pub(crate) undo_id: Option<UndoId>,
    pub(crate) data: Vec<u8>,
}

impl Tuple {
    pub fn new(id: DBSizeType, data: &[u8]) -> Self {
        Self {
            id: DBIdType::Int(id),
            data: data.to_vec(),
            ..Default::default()
        }
    }

    pub fn new_in_txn(id: DBIdType, data: &[u8], txn_id: TransactionId, undo_id: UndoId) -> Self {
        Self {
            id,
            data: data.to_vec(),
            txn_id: Some(txn_id),
            undo_id: Some(undo_id),
        }
    }

    pub fn set_txn_id(&mut self, id: TransactionId) {
        self.txn_id = Some(id)
    }

    pub fn set_undo_id(&mut self, id: UndoId) {
        self.undo_id = Some(id)
    }

    pub fn from(bytes: &[u8]) -> Result<Self, StoreError> {
        let t: Tuple = from_bytes(bytes)?;
        Ok(t)
    }

    pub fn size(&self) -> DBSizeType {
        (self.data.len() + size_of_val(self)) as DBSizeType
    }

    pub fn to(&self) -> Vec<u8> {
        to_allocvec(&self).unwrap()
    }
}

impl Default for DBIdType {
    fn default() -> Self {
        Self::Int(0)
    }
}

impl Display for DBIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            DBIdType::Int(i) => write!(f, "{}", i),
            DBIdType::Vec(v) => write!(f, "{:?}", v),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::tuple::{DBIdType, Tuple};

    #[test]
    fn test_tuple() {
        let t = Tuple {
            id: DBIdType::Int(0),
            data: vec![b'h', b'e', b'l', b'l', b'o'],
            txn_id: None,
            undo_id: None,
        };
        let b = t.to();
        let t1 = Tuple::from(&b).unwrap();
        assert_eq!(t1.id, DBIdType::Int(0));
        assert_eq!(t1.data, vec![b'h', b'e', b'l', b'l', b'o']);
    }
}
