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

    #[test]
    fn test_tuple_vec_id() {
        let id = DBIdType::Vec(b"my_key".to_vec());
        let t = Tuple {
            id: id.clone(),
            data: b"value".to_vec(),
            txn_id: None,
            undo_id: None,
        };
        let b = t.to();
        let t1 = Tuple::from(&b).unwrap();
        assert_eq!(t1.id, id);
        assert_eq!(t1.data, b"value");
    }

    #[test]
    fn test_tuple_set_txn_id() {
        use crate::txn::TransactionId;
        let mut t = Tuple::new(5, b"hello");
        assert!(t.txn_id.is_none());
        assert!(t.undo_id.is_none());
        t.set_txn_id(TransactionId(99));
        assert_eq!(t.txn_id, Some(TransactionId(99)));
        assert!(t.undo_id.is_none());
    }

    #[test]
    fn test_tuple_size_includes_overhead() {
        let data = b"hello world";
        let t = Tuple::new(1, data);
        // size must be at least the data length
        assert!(t.size() >= data.len() as u64);
        // and larger due to struct overhead
        assert!(t.size() > data.len() as u64);
    }

    #[test]
    fn test_dbid_ordering() {
        // Int variant ordering
        assert!(DBIdType::Int(1) < DBIdType::Int(2));
        assert!(DBIdType::Int(5) > DBIdType::Int(3));
        assert_eq!(DBIdType::Int(7), DBIdType::Int(7));
    }

    #[test]
    fn test_tuple_roundtrip_with_txn_id() {
        use crate::txn::TransactionId;
        let mut t = Tuple::new(10, b"payload");
        t.set_txn_id(TransactionId(1));
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.id, DBIdType::Int(10));
        assert_eq!(t2.data, b"payload");
        assert_eq!(t2.txn_id, Some(TransactionId(1)));
    }
}
