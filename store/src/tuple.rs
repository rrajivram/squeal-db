use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, error::StoreError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct Tuple {
    #[serde(with = "postcard::fixint::le")]
    pub(crate) id: DBSizeType,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TupleView<'a> {
    #[serde(with = "postcard::fixint::le")]
    id: DBSizeType,
    data: &'a [u8],
}

impl Tuple {
    pub fn new(id: DBSizeType, data: &[u8]) -> Self {
        Self {
            id,
            data: data.to_vec(),
        }
    }
    pub fn from(bytes: &[u8]) -> Result<Self, StoreError> {
        let t: Tuple = from_bytes(bytes)?;
        Ok(t)
    }

    pub fn size(&self) -> DBSizeType {
        (self.data.len() + size_of::<usize>() + size_of::<DBSizeType>()) as DBSizeType
    }

    pub fn to(&self) -> Vec<u8> {
        to_allocvec(&self).unwrap()
    }
}

impl<'a> TupleView<'a> {
    pub(crate) fn from(bytes: &'a [u8]) -> Result<Self, StoreError> {
        let t: TupleView = from_bytes(bytes)?;
        Ok(t)
    }
}

#[cfg(test)]
mod tests {

    use crate::tuple::{Tuple, TupleView};

    #[test]
    fn test_tuple() {
        let t = Tuple {
            id: 10,
            data: vec![b'h', b'e', b'l', b'l', b'o'],
        };
        let b = t.to();
        assert_eq!(t.size(), 21);
        let t1 = Tuple::from(&b).unwrap();
        let t2 = TupleView::from(&b).unwrap();
        assert_eq!(t1.size(), 21);
        assert_eq!(t1.id, 10);
        assert_eq!(t1.data, vec![b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(t1.id, t2.id);
        assert_eq!(t1.data, t2.data);
    }
}
