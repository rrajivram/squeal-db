use std::sync::atomic::{AtomicU16, AtomicU32};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, error::StoreError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct Tuple {
    #[serde(with = "postcard::fixint::le")]
    pub(crate) id: DBSizeType,
    pub(crate) lsn: u16,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TupleView<'a> {
    #[serde(with = "postcard::fixint::le")]
    id: DBSizeType,
    data: &'a [u8],
}

static LSN_COUNTER: AtomicU16 = AtomicU16::new(0);

impl Tuple {
    pub fn new(id: DBSizeType, data: &[u8]) -> Self {
        Self {
            id,
            lsn: LSN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            data: data.to_vec(),
        }
    }
    pub fn new_cheap(id: DBSizeType, data: &[u8]) -> Self {
        Self {
            id,
            lsn: 0,
            data: data.to_vec(),
        }
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

impl<'a> TupleView<'a> {
    pub(crate) fn from(bytes: &'a [u8]) -> Result<Self, StoreError> {
        let t: TupleView = from_bytes(bytes)?;
        Ok(t)
    }
}

#[cfg(test)]
mod tests {

    use std::{thread, time::Instant};

    use crate::tuple::{Tuple, TupleView};

    #[test]
    fn test_tuple() {
        let t = Tuple {
            id: 10,
            lsn: 0,
            data: vec![b'h', b'e', b'l', b'l', b'o'],
        };
        let b = t.to();
        let t1 = Tuple::from(&b).unwrap();
        let t2 = TupleView::from(&b).unwrap();
        assert_eq!(t1.id, 10);
        assert_eq!(t1.data, vec![b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(t1.id, t2.id);
        assert_eq!(t1.data, t2.data);
    }

    #[ignore = "Perf test"]
    #[test]
    fn test_tuple_mt_create() {
        let start = Instant::now();
        let mut v = vec![];
        for _ in 0..16 {
            v.push(thread::spawn(|| {
                for i in 0..100_000 {
                    let _n = Tuple::new(i, b"abcdef");
                }
            }));
        }
        for h in v {
            h.join().unwrap();
        }
        println!("regular took {} us", start.elapsed().as_micros());
        let start = Instant::now();
        let mut v = vec![];
        for _ in 0..16 {
            v.push(thread::spawn(|| {
                for i in 0..100_000 {
                    let _n = Tuple::new_cheap(i, b"abcdef");
                }
            }));
        }
        for h in v {
            h.join().unwrap();
        }
        println!("cheap took {} us", start.elapsed().as_micros());
    }
}
