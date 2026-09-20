#[derive(Debug, Clone)]
pub struct BitVec {
    data: Vec<u8>,
    count: usize,
}

impl BitVec {
    pub fn with_capacity(capacity: usize) -> Self {
        let byte_capacity = if capacity.is_multiple_of(8) {
            capacity / 8
        } else {
            capacity / 8 + 1
        };
        Self {
            data: vec![0u8; byte_capacity],
            count: capacity,
        }
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn set(&mut self, index: usize) {
        if index >= self.count {
            panic!("Index is {index},but count is {}", self.count);
        }
        let byte = index / 8;
        let bit = index % 8;
        self.data[byte] |= 1 << bit;
    }

    #[allow(unused)]
    pub fn unset(&mut self, index: usize) {
        if index >= self.count {
            panic!("Index is {index},but count is {}", self.count);
        }
        let byte = index / 8;
        let bit = index % 8;
        self.data[byte] &= !(1 << bit);
    }

    pub fn is_set(&self, index: usize) -> bool {
        if index >= self.count {
            panic!("Index is {index},but count is {}", self.count);
        }
        let byte = index / 8;
        let bit = index % 8;
        self.data[byte] & (1 << bit) != 0
    }

    #[allow(clippy::manual_find)]
    pub fn first_available(&self, from: usize) -> Option<usize> {
        for index in from + 1..self.count {
            if !self.is_set(index) {
                return Some(index);
            }
        }
        for index in 0..from {
            if !self.is_set(index) {
                return Some(index);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testvec() {
        let mut v = BitVec::with_capacity(1000);
        for i in (0..1000).step_by(3) {
            v.set(i);
        }
        for i in 0..1000 {
            if i % 3 == 0 {
                assert!(v.is_set(i));
                v.unset(i);
            } else {
                assert!(!v.is_set(i));
            }
        }
        for i in 0..1000 {
            assert!(!v.is_set(i));
        }
    }

    #[test]
    #[should_panic]
    fn test_panic() {
        let mut v = BitVec::with_capacity(100);
        for i in 0..v.len() {
            v.set(i);
        }
        v.set(101);
    }
}
