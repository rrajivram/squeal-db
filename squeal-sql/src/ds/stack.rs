pub(crate) struct Stack<T> {
    head: Option<Box<Stacktem<T>>>,
    count: usize,
}

struct Stacktem<T> {
    item: T,
    next: Option<Box<Stacktem<T>>>,
}

impl<T> Stack<T> {
    pub(crate) fn new() -> Self {
        Self {
            head: None,
            count: 0,
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        let new_item = Box::new(Stacktem {
            item,
            next: self.head.take(),
        });
        let _ = self.head.replace(new_item);
        self.count += 1;
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        if let Some(t) = self.head.take() {
            self.head = t.next;
            self.count -= 1;
            Some(t.item)
        } else {
            None
        }
    }

    pub(crate) fn peek(&self, num: usize) -> Option<&T> {
        let mut i = 0;
        let mut start = &self.head;
        loop {
            if i == num || i == self.count {
                break;
            }
            i += 1;
            if let Some(t) = start {
                start = &t.next;
            }
        }
        if let Some(t) = start {
            Some(&t.item)
        } else {
            None
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_simple() {
        let mut st = Stack::new();
        st.push(1);
        st.push(2);
        assert_eq!(st.count, 2);
        assert!(st.peek(0) == Some(&2));
        assert!(st.peek(1) == Some(&1));
        assert_eq!(st.peek(3), None);
        assert_eq!(st.count, 2);
        assert!(st.pop() == Some(2));
        assert!(st.pop() == Some(1));
        assert_eq!(st.pop(), None);
        assert_eq!(st.count, 0);
    }
}
