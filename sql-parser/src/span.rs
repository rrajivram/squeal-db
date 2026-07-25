use chumsky::span::SimpleSpan;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TokenSpan {
    pub start: usize,
    pub end: usize,
}

impl From<SimpleSpan> for TokenSpan {
    fn from(value: SimpleSpan) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}
