#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Seq<T, S> {
    pub head: Box<T>,
    pub tail: Vec<(T, S)>,
}
