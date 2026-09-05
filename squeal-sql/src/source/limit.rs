use crate::source::Source;

#[derive(Debug)]
pub(crate) struct Limit {
    source: Box<dyn Source>,
    limit: usize,
    yielded: usize,
}

impl Limit {
    pub(crate) fn new(source: Box<dyn Source>, limit: usize) -> Self {
        Self {
            source,
            limit,
            yielded: 0,
        }
    }
}

impl Source for Limit {
    fn fields(&self) -> std::sync::Arc<[super::ProjectableField]> {
        self.source.as_ref().fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        if self.yielded < self.limit {
            self.yielded += 1;
            self.source.as_mut().next()
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<(), crate::error::SchemaError> {
        self.source.reset()?;
        self.yielded = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn src(rows: &[i64]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["v"],
            rows.iter().map(|v| vec![ValueItem::Integer(*v)]).collect(),
        ))
    }

    #[test]
    fn test_limit_caps_output_when_the_source_has_more_rows() {
        let mut l = Limit::new(src(&[1, 2, 3, 4, 5]), 3);
        assert_eq!(
            drain(&mut l),
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ]
        );
    }

    #[test]
    fn test_limit_is_a_no_op_when_the_source_has_fewer_rows() {
        let mut l = Limit::new(src(&[1, 2]), 10);
        assert_eq!(
            drain(&mut l),
            vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
        );
    }

    #[test]
    fn test_limit_zero_yields_nothing() {
        let mut l = Limit::new(src(&[1, 2, 3]), 0);
        assert_eq!(drain(&mut l), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_reset_restarts_both_the_count_and_the_underlying_source() {
        let mut l = Limit::new(src(&[1, 2, 3, 4, 5]), 2);
        let first_pass = drain(&mut l);
        assert_eq!(first_pass.len(), 2);

        l.reset().unwrap();
        let second_pass = drain(&mut l);
        assert_eq!(
            second_pass, first_pass,
            "reset must re-apply the same limit from the start"
        );
    }
}
