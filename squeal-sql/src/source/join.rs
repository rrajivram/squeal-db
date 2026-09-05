use std::sync::Arc;

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    source::{ProjectableField, Source},
};

/// A comma-joined FROM clause (`FROM a, b, c`), i.e. a full cross join
/// across every source — every combination of one row from each source,
/// exactly once.
#[derive(Debug)]
pub(crate) struct UnionJoin {
    sources: Vec<Box<dyn Source>>,
    fields: Arc<[ProjectableField]>,
    // The row currently held by each source — the "digits" of a
    // mixed-radix odometer counting through every combination: the last
    // source cycles fastest, the first slowest, the same way carrying a
    // digit works when counting past 9. `None` before the first `next()`
    // call, and again once every combination has been produced.
    current: Option<Vec<IndexKey>>,
}

impl UnionJoin {
    pub(crate) fn new(sources: Vec<Box<dyn Source>>) -> Result<Self, SchemaError> {
        let mut fields = vec![];
        for s in &sources {
            let f = s.as_ref().fields();
            for fi in f.iter() {
                fields.push(fi.clone());
            }
        }
        Ok(Self {
            sources,
            fields: Arc::from(fields.as_slice()),
            current: None,
        })
    }

    fn combine(rows: &[IndexKey]) -> Result<IndexKey, SchemaError> {
        let mut values = vec![];
        for row in rows {
            values.extend_from_slice(row.values());
        }
        Ok(IndexKey::new_from_owned(values)?)
    }
}

impl Source for UnionJoin {
    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if self.current.is_none() {
            // First call: seed one row from every source. A source with
            // no rows at all makes the whole cross product empty — a
            // cross join against nothing has nothing on either side,
            // same as relational algebra's empty-relation rule.
            let mut rows = Vec::with_capacity(self.sources.len());
            for s in &mut self.sources {
                match s.next()? {
                    Some(row) => rows.push(row),
                    None => return Ok(None),
                }
            }
            // Zero sources (a `SELECT` with no FROM at all) is the one
            // legitimate case where `rows` stays empty — conventionally a
            // single row with zero columns, not "no rows", matching how
            // a FROM-less SELECT (`SELECT 1+2`) is expected to still
            // produce exactly one output row.
            let combined = Self::combine(&rows)?;
            self.current = Some(rows);
            return Ok(Some(combined));
        }

        // Advance like an odometer: try the last source first. If it's
        // exhausted, reset it back to its own first row and carry the
        // advance one source to the left. Reaching past the first source
        // means every combination has already been produced.
        let mut i = self.sources.len();
        loop {
            if i == 0 {
                self.current = None;
                return Ok(None);
            }
            i -= 1;
            match self.sources[i].next()? {
                Some(row) => {
                    self.current.as_mut().unwrap()[i] = row;
                    break;
                }
                None => {
                    self.sources[i].reset()?;
                    let first = self.sources[i].next()?.ok_or_else(|| {
                        SchemaError::InternalSchemaError(
                            "source produced no rows after reset, despite having at least one \
                             earlier"
                                .into(),
                        )
                    })?;
                    self.current.as_mut().unwrap()[i] = first;
                }
            }
        }
        Ok(Some(Self::combine(self.current.as_ref().unwrap())?))
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.as_mut().reset()?;
        }
        self.current = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn src(name: &str, rows: &[i64]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &[name],
            rows.iter().map(|v| vec![ValueItem::Integer(*v)]).collect(),
        ))
    }

    fn row(vals: &[i64]) -> Vec<ValueItem> {
        vals.iter().map(|v| ValueItem::Integer(*v)).collect()
    }

    #[test]
    fn test_single_source_behaves_like_a_plain_scan() {
        let mut join = UnionJoin::new(vec![src("a", &[1, 2, 3])]).unwrap();
        assert_eq!(drain(&mut join), vec![row(&[1]), row(&[2]), row(&[3])]);
    }

    #[test]
    fn test_two_sources_produce_the_full_cross_product_not_a_zip() {
        // Regression test: this used to only zip corresponding positions
        // together (2 rows for two 2-row sources) instead of producing
        // every combination (4 rows) — see UnionJoin::next's own doc
        // comment on the odometer approach that replaced it.
        let mut join = UnionJoin::new(vec![src("a", &[1, 2]), src("b", &[10, 20])]).unwrap();
        assert_eq!(
            drain(&mut join),
            vec![row(&[1, 10]), row(&[1, 20]), row(&[2, 10]), row(&[2, 20]),]
        );
    }

    #[test]
    fn test_equal_length_self_join_produces_every_combination() {
        // The specific case that most clearly exposed the old zip bug:
        // two sources of the same length must still produce len*len rows,
        // not just len (pairing each row with only itself).
        let mut join = UnionJoin::new(vec![src("a", &[1, 2, 3]), src("b", &[1, 2, 3])]).unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 9);
        for a in [1, 2, 3] {
            for b in [1, 2, 3] {
                assert!(
                    rows.contains(&row(&[a, b])),
                    "missing combination ({a}, {b})"
                );
            }
        }
    }

    #[test]
    fn test_uneven_length_sources_produce_the_full_cross_product() {
        let mut join = UnionJoin::new(vec![src("a", &[1, 2, 3]), src("b", &[10, 20])]).unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 6);
        for a in [1, 2, 3] {
            for b in [10, 20] {
                assert!(
                    rows.contains(&row(&[a, b])),
                    "missing combination ({a}, {b})"
                );
            }
        }
    }

    #[test]
    fn test_three_sources_produce_the_full_cross_product() {
        let mut join = UnionJoin::new(vec![
            src("a", &[1, 2]),
            src("b", &[10, 20]),
            src("c", &[100]),
        ])
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 4);
        for a in [1, 2] {
            for b in [10, 20] {
                assert!(
                    rows.contains(&row(&[a, b, 100])),
                    "missing combination ({a}, {b}, 100)"
                );
            }
        }
    }

    #[test]
    fn test_any_empty_source_makes_the_whole_join_empty() {
        let mut join = UnionJoin::new(vec![src("a", &[1, 2]), src("b", &[])]).unwrap();
        assert_eq!(drain(&mut join), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_zero_sources_produce_exactly_one_empty_row() {
        // A FROM-less SELECT (e.g. `SELECT 1+2`) still needs exactly one
        // output row to project against — the empty cross product is
        // conventionally a single zero-column row, not "no rows".
        let mut join = UnionJoin::new(vec![]).unwrap();
        assert_eq!(drain(&mut join), vec![Vec::<ValueItem>::new()]);
    }

    #[test]
    fn test_reset_lets_the_same_join_rescan_from_the_start() {
        let mut join = UnionJoin::new(vec![src("a", &[1, 2]), src("b", &[10, 20])]).unwrap();
        let first_pass = drain(&mut join);
        assert_eq!(first_pass.len(), 4);

        join.reset().unwrap();
        let second_pass = drain(&mut join);
        assert_eq!(second_pass, first_pass);
    }

    #[test]
    fn test_fields_concatenates_every_source_in_order() {
        let join = UnionJoin::new(vec![src("a", &[1]), src("b", &[2])]).unwrap();
        let names = join
            .fields()
            .iter()
            .map(|f| f.display_name.clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }
}
