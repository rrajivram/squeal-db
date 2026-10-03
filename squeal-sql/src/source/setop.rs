use std::{cmp::Ordering, sync::Arc};

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    source::{
        ProjectableField, Source,
        planinfo::PlanNode,
        sort::{SortField, cmp_by_fields},
    },
};

// UNION ALL: every row of each input, one input after the other. The
// inputs have the same number of columns; the rows keep the first input's
// column names.
#[derive(Debug)]
pub(crate) struct ConcatSource {
    sources: Vec<Box<dyn Source>>,
    current: usize,
}

impl ConcatSource {
    pub(crate) fn new(sources: Vec<Box<dyn Source>>) -> Self {
        Self {
            sources,
            current: 0,
        }
    }
}

impl Source for ConcatSource {
    fn plan(&self) -> PlanNode {
        PlanNode::new("UnionAll").children(self.sources.iter().map(|s| s.plan()).collect())
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.sources[0].fields()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        while let Some(source) = self.sources.get_mut(self.current) {
            if let Some(row) = source.next()? {
                return Ok(Some(row));
            }
            self.current += 1;
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.reset()?;
        }
        self.current = 0;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SetOp {
    Intersect,
    Except,
}

// INTERSECT / EXCEPT over two inputs that are each sorted on every column
// and free of duplicates: a merge, keeping the left rows the right input
// has (INTERSECT) or lacks (EXCEPT). Rows are equal when every column is —
// NULL equal to NULL, as SQL's set operations count them.
#[derive(Debug)]
pub(crate) struct MergeSetOp {
    op: SetOp,
    left: Box<dyn Source>,
    right: Box<dyn Source>,
    // The right input's current row; None before the first, Some(None) once
    // it is exhausted.
    right_row: Option<Option<IndexKey>>,
    order: Vec<SortField>,
}

impl MergeSetOp {
    pub(crate) fn new(op: SetOp, left: Box<dyn Source>, right: Box<dyn Source>) -> Self {
        let order = (0..left.fields().len())
            .map(|index| SortField {
                asc: true,
                null_first: true,
                index,
            })
            .collect();
        Self {
            op,
            left,
            right,
            right_row: None,
            order,
        }
    }
}

impl Source for MergeSetOp {
    fn plan(&self) -> PlanNode {
        PlanNode::new(match self.op {
            SetOp::Intersect => "Intersect",
            SetOp::Except => "Except",
        })
        .child(self.left.plan())
        .child(self.right.plan())
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.left.fields()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if self.right_row.is_none() {
            self.right_row = Some(self.right.next()?);
        }
        while let Some(left) = self.left.next()? {
            // The right input up to the first row not below this one.
            let found = loop {
                let Some(Some(right)) = &self.right_row else {
                    break false;
                };
                match cmp_by_fields(right, &left, &self.order) {
                    Ordering::Less => self.right_row = Some(self.right.next()?),
                    Ordering::Equal => break true,
                    Ordering::Greater => break false,
                }
            };
            if found == (self.op == SetOp::Intersect) {
                return Ok(Some(left));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.left.reset()?;
        self.right.reset()?;
        self.right_row = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn rows(values: &[Option<i64>]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["n"],
            values
                .iter()
                .map(|v| vec![v.map_or(ValueItem::Null, ValueItem::Integer)])
                .collect(),
        ))
    }

    fn flat(rows: Vec<Vec<ValueItem>>) -> Vec<Option<i64>> {
        rows.into_iter()
            .map(|r| match r[0] {
                ValueItem::Integer(n) => Some(n),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_merging_sorted_distinct_inputs_intersects_and_subtracts() {
        let run = |op, l: &[Option<i64>], r: &[Option<i64>]| {
            let mut s = MergeSetOp::new(op, rows(l), rows(r));
            let first = flat(drain(&mut s));
            s.reset().unwrap();
            assert_eq!(flat(drain(&mut s)), first, "again after a reset");
            first
        };
        let (l, r) = (
            [None, Some(1), Some(3), Some(5), Some(7)],
            [None, Some(2), Some(3), Some(7), Some(9)],
        );
        assert_eq!(run(SetOp::Intersect, &l, &r), [None, Some(3), Some(7)]);
        assert_eq!(run(SetOp::Except, &l, &r), [Some(1), Some(5)]);
        assert_eq!(run(SetOp::Except, &l, &[]), l);
        assert_eq!(run(SetOp::Intersect, &[], &r), []);
        let mut all = ConcatSource::new(vec![rows(&[Some(1)]), rows(&[]), rows(&[Some(1), None])]);
        assert_eq!(flat(drain(&mut all)), [Some(1), Some(1), None]);
    }
}
