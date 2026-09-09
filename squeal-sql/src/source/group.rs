use std::sync::Arc;

use store::valueitem::{IndexKey, ValueItem};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, funcs::FuncTrait},
    source::{ProjectableField, Source},
};

// Groups an already-sorted (by the GROUP BY key's raw field positions)
// raw-row source into one output row per group, evaluating the full
// SELECT list (both plain group-by-key columns and aggregate function
// calls) once per *group* rather than once per *input row* — unlike
// AggregatingSource (a plain whole-row dedup, correct for DISTINCT), an
// aggregate function needs to see every row in its group fed through its
// own accumulator (see FuncTrait::eval/reset) before the group's single
// output row can be produced. Evaluating the SELECT list eagerly per raw
// row and only deduping identical *results* afterward (what the DISTINCT
// path does, and what an earlier version of GROUP BY support tried to
// reuse) can't work here: an aggregate's own output column changes on
// every row it's fed, so no two rows in the same group would ever
// compare equal, and nothing would ever collapse.
//
// `key_positions` empty means "one implicit group covering the whole
// input" (a bare aggregate with no GROUP BY clause, e.g. `SELECT
// COUNT(*) FROM t`) — every row's (empty) key trivially equals every
// other's, so nothing ever splits into more than one group. That case
// also has to emit exactly one row even when the input has zero rows
// (COUNT(*) over an empty table is 0, not "no rows") — a real GROUP BY
// (key_positions non-empty) correctly must NOT do that; see `next()`'s
// handling of an empty first pull.
#[derive(Debug)]
pub(crate) struct GroupSource {
    source: Box<dyn Source>,
    fields: Vec<ProjectableField>,
    key_positions: Vec<usize>,
    // The next raw row to start a new group with, and its own group key
    // — already pulled from `source` while scanning ahead to find the
    // end of the *previous* group, so it can't be pulled again.
    pending: Option<(IndexKey, Vec<ValueItem>)>,
    started: bool,
    done: bool,
}

impl GroupSource {
    pub(crate) fn new(
        source: Box<dyn Source>,
        fields: Vec<ProjectableField>,
        key_positions: Vec<usize>,
    ) -> Self {
        Self {
            source,
            fields,
            key_positions,
            pending: None,
            started: false,
            done: false,
        }
    }

    fn key_of(&self, row: &IndexKey) -> Vec<ValueItem> {
        self.key_positions.iter().map(|&i| row[i].clone()).collect()
    }

    fn reset_aggregates(&self) -> Result<(), SchemaError> {
        for f in &self.fields {
            for func in f.expr.get_funcs() {
                func.reset()?;
            }
        }
        Ok(())
    }

    // Evaluates the full SELECT list against `row` — feeds every
    // aggregate function call one more row (mutating its own
    // accumulator via FuncTrait::eval), and, harmlessly, re-evaluates
    // each plain group-by-key column too (within one group they're
    // guaranteed to already agree, so re-evaluating them costs a little
    // but changes nothing). Only the *last* call's return value for a
    // given group is ever actually used — see `next()`.
    fn eval_row(&self, row: &IndexKey) -> Result<IndexKey, SchemaError> {
        let data = [row.clone()];
        let mut out = vec![];
        for (i, f) in self.fields.iter().enumerate() {
            out.push(f.expr.eval(&data, i)?);
        }
        Ok(IndexKey::new_from_owned(out)?)
    }

    // The one output row a grand-total aggregate (no GROUP BY at all)
    // still owes when the input is completely empty — each aggregate's
    // freshly-reset `current()` value, not `eval_row`'s `eval()`, which
    // would incorrectly count a row that was never actually there.
    // validate_aggregations (see logical.rs) already guarantees every
    // projected field is either a bare aggregate call or a GROUP BY key
    // column, and key_positions is empty in exactly this branch, so
    // there are no non-aggregate fields left to evaluate here.
    fn empty_group_row(&self) -> Result<IndexKey, SchemaError> {
        let mut out = vec![];
        for f in &self.fields {
            out.push(match &f.expr {
                EvalExpr::Function(func) => func.current(),
                _ => ValueItem::Null,
            });
        }
        Ok(IndexKey::new_from_owned(out)?)
    }
}

impl Source for GroupSource {
    fn fields(&self) -> Arc<[ProjectableField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if self.done {
            return Ok(None);
        }

        let first_row = if let Some((row, _key)) = self.pending.take() {
            Some(row)
        } else {
            self.source.next()?
        };

        let Some(mut current) = first_row else {
            self.done = true;
            if !self.started && self.key_positions.is_empty() {
                self.reset_aggregates()?;
                return Ok(Some(self.empty_group_row()?));
            }
            return Ok(None);
        };

        self.started = true;
        let current_key = self.key_of(&current);
        self.reset_aggregates()?;

        let mut last_evaluated = self.eval_row(&current)?;
        loop {
            match self.source.next()? {
                Some(next_row) => {
                    let next_key = self.key_of(&next_row);
                    if next_key == current_key {
                        current = next_row;
                        last_evaluated = self.eval_row(&current)?;
                    } else {
                        self.pending = Some((next_row, next_key));
                        return Ok(Some(last_evaluated));
                    }
                }
                None => {
                    self.done = true;
                    return Ok(Some(last_evaluated));
                }
            }
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        self.pending = None;
        self.started = false;
        self.done = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::{
        plan::funcs::{Count, FuncArgs, FuncObj},
        source::test_support::{VecSource, drain},
        table::Field,
    };

    fn key_field(name: &str, pos: usize) -> ProjectableField {
        ProjectableField::from_field(Arc::new(Field::from(name)), 0, pos)
    }

    fn count_star_field(name: &str) -> ProjectableField {
        ProjectableField::new_with_field(
            name.to_string(),
            Arc::new(Field::from(name)),
            0,
            0,
            EvalExpr::Function(FuncObj::Count(
                Count::new(vec![FuncArgs::Wildcard], false, None).unwrap(),
            )),
        )
    }

    #[test]
    fn test_group_by_collapses_rows_sharing_a_key_and_counts_each_group_independently() {
        // Regression test: the previous approach (evaluate COUNT(*) per
        // raw row via Projection, then dedup on the whole evaluated row)
        // never collapsed anything, since the count column changed on
        // every single row.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["category"],
            vec![
                vec![ValueItem::Str(("a".into(), 1))],
                vec![ValueItem::Str(("a".into(), 1))],
                vec![ValueItem::Str(("b".into(), 1))],
            ],
        ));
        let fields = vec![key_field("category", 0), count_star_field("count")];
        let mut group = GroupSource::new(source, fields, vec![0]);
        let rows = drain(&mut group);
        assert_eq!(
            rows,
            vec![
                vec![ValueItem::Str(("a".into(), 1)), ValueItem::Integer(2)],
                vec![ValueItem::Str(("b".into(), 1)), ValueItem::Integer(1)],
            ]
        );
    }

    #[test]
    fn test_group_by_with_no_rows_produces_no_rows() {
        let source: Box<dyn Source> = Box::new(VecSource::new(&["category"], vec![]));
        let fields = vec![key_field("category", 0), count_star_field("count")];
        let mut group = GroupSource::new(source, fields, vec![0]);
        assert_eq!(drain(&mut group), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_grand_total_with_no_group_by_key_collapses_everything_into_one_row() {
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["id"],
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ],
        ));
        let fields = vec![count_star_field("count")];
        let mut group = GroupSource::new(source, fields, vec![]);
        assert_eq!(drain(&mut group), vec![vec![ValueItem::Integer(3)]]);
    }

    #[test]
    fn test_grand_total_with_no_rows_still_emits_one_row_reporting_zero() {
        // COUNT(*) over an empty table is 0, not "no rows" — the one
        // case a real GROUP BY (non-empty key_positions) must NOT do,
        // since there every empty input correctly means zero groups.
        let source: Box<dyn Source> = Box::new(VecSource::new(&["id"], vec![]));
        let fields = vec![count_star_field("count")];
        let mut group = GroupSource::new(source, fields, vec![]);
        assert_eq!(drain(&mut group), vec![vec![ValueItem::Integer(0)]]);
    }

    #[test]
    fn test_group_by_resets_between_groups_not_just_at_the_very_start() {
        // Guards against a reset-once-at-construction bug: the second
        // group's count must not carry over the first group's total.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["category"],
            vec![
                vec![ValueItem::Str(("a".into(), 1))],
                vec![ValueItem::Str(("a".into(), 1))],
                vec![ValueItem::Str(("a".into(), 1))],
                vec![ValueItem::Str(("b".into(), 1))],
            ],
        ));
        let fields = vec![key_field("category", 0), count_star_field("count")];
        let mut group = GroupSource::new(source, fields, vec![0]);
        assert_eq!(
            drain(&mut group),
            vec![
                vec![ValueItem::Str(("a".into(), 1)), ValueItem::Integer(3)],
                vec![ValueItem::Str(("b".into(), 1)), ValueItem::Integer(1)],
            ]
        );
    }
}
