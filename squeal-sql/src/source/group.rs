use crate::source::{column_names, output_label, planinfo::PlanNode};
use std::{collections::HashMap, sync::Arc};
use store::clock::Instant;

use store::valueitem::{IndexKey, ValueItem};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, funcs::FuncTrait},
    source::{ProjectableField, QueryStats, Source, merge_stats},
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
    timer: crate::source::timing::RowTimer,
    timer_eval: crate::source::timing::RowTimer,
    source: Box<dyn Source>,
    fields: Vec<ProjectableField>,
    key_positions: Vec<usize>,
    // HAVING: read over the raw rows like the SELECT list (its aggregates
    // accumulate alongside), and a group is output only when it is true.
    having: Option<EvalExpr>,
    // The SELECT list's values as of the latest row evaluated: reused row
    // after row, an output row made of it once per group.
    values: Vec<ValueItem>,
    // The next raw row to start a new group with, and its own group key
    // — already pulled from `source` while scanning ahead to find the
    // end of the *previous* group, so it can't be pulled again.
    pending: Option<(IndexKey, Vec<ValueItem>)>,
    started: bool,
    done: bool,
    time_spent: u128,
    eval_time: u128,
}

impl GroupSource {
    pub(crate) fn new(
        source: Box<dyn Source>,
        fields: Vec<ProjectableField>,
        key_positions: Vec<usize>,
    ) -> Self {
        Self {
            timer: Default::default(),
            timer_eval: Default::default(),
            source,
            fields,
            key_positions,
            having: None,
            values: vec![],
            pending: None,
            started: false,
            done: false,
            time_spent: 0,
            eval_time: 0,
        }
    }

    pub(crate) fn with_having(mut self, having: EvalExpr) -> Self {
        self.having = Some(having);
        self
    }

    fn key_of(&self, row: &IndexKey) -> Vec<ValueItem> {
        self.key_positions.iter().map(|&i| row[i].clone()).collect()
    }

    fn reset_aggregates(&mut self) -> Result<(), SchemaError> {
        for f in &mut self.fields {
            for func in &mut f.expr.get_funcs() {
                func.reset()?;
            }
        }
        if let Some(having) = &mut self.having {
            for func in &mut having.get_funcs() {
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
    //
    // HAVING is evaluated the same way; the second value is whether the
    // group passes it, as of this row.
    //
    // The values go to `self.values`; next_group makes the output row of
    // the group's last.
    fn eval_row(&mut self, row: &IndexKey) -> Result<bool, SchemaError> {
        let start = self.timer_eval.start();
        let data = [row.clone()];
        self.values.clear();
        for (i, f) in self.fields.iter_mut().enumerate() {
            self.values.push(f.expr.eval(&data, i)?);
        }
        let keep = match &mut self.having {
            Some(having) => Self::is_true(having.eval(&data, 0)?)?,
            None => true,
        };
        crate::source::timing::add(&mut self.eval_time, start);
        Ok(keep)
    }

    // A NULL HAVING is "not true", as in WHERE: the group is left out.
    fn is_true(v: ValueItem) -> Result<bool, SchemaError> {
        match v {
            ValueItem::Boolean(b) => Ok(b),
            ValueItem::Null => Ok(false),
            _ => Err(SchemaError::InternalSchemaError(
                "Output of having is not boolean.".into(),
            )),
        }
    }

    // The one output row a grand-total aggregate (no GROUP BY at all)
    // still owes when the input is completely empty. Aggregate calls use
    // their freshly-`reset` `current()` value, not `eval_row`'s `eval()`,
    // which would incorrectly count a row that was never actually there.
    // Anything else (a scalar function call, or a bare literal) has no
    // accumulator to read `current()` from — validate_aggreations (see
    // logical.rs) guarantees such a field has no live column references
    // when key_positions is empty (no GROUP BY to source one from), so
    // it's safe to evaluate it directly against an empty row.
    //
    // None when HAVING rejects that group (`HAVING count(*) > 0`).
    fn empty_group_row(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = self.timer_eval.start();
        let mut out = vec![];
        for f in &self.fields {
            out.push(f.expr.eval_empty_group()?);
        }
        let keep = match &self.having {
            Some(having) => Self::is_true(having.eval_empty_group()?)?,
            None => true,
        };
        crate::source::timing::add(&mut self.eval_time, start);
        Ok(if keep {
            Some(IndexKey::new_from_owned(out)?)
        } else {
            None
        })
    }

    // The next group's output row and whether it passes HAVING; None once
    // the input is exhausted.
    fn next_group(&mut self) -> Result<Option<(IndexKey, bool)>, SchemaError> {
        let first_row = if let Some((row, _key)) = self.pending.take() {
            Some(row)
        } else {
            self.source.next()?
        };

        let Some(mut current) = first_row else {
            self.done = true;
            if !self.started && self.key_positions.is_empty() {
                self.reset_aggregates()?;
                return Ok(self.empty_group_row()?.map(|row| (row, true)));
            }
            return Ok(None);
        };

        self.started = true;
        let current_key = self.key_of(&current);
        self.reset_aggregates()?;

        let mut keep = self.eval_row(&current)?;
        loop {
            match self.source.next()? {
                Some(next_row) => {
                    let next_key = self.key_of(&next_row);
                    if next_key == current_key {
                        current = next_row;
                        keep = self.eval_row(&current)?;
                    } else {
                        self.pending = Some((next_row, next_key));
                        break;
                    }
                }
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        Ok(Some((IndexKey::new_from(&self.values)?, keep)))
    }
}

impl Source for GroupSource {
    fn plan(&self) -> PlanNode {
        let names = column_names(&self.source.fields());
        let outputs = self
            .fields
            .iter()
            .map(|f| output_label(f, &names))
            .collect::<Vec<_>>()
            .join(", ");
        let outputs = match &self.having {
            Some(having) => format!("{outputs} having {}", having.describe(&names)),
            None => outputs,
        };
        if self.key_positions.is_empty() {
            PlanNode::new("Aggregate")
                .detail(outputs)
                .child(self.source.plan())
        } else {
            let keys = self
                .key_positions
                .iter()
                .map(|k| names.get(*k).cloned().unwrap_or_else(|| format!("#{k}")))
                .collect::<Vec<_>>()
                .join(", ");
            PlanNode::new("GroupAggregate")
                .detail(format!("by {keys}: {outputs}"))
                .child(self.source.plan())
        }
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = self.timer.start();
        let mut out = None;
        while !self.done {
            match self.next_group()? {
                Some((row, true)) => {
                    out = Some(row);
                    break;
                }
                // A group HAVING rejects: on to the next one.
                Some((_, false)) => continue,
                None => break,
            }
        }
        crate::source::timing::add(&mut self.time_spent, start);
        Ok(out)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        self.pending = None;
        self.started = false;
        self.done = false;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        let query_stats = QueryStats {
            stats: HashMap::from([
                ("eval_ns".to_string(), self.eval_time as f64),
                ("time_ns".into(), self.time_spent as f64),
            ]),
            level: 0,
        };
        Some(merge_stats(
            vec![("GroupSource".into(), query_stats)],
            self.source.query_stats(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::plan::eval::EvalExpr;
    use crate::{
        plan::funcs::{Avg, Count, FuncArgs, FuncObj, Upper},
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

    fn avg_field(name: &str, pos: usize) -> ProjectableField {
        ProjectableField::new_with_field(
            name.to_string(),
            Arc::new(Field::from(name)),
            0,
            0,
            EvalExpr::Function(FuncObj::Avg(
                Avg::new(vec![FuncArgs::Field(Box::new(EvalExpr::Value(pos)))], None).unwrap(),
            )),
        )
    }

    // A scalar (non-aggregate) function call over a plain literal — legal
    // per validate_aggreations even with an aggregate in the same SELECT
    // list and no GROUP BY, since it has no live column reference. See
    // test_grand_total_with_scalar_function_alongside_aggregate_over_empty_table.
    fn upper_literal_field(name: &str, s: &str) -> ProjectableField {
        ProjectableField::new_with_field(
            name.to_string(),
            Arc::new(Field::from(name)),
            0,
            0,
            EvalExpr::Function(FuncObj::Upper(
                Upper::new(vec![FuncArgs::Field(Box::new(EvalExpr::Literal(
                    ValueItem::Str((s.to_string(), s.len() as u32)),
                )))])
                .unwrap(),
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

    // Regression: SortSource::reset() used to be a no-op — since
    // GroupSource::reset() delegates straight to its (sorting) source, a
    // re-scanned GROUP BY silently returned zero rows on the second pass
    // (results/progress were never cleared, and the first pass had already
    // drained them). Wires GroupSource over a real SortSource, the same
    // shape plan::logical actually builds for GROUP BY (SortSource::
    // with_fields, then GroupSource::new over it) — the group.rs tests
    // above all feed GroupSource pre-sorted data directly, which never
    // touches SortSource::reset() at all.
    #[test]
    fn test_group_by_over_a_real_sort_can_be_reset_and_rescanned() {
        use crate::plan::memory::QueryMemory;
        use crate::source::sort::{SortField, SortSource};

        let raw: Box<dyn Source> = Box::new(VecSource::new(
            &["cat"],
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(0)],
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(0)],
                vec![ValueItem::Integer(0)],
            ],
        ));
        let db = store::db::Db::<store::memfile::MemFile>::create("group_reset_test.db").unwrap();
        let sorted = SortSource::new(
            raw,
            &[SortField {
                asc: true,
                null_first: true,
                index: 0,
            }],
            None,
            db,
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let fields = vec![key_field("cat", 0), count_star_field("count")];
        let mut group = GroupSource::new(Box::new(sorted), fields, vec![0]);

        let first = drain(&mut group);
        assert_eq!(
            first,
            vec![
                vec![ValueItem::Integer(0), ValueItem::Integer(3)],
                vec![ValueItem::Integer(1), ValueItem::Integer(2)],
            ]
        );

        group.reset().unwrap();
        let second = drain(&mut group);
        assert_eq!(
            second, first,
            "a reset GROUP BY over a real sort must reproduce the same groups"
        );
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

    // Proves FuncObj's dispatch generalizes past a single aggregate
    // variant: get_funcs()/reset_aggregates() must find and reset *both*
    // the Count and the Avg call for every group, not just repeats of
    // whichever variant existed when that code was written.
    #[test]
    fn test_group_by_with_multiple_aggregate_variants_in_one_query() {
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["category", "amount"],
            vec![
                vec![ValueItem::Str(("a".into(), 1)), ValueItem::Integer(10)],
                vec![ValueItem::Str(("a".into(), 1)), ValueItem::Integer(20)],
                vec![ValueItem::Str(("b".into(), 1)), ValueItem::Integer(5)],
            ],
        ));
        let fields = vec![
            key_field("category", 0),
            count_star_field("count"),
            avg_field("avg_amount", 1),
        ];
        let mut group = GroupSource::new(source, fields, vec![0]);
        assert_eq!(
            drain(&mut group),
            vec![
                vec![
                    ValueItem::Str(("a".into(), 1)),
                    ValueItem::Integer(2),
                    ValueItem::Double(15.0)
                ],
                vec![
                    ValueItem::Str(("b".into(), 1)),
                    ValueItem::Integer(1),
                    ValueItem::Double(5.0)
                ],
            ]
        );
    }

    // Regression test for the get_funcs()/empty_group_row() aggregate-
    // awareness fix (see both methods' own doc comments). Before that
    // fix: reset_aggregates() would call FuncTrait::reset() on the
    // scalar UPPER(...) call, and empty_group_row() would call
    // FuncTrait::current() on it — Upper deliberately panics on both
    // (see funcs.rs), so this test would fail loudly instead of quietly
    // passing if that filtering regressed.
    #[test]
    fn test_grand_total_with_scalar_function_alongside_aggregate_over_empty_table() {
        let source: Box<dyn Source> = Box::new(VecSource::new(&["id"], vec![]));
        let fields = vec![
            count_star_field("count"),
            upper_literal_field("shout", "hello"),
        ];
        let mut group = GroupSource::new(source, fields, vec![]);
        assert_eq!(
            drain(&mut group),
            vec![vec![
                ValueItem::Integer(0),
                ValueItem::Str(("HELLO".into(), 5))
            ]]
        );
    }
}
