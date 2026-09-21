use crate::source::{planinfo::PlanNode};
use std::{collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc, time::Instant};

use sql_parser::expr::BinaryOp;
use store::{
    db::{DBFile, Db},
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, memory::QueryMemory},
    source::{
        ComputedTableStat, ProjectableField, QueryStats, Source, hash::HashedSource, merge_stats,
    },
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

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
    time_spent: u128,
}

// Hash-join wrapper: resolves `on_expr` down to the equi-join field
// positions HashedSource needs, then delegates the actual build/probe/
// scan work (including LEFT/RIGHT/FULL's unmatched-row handling) to it
// entirely — see HashedSource::next's own doc comments.
pub(crate) struct JoinSource<F: DBFile + 'static> {
    fields: Arc<[ProjectableField]>,
    join_type: JoinType,
    source: Box<dyn Source>,
    _phantom: PhantomData<F>,
}

impl<F: DBFile + 'static> JoinSource<F> {
    pub(crate) fn new(
        left_source: Box<dyn Source>,
        right_source: Box<dyn Source>,
        on_expr: EvalExpr,
        join_type: JoinType,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
    ) -> Result<Self, SchemaError> {
        let fields = Arc::from(
            left_source
                .fields()
                .iter()
                .cloned()
                .chain(right_source.fields().iter().cloned())
                .collect::<Vec<_>>(),
        );
        // CROSS JOIN has no ON clause at all (on_expr is EvalExpr::None
        // — see get_tables) — equi_join_fields has no case for that and
        // would reject it outright, so it must never be called for this
        // join type. Every other type genuinely needs the equi-join
        // field positions to build its HashedSource.
        let source: Box<dyn Source> = match join_type {
            JoinType::Cross => Box::new(UnionJoin::new(vec![left_source, right_source])?),
            _ => {
                let left_field_count = left_source.fields().len();
                let (left_fields, right_fields) =
                    equi_join_fields(&on_expr, left_field_count).map_err(SchemaError::UserError)?;
                Box::new(HashedSource::new(
                    left_source,
                    right_source,
                    db,
                    mem,
                    &left_fields,
                    &right_fields,
                    join_type,
                )?)
            }
        };
        Ok(Self {
            fields,
            join_type,
            source,
            _phantom: PhantomData,
        })
    }
}

// Resolves an ON clause down to the equi-join field positions
// HashedSource needs: a plain equality between a left and a right
// column (`a.x = b.y`), or an AND-chain of such (composite keys). Every
// EvalExpr::Value(pos) here is a flat offset into the combined left++
// right row (see EvalExpr::Value's own doc comment) — positions below
// `left_field_count` are left columns, at or above it are right columns
// (offset by `left_field_count` to become right-row-relative, which is
// what HashedSource's own left_fields/right_fields expect: positions
// within each SIDE's own row, not the combined space).
fn equi_join_fields(
    on_expr: &EvalExpr,
    left_field_count: usize,
) -> Result<(Vec<usize>, Vec<usize>), String> {
    let mut left_fields = vec![];
    let mut right_fields = vec![];
    collect_equi_join_fields(
        on_expr,
        left_field_count,
        &mut left_fields,
        &mut right_fields,
    )?;
    Ok((left_fields, right_fields))
}

fn collect_equi_join_fields(
    expr: &EvalExpr,
    left_field_count: usize,
    left_fields: &mut Vec<usize>,
    right_fields: &mut Vec<usize>,
) -> Result<(), String> {
    match expr {
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => {
            collect_equi_join_fields(lhs, left_field_count, left_fields, right_fields)?;
            collect_equi_join_fields(rhs, left_field_count, left_fields, right_fields)
        }
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::Eq,
            rhs,
        } => match (lhs.as_ref(), rhs.as_ref()) {
            (EvalExpr::Value(l), EvalExpr::Value(r)) => {
                let (left_pos, right_pos) = match (*l < left_field_count, *r < left_field_count) {
                    (true, false) => (*l, *r - left_field_count),
                    (false, true) => (*r, *l - left_field_count),
                    _ => {
                        return Err(
                            "hash join ON clause must equate one left column with one right \
                             column, not two columns from the same side"
                                .into(),
                        );
                    }
                };
                left_fields.push(left_pos);
                right_fields.push(right_pos);
                Ok(())
            }
            _ => Err(
                "hash join only supports equi-join conditions between plain columns, not \
                      computed expressions"
                    .into(),
            ),
        },
        _ => Err(
            "hash join ON clause must be an equality, or an AND of equalities, between a \
                  left and a right column"
                .into(),
        ),
    }
}

impl<F: DBFile + 'static> Source for JoinSource<F> {
    // A pass-through: the join algorithm underneath is the plan step.
    fn plan(&self) -> PlanNode {
        self.source.plan()
    }


    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        self.source.next()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        self.source.query_stats()
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.source.table_stats()
    }
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
            time_spent: 0,
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
    fn plan(&self) -> PlanNode {
        PlanNode::new("CrossJoin").children(self.sources.iter().map(|s| s.plan()).collect())
    }


    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        if self.current.is_none() {
            // First call: seed one row from every source. A source with
            // no rows at all makes the whole cross product empty — a
            // cross join against nothing has nothing on either side,
            // same as relational algebra's empty-relation rule.
            let mut rows = Vec::with_capacity(self.sources.len());
            for s in &mut self.sources {
                match s.next()? {
                    Some(row) => rows.push(row),
                    None => {
                        self.time_spent += start.elapsed().as_nanos();
                        return Ok(None);
                    }
                }
            }
            // Zero sources (a `SELECT` with no FROM at all) is the one
            // legitimate case where `rows` stays empty — conventionally a
            // single row with zero columns, not "no rows", matching how
            // a FROM-less SELECT (`SELECT 1+2`) is expected to still
            // produce exactly one output row.
            let combined = Self::combine(&rows)?;
            self.current = Some(rows);
            self.time_spent += start.elapsed().as_nanos();
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
                self.time_spent += start.elapsed().as_nanos();
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
        let out = Self::combine(self.current.as_ref().unwrap())?;
        self.time_spent += start.elapsed().as_nanos();
        Ok(Some(out))
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.as_mut().reset()?;
        }
        self.current = None;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let mut res = vec![(
            "UnionJoin".to_string(),
            QueryStats {
                stats: HashMap::from([("time_ns".into(), self.time_spent as f64)]),
                level: 0,
            },
        )];
        // Every source's own subtree folds in as a sibling, one level
        // deeper than this UnionJoin's own entry — including the common
        // single-source case (a query with no comma-joined FROM list
        // still passes through exactly one UnionJoin), so the indent
        // consistently reflects that this node really does sit in the
        // pipeline, doing real (if usually small) row-combining work,
        // same convention HashedSource uses for its own left/right
        // children.
        for s in &self.sources {
            res = merge_stats(res, s.query_stats());
        }
        Some(res)
    }
}

impl<F: DBFile + 'static> Debug for JoinSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Join")
            .field("type", &self.join_type)
            .field("hashed", &self.source)
            .finish()
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

#[cfg(test)]
mod hash_join_tests {
    use store::{db::Db, memfile::MemFile, valueitem::ValueItem};

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn make_db() -> Arc<Db<MemFile>> {
        Db::<MemFile>::create("join_source_test.db").unwrap()
    }

    fn left_source() -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["id", "val"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(100)],
                vec![ValueItem::Integer(2), ValueItem::Integer(200)],
                vec![ValueItem::Integer(3), ValueItem::Integer(300)],
            ],
        ))
    }

    fn right_source() -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![
                vec![ValueItem::Integer(2), ValueItem::Integer(9002)],
                vec![ValueItem::Integer(3), ValueItem::Integer(9003)],
                vec![ValueItem::Integer(99), ValueItem::Integer(9099)],
            ],
        ))
    }

    // left.id (position 0) = right.user_id (flat position 2, since left
    // has 2 columns).
    fn on_id_eq_user_id() -> EvalExpr {
        EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Value(0)),
            op: BinaryOp::Eq,
            rhs: Box::new(EvalExpr::Value(2)),
        }
    }

    #[test]
    fn test_inner_join_emits_only_matched_pairs() {
        let mut join = JoinSource::new(
            left_source(),
            right_source(),
            on_id_eq_user_id(),
            JoinType::Inner,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(rows.contains(&vec![
            ValueItem::Integer(2),
            ValueItem::Integer(200),
            ValueItem::Integer(2),
            ValueItem::Integer(9002),
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Integer(3),
            ValueItem::Integer(300),
            ValueItem::Integer(3),
            ValueItem::Integer(9003),
        ]));
    }

    #[test]
    fn test_left_join_emits_unmatched_left_rows_paired_with_right_nulls() {
        let mut join = JoinSource::new(
            left_source(),
            right_source(),
            on_id_eq_user_id(),
            JoinType::Left,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 3, "{rows:?}"); // ids 1,2,3 — 1 unmatched
        assert!(rows.contains(&vec![
            ValueItem::Integer(1),
            ValueItem::Integer(100),
            ValueItem::Null,
            ValueItem::Null,
        ]));
    }

    #[test]
    fn test_right_join_emits_unmatched_right_rows_paired_with_left_nulls() {
        let mut join = JoinSource::new(
            left_source(),
            right_source(),
            on_id_eq_user_id(),
            JoinType::Right,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 3, "{rows:?}"); // right rows 2,3,99 — 99 unmatched
        assert!(rows.contains(&vec![
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Integer(99),
            ValueItem::Integer(9099),
        ]));
    }

    #[test]
    fn test_full_join_emits_unmatched_rows_from_both_sides() {
        let mut join = JoinSource::new(
            left_source(),
            right_source(),
            on_id_eq_user_id(),
            JoinType::Full,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(rows.len(), 4, "{rows:?}");
        assert!(rows.contains(&vec![
            ValueItem::Integer(1),
            ValueItem::Integer(100),
            ValueItem::Null,
            ValueItem::Null,
        ]));
        assert!(rows.contains(&vec![
            ValueItem::Null,
            ValueItem::Null,
            ValueItem::Integer(99),
            ValueItem::Integer(9099),
        ]));
    }

    #[test]
    fn test_on_expr_comparing_two_columns_from_the_same_side_is_rejected() {
        // left.id (0) = left.val (1) — both positions are < left_field_count.
        let bad_on = EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Value(0)),
            op: BinaryOp::Eq,
            rhs: Box::new(EvalExpr::Value(1)),
        };
        let result = JoinSource::new(
            left_source(),
            right_source(),
            bad_on,
            JoinType::Inner,
            make_db(),
            QueryMemory::new(1024 * 1024),
        );
        assert!(
            matches!(result, Err(SchemaError::UserError(_))),
            "{result:?}"
        );
    }

    #[test]
    fn test_composite_equi_join_key_with_and() {
        // left: (id, val) both must match right: (user_id, amount) at
        // flat positions 2 and 3.
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(10)],
                vec![ValueItem::Integer(1), ValueItem::Integer(20)],
            ],
        ));
        let right = Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(20)]],
        ));
        let on_expr = EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Binary {
                lhs: Box::new(EvalExpr::Value(0)),
                op: BinaryOp::Eq,
                rhs: Box::new(EvalExpr::Value(2)),
            }),
            op: BinaryOp::And,
            rhs: Box::new(EvalExpr::Binary {
                lhs: Box::new(EvalExpr::Value(1)),
                op: BinaryOp::Eq,
                rhs: Box::new(EvalExpr::Value(3)),
            }),
        };
        let mut join = JoinSource::new(
            left,
            right,
            on_expr,
            JoinType::Inner,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        assert_eq!(
            rows,
            vec![vec![
                ValueItem::Integer(1),
                ValueItem::Integer(20),
                ValueItem::Integer(1),
                ValueItem::Integer(20),
            ]],
            "only the (id=1, val=20) left row matches (user_id=1, amount=20) on both key parts"
        );
    }

    // Regression test: multiple right rows matching the same left key
    // used to silently collapse to just the last one probed
    // (HashedSource.HashValue.right_value was a single Option<IndexKey>
    // per left-row slot). HashedSource was since rewritten to stream
    // the right side and walk the full probe chain per right row
    // instead of pre-computing a single match into the table, which
    // fixes this at the source JoinSource delegates to.
    #[test]
    fn test_multiple_right_matches_for_the_same_left_key_all_produce_output_rows() {
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(100)]],
        ));
        let right = Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(9001)],
                vec![ValueItem::Integer(1), ValueItem::Integer(9002)],
                vec![ValueItem::Integer(1), ValueItem::Integer(9003)],
            ],
        ));
        let mut join = JoinSource::new(
            left,
            right,
            on_id_eq_user_id(),
            JoinType::Inner,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        let amounts: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| match r[3] {
                ValueItem::Integer(v) => v,
                _ => panic!("expected an integer"),
            })
            .collect();
        assert_eq!(
            amounts,
            [9001, 9002, 9003].into_iter().collect(),
            "every matching right row must produce its own output row: {rows:?}"
        );
    }

    // Mirror case: multiple LEFT rows sharing a key, one matching right
    // row — every left row must also get its own output row.
    #[test]
    fn test_multiple_left_matches_for_the_same_right_key_all_produce_output_rows() {
        let left = Box::new(VecSource::new(
            &["id", "val"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(10)],
                vec![ValueItem::Integer(1), ValueItem::Integer(20)],
                vec![ValueItem::Integer(1), ValueItem::Integer(30)],
            ],
        ));
        let right = Box::new(VecSource::new(
            &["user_id", "amount"],
            vec![vec![ValueItem::Integer(1), ValueItem::Integer(9001)]],
        ));
        let mut join = JoinSource::new(
            left,
            right,
            on_id_eq_user_id(),
            JoinType::Inner,
            make_db(),
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let rows = drain(&mut join);
        let vals: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| match r[1] {
                ValueItem::Integer(v) => v,
                _ => panic!("expected an integer"),
            })
            .collect();
        assert_eq!(
            vals,
            [10, 20, 30].into_iter().collect(),
            "every matching left row must produce its own output row: {rows:?}"
        );
    }
}
