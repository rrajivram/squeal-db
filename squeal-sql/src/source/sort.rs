use std::{cmp::Ordering, collections::BinaryHeap, fmt::Debug, slice::Iter, sync::Arc};

use sql_parser::{keyword::No, query::OrderByClause};
use store::{
    db::{DBFile, Db},
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, logical::TableQuery},
    source::Source,
};

#[derive(Debug, Clone)]
pub(crate) struct SortField {
    expr: EvalExpr,
    asc: bool,
    null_first: bool,
    index: usize,
}

pub(crate) struct SortSource<F: DBFile + 'static> {
    source: Box<dyn Source>,
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    results: Option<Vec<IndexKey>>,
    db: Arc<Db<F>>,
}

#[derive(Debug)]
struct CrateItem<'a> {
    key: IndexKey,
    order: &'a Vec<SortField>,
}

#[derive(Debug)]
struct CrateHeap<'a> {
    heap: BinaryHeap<CrateItem<'a>>,
    source: Vec<SortField>,
    limit: usize,
}

impl<F> SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn create_from(
        source: Box<dyn Source>,
        clause: &OrderByClause,
        tables: &[TableQuery<F>],
        limit: Option<usize>,
        db: Arc<Db<F>>,
    ) -> Result<Self, SchemaError> {
        let mut items = vec![];
        for c in clause.items.items() {
            let expr = EvalExpr::from_expr(&c.expr, tables)?;
            let index = match expr.as_ref() {
                EvalExpr::Value(n) => *n,
                _ => {
                    return Err(SchemaError::UnknownError(
                        "Do not know how to process non-value sort value".into(),
                    ));
                }
            };
            let asc = c.direction.map(|a| a.is_left()).unwrap_or(true);
            let null_first = c.nulls.map(|(_, n)| n.is_left()).unwrap_or(false);
            items.push(SortField {
                asc,
                expr: *expr,
                null_first,
                index,
            });
        }
        Ok(Self {
            sort_fields: items,
            source,
            limit,
            results: None,
            db,
        })
    }
}

impl<F> Source for SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn fields(&self) -> Arc<[super::ProjectableField]> {
        self.source.fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        if let Some(results) = &mut self.results {
            return Ok(results.pop().to_owned());
        }
        let mut limited = false;
        if let Some(_limit) = self.limit {
            limited = true;
        }
        if limited {
            let mut heap = CrateHeap::new(self);
            while let Some(rec) = self.source.next()? {
                heap.push(CrateItem {
                    key: rec.to_owned(),
                    order: &self.sort_fields,
                });
            }
            // into_sorted_vec() returns ascending order, but next() reads
            // it back with .pop() (removes from the *end*) — reversed
            // here so the first pop() yields the smallest (ASC-first)
            // item, not the largest.
            let mut sorted = heap
                .heap
                .into_sorted_vec()
                .drain(..)
                .map(|i| i.key)
                .collect::<Vec<_>>();
            sorted.reverse();
            self.results = Some(sorted);
            self.next()
        } else {
            todo!()
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }
}

impl<F> Debug for SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortSource")
            .field("fields", &self.sort_fields)
            .finish()
    }
}

impl<'a> CrateHeap<'a> {
    fn new<F: DBFile + 'static>(source: &SortSource<F>) -> Self {
        Self {
            source: source.sort_fields.clone(),
            heap: BinaryHeap::new(),
            limit: source.limit.unwrap(),
        }
    }

    fn push(&mut self, item: CrateItem<'a>) {
        self.heap.push(item);
        // Strictly greater, not equal: this is a max-heap holding the
        // smallest `limit` items seen so far (ASC) by always evicting the
        // current worst offender once there's one too many. Evicting the
        // moment the heap merely *reaches* `limit` (as opposed to
        // exceeding it) throws away a row that belongs in the result on
        // every single push from then on — with exactly `limit` total
        // rows, this used to end up one row short every time.
        if self.heap.len() > self.limit {
            self.heap.pop();
        }
    }
}

impl<'a> Ord for CrateItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut results = vec![];
        // Iterate the ORDER BY clauses themselves, in their own order —
        // not self.key.values() — and use each one's own `index` (its
        // flat position in the row, resolved back in create_from) to
        // pull its value out of the row. `ORDER BY name` on a 3-column
        // `SELECT *` has exactly one sort field but a 3-value row; the
        // two lengths only ever coincide when every projected column is
        // also an ORDER BY key, which isn't the general case.
        for field in self.order.iter() {
            let lhs = &self.key.values()[field.index];
            let rhs = &other.key.values()[field.index];
            match (lhs, rhs) {
                // Both NULL on this column is a tie — fall through to the
                // next sort key, not a forced Less/Greater.
                (ValueItem::Null, ValueItem::Null) => results.push(Ordering::Equal),
                // Only one side is NULL: which side it's on decides the
                // verdict, not just null_first alone — self holding NULL
                // and other holding NULL need opposite answers for the
                // same null_first setting, or cmp(a,b)/cmp(b,a) stop being
                // exact opposites (a real Ord violation: sort()/BinaryHeap
                // both assume that).
                (ValueItem::Null, _) => {
                    if field.null_first {
                        results.push(Ordering::Less);
                    } else {
                        results.push(Ordering::Greater);
                    }
                }
                (_, ValueItem::Null) => {
                    if field.null_first {
                        results.push(Ordering::Greater);
                    } else {
                        results.push(Ordering::Less);
                    }
                }
                _ => {
                    if field.asc {
                        results.push(lhs.cmp(rhs));
                    } else {
                        results.push(rhs.cmp(lhs));
                    }
                }
            }
        }
        let iter = results.iter();
        for order in iter {
            if !matches!(order, Ordering::Equal) {
                return *order;
            }
        }
        Ordering::Equal
    }
}

impl<'a> PartialOrd for CrateItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for CrateItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for CrateItem<'a> {}

#[cfg(test)]
mod tests {
    use store::valueitem::{IndexKey, ValueItem};

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    // Only asc/null_first/index matter to CrateItem::cmp — expr is never
    // read by it (that's SortSource::next's own concern, once it's
    // implemented), so a dummy literal is fine here.
    fn field(index: usize, asc: bool, null_first: bool) -> SortField {
        SortField {
            expr: EvalExpr::Literal(ValueItem::Null),
            asc,
            null_first,
            index,
        }
    }

    fn key(values: &[ValueItem]) -> IndexKey {
        IndexKey::new_from(values).unwrap()
    }

    fn item<'a>(key: &IndexKey, order: &'a Vec<SortField>) -> CrateItem<'a> {
        CrateItem {
            key: key.clone(),
            order,
        }
    }

    #[test]
    fn test_single_column_ascending() {
        let order = vec![field(0, true, false)];
        let a = key(&[ValueItem::Integer(1)]);
        let b = key(&[ValueItem::Integer(2)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Less);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Greater);
    }

    #[test]
    fn test_single_column_descending() {
        let order = vec![field(0, false, false)];
        let a = key(&[ValueItem::Integer(1)]);
        let b = key(&[ValueItem::Integer(2)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Greater);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Less);
    }

    #[test]
    fn test_nulls_first_sorts_null_before_non_null_regardless_of_which_side_is_null() {
        let order = vec![field(0, true, true)];
        let null_key = key(&[ValueItem::Null]);
        let val_key = key(&[ValueItem::Integer(1)]);
        assert_eq!(
            item(&null_key, &order).cmp(&item(&val_key, &order)),
            Ordering::Less,
            "null should sort first"
        );
        assert_eq!(
            item(&val_key, &order).cmp(&item(&null_key, &order)),
            Ordering::Greater,
            "compared the other way round, the non-null side must consistently be Greater"
        );
    }

    #[test]
    fn test_nulls_last_sorts_null_after_non_null_regardless_of_which_side_is_null() {
        let order = vec![field(0, true, false)];
        let null_key = key(&[ValueItem::Null]);
        let val_key = key(&[ValueItem::Integer(1)]);
        assert_eq!(
            item(&null_key, &order).cmp(&item(&val_key, &order)),
            Ordering::Greater,
            "null should sort last"
        );
        assert_eq!(
            item(&val_key, &order).cmp(&item(&null_key, &order)),
            Ordering::Less
        );
    }

    #[test]
    fn test_two_nulls_on_the_same_column_are_a_tie() {
        let order = vec![field(0, true, true)];
        let a = key(&[ValueItem::Null]);
        let b = key(&[ValueItem::Null]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Equal);
    }

    #[test]
    fn test_cmp_is_antisymmetric_for_every_null_combination() {
        for null_first in [true, false] {
            let order = vec![field(0, true, null_first)];
            let null_key = key(&[ValueItem::Null]);
            let val_key = key(&[ValueItem::Integer(1)]);
            let forward = item(&null_key, &order).cmp(&item(&val_key, &order));
            let backward = item(&val_key, &order).cmp(&item(&null_key, &order));
            assert_eq!(
                backward,
                forward.reverse(),
                "cmp(a,b) and cmp(b,a) must be exact opposites (null_first={null_first})"
            );
        }
    }

    #[test]
    fn test_multi_column_secondary_breaks_tie_on_primary() {
        // ORDER BY col0 ASC, col1 DESC
        let order = vec![field(0, true, false), field(1, false, false)];
        let a = key(&[ValueItem::Integer(1), ValueItem::Integer(10)]);
        let b = key(&[ValueItem::Integer(1), ValueItem::Integer(20)]);
        // Same col0 (tie) -> col1 DESC means the larger col1 sorts first.
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Greater);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Less);
    }

    #[test]
    fn test_multi_column_primary_decides_when_it_differs() {
        let order = vec![field(0, true, false), field(1, false, false)];
        let a = key(&[ValueItem::Integer(1), ValueItem::Integer(999)]);
        let b = key(&[ValueItem::Integer(2), ValueItem::Integer(1)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Less);
    }

    #[test]
    fn test_sorting_a_vec_end_to_end_matches_expected_order() {
        let order = vec![field(0, true, true)]; // ASC, NULLS FIRST
        let keys = vec![
            key(&[ValueItem::Integer(3)]),
            key(&[ValueItem::Null]),
            key(&[ValueItem::Integer(1)]),
            key(&[ValueItem::Integer(2)]),
        ];
        let mut items: Vec<CrateItem> = keys.iter().map(|k| item(k, &order)).collect();
        items.sort();
        let sorted_vals: Vec<&ValueItem> = items.iter().map(|it| &it.key.values()[0]).collect();
        assert_eq!(
            sorted_vals,
            vec![
                &ValueItem::Null,
                &ValueItem::Integer(1),
                &ValueItem::Integer(2),
                &ValueItem::Integer(3)
            ]
        );
    }

    // Pinned to MemFile, matching this codebase's usual test convention
    // (a fresh in-memory Db, cheap, isolated) — nothing in these tests
    // exercises `db` itself yet (it's only used by the `else { todo!() }`
    // unlimited-sort branch, for building Runs), so any valid handle
    // will do.
    fn sort_source(
        source: Box<dyn Source>,
        sort_fields: Vec<SortField>,
        limit: usize,
    ) -> SortSource<store::memfile::MemFile> {
        // MemFile hands back a fresh, independent in-memory buffer per
        // create() call regardless of name, so a fixed literal is fine.
        let db = store::db::Db::<store::memfile::MemFile>::create("sort_test").unwrap();
        SortSource {
            source,
            sort_fields,
            limit: Some(limit),
            results: None,
            db,
        }
    }

    fn str_row(rows: &[&str]) -> Vec<Vec<ValueItem>> {
        rows.iter()
            .map(|s| vec![ValueItem::Str((s.to_string(), s.len() as u32))])
            .collect()
    }

    #[test]
    fn test_sort_source_limit_equal_to_row_count_returns_every_row_in_order() {
        // Regression test: CrateHeap::push used to evict as soon as the
        // heap merely *reached* `limit` (not exceeded it), so a
        // limit-equals-row-count query — the ordinary case for a small
        // table — silently lost one row every time.
        let source: Box<dyn Source> =
            Box::new(VecSource::new(&["name"], str_row(&["raj", "kav", "gan"])));
        let mut sort = sort_source(source, vec![field(0, true, false)], 3);
        assert_eq!(drain(&mut sort), str_row(&["gan", "kav", "raj"]));
    }

    #[test]
    fn test_sort_source_limit_smaller_than_row_count_keeps_the_smallest_asc() {
        let source: Box<dyn Source> =
            Box::new(VecSource::new(&["name"], str_row(&["raj", "kav", "gan"])));
        let mut sort = sort_source(source, vec![field(0, true, false)], 2);
        assert_eq!(drain(&mut sort), str_row(&["gan", "kav"]));
    }

    #[test]
    fn test_sort_source_returns_rows_in_ascending_order_not_reversed() {
        // Regression test: into_sorted_vec() is ascending, but next()
        // used to read it back with Vec::pop() (removes from the end)
        // without reversing first, so results came out descending.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["n"],
            vec![
                vec![ValueItem::Integer(3)],
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
            ],
        ));
        let mut sort = sort_source(source, vec![field(0, true, false)], 3);
        assert_eq!(
            drain(&mut sort),
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ]
        );
    }

    #[test]
    fn test_sort_field_index_can_point_into_a_wider_row() {
        // Regression test for the actual reported crash: `SELECT *
        // FROM t1 ORDER BY name` on a 3-column table has one SortField
        // but a 3-value row per record — the field's own `index` (not
        // its position among the sort fields) must be used to find its
        // value in the row.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["id", "name"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Str(("raj".into(), 3))],
                vec![ValueItem::Integer(2), ValueItem::Str(("kav".into(), 3))],
                vec![ValueItem::Integer(5), ValueItem::Str(("gan".into(), 3))],
            ],
        ));
        // ORDER BY name (row position 1), not id.
        let mut sort = sort_source(source, vec![field(1, true, false)], 3);
        let rows = drain(&mut sort);
        let names: Vec<&ValueItem> = rows.iter().map(|r| &r[1]).collect();
        assert_eq!(
            names,
            vec![
                &ValueItem::Str(("gan".into(), 3)),
                &ValueItem::Str(("kav".into(), 3)),
                &ValueItem::Str(("raj".into(), 3)),
            ]
        );
    }
}
