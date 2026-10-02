use std::{cmp::Ordering, sync::Arc};

use store::valueitem::{IndexKey, ValueItem};

use crate::{
    error::SchemaError,
    optim::table_stats::ComputedTableStat,
    source::{ProjectableField, QueryStats, Source, planinfo::PlanNode},
    table::SqlTable,
};

// Reads a partitioned table: the source of each partition it reads (those
// the query's conditions leave — see Partitioning::may_hold), one after the
// other. Every source has the table's row layout, so the rows pass through
// unchanged.
//
// With `order`, each source gives its rows in that order, and they are
// merged into it: always taking the lowest of the sources' next rows. The
// whole read is then in that order, as one partition's would be.
#[derive(Debug)]
pub(crate) struct AppendSource {
    table: Arc<SqlTable>,
    // The partitions read (positions in table.partitions), one per source.
    parts: Vec<usize>,
    sources: Vec<Box<dyn Source>>,
    fields: Arc<[ProjectableField]>,
    // (column, NULLs first), ascending: merge on these.
    order: Option<Vec<(usize, bool)>>,
    // Merging: each source's next row, None once it has none. Filled on
    // the first next().
    heads: Option<Vec<Option<IndexKey>>>,
    // The source the last row came from.
    current: usize,
    // Merging: the source whose row was handed out last, to be asked for
    // its next one on the next call — not before, as its last_id must stay
    // that row's until then.
    refill: Option<usize>,
    // Expected rows over the partitions read.
    rows: Option<usize>,
    // Per source: left unread, as no row it holds can be wanted (see
    // keep_matching).
    skip: Vec<bool>,
}

impl AppendSource {
    pub(crate) fn new(
        table: Arc<SqlTable>,
        parts: Vec<usize>,
        sources: Vec<Box<dyn Source>>,
        order: Option<Vec<(usize, bool)>>,
        rows: Option<usize>,
    ) -> Self {
        let fields = table
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>()
            .into();
        Self {
            table,
            parts,
            sources,
            fields,
            order,
            heads: None,
            current: 0,
            refill: None,
            rows,
            skip: vec![],
        }
    }

    // How two rows compare in `order`.
    fn cmp(order: &[(usize, bool)], a: &IndexKey, b: &IndexKey) -> Ordering {
        for (c, nulls_first) in order {
            let ord = match (&a[*c], &b[*c]) {
                (ValueItem::Null, ValueItem::Null) => Ordering::Equal,
                (ValueItem::Null, _) if *nulls_first => Ordering::Less,
                (ValueItem::Null, _) => Ordering::Greater,
                (_, ValueItem::Null) if *nulls_first => Ordering::Greater,
                (_, ValueItem::Null) => Ordering::Less,
                (x, y) => crate::numeric::cmp_mixed(x, y)
                    .flatten()
                    .unwrap_or_else(|| x.cmp(y)),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    fn next_merged(&mut self, order: &[(usize, bool)]) -> Result<Option<IndexKey>, SchemaError> {
        match (self.heads.as_mut(), self.refill.take()) {
            (None, _) => {
                let mut heads = Vec::with_capacity(self.sources.len());
                for (i, s) in self.sources.iter_mut().enumerate() {
                    heads.push(if self.skip.get(i) == Some(&true) {
                        None
                    } else {
                        s.next()?
                    });
                }
                self.heads = Some(heads);
            }
            (Some(heads), Some(i)) => heads[i] = self.sources[i].next()?,
            (Some(_), None) => {}
        }
        let heads = self.heads.as_mut().expect("filled above");
        // Ties go to the earlier source: partitions in their own order.
        let mut best: Option<usize> = None;
        for (i, h) in heads.iter().enumerate() {
            let Some(row) = h else { continue };
            match best {
                Some(b) if Self::cmp(order, row, heads[b].as_ref().unwrap()) != Ordering::Less => {}
                _ => best = Some(i),
            }
        }
        let Some(i) = best else { return Ok(None) };
        self.current = i;
        self.refill = Some(i);
        Ok(heads[i].take())
    }
}

// The source reading `table`'s partitions `parts` (positions in
// table.partitions): `open` for each. A table that is not partitioned is
// its one partition's source as it is. `order`: each partition's source
// gives its rows in this order, and so must the whole read (see
// AppendSource). `rows` is the expected row count, for EXPLAIN.
pub(crate) fn over_partitions(
    table: &Arc<SqlTable>,
    parts: &[usize],
    order: Option<&[(usize, bool)]>,
    rows: Option<usize>,
    mut open: impl FnMut(usize) -> Result<Box<dyn Source>, SchemaError>,
) -> Result<Box<dyn Source>, SchemaError> {
    if !table.is_partitioned()
        && let [only] = parts
    {
        return open(*only);
    }
    let sources = parts
        .iter()
        .map(|p| open(*p))
        .collect::<Result<Vec<_>, _>>()?;
    let order = order.filter(|o| parts.len() > 1 && !in_bound_order(table, o));
    Ok(Box::new(AppendSource::new(
        table.clone(),
        parts.to_vec(),
        sources,
        order.map(|o| o.to_vec()),
        rows,
    )))
}

// Whether reading a RANGE table's partitions one after the other, each in
// `order`, is already reading the table in `order`: the order leads with the
// partition column, and every value of a partition is below the next one's.
// NULLs, below every bound, are in the first partition, so they must come
// first too (or there must be none).
fn in_bound_order(table: &SqlTable, order: &[(usize, bool)]) -> bool {
    let (Some(by), Some((column, field)), Some((first, nulls_first))) =
        (&table.partitioning, table.partition_column(), order.first())
    else {
        return false;
    };
    by.kind == crate::partition::PartitionKind::Range
        && *first == column
        && (*nulls_first || !field.nullable)
}

// Every partition of `table`.
pub(crate) fn all_partitions(table: &SqlTable) -> Vec<usize> {
    (0..table.partitions.len()).collect()
}

impl Source for AppendSource {
    fn plan(&self) -> PlanNode {
        let skipped = self.skip.iter().filter(|s| **s).count();
        let how = if self.order.is_some() {
            "MergeAppend"
        } else {
            "Append"
        };
        let total = self.table.partitions.len();
        let read = self.parts.len();
        let detail = if read == total {
            format!("{} ({total} partitions)", self.table.name)
        } else {
            format!("{} ({read} of {total} partitions)", self.table.name)
        };
        // Only after a run (EXPLAIN builds no hash table): how many were
        // left unread for a join's keys.
        let detail = if skipped > 0 {
            format!("{detail}, {skipped} skipped for the join's keys")
        } else {
            detail
        };
        PlanNode::new(how)
            .detail(detail)
            .rows(self.rows)
            .children(self.sources.iter().map(|s| s.plan()).collect())
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if let Some(order) = self.order.clone() {
            return self.next_merged(&order);
        }
        while let Some(source) = self.sources.get_mut(self.current) {
            if self.skip.get(self.current) == Some(&true) {
                self.current += 1;
                continue;
            }
            if let Some(row) = source.next()? {
                return Ok(Some(row));
            }
            self.current += 1;
        }
        Ok(None)
    }

    // The row just yielded came from the source read last.
    fn last_id(&self) -> Option<store::tuple::DBIdType> {
        self.sources.get(self.current)?.last_id()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.reset()?;
        }
        self.current = 0;
        self.heads = None;
        self.refill = None;
        self.skip.clear();
        Ok(())
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.sources.first()?.table_stats()
    }

    // Only on the partition column: the partitions none of `values` routes
    // to are left unread.
    fn keep_matching(&mut self, column: usize, values: &[ValueItem]) {
        let (Some(by), Some((pc, field))) =
            (&self.table.partitioning, self.table.partition_column())
        else {
            return;
        };
        if pc != column {
            return;
        }
        let wanted: Vec<usize> = values
            .iter()
            .flat_map(|v| crate::plan::sarg::equal_points(field.datatype, v).unwrap_or_default())
            .filter_map(|v| by.route(&self.table.partitions, &v))
            .collect();
        if self.skip.is_empty() {
            self.skip = vec![false; self.parts.len()];
        }
        for (skip, p) in self.skip.iter_mut().zip(&self.parts) {
            *skip |= !wanted.contains(p);
        }
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let stats: Vec<_> = self
            .sources
            .iter()
            .filter_map(|s| s.query_stats())
            .flatten()
            .collect();
        (!stats.is_empty()).then_some(stats)
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn rows(values: &[Option<i64>]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["id"],
            values
                .iter()
                .map(|v| vec![v.map_or(ValueItem::Null, ValueItem::Integer)])
                .collect(),
        ))
    }

    fn ints(values: &[i64]) -> Box<dyn Source> {
        rows(&values.iter().map(|v| Some(*v)).collect::<Vec<_>>())
    }

    fn table() -> Arc<SqlTable> {
        let mut tb = crate::table::TableBuilder::new();
        tb.with_name("t".into());
        tb.with_field(&crate::table::Field::from("id"));
        Arc::new(tb.build().unwrap())
    }

    fn flat(rows: Vec<Vec<ValueItem>>) -> Vec<ValueItem> {
        rows.into_iter().flatten().collect()
    }

    #[test]
    fn test_append_reads_each_source_in_turn_and_again_after_a_reset() {
        let mut append = AppendSource::new(
            table(),
            vec![0, 1, 2],
            vec![ints(&[1, 2]), ints(&[]), ints(&[3])],
            None,
            None,
        );
        let all = vec![
            ValueItem::Integer(1),
            ValueItem::Integer(2),
            ValueItem::Integer(3),
        ];
        assert_eq!(flat(drain(&mut append)), all);
        assert!(append.next().unwrap().is_none(), "stays exhausted");
        append.reset().unwrap();
        assert_eq!(flat(drain(&mut append)), all);
    }

    #[test]
    fn test_merging_sorted_sources_gives_one_sorted_stream() {
        let merged = |order: Vec<(usize, bool)>, sources: Vec<Box<dyn Source>>| {
            let n = sources.len();
            let mut a = AppendSource::new(table(), (0..n).collect(), sources, Some(order), None);
            let first = flat(drain(&mut a));
            a.reset().unwrap();
            assert_eq!(flat(drain(&mut a)), first, "again after a reset");
            first
        };
        let int = |v: &[Option<i64>]| -> Vec<ValueItem> {
            v.iter()
                .map(|v| v.map_or(ValueItem::Null, ValueItem::Integer))
                .collect()
        };
        assert_eq!(
            merged(
                vec![(0, true)],
                vec![ints(&[1, 4, 9]), ints(&[]), ints(&[2, 3, 10]), ints(&[4])]
            ),
            int(&[
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(4),
                Some(9),
                Some(10)
            ])
        );
        // NULLs where the order puts them.
        assert_eq!(
            merged(
                vec![(0, true)],
                vec![rows(&[None, Some(5)]), rows(&[None, Some(1)])]
            ),
            int(&[None, None, Some(1), Some(5)])
        );
        assert_eq!(
            merged(
                vec![(0, false)],
                vec![rows(&[Some(5), None]), rows(&[Some(1), None])]
            ),
            int(&[Some(1), Some(5), None, None])
        );
        // No sources at all: no rows, and the table's columns.
        let mut empty = AppendSource::new(table(), vec![], vec![], None, None);
        assert!(empty.next().unwrap().is_none());
        assert_eq!(empty.fields().len(), 1);
    }
}
