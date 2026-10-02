use std::sync::Arc;

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    optim::table_stats::ComputedTableStat,
    source::{ProjectableField, QueryStats, Source, planinfo::PlanNode},
    table::SqlTable,
};

// Reads a partitioned table: each partition's own source, one after the
// other. Every source has the table's row layout, so the rows pass through
// unchanged; they come out in no order across partitions, whatever order
// each partition's source gives.
#[derive(Debug)]
pub(crate) struct AppendSource {
    table: Arc<SqlTable>,
    sources: Vec<Box<dyn Source>>,
    // The source being read.
    current: usize,
    // Expected rows over all partitions.
    rows: Option<usize>,
}

impl AppendSource {
    // `sources` is one per partition of `table`, in order, and not empty.
    pub(crate) fn new(
        table: Arc<SqlTable>,
        sources: Vec<Box<dyn Source>>,
        rows: Option<usize>,
    ) -> Self {
        Self {
            table,
            sources,
            current: 0,
            rows,
        }
    }
}

// The source reading `table`: `open` for its one partition, or for each of
// several under an AppendSource. `rows` is the expected row count over the
// whole table, for EXPLAIN.
pub(crate) fn over_partitions(
    table: &Arc<SqlTable>,
    rows: Option<usize>,
    mut open: impl FnMut(usize) -> Result<Box<dyn Source>, SchemaError>,
) -> Result<Box<dyn Source>, SchemaError> {
    if let [_] = table.partitions.as_slice() {
        return open(0);
    }
    let sources = (0..table.partitions.len())
        .map(&mut open)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Box::new(AppendSource::new(table.clone(), sources, rows)))
}

impl Source for AppendSource {
    fn plan(&self) -> PlanNode {
        PlanNode::new("Append")
            .detail(format!(
                "{} ({} partitions)",
                self.table.name,
                self.sources.len()
            ))
            .rows(self.rows)
            .children(self.sources.iter().map(|s| s.plan()).collect())
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

    // The row just yielded came from the source being read.
    fn last_id(&self) -> Option<store::tuple::DBIdType> {
        self.sources.get(self.current)?.last_id()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.reset()?;
        }
        self.current = 0;
        Ok(())
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.sources[0].table_stats()
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

    fn rows(values: &[i64]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["id"],
            values
                .iter()
                .map(|v| vec![ValueItem::Integer(*v)])
                .collect(),
        ))
    }

    fn table() -> Arc<SqlTable> {
        let mut tb = crate::table::TableBuilder::new();
        tb.with_name("t".into());
        tb.with_field(&crate::table::Field::from("id"));
        Arc::new(tb.build().unwrap())
    }

    #[test]
    fn test_append_reads_each_source_in_turn_and_again_after_a_reset() {
        let mut append =
            AppendSource::new(table(), vec![rows(&[1, 2]), rows(&[]), rows(&[3])], None);
        let ints =
            |rows: Vec<Vec<ValueItem>>| -> Vec<ValueItem> { rows.into_iter().flatten().collect() };
        let all = vec![
            ValueItem::Integer(1),
            ValueItem::Integer(2),
            ValueItem::Integer(3),
        ];
        assert_eq!(ints(drain(&mut append)), all);
        assert!(append.next().unwrap().is_none(), "stays exhausted");
        append.reset().unwrap();
        assert_eq!(ints(drain(&mut append)), all);
    }
}
