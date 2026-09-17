use std::{collections::HashMap, time::Instant};

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    source::{QueryStats, Source, merge_stats},
};

#[derive(Debug)]
pub(crate) struct AggregatingSource {
    source: Box<dyn Source>,
    next_emit: Option<IndexKey>,
    time_spent: u128,
}

impl AggregatingSource {
    pub(crate) fn new(source: Box<dyn Source>) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            next_emit: None,
            time_spent: 0,
        })
    }
}

impl Source for AggregatingSource {
    fn fields(&self) -> std::sync::Arc<[super::ProjectableField]> {
        self.source.fields()
    }
    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        let start = Instant::now();
        //if next emit is some, continue till next() is not = to next_emit

        let next_emit = if let Some(s) = self.next_emit.take() {
            Some(s)
        } else {
            self.source.next()?
        };
        if let Some(this) = next_emit {
            loop {
                if let Some(next) = self.source.next()? {
                    if this != next {
                        self.next_emit = Some(next);
                        self.time_spent += start.elapsed().as_nanos();
                        return Ok(Some(this));
                    }
                } else {
                    self.time_spent += start.elapsed().as_nanos();
                    return Ok(Some(this));
                }
            }
        }
        self.time_spent += start.elapsed().as_nanos();
        Ok(None)
    }
    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }

    fn stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        let time_spent = self.time_spent as f64;
        let this_query = QueryStats {
            stats: HashMap::from([("time_ns".into(), time_spent)]),
            level: 0,
        };
        let this_stats = vec![("AggegatingSource".to_string(), this_query)];
        Some(merge_stats(this_stats, self.source.stats()))
    }
}
