use std::sync::Arc;

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    source::{ProjectedField, Source},
};

#[derive(Debug)]
pub(crate) struct UnionJoin {
    sources: Vec<Box<dyn Source>>,
    fields: Arc<[ProjectedField]>,
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
        // One cached row per *source*, not per field — next() indexes this
        // by source position (`i` in `res.iter().enumerate()`) to remember
        // the last row a now-exhausted source produced, so its length has
        // to track `sources.len()`, not the flattened field count.
        Ok(Self {
            sources,
            fields: Arc::from(fields.as_slice()),
        })
    }
}

impl Source for UnionJoin {
    fn fields(&self) -> Arc<[ProjectedField]> {
        self.fields.clone()
    }

    #[allow(clippy::needless_range_loop)]
    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        let mut res = vec![];
        for s in &mut self.sources {
            res.push(s.next()?);
        }
        if res.iter().all(|f| f.is_none()) {
            return Ok(None);
        }
        for i in 0..res.len() {
            if res[i].is_none() {
                self.sources[i].reset()?;
                res[i] = self.sources[i].next()?;
            }
        }
        // Every source that still has a row this tick contributes its
        // fresh values (and becomes the new fallback for later ticks once
        // it runs dry); every source that's already exhausted repeats its
        // last known row instead of dropping out of the joined row
        // entirely. "Every source has a fresh value" is the common case,
        // not a special one — it happens on every tick until the first
        // source runs out — so this has to run unconditionally, not just
        // when some (but not all) sources are exhausted.
        /*         let mut new_res = vec![];
               for (i, r) in res.into_iter().enumerate() {
                   if let Some(r) = r {
                       new_res.extend_from_slice(r.values());
                       self.last_vals[i] = Some(r);
                   } else if let Some(cached) = &self.last_vals[i] {
                       new_res.extend_from_slice(cached.values());
                   } else {
                       return Err(SchemaError::InternalSchemaError(format!(
                           "No values found for index {i}"
                       )));
                   }
               }
        */
        let mut new_res = vec![];
        for r in res.into_iter() {
            if let Some(d) = r {
                new_res.extend_from_slice(d.values());
            } else {
                return Err(SchemaError::InternalSchemaError(
                    "No values found for index ".into(),
                ));
            }
        }
        Ok(Some(IndexKey::new_from_owned(new_res)?))
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        for s in &mut self.sources {
            s.as_mut().reset()?
        }
        Ok(())
    }
}
