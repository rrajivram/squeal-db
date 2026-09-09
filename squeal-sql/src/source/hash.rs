use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use sql_parser::keyword::Index;
use store::{
    db::{DBFile, Db},
    error::StoreError,
    run::Run,
    valueitem::IndexKey,
};

use crate::{error::SchemaError, source::Source};

pub(crate) struct HashedSource<F: DBFile + 'static> {
    source: Box<dyn Source>,
    capacity: usize,
    db: Arc<Db<F>>,
    run: Run<F>,
    record_size: usize,
    fields: Vec<usize>,
    count: usize,
    bitmask: Vec<u8>,
}

impl<F: DBFile + 'static> HashedSource<F> {
    pub(crate) fn new(
        db: Arc<Db<F>>,
        source: Box<dyn Source>,
        fields: &[usize],
    ) -> Result<Self, SchemaError> {
        let record_size = source
            .fields()
            .iter()
            .map(|f| f.field.datatype.size())
            .sum();
        let mut run = db.create_run()?;
        let capacity = run.data_size() as usize / (record_size + size_of::<Option<u64>>());
        let row = vec![0u8; record_size];
        let data = vec![(Option::<u64>::None, Option::<IndexKey>::None); capacity];
        run.set_content(&to_allocvec(&data)?)?;
        Ok(Self {
            source,
            capacity,
            run,
            record_size,
            db,
            fields: fields.to_vec(),
            count: 0,
            bitmask: vec![0u8; capacity / 8 + 1],
        })
    }

    fn slot_available(&self, index: usize) -> Result<bool, SchemaError> {
        if index >= self.capacity {
            return Err(SchemaError::UnknownError(format!(
                "Index {index} out of range ({}) ",
                self.capacity
            )));
        }
        let v_index = index / 8;
        let bit = 1 << (index % 8);
        Ok(self.bitmask[v_index] & bit != 0)
    }

    fn find_next_slot(&self, index: usize) -> Result<usize, SchemaError> {
        let mut index = index + 1;
        for _ in 0..self.capacity {
            if index >= self.capacity {
                index = 0;
            }
            if self.slot_available(index)? {
                return Ok(index);
            }
        }
        Err(SchemaError::UnknownError("Unable to find free slot".into()))
    }

    fn claim_slot(&mut self, index: usize) -> Result<(), SchemaError> {
        if !self.slot_available(index)? {
            return Err(SchemaError::UnknownError(format!(
                "Index {index} already occupied"
            )));
        }
        let v_index = index / 8;
        let bit = 1 << (index % 8) as u8;

        self.bitmask[v_index] |= bit;
        assert!(!self.slot_available(index)?);
        Ok(())
    }

    fn insert(&mut self, item: IndexKey) -> Result<(), SchemaError> {
        if self.count == self.capacity {
            self.rehash(self.capacity * 2)?;
        }

        let hash = IndexKey::hash_fields(self.fields.iter().map(|f| &item.values()[*f]));
        self.insert_with_hash(item, hash)?;

        Ok(())
    }

    fn insert_with_hash(&mut self, item: IndexKey, hash: u64) -> Result<(), SchemaError> {
        let mut index = (hash % self.capacity as u64) as usize;
        if !self.slot_available(index)? {
            index = self.find_next_slot(index)?
        }
        self.claim_slot(index)?;
        let page_index = index / self.run.page_count();
        assert!(page_index < self.run.page_count());
        let data = self.run.get_content_at(page_index)?.unwrap();
        let mut data = from_bytes::<Vec<(Option<u64>, Option<IndexKey>)>>(&data)?; // Unwrapping is safe because anytime new page is added, content is set to empty
        let row_index = index % self.run.page_count();
        assert!(data[row_index].0.is_none());
        let row = &mut data[row_index];
        row.0 = Some(hash);
        row.1 = Some(item);
        self.run.set_content_at(page_index, &to_allocvec(&data)?)?;
        Ok(())
    }

    fn get(&self, item: &IndexKey) -> Result<&Option<IndexKey>, SchemaError> {
        let hash = IndexKey::hash_fields(self.fields.iter().map(|f| &item.values()[*f]));
        todo!()
    }

    fn rehash(&mut self, new_capacity: usize) -> Result<(), StoreError> {
        todo!()
    }
}

// Temporary stub — Source requires Debug, but Run<F>/Db<F> don't
// implement it (and deriving would also force F: Debug on every
// HashedSource<F> regardless). Real impl can replace this once the
// fields settle; matches the same finish_non_exhaustive() pattern
// RunSource/TableRef already use for the same reason.
impl<F: DBFile + 'static> std::fmt::Debug for HashedSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashedSource").finish_non_exhaustive()
    }
}

impl<F: DBFile + 'static> Source for HashedSource<F> {
    fn fields(&self) -> Arc<[super::ProjectableField]> {
        self.source.fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        todo!()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.run = self.db.create_run()?;
        Ok(())
    }
}
