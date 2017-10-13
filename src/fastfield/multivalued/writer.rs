use itertools::Itertools;
use fastfield::FastFieldSerializer;
use schema::Field;
use std::io;

pub struct MultiValueIntFastFieldWriter {
    field: Field,
    vals: Vec<u64>,
    doc_index: Vec<u64>,
}

impl MultiValueIntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field) -> Self {
        MultiValueIntFastFieldWriter {
            field: field,
            vals: Vec::new(),
            doc_index: Vec::new(),
        }
    }

    pub fn field(&self) -> Field {
        self.field
    }

    pub fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len() as u64);
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: u64) {
        self.vals.push(val);
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        {
            // writing the offset index
            let max = self.doc_index.iter().cloned().max().unwrap_or(0);
            let mut doc_index_serializer = serializer.new_u64_fast_field_with_idx(self.field, 0, max, 0)?;
            for &offset in &self.doc_index {
                doc_index_serializer.add_val(offset)?;
            }
            doc_index_serializer.add_val(self.vals.len() as u64)?;
            doc_index_serializer.close_field()?;
        }
        {
            // writing the values themselves.
            let (min, max) = self.vals.iter().cloned().minmax().into_option().unwrap_or((0, 0));
            let mut value_serializer = serializer.new_u64_fast_field_with_idx(self.field, min, max, 1)?;
            for &val in &self.vals {
                value_serializer.add_val(val)?;
            }
        }
        Ok(())

    }
}
