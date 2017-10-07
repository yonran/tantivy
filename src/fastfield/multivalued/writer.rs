use itertools::Itertools;
use fastfield::FastFieldSerializer;
use common;
use schema::{Field, Document, Value};
use DocId;
use std::io;

pub struct MultiValueIntFastFieldWriter {
    field: Field,
    vals: Vec<u64>,
    doc_index: Vec<usize>,
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

    /// Ensures all of the fast field writer have
    /// reached `doc`. (included)
    ///
    /// The missing values will be filled with 0.
    fn fill_val_up_to(&mut self, doc: DocId) {
        self.doc_index.resize(doc as usize, self.vals.len());
    }

    fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len());
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: u64) {
        self.vals.push(val);
    }


    /// Extract the fast field value from the document
    /// (or use the default value) and records it.
    pub fn add_document(&mut self, doc: &Document) {
        for val in doc.get_all(self.field) {
            match *val {
                Value::U64(ref val) => self.vals.push(*val),
                Value::I64(ref val) => self.vals.push(common::i64_to_u64(*val)),
                _ => {},
            }
        }
        self.next_doc();
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        let (min, max) = self.vals.iter().cloned().minmax().into_option().unwrap_or((0, 0));
        let mut single_field_serializer = serializer.new_u64_fast_field(self.field, min, max)?;
        for &val in &self.vals {
            single_field_serializer.add_val(val)?;
        }
        single_field_serializer.close_field()
    }
}
