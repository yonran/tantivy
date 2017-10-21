use DocId;
use fastfield::FastFieldReader;

use fastfield::U64FastFieldReader;

/// Reader for a multivalued `u64` fast field.
///
/// The reader is implemented as two `u64` fast field.
///
/// The `vals_reader` will access the concatenated list of all
/// values for all reader.
/// The `idx_reader` associated, for each document, the index of its first value.
///
pub struct MultiValueIntFastFieldReader {
    vals: Vec<u64>,
    idx_reader: U64FastFieldReader,
    vals_reader: U64FastFieldReader,
}

impl MultiValueIntFastFieldReader {

    pub(crate) fn open(idx_reader: U64FastFieldReader, vals_reader: U64FastFieldReader) -> MultiValueIntFastFieldReader {
        MultiValueIntFastFieldReader {
            vals: vec!(),
            idx_reader: idx_reader,
            vals_reader: vals_reader,
        }
    }

    /// Returns the array of values associated to the given `doc`.
    pub fn get_vals(&mut self, doc: DocId) -> &[u64] {
        let start = self.idx_reader.get(doc) as u32;
        let stop = self.idx_reader.get(doc + 1) as u32;
        self.vals.clear();
        for val_id in start..stop {
            let val = self.vals_reader.get(val_id);
            self.vals.push(val);
        }
        &self.vals[..]
    }
}


#[cfg(test)]
mod tests {

    use core::Index;
    use schema::{Document, SchemaBuilder};

    #[test]
    fn test_multifastfield_reader() {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facets");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 30_000_000).expect("Failed to create index writer.");
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat2");
            doc.add_facet(facet_field, "/category/cat1");
            index_writer.add_document(doc);
        }
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat2");
            index_writer.add_document(doc);
        }
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat3");
            index_writer.add_document(doc);
        }
        index_writer.commit().expect("Commit failed");
        index.load_searchers().expect("Reloading searchers");
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut facet_reader = segment_reader
            .facet_reader(facet_field)
            .unwrap();

        for i in 0..4 {
            println!("facet_reader.facet_from_id(0).to_string() {}", facet_reader.facet_from_ord(i).to_string());
        }
        assert_eq!(facet_reader.facet_from_ord(0).to_string(), "/category");
        assert_eq!(facet_reader.facet_from_ord(1).to_string(), "/category/cat1");
        assert_eq!(facet_reader.facet_from_ord(2).to_string(), "/category/cat2");
        assert_eq!(facet_reader.facet_from_ord(3).to_string(), "/category/cat3");

        assert_eq!(facet_reader.facet_ords(0), &[2, 1]);
        assert_eq!(facet_reader.facet_ords(1), &[2]);
        assert_eq!(facet_reader.facet_ords(2), &[3]);


    }
}