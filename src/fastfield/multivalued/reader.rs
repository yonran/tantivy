use core::SegmentReader;
use error::Result;
use DocId;
use schema::Field;
use fastfield::FastFieldReader;

use fastfield::U64FastFieldReader;

pub struct MultiValueIntFastFieldReader {
    vals: Vec<u64>,
    idx_reader: U64FastFieldReader,
    vals_reader: U64FastFieldReader,
}

impl MultiValueIntFastFieldReader {

    pub fn open(segment_reader: &SegmentReader, field: Field) -> Result<MultiValueIntFastFieldReader> {
        let idx_reader = segment_reader.get_fast_field_reader(field)?;
        let vals_reader = segment_reader.get_fast_field_reader(field)?;
        Ok(MultiValueIntFastFieldReader {
            vals: vec!(),
            idx_reader: idx_reader,
            vals_reader: vals_reader,
        })
    }

    pub fn term_ords(&mut self, doc: DocId) -> &[u64] {
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
        
        assert_eq!(facet_reader.term_ords(0), &[0,1]);
        assert_eq!(facet_reader.term_ords(1), &[1]);
        assert_eq!(facet_reader.term_ords(2), &[2]);
    }
}