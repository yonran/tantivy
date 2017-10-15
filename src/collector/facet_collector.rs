use collector::Collector;
use fastfield::FacetReader;
use fastfield::FastFieldReader;
use schema::Field;
use std::cell::RefCell;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;

pub struct FacetCollector {
    // local_counters: HashMap::new(),
    field: Field,
    ff_reader: Option<RefCell<FacetReader>>,
    local_counters: Vec<u64>
}


impl FacetCollector {
    /// Creates a new facet collector for aggregating a given field.
    pub fn new(field: Field) -> FacetCollector {
        FacetCollector {
            field: field,
            ff_reader: None,
            local_counters: vec![],
        }
    }
}


impl Collector for FacetCollector
{
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.local_counters.clear();
        let facet_reader = reader.facet_reader(self.field)?;
        self.local_counters.resize(facet_reader.num_terms(), 0);
        self.ff_reader = Some(RefCell::new(facet_reader));
        // TODO use the number of terms to resize the local counters
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let mut facet_reader = self.ff_reader
            .as_ref()
            .expect(
                "collect() was called before set_segment. \
                This should never happen.",
            )
            .borrow_mut();
        let facet_ords: &[u64] = facet_reader.term_ords(doc);
        for &facet_ord in facet_ords {
            self.local_counters[facet_ord as usize] += 1;
        }
    }
}


