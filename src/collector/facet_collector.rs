use collector::Collector;
use fastfield::FacetReader;
use schema::Field;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use schema::Facet;
use std::borrow::BorrowMut;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;

pub struct FacetCollector {
    // local_counters: HashMap::new(),
    field: Field,
    ff_reader: Option<UnsafeCell<FacetReader>>,
    local_counters: Vec<u64>,
    global_counters: HashMap<Facet, u64>,
}


impl FacetCollector {
    /// Creates a new facet collector for aggregating a given field.
    pub fn new(field: Field) -> FacetCollector {
        FacetCollector {
            field: field,
            ff_reader: None,
            local_counters: vec![],
            global_counters: HashMap::new(),
        }
    }

    fn translate_ordinals(&mut self) {
        for (term_ord, count) in self.local_counters.iter_mut().enumerate() {
            if *count > 0 {
                if let Some(ff_reader) = self.ff_reader.as_mut() {
                    let facet = unsafe { (*ff_reader.get()).facet_from_ord(term_ord).clone() };
                    *self.global_counters.entry(facet)
                        .or_insert(0) += *count;
                }
                *count = 0;
            }
        }
    }
}


impl Collector for FacetCollector
{


    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.translate_ordinals();
        self.local_counters.clear();
        let facet_reader = reader.facet_reader(self.field)?;
        self.local_counters.resize(facet_reader.num_terms(), 0);
        self.ff_reader = Some(UnsafeCell::new(facet_reader));
        // TODO use the number of terms to resize the local counters
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let mut facet_reader: &mut FacetReader =
            unsafe {
                &mut *self.ff_reader
                    .as_ref()
                    .expect("collect() was called before set_segment. \
                This should never happen.",
                    )
                    .get()
            };
        let facet_ords: &[u64] = facet_reader.term_ords(doc);
        for &facet_ord in facet_ords {
            self.local_counters[facet_ord as usize] += 1;
        }
    }
}


