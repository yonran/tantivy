use collector::Collector;
use fastfield::FacetReader;
use schema::Field;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use schema::Facet;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;

/// Builder for the `FacetCollector`.
#[derive(Clone)]
pub struct FacetCollectorBuilder {
    field: Field,
    root_facet: Facet,
    depth: Option<usize>,
}

impl FacetCollectorBuilder {

    /// Crate a facet collector to collect the facets
    /// from a specific `Field`.
    pub fn for_field(field: Field) -> FacetCollectorBuilder {
        FacetCollectorBuilder {
            field: field,
            root_facet: Facet::root(),
            depth: None,
        }
    }

    /// Sets the root facet.
    ///
    /// Only the descendant of the root
    /// facet will be collected.
    ///
    /// Together with `set_depth(...)` this method
    /// makes it possible to implement a "drill down" user
    /// experience.
    pub fn set_root_facet(mut self, facet: Facet) -> FacetCollectorBuilder {
        self.root_facet = facet;
        self
    }

    /// Sets the depth that should be considered for
    /// collections.
    ///
    /// For instance, if depth is set to 1, only the
    /// counts of the direct child of the `root_facet`
    /// will be collected.
    pub fn set_depth(mut self, depth: usize) -> FacetCollectorBuilder {
        self.depth = Some(depth);
        self
    }

    /// Builds the facet collector.
    pub fn build(self) -> FacetCollector {
        FacetCollector {
            field: self.field,
            ff_reader: None,
            local_counters: vec![],
            global_counters: HashMap::new(),
        }
    }
}

pub struct FacetCollector {
    field: Field,
    ff_reader: Option<UnsafeCell<FacetReader>>,
    local_counters: Vec<u64>,
    global_counters: HashMap<Facet, u64>,
}

impl FacetCollector {
    fn translate_ordinals(&mut self) {
        for (term_ord, count) in self.local_counters.iter_mut().enumerate() {
            if *count > 0 {
                if let Some(ff_reader) = self.ff_reader.as_mut() {
                    let facet = unsafe { (*ff_reader.get()).facet_from_ord(term_ord as u64).clone() };
                    *self.global_counters.entry(facet)
                        .or_insert(0) += *count;
                }
                *count = 0;
            }
        }
    }


    /// Returns the results of the collection.
    ///
    /// This method does not just return the counters,
    /// it also translates the facet ordinals of the last segment.
    pub fn counts(mut self) -> HashMap<Facet, u64> {
        self.translate_ordinals();
        self.global_counters

    }
}


impl Collector for FacetCollector {
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.translate_ordinals();
        self.local_counters.clear();
        let facet_reader = reader.facet_reader(self.field)?;
        self.local_counters.resize(facet_reader.num_facets(), 0);
        self.ff_reader = Some(UnsafeCell::new(facet_reader));
        // TODO use the number of terms to resize the local counters
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let facet_reader: &mut FacetReader =
            unsafe {
                &mut *self.ff_reader
                    .as_ref()
                    .expect("collect() was called before set_segment. \
                This should never happen.",
                    )
                    .get()
            };
        let facet_ords: &[u64] = facet_reader.facet_ords(doc);
        for &facet_ord in facet_ords {
            self.local_counters[facet_ord as usize] += 1;
        }
    }
}



#[cfg(test)]
mod tests {

    use schema::SchemaBuilder;
    use core::Index;
    use schema::Document;
    use schema::Facet;
    use query::AllQuery;
    use super::FacetCollectorBuilder;

    #[test]
    fn test_facet_collector() {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facet");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);


        let mut index_writer = index.writer(3_000_000).unwrap();
        let num_facets: usize = 3 * 4 * 5;
        let facets: Vec<Facet> = (0..num_facets)
            .map(|mut n| {
                let top = n % 3;
                n /= 3;
                let mid = n % 4;
                n /= 4;
                let leaf = n % 5;
                Facet::from(&format!("/top{}/mid{}/leaf{}", top, mid, leaf))
            })
            .collect();
        for i in 0..num_facets * 10 {
            let mut doc = Document::new();
            doc.add_facet(facet_field, facets[i % num_facets].clone());
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();


        index.load_searchers().unwrap() ;
        let searcher = index.searcher();

        let mut facet_collector = FacetCollectorBuilder
            ::for_field(facet_field)
            .set_depth(1)
            .build();

        searcher.search(&AllQuery, &mut facet_collector).unwrap();
        let counts = facet_collector.counts();
        for facet in facets {
            assert_eq!(*counts.get(&facet).unwrap(), 10u64);
        }
    }
}

