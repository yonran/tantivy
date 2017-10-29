use std::mem;
use collector::Collector;
use fastfield::FacetReader;
use schema::Field;
use std::cell::UnsafeCell;
use schema::Facet;
use termdict::TermDictionary;
use termdict::TermStreamer;
use termdict::TermStreamerBuilder;
use termdict::TermMerger;
use std::sync::Arc;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;


struct SegmentFacetCounter {
    pub facet_reader: FacetReader,
    pub facet_counts: Vec<u64>,
}

fn facet_depth(facet_bytes: &[u8]) -> usize {
    if facet_bytes.is_empty() {
        0
    } else {
        facet_bytes
            .iter()
            .cloned()
            .filter(|b| *b == 0u8)
            .count() + 1
    }
}



/// Collector for faceting
///
/// The collector collects all facets. Once collection is
/// finished, you can harvest its results in the form
/// of a `FacetCounts` object, and extract different
/// facetting information from it.
///
/// This approach assumes you are working with a number
/// of facets that is much lower than your number of documents.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{Result, Schema, TEXT};
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
///
///     // facet have their own specific type.
///     let facet = schema_builder.add_facet_field("facet");
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///
///     let mut index_writer = index.writer(3_000_000)?;
///     // a document can be associated to any number of facets
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///         facet => "/lang/en",
///         facet => "/category/fiction/fantasy",
///     ));
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///         facet => "/lang/en",
///         facet => "/category/fiction/fantasy",
///     );
///     index_writer.commit().unwrap();
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     let mut facet_collector = FacetCollector::for_field(facet_field);
///     searcher.search(&AllQuery, &mut facet_collector).unwrap();
///
///     // this object contains count aggregate for all of the facets.
///     let counts: FacetCounts = facet_collector.harvest();
///
///     let facets: Vec<(Facet, u64)> = counts
///         .iter()
///         .map(|(facet, count)| (facet.to_string(), count))
///         .collect();
///     assert!(facets, vec!(""));
/// }
/// ```
pub struct FacetCollector {
    facet_ords: Vec<u64>,
    field: Field,
    ff_reader: Option<UnsafeCell<FacetReader>>,
    segment_counters: Vec<SegmentFacetCounter>,
    current_segment_counts: Vec<u64>,
}


impl FacetCollector {

    /// Create a facet collector to collect the facets
    /// from a specific facet `Field`.
    ///
    /// This function does not check whether the field
    /// is of the proper type.
    pub fn for_field(field: Field) -> FacetCollector {
        FacetCollector {
            facet_ords: Vec::with_capacity(255),
            field: field,
            ff_reader: None,
            segment_counters: Vec::new(),
            current_segment_counts: Vec::new(),
        }
    }

    fn finalize_segment(&mut self) {
        if self.ff_reader.is_some() {
            self.segment_counters.push(
                SegmentFacetCounter {
                    facet_reader: unsafe { self.ff_reader.take().unwrap().into_inner() },
                    facet_counts: mem::replace(&mut self.current_segment_counts, Vec::new()),
                }
            );
            self.current_segment_counts.clear();
        }
    }

    /// Returns the results of the collection.
    ///
    /// This method does not just return the counters,
    /// it also translates the facet ordinals of the last segment.
    pub fn harvest(mut self) -> FacetCounts {
        self.finalize_segment();
        FacetCounts {
            segments_counts: Arc::new(self.segment_counters),
            root: Facet::root(),
        }
    }
}


impl Collector for FacetCollector {
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.finalize_segment();
        let facet_reader = reader.facet_reader(self.field)?;
        self.current_segment_counts.resize(facet_reader.num_facets(), 0);
        self.ff_reader = Some(UnsafeCell::new(facet_reader));
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
        facet_reader.facet_ords(doc, &mut self.facet_ords);
        for &facet_ord in &self.facet_ords {
            self.current_segment_counts[facet_ord as usize] += 1;
        }
    }
}


pub struct FacetIteratorWithDepth<'a> {
    facet_stream: TermMerger<'a>,
    vals: Vec<&'a [u64]>,
    depth: usize,
    current_facet: Option<Facet>,
    current_count: u64,
}

impl<'a> Iterator for FacetIteratorWithDepth<'a> {

    type Item = (Facet, u64);

    fn next(&mut self) -> Option<(Facet, u64)> {
        {
            while self.facet_stream.advance() {

                let bytes = self.facet_stream.key();
                let facet_count: u64 = self.facet_stream
                    .current_kvs()
                    .iter()
                    .map(|it| {
                        let term_ord = it.streamer.term_ord();
                        let seg_ord = it.segment_ord;
                        self.vals[seg_ord][term_ord as usize]
                    })
                    .sum();

                if facet_depth(bytes) == self.depth {
                    let facet = self.current_facet.take();
                    let emitting_count = mem::replace(&mut self.current_count, facet_count);
                    self.current_facet = Some(Facet::from_encoded(bytes.to_owned()));
                    if let Some(facet) = facet {
                        if emitting_count != 0u64 {
                            return Some((facet, emitting_count));
                        }
                    }
                }

                self.current_count += facet_count;
            }
        }
        self.current_facet
            .take()
            .map(|facet| (facet, self.current_count))
    }
}


pub struct FacetIterator<'a> {
    facet_stream: TermMerger<'a>,
    vals: Vec<&'a [u64]>,
}

impl<'a> Iterator for FacetIterator<'a> {

    type Item = (Facet, u64);

    fn next(&mut self) -> Option<(Facet, u64)> {
        while self.facet_stream.advance() {
            let count = self.facet_stream
                .current_kvs()
                .iter()
                .map(|it| {
                    let term_ord = it.streamer.term_ord();
                    let seg_ord = it.segment_ord;
                    self.vals[seg_ord][term_ord as usize]
                })
                .sum();
            if count > 0u64 {
                let bytes = self.facet_stream.key().to_owned();
                return Some((Facet::from_encoded(bytes), count))
            }
        }
        None
    }
}


/// Intermediary result of the `FacetCollector` that stores
/// the facet counts for all the segments.
///
/// Check
pub struct FacetCounts {
    segments_counts: Arc<Vec<SegmentFacetCounter>>,
    root: Facet,
}

impl FacetCounts {

    /// View of the facet count restricted to facets that
    /// children of the given root.
    pub fn root(&self, root: Facet) -> FacetCounts {
        FacetCounts {
            segments_counts: self.segments_counts.clone(),
            root: root
        }
    }

    /// Iterates the facets at a given depth below the root.
    ///
    /// Deeper facets will all be accumulated in their parents count.
    ///
    ///
    pub fn with_depth<'a>(&'a self, depth: usize) -> impl 'a + Iterator<Item=(Facet, u64)> {
        let depth: usize = facet_depth(self.root.encoded_bytes()) + depth;
        FacetIteratorWithDepth {
            facet_stream: self.facets(),
            vals: self.vals(),
            depth: depth,
            current_facet: None,
            current_count: 0
        }
    }

    fn vals(&self) -> Vec<&[u64]> {
        self.segments_counts
            .iter()
            .map(Vec::as_slice)
            .collect()
    }

    fn facets(&self) -> TermMerger {
        let facet_streams: Vec<_>;
        if !self.root.is_root() {
            let mut facet_bytes_after = Vec::from(self.root.encoded_bytes());
            facet_bytes_after.push(1u8);
            facet_streams = self.segments_counts
                .iter()
                .map(|seg_counts| {
                    seg_counts
                        .facet_reader
                        .facet_dict()
                        .range()
                        .ge(&self.root.encoded_bytes())
                        .lt(&facet_bytes_after)
                        .into_stream()
                })
                .collect();
        } else {
            facet_streams = self.segments_counts
                .iter()
                .map(|seg_counts| {
                    seg_counts
                        .facet_reader
                        .facet_dict()
                        .range()
                        .into_stream()
                })
                .collect();
        }
        TermMerger::new(facet_streams)
    }

    /// Returns an iterator over all of the
    /// facets with a *non-zero* count that are
    /// descendants of the root facet. (by default all facets).
    ///
    /// Facets are sorted in the pre-order DFS ordering,
    /// with a node, facets are sorted lexicographically.
    ///
    /// e.g:
    ///
    /// - /a/
    /// - /a/a/
    /// - /a/a/a
    /// - /a/a/b
    /// - /a/b
    ///
    /// In this iteration, a document associated to the facet
    /// `/location/europe/france` only contributes to the count of this facet,
    /// and does not contribute to the facet of its ancestors.
    /// `/location/europe` and `/location`.
    ///
    /// To get aggregate at a given level, check out [`.with_depth(usize)`](#method.with_depth).
    pub fn iter<'a>(&'a self) -> impl 'a + Iterator<Item=(Facet, u64)> {
        FacetIterator {
            facet_stream: self.facets(),
            vals: self.vals(),
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
    use super::{FacetCollector, FacetCounts};

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


        index.load_searchers().unwrap();
        let searcher = index.searcher();

        let mut facet_collector = FacetCollector::for_field(facet_field);
        searcher.search(&AllQuery, &mut facet_collector).unwrap();

        let counts: FacetCounts = facet_collector.harvest();
        {
            let facets: Vec<(String, u64)> = counts
                .iter()
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(facets, [
                ("/top0/mid0/leaf0", 10),
                ("/top0/mid0/leaf1", 10),
                ("/top0/mid0/leaf2", 10),
                ("/top0/mid0/leaf3", 10),
                ("/top0/mid0/leaf4", 10),
                ("/top0/mid1/leaf0", 10),
                ("/top0/mid1/leaf1", 10),
                ("/top0/mid1/leaf2", 10),
                ("/top0/mid1/leaf3", 10),
                ("/top0/mid1/leaf4", 10),
                ("/top0/mid2/leaf0", 10),
                ("/top0/mid2/leaf1", 10),
                ("/top0/mid2/leaf2", 10),
                ("/top0/mid2/leaf3", 10),
                ("/top0/mid2/leaf4", 10),
                ("/top0/mid3/leaf0", 10),
                ("/top0/mid3/leaf1", 10),
                ("/top0/mid3/leaf2", 10),
                ("/top0/mid3/leaf3", 10),
                ("/top0/mid3/leaf4", 10),
                ("/top1/mid0/leaf0", 10),
                ("/top1/mid0/leaf1", 10),
                ("/top1/mid0/leaf2", 10),
                ("/top1/mid0/leaf3", 10),
                ("/top1/mid0/leaf4", 10),
                ("/top1/mid1/leaf0", 10),
                ("/top1/mid1/leaf1", 10),
                ("/top1/mid1/leaf2", 10),
                ("/top1/mid1/leaf3", 10),
                ("/top1/mid1/leaf4", 10),
                ("/top1/mid2/leaf0", 10),
                ("/top1/mid2/leaf1", 10),
                ("/top1/mid2/leaf2", 10),
                ("/top1/mid2/leaf3", 10),
                ("/top1/mid2/leaf4", 10),
                ("/top1/mid3/leaf0", 10),
                ("/top1/mid3/leaf1", 10),
                ("/top1/mid3/leaf2", 10),
                ("/top1/mid3/leaf3", 10),
                ("/top1/mid3/leaf4", 10),
                ("/top2/mid0/leaf0", 10),
                ("/top2/mid0/leaf1", 10),
                ("/top2/mid0/leaf2", 10),
                ("/top2/mid0/leaf3", 10),
                ("/top2/mid0/leaf4", 10),
                ("/top2/mid1/leaf0", 10),
                ("/top2/mid1/leaf1", 10),
                ("/top2/mid1/leaf2", 10),
                ("/top2/mid1/leaf3", 10),
                ("/top2/mid1/leaf4", 10),
                ("/top2/mid2/leaf0", 10),
                ("/top2/mid2/leaf1", 10),
                ("/top2/mid2/leaf2", 10),
                ("/top2/mid2/leaf3", 10),
                ("/top2/mid2/leaf4", 10),
                ("/top2/mid3/leaf0", 10),
                ("/top2/mid3/leaf1", 10),
                ("/top2/mid3/leaf2", 10),
                ("/top2/mid3/leaf3", 10),
                ("/top2/mid3/leaf4", 10)
            ].iter()
                .map(|&(facet_str, count)| {
                    (String::from(facet_str), count)
                })
                .collect::<Vec<_>>());
        }
        {
            let facets: Vec<(String, u64)> = counts
                .root(Facet::from("/top1"))
                .iter()
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(facets, [
                ("/top1/mid0/leaf0", 10),
                ("/top1/mid0/leaf1", 10),
                ("/top1/mid0/leaf2", 10),
                ("/top1/mid0/leaf3", 10),
                ("/top1/mid0/leaf4", 10),
                ("/top1/mid1/leaf0", 10),
                ("/top1/mid1/leaf1", 10),
                ("/top1/mid1/leaf2", 10),
                ("/top1/mid1/leaf3", 10),
                ("/top1/mid1/leaf4", 10),
                ("/top1/mid2/leaf0", 10),
                ("/top1/mid2/leaf1", 10),
                ("/top1/mid2/leaf2", 10),
                ("/top1/mid2/leaf3", 10),
                ("/top1/mid2/leaf4", 10),
                ("/top1/mid3/leaf0", 10),
                ("/top1/mid3/leaf1", 10),
                ("/top1/mid3/leaf2", 10),
                ("/top1/mid3/leaf3", 10),
                ("/top1/mid3/leaf4", 10),
            ].iter()
                .map(|&(facet_str, count)| {
                    (String::from(facet_str), count)
                })
                .collect::<Vec<_>>());
        }



        {
            let facets: Vec<(String, u64)> = counts
                .with_depth(1)
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(facets, [
                ("/top0", 200), ("/top1", 200), ("/top2", 200),
            ].iter()
                .map(|&(facet_str, count)| {
                    (String::from(facet_str), count)
                })
                .collect::<Vec<_>>());
        }
    }

}

