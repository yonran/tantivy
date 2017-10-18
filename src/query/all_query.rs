use query::Query;
use query::Weight;
use query::Scorer;
use core::SegmentReader;
use Result;
use DocSet;
use Score;
use DocId;
use std::any::Any;
use core::Searcher;

#[derive(Debug)]
pub struct AllQuery;

impl Query for AllQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box AllWeight)
    }
}


pub struct AllWeight;

impl Weight for AllWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        Ok(box AllScorer {
            started: false,
            doc: 0u32,
            max_doc: reader.max_doc()
        })
    }
}

pub struct AllScorer {
    started: bool,
    doc: DocId,
    max_doc: DocId,
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> bool {
        if self.started {
            self.doc += 1u32;
        }
        else {
            self.started = true;
        }
        self.doc < self.max_doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> usize {
        self.max_doc as usize
    }
}

impl Scorer for AllScorer {
    fn score(&self) -> Score {
        1f32
    }
}