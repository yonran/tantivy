use Score;
use DocId;
use postings::{DocSet, SkipResult, DifferenceDocSet};
use query::Scorer;

/// Represents a `Scorer` for the difference of `Scorer`s
pub struct DifferenceScorer<TLeftScorer: Scorer, TRightScorer: Scorer> {
    inner: DifferenceDocSet<TLeftScorer, TRightScorer>,
}

impl<TLeftScorer: Scorer, TRightScorer: Scorer> DifferenceScorer<TLeftScorer, TRightScorer> {
    /// Creates a `DifferenceScorer` of two other `Scorer`s. It will return documents on the
    /// `left` side that are not present on the `right`.
    pub fn new(left: TLeftScorer, right: TRightScorer) -> Self {
        DifferenceScorer {
            inner: DifferenceDocSet::new(left, right),
        }
    }
}

impl<TLeftScorer: Scorer, TRightScorer: Scorer> DocSet
    for DifferenceScorer<TLeftScorer, TRightScorer> {
    fn advance(&mut self) -> bool {
        self.inner.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.inner.skip_next(target)
    }

    fn doc(&self) -> DocId {
        self.inner.doc()
    }

    fn size_hint(&self) -> usize {
        self.inner.size_hint()
    }
}

impl<TLeftScorer: Scorer, TRightScorer: Scorer> Scorer
    for DifferenceScorer<TLeftScorer, TRightScorer> {
    fn score(&self) -> Score {
        self.inner.left().score()
    }
}
