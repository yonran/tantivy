use Score;
use DocId;
use postings::{DocSet, DocSetGroup, SkipResult, UnionDocSet};
use query::Scorer;
use query::boolean_query::ScoreCombiner;

/// Represents a `Scorer` for a union of `Scorer`s
/// Skips the duplicate elements
pub struct UnionScorer<TScorer: Scorer> {
    inner: UnionDocSet<TScorer>,
    score_combiner: ScoreCombiner,
}

impl<TScorer: Scorer> From<Vec<TScorer>> for UnionScorer<TScorer> {
    fn from(scorers: Vec<TScorer>) -> Self {
        let num_scorers = scorers.len();
        UnionScorer {
            inner: UnionDocSet::from(scorers),
            score_combiner: ScoreCombiner::default_for_num_scorers(num_scorers),
        }
    }
}

impl<TScorer: Scorer> DocSet for UnionScorer<TScorer> {
    fn advance(&mut self) -> bool {
        if !self.inner.advance() {
            return false;
        }

        self.score_combiner.clear();
        for scorer in self.inner.docsets() {
            self.score_combiner.update(scorer.score());
        }

        true
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let res = self.inner.skip_next(target);
        if res == SkipResult::Reached {
            self.score_combiner.clear();
            for scorer in self.inner.docsets() {
                self.score_combiner.update(scorer.score());
            }
        }
        res
    }

    fn doc(&self) -> DocId {
        self.inner.doc()
    }

    fn size_hint(&self) -> usize {
        self.inner.size_hint()
    }
}

impl<TScorer: Scorer> Scorer for UnionScorer<TScorer> {
    fn score(&self) -> Score {
        self.score_combiner.score()
    }
}
