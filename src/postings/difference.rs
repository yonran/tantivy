use postings::DocSet;
use postings::SkipResult;
use DocId;

/// Creates a `DocSet` that iterates through the difference of two or more `DocSet`s.
pub struct DifferenceDocSet<TLeftDocSet: DocSet, TRightDocSet: DocSet> {
    left: TLeftDocSet,
    right: TRightDocSet,
    right_finished: bool,
}

impl<TLeftDocSet: DocSet, TRightDocSet: DocSet> DifferenceDocSet<TLeftDocSet, TRightDocSet> {
    /// Returns a `DifferenceDocSet` of two other `DocSet`s
    pub fn new(
        left: TLeftDocSet,
        mut right: TRightDocSet,
    ) -> DifferenceDocSet<TLeftDocSet, TRightDocSet> {
        let right_finished = !right.advance();

        DifferenceDocSet {
            left: left,
            right: right,
            right_finished: right_finished,
        }
    }

    /// Returns the left `DocSet`
    pub fn left(&self) -> &TLeftDocSet {
        &self.left
    }

    /// Returns the right `DocSet`
    pub fn right(&self) -> &TRightDocSet {
        &self.right
    }
}

impl<TLeftDocSet: DocSet, TRightDocSet: DocSet> DocSet
    for DifferenceDocSet<TLeftDocSet, TRightDocSet> {
    fn size_hint(&self) -> usize {
        self.left.size_hint()
    }

    #[allow(never_loop)]
    fn advance(&mut self) -> bool {
        loop {
            if !self.left.advance() {
                return false;
            }

            if self.right_finished || self.left.doc() < self.right.doc() {
                return true;
            }
            if self.left.doc() == self.right.doc() {
                continue;
            }
            match self.right.skip_next(self.left.doc()) {
                SkipResult::Reached => continue,
                SkipResult::OverStep => return true,
                SkipResult::End => {
                    self.right_finished = true;
                    return true;
                }
            }
        }
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let res = self.left.skip_next(target);
        match res {
            SkipResult::End => SkipResult::End,
            SkipResult::Reached | SkipResult::OverStep => {
                if self.right_finished || self.left.doc() < self.right.doc() {
                    return res;
                }
                if self.left.doc() == self.right.doc() {
                    return if self.left.advance() {
                        SkipResult::OverStep
                    } else {
                        SkipResult::End
                    };
                }
                match self.right.skip_next(self.left.doc()) {
                    SkipResult::Reached => {
                        if self.advance() {
                            SkipResult::OverStep
                        } else {
                            SkipResult::End
                        }
                    }
                    SkipResult::OverStep => res,
                    SkipResult::End => {
                        self.right_finished = true;
                        res
                    }
                }
            }
        }
    }

    fn doc(&self) -> DocId {
        self.left.doc()
    }
}


#[cfg(test)]
mod tests {

    use postings::{DocSet, SkipResult, VecPostings, DifferenceDocSet};

    #[test]
    fn test_difference() {
        let left = VecPostings::from(vec![1, 2, 3, 9, 14]);
        let right = VecPostings::from(vec![3, 4, 9, 12]);
        let mut difference = DifferenceDocSet::new(left, right);
        assert!(difference.advance());
        assert_eq!(difference.doc(), 1);
        assert!(difference.advance());
        assert_eq!(difference.doc(), 2);
        assert!(difference.advance());
        assert_eq!(difference.doc(), 14);
        assert!(!difference.advance());
    }

    #[test]
    fn test_difference_right_empty() {
        let left = VecPostings::from(vec![1, 2]);
        let right = VecPostings::from(vec![]);
        let mut difference = DifferenceDocSet::new(left, right);
        assert!(difference.advance());
        assert_eq!(difference.doc(), 1);
        assert!(difference.advance());
        assert_eq!(difference.doc(), 2);
        assert!(!difference.advance());
    }

    #[test]
    fn test_difference_left_empty() {
        let left = VecPostings::from(vec![]);
        let right = VecPostings::from(vec![1, 2, 3]);
        let mut difference = DifferenceDocSet::new(left, right);
        assert!(!difference.advance());
    }

    #[test]
    fn test_difference_empty() {
        let left = VecPostings::from(vec![]);
        let right = VecPostings::from(vec![]);
        let mut difference = DifferenceDocSet::new(left, right);
        assert!(!difference.advance());
    }

    #[test]
    fn test_difference_skip_next() {
        let left = VecPostings::from(vec![1, 3, 7, 8, 10, 13]);
        let right = VecPostings::from(vec![7, 8, 10, 12, 14, 15, 20]);
        let mut difference = DifferenceDocSet::new(left, right);
        assert_eq!(difference.skip_next(1), SkipResult::Reached);
        assert_eq!(difference.doc(), 1);
        assert_eq!(difference.skip_next(2), SkipResult::OverStep);
        assert_eq!(difference.doc(), 3);
        assert_eq!(difference.skip_next(8), SkipResult::OverStep);
        assert_eq!(difference.doc(), 13);
        assert!(!difference.advance());
        assert_eq!(difference.skip_next(22), SkipResult::End);
    }
}
