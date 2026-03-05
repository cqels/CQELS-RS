use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Sort direction for ranking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// A sort key specification.
#[derive(Clone, Debug)]
pub struct SortKey {
    pub name: String,
    pub direction: SortDirection,
}

impl SortKey {
    pub fn asc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            direction: SortDirection::Ascending,
        }
    }

    pub fn desc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            direction: SortDirection::Descending,
        }
    }
}

/// A scored element wrapper for the heap.
struct ScoredElement<T> {
    element: T,
    score: f64,
    direction: SortDirection,
}

impl<T> PartialEq for ScoredElement<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl<T> Eq for ScoredElement<T> {}

impl<T> PartialOrd for ScoredElement<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ScoredElement<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self
            .score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal);
        match self.direction {
            // For descending top-K, the heap must pop the smallest score (min-heap),
            // so we reverse the natural ordering.
            SortDirection::Descending => cmp.reverse(),
            // For ascending top-K, the heap must pop the largest score (max-heap),
            // so we use natural ordering.
            SortDirection::Ascending => cmp,
        }
    }
}

/// An element with its rank.
#[derive(Clone, Debug)]
pub struct RankedElement<T> {
    pub element: T,
    pub rank: usize,
}

/// Top-K operator that maintains the top K elements by a scoring function.
///
/// Maps to Java's `TopKOperator`.
pub struct TopKOperator<T, F: Fn(&T) -> f64> {
    k: usize,
    scorer: F,
    direction: SortDirection,
    heap: BinaryHeap<ScoredElement<T>>,
}

impl<T, F: Fn(&T) -> f64> TopKOperator<T, F> {
    pub fn new(k: usize, scorer: F, direction: SortDirection) -> Self {
        Self {
            k,
            scorer,
            direction,
            heap: BinaryHeap::with_capacity(k + 1),
        }
    }

    /// Adds an element. If the heap exceeds k, removes the worst element.
    pub fn add(&mut self, element: T) {
        let score = (self.scorer)(&element);
        self.heap.push(ScoredElement {
            element,
            score,
            direction: self.direction,
        });
        if self.heap.len() > self.k {
            self.heap.pop();
        }
    }

    /// Returns the current top-K elements in ranked order.
    pub fn get_top_k(&self) -> Vec<&T>
    where
        T: Clone,
    {
        let mut elements: Vec<_> = self.heap.iter().collect();
        elements.sort_by(|a, b| {
            let cmp = a
                .score
                .partial_cmp(&b.score)
                .unwrap_or(Ordering::Equal);
            match self.direction {
                SortDirection::Descending => cmp.reverse(), // highest first
                SortDirection::Ascending => cmp,            // lowest first
            }
        });
        elements.iter().map(|e| &e.element).collect()
    }

    /// Returns the current top-K with rank numbers.
    pub fn get_top_k_ranked(&self) -> Vec<RankedElement<&T>>
    where
        T: Clone,
    {
        self.get_top_k()
            .into_iter()
            .enumerate()
            .map(|(i, elem)| RankedElement {
                element: elem,
                rank: i + 1,
            })
            .collect()
    }

    pub fn size(&self) -> usize {
        self.heap.len()
    }

    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.k
    }

    pub fn reset(&mut self) {
        self.heap.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_k_descending() {
        let mut topk = TopKOperator::new(3, |x: &i32| *x as f64, SortDirection::Descending);
        for i in 0..10 {
            topk.add(i);
        }
        let results = topk.get_top_k();
        assert_eq!(results.len(), 3);
        assert_eq!(*results[0], 9);
        assert_eq!(*results[1], 8);
        assert_eq!(*results[2], 7);
    }

    #[test]
    fn test_top_k_ascending() {
        let mut topk = TopKOperator::new(3, |x: &i32| *x as f64, SortDirection::Ascending);
        for i in 0..10 {
            topk.add(i);
        }
        let results = topk.get_top_k();
        assert_eq!(results.len(), 3);
        assert_eq!(*results[0], 0);
        assert_eq!(*results[1], 1);
        assert_eq!(*results[2], 2);
    }

    #[test]
    fn test_top_k_ranked() {
        let mut topk = TopKOperator::new(3, |x: &i32| *x as f64, SortDirection::Descending);
        topk.add(5);
        topk.add(10);
        topk.add(3);
        topk.add(8);

        let ranked = topk.get_top_k_ranked();
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].rank, 1);
        assert_eq!(*ranked[0].element, 10);
        assert_eq!(ranked[1].rank, 2);
        assert_eq!(*ranked[1].element, 8);
    }

    #[test]
    fn test_top_k_size_and_full() {
        let mut topk = TopKOperator::new(3, |x: &i32| *x as f64, SortDirection::Descending);
        assert_eq!(topk.size(), 0);
        assert!(!topk.is_full());

        topk.add(1);
        topk.add(2);
        topk.add(3);
        assert!(topk.is_full());

        topk.reset();
        assert_eq!(topk.size(), 0);
    }
}
