use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

use cqels_model::BindingSet;

/// Filter operator that evaluates a predicate on each binding set.
///
/// Maps to Java's `FilterOperator`.
pub struct FilterOperator<F: Fn(&BindingSet) -> bool + Send + Sync> {
    predicate: F,
}

impl<F: Fn(&BindingSet) -> bool + Send + Sync> FilterOperator<F> {
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }

    /// Applies the filter to a stream of binding sets.
    pub fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = BindingSet> + Send + '_>>,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send + '_>> {
        let stream = stream.filter(|bs| {
            let result = (self.predicate)(bs);
            futures::future::ready(result)
        });
        Box::pin(stream)
    }

    /// Evaluates the predicate on a single binding set.
    pub fn evaluate(&self, binding: &BindingSet) -> bool {
        (self.predicate)(binding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::Value;

    #[test]
    fn test_filter_evaluate() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            bs.get("x")
                .and_then(|v| v.as_integer())
                .map(|i| i > 10)
                .unwrap_or(false)
        });

        let mut bs1 = BindingSet::new(0);
        bs1.insert("x", Value::Integer(15));
        assert!(filter.evaluate(&bs1));

        let mut bs2 = BindingSet::new(0);
        bs2.insert("x", Value::Integer(5));
        assert!(!filter.evaluate(&bs2));
    }

    #[tokio::test]
    async fn test_filter_stream() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            bs.get("val")
                .and_then(|v| v.as_integer())
                .map(|i| i % 2 == 0)
                .unwrap_or(false)
        });

        let bindings: Vec<BindingSet> = (0..10)
            .map(|i| {
                let mut bs = BindingSet::new(i);
                bs.insert("val", Value::Integer(i));
                bs
            })
            .collect();

        let stream = Box::pin(futures::stream::iter(bindings));
        let results: Vec<_> = filter.apply(stream).collect().await;
        assert_eq!(results.len(), 5); // 0, 2, 4, 6, 8
    }
}
