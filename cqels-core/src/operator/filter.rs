//! FILTER operator for predicate-based stream filtering.
//!
//! [`FilterOperator`] evaluates a boolean predicate on each incoming
//! [`BindingSet`], passing through only those that satisfy the condition.
//! Implements the SPARQL `FILTER(expr)` semantics.

use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

use cqels_model::BindingSet;

/// Filter operator that evaluates a predicate on each binding set.
///
/// Maps to Java's `FilterOperator`.
///
/// # Examples
///
/// ```
/// use cqels_core::operator::filter::FilterOperator;
/// use cqels_model::{BindingSet, Value};
///
/// let filter = FilterOperator::new(|bs: &BindingSet| {
///     bs.get("x").and_then(|v| v.as_integer()).map(|i| i > 10).unwrap_or(false)
/// });
///
/// let mut bs = BindingSet::new(0);
/// bs.insert("x", Value::Integer(15));
/// assert!(filter.evaluate(&bs));
///
/// let mut bs2 = BindingSet::new(0);
/// bs2.insert("x", Value::Integer(5));
/// assert!(!filter.evaluate(&bs2));
/// ```
pub struct FilterOperator<F: Fn(&BindingSet) -> bool + Send + Sync> {
    predicate: F,
}

impl<F: Fn(&BindingSet) -> bool + Send + Sync> FilterOperator<F> {
    /// Creates a new filter operator with the given predicate closure.
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }

    /// Applies the filter to a stream of binding sets.
    pub fn apply<'a>(
        &'a self,
        stream: Pin<Box<dyn Stream<Item = BindingSet> + Send + 'a>>,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send + 'a>> {
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

    #[test]
    fn test_filter_missing_variable() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            bs.get("missing")
                .and_then(|v| v.as_integer())
                .map(|i| i > 0)
                .unwrap_or(false)
        });
        let bs = BindingSet::new(0);
        assert!(!filter.evaluate(&bs));
    }

    #[test]
    fn test_filter_null_value() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            bs.get("x").map(|v| !v.is_null()).unwrap_or(false)
        });
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Null);
        assert!(!filter.evaluate(&bs));
    }

    #[test]
    fn test_filter_string_predicate() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            bs.get("name")
                .and_then(|v| v.as_string())
                .map(|s| s.starts_with("A"))
                .unwrap_or(false)
        });

        let mut bs1 = BindingSet::new(0);
        bs1.insert("name", Value::String("Alice".into()));
        assert!(filter.evaluate(&bs1));

        let mut bs2 = BindingSet::new(0);
        bs2.insert("name", Value::String("Bob".into()));
        assert!(!filter.evaluate(&bs2));
    }

    #[test]
    fn test_filter_compound_conditions() {
        let filter = FilterOperator::new(|bs: &BindingSet| {
            let age_ok = bs
                .get("age")
                .and_then(|v| v.as_integer())
                .map(|i| i >= 18)
                .unwrap_or(false);
            let active = bs
                .get("active")
                .and_then(|v| v.as_boolean())
                .unwrap_or(false);
            age_ok && active
        });

        let mut bs = BindingSet::new(0);
        bs.insert("age", Value::Integer(25));
        bs.insert("active", Value::Boolean(true));
        assert!(filter.evaluate(&bs));

        let mut bs2 = BindingSet::new(0);
        bs2.insert("age", Value::Integer(15));
        bs2.insert("active", Value::Boolean(true));
        assert!(!filter.evaluate(&bs2));
    }

    #[tokio::test]
    async fn test_filter_empty_stream() {
        let filter = FilterOperator::new(|_: &BindingSet| true);
        let stream = Box::pin(futures::stream::iter(Vec::<BindingSet>::new()));
        let results: Vec<_> = filter.apply(stream).collect().await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_all_pass() {
        let filter = FilterOperator::new(|_: &BindingSet| true);
        let bindings: Vec<BindingSet> = (0..5).map(BindingSet::new).collect();
        let stream = Box::pin(futures::stream::iter(bindings));
        let results: Vec<_> = filter.apply(stream).collect().await;
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_filter_none_pass() {
        let filter = FilterOperator::new(|_: &BindingSet| false);
        let bindings: Vec<BindingSet> = (0..5).map(BindingSet::new).collect();
        let stream = Box::pin(futures::stream::iter(bindings));
        let results: Vec<_> = filter.apply(stream).collect().await;
        assert_eq!(results.len(), 0);
    }
}
