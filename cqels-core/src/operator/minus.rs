//! MINUS operator implementing SPARQL 1.1 set-difference (anti-join) semantics.
//!
//! [`MinusOperator`] removes solution mappings from the main stream that are
//! *compatible* with any solution mapping in a pre-collected exclusion set.
//!
//! Two solution mappings μ₁ and μ₂ are **compatible** under SPARQL 1.1 §8.3
//! when:
//! 1. `dom(μ₁) ∩ dom(μ₂) ≠ ∅` (at least one shared variable)
//! 2. `∀ v ∈ dom(μ₁) ∩ dom(μ₂): μ₁(v) = μ₂(v)`
//!
//! Disjoint domains are **not** compatible — the candidate is *not* excluded.
//!
//! Maps to Java's `org.cqels.operator.minus.MinusOperator`.
//!
//! # Examples
//!
//! ```
//! use cqels_core::operator::minus::MinusOperator;
//! use cqels_model::{BindingSet, Value};
//!
//! let mut excluded = BindingSet::new(0);
//! excluded.insert("person", Value::String("alice".into()));
//! let minus = MinusOperator::new(vec![excluded]);
//!
//! let mut alice = BindingSet::new(100);
//! alice.insert("person", Value::String("alice".into()));
//! let mut bob = BindingSet::new(200);
//! bob.insert("person", Value::String("bob".into()));
//!
//! assert!(minus.is_excluded(&alice));
//! assert!(!minus.is_excluded(&bob));
//! ```

use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

use cqels_model::BindingSet;

/// Reactive operator implementing SPARQL 1.1 MINUS semantics.
///
/// Holds a pre-collected list of exclusion bindings; pass them through a
/// stream to filter the main stream. For very large exclusion sets, consider
/// pre-filtering or a hash-based anti-join index.
#[derive(Debug, Clone)]
pub struct MinusOperator {
    exclusions: Vec<BindingSet>,
}

impl MinusOperator {
    /// Creates a MINUS operator from a pre-collected list of exclusion bindings.
    pub fn new(exclusions: Vec<BindingSet>) -> Self {
        Self { exclusions }
    }

    /// Convenience factory mirroring Java's `MinusOperator.excluding(...)`.
    pub fn excluding(exclusions: Vec<BindingSet>) -> Self {
        Self::new(exclusions)
    }

    /// Returns `true` if `candidate` is compatible with any exclusion.
    pub fn is_excluded(&self, candidate: &BindingSet) -> bool {
        self.exclusions.iter().any(|e| compatible(candidate, e))
    }

    /// Applies MINUS to a stream of binding sets.
    pub fn apply<'a>(
        &'a self,
        stream: Pin<Box<dyn Stream<Item = BindingSet> + Send + 'a>>,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send + 'a>> {
        let filtered = stream.filter(move |bs| {
            let keep = !self.is_excluded(bs);
            futures::future::ready(keep)
        });
        Box::pin(filtered)
    }
}

/// SPARQL 1.1 §8.3 compatibility check.
///
/// Returns `true` iff the two mappings share at least one variable AND
/// agree on the values of every shared variable.
pub fn compatible(mu1: &BindingSet, mu2: &BindingSet) -> bool {
    let mut shared = false;
    for (var, v1) in mu1.iter() {
        if let Some(v2) = mu2.get(var) {
            shared = true;
            if v1 != v2 {
                return false;
            }
        }
    }
    shared
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::Value;

    fn binding(ts: i64, var: &str, val: Value) -> BindingSet {
        let mut bs = BindingSet::new(ts);
        bs.insert(var, val);
        bs
    }

    #[test]
    fn simple_minus_removes_compatible_row() {
        let alice = Value::String("alice".into());
        let bob = Value::String("bob".into());

        let exclusion = binding(0, "person", alice.clone());
        let minus = MinusOperator::excluding(vec![exclusion]);

        let alice_row = binding(100, "person", alice);
        let bob_row = binding(200, "person", bob);

        assert!(minus.is_excluded(&alice_row));
        assert!(!minus.is_excluded(&bob_row));
    }

    #[test]
    fn empty_exclusion_passes_all_rows() {
        let minus = MinusOperator::excluding(vec![]);
        let bs = binding(1, "x", Value::Integer(1));
        assert!(!minus.is_excluded(&bs));
    }

    #[test]
    fn disjoint_variables_row_not_excluded() {
        // SPARQL 1.1 §8.3: disjoint domains → NOT compatible → NOT excluded
        let main = binding(100, "person", Value::String("alice".into()));
        let exclusion = binding(0, "city", Value::String("Berlin".into()));
        let minus = MinusOperator::excluding(vec![exclusion]);
        assert!(!minus.is_excluded(&main));
    }

    #[test]
    fn partial_overlap_value_mismatch_not_excluded() {
        // shared "person", different values → not compatible → not excluded
        let mut main = BindingSet::new(100);
        main.insert("person", Value::String("alice".into()));
        main.insert("age", Value::Integer(30));

        let exclusion = binding(0, "person", Value::String("bob".into()));
        let minus = MinusOperator::excluding(vec![exclusion]);
        assert!(!minus.is_excluded(&main));
    }

    #[test]
    fn partial_overlap_value_match_excluded() {
        let mut main = BindingSet::new(100);
        main.insert("person", Value::String("alice".into()));
        main.insert("age", Value::Integer(30));

        let exclusion = binding(0, "person", Value::String("alice".into()));
        let minus = MinusOperator::excluding(vec![exclusion]);
        assert!(minus.is_excluded(&main));
    }

    #[test]
    fn multiple_exclusions_excludes_each_match() {
        let alice = Value::String("alice".into());
        let bob = Value::String("bob".into());
        let carol = Value::String("carol".into());

        let exclusions = vec![
            binding(0, "person", alice.clone()),
            binding(0, "person", bob.clone()),
        ];
        let minus = MinusOperator::excluding(exclusions);

        assert!(minus.is_excluded(&binding(1, "person", alice)));
        assert!(minus.is_excluded(&binding(2, "person", bob)));
        assert!(!minus.is_excluded(&binding(3, "person", carol)));
    }

    #[test]
    fn compatible_shared_var_equal_value() {
        let v = Value::String("same".into());
        assert!(compatible(&binding(0, "x", v.clone()), &binding(0, "x", v),));
    }

    #[test]
    fn compatible_shared_var_different_value() {
        assert!(!compatible(
            &binding(0, "x", Value::String("a".into())),
            &binding(0, "x", Value::String("b".into())),
        ));
    }

    #[test]
    fn compatible_disjoint_domains_returns_false() {
        assert!(!compatible(
            &binding(0, "x", Value::Integer(1)),
            &binding(0, "y", Value::Integer(1)),
        ));
    }

    #[test]
    fn compatible_multiple_shared_all_match() {
        let mut r1 = BindingSet::new(0);
        r1.insert("a", Value::Integer(1));
        r1.insert("b", Value::Integer(2));
        let mut r2 = BindingSet::new(0);
        r2.insert("a", Value::Integer(1));
        r2.insert("b", Value::Integer(2));
        r2.insert("c", Value::Integer(3));
        assert!(compatible(&r1, &r2));
    }

    #[test]
    fn compatible_multiple_shared_one_mismatch() {
        let mut r1 = BindingSet::new(0);
        r1.insert("a", Value::Integer(1));
        r1.insert("b", Value::Integer(99));
        let mut r2 = BindingSet::new(0);
        r2.insert("a", Value::Integer(1));
        r2.insert("b", Value::Integer(2));
        assert!(!compatible(&r1, &r2));
    }

    #[tokio::test]
    async fn apply_filters_stream() {
        let alice = Value::String("alice".into());
        let bob = Value::String("bob".into());

        let exclusions = vec![binding(0, "person", alice.clone())];
        let minus = MinusOperator::excluding(exclusions);

        let bindings = vec![
            binding(100, "person", alice),
            binding(200, "person", bob.clone()),
        ];
        let stream = Box::pin(futures::stream::iter(bindings));
        let results: Vec<_> = minus.apply(stream).collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("person"), Some(&bob));
    }

    #[tokio::test]
    async fn apply_empty_main_yields_empty() {
        let exclusions = vec![binding(0, "x", Value::Integer(1))];
        let minus = MinusOperator::excluding(exclusions);
        let stream = Box::pin(futures::stream::iter(Vec::<BindingSet>::new()));
        let results: Vec<_> = minus.apply(stream).collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn apply_preserves_timestamp_and_extra_bindings() {
        let mut main = BindingSet::new(42);
        main.insert("person", Value::String("bob".into()));
        main.insert("age", Value::Integer(25));
        main.insert("city", Value::String("Munich".into()));

        let exclusion = binding(0, "person", Value::String("alice".into()));
        let minus = MinusOperator::excluding(vec![exclusion]);

        let stream = Box::pin(futures::stream::iter(vec![main]));
        let results: Vec<_> = minus.apply(stream).collect().await;

        assert_eq!(results.len(), 1);
        let out = &results[0];
        assert_eq!(out.timestamp(), 42);
        assert_eq!(out.get("age"), Some(&Value::Integer(25)));
        assert_eq!(out.get("city"), Some(&Value::String("Munich".into())));
    }
}
