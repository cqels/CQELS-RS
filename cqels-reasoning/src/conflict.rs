//! Conflict resolution strategies for the RETE reasoning engine.
//!
//! When multiple rules match simultaneously, the [`ConflictResolver`]
//! selects which [`Activation`]s to fire based on the configured
//! [`ConflictResolution`] strategy (priority-based or fire-all).

use std::collections::HashSet;
use std::fmt;

use cqels_model::Statement;

use crate::production::Activation;
use crate::rule::RuleSet;

/// Strategy for resolving conflicts when multiple rules fire simultaneously.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::ConflictResolution;
///
/// let strategy = ConflictResolution::Priority;
/// assert_eq!(strategy, ConflictResolution::Priority);
/// assert_ne!(strategy, ConflictResolution::All);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConflictResolution {
    /// Higher priority rules fire first; lower priority rules are suppressed
    /// if they produce the same inferred triple.
    Priority,
    /// All applicable rules fire (no suppression).
    All,
}

/// Resolves conflicts between activations based on the configured strategy.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::{ConflictResolution, ConflictResolver};
///
/// let resolver = ConflictResolver::new(ConflictResolution::All);
/// assert_eq!(resolver.strategy(), ConflictResolution::All);
/// ```
pub struct ConflictResolver {
    strategy: ConflictResolution,
}

impl ConflictResolver {
    pub fn new(strategy: ConflictResolution) -> Self {
        Self { strategy }
    }

    pub fn strategy(&self) -> ConflictResolution {
        self.strategy
    }

    /// Resolves activations according to the strategy.
    ///
    /// Returns the filtered/ordered list of activations that should fire.
    pub fn resolve(&self, mut activations: Vec<Activation>, rule_set: &RuleSet) -> Vec<Activation> {
        match self.strategy {
            ConflictResolution::All => activations,
            ConflictResolution::Priority => {
                // Sort by priority (highest first)
                activations.sort_by(|a, b| {
                    let a_priority = rule_set.rules()[a.rule_index()].priority();
                    let b_priority = rule_set.rules()[b.rule_index()].priority();
                    b_priority.cmp(&a_priority)
                });

                // Deduplicate: only keep activations that produce new statements
                let mut seen_statements: HashSet<Statement> = HashSet::new();
                let mut result = Vec::new();

                for activation in activations {
                    let rule = &rule_set.rules()[activation.rule_index()];
                    let inferred = activation.fire(rule);
                    let has_new = inferred.iter().any(|s| !seen_statements.contains(s));
                    if has_new {
                        for s in &inferred {
                            seen_statements.insert(s.clone());
                        }
                        result.push(activation);
                    }
                }

                result
            }
        }
    }
}

impl fmt::Debug for ConflictResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConflictResolver")
            .field("strategy", &self.strategy)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{PatternTerm, TripleTemplate};
    use crate::rule::Rule;
    use cqels_model::term::IriTerm;
    use cqels_model::Term;
    use std::collections::HashMap;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    #[test]
    fn test_conflict_all_fires_everything() {
        let resolver = ConflictResolver::new(ConflictResolution::All);

        let r1 = Rule::builder()
            .id("r1")
            .priority(10)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();
        let r2 = Rule::builder()
            .id("r2")
            .priority(5)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![r1, r2]);

        let a1 = Activation::new(0, HashMap::new(), Vec::new(), 1000);
        let a2 = Activation::new(1, HashMap::new(), Vec::new(), 1000);

        let result = resolver.resolve(vec![a1, a2], &rule_set);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_conflict_priority_deduplicates() {
        let resolver = ConflictResolver::new(ConflictResolution::Priority);

        let r1 = Rule::builder()
            .id("r1")
            .priority(10)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();
        let r2 = Rule::builder()
            .id("r2")
            .priority(5)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![r1, r2]);

        let a1 = Activation::new(0, HashMap::new(), Vec::new(), 1000);
        let a2 = Activation::new(1, HashMap::new(), Vec::new(), 1000);

        let result = resolver.resolve(vec![a1, a2], &rule_set);
        // r1 has higher priority (index 0 after sort) and produces same triple, so r2 is suppressed
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_conflict_priority_keeps_different_triples() {
        let resolver = ConflictResolver::new(ConflictResolution::Priority);

        let r1 = Rule::builder()
            .id("r1")
            .priority(10)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();
        let r2 = Rule::builder()
            .id("r2")
            .priority(5)
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/q")),
                PatternTerm::constant(iri("http://ex.org/c")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![r1, r2]);

        let a1 = Activation::new(0, HashMap::new(), Vec::new(), 1000);
        let a2 = Activation::new(1, HashMap::new(), Vec::new(), 1000);

        let result = resolver.resolve(vec![a1, a2], &rule_set);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_conflict_all_empty_activations() {
        let resolver = ConflictResolver::new(ConflictResolution::All);
        let rule_set = RuleSet::new(vec![]);
        let result = resolver.resolve(vec![], &rule_set);
        assert!(result.is_empty());
    }

    #[test]
    fn test_conflict_priority_empty_activations() {
        let resolver = ConflictResolver::new(ConflictResolution::Priority);
        let rule_set = RuleSet::new(vec![]);
        let result = resolver.resolve(vec![], &rule_set);
        assert!(result.is_empty());
    }

    #[test]
    fn test_conflict_resolver_strategy_accessor() {
        let resolver = ConflictResolver::new(ConflictResolution::Priority);
        assert_eq!(resolver.strategy(), ConflictResolution::Priority);

        let resolver = ConflictResolver::new(ConflictResolution::All);
        assert_eq!(resolver.strategy(), ConflictResolution::All);
    }

    #[test]
    fn test_conflict_priority_three_rules_same_triple() {
        let resolver = ConflictResolver::new(ConflictResolution::Priority);

        let rules: Vec<Rule> = (0..3)
            .map(|i| {
                Rule::builder()
                    .id(format!("r{i}"))
                    .priority(10 - i) // priorities: 10, 9, 8
                    .template(TripleTemplate::new(
                        PatternTerm::constant(iri("http://ex.org/a")),
                        PatternTerm::constant(iri("http://ex.org/p")),
                        PatternTerm::constant(iri("http://ex.org/b")),
                    ))
                    .build()
            })
            .collect();

        let rule_set = RuleSet::new(rules);
        let activations: Vec<Activation> = (0..3)
            .map(|i| Activation::new(i, HashMap::new(), Vec::new(), 1000))
            .collect();

        let result = resolver.resolve(activations, &rule_set);
        // Only highest priority rule should fire (others produce same triple)
        assert_eq!(result.len(), 1);
    }
}
