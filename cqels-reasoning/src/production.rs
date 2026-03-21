//! Production nodes (activations) for the RETE reasoning engine.
//!
//! An [`Activation`] represents a fully matched rule with concrete variable
//! bindings, ready to fire and produce inferred RDF statements by
//! instantiating the rule's consequent templates.

use std::collections::HashMap;
use std::fmt;

use cqels_model::{Statement, Term};

use crate::rule::Rule;

/// An activation represents a rule that is ready to fire with specific bindings.
///
/// Stores a rule index (into a `RuleSet`) rather than a reference, to avoid
/// lifetime entanglement with the beta network.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use cqels_reasoning::{Activation, Rule, PatternTerm, TripleTemplate};
/// use cqels_model::{Term, IriTerm};
///
/// let rule = Rule::builder()
///     .id("r1")
///     .template(TripleTemplate::new(
///         PatternTerm::var("s"),
///         PatternTerm::constant(Term::Iri(IriTerm::new("http://ex.org/is"))),
///         PatternTerm::var("o"),
///     ))
///     .build();
///
/// let mut bindings = HashMap::new();
/// bindings.insert("s".to_string(), Term::Iri(IriTerm::new("http://ex.org/alice")));
/// bindings.insert("o".to_string(), Term::Iri(IriTerm::new("http://ex.org/Person")));
///
/// let activation = Activation::new(0, bindings, vec![], 1000);
/// let inferred = activation.fire(&rule);
/// assert_eq!(inferred.len(), 1);
/// ```
pub struct Activation {
    rule_index: usize,
    bindings: HashMap<String, Term>,
    premises: Vec<Statement>,
    timestamp: i64,
}

impl Activation {
    pub fn new(
        rule_index: usize,
        bindings: HashMap<String, Term>,
        premises: Vec<Statement>,
        timestamp: i64,
    ) -> Self {
        Self {
            rule_index,
            bindings,
            premises,
            timestamp,
        }
    }

    pub fn rule_index(&self) -> usize {
        self.rule_index
    }

    pub fn bindings(&self) -> &HashMap<String, Term> {
        &self.bindings
    }

    pub fn premises(&self) -> &[Statement] {
        &self.premises
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Fires the activation against a rule, producing inferred statements.
    pub fn fire(&self, rule: &Rule) -> Vec<Statement> {
        rule.consequent().instantiate(&self.bindings)
    }
}

impl fmt::Debug for Activation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Activation")
            .field("rule_index", &self.rule_index)
            .field("bindings_count", &self.bindings.len())
            .field("premises_count", &self.premises.len())
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{PatternTerm, TriplePattern, TripleTemplate};
    use cqels_model::term::IriTerm;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    #[test]
    fn test_activation_accessors() {
        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/alice"));
        let premises = vec![Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Person"),
        )];

        let activation = Activation::new(3, bindings.clone(), premises.clone(), 5000);
        assert_eq!(activation.rule_index(), 3);
        assert_eq!(activation.timestamp(), 5000);
        assert_eq!(activation.bindings().len(), 1);
        assert_eq!(
            activation.bindings().get("s"),
            Some(&iri("http://ex.org/alice"))
        );
        assert_eq!(activation.premises().len(), 1);
        assert_eq!(activation.premises()[0].subject, iri("http://ex.org/alice"));
    }

    #[test]
    fn test_activation_debug() {
        let activation = Activation::new(0, HashMap::new(), Vec::new(), 1000);
        let debug = format!("{:?}", activation);
        assert!(debug.contains("Activation"));
        assert!(debug.contains("rule_index"));
        assert!(debug.contains("bindings_count"));
        assert!(debug.contains("premises_count"));
        assert!(debug.contains("timestamp"));
    }

    #[test]
    fn test_activation_empty_bindings() {
        let rule = Rule::builder()
            .id("r1")
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/a")),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::constant(iri("http://ex.org/b")),
            ))
            .build();

        let activation = Activation::new(0, HashMap::new(), Vec::new(), 0);
        let results = activation.fire(&rule);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, iri("http://ex.org/a"));
    }

    #[test]
    fn test_activation_multiple_templates() {
        let rule = Rule::builder()
            .id("r1")
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/has")),
                PatternTerm::constant(iri("http://ex.org/X")),
            ))
            .build();

        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/alice"));
        bindings.insert("o".to_string(), iri("http://ex.org/Person"));

        let activation = Activation::new(0, bindings, Vec::new(), 1000);
        let results = activation.fire(&rule);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_activation_fire() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::var("o"),
            ))
            .build();

        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/alice"));
        bindings.insert("o".to_string(), iri("http://ex.org/Person"));

        let activation = Activation::new(0, bindings, Vec::new(), 1000);
        let results = activation.fire(&rule);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, iri("http://ex.org/alice"));
        assert_eq!(results[0].predicate.as_str(), "http://ex.org/is");
        assert_eq!(results[0].object, iri("http://ex.org/Person"));
    }
}
