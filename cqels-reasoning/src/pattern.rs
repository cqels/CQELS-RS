use std::collections::HashMap;
use std::fmt;

use cqels_model::term::IriTerm;
use cqels_model::{Statement, Term};

/// A term in a triple pattern — either a variable or a constant.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::PatternTerm;
/// use cqels_model::{Term, IriTerm};
///
/// let var = PatternTerm::var("x");
/// assert!(var.is_variable());
/// assert_eq!(var.variable_name(), Some("x"));
///
/// let constant = PatternTerm::constant(Term::Iri(IriTerm::new("http://example.org/p")));
/// assert!(constant.is_constant());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PatternTerm {
    Variable(String),
    Constant(Term),
}

impl PatternTerm {
    pub fn var(name: impl Into<String>) -> Self {
        PatternTerm::Variable(name.into())
    }

    pub fn constant(term: Term) -> Self {
        PatternTerm::Constant(term)
    }

    pub fn is_variable(&self) -> bool {
        matches!(self, PatternTerm::Variable(_))
    }

    pub fn is_constant(&self) -> bool {
        matches!(self, PatternTerm::Constant(_))
    }

    pub fn variable_name(&self) -> Option<&str> {
        match self {
            PatternTerm::Variable(name) => Some(name),
            PatternTerm::Constant(_) => None,
        }
    }
}

impl fmt::Display for PatternTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternTerm::Variable(name) => write!(f, "?{name}"),
            PatternTerm::Constant(term) => write!(f, "{term}"),
        }
    }
}

/// A triple pattern for matching RDF statements.
///
/// Each position (subject, predicate, object) can be a variable or a constant.
/// Variables bind to the actual values in matched statements.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::{TriplePattern, PatternTerm};
/// use cqels_model::{Statement, Term, IriTerm};
///
/// // Match any statement with predicate rdf:type
/// let pattern = TriplePattern::new(
///     PatternTerm::var("s"),
///     PatternTerm::constant(Term::Iri(IriTerm::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))),
///     PatternTerm::var("o"),
/// );
///
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://example.org/alice")),
///     IriTerm::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
///     Term::Iri(IriTerm::new("http://example.org/Person")),
/// );
/// let bindings = pattern.match_statement(&stmt).unwrap();
/// assert_eq!(bindings.get("s"), Some(&Term::Iri(IriTerm::new("http://example.org/alice"))));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TriplePattern {
    pub subject: PatternTerm,
    pub predicate: PatternTerm,
    pub object: PatternTerm,
}

impl TriplePattern {
    pub fn new(subject: PatternTerm, predicate: PatternTerm, object: PatternTerm) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }

    /// Attempts to match this pattern against a statement, returning bindings if successful.
    ///
    /// Returns `None` if the pattern does not match.
    ///
    /// Optimized: avoids cloning terms until a variable actually needs to bind,
    /// and uses a capacity hint for the bindings HashMap.
    pub fn match_statement(&self, stmt: &Statement) -> Option<HashMap<String, Term>> {
        // At most 3 variables can bind (subject, predicate, object)
        let mut bindings = HashMap::with_capacity(3);

        if !self.match_position(&self.subject, &stmt.subject, &mut bindings) {
            return None;
        }

        // Only construct Term::Iri wrapper when needed for the predicate position
        let predicate_term = Term::Iri(stmt.predicate.clone());
        if !self.match_position(&self.predicate, &predicate_term, &mut bindings) {
            return None;
        }

        if !self.match_position(&self.object, &stmt.object, &mut bindings) {
            return None;
        }

        Some(bindings)
    }

    /// Checks if this pattern matches a statement (without returning bindings).
    pub fn matches(&self, stmt: &Statement) -> bool {
        self.match_statement(stmt).is_some()
    }

    fn match_position(
        &self,
        pattern_term: &PatternTerm,
        actual: &Term,
        bindings: &mut HashMap<String, Term>,
    ) -> bool {
        match pattern_term {
            PatternTerm::Constant(expected) => expected == actual,
            PatternTerm::Variable(name) => {
                if let Some(existing) = bindings.get(name) {
                    existing == actual
                } else {
                    bindings.insert(name.clone(), actual.clone());
                    true
                }
            }
        }
    }

    /// Returns the set of variable names used in this pattern.
    pub fn variable_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let PatternTerm::Variable(name) = &self.subject {
            names.push(name.clone());
        }
        if let PatternTerm::Variable(name) = &self.predicate {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        if let PatternTerm::Variable(name) = &self.object {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    }

    pub fn has_constant_subject(&self) -> bool {
        self.subject.is_constant()
    }

    pub fn has_constant_predicate(&self) -> bool {
        self.predicate.is_constant()
    }

    pub fn has_constant_object(&self) -> bool {
        self.object.is_constant()
    }
}

impl fmt::Display for TriplePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.subject, self.predicate, self.object)
    }
}

/// A template for producing new RDF statements from variable bindings.
///
/// Used in rule consequents to instantiate inferred triples.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use cqels_reasoning::{TripleTemplate, PatternTerm};
/// use cqels_model::{Term, IriTerm};
///
/// let template = TripleTemplate::new(
///     PatternTerm::var("s"),
///     PatternTerm::constant(Term::Iri(IriTerm::new("http://example.org/inferred"))),
///     PatternTerm::var("o"),
/// );
///
/// let mut bindings = HashMap::new();
/// bindings.insert("s".to_string(), Term::Iri(IriTerm::new("http://example.org/a")));
/// bindings.insert("o".to_string(), Term::Iri(IriTerm::new("http://example.org/b")));
///
/// let stmt = template.instantiate(&bindings).unwrap();
/// assert_eq!(stmt.predicate.as_str(), "http://example.org/inferred");
/// ```
#[derive(Clone, Debug)]
pub struct TripleTemplate {
    pub subject: PatternTerm,
    pub predicate: PatternTerm,
    pub object: PatternTerm,
}

impl TripleTemplate {
    pub fn new(subject: PatternTerm, predicate: PatternTerm, object: PatternTerm) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }

    /// Instantiates this template with bindings, producing a Statement.
    ///
    /// Returns `None` if any position cannot be resolved (e.g., unbound variable).
    pub fn instantiate(&self, bindings: &HashMap<String, Term>) -> Option<Statement> {
        let subject = self.resolve_term(&self.subject, bindings)?;
        let predicate = self.resolve_as_iri(&self.predicate, bindings)?;
        let object = self.resolve_term(&self.object, bindings)?;
        Some(Statement::new(subject, predicate, object))
    }

    fn resolve_term(
        &self,
        pattern_term: &PatternTerm,
        bindings: &HashMap<String, Term>,
    ) -> Option<Term> {
        match pattern_term {
            PatternTerm::Constant(term) => Some(term.clone()),
            PatternTerm::Variable(name) => bindings.get(name).cloned(),
        }
    }

    fn resolve_as_iri(
        &self,
        pattern_term: &PatternTerm,
        bindings: &HashMap<String, Term>,
    ) -> Option<IriTerm> {
        let term = self.resolve_term(pattern_term, bindings)?;
        match term {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }
}

impl fmt::Display for TripleTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.subject, self.predicate, self.object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::{IriTerm, LiteralTerm};

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    fn lit(s: &str) -> Term {
        Term::Literal(LiteralTerm::new(s))
    }

    #[test]
    fn test_pattern_match_all_variables() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/knows"),
            iri("http://ex.org/bob"),
        );
        let bindings = pattern.match_statement(&stmt).unwrap();
        assert_eq!(bindings.get("s"), Some(&iri("http://ex.org/alice")));
        assert_eq!(bindings.get("p"), Some(&iri("http://ex.org/knows")));
        assert_eq!(bindings.get("o"), Some(&iri("http://ex.org/bob")));
    }

    #[test]
    fn test_pattern_match_with_constant() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Person"),
        );
        let bindings = pattern.match_statement(&stmt).unwrap();
        assert_eq!(bindings.get("s"), Some(&iri("http://ex.org/alice")));
    }

    #[test]
    fn test_pattern_no_match_constant_mismatch() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/knows"),
            iri("http://ex.org/bob"),
        );
        assert!(pattern.match_statement(&stmt).is_none());
    }

    #[test]
    fn test_pattern_same_variable_consistency() {
        let pattern = TriplePattern::new(
            PatternTerm::var("x"),
            PatternTerm::var("p"),
            PatternTerm::var("x"),
        );
        // Subject and object are the same — should match
        let stmt1 = Statement::new(
            iri("http://ex.org/a"),
            IriTerm::new("http://ex.org/rel"),
            iri("http://ex.org/a"),
        );
        assert!(pattern.match_statement(&stmt1).is_some());

        // Subject and object differ — should NOT match
        let stmt2 = Statement::new(
            iri("http://ex.org/a"),
            IriTerm::new("http://ex.org/rel"),
            iri("http://ex.org/b"),
        );
        assert!(pattern.match_statement(&stmt2).is_none());
    }

    #[test]
    fn test_pattern_variable_names() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::var("o"),
        );
        let names = pattern.variable_names();
        assert!(names.contains(&"s".to_string()));
        assert!(names.contains(&"o".to_string()));
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_template_instantiate() {
        let template = TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/alice"));

        let stmt = template.instantiate(&bindings).unwrap();
        assert_eq!(stmt.subject, iri("http://ex.org/alice"));
        assert_eq!(stmt.predicate.as_str(), "http://ex.org/type");
        assert_eq!(stmt.object, iri("http://ex.org/Person"));
    }

    #[test]
    fn test_template_unbound_variable() {
        let template = TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let bindings = HashMap::new();
        assert!(template.instantiate(&bindings).is_none());
    }

    #[test]
    fn test_template_predicate_must_be_iri() {
        let template = TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/s"));
        bindings.insert("p".to_string(), lit("not-an-iri")); // Literal can't be predicate
        bindings.insert("o".to_string(), iri("http://ex.org/o"));
        assert!(template.instantiate(&bindings).is_none());
    }

    #[test]
    fn test_pattern_term_display() {
        let var = PatternTerm::var("s");
        assert_eq!(var.to_string(), "?s");

        let constant = PatternTerm::constant(iri("http://ex.org/x"));
        assert_eq!(constant.to_string(), "<http://ex.org/x>");
    }

    #[test]
    fn test_pattern_term_accessors() {
        let var = PatternTerm::var("name");
        assert!(var.is_variable());
        assert!(!var.is_constant());
        assert_eq!(var.variable_name(), Some("name"));

        let constant = PatternTerm::constant(iri("http://ex.org/x"));
        assert!(!constant.is_variable());
        assert!(constant.is_constant());
        assert_eq!(constant.variable_name(), None);
    }

    #[test]
    fn test_pattern_has_constant_methods() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::var("o"),
        );
        assert!(!pattern.has_constant_subject());
        assert!(pattern.has_constant_predicate());
        assert!(!pattern.has_constant_object());
    }

    #[test]
    fn test_pattern_display() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::var("o"),
        );
        let display = pattern.to_string();
        assert!(display.contains("?s"));
        assert!(display.contains("http://ex.org/p"));
        assert!(display.contains("?o"));
    }

    #[test]
    fn test_pattern_variable_names_dedup() {
        // Same variable in subject and object
        let pattern = TriplePattern::new(
            PatternTerm::var("x"),
            PatternTerm::var("p"),
            PatternTerm::var("x"),
        );
        let names = pattern.variable_names();
        assert_eq!(names.len(), 2); // x and p, not x, p, x
        assert!(names.contains(&"x".to_string()));
        assert!(names.contains(&"p".to_string()));
    }

    #[test]
    fn test_pattern_no_variables() {
        let pattern = TriplePattern::new(
            PatternTerm::constant(iri("http://ex.org/s")),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::constant(iri("http://ex.org/o")),
        );
        assert!(pattern.variable_names().is_empty());

        // Exact match
        let stmt = Statement::new(
            iri("http://ex.org/s"),
            IriTerm::new("http://ex.org/p"),
            iri("http://ex.org/o"),
        );
        assert!(pattern.matches(&stmt));
    }

    #[test]
    fn test_pattern_matches_shorthand() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let stmt = Statement::new(
            iri("http://ex.org/a"),
            IriTerm::new("http://ex.org/b"),
            iri("http://ex.org/c"),
        );
        assert!(pattern.matches(&stmt));
    }

    #[test]
    fn test_pattern_match_literal_object() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/label")),
            PatternTerm::var("o"),
        );
        let stmt = Statement::new(
            iri("http://ex.org/x"),
            IriTerm::new("http://ex.org/label"),
            lit("Hello World"),
        );
        let bindings = pattern.match_statement(&stmt).unwrap();
        assert_eq!(bindings.get("o"), Some(&lit("Hello World")));
    }

    #[test]
    fn test_template_all_constants() {
        let template = TripleTemplate::new(
            PatternTerm::constant(iri("http://ex.org/s")),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::constant(iri("http://ex.org/o")),
        );
        let bindings = HashMap::new();
        let stmt = template.instantiate(&bindings).unwrap();
        assert_eq!(stmt.subject, iri("http://ex.org/s"));
        assert_eq!(stmt.predicate.as_str(), "http://ex.org/p");
        assert_eq!(stmt.object, iri("http://ex.org/o"));
    }

    #[test]
    fn test_template_display() {
        let template = TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::var("o"),
        );
        let display = template.to_string();
        assert!(display.contains("?s"));
        assert!(display.contains("?o"));
    }

    #[test]
    fn test_pattern_clone_and_eq() {
        let p1 = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/p")),
            PatternTerm::var("o"),
        );
        let p2 = p1.clone();
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_pattern_term_clone_and_eq() {
        let t1 = PatternTerm::var("x");
        let t2 = t1.clone();
        assert_eq!(t1, t2);

        let t3 = PatternTerm::constant(iri("http://ex.org/a"));
        let t4 = PatternTerm::constant(iri("http://ex.org/b"));
        assert_ne!(t3, t4);
    }
}
