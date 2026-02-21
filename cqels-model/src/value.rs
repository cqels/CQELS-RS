use std::fmt;

use serde::{Deserialize, Serialize};

use crate::term::{IriTerm, LiteralTerm, Term};

/// A typed value extracted from an RDF term, used in expression evaluation
/// and binding sets.
///
/// Maps to the various typed representations needed when evaluating
/// SPARQL/Cypher expressions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// An RDF term (IRI, blank node, or literal).
    Term(Term),
    /// A string value.
    String(String),
    /// A 64-bit integer.
    Integer(i64),
    /// A 64-bit floating point number.
    Float(f64),
    /// A boolean value.
    Boolean(bool),
    /// A null/unbound value.
    Null,
}

impl Value {
    /// Creates a Value from an RDF Term.
    pub fn from_term(term: Term) -> Self {
        Value::Term(term)
    }

    /// Attempts to extract a string value.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            Value::Term(Term::Literal(lit)) => Some(lit.value()),
            Value::Term(Term::Iri(iri)) => Some(iri.as_str()),
            _ => None,
        }
    }

    /// Attempts to extract an integer value.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            Value::Term(Term::Literal(lit)) => lit.value().parse().ok(),
            _ => None,
        }
    }

    /// Attempts to extract a float value.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            Value::Term(Term::Literal(lit)) => lit.value().parse().ok(),
            _ => None,
        }
    }

    /// Attempts to extract a boolean value.
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Term(Term::Literal(lit)) => lit.value().parse().ok(),
            _ => None,
        }
    }

    /// Returns `true` if this value is null/unbound.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns `true` if this value is an IRI.
    pub fn is_iri(&self) -> bool {
        matches!(self, Value::Term(Term::Iri(_)))
    }

    /// Returns `true` if this value is a blank node.
    pub fn is_blank_node(&self) -> bool {
        matches!(self, Value::Term(Term::BlankNode(_)))
    }

    /// Returns `true` if this value is a literal.
    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            Value::Term(Term::Literal(_))
                | Value::String(_)
                | Value::Integer(_)
                | Value::Float(_)
                | Value::Boolean(_)
        )
    }

    /// Converts this value to an RDF Term if possible.
    pub fn to_term(&self) -> Option<Term> {
        match self {
            Value::Term(t) => Some(t.clone()),
            Value::String(s) => Some(Term::Literal(LiteralTerm::new(s))),
            Value::Integer(i) => Some(Term::Literal(
                LiteralTerm::new(i.to_string())
                    .with_datatype("http://www.w3.org/2001/XMLSchema#integer"),
            )),
            Value::Float(f) => Some(Term::Literal(
                LiteralTerm::new(f.to_string())
                    .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
            )),
            Value::Boolean(b) => Some(Term::Literal(
                LiteralTerm::new(b.to_string())
                    .with_datatype("http://www.w3.org/2001/XMLSchema#boolean"),
            )),
            Value::Null => None,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Term(t) => t.hash(state),
            Value::String(s) => s.hash(state),
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Null => {}
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Term(t) => t.fmt(f),
            Value::String(s) => write!(f, "\"{s}\""),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Null => write!(f, "null"),
        }
    }
}

impl From<Term> for Value {
    fn from(term: Term) -> Self {
        Value::Term(term)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<IriTerm> for Value {
    fn from(iri: IriTerm) -> Self {
        Value::Term(Term::Iri(iri))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::IriTerm;

    #[test]
    fn test_value_from_types() {
        assert_eq!(Value::from(42i64), Value::Integer(42));
        assert_eq!(Value::from(3.14f64), Value::Float(3.14));
        assert_eq!(Value::from(true), Value::Boolean(true));
        assert_eq!(Value::from("hello"), Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_as_conversions() {
        assert_eq!(Value::Integer(42).as_integer(), Some(42));
        assert_eq!(Value::Float(3.14).as_float(), Some(3.14));
        assert_eq!(Value::Boolean(true).as_boolean(), Some(true));
        assert_eq!(Value::String("hi".into()).as_string(), Some("hi"));
    }

    #[test]
    fn test_value_cross_type_conversions() {
        assert_eq!(Value::Float(3.0).as_integer(), Some(3));
        assert_eq!(Value::Integer(5).as_float(), Some(5.0));
    }

    #[test]
    fn test_value_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Integer(0).is_null());
    }

    #[test]
    fn test_value_is_iri() {
        let v = Value::from(Term::Iri(IriTerm::new("http://example.org")));
        assert!(v.is_iri());
        assert!(!Value::Integer(0).is_iri());
    }

    #[test]
    fn test_value_to_term() {
        let v = Value::Integer(42);
        let term = v.to_term().unwrap();
        assert!(term.is_literal());

        assert!(Value::Null.to_term().is_none());
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Float(3.14).to_string(), "3.14");
        assert_eq!(Value::Boolean(true).to_string(), "true");
        assert_eq!(Value::Null.to_string(), "null");
    }

    #[test]
    fn test_value_hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Value::Integer(42));
        set.insert(Value::Integer(42));
        assert_eq!(set.len(), 1);
    }
}
