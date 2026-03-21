use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::term::{IriTerm, LiteralTerm, Term};

/// A typed value extracted from an RDF term, used in expression evaluation
/// and binding sets.
///
/// Maps to the various typed representations needed when evaluating
/// SPARQL/Cypher expressions.
///
/// # Examples
///
/// ```
/// use cqels_model::Value;
///
/// // Create values from Rust types
/// let i = Value::from(42i64);
/// let f = Value::from(3.14f64);
/// let s = Value::from("hello");
/// let b = Value::from(true);
///
/// // Extract typed values
/// assert_eq!(i.as_integer(), Some(42));
/// assert_eq!(f.as_float(), Some(3.14));
/// assert_eq!(s.as_string(), Some("hello"));
/// assert_eq!(b.as_boolean(), Some(true));
///
/// // Convert to RDF terms
/// let term = i.to_term().unwrap();
/// assert!(term.is_literal());
///
/// // Null handling
/// assert!(Value::Null.is_null());
/// assert!(Value::Null.to_term().is_none());
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
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
    ///
    /// Returns `None` for non-finite floats (NaN, Infinity) or values outside i64 range.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => {
                if f.is_finite() && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    Some(*f as i64)
                } else {
                    None
                }
            }
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

    /// Attempts to extract a numeric value as f64.
    ///
    /// Converts `Integer` and `Float` variants to `f64`.
    /// Returns `None` for non-numeric types.
    pub fn as_numeric(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns `true` if this value is a numeric type (`Integer` or `Float`).
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Integer(_) | Value::Float(_))
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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // Numeric comparison with Integer→Float promotion
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),

            // Boolean comparison
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),

            // String comparison
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),

            // Term comparison (IRI string comparison)
            (Value::Term(a), Value::Term(b)) => a.to_string().partial_cmp(&b.to_string()),

            // Null is not comparable
            (Value::Null, _) | (_, Value::Null) => None,

            // Different incompatible types are not comparable
            _ => None,
        }
    }
}

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
        assert_eq!(Value::from(3.15f64), Value::Float(3.15));
        assert_eq!(Value::from(true), Value::Boolean(true));
        assert_eq!(Value::from("hello"), Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_as_conversions() {
        assert_eq!(Value::Integer(42).as_integer(), Some(42));
        assert_eq!(Value::Float(3.15).as_float(), Some(3.15));
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
        assert_eq!(Value::Float(3.15).to_string(), "3.15");
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

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_value_wrong_type_conversions() {
        assert_eq!(Value::String("hello".into()).as_integer(), None);
        assert_eq!(Value::Boolean(true).as_integer(), None);
        assert_eq!(Value::Null.as_integer(), None);
        assert_eq!(Value::Null.as_float(), None);
        assert_eq!(Value::Null.as_string(), None);
        assert_eq!(Value::Null.as_boolean(), None);
    }

    #[test]
    fn test_value_negative_numbers() {
        let neg_int = Value::Integer(-100);
        assert_eq!(neg_int.as_integer(), Some(-100));
        assert_eq!(neg_int.to_string(), "-100");

        let neg_float = Value::Float(-2.72);
        assert_eq!(neg_float.as_float(), Some(-2.72));
    }

    #[test]
    fn test_value_boundary_integers() {
        let max = Value::Integer(i64::MAX);
        assert_eq!(max.as_integer(), Some(i64::MAX));

        let min = Value::Integer(i64::MIN);
        assert_eq!(min.as_integer(), Some(i64::MIN));
    }

    #[test]
    fn test_value_float_special_cases() {
        let inf = Value::Float(f64::INFINITY);
        assert_eq!(inf.as_float(), Some(f64::INFINITY));

        let neg_inf = Value::Float(f64::NEG_INFINITY);
        assert_eq!(neg_inf.as_float(), Some(f64::NEG_INFINITY));

        let nan = Value::Float(f64::NAN);
        assert!(nan.as_float().unwrap().is_nan());
    }

    #[test]
    fn test_value_empty_string() {
        let v = Value::String(String::new());
        assert_eq!(v.as_string(), Some(""));
        assert!(!v.is_null());
    }

    #[test]
    fn test_value_to_term_all_types() {
        assert!(Value::Integer(42).to_term().is_some());
        assert!(Value::Float(2.72).to_term().is_some());
        assert!(Value::Boolean(true).to_term().is_some());
        assert!(Value::String("hello".into()).to_term().is_some());
        assert!(Value::Null.to_term().is_none());
    }

    #[test]
    fn test_value_different_types_not_equal() {
        // Integer 42 and Float 42.0 should not be equal as values
        let int_val = Value::Integer(42);
        let float_val = Value::Float(42.0);
        assert_ne!(int_val, float_val);
    }

    #[test]
    fn test_value_ordering() {
        let v1 = Value::Integer(1);
        let v2 = Value::Integer(2);
        assert_ne!(v1, v2);
        assert!(v1 < v2);
        assert!(v2 > v1);
    }

    #[test]
    fn test_value_as_numeric() {
        assert_eq!(Value::Integer(42).as_numeric(), Some(42.0));
        assert_eq!(Value::Float(3.15).as_numeric(), Some(3.15));
        assert_eq!(Value::String("hello".into()).as_numeric(), None);
        assert_eq!(Value::Boolean(true).as_numeric(), None);
        assert_eq!(Value::Null.as_numeric(), None);
    }

    #[test]
    fn test_value_is_numeric() {
        assert!(Value::Integer(1).is_numeric());
        assert!(Value::Float(1.0).is_numeric());
        assert!(!Value::String("1".into()).is_numeric());
        assert!(!Value::Boolean(true).is_numeric());
        assert!(!Value::Null.is_numeric());
    }

    #[test]
    fn test_value_partial_ord_cross_type() {
        // Integer vs Float promotion
        assert!(Value::Integer(1) < Value::Float(2.0));
        assert!(Value::Float(1.0) < Value::Integer(2));
        assert_eq!(
            Value::Integer(5).partial_cmp(&Value::Float(5.0)),
            Some(std::cmp::Ordering::Equal)
        );

        // Null is not comparable
        assert_eq!(Value::Null.partial_cmp(&Value::Integer(1)), None);
        assert_eq!(Value::Integer(1).partial_cmp(&Value::Null), None);

        // Different incompatible types are not comparable
        assert_eq!(
            Value::Integer(1).partial_cmp(&Value::String("1".into())),
            None
        );
    }

    #[test]
    fn test_value_clone() {
        let v = Value::String("test".into());
        let cloned = v.clone();
        assert_eq!(v, cloned);
    }
}
