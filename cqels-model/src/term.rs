use std::fmt;

use oxrdf::{BlankNode, Literal, NamedNode};
use serde::{Deserialize, Serialize};

/// An RDF term — the building block of RDF statements.
///
/// Maps to RDF4J's `Value` interface hierarchy. In Rust we use a flat enum
/// rather than a class hierarchy.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Term {
    /// An IRI (Internationalized Resource Identifier).
    Iri(IriTerm),
    /// A blank node (anonymous resource).
    BlankNode(BlankNodeTerm),
    /// A literal value (string, number, date, etc.).
    Literal(LiteralTerm),
}

/// An IRI term wrapping a string representation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IriTerm {
    value: String,
}

impl IriTerm {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for IriTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.value)
    }
}

/// A blank node term.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlankNodeTerm {
    id: String,
}

impl BlankNodeTerm {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for BlankNodeTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "_:{}", self.id)
    }
}

/// A literal term with an optional language tag or datatype IRI.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LiteralTerm {
    value: String,
    /// The datatype IRI (e.g., xsd:string, xsd:integer).
    datatype: Option<String>,
    /// The language tag (e.g., "en", "de").
    language: Option<String>,
}

impl LiteralTerm {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            datatype: None,
            language: None,
        }
    }

    pub fn with_datatype(mut self, datatype: impl Into<String>) -> Self {
        self.datatype = Some(datatype.into());
        self
    }

    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn datatype(&self) -> Option<&str> {
        self.datatype.as_deref()
    }

    pub fn language(&self) -> Option<&str> {
        self.language.as_deref()
    }
}

impl fmt::Display for LiteralTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.value)?;
        if let Some(lang) = &self.language {
            write!(f, "@{lang}")?;
        } else if let Some(dt) = &self.datatype {
            write!(f, "^^<{dt}>")?;
        }
        Ok(())
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Iri(iri) => iri.fmt(f),
            Term::BlankNode(bn) => bn.fmt(f),
            Term::Literal(lit) => lit.fmt(f),
        }
    }
}

// --- Conversions from oxigraph types ---

impl From<NamedNode> for IriTerm {
    fn from(nn: NamedNode) -> Self {
        IriTerm::new(nn.as_str())
    }
}

impl From<&NamedNode> for IriTerm {
    fn from(nn: &NamedNode) -> Self {
        IriTerm::new(nn.as_str())
    }
}

impl From<NamedNode> for Term {
    fn from(nn: NamedNode) -> Self {
        Term::Iri(IriTerm::from(nn))
    }
}

impl From<BlankNode> for BlankNodeTerm {
    fn from(bn: BlankNode) -> Self {
        BlankNodeTerm::new(bn.as_str())
    }
}

impl From<&BlankNode> for BlankNodeTerm {
    fn from(bn: &BlankNode) -> Self {
        BlankNodeTerm::new(bn.as_str())
    }
}

impl From<BlankNode> for Term {
    fn from(bn: BlankNode) -> Self {
        Term::BlankNode(BlankNodeTerm::from(bn))
    }
}

impl From<Literal> for LiteralTerm {
    fn from(lit: Literal) -> Self {
        let mut term = LiteralTerm::new(lit.value());
        if let Some(lang) = lit.language() {
            term = term.with_language(lang);
        } else {
            term = term.with_datatype(lit.datatype().as_str());
        }
        term
    }
}

impl From<&Literal> for LiteralTerm {
    fn from(lit: &Literal) -> Self {
        let mut term = LiteralTerm::new(lit.value());
        if let Some(lang) = lit.language() {
            term = term.with_language(lang);
        } else {
            term = term.with_datatype(lit.datatype().as_str());
        }
        term
    }
}

impl From<Literal> for Term {
    fn from(lit: Literal) -> Self {
        Term::Literal(LiteralTerm::from(lit))
    }
}

impl From<oxrdf::Term> for Term {
    fn from(term: oxrdf::Term) -> Self {
        match term {
            oxrdf::Term::NamedNode(nn) => Term::from(nn),
            oxrdf::Term::BlankNode(bn) => Term::from(bn),
            oxrdf::Term::Literal(lit) => Term::from(lit),
            oxrdf::Term::Triple(_) => {
                // RDF-star triples are not yet supported
                Term::BlankNode(BlankNodeTerm::new("unsupported-triple"))
            }
        }
    }
}

// --- Conversions back to oxigraph types ---

impl TryFrom<&IriTerm> for NamedNode {
    type Error = oxrdf::IriParseError;

    fn try_from(iri: &IriTerm) -> Result<Self, Self::Error> {
        NamedNode::new(&iri.value)
    }
}

impl TryFrom<&Term> for oxrdf::Term {
    type Error = crate::CqelsError;

    fn try_from(term: &Term) -> Result<Self, Self::Error> {
        match term {
            Term::Iri(iri) => {
                let nn = NamedNode::new(iri.as_str())
                    .map_err(|e| crate::CqelsError::InvalidTerm { detail: e.to_string() })?;
                Ok(oxrdf::Term::NamedNode(nn))
            }
            Term::BlankNode(bn) => {
                let b = BlankNode::new(bn.id())
                    .map_err(|e| crate::CqelsError::InvalidTerm { detail: e.to_string() })?;
                Ok(oxrdf::Term::BlankNode(b))
            }
            Term::Literal(lit) => {
                let l = if let Some(lang) = lit.language() {
                    Literal::new_language_tagged_literal(lit.value(), lang)
                        .map_err(|e| crate::CqelsError::InvalidTerm { detail: e.to_string() })?
                } else if let Some(dt) = lit.datatype() {
                    let dt_nn = NamedNode::new(dt)
                        .map_err(|e| crate::CqelsError::InvalidTerm { detail: e.to_string() })?;
                    Literal::new_typed_literal(lit.value(), dt_nn)
                } else {
                    Literal::new_simple_literal(lit.value())
                };
                Ok(oxrdf::Term::Literal(l))
            }
        }
    }
}

impl Term {
    /// Returns `true` if this term is an IRI.
    pub fn is_iri(&self) -> bool {
        matches!(self, Term::Iri(_))
    }

    /// Returns `true` if this term is a blank node.
    pub fn is_blank_node(&self) -> bool {
        matches!(self, Term::BlankNode(_))
    }

    /// Returns `true` if this term is a literal.
    pub fn is_literal(&self) -> bool {
        matches!(self, Term::Literal(_))
    }

    /// Returns the IRI term if this is an IRI, `None` otherwise.
    pub fn as_iri(&self) -> Option<&IriTerm> {
        match self {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Returns the blank node term if this is a blank node, `None` otherwise.
    pub fn as_blank_node(&self) -> Option<&BlankNodeTerm> {
        match self {
            Term::BlankNode(bn) => Some(bn),
            _ => None,
        }
    }

    /// Returns the literal term if this is a literal, `None` otherwise.
    pub fn as_literal(&self) -> Option<&LiteralTerm> {
        match self {
            Term::Literal(lit) => Some(lit),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iri_term_creation() {
        let iri = IriTerm::new("http://example.org/resource");
        assert_eq!(iri.as_str(), "http://example.org/resource");
        assert_eq!(iri.to_string(), "<http://example.org/resource>");
    }

    #[test]
    fn test_blank_node_creation() {
        let bn = BlankNodeTerm::new("b0");
        assert_eq!(bn.id(), "b0");
        assert_eq!(bn.to_string(), "_:b0");
    }

    #[test]
    fn test_literal_plain() {
        let lit = LiteralTerm::new("hello");
        assert_eq!(lit.value(), "hello");
        assert_eq!(lit.datatype(), None);
        assert_eq!(lit.language(), None);
        assert_eq!(lit.to_string(), "\"hello\"");
    }

    #[test]
    fn test_literal_with_language() {
        let lit = LiteralTerm::new("hello").with_language("en");
        assert_eq!(lit.language(), Some("en"));
        assert_eq!(lit.to_string(), "\"hello\"@en");
    }

    #[test]
    fn test_literal_with_datatype() {
        let lit = LiteralTerm::new("42").with_datatype("http://www.w3.org/2001/XMLSchema#integer");
        assert_eq!(
            lit.datatype(),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
        assert_eq!(
            lit.to_string(),
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn test_term_variants() {
        let iri = Term::Iri(IriTerm::new("http://example.org/s"));
        assert!(iri.is_iri());
        assert!(!iri.is_blank_node());
        assert!(!iri.is_literal());

        let bn = Term::BlankNode(BlankNodeTerm::new("b0"));
        assert!(bn.is_blank_node());

        let lit = Term::Literal(LiteralTerm::new("value"));
        assert!(lit.is_literal());
    }

    #[test]
    fn test_term_equality_and_hash() {
        use std::collections::HashSet;

        let t1 = Term::Iri(IriTerm::new("http://example.org/a"));
        let t2 = Term::Iri(IriTerm::new("http://example.org/a"));
        let t3 = Term::Iri(IriTerm::new("http://example.org/b"));

        assert_eq!(t1, t2);
        assert_ne!(t1, t3);

        let mut set = HashSet::new();
        set.insert(t1.clone());
        set.insert(t2.clone());
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_from_oxigraph_named_node() {
        let nn = NamedNode::new("http://example.org/test").unwrap();
        let term = Term::from(nn);
        assert!(term.is_iri());
        assert_eq!(term.as_iri().unwrap().as_str(), "http://example.org/test");
    }

    #[test]
    fn test_from_oxigraph_blank_node() {
        let bn = BlankNode::new("b1").unwrap();
        let term = Term::from(bn);
        assert!(term.is_blank_node());
        assert_eq!(term.as_blank_node().unwrap().id(), "b1");
    }

    #[test]
    fn test_from_oxigraph_literal() {
        let lit = Literal::new_simple_literal("hello world");
        let term = Term::from(lit);
        assert!(term.is_literal());
        assert_eq!(term.as_literal().unwrap().value(), "hello world");
    }

    #[test]
    fn test_roundtrip_iri() {
        let original = Term::Iri(IriTerm::new("http://example.org/roundtrip"));
        let ox_term: oxrdf::Term = (&original).try_into().unwrap();
        let back = Term::from(ox_term);
        assert_eq!(original, back);
    }

    #[test]
    fn test_roundtrip_literal_with_language() {
        let original = Term::Literal(LiteralTerm::new("bonjour").with_language("fr"));
        let ox_term: oxrdf::Term = (&original).try_into().unwrap();
        let back = Term::from(ox_term);
        assert_eq!(original, back);
    }

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_iri_term_empty() {
        let iri = IriTerm::new("");
        assert_eq!(iri.as_str(), "");
        assert_eq!(iri.to_string(), "<>");
    }

    #[test]
    fn test_iri_term_with_special_chars() {
        let iri = IriTerm::new("http://example.org/resource#fragment?query=1&foo=bar");
        assert!(iri.as_str().contains('#'));
        assert!(iri.as_str().contains('?'));
    }

    #[test]
    fn test_blank_node_uniqueness() {
        let bn1 = BlankNodeTerm::new("b1");
        let bn2 = BlankNodeTerm::new("b2");
        assert_ne!(bn1.id(), bn2.id());
        assert_ne!(bn1, bn2);
    }

    #[test]
    fn test_literal_empty_value() {
        let lit = LiteralTerm::new("");
        assert_eq!(lit.value(), "");
        assert_eq!(lit.to_string(), "\"\"");
    }

    #[test]
    fn test_literal_with_both_language_and_datatype() {
        // Language tag takes precedence over datatype in display
        let lit = LiteralTerm::new("hello")
            .with_language("en")
            .with_datatype("http://www.w3.org/2001/XMLSchema#string");
        // When both set, language should still be accessible
        assert_eq!(lit.language(), Some("en"));
    }

    #[test]
    fn test_term_as_accessors_return_none_for_wrong_type() {
        let iri = Term::Iri(IriTerm::new("http://example.org"));
        assert!(iri.as_iri().is_some());
        assert!(iri.as_blank_node().is_none());
        assert!(iri.as_literal().is_none());

        let bn = Term::BlankNode(BlankNodeTerm::new("b0"));
        assert!(bn.as_iri().is_none());
        assert!(bn.as_blank_node().is_some());
        assert!(bn.as_literal().is_none());

        let lit = Term::Literal(LiteralTerm::new("v"));
        assert!(lit.as_iri().is_none());
        assert!(lit.as_blank_node().is_none());
        assert!(lit.as_literal().is_some());
    }

    #[test]
    fn test_term_clone_and_debug() {
        let term = Term::Iri(IriTerm::new("http://example.org/test"));
        let cloned = term.clone();
        assert_eq!(term, cloned);
        let debug = format!("{:?}", term);
        assert!(debug.contains("Iri"));
    }

    #[test]
    fn test_term_display() {
        let iri = Term::Iri(IriTerm::new("http://example.org/s"));
        assert_eq!(iri.to_string(), "<http://example.org/s>");

        let bn = Term::BlankNode(BlankNodeTerm::new("x1"));
        assert_eq!(bn.to_string(), "_:x1");

        let lit = Term::Literal(LiteralTerm::new("42").with_datatype("http://www.w3.org/2001/XMLSchema#integer"));
        assert!(lit.to_string().contains("42"));
    }

    #[test]
    fn test_roundtrip_blank_node() {
        let original = Term::BlankNode(BlankNodeTerm::new("bn42"));
        let ox_term: oxrdf::Term = (&original).try_into().unwrap();
        let back = Term::from(ox_term);
        assert_eq!(original, back);
    }

    #[test]
    fn test_roundtrip_typed_literal() {
        let original = Term::Literal(
            LiteralTerm::new("3.14")
                .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
        );
        let ox_term: oxrdf::Term = (&original).try_into().unwrap();
        let back = Term::from(ox_term);
        assert_eq!(original, back);
    }

    #[test]
    fn test_different_term_types_not_equal() {
        let iri = Term::Iri(IriTerm::new("b0"));
        let bn = Term::BlankNode(BlankNodeTerm::new("b0"));
        assert_ne!(iri, bn, "IRI and BlankNode should not be equal even with same string");
    }

    #[test]
    fn test_large_literal_value() {
        let large = "x".repeat(10000);
        let lit = LiteralTerm::new(&large);
        assert_eq!(lit.value().len(), 10000);
    }

    #[test]
    fn test_unicode_literal() {
        let lit = LiteralTerm::new("日本語テスト 🌍");
        assert_eq!(lit.value(), "日本語テスト 🌍");
    }
}
