use std::fmt;

use serde::{Deserialize, Serialize};

use crate::term::{IriTerm, Term};
use crate::CqelsError;

/// An RDF statement (triple or quad).
///
/// Maps to RDF4J's `Statement` interface. Contains a subject, predicate, object,
/// and an optional named graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Statement {
    pub subject: Term,
    pub predicate: IriTerm,
    pub object: Term,
    pub graph: Option<IriTerm>,
}

impl Statement {
    /// Creates a new triple (statement without named graph).
    pub fn new(subject: Term, predicate: IriTerm, object: Term) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph: None,
        }
    }

    /// Creates a new quad (statement with named graph).
    pub fn new_quad(
        subject: Term,
        predicate: IriTerm,
        object: Term,
        graph: IriTerm,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph: Some(graph),
        }
    }

    /// Returns `true` if this statement is in a named graph.
    pub fn is_quad(&self) -> bool {
        self.graph.is_some()
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.subject, self.predicate, self.object)?;
        if let Some(g) = &self.graph {
            write!(f, " {g}")?;
        }
        write!(f, " .")
    }
}

// --- Conversions from oxigraph types ---

impl From<oxrdf::Quad> for Statement {
    fn from(quad: oxrdf::Quad) -> Self {
        let subject = match quad.subject {
            oxrdf::Subject::NamedNode(nn) => Term::from(nn),
            oxrdf::Subject::BlankNode(bn) => Term::from(bn),
            oxrdf::Subject::Triple(_) => {
                Term::BlankNode(crate::term::BlankNodeTerm::new("unsupported-triple"))
            }
        };
        let predicate = IriTerm::from(quad.predicate);
        let object = Term::from(quad.object);
        let graph = match quad.graph_name {
            oxrdf::GraphName::NamedNode(nn) => Some(IriTerm::from(nn)),
            _ => None,
        };
        Statement {
            subject,
            predicate,
            object,
            graph,
        }
    }
}

impl TryFrom<&Statement> for oxrdf::Quad {
    type Error = CqelsError;

    fn try_from(stmt: &Statement) -> Result<Self, Self::Error> {
        let subject = match &stmt.subject {
            Term::Iri(iri) => {
                let nn = oxrdf::NamedNode::new(iri.as_str())
                    .map_err(|e| CqelsError::InvalidTerm { detail: e.to_string() })?;
                oxrdf::Subject::NamedNode(nn)
            }
            Term::BlankNode(bn) => {
                let b = oxrdf::BlankNode::new(bn.id())
                    .map_err(|e| CqelsError::InvalidTerm { detail: e.to_string() })?;
                oxrdf::Subject::BlankNode(b)
            }
            Term::Literal(_) => {
                return Err(CqelsError::InvalidTerm {
                    detail: "literal cannot be a subject".to_string(),
                });
            }
        };

        let predicate = oxrdf::NamedNode::new(stmt.predicate.as_str())
            .map_err(|e| CqelsError::InvalidTerm { detail: e.to_string() })?;

        let object: oxrdf::Term = (&stmt.object)
            .try_into()
            .map_err(|e: CqelsError| CqelsError::InvalidTerm { detail: e.to_string() })?;

        let graph_name = match &stmt.graph {
            Some(g) => {
                let nn = oxrdf::NamedNode::new(g.as_str())
                    .map_err(|e| CqelsError::InvalidTerm { detail: e.to_string() })?;
                oxrdf::GraphName::NamedNode(nn)
            }
            None => oxrdf::GraphName::DefaultGraph,
        };

        Ok(oxrdf::Quad::new(subject, predicate, object, graph_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::{BlankNodeTerm, LiteralTerm};

    fn example_statement() -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new("http://example.org/subject")),
            IriTerm::new("http://example.org/predicate"),
            Term::Literal(LiteralTerm::new("object value")),
        )
    }

    #[test]
    fn test_statement_creation() {
        let stmt = example_statement();
        assert!(stmt.subject.is_iri());
        assert_eq!(stmt.predicate.as_str(), "http://example.org/predicate");
        assert!(stmt.object.is_literal());
        assert!(!stmt.is_quad());
    }

    #[test]
    fn test_quad_creation() {
        let stmt = Statement::new_quad(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Iri(IriTerm::new("http://example.org/o")),
            IriTerm::new("http://example.org/graph"),
        );
        assert!(stmt.is_quad());
    }

    #[test]
    fn test_statement_display() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("hello")),
        );
        let display = stmt.to_string();
        assert!(display.contains("<http://example.org/s>"));
        assert!(display.contains("<http://example.org/p>"));
        assert!(display.contains("\"hello\""));
        assert!(display.ends_with(" ."));
    }

    #[test]
    fn test_statement_equality() {
        let s1 = example_statement();
        let s2 = example_statement();
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_statement_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(example_statement());
        set.insert(example_statement());
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_roundtrip_statement() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Iri(IriTerm::new("http://example.org/o")),
        );
        let quad: oxrdf::Quad = (&stmt).try_into().unwrap();
        let back = Statement::from(quad);
        assert_eq!(stmt, back);
    }

    #[test]
    fn test_roundtrip_quad_statement() {
        let stmt = Statement::new_quad(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::BlankNode(BlankNodeTerm::new("b0")),
            IriTerm::new("http://example.org/g"),
        );
        let quad: oxrdf::Quad = (&stmt).try_into().unwrap();
        let back = Statement::from(quad);
        assert_eq!(stmt, back);
    }

    #[test]
    fn test_literal_subject_rejected() {
        let stmt = Statement::new(
            Term::Literal(LiteralTerm::new("not a valid subject")),
            IriTerm::new("http://example.org/p"),
            Term::Iri(IriTerm::new("http://example.org/o")),
        );
        let result: Result<oxrdf::Quad, _> = (&stmt).try_into();
        assert!(result.is_err());
    }
}
