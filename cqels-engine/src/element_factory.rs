//! Static factory functions for creating [`StreamElement`] values.
//!
//! Convenience constructors for common RDF stream element patterns,
//! both with automatic and explicit timestamps.

use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::{Statement, Term};

/// Creates a stream element with a string literal object and automatic timestamp.
pub fn triple_literal(subject: &str, predicate: &str, literal: &str) -> StreamElement {
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(LiteralTerm::new(literal)),
    );
    StreamElement::Rdf(RdfStreamElement::now(stmt))
}

/// Creates a stream element with an IRI object and automatic timestamp.
pub fn triple_uri(subject: &str, predicate: &str, object: &str) -> StreamElement {
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Iri(IriTerm::new(object)),
    );
    StreamElement::Rdf(RdfStreamElement::now(stmt))
}

/// Creates a stream element with an `f64` literal and automatic timestamp.
pub fn triple_f64(subject: &str, predicate: &str, value: f64) -> StreamElement {
    let lit = LiteralTerm::new(value.to_string())
        .with_datatype("http://www.w3.org/2001/XMLSchema#double");
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(lit),
    );
    StreamElement::Rdf(RdfStreamElement::now(stmt))
}

/// Creates a stream element with an `i64` literal and automatic timestamp.
pub fn triple_i64(subject: &str, predicate: &str, value: i64) -> StreamElement {
    let lit = LiteralTerm::new(value.to_string())
        .with_datatype("http://www.w3.org/2001/XMLSchema#integer");
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(lit),
    );
    StreamElement::Rdf(RdfStreamElement::now(stmt))
}

/// Creates a stream element with a boolean literal and automatic timestamp.
pub fn triple_bool(subject: &str, predicate: &str, value: bool) -> StreamElement {
    let lit = LiteralTerm::new(value.to_string())
        .with_datatype("http://www.w3.org/2001/XMLSchema#boolean");
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(lit),
    );
    StreamElement::Rdf(RdfStreamElement::now(stmt))
}

/// Creates a stream element with a string literal object and explicit timestamp.
pub fn triple_literal_at(
    subject: &str,
    predicate: &str,
    literal: &str,
    timestamp: i64,
) -> StreamElement {
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(LiteralTerm::new(literal)),
    );
    StreamElement::Rdf(RdfStreamElement::new(stmt, timestamp))
}

/// Creates a stream element with an IRI object and explicit timestamp.
pub fn triple_uri_at(
    subject: &str,
    predicate: &str,
    object: &str,
    timestamp: i64,
) -> StreamElement {
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Iri(IriTerm::new(object)),
    );
    StreamElement::Rdf(RdfStreamElement::new(stmt, timestamp))
}

/// Creates a stream element with an `f64` literal and explicit timestamp.
pub fn triple_f64_at(subject: &str, predicate: &str, value: f64, timestamp: i64) -> StreamElement {
    let lit = LiteralTerm::new(value.to_string())
        .with_datatype("http://www.w3.org/2001/XMLSchema#double");
    let stmt = Statement::new(
        Term::Iri(IriTerm::new(subject)),
        IriTerm::new(predicate),
        Term::Literal(lit),
    );
    StreamElement::Rdf(RdfStreamElement::new(stmt, timestamp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_triple_literal() {
        let elem = triple_literal("http://s", "http://p", "hello");
        assert!(elem.is_rdf());
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => assert_eq!(lit.value(), "hello"),
            _ => panic!("expected literal"),
        }
    }

    #[test]
    fn test_triple_uri() {
        let elem = triple_uri("http://s", "http://p", "http://o");
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Iri(iri) => assert_eq!(iri.as_str(), "http://o"),
            _ => panic!("expected iri"),
        }
    }

    #[test]
    fn test_triple_f64() {
        let elem = triple_f64("http://s", "http://p", 3.125);
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => {
                assert_eq!(lit.value(), "3.125");
                assert_eq!(
                    lit.datatype(),
                    Some("http://www.w3.org/2001/XMLSchema#double")
                );
            }
            _ => panic!("expected literal"),
        }
    }

    #[test]
    fn test_triple_i64() {
        let elem = triple_i64("http://s", "http://p", 42);
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => assert_eq!(lit.value(), "42"),
            _ => panic!("expected literal"),
        }
    }

    #[test]
    fn test_triple_bool() {
        let elem = triple_bool("http://s", "http://p", false);
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => assert_eq!(lit.value(), "false"),
            _ => panic!("expected literal"),
        }
    }

    #[test]
    fn test_triple_literal_at() {
        let elem = triple_literal_at("http://s", "http://p", "val", 12345);
        assert_eq!(elem.timestamp(), 12345);
    }

    #[test]
    fn test_triple_uri_at() {
        let elem = triple_uri_at("http://s", "http://p", "http://o", 99);
        assert_eq!(elem.timestamp(), 99);
    }

    #[test]
    fn test_triple_f64_at() {
        let elem = triple_f64_at("http://s", "http://p", 1.5, 500);
        assert_eq!(elem.timestamp(), 500);
    }
}
