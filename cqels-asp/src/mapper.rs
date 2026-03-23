//! RDF statement to ASP fact mapping.
//!
//! [`AspFactMapper`] converts RDF [`Statement`]s into ASP facts using the
//! `rdf(subject, predicate, object)` encoding convention, and
//! [`AspFactDelta`] represents incremental additions and deletions.

use cqels_model::Statement;

use crate::codec::AspTermCodec;

/// Converts RDF statements into ASP fact strings.
///
/// Each RDF triple `(s, p, o)` is encoded as an ASP fact:
/// `rdf(encoded_s, encoded_p, encoded_o).`
///
/// # Examples
///
/// ```
/// use cqels_model::{Statement, Term, IriTerm, LiteralTerm};
/// use cqels_asp::AspFactMapper;
///
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://example.org/alice")),
///     IriTerm::new("http://example.org/name"),
///     Term::Literal(LiteralTerm::new("Alice")),
/// );
/// let fact = AspFactMapper::statement_to_fact(&stmt);
/// assert!(fact.starts_with("rdf("));
/// assert!(fact.ends_with(")."));
/// ```
pub struct AspFactMapper;

impl AspFactMapper {
    /// Converts a single RDF statement into an ASP fact string.
    ///
    /// The fact has the form `rdf(subject, predicate, object).`
    pub fn statement_to_fact(stmt: &Statement) -> String {
        let s = AspTermCodec::encode_term(&stmt.subject);
        let p = AspTermCodec::encode_iri(stmt.predicate.as_str());
        let o = AspTermCodec::encode_term(&stmt.object);
        format!("rdf({s},{p},{o}).")
    }

    /// Converts a slice of RDF statements into an ASP program of facts,
    /// one fact per line.
    pub fn statements_to_program(stmts: &[Statement]) -> String {
        stmts
            .iter()
            .map(Self::statement_to_fact)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Represents an incremental change to the set of RDF statements
/// fed to an ASP solver.
pub struct AspFactDelta {
    /// Statements to add to the knowledge base.
    pub additions: Vec<Statement>,
    /// Statements to remove from the knowledge base.
    pub deletions: Vec<Statement>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::{IriTerm, LiteralTerm, Term};

    fn sample_statement() -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new("http://example.org/alice")),
            IriTerm::new("http://example.org/name"),
            Term::Literal(LiteralTerm::new("Alice")),
        )
    }

    #[test]
    fn test_statement_to_fact() {
        let fact = AspFactMapper::statement_to_fact(&sample_statement());
        assert!(fact.starts_with("rdf("));
        assert!(fact.ends_with(")."));
        assert!(fact.contains("iri(\"http://example.org/alice\")"));
        assert!(fact.contains("iri(\"http://example.org/name\")"));
    }

    #[test]
    fn test_statements_to_program() {
        let stmts = vec![sample_statement(), sample_statement()];
        let program = AspFactMapper::statements_to_program(&stmts);
        let lines: Vec<&str> = program.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            assert!(line.starts_with("rdf("));
            assert!(line.ends_with(")."));
        }
    }

    #[test]
    fn test_statements_to_program_empty() {
        let program = AspFactMapper::statements_to_program(&[]);
        assert!(program.is_empty());
    }

    #[test]
    fn test_fact_delta_structure() {
        let delta = AspFactDelta {
            additions: vec![sample_statement()],
            deletions: vec![],
        };
        assert_eq!(delta.additions.len(), 1);
        assert!(delta.deletions.is_empty());
    }
}
