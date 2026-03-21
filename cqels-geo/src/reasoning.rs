//! Spatial reasoning for GeoSPARQL.
//!
//! Provides the [`GeoReasoner`] trait and a baseline implementation
//! ([`GeoSparqlSimpleReasoner`]) that infers symmetric and inverse
//! topological relations from asserted spatial facts.

use cqels_model::statement::Statement;
use cqels_model::term::IriTerm;
use cqels_model::Term;

use crate::vocabulary;

/// Trait for spatial reasoning modules.
///
/// Implementations derive additional spatial statements from asserted facts.
pub trait GeoReasoner: Send + Sync {
    /// Infers new statements from the given asserted facts.
    ///
    /// Returns a (possibly empty) vector of newly inferred statements.
    fn infer(&self, asserted_facts: &[Statement]) -> Vec<Statement>;
}

// ─── Rule definitions ────────────────────────────────────────────────────────

/// A single inference rule: if predicate matches `from`, produce a new
/// statement with predicate `to` and subject/object swapped.
struct InferenceRule {
    from: &'static str,
    to: &'static str,
    swap: bool,
}

const RULES: &[InferenceRule] = &[
    // sfWithin(x, y) => sfContains(y, x)
    InferenceRule {
        from: vocabulary::GEO_SF_WITHIN,
        to: vocabulary::GEO_SF_CONTAINS,
        swap: true,
    },
    // sfContains(x, y) => sfWithin(y, x)
    InferenceRule {
        from: vocabulary::GEO_SF_CONTAINS,
        to: vocabulary::GEO_SF_WITHIN,
        swap: true,
    },
    // sfEquals(x, y) => sfEquals(y, x)
    InferenceRule {
        from: vocabulary::GEO_SF_EQUALS,
        to: vocabulary::GEO_SF_EQUALS,
        swap: true,
    },
    // sfIntersects(x, y) => sfIntersects(y, x)
    InferenceRule {
        from: vocabulary::GEO_SF_INTERSECTS,
        to: vocabulary::GEO_SF_INTERSECTS,
        swap: true,
    },
    // sfDisjoint(x, y) => sfDisjoint(y, x)
    InferenceRule {
        from: vocabulary::GEO_SF_DISJOINT,
        to: vocabulary::GEO_SF_DISJOINT,
        swap: true,
    },
];

// ─── GeoSparqlSimpleReasoner ─────────────────────────────────────────────────

/// Minimal GeoSPARQL reasoner implementing symmetry and inverse rules.
///
/// For each asserted spatial statement, the reasoner produces the
/// corresponding symmetric or inverse statement:
///
/// | Asserted | Inferred |
/// |----------|----------|
/// | `sfWithin(x, y)` | `sfContains(y, x)` |
/// | `sfContains(x, y)` | `sfWithin(y, x)` |
/// | `sfEquals(x, y)` | `sfEquals(y, x)` |
/// | `sfIntersects(x, y)` | `sfIntersects(y, x)` |
/// | `sfDisjoint(x, y)` | `sfDisjoint(y, x)` |
///
/// # Examples
///
/// ```
/// use cqels_geo::{GeoSparqlSimpleReasoner, GeoReasoner};
/// use cqels_geo::vocabulary;
/// use cqels_model::{Statement, Term, IriTerm};
///
/// let reasoner = GeoSparqlSimpleReasoner;
/// let facts = vec![Statement::new(
///     Term::Iri(IriTerm::new("http://example.org/park")),
///     IriTerm::new(vocabulary::GEO_SF_WITHIN),
///     Term::Iri(IriTerm::new("http://example.org/city")),
/// )];
///
/// let inferred = reasoner.infer(&facts);
/// assert_eq!(inferred.len(), 1);
/// // sfWithin(park, city) => sfContains(city, park)
/// assert_eq!(inferred[0].predicate.as_str(), vocabulary::GEO_SF_CONTAINS);
/// assert_eq!(inferred[0].subject, Term::Iri(IriTerm::new("http://example.org/city")));
/// ```
pub struct GeoSparqlSimpleReasoner;

impl GeoReasoner for GeoSparqlSimpleReasoner {
    fn infer(&self, asserted_facts: &[Statement]) -> Vec<Statement> {
        let mut inferred = Vec::new();

        for stmt in asserted_facts {
            // Only process statements where object is a resource (IRI or blank node),
            // not a literal -- since a literal cannot be a subject.
            let object_is_resource = matches!(&stmt.object, Term::Iri(_) | Term::BlankNode(_));
            if !object_is_resource {
                continue;
            }

            let pred_str = stmt.predicate.as_str();

            for rule in RULES {
                if pred_str == rule.from {
                    let (new_subj, new_obj) = if rule.swap {
                        (stmt.object.clone(), stmt.subject.clone())
                    } else {
                        (stmt.subject.clone(), stmt.object.clone())
                    };

                    inferred.push(Statement::new(new_subj, IriTerm::new(rule.to), new_obj));
                }
            }
        }

        inferred
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    #[test]
    fn test_within_infers_contains() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_WITHIN),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].predicate.as_str(), vocabulary::GEO_SF_CONTAINS);
        // Subject and object should be swapped
        assert_eq!(inferred[0].subject, iri("http://example.org/b"));
        assert_eq!(inferred[0].object, iri("http://example.org/a"));
    }

    #[test]
    fn test_contains_infers_within() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_CONTAINS),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].predicate.as_str(), vocabulary::GEO_SF_WITHIN);
        assert_eq!(inferred[0].subject, iri("http://example.org/b"));
        assert_eq!(inferred[0].object, iri("http://example.org/a"));
    }

    #[test]
    fn test_equals_infers_symmetry() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_EQUALS),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].predicate.as_str(), vocabulary::GEO_SF_EQUALS);
        assert_eq!(inferred[0].subject, iri("http://example.org/b"));
        assert_eq!(inferred[0].object, iri("http://example.org/a"));
    }

    #[test]
    fn test_intersects_infers_symmetry() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_INTERSECTS),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert_eq!(inferred.len(), 1);
        assert_eq!(
            inferred[0].predicate.as_str(),
            vocabulary::GEO_SF_INTERSECTS
        );
    }

    #[test]
    fn test_disjoint_infers_symmetry() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_DISJOINT),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].predicate.as_str(), vocabulary::GEO_SF_DISJOINT);
    }

    #[test]
    fn test_literal_object_skipped() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new(vocabulary::GEO_SF_WITHIN),
            Term::Literal(cqels_model::term::LiteralTerm::new("POINT(1 2)")),
        )];

        let inferred = reasoner.infer(&facts);
        assert!(inferred.is_empty());
    }

    #[test]
    fn test_non_spatial_predicate_skipped() {
        let reasoner = GeoSparqlSimpleReasoner;
        let facts = vec![Statement::new(
            iri("http://example.org/a"),
            IriTerm::new("http://example.org/somePredicate"),
            iri("http://example.org/b"),
        )];

        let inferred = reasoner.infer(&facts);
        assert!(inferred.is_empty());
    }

    #[test]
    fn test_empty_input() {
        let reasoner = GeoSparqlSimpleReasoner;
        assert!(reasoner.infer(&[]).is_empty());
    }
}
