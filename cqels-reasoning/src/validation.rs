//! Schema validation for reasoning profiles.
//!
//! Detects constructs in a schema (set of [`Statement`]s) that are unsupported
//! by a given [`ReasoningProfile`]. For example, OWL 2 EL does not support
//! `owl:inverseOf`, and OWL 2 RL does not support cardinality restrictions.

use cqels_model::{Statement, Term};

use crate::profile::ReasoningProfile;

const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Severity of a validation issue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ValidationSeverity {
    /// The construct may produce incomplete or unexpected inference results.
    Warning,
    /// The construct is definitively unsupported and will be silently ignored.
    Error,
}

/// A single validation issue found in a schema.
#[derive(Clone, Debug)]
pub struct ValidationIssue {
    /// Severity of this issue.
    pub severity: ValidationSeverity,
    /// The profile that flagged this issue.
    pub profile: ReasoningProfile,
    /// Machine-readable error code (e.g., `"EL_UNSUPPORTED_PREDICATE"`).
    pub code: String,
    /// Human-readable description.
    pub message: String,
    /// The offending statement, if applicable.
    pub statement: Option<Statement>,
}

/// Validates a schema (set of statements) against a reasoning profile.
///
/// Returns a list of [`ValidationIssue`]s for constructs that the profile
/// does not support. An empty list means the schema is fully compatible.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::{ReasoningProfile, validate_schema};
/// use cqels_model::{Statement, Term, IriTerm};
///
/// let schema = vec![
///     Statement::new(
///         Term::Iri(IriTerm::new("http://example.org/prop")),
///         IriTerm::new("http://www.w3.org/2002/07/owl#inverseOf"),
///         Term::Iri(IriTerm::new("http://example.org/invProp")),
///     ),
/// ];
///
/// let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
/// assert_eq!(issues.len(), 1);
/// assert_eq!(issues[0].code, "EL_UNSUPPORTED_PREDICATE");
/// ```
pub fn validate_schema(profile: ReasoningProfile, schema: &[Statement]) -> Vec<ValidationIssue> {
    match profile {
        ReasoningProfile::Owl2El => validate_owl2_el(schema),
        ReasoningProfile::Owl2Rl => validate_owl2_rl(schema),
        _ => vec![],
    }
}

// ── OWL 2 EL validation ─────────────────────────────────────────────

const EL_UNSUPPORTED_PREDICATES: &[&str] = &[
    "inverseOf",
    "propertyChainAxiom",
    "maxCardinality",
    "minCardinality",
    "cardinality",
    "maxQualifiedCardinality",
    "minQualifiedCardinality",
    "qualifiedCardinality",
    "oneOf",
    "unionOf",
    "complementOf",
    "hasValue",
    "hasKey",
];

const EL_UNSUPPORTED_TYPES: &[&str] = &[
    "SymmetricProperty",
    "TransitiveProperty",
    "FunctionalProperty",
    "InverseFunctionalProperty",
    "AsymmetricProperty",
    "IrreflexiveProperty",
];

fn validate_owl2_el(schema: &[Statement]) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let profile = ReasoningProfile::Owl2El;

    for stmt in schema {
        let pred = stmt.predicate.as_str();

        // Check unsupported predicates
        for &local in EL_UNSUPPORTED_PREDICATES {
            let full = format!("{OWL}{local}");
            if pred == full {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Warning,
                    profile,
                    code: "EL_UNSUPPORTED_PREDICATE".to_string(),
                    message: format!("OWL 2 EL does not support owl:{local} (found in predicate)"),
                    statement: Some(stmt.clone()),
                });
            }
        }

        // Check unsupported rdf:type values
        if pred == RDF_TYPE {
            if let Term::Iri(obj_iri) = &stmt.object {
                for &local in EL_UNSUPPORTED_TYPES {
                    let full = format!("{OWL}{local}");
                    if obj_iri.as_str() == full {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            profile,
                            code: "EL_UNSUPPORTED_TYPE".to_string(),
                            message: format!("OWL 2 EL does not support owl:{local} as a type"),
                            statement: Some(stmt.clone()),
                        });
                    }
                }
            }
        }
    }
    issues
}

// ── OWL 2 RL validation ─────────────────────────────────────────────

const RL_UNSUPPORTED_PREDICATES: &[&str] = &[
    "propertyChainAxiom",
    "maxCardinality",
    "minCardinality",
    "cardinality",
    "maxQualifiedCardinality",
    "minQualifiedCardinality",
    "qualifiedCardinality",
    "oneOf",
    "hasKey",
];

const RL_UNSUPPORTED_TYPES: &[&str] = &[
    "FunctionalProperty",
    "InverseFunctionalProperty",
    "AsymmetricProperty",
    "IrreflexiveProperty",
];

fn validate_owl2_rl(schema: &[Statement]) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let profile = ReasoningProfile::Owl2Rl;

    for stmt in schema {
        let pred = stmt.predicate.as_str();

        // Check unsupported predicates
        for &local in RL_UNSUPPORTED_PREDICATES {
            let full = format!("{OWL}{local}");
            if pred == full {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Warning,
                    profile,
                    code: "RL_UNSUPPORTED_PREDICATE".to_string(),
                    message: format!("OWL 2 RL does not support owl:{local} (found in predicate)"),
                    statement: Some(stmt.clone()),
                });
            }
        }

        // Check unsupported rdf:type values
        if pred == RDF_TYPE {
            if let Term::Iri(obj_iri) = &stmt.object {
                for &local in RL_UNSUPPORTED_TYPES {
                    let full = format!("{OWL}{local}");
                    if obj_iri.as_str() == full {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            profile,
                            code: "RL_UNSUPPORTED_TYPE".to_string(),
                            message: format!("OWL 2 RL does not support owl:{local} as a type"),
                            statement: Some(stmt.clone()),
                        });
                    }
                }
            }
        }
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::IriTerm;

    fn owl_stmt(subject: &str, predicate_local: &str, object: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(format!("{OWL}{predicate_local}")),
            Term::Iri(IriTerm::new(object)),
        )
    }

    fn type_stmt(subject: &str, type_local: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(RDF_TYPE),
            Term::Iri(IriTerm::new(format!("{OWL}{type_local}"))),
        )
    }

    // ── OWL 2 EL tests ──

    #[test]
    fn test_el_unsupported_inverse_of() {
        let schema = vec![owl_stmt("http://ex/p", "inverseOf", "http://ex/q")];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "EL_UNSUPPORTED_PREDICATE");
    }

    #[test]
    fn test_el_unsupported_cardinality() {
        let schema = vec![owl_stmt("http://ex/r", "maxCardinality", "http://ex/1")];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "EL_UNSUPPORTED_PREDICATE");
    }

    #[test]
    fn test_el_unsupported_type() {
        let schema = vec![type_stmt("http://ex/p", "SymmetricProperty")];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "EL_UNSUPPORTED_TYPE");
    }

    #[test]
    fn test_el_valid_schema() {
        let schema = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex/A")),
            IriTerm::new(format!("{OWL}equivalentClass")),
            Term::Iri(IriTerm::new("http://ex/B")),
        )];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert!(issues.is_empty());
    }

    #[test]
    fn test_el_multiple_issues() {
        let schema = vec![
            owl_stmt("http://ex/p", "inverseOf", "http://ex/q"),
            owl_stmt("http://ex/r", "unionOf", "http://ex/list"),
            type_stmt("http://ex/p", "TransitiveProperty"),
        ];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert_eq!(issues.len(), 3);
    }

    // ── OWL 2 RL tests ──

    #[test]
    fn test_rl_unsupported_chain() {
        let schema = vec![owl_stmt(
            "http://ex/p",
            "propertyChainAxiom",
            "http://ex/chain",
        )];
        let issues = validate_schema(ReasoningProfile::Owl2Rl, &schema);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "RL_UNSUPPORTED_PREDICATE");
    }

    #[test]
    fn test_rl_unsupported_type() {
        let schema = vec![type_stmt("http://ex/p", "FunctionalProperty")];
        let issues = validate_schema(ReasoningProfile::Owl2Rl, &schema);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "RL_UNSUPPORTED_TYPE");
    }

    #[test]
    fn test_rl_supports_inverse() {
        // OWL 2 RL DOES support owl:inverseOf (unlike OWL 2 EL)
        let schema = vec![owl_stmt("http://ex/p", "inverseOf", "http://ex/q")];
        let issues = validate_schema(ReasoningProfile::Owl2Rl, &schema);
        assert!(issues.is_empty());
    }

    // ── Other profiles ──

    #[test]
    fn test_rdfs_no_validation() {
        let schema = vec![owl_stmt("http://ex/p", "inverseOf", "http://ex/q")];
        let issues = validate_schema(ReasoningProfile::Rdfs, &schema);
        assert!(issues.is_empty());
    }

    #[test]
    fn test_none_no_validation() {
        let schema = vec![owl_stmt("http://ex/p", "inverseOf", "http://ex/q")];
        let issues = validate_schema(ReasoningProfile::None, &schema);
        assert!(issues.is_empty());
    }

    #[test]
    fn test_validation_issue_has_statement() {
        let stmt = owl_stmt("http://ex/p", "inverseOf", "http://ex/q");
        let issues = validate_schema(ReasoningProfile::Owl2El, std::slice::from_ref(&stmt));
        assert!(issues[0].statement.is_some());
    }

    #[test]
    fn test_validation_severity_is_warning() {
        let schema = vec![owl_stmt("http://ex/p", "inverseOf", "http://ex/q")];
        let issues = validate_schema(ReasoningProfile::Owl2El, &schema);
        assert_eq!(issues[0].severity, ValidationSeverity::Warning);
    }
}
