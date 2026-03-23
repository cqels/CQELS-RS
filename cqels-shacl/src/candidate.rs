//! Repair candidate generation for SHACL violations.
//!
//! Given a shapes graph and data snapshot, [`ShaclCandidateGenerator`]
//! produces a [`ShaclCandidateSet`] of potential add/delete edits that
//! the ASP solver can choose from during repair search.

use cqels_model::{IriTerm, LiteralTerm, Statement, Term};

use crate::model::{NodeShape, ShaclShapeGraph};

/// A set of candidate add/delete operations for repair search.
#[derive(Clone, Debug, Default)]
pub struct ShaclCandidateSet {
    /// Statements that could be added to repair violations.
    pub add_candidates: Vec<Statement>,
    /// Statements that could be deleted to repair violations.
    pub delete_candidates: Vec<Statement>,
}

/// Generates repair candidates based on SHACL shapes and a data snapshot.
///
/// For each shape constraint, this generator identifies:
/// - **Delete candidates**: existing triples that may cause violations
///   (e.g. exceeding `sh:maxCount`).
/// - **Add candidates**: triples that could be added to satisfy missing
///   constraints (e.g. below `sh:minCount`).
pub struct ShaclCandidateGenerator;

impl ShaclCandidateGenerator {
    /// Generates a candidate set of potential repair edits.
    ///
    /// # Arguments
    ///
    /// * `shapes` -- The SHACL shapes graph defining constraints.
    /// * `data` -- The current data snapshot as RDF statements.
    pub fn generate(shapes: &ShaclShapeGraph, data: &[Statement]) -> ShaclCandidateSet {
        let mut candidates = ShaclCandidateSet::default();

        for node_shape in &shapes.node_shapes {
            let focus_nodes = collect_focus_nodes(node_shape, data);

            for focus in &focus_nodes {
                for ps in &node_shape.property_shapes {
                    // Collect existing triples matching this focus node and property.
                    let matching: Vec<&Statement> = data
                        .iter()
                        .filter(|s| {
                            term_matches_str(&s.subject, focus) && s.predicate.as_str() == ps.path
                        })
                        .collect();

                    // Delete candidates: any matching triple could be deleted
                    // (useful for maxCount violations or closed shape repairs).
                    for stmt in &matching {
                        candidates.delete_candidates.push((*stmt).clone());
                    }

                    // Add candidates: if min_count requires more triples, generate
                    // a placeholder candidate.
                    if let Some(min) = ps.min_count {
                        let count = matching.len() as u32;
                        if count < min {
                            // Generate a candidate triple with a placeholder value.
                            let subject = Term::Iri(IriTerm::new(focus.as_str()));
                            let predicate = IriTerm::new(&ps.path);

                            // Use hasValue if available, otherwise a placeholder.
                            let object = if let Some(hv) = &ps.has_value {
                                Term::Literal(LiteralTerm::new(hv))
                            } else {
                                Term::Literal(LiteralTerm::new(format!(
                                    "__placeholder_{}_{}",
                                    ps.id, count
                                )))
                            };

                            candidates
                                .add_candidates
                                .push(Statement::new(subject, predicate, object));
                        }
                    }
                }

                // Closed shape: any triple with an unallowed property is a
                // delete candidate.
                if node_shape.closed {
                    let allowed = node_shape.allowed_properties();
                    for stmt in data {
                        if term_matches_str(&stmt.subject, focus)
                            && !allowed.contains(&stmt.predicate.as_str().to_string())
                        {
                            candidates.delete_candidates.push(stmt.clone());
                        }
                    }
                }
            }
        }

        candidates
    }
}

/// Collects focus nodes for a shape from target declarations and data.
fn collect_focus_nodes(shape: &NodeShape, data: &[Statement]) -> Vec<String> {
    let mut nodes: Vec<String> = shape.target_nodes.clone();

    // For targetClass, find all instances of the class.
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    for tc in &shape.target_classes {
        for stmt in data {
            if stmt.predicate.as_str() == rdf_type && term_matches_str(&stmt.object, tc) {
                let subject_str = term_to_str(&stmt.subject);
                if !nodes.contains(&subject_str) {
                    nodes.push(subject_str);
                }
            }
        }
    }

    nodes
}

/// Checks whether a [`Term`] matches the given string representation.
fn term_matches_str(term: &Term, value: &str) -> bool {
    term_to_str(term) == value
}

/// Converts a [`Term`] to its string representation for comparison.
fn term_to_str(term: &Term) -> String {
    match term {
        Term::Iri(iri) => iri.as_str().to_string(),
        Term::BlankNode(bn) => format!("_:{}", bn.id()),
        Term::Literal(lit) => lit.value().to_string(),
        _ => format!("{term:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeShape, PropertyShape, ShaclShapeGraph};

    fn make_stmt(s: &str, p: &str, o_lit: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(s)),
            IriTerm::new(p),
            Term::Literal(LiteralTerm::new(o_lit)),
        )
    }

    fn make_stmt_iri_obj(s: &str, p: &str, o: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(s)),
            IriTerm::new(p),
            Term::Iri(IriTerm::new(o)),
        )
    }

    #[test]
    fn test_generate_delete_candidates() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec!["http://example.org/alice".into()],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/name".into(),
                min_count: None,
                max_count: Some(1),
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: false,
            ignored_properties: vec![],
        });

        let data = vec![
            make_stmt(
                "http://example.org/alice",
                "http://example.org/name",
                "Alice",
            ),
            make_stmt("http://example.org/alice", "http://example.org/name", "Ali"),
        ];

        let candidates = ShaclCandidateGenerator::generate(&g, &data);
        // Both triples are delete candidates since they match the constrained property.
        assert_eq!(candidates.delete_candidates.len(), 2);
    }

    #[test]
    fn test_generate_add_candidates_for_min_count() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec!["http://example.org/bob".into()],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/name".into(),
                min_count: Some(1),
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: false,
            ignored_properties: vec![],
        });

        // Bob has no name triple.
        let data = vec![];
        let candidates = ShaclCandidateGenerator::generate(&g, &data);
        assert_eq!(candidates.add_candidates.len(), 1);
    }

    #[test]
    fn test_generate_no_add_when_satisfied() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec!["http://example.org/bob".into()],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/name".into(),
                min_count: Some(1),
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: false,
            ignored_properties: vec![],
        });

        let data = vec![make_stmt(
            "http://example.org/bob",
            "http://example.org/name",
            "Bob",
        )];
        let candidates = ShaclCandidateGenerator::generate(&g, &data);
        assert!(candidates.add_candidates.is_empty());
    }

    #[test]
    fn test_generate_closed_shape_delete_candidates() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec!["http://example.org/alice".into()],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/name".into(),
                min_count: None,
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: true,
            ignored_properties: vec![],
        });

        let data = vec![
            make_stmt(
                "http://example.org/alice",
                "http://example.org/name",
                "Alice",
            ),
            make_stmt(
                "http://example.org/alice",
                "http://example.org/email",
                "alice@example.org",
            ),
        ];

        let candidates = ShaclCandidateGenerator::generate(&g, &data);
        // The email triple should appear as a delete candidate (not allowed).
        let del_preds: Vec<_> = candidates
            .delete_candidates
            .iter()
            .map(|s| s.predicate.as_str().to_string())
            .collect();
        assert!(del_preds.contains(&"http://example.org/email".to_string()));
    }

    #[test]
    fn test_generate_target_class_focus_nodes() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec![],
            target_classes: vec!["http://example.org/Person".into()],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/name".into(),
                min_count: Some(1),
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: false,
            ignored_properties: vec![],
        });

        let data = vec![make_stmt_iri_obj(
            "http://example.org/alice",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://example.org/Person",
        )];

        let candidates = ShaclCandidateGenerator::generate(&g, &data);
        // Alice is a Person but has no name, so we should get an add candidate.
        assert_eq!(candidates.add_candidates.len(), 1);
    }

    #[test]
    fn test_empty_shapes_and_data() {
        let g = ShaclShapeGraph::new();
        let candidates = ShaclCandidateGenerator::generate(&g, &[]);
        assert!(candidates.add_candidates.is_empty());
        assert!(candidates.delete_candidates.is_empty());
    }
}
