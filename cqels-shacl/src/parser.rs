//! SHACL shape parser that constructs shape models from RDF statements.
//!
//! [`ShaclShapeParser`] takes a slice of [`cqels_model::Statement`] values
//! (already parsed from Turtle, N-Triples, etc.) and builds a
//! [`ShaclShapeGraph`].

use std::collections::HashMap;

use cqels_model::{Statement, Term};

use crate::error::ShaclError;
use crate::model::{NodeKind, NodeShape, PropertyShape, ShaclShapeGraph};

/// The SHACL namespace IRI prefix.
const SH: &str = "http://www.w3.org/ns/shacl#";

/// The RDF namespace IRI prefix.
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

/// Constructs a full SHACL IRI from a local name.
fn sh(local: &str) -> String {
    format!("{SH}{local}")
}

/// Returns the string representation of a [`Term`].
fn term_str(term: &Term) -> String {
    match term {
        Term::Iri(iri) => iri.as_str().to_string(),
        Term::BlankNode(bn) => format!("_:{}", bn.id()),
        Term::Literal(lit) => lit.value().to_string(),
        _ => format!("{term:?}"),
    }
}

/// Parser that constructs a [`ShaclShapeGraph`] from RDF statements.
///
/// This parser recognises SHACL vocabulary predicates such as `sh:NodeShape`,
/// `sh:property`, `sh:path`, `sh:minCount`, and so on.
pub struct ShaclShapeParser;

impl ShaclShapeParser {
    /// Parses a set of RDF statements and returns a [`ShaclShapeGraph`].
    ///
    /// # Errors
    ///
    /// Returns [`ShaclError::ParseError`] if required shape structure is
    /// missing or malformed (e.g. a `sh:minCount` value that is not a valid
    /// integer).
    pub fn parse(statements: &[Statement]) -> Result<ShaclShapeGraph, ShaclError> {
        let rdf_type = format!("{RDF}type");
        let sh_node_shape = sh("NodeShape");
        let sh_property = sh("property");
        let sh_path = sh("path");
        let sh_min_count = sh("minCount");
        let sh_max_count = sh("maxCount");
        let sh_datatype = sh("datatype");
        let sh_class = sh("class");
        let sh_node_kind = sh("nodeKind");
        let sh_in = sh("in");
        let sh_has_value = sh("hasValue");
        let sh_target_node = sh("targetNode");
        let sh_target_class = sh("targetClass");
        let sh_closed = sh("closed");
        let sh_ignored_properties = sh("ignoredProperties");

        // Collect node shape IDs: subjects with rdf:type sh:NodeShape.
        let mut node_shape_ids: Vec<String> = Vec::new();
        for stmt in statements {
            if stmt.predicate.as_str() == rdf_type && term_str(&stmt.object) == sh_node_shape {
                node_shape_ids.push(term_str(&stmt.subject));
            }
        }

        // Index statements by subject for efficient lookup.
        let mut by_subject: HashMap<String, Vec<&Statement>> = HashMap::new();
        for stmt in statements {
            by_subject
                .entry(term_str(&stmt.subject))
                .or_default()
                .push(stmt);
        }

        let mut graph = ShaclShapeGraph::new();

        for shape_id in &node_shape_ids {
            let stmts = by_subject.get(shape_id).cloned().unwrap_or_default();

            let mut target_nodes = Vec::new();
            let mut target_classes = Vec::new();
            let mut property_shape_ids = Vec::new();
            let mut closed = false;
            let mut ignored_properties = Vec::new();

            for stmt in &stmts {
                let pred = stmt.predicate.as_str();
                if pred == sh_target_node {
                    target_nodes.push(term_str(&stmt.object));
                } else if pred == sh_target_class {
                    target_classes.push(term_str(&stmt.object));
                } else if pred == sh_property {
                    property_shape_ids.push(term_str(&stmt.object));
                } else if pred == sh_closed {
                    closed = term_str(&stmt.object) == "true";
                } else if pred == sh_ignored_properties {
                    ignored_properties.push(term_str(&stmt.object));
                }
            }

            // Parse each property shape.
            let mut property_shapes = Vec::new();
            for ps_id in &property_shape_ids {
                let ps_stmts = by_subject.get(ps_id).cloned().unwrap_or_default();

                let mut path: Option<String> = None;
                let mut min_count: Option<u32> = None;
                let mut max_count: Option<u32> = None;
                let mut datatype: Option<String> = None;
                let mut class: Option<String> = None;
                let mut node_kind: Option<NodeKind> = None;
                let mut in_values: Vec<String> = Vec::new();
                let mut has_value: Option<String> = None;

                for stmt in &ps_stmts {
                    let pred = stmt.predicate.as_str();
                    let obj = term_str(&stmt.object);

                    if pred == sh_path {
                        path = Some(obj);
                    } else if pred == sh_min_count {
                        min_count =
                            Some(obj.parse::<u32>().map_err(|e| ShaclError::ParseError {
                                message: format!(
                                    "invalid sh:minCount value '{obj}' for {ps_id}: {e}"
                                ),
                            })?);
                    } else if pred == sh_max_count {
                        max_count =
                            Some(obj.parse::<u32>().map_err(|e| ShaclError::ParseError {
                                message: format!(
                                    "invalid sh:maxCount value '{obj}' for {ps_id}: {e}"
                                ),
                            })?);
                    } else if pred == sh_datatype {
                        datatype = Some(obj);
                    } else if pred == sh_class {
                        class = Some(obj);
                    } else if pred == sh_node_kind {
                        node_kind = Some(parse_node_kind(&obj)?);
                    } else if pred == sh_in {
                        in_values.push(obj);
                    } else if pred == sh_has_value {
                        has_value = Some(obj);
                    }
                }

                let path = path.ok_or_else(|| ShaclError::ParseError {
                    message: format!("property shape {ps_id} is missing sh:path"),
                })?;

                property_shapes.push(PropertyShape {
                    id: ps_id.clone(),
                    path,
                    min_count,
                    max_count,
                    datatype,
                    class,
                    node_kind,
                    in_values,
                    has_value,
                });
            }

            graph.add_shape(NodeShape {
                id: shape_id.clone(),
                target_nodes,
                target_classes,
                property_shapes,
                closed,
                ignored_properties,
            });
        }

        Ok(graph)
    }
}

/// Parses a `sh:nodeKind` IRI value into a [`NodeKind`].
fn parse_node_kind(value: &str) -> Result<NodeKind, ShaclError> {
    let sh_iri = sh("IRI");
    let sh_blank = sh("BlankNode");
    let sh_literal = sh("Literal");
    let sh_blank_or_iri = sh("BlankNodeOrIRI");
    let sh_blank_or_literal = sh("BlankNodeOrLiteral");
    let sh_iri_or_literal = sh("IRIOrLiteral");

    if value == sh_iri {
        Ok(NodeKind::Iri)
    } else if value == sh_blank {
        Ok(NodeKind::BlankNode)
    } else if value == sh_literal {
        Ok(NodeKind::Literal)
    } else if value == sh_blank_or_iri {
        Ok(NodeKind::BlankNodeOrIri)
    } else if value == sh_blank_or_literal {
        Ok(NodeKind::BlankNodeOrLiteral)
    } else if value == sh_iri_or_literal {
        Ok(NodeKind::IriOrLiteral)
    } else {
        Err(ShaclError::ParseError {
            message: format!("unknown sh:nodeKind value: {value}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::{IriTerm, LiteralTerm};

    /// Helper to build an IRI term.
    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    /// Helper to build a literal term.
    fn lit(s: &str) -> Term {
        Term::Literal(LiteralTerm::new(s))
    }

    /// Helper to build a statement.
    fn stmt(s: Term, p: &str, o: Term) -> Statement {
        Statement::new(s, IriTerm::new(p), o)
    }

    fn rdf_type() -> String {
        format!("{RDF}type")
    }

    #[test]
    fn test_parse_empty() {
        let graph = ShaclShapeParser::parse(&[]).unwrap();
        assert!(graph.node_shapes.is_empty());
    }

    #[test]
    fn test_parse_node_shape_basic() {
        let shape_iri = "http://example.org/Shape1";
        let ps_iri = "http://example.org/ps1";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(
                iri(shape_iri),
                &sh("targetNode"),
                iri("http://example.org/node1"),
            ),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/name")),
            stmt(iri(ps_iri), &sh("minCount"), lit("1")),
            stmt(iri(ps_iri), &sh("maxCount"), lit("5")),
        ];

        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        assert_eq!(graph.node_shapes.len(), 1);

        let shape = &graph.node_shapes[0];
        assert_eq!(shape.id, shape_iri);
        assert_eq!(shape.target_nodes, vec!["http://example.org/node1"]);
        assert_eq!(shape.property_shapes.len(), 1);

        let ps = &shape.property_shapes[0];
        assert_eq!(ps.path, "http://example.org/name");
        assert_eq!(ps.min_count, Some(1));
        assert_eq!(ps.max_count, Some(5));
    }

    #[test]
    fn test_parse_property_shape_with_datatype() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let xsd_int = "http://www.w3.org/2001/XMLSchema#integer";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/age")),
            stmt(iri(ps_iri), &sh("datatype"), iri(xsd_int)),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        let ps = &graph.node_shapes[0].property_shapes[0];
        assert_eq!(ps.datatype.as_deref(), Some(xsd_int));
    }

    #[test]
    fn test_parse_node_kind() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/p")),
            stmt(iri(ps_iri), &sh("nodeKind"), iri(&sh("IRI"))),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        let ps = &graph.node_shapes[0].property_shapes[0];
        assert_eq!(ps.node_kind, Some(NodeKind::Iri));
    }

    #[test]
    fn test_parse_closed_shape() {
        let shape_iri = "http://example.org/S";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("closed"), lit("true")),
            stmt(
                iri(shape_iri),
                &sh("ignoredProperties"),
                iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            ),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        let shape = &graph.node_shapes[0];
        assert!(shape.closed);
        assert_eq!(shape.ignored_properties.len(), 1);
    }

    #[test]
    fn test_parse_target_class() {
        let shape_iri = "http://example.org/S";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(
                iri(shape_iri),
                &sh("targetClass"),
                iri("http://example.org/Person"),
            ),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        assert_eq!(
            graph.node_shapes[0].target_classes,
            vec!["http://example.org/Person"]
        );
    }

    #[test]
    fn test_parse_missing_path_error() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            // ps has no sh:path -- should error
            stmt(iri(ps_iri), &sh("minCount"), lit("1")),
        ];
        let result = ShaclShapeParser::parse(&stmts);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("missing sh:path"));
    }

    #[test]
    fn test_parse_invalid_min_count() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/p")),
            stmt(iri(ps_iri), &sh("minCount"), lit("not_a_number")),
        ];
        let result = ShaclShapeParser::parse(&stmts);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_in_values() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/color")),
            stmt(iri(ps_iri), &sh("in"), lit("red")),
            stmt(iri(ps_iri), &sh("in"), lit("green")),
            stmt(iri(ps_iri), &sh("in"), lit("blue")),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        let ps = &graph.node_shapes[0].property_shapes[0];
        assert_eq!(ps.in_values.len(), 3);
        assert!(ps.in_values.contains(&"red".to_string()));
        assert!(ps.in_values.contains(&"green".to_string()));
        assert!(ps.in_values.contains(&"blue".to_string()));
    }

    #[test]
    fn test_parse_has_value() {
        let shape_iri = "http://example.org/S";
        let ps_iri = "http://example.org/ps";
        let stmts = vec![
            stmt(iri(shape_iri), &rdf_type(), iri(&sh("NodeShape"))),
            stmt(iri(shape_iri), &sh("property"), iri(ps_iri)),
            stmt(iri(ps_iri), &sh("path"), iri("http://example.org/status")),
            stmt(iri(ps_iri), &sh("hasValue"), lit("active")),
        ];
        let graph = ShaclShapeParser::parse(&stmts).unwrap();
        let ps = &graph.node_shapes[0].property_shapes[0];
        assert_eq!(ps.has_value.as_deref(), Some("active"));
    }
}
