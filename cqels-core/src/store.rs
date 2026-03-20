//! RDF Store abstraction and oxigraph-based implementation.
//!
//! Provides a trait for querying persistent RDF data (static triples)
//! and a concrete implementation backed by oxigraph's in-memory store.

use std::collections::HashMap;
use std::sync::Arc;

use oxigraph::store::Store;
use oxrdf::{GraphNameRef, NamedNodeRef, QuadRef};

use cqels_model::{BindingSet, CqelsError, Statement};

use crate::compiler::pipeline::match_triple_pattern;
use crate::parser::ast::TriplePattern;

/// Trait for querying persistent RDF data.
///
/// Used by the compiled query pipeline to resolve `STATIC { ... }` and
/// `GRAPH <uri> { ... }` patterns against a pre-loaded RDF store.
pub trait RdfStore: Send + Sync {
    /// Queries the default graph for triples matching the given pattern.
    fn query_pattern(
        &self,
        pattern: &TriplePattern,
        prefixes: &HashMap<String, String>,
    ) -> Vec<BindingSet>;

    /// Queries a named graph for triples matching the given pattern.
    fn query_named_graph_pattern(
        &self,
        graph_uri: &str,
        pattern: &TriplePattern,
        prefixes: &HashMap<String, String>,
    ) -> Vec<BindingSet>;

    /// Loads statements into the default graph.
    fn load_statements(&self, statements: &[Statement]) -> Result<(), CqelsError>;

    /// Loads statements into a named graph.
    fn load_named_graph(&self, graph_uri: &str, statements: &[Statement])
        -> Result<(), CqelsError>;
}

/// An RDF store backed by oxigraph's in-memory store.
pub struct OxigraphRdfStore {
    store: Store,
}

impl OxigraphRdfStore {
    /// Creates a new empty in-memory store.
    pub fn new() -> Result<Self, CqelsError> {
        let store = Store::new().map_err(|e| CqelsError::Evaluation {
            message: format!("oxigraph store creation failed: {e}"),
        })?;
        Ok(Self { store })
    }
}

impl Default for OxigraphRdfStore {
    fn default() -> Self {
        Self::new().expect("failed to create in-memory oxigraph store")
    }
}

impl RdfStore for OxigraphRdfStore {
    fn query_pattern(
        &self,
        pattern: &TriplePattern,
        prefixes: &HashMap<String, String>,
    ) -> Vec<BindingSet> {
        query_store_pattern(&self.store, None, pattern, prefixes)
    }

    fn query_named_graph_pattern(
        &self,
        graph_uri: &str,
        pattern: &TriplePattern,
        prefixes: &HashMap<String, String>,
    ) -> Vec<BindingSet> {
        query_store_pattern(&self.store, Some(graph_uri), pattern, prefixes)
    }

    fn load_statements(&self, statements: &[Statement]) -> Result<(), CqelsError> {
        for stmt in statements {
            let quad: oxrdf::Quad = stmt.try_into()?;
            self.store
                .insert(QuadRef::from(&quad))
                .map_err(|e| CqelsError::Evaluation {
                    message: format!("store insert failed: {e}"),
                })?;
        }
        Ok(())
    }

    fn load_named_graph(
        &self,
        graph_uri: &str,
        statements: &[Statement],
    ) -> Result<(), CqelsError> {
        let graph_node = oxrdf::NamedNode::new(graph_uri).map_err(|e| CqelsError::Evaluation {
            message: format!("invalid graph URI: {e}"),
        })?;
        for stmt in statements {
            let quad: oxrdf::Quad = stmt.try_into()?;
            // Override the graph name with the specified named graph
            let new_quad = oxrdf::Quad::new(
                quad.subject,
                quad.predicate,
                quad.object,
                graph_node.clone(),
            );
            self.store
                .insert(QuadRef::from(&new_quad))
                .map_err(|e| CqelsError::Evaluation {
                    message: format!("store insert failed: {e}"),
                })?;
        }
        Ok(())
    }
}

/// Resolves a pattern term to an optional oxrdf NamedNode for filtering.
/// Variables (starting with `?` or `$`) → None (wildcard).
/// Constants (IRIs in `<...>` or prefixed names) → Some(NamedNode).
fn resolve_pattern_term(
    term: &str,
    prefixes: &HashMap<String, String>,
) -> Option<oxrdf::NamedNode> {
    if term.starts_with('?') || term.starts_with('$') {
        return None; // variable → wildcard
    }
    // Strip angle brackets for IRIs
    let iri = if term.starts_with('<') && term.ends_with('>') {
        &term[1..term.len() - 1]
    } else if let Some(colon_pos) = term.find(':') {
        // Prefixed name → expand
        let prefix = &term[..colon_pos];
        let local = &term[colon_pos + 1..];
        if let Some(base) = prefixes.get(prefix) {
            // We need to return an owned string, so we'll construct the node directly
            let expanded = format!("{base}{local}");
            return oxrdf::NamedNode::new(&expanded).ok();
        } else {
            return None;
        }
    } else {
        term
    };
    oxrdf::NamedNode::new(iri).ok()
}

/// Queries the oxigraph store for quads matching a pattern, converting
/// each match into a `BindingSet` using `match_triple_pattern`.
fn query_store_pattern(
    store: &Store,
    graph_uri: Option<&str>,
    pattern: &TriplePattern,
    prefixes: &HashMap<String, String>,
) -> Vec<BindingSet> {
    let subject = resolve_pattern_term(&pattern.subject, prefixes);
    let predicate = resolve_pattern_term(&pattern.predicate, prefixes);
    let object = resolve_pattern_term(&pattern.object, prefixes);

    let subj_ref = subject.as_ref().map(|n| n.as_ref().into());
    let pred_ref = predicate.as_ref().map(NamedNodeRef::from);
    let obj_ref = object.as_ref().map(|n| oxrdf::TermRef::from(n.as_ref()));

    let graph_name = graph_uri
        .and_then(|uri| oxrdf::NamedNode::new(uri).ok())
        .map(oxrdf::GraphName::NamedNode);
    let graph_ref: Option<GraphNameRef<'_>> = match &graph_name {
        Some(g) => Some(g.as_ref()),
        None => Some(GraphNameRef::DefaultGraph),
    };

    let mut results = Vec::new();

    for quad in store
        .quads_for_pattern(subj_ref, pred_ref, obj_ref, graph_ref)
        .flatten()
    {
        let stmt = Statement::from(quad);
        if let Some(bs) = match_triple_pattern(pattern, &stmt, prefixes, 0) {
            results.push(bs);
        }
    }

    results
}

/// Creates a new `OxigraphRdfStore` wrapped in an `Arc` for use with the compiler.
pub fn create_rdf_store() -> Result<Arc<dyn RdfStore>, CqelsError> {
    Ok(Arc::new(OxigraphRdfStore::new()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::Term;

    fn make_stmt(subj: &str, pred: &str, obj_iri: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(subj)),
            IriTerm::new(pred),
            Term::Iri(IriTerm::new(obj_iri)),
        )
    }

    fn make_literal_stmt(subj: &str, pred: &str, obj_val: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(subj)),
            IriTerm::new(pred),
            Term::Literal(LiteralTerm::new(obj_val)),
        )
    }

    #[test]
    fn test_load_and_query_roundtrip() {
        let store = OxigraphRdfStore::new().unwrap();
        let stmts = vec![
            make_stmt(
                "http://example.org/sensor1",
                "http://example.org/type",
                "http://example.org/TemperatureSensor",
            ),
            make_stmt(
                "http://example.org/sensor2",
                "http://example.org/type",
                "http://example.org/HumiditySensor",
            ),
        ];
        store.load_statements(&stmts).unwrap();

        // Query with all variables — should return both
        let pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "?p".to_string(),
            object: "?o".to_string(),
        };
        let results = store.query_pattern(&pattern, &HashMap::new());
        assert_eq!(results.len(), 2);

        // Query with constant predicate — should return both
        let pattern2 = TriplePattern {
            subject: "?s".to_string(),
            predicate: "<http://example.org/type>".to_string(),
            object: "?o".to_string(),
        };
        let results2 = store.query_pattern(&pattern2, &HashMap::new());
        assert_eq!(results2.len(), 2);

        // Query with constant subject — should return only one
        let pattern3 = TriplePattern {
            subject: "<http://example.org/sensor1>".to_string(),
            predicate: "<http://example.org/type>".to_string(),
            object: "?type".to_string(),
        };
        let results3 = store.query_pattern(&pattern3, &HashMap::new());
        assert_eq!(results3.len(), 1);
        assert!(results3[0].contains("type"));
    }

    #[test]
    fn test_constant_filtering() {
        let store = OxigraphRdfStore::new().unwrap();
        let stmts = vec![
            make_stmt(
                "http://example.org/a",
                "http://example.org/p",
                "http://example.org/x",
            ),
            make_stmt(
                "http://example.org/b",
                "http://example.org/q",
                "http://example.org/y",
            ),
        ];
        store.load_statements(&stmts).unwrap();

        // Filter by specific predicate
        let pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "<http://example.org/p>".to_string(),
            object: "?o".to_string(),
        };
        let results = store.query_pattern(&pattern, &HashMap::new());
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_named_graph_isolation() {
        let store = OxigraphRdfStore::new().unwrap();

        // Load into default graph
        let default_stmts = vec![make_stmt(
            "http://example.org/a",
            "http://example.org/p",
            "http://example.org/default_val",
        )];
        store.load_statements(&default_stmts).unwrap();

        // Load into named graph
        let named_stmts = vec![make_stmt(
            "http://example.org/b",
            "http://example.org/p",
            "http://example.org/named_val",
        )];
        store
            .load_named_graph("http://example.org/graph1", &named_stmts)
            .unwrap();

        let all_pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "?p".to_string(),
            object: "?o".to_string(),
        };

        // Default graph should only have 1
        let default_results = store.query_pattern(&all_pattern, &HashMap::new());
        assert_eq!(default_results.len(), 1);

        // Named graph should only have 1
        let named_results = store.query_named_graph_pattern(
            "http://example.org/graph1",
            &all_pattern,
            &HashMap::new(),
        );
        assert_eq!(named_results.len(), 1);

        // Different named graph should be empty
        let other_results = store.query_named_graph_pattern(
            "http://example.org/graph2",
            &all_pattern,
            &HashMap::new(),
        );
        assert_eq!(other_results.len(), 0);
    }

    #[test]
    fn test_empty_store() {
        let store = OxigraphRdfStore::new().unwrap();
        let pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "?p".to_string(),
            object: "?o".to_string(),
        };
        let results = store.query_pattern(&pattern, &HashMap::new());
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_resolution() {
        let store = OxigraphRdfStore::new().unwrap();
        let stmts = vec![make_stmt(
            "http://example.org/sensor1",
            "http://example.org/type",
            "http://example.org/TempSensor",
        )];
        store.load_statements(&stmts).unwrap();

        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());

        let pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "ex:type".to_string(),
            object: "?type".to_string(),
        };
        let results = store.query_pattern(&pattern, &prefixes);
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("type"));
    }

    #[test]
    fn test_literal_objects() {
        let store = OxigraphRdfStore::new().unwrap();
        let stmts = vec![make_literal_stmt(
            "http://example.org/sensor1",
            "http://example.org/label",
            "Temperature Sensor",
        )];
        store.load_statements(&stmts).unwrap();

        let pattern = TriplePattern {
            subject: "?s".to_string(),
            predicate: "<http://example.org/label>".to_string(),
            object: "?label".to_string(),
        };
        let results = store.query_pattern(&pattern, &HashMap::new());
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("label"));
    }
}
