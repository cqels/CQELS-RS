//! SHACL continuous query implementation.
//!
//! [`ShaclContinuousQuery`] implements the [`ContinuousQuery`] trait from
//! `cqels-core`, validating streaming RDF data against SHACL shapes via
//! the ASP-backed [`ShaclValidationEngine`].

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};

use cqels_asp::{AnswerSet, AspError, AspSolver};
use cqels_core::query::{ContinuousQuery, QueryInputs, QueryType};
use cqels_core::stream::{StreamElement, StreamRecord};

use crate::config::ShaclStreamSolveConfig;
use crate::engine::ShaclValidationEngine;
use crate::model::ShaclShapeGraph;

/// Newtype wrapper enabling `Arc<dyn AspSolver>` to be used as a generic
/// `S: AspSolver` parameter (required because `async_trait` doesn't generate
/// blanket impls for `Arc<dyn Trait>`).
struct SharedSolver(Arc<dyn AspSolver>);

#[async_trait]
impl AspSolver for SharedSolver {
    async fn solve(&self, program: &str, max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        self.0.solve(program, max_models).await
    }
}

/// A continuous query that validates streaming data against SHACL shapes.
///
/// For each RDF element, the validation engine checks the accumulated
/// statements against the shapes graph. Results (conformance or violations)
/// are yielded as [`StreamRecord`] elements.
pub struct ShaclContinuousQuery {
    id: String,
    shapes: ShaclShapeGraph,
    config: ShaclStreamSolveConfig,
    solver: Option<Arc<dyn AspSolver>>,
}

impl ShaclContinuousQuery {
    /// Creates a new continuous SHACL query (without a solver).
    ///
    /// Without a solver, `execute()` yields an empty stream. Use
    /// [`with_solver`](Self::with_solver) to supply an ASP backend.
    pub fn new(id: String, shapes: ShaclShapeGraph, config: ShaclStreamSolveConfig) -> Self {
        Self {
            id,
            shapes,
            config,
            solver: None,
        }
    }

    /// Creates a new continuous SHACL query with the given solver.
    pub fn with_solver(
        id: String,
        shapes: ShaclShapeGraph,
        config: ShaclStreamSolveConfig,
        solver: Arc<dyn AspSolver>,
    ) -> Self {
        Self {
            id,
            shapes,
            config,
            solver: Some(solver),
        }
    }

    /// Returns the query identifier.
    pub fn query_id(&self) -> &str {
        &self.id
    }

    /// Returns a reference to the shapes graph.
    pub fn shapes(&self) -> &ShaclShapeGraph {
        &self.shapes
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &ShaclStreamSolveConfig {
        &self.config
    }
}

#[async_trait]
impl ContinuousQuery for ShaclContinuousQuery {
    type Result = StreamElement;

    fn query_id(&self) -> &str {
        &self.id
    }

    fn query_string(&self) -> &str {
        "SHACL"
    }

    fn query_type(&self) -> QueryType {
        QueryType::Custom
    }

    fn execute(&self, mut inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Self::Result> + Send>> {
        let Some(solver) = self.solver.clone() else {
            return Box::pin(stream::empty());
        };

        // Take the first available input stream.
        let input = {
            let name = inputs.stream_names().next().map(|s| s.to_string());
            name.and_then(|n| inputs.take_stream(&n))
        };

        let Some(input) = input else {
            return Box::pin(stream::empty());
        };

        let shapes = self.shapes.clone();
        let config = self.config.clone();

        Box::pin(input.filter_map(move |elem| {
            let solver = solver.clone();
            let shapes = shapes.clone();
            let config = config.clone();
            async move {
                let stmt = elem.as_statement()?.clone();
                let ts = elem.timestamp();
                let engine = ShaclValidationEngine::new(config, SharedSolver(solver));
                let result = engine.validate(&shapes, &[stmt], ts, "stream").await.ok()?;
                let status = if result.conforms {
                    "conforms".to_string()
                } else {
                    let violations: Vec<String> = result
                        .violations
                        .iter()
                        .map(|v| {
                            format!(
                                "violation(shape={},focus={},constraint={})",
                                v.shape, v.focus, v.constraint
                            )
                        })
                        .collect();
                    violations.join("; ")
                };
                Some(StreamElement::Record(StreamRecord::new(status, ts)))
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_asp::Atom;
    use cqels_core::stream::RdfStreamElement;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::{Statement, Term};

    struct MockSolver {
        answer_sets: Vec<AnswerSet>,
    }

    #[async_trait]
    impl AspSolver for MockSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Ok(self.answer_sets.clone())
        }
    }

    fn make_rdf_element(s: &str, p: &str, o: &str, ts: i64) -> StreamElement {
        StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new(s)),
                IriTerm::new(p),
                Term::Literal(LiteralTerm::new(o)),
            ),
            ts,
        ))
    }

    #[test]
    fn test_continuous_query_creation() {
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::default();
        let query = ShaclContinuousQuery::new("q1".into(), shapes, config);
        assert_eq!(ShaclContinuousQuery::query_id(&query), "q1");
    }

    #[test]
    fn test_continuous_query_accessors() {
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::builder()
            .enable_repair_search(true)
            .build();
        let query = ShaclContinuousQuery::new("q2".into(), shapes, config);
        assert!(query.shapes().node_shapes.is_empty());
        assert!(query.config().enable_repair_search);
    }

    #[tokio::test]
    async fn test_execute_without_solver_yields_empty() {
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::default();
        let query = ShaclContinuousQuery::new("q".into(), shapes, config);
        let inputs = QueryInputs::new();
        let results: Vec<_> = query.execute(inputs).collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_conforming() {
        // Empty answer set = no violations = conforms
        let solver = Arc::new(MockSolver {
            answer_sets: vec![AnswerSet::new(vec![])],
        });
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::default();
        let query = ShaclContinuousQuery::with_solver("q".into(), shapes, config, solver);

        let mut inputs = QueryInputs::new();
        let elem = make_rdf_element("http://ex/s", "http://ex/p", "val", 1000);
        inputs.add_stream("data", Box::pin(futures::stream::iter(vec![elem])));

        let results: Vec<_> = query.execute(inputs).collect().await;
        assert_eq!(results.len(), 1);
        match &results[0] {
            StreamElement::Record(rec) => assert_eq!(rec.value, "conforms"),
            _ => panic!("expected Record"),
        }
    }

    #[tokio::test]
    async fn test_execute_with_violations() {
        let violation = Atom::new(
            "violation",
            vec![
                "\"S1\"".into(),
                "\"http://ex/s\"".into(),
                "\"minCount\"".into(),
                "\"http://ex/p\"".into(),
            ],
        );
        let solver = Arc::new(MockSolver {
            answer_sets: vec![AnswerSet::new(vec![violation])],
        });
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::default();
        let query = ShaclContinuousQuery::with_solver("q".into(), shapes, config, solver);

        let mut inputs = QueryInputs::new();
        let elem = make_rdf_element("http://ex/s", "http://ex/p", "val", 2000);
        inputs.add_stream("data", Box::pin(futures::stream::iter(vec![elem])));

        let results: Vec<_> = query.execute(inputs).collect().await;
        assert_eq!(results.len(), 1);
        match &results[0] {
            StreamElement::Record(rec) => assert!(rec.value.contains("violation")),
            _ => panic!("expected Record"),
        }
    }
}
