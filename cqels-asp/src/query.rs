//! ASP-based continuous query implementation.
//!
//! [`AspContinuousQuery`] implements the [`ContinuousQuery`] trait from
//! `cqels-core`, bridging ASP solving into the CQELS streaming pipeline.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::{Stream, StreamExt};

use cqels_core::query::{ContinuousQuery, QueryInputs, QueryType};
use cqels_core::stream::{StreamElement, StreamRecord};

use crate::config::AspStreamSolveConfig;
use crate::mapper::{AspFactDelta, AspFactMapper};
use crate::solver::{AnswerSet, AspSolver};

/// The result of an ASP solve invocation within a continuous query.
#[derive(Clone, Debug)]
pub struct AspSolveResult {
    /// The answer sets produced by the solver.
    pub answer_sets: Vec<AnswerSet>,
    /// Timestamp (milliseconds since epoch) of the solve invocation.
    pub timestamp: i64,
}

/// A continuous query backed by ASP solving.
///
/// For each RDF element received from the first input stream, the solver is
/// invoked with the accumulated facts, and each answer set atom is yielded as
/// a [`StreamRecord`].
pub struct AspContinuousQuery {
    id: String,
    config: AspStreamSolveConfig,
    solver: Option<Arc<dyn AspSolver>>,
    /// Name of the input stream to consume (defaults to the first available).
    stream_name: Option<String>,
}

impl AspContinuousQuery {
    /// Creates a new ASP continuous query with the given id and configuration.
    ///
    /// Without a solver, `execute()` yields an empty stream. Use
    /// [`with_solver`](Self::with_solver) to supply a solver backend.
    pub fn new(id: impl Into<String>, config: AspStreamSolveConfig) -> Self {
        Self {
            id: id.into(),
            config,
            solver: None,
            stream_name: None,
        }
    }

    /// Creates a new ASP continuous query with the given solver.
    pub fn with_solver(
        id: impl Into<String>,
        config: AspStreamSolveConfig,
        solver: Arc<dyn AspSolver>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            solver: Some(solver),
            stream_name: None,
        }
    }

    /// Sets the input stream name to consume.
    pub fn stream_name(mut self, name: impl Into<String>) -> Self {
        self.stream_name = Some(name.into());
        self
    }
}

#[async_trait]
impl ContinuousQuery for AspContinuousQuery {
    type Result = StreamElement;

    fn query_id(&self) -> &str {
        &self.id
    }

    fn query_string(&self) -> &str {
        &self.config.base_program
    }

    fn query_type(&self) -> QueryType {
        QueryType::Custom
    }

    fn execute(&self, mut inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Self::Result> + Send>> {
        let Some(solver) = self.solver.clone() else {
            return Box::pin(stream::empty());
        };

        // Take the named stream or the first available one.
        let input = if let Some(ref name) = self.stream_name {
            inputs.take_stream(name)
        } else {
            let name = inputs.stream_names().next().map(|s| s.to_string());
            name.and_then(|n| inputs.take_stream(&n))
        };

        let Some(input) = input else {
            return Box::pin(stream::empty());
        };

        let base_program = self.config.base_program.clone();

        Box::pin(
            input
                .filter_map(move |elem| {
                    let solver = solver.clone();
                    let base_program = base_program.clone();
                    async move {
                        let stmt = elem.as_statement()?.clone();
                        let ts = elem.timestamp();
                        let delta = AspFactDelta {
                            additions: vec![stmt],
                            deletions: vec![],
                        };
                        let facts = AspFactMapper::statements_to_program(&delta.additions);
                        let full_program = if base_program.is_empty() {
                            facts
                        } else {
                            format!("{base_program}\n{facts}")
                        };
                        let answer_sets = solver.solve(&full_program, 1).await.ok()?;
                        let records: Vec<StreamElement> = answer_sets
                            .into_iter()
                            .flat_map(|as_| {
                                as_.atoms.into_iter().map(move |atom| {
                                    StreamElement::Record(StreamRecord::new(atom.to_string(), ts))
                                })
                            })
                            .collect();
                        Some(futures::stream::iter(records))
                    }
                })
                .flatten(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AspError;
    use crate::solver::Atom;
    use cqels_core::stream::RdfStreamElement;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::{Statement, Term};

    struct MockSolver {
        response: Vec<AnswerSet>,
    }

    #[async_trait]
    impl AspSolver for MockSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Ok(self.response.clone())
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
    fn test_asp_continuous_query_id() {
        let config = AspStreamSolveConfig::builder()
            .base_program("a :- not b.")
            .build();
        let query = AspContinuousQuery::new("q1", config);
        assert_eq!(query.query_id(), "q1");
    }

    #[test]
    fn test_asp_continuous_query_type() {
        let config = AspStreamSolveConfig::default();
        let query = AspContinuousQuery::new("q2", config);
        assert_eq!(query.query_type(), QueryType::Custom);
    }

    #[test]
    fn test_asp_continuous_query_string() {
        let config = AspStreamSolveConfig::builder()
            .base_program("edge(a,b).")
            .build();
        let query = AspContinuousQuery::new("q3", config);
        assert_eq!(query.query_string(), "edge(a,b).");
    }

    #[test]
    fn test_asp_solve_result() {
        let result = AspSolveResult {
            answer_sets: vec![AnswerSet::new(vec![Atom::new("ok", vec![])])],
            timestamp: 12345,
        };
        assert_eq!(result.answer_sets.len(), 1);
        assert_eq!(result.timestamp, 12345);
    }

    #[tokio::test]
    async fn test_execute_without_solver_yields_empty() {
        let config = AspStreamSolveConfig::default();
        let query = AspContinuousQuery::new("q", config);
        let inputs = QueryInputs::new();
        let results: Vec<_> = query.execute(inputs).collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_with_solver() {
        let solver = Arc::new(MockSolver {
            response: vec![AnswerSet::new(vec![Atom::new("result", vec!["ok".into()])])],
        });
        let config = AspStreamSolveConfig::default();
        let query = AspContinuousQuery::with_solver("q", config, solver);

        let mut inputs = QueryInputs::new();
        let elem = make_rdf_element("http://ex/s", "http://ex/p", "val", 1000);
        inputs.add_stream("data", Box::pin(futures::stream::iter(vec![elem])));

        let results: Vec<_> = query.execute(inputs).collect().await;
        assert_eq!(results.len(), 1);
        match &results[0] {
            StreamElement::Record(rec) => {
                assert!(rec.value.contains("result"));
                assert_eq!(rec.timestamp, 1000);
            }
            _ => panic!("expected Record"),
        }
    }

    #[tokio::test]
    async fn test_execute_skips_non_rdf() {
        let solver = Arc::new(MockSolver {
            response: vec![AnswerSet::new(vec![Atom::new("ok", vec![])])],
        });
        let config = AspStreamSolveConfig::default();
        let query = AspContinuousQuery::with_solver("q", config, solver);

        let mut inputs = QueryInputs::new();
        let record = StreamElement::Record(StreamRecord::new("not-rdf", 500));
        inputs.add_stream("data", Box::pin(futures::stream::iter(vec![record])));

        let results: Vec<_> = query.execute(inputs).collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_no_inputs_yields_empty() {
        let solver = Arc::new(MockSolver { response: vec![] });
        let config = AspStreamSolveConfig::default();
        let query = AspContinuousQuery::with_solver("q", config, solver);
        let inputs = QueryInputs::new();
        let results: Vec<_> = query.execute(inputs).collect().await;
        assert!(results.is_empty());
    }
}
