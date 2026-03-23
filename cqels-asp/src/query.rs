//! ASP-based continuous query implementation.
//!
//! [`AspContinuousQuery`] implements the [`ContinuousQuery`] trait from
//! `cqels-core`, bridging ASP solving into the CQELS streaming pipeline.

use std::pin::Pin;

use async_trait::async_trait;
use futures::stream;
use futures::Stream;

use cqels_core::query::{ContinuousQuery, QueryInputs, QueryType};
use cqels_core::stream::StreamElement;

use crate::config::AspStreamSolveConfig;
use crate::solver::AnswerSet;

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
/// This query type uses [`StreamElement`] as its result type for compatibility
/// with the `cqels-core` streaming pipeline. The actual ASP solving logic
/// will be fully wired in a future iteration; this provides the type structure.
pub struct AspContinuousQuery {
    id: String,
    config: AspStreamSolveConfig,
}

impl AspContinuousQuery {
    /// Creates a new ASP continuous query with the given id and configuration.
    pub fn new(id: impl Into<String>, config: AspStreamSolveConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
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

    fn execute(&self, _inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Self::Result> + Send>> {
        // Placeholder: full ASP-stream integration will be implemented in a future iteration.
        Box::pin(stream::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use crate::solver::Atom;

        let result = AspSolveResult {
            answer_sets: vec![AnswerSet::new(vec![Atom::new("ok", vec![])])],
            timestamp: 12345,
        };
        assert_eq!(result.answer_sets.len(), 1);
        assert_eq!(result.timestamp, 12345);
    }
}
