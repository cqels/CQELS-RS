//! SHACL continuous query stub.
//!
//! [`ShaclContinuousQuery`] is a placeholder for integrating SHACL validation
//! into the CQELS continuous query pipeline.

use crate::config::ShaclStreamSolveConfig;
use crate::model::ShaclShapeGraph;

/// A continuous query that validates streaming data against SHACL shapes.
///
/// This is a structural stub; the full pipeline integration is implemented
/// in the `cqels-engine` crate.
pub struct ShaclContinuousQuery {
    id: String,
    shapes: ShaclShapeGraph,
    config: ShaclStreamSolveConfig,
}

impl ShaclContinuousQuery {
    /// Creates a new continuous SHACL query.
    pub fn new(id: String, shapes: ShaclShapeGraph, config: ShaclStreamSolveConfig) -> Self {
        Self { id, shapes, config }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_continuous_query_creation() {
        let shapes = ShaclShapeGraph::new();
        let config = ShaclStreamSolveConfig::default();
        let query = ShaclContinuousQuery::new("q1".into(), shapes, config);
        assert_eq!(query.query_id(), "q1");
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
}
