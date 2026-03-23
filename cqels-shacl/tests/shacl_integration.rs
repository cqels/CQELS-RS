//! Integration tests for the cqels-shacl crate.

use async_trait::async_trait;

use cqels_asp::{AnswerSet, AspError, AspSolver, Atom};
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::{Statement, Term};
use cqels_shacl::{
    ShaclShapeGraph, ShaclStreamSolveConfig, ShaclValidationEngine, ShaclValidationStatus,
};

// -- Mock solver -----------------------------------------------------------

struct MockSolver {
    answer_sets: Vec<AnswerSet>,
}

#[async_trait]
impl AspSolver for MockSolver {
    async fn solve(&self, _program: &str, _max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        Ok(self.answer_sets.clone())
    }
}

fn make_stmt(s: &str, p: &str, o: &str) -> Statement {
    Statement::new(
        Term::Iri(IriTerm::new(s)),
        IriTerm::new(p),
        Term::Literal(LiteralTerm::new(o)),
    )
}

// -- Validation pipeline ---------------------------------------------------

#[tokio::test]
async fn pipeline_conforming_data() {
    let solver = MockSolver {
        answer_sets: vec![AnswerSet::new(vec![])],
    };
    let config = ShaclStreamSolveConfig::default();
    let engine = ShaclValidationEngine::new(config, solver);

    let shapes = ShaclShapeGraph::new();
    let data = vec![make_stmt("http://ex/s", "http://ex/name", "Alice")];

    let result = engine.validate(&shapes, &data, 1000, "w1").await.unwrap();
    assert!(result.conforms);
    assert_eq!(result.status, ShaclValidationStatus::Conforms);
    assert!(result.violations.is_empty());
    assert!(result.candidates.is_empty());
}

#[tokio::test]
async fn pipeline_with_violations() {
    let violation = Atom::new(
        "violation",
        vec![
            "\"PersonShape\"".into(),
            "\"http://ex/alice\"".into(),
            "\"minCount\"".into(),
            "\"http://ex/name\"".into(),
        ],
    );
    let solver = MockSolver {
        answer_sets: vec![AnswerSet::new(vec![violation])],
    };
    let config = ShaclStreamSolveConfig::default();
    let engine = ShaclValidationEngine::new(config, solver);

    let shapes = ShaclShapeGraph::new();
    let data = vec![];

    let result = engine.validate(&shapes, &data, 2000, "w2").await.unwrap();
    assert!(!result.conforms);
    assert_eq!(result.status, ShaclValidationStatus::Violates);
    assert_eq!(result.violations.len(), 1);
    assert_eq!(result.violations[0].shape, "PersonShape");
    assert_eq!(result.violations[0].focus, "http://ex/alice");
    assert_eq!(result.violations[0].constraint, "minCount");
}

#[tokio::test]
async fn pipeline_repair_candidates() {
    let violation = Atom::new(
        "violation",
        vec![
            "\"S1\"".into(),
            "\"http://ex/n1\"".into(),
            "\"minCount\"".into(),
            "\"http://ex/p\"".into(),
        ],
    );
    let repair_add = Atom::new(
        "repair_add",
        vec![
            "\"http://ex/n1\"".into(),
            "\"http://ex/p\"".into(),
            "\"value\"".into(),
        ],
    );
    let solver = MockSolver {
        answer_sets: vec![AnswerSet::new(vec![violation, repair_add])],
    };
    let config = ShaclStreamSolveConfig::builder()
        .enable_repair_search(true)
        .build();
    let engine = ShaclValidationEngine::new(config, solver);

    let shapes = ShaclShapeGraph::new();
    let data = vec![];

    let result = engine.validate(&shapes, &data, 3000, "w3").await.unwrap();
    assert!(!result.conforms);
    // Repair search was run; candidates may be found depending on answer sets
    assert!(!result.violations.is_empty());
}

#[tokio::test]
async fn pipeline_solver_error() {
    struct FailSolver;

    #[async_trait]
    impl AspSolver for FailSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Err(AspError::SolverNotFound)
        }
    }

    let config = ShaclStreamSolveConfig::default();
    let engine = ShaclValidationEngine::new(config, FailSolver);

    let shapes = ShaclShapeGraph::new();
    let result = engine.validate(&shapes, &[], 0, "w0").await;
    assert!(result.is_err());
}
