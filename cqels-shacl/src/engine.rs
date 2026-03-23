//! SHACL validation engine backed by an ASP solver.
//!
//! [`ShaclValidationEngine`] orchestrates the full validation pipeline:
//! compiling shapes to ASP, encoding data facts, running the solver,
//! and interpreting the results.

use cqels_asp::{AnswerSet, AspSolver};
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};

use crate::candidate::ShaclCandidateGenerator;
use crate::compiler::ShaclAspProgramCompiler;
use crate::config::ShaclStreamSolveConfig;
use crate::error::ShaclError;
use crate::model::ShaclShapeGraph;
use crate::result::{
    ShaclRepairCandidate, ShaclRepairEdit, ShaclRepairOperation, ShaclValidationResult,
    ShaclValidationStatus, ShaclViolation,
};

/// Validation engine that uses an ASP solver to check SHACL shapes.
pub struct ShaclValidationEngine<S: AspSolver> {
    config: ShaclStreamSolveConfig,
    solver: S,
}

impl<S: AspSolver> ShaclValidationEngine<S> {
    /// Creates a new engine with the given configuration and solver.
    pub fn new(config: ShaclStreamSolveConfig, solver: S) -> Self {
        Self { config, solver }
    }

    /// Validates a snapshot of RDF data against the given shapes graph.
    ///
    /// # Arguments
    ///
    /// * `shapes` -- The SHACL shapes graph.
    /// * `snapshot` -- The data to validate (window snapshot).
    /// * `timestamp` -- The timestamp of the snapshot.
    /// * `window_id` -- The identifier of the window.
    ///
    /// # Errors
    ///
    /// Returns [`ShaclError`] if compilation fails or the solver returns
    /// an error.
    pub async fn validate(
        &self,
        shapes: &ShaclShapeGraph,
        snapshot: &[Statement],
        timestamp: i64,
        window_id: &str,
    ) -> Result<ShaclValidationResult, ShaclError> {
        // Step 1: Compile shapes to ASP validation program.
        let validation_prog = ShaclAspProgramCompiler::compile_validation(shapes);

        // Step 2: Encode data snapshot as ASP facts.
        let data_facts = encode_data_facts(snapshot);

        // Step 3: Combine and solve.
        let full_program = format!("{validation_prog}\n{data_facts}");
        let answer_sets = self
            .solver
            .solve(&full_program, 1)
            .await
            .map_err(ShaclError::from)?;

        // Step 4: Parse violations from the first answer set.
        let violations = if let Some(answer_set) = answer_sets.first() {
            parse_violations(answer_set)
        } else {
            Vec::new()
        };

        let conforms = violations.is_empty();

        // Step 5: If repair is enabled and violations exist, run repair search.
        let (candidates, status) = if !conforms && self.config.enable_repair_search {
            let repair_result = self.run_repair_search(shapes, snapshot).await?;
            let status = if repair_result.is_empty() {
                ShaclValidationStatus::Violates
            } else if repair_result.len() >= self.config.max_candidate_models {
                ShaclValidationStatus::SearchBounded
            } else {
                ShaclValidationStatus::RepairCandidatesFound
            };
            (repair_result, status)
        } else if conforms {
            (Vec::new(), ShaclValidationStatus::Conforms)
        } else {
            (Vec::new(), ShaclValidationStatus::Violates)
        };

        Ok(ShaclValidationResult {
            timestamp,
            window_id: window_id.to_string(),
            conforms,
            violations,
            candidates,
            status,
        })
    }

    /// Runs the repair search phase.
    async fn run_repair_search(
        &self,
        shapes: &ShaclShapeGraph,
        snapshot: &[Statement],
    ) -> Result<Vec<ShaclRepairCandidate>, ShaclError> {
        // Generate candidate add/delete facts.
        let candidate_set = ShaclCandidateGenerator::generate(shapes, snapshot);

        // Compile repair program.
        let repair_prog = ShaclAspProgramCompiler::compile_repair(shapes);

        // Encode data and candidate facts.
        let data_facts = encode_data_facts(snapshot);
        let candidate_facts = encode_candidate_facts(&candidate_set);

        let full_program = format!("{repair_prog}\n{data_facts}\n{candidate_facts}");

        let answer_sets = self
            .solver
            .solve(&full_program, self.config.max_candidate_models)
            .await
            .map_err(ShaclError::from)?;

        // Parse repair candidates from answer sets.
        let mut candidates: Vec<ShaclRepairCandidate> =
            answer_sets.iter().map(parse_repair_candidate).collect();

        // Limit to top candidates.
        candidates.truncate(self.config.top_repair_candidates);

        Ok(candidates)
    }
}

/// Encodes a slice of RDF statements as ASP `triple/3` facts.
fn encode_data_facts(statements: &[Statement]) -> String {
    let mut facts = String::new();
    facts.push_str("% Data facts\n");
    for stmt in statements {
        let s = encode_term(&stmt.subject);
        let p = format!("\"{}\"", stmt.predicate.as_str());
        let o = encode_term(&stmt.object);
        facts.push_str(&format!("triple({s},{p},{o}).\n"));
    }
    facts
}

/// Encodes candidate add/delete operations as ASP facts.
fn encode_candidate_facts(candidates: &crate::candidate::ShaclCandidateSet) -> String {
    let mut facts = String::new();
    facts.push_str("% Candidate facts\n");
    for stmt in &candidates.add_candidates {
        let s = encode_term(&stmt.subject);
        let p = format!("\"{}\"", stmt.predicate.as_str());
        let o = encode_term(&stmt.object);
        facts.push_str(&format!("candidate_add({s},{p},{o}).\n"));
    }
    for stmt in &candidates.delete_candidates {
        let s = encode_term(&stmt.subject);
        let p = format!("\"{}\"", stmt.predicate.as_str());
        let o = encode_term(&stmt.object);
        facts.push_str(&format!("candidate_del({s},{p},{o}).\n"));
    }
    facts
}

/// Encodes a single [`Term`] as an ASP term string.
fn encode_term(term: &Term) -> String {
    match term {
        Term::Iri(iri) => format!("\"{}\"", iri.as_str()),
        Term::BlankNode(bn) => format!("\"_:{}\"", bn.id()),
        Term::Literal(lit) => format!("\"{}\"", lit.value().replace('"', "\\\"")),
        _ => format!("\"{term:?}\""),
    }
}

/// Parses `violation/4` atoms from an answer set into [`ShaclViolation`] values.
fn parse_violations(answer_set: &AnswerSet) -> Vec<ShaclViolation> {
    answer_set
        .query_predicate("violation")
        .into_iter()
        .filter_map(|atom| {
            if atom.terms.len() >= 4 {
                Some(ShaclViolation {
                    shape: unquote(&atom.terms[0]),
                    focus: unquote(&atom.terms[1]),
                    constraint: unquote(&atom.terms[2]),
                    detail: unquote(&atom.terms[3]),
                })
            } else {
                None
            }
        })
        .collect()
}

/// Parses repair_add/repair_del atoms from an answer set.
fn parse_repair_candidate(answer_set: &AnswerSet) -> ShaclRepairCandidate {
    let mut edits = Vec::new();

    for atom in &answer_set.atoms {
        let (operation, terms) = match atom.predicate.as_str() {
            "repair_add" if atom.terms.len() >= 3 => (ShaclRepairOperation::Add, &atom.terms),
            "repair_del" if atom.terms.len() >= 3 => (ShaclRepairOperation::Delete, &atom.terms),
            _ => continue,
        };

        let subject = Term::Iri(IriTerm::new(unquote(&terms[0])));
        let predicate = IriTerm::new(unquote(&terms[1]));
        let object = Term::Literal(LiteralTerm::new(unquote(&terms[2])));
        let statement = Statement::new(subject, predicate, object);

        edits.push(ShaclRepairEdit {
            operation,
            statement,
        });
    }

    ShaclRepairCandidate { edits }
}

/// Strips surrounding double quotes from an ASP term string.
fn unquote(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_asp::{AnswerSet, AspError, Atom};

    /// A mock ASP solver for testing.
    struct MockSolver {
        answer_sets: Vec<AnswerSet>,
    }

    #[async_trait::async_trait]
    impl AspSolver for MockSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
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

    #[tokio::test]
    async fn test_validate_conforming() {
        let solver = MockSolver {
            answer_sets: vec![AnswerSet::new(vec![])],
        };
        let config = ShaclStreamSolveConfig::default();
        let engine = ShaclValidationEngine::new(config, solver);

        let shapes = ShaclShapeGraph::new();
        let data = vec![make_stmt("http://ex/s", "http://ex/p", "val")];

        let result = engine.validate(&shapes, &data, 1000, "w1").await.unwrap();
        assert!(result.conforms);
        assert_eq!(result.status, ShaclValidationStatus::Conforms);
        assert!(result.violations.is_empty());
    }

    #[tokio::test]
    async fn test_validate_with_violations() {
        let violation_atom = Atom::new(
            "violation",
            vec![
                "\"PersonShape\"".into(),
                "\"http://ex/alice\"".into(),
                "\"minCount\"".into(),
                "\"http://ex/name\"".into(),
            ],
        );
        let solver = MockSolver {
            answer_sets: vec![AnswerSet::new(vec![violation_atom])],
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
        assert_eq!(result.violations[0].constraint, "minCount");
    }

    #[tokio::test]
    async fn test_validate_with_repair_search() {
        let violation_atom = Atom::new(
            "violation",
            vec![
                "\"S1\"".into(),
                "\"http://ex/n1\"".into(),
                "\"minCount\"".into(),
                "\"http://ex/p\"".into(),
            ],
        );
        let repair_atom = Atom::new(
            "repair_add",
            vec![
                "\"http://ex/n1\"".into(),
                "\"http://ex/p\"".into(),
                "\"value\"".into(),
            ],
        );
        let solver = MockSolver {
            answer_sets: vec![
                // First call returns violations.
                AnswerSet::new(vec![violation_atom]),
                // Second call returns repair candidates.
                AnswerSet::new(vec![repair_atom]),
            ],
        };
        let config = ShaclStreamSolveConfig::builder()
            .enable_repair_search(true)
            .build();
        let engine = ShaclValidationEngine::new(config, solver);

        let shapes = ShaclShapeGraph::new();
        let data = vec![];

        let result = engine.validate(&shapes, &data, 3000, "w3").await.unwrap();
        assert!(!result.conforms);
        // The mock returns one repair answer set on the second call.
        // Since the mock always returns the same answer_sets, the repair
        // search will also get the violation atom, but the repair_add atom
        // will be parsed as a candidate.
    }

    #[tokio::test]
    async fn test_validate_solver_error() {
        struct FailingSolver;

        #[async_trait::async_trait]
        impl AspSolver for FailingSolver {
            async fn solve(
                &self,
                _program: &str,
                _max_models: usize,
            ) -> Result<Vec<AnswerSet>, AspError> {
                Err(AspError::SolverNotFound)
            }
        }

        let config = ShaclStreamSolveConfig::default();
        let engine = ShaclValidationEngine::new(config, FailingSolver);

        let shapes = ShaclShapeGraph::new();
        let result = engine.validate(&shapes, &[], 0, "w0").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_data_facts() {
        let stmts = vec![make_stmt("http://ex/s", "http://ex/p", "hello")];
        let facts = encode_data_facts(&stmts);
        assert!(facts.contains("triple("));
        assert!(facts.contains("http://ex/s"));
        assert!(facts.contains("http://ex/p"));
        assert!(facts.contains("hello"));
    }

    #[test]
    fn test_parse_violations_from_answer_set() {
        let answer_set = AnswerSet::new(vec![
            Atom::new(
                "violation",
                vec![
                    "\"S1\"".into(),
                    "\"n1\"".into(),
                    "\"minCount\"".into(),
                    "\"p1\"".into(),
                ],
            ),
            Atom::new("other", vec!["x".into()]),
        ]);
        let violations = parse_violations(&answer_set);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].shape, "S1");
    }

    #[test]
    fn test_unquote() {
        assert_eq!(unquote("\"hello\""), "hello");
        assert_eq!(unquote("noquotes"), "noquotes");
        assert_eq!(unquote("\"\""), "");
    }

    #[test]
    fn test_encode_term_variants() {
        let iri = Term::Iri(IriTerm::new("http://example.org/s"));
        assert_eq!(encode_term(&iri), "\"http://example.org/s\"");

        let blank = Term::BlankNode(cqels_model::BlankNodeTerm::new("b0"));
        assert_eq!(encode_term(&blank), "\"_:b0\"");

        let lit = Term::Literal(LiteralTerm::new("val"));
        assert_eq!(encode_term(&lit), "\"val\"");
    }
}
