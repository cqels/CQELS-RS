//! Live SHACL validation MCP tool.
//!
//! Bridges [`cqels-shacl`](cqels_shacl)'s [`ShaclValidationEngine`] to
//! the MCP tool surface. The tool accepts a SHACL shape graph and a
//! data graph (both as arrays of `{s, p, o}` triples), runs validation
//! through an [`AspSolver`], and returns a structured result with
//! violations and optional repair candidates.
//!
//! ## Solver wiring
//!
//! The default constructor [`validate_tool`] uses
//! [`ClingoSubprocessSolver`], which shells out to a `clingo` binary on
//! `$PATH`. If `clingo` is not installed, the tool returns a clear
//! error rather than panicking — making it "clingo-conditional" from
//! the LLM agent's perspective.
//!
//! For tests and custom backends, use [`validate_tool_with_solver`]
//! and pass any `Arc<dyn AspSolver>`.

use std::sync::Arc;

use async_trait::async_trait;
use cqels_asp::{AnswerSet, AspError, AspSolver, ClingoSubprocessSolver};
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};
use cqels_shacl::{
    ShaclShapeParser, ShaclStreamSolveConfig, ShaclValidationEngine, ShaclViolation,
};
use serde_json::json;
use tokio::runtime::Runtime;

use crate::tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};

/// Returns a [`ValidateTool`] backed by a subprocess Clingo solver.
///
/// The tool will fail with a clear "solver not found" error if `clingo`
/// is not on `$PATH`.
pub fn validate_tool() -> ValidateTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    ValidateTool::new(solver)
}

/// Returns a [`ValidateTool`] backed by the supplied solver.
///
/// Useful for tests (with a mock solver) or for plugging in a custom
/// ASP backend without spawning a subprocess.
pub fn validate_tool_with_solver(solver: Arc<dyn AspSolver>) -> ValidateTool {
    ValidateTool::new(solver)
}

/// MCP tool that runs SHACL validation over a snapshot of triples.
pub struct ValidateTool {
    solver: Arc<dyn AspSolver>,
    runtime: Arc<Runtime>,
}

impl ValidateTool {
    fn new(solver: Arc<dyn AspSolver>) -> Self {
        let runtime =
            Arc::new(Runtime::new().expect("tokio multi-thread runtime for ValidateTool"));
        Self { solver, runtime }
    }
}

impl McpTool for ValidateTool {
    fn name(&self) -> &str {
        "validate"
    }

    fn description(&self) -> &str {
        "Validate an RDF data snapshot against a SHACL shape graph. Both \
         `shapes` and `data` are arrays of `{s, p, o}` triples. Returns \
         `conforms`, the list of violations, and (if `enable_repair_search` \
         is true) repair candidates. Requires an installed ASP solver; the \
         default backend shells out to `clingo`."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "shapes",
                json!({
                    "type": "array",
                    "description": "Shape graph as { s, p, o } triples (Turtle-style IRI/literal heuristics).",
                    "items": triple_schema()
                }),
            )
            .with_property(
                "data",
                json!({
                    "type": "array",
                    "description": "Data graph to validate, as { s, p, o } triples.",
                    "items": triple_schema()
                }),
            )
            .with_property(
                "timestamp",
                json!({
                    "type": "integer",
                    "description": "Snapshot timestamp (ms). Defaults to 0.",
                    "default": 0
                }),
            )
            .with_property(
                "window_id",
                json!({
                    "type": "string",
                    "description": "Identifier of the snapshot window. Defaults to \"snapshot\".",
                    "default": "snapshot"
                }),
            )
            .with_property(
                "enable_repair_search",
                json!({
                    "type": "boolean",
                    "description": "If true, enumerate repair candidates after violations are found. Defaults to false.",
                    "default": false
                }),
            )
            .require("shapes")
            .require("data")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let shapes_triples = match invocation.get("shapes").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return ToolResult::error("missing or non-array `shapes` argument"),
        };
        let data_triples = match invocation.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return ToolResult::error("missing or non-array `data` argument"),
        };
        let timestamp = invocation
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let window_id = invocation
            .get_str("window_id")
            .unwrap_or("snapshot")
            .to_string();
        let enable_repair_search = invocation
            .get("enable_repair_search")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let shapes_stmts = match parse_triple_array(shapes_triples, "shapes") {
            Ok(s) => s,
            Err(e) => return ToolResult::error(e),
        };
        let data_stmts = match parse_triple_array(data_triples, "data") {
            Ok(s) => s,
            Err(e) => return ToolResult::error(e),
        };

        let shape_graph = match ShaclShapeParser::parse(&shapes_stmts) {
            Ok(g) => g,
            Err(e) => return ToolResult::error(format!("shape parse error: {e}")),
        };

        let config = ShaclStreamSolveConfig::builder()
            .enable_repair_search(enable_repair_search)
            .build();
        let engine = ShaclValidationEngine::new(config, DynAspSolver(self.solver.clone()));

        let result = self.runtime.block_on(async {
            engine
                .validate(&shape_graph, &data_stmts, timestamp, &window_id)
                .await
        });

        match result {
            Ok(validation) => ToolResult::success(json!({
                "conforms": validation.conforms,
                "status": format!("{:?}", validation.status),
                "timestamp": validation.timestamp,
                "window_id": validation.window_id,
                "violation_count": validation.violations.len(),
                "violations": violations_to_json(&validation.violations),
                "candidate_count": validation.candidates.len(),
                // Candidates are a Vec<ShaclRepairCandidate>; render via Debug
                // since the result struct's serde shape isn't part of this
                // tool's public contract.
                "candidates": validation
                    .candidates
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect::<Vec<_>>(),
            })),
            Err(e) => ToolResult::error(format!("validation failed: {e}")),
        }
    }
}

/// Wraps an `Arc<dyn AspSolver>` so it itself implements [`AspSolver`].
///
/// [`ShaclValidationEngine`] is generic over `S: AspSolver`, but
/// `Arc<dyn AspSolver>` does not automatically implement the trait —
/// this newtype bridges the gap.
struct DynAspSolver(Arc<dyn AspSolver>);

#[async_trait]
impl AspSolver for DynAspSolver {
    async fn solve(&self, program: &str, max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        self.0.solve(program, max_models).await
    }
}

fn triple_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "s": { "type": "string" },
            "p": { "type": "string" },
            "o": { "type": "string" }
        },
        "required": ["s", "p", "o"]
    })
}

fn parse_triple_array(
    triples: &[serde_json::Value],
    field: &str,
) -> Result<Vec<Statement>, String> {
    let mut out = Vec::with_capacity(triples.len());
    for (i, t) in triples.iter().enumerate() {
        let (s, p, o) = match (
            t.get("s").and_then(|v| v.as_str()),
            t.get("p").and_then(|v| v.as_str()),
            t.get("o").and_then(|v| v.as_str()),
        ) {
            (Some(s), Some(p), Some(o)) => (s, p, o),
            _ => {
                return Err(format!("{field}[{i}] must be {{ s, p, o }} of strings"));
            }
        };
        out.push(Statement::new(
            parse_term(s),
            IriTerm::new(p),
            parse_term(o),
        ));
    }
    Ok(out)
}

/// Same heuristic as `tools::parse_term`: bracketed `<...>` → IRI,
/// `http://` / `urn:` / `https://` / `file://` prefix → IRI, else
/// literal.
fn parse_term(s: &str) -> Term {
    let trimmed = s.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') && trimmed.len() >= 2 {
        return Term::Iri(IriTerm::new(&trimmed[1..trimmed.len() - 1]));
    }
    let looks_like_iri = trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("urn:")
        || trimmed.starts_with("file://");
    if looks_like_iri {
        Term::Iri(IriTerm::new(trimmed))
    } else {
        Term::Literal(LiteralTerm::new(trimmed))
    }
}

fn violations_to_json(violations: &[ShaclViolation]) -> Vec<serde_json::Value> {
    violations
        .iter()
        .map(|v| {
            json!({
                "shape": v.shape,
                "focus": v.focus,
                "constraint": v.constraint,
                "detail": v.detail,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_asp::Atom;
    use std::sync::Mutex;

    /// Mock solver that returns canned answer sets, recording the
    /// programs it was asked to solve.
    struct MockSolver {
        responses: Mutex<Vec<Vec<AnswerSet>>>,
        programs: Mutex<Vec<String>>,
    }

    impl MockSolver {
        fn new(responses: Vec<Vec<AnswerSet>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                programs: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AspSolver for MockSolver {
        async fn solve(
            &self,
            program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            self.programs.lock().unwrap().push(program.to_string());
            let mut guard = self.responses.lock().unwrap();
            if guard.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(guard.remove(0))
            }
        }
    }

    struct FailingSolver;

    #[async_trait]
    impl AspSolver for FailingSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Err(AspError::SolverNotFound)
        }
    }

    fn empty_data_invocation() -> ToolInvocation {
        ToolInvocation::new()
            .with_arg("shapes", json!([]))
            .with_arg("data", json!([]))
    }

    #[test]
    fn validate_with_empty_solver_response_reports_conforming() {
        let solver: Arc<dyn AspSolver> =
            Arc::new(MockSolver::new(vec![vec![AnswerSet::new(Vec::new())]]));
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(&empty_data_invocation());
        assert!(!res.is_error, "validate failed: {:?}", res.content);
        assert_eq!(res.content["conforms"], true);
        assert_eq!(res.content["violation_count"], 0);
    }

    #[test]
    fn validate_reports_violation_atoms_back_as_structured_violations() {
        let violation = Atom::new(
            "violation",
            vec![
                "\"PersonShape\"".into(),
                "\"http://ex/alice\"".into(),
                "\"minCount\"".into(),
                "\"http://ex/name\"".into(),
            ],
        );
        let solver: Arc<dyn AspSolver> =
            Arc::new(MockSolver::new(vec![vec![AnswerSet::new(vec![violation])]]));
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(&empty_data_invocation());
        assert!(!res.is_error);
        assert_eq!(res.content["conforms"], false);
        assert_eq!(res.content["violation_count"], 1);
        let violations = res.content["violations"].as_array().unwrap();
        assert_eq!(violations[0]["shape"], "PersonShape");
        assert_eq!(violations[0]["constraint"], "minCount");
    }

    #[test]
    fn validate_propagates_solver_errors() {
        let solver: Arc<dyn AspSolver> = Arc::new(FailingSolver);
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(&empty_data_invocation());
        assert!(res.is_error, "solver error should surface as tool error");
        let msg = res.content["message"].as_str().unwrap();
        assert!(
            msg.contains("validation failed") || msg.contains("solver"),
            "got: {msg}"
        );
    }

    #[test]
    fn validate_rejects_missing_shapes_argument() {
        let solver: Arc<dyn AspSolver> = Arc::new(MockSolver::new(Vec::new()));
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(&ToolInvocation::new().with_arg("data", json!([])));
        assert!(res.is_error);
    }

    #[test]
    fn validate_rejects_missing_data_argument() {
        let solver: Arc<dyn AspSolver> = Arc::new(MockSolver::new(Vec::new()));
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(&ToolInvocation::new().with_arg("shapes", json!([])));
        assert!(res.is_error);
    }

    #[test]
    fn validate_rejects_malformed_triple() {
        let solver: Arc<dyn AspSolver> = Arc::new(MockSolver::new(Vec::new()));
        let tool = validate_tool_with_solver(solver);
        let res = tool.call(
            &ToolInvocation::new()
                .with_arg("shapes", json!([{ "s": "x", "p": "y" }]))
                .with_arg("data", json!([])),
        );
        assert!(res.is_error);
        let msg = res.content["message"].as_str().unwrap();
        assert!(msg.contains("shapes[0]"), "got: {msg}");
    }

    #[test]
    fn validate_threads_data_triples_into_solver_program() {
        let solver = Arc::new(MockSolver::new(vec![vec![AnswerSet::new(Vec::new())]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = validate_tool_with_solver(solver_arc);
        let res = tool.call(
            &ToolInvocation::new()
                .with_arg("shapes", json!([]))
                .with_arg(
                    "data",
                    json!([{
                        "s": "http://ex.org/alice",
                        "p": "http://ex.org/name",
                        "o": "Alice"
                    }]),
                ),
        );
        assert!(!res.is_error);
        let programs = solver.programs.lock().unwrap();
        assert_eq!(programs.len(), 1, "solver invoked once");
        assert!(
            programs[0].contains("alice") && programs[0].contains("Alice"),
            "program should embed data facts: {}",
            programs[0]
        );
    }

    #[test]
    fn validate_default_constructor_advertises_schema() {
        let tool = validate_tool();
        assert_eq!(tool.name(), "validate");
        let schema = tool.input_schema();
        assert!(schema.required.contains(&"shapes".to_string()));
        assert!(schema.required.contains(&"data".to_string()));
        assert!(schema.properties.contains_key("enable_repair_search"));
    }
}
