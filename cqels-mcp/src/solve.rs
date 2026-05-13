//! Direct ASP-solver MCP tool.
//!
//! Exposes [`AspSolver::solve`] as an MCP tool: clients submit an
//! ASP-Core-2 program and receive zero or more answer sets.
//! Complements the [`crate::validate`] tool, which runs the same
//! solver behind the SHACL validation pipeline.
//!
//! Like `validate`, the default constructor [`solve_tool`] wraps
//! [`ClingoSubprocessSolver`] (clingo-conditional). Use
//! [`solve_tool_with_solver`] to plug in any `Arc<dyn AspSolver>`.

use std::sync::Arc;

use cqels_asp::{AspSolver, ClingoSubprocessSolver};
use serde_json::json;
use tokio::runtime::Runtime;

use crate::tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};

/// Returns a [`SolveTool`] backed by a subprocess Clingo solver.
///
/// Will fail with a clear error if `clingo` is not on `$PATH`.
pub fn solve_tool() -> SolveTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    SolveTool::new(solver)
}

/// Returns a [`SolveTool`] backed by the supplied solver.
pub fn solve_tool_with_solver(solver: Arc<dyn AspSolver>) -> SolveTool {
    SolveTool::new(solver)
}

/// MCP tool that runs an ASP solver directly on a user-supplied program.
pub struct SolveTool {
    solver: Arc<dyn AspSolver>,
    runtime: Arc<Runtime>,
}

impl SolveTool {
    fn new(solver: Arc<dyn AspSolver>) -> Self {
        let runtime = Arc::new(Runtime::new().expect("tokio multi-thread runtime for SolveTool"));
        Self { solver, runtime }
    }
}

const DEFAULT_MAX_MODELS: usize = 1;

impl McpTool for SolveTool {
    fn name(&self) -> &str {
        "solve"
    }

    fn description(&self) -> &str {
        "Run an ASP-Core-2 logic program through the configured solver \
         (default: Clingo subprocess) and return up to `max_models` \
         answer sets. Each answer set is a list of ground atoms. \
         `max_models = 0` enumerates all answer sets. Requires an \
         installed ASP solver on $PATH for the default backend."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "program",
                json!({
                    "type": "string",
                    "description": "ASP-Core-2 program text."
                }),
            )
            .with_property(
                "max_models",
                json!({
                    "type": "integer",
                    "description": "Upper bound on answer sets to enumerate. 0 = all. Defaults to 1.",
                    "default": DEFAULT_MAX_MODELS,
                    "minimum": 0
                }),
            )
            .require("program")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(program) = invocation.get_str("program").map(str::to_string) else {
            return ToolResult::error("missing `program` argument");
        };
        let max_models = invocation
            .get("max_models")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_MAX_MODELS);

        let solver = self.solver.clone();
        let result = self
            .runtime
            .block_on(async move { solver.solve(&program, max_models).await });

        match result {
            Ok(answer_sets) => {
                let serialized: Vec<serde_json::Value> = answer_sets
                    .iter()
                    .map(|set| {
                        json!({
                            "atom_count": set.atoms.len(),
                            "atoms": set
                                .atoms
                                .iter()
                                .map(|a| {
                                    json!({
                                        "predicate": a.predicate,
                                        "terms": a.terms,
                                    })
                                })
                                .collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                ToolResult::success(json!({
                    "model_count": answer_sets.len(),
                    "max_models": max_models,
                    "answer_sets": serialized,
                }))
            }
            Err(e) => ToolResult::error(format!("solve failed: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cqels_asp::{AnswerSet, AspError, Atom};
    use std::sync::Mutex;

    struct MockSolver {
        responses: Mutex<Vec<Vec<AnswerSet>>>,
        programs: Mutex<Vec<String>>,
        max_models_seen: Mutex<Vec<usize>>,
    }

    impl MockSolver {
        fn new(responses: Vec<Vec<AnswerSet>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                programs: Mutex::new(Vec::new()),
                max_models_seen: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AspSolver for MockSolver {
        async fn solve(
            &self,
            program: &str,
            max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            self.programs.lock().unwrap().push(program.to_string());
            self.max_models_seen.lock().unwrap().push(max_models);
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

    #[test]
    fn solve_returns_atoms_for_canned_answer_set() {
        let solver = Arc::new(MockSolver::new(vec![vec![AnswerSet::new(vec![
            Atom::new("path", vec!["a".into(), "b".into()]),
            Atom::new("path", vec!["b".into(), "c".into()]),
        ])]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = solve_tool_with_solver(solver_arc);
        let res = tool.call(&ToolInvocation::new().with_arg("program", json!("path(a,b).")));
        assert!(!res.is_error, "solve failed: {:?}", res.content);
        assert_eq!(res.content["model_count"], 1);
        let answer_sets = res.content["answer_sets"].as_array().unwrap();
        assert_eq!(answer_sets[0]["atom_count"], 2);
        let first_atom = &answer_sets[0]["atoms"][0];
        assert_eq!(first_atom["predicate"], "path");
        let terms = first_atom["terms"].as_array().unwrap();
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn solve_passes_program_and_max_models_to_solver() {
        let solver = Arc::new(MockSolver::new(vec![vec![]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = solve_tool_with_solver(solver_arc);
        tool.call(
            &ToolInvocation::new()
                .with_arg("program", json!("a :- not b. b :- not a."))
                .with_arg("max_models", json!(5)),
        );
        let programs = solver.programs.lock().unwrap();
        assert_eq!(programs[0], "a :- not b. b :- not a.");
        let maxes = solver.max_models_seen.lock().unwrap();
        assert_eq!(maxes[0], 5);
    }

    #[test]
    fn solve_defaults_max_models_to_one() {
        let solver = Arc::new(MockSolver::new(vec![vec![]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = solve_tool_with_solver(solver_arc);
        tool.call(&ToolInvocation::new().with_arg("program", json!("p.")));
        let maxes = solver.max_models_seen.lock().unwrap();
        assert_eq!(maxes[0], 1);
    }

    #[test]
    fn solve_propagates_solver_errors() {
        let solver: Arc<dyn AspSolver> = Arc::new(FailingSolver);
        let tool = solve_tool_with_solver(solver);
        let res = tool.call(&ToolInvocation::new().with_arg("program", json!("p.")));
        assert!(res.is_error);
    }

    #[test]
    fn solve_rejects_missing_program() {
        let solver: Arc<dyn AspSolver> = Arc::new(MockSolver::new(Vec::new()));
        let tool = solve_tool_with_solver(solver);
        let res = tool.call(&ToolInvocation::new());
        assert!(res.is_error);
    }

    #[test]
    fn solve_returns_empty_model_list_for_unsat() {
        // Empty Vec<AnswerSet> models UNSAT.
        let solver = Arc::new(MockSolver::new(vec![vec![]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = solve_tool_with_solver(solver_arc);
        let res = tool.call(&ToolInvocation::new().with_arg("program", json!(":- a. :- not a.")));
        assert!(!res.is_error);
        assert_eq!(res.content["model_count"], 0);
        assert_eq!(res.content["answer_sets"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn solve_handles_multiple_answer_sets() {
        let solver = Arc::new(MockSolver::new(vec![vec![
            AnswerSet::new(vec![Atom::new("a", vec![])]),
            AnswerSet::new(vec![Atom::new("b", vec![])]),
            AnswerSet::new(vec![Atom::new("c", vec![])]),
        ]]));
        let solver_arc: Arc<dyn AspSolver> = solver.clone();
        let tool = solve_tool_with_solver(solver_arc);
        let res = tool.call(
            &ToolInvocation::new()
                .with_arg("program", json!("1 { a; b; c } 1."))
                .with_arg("max_models", json!(0)),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["model_count"], 3);
    }

    #[test]
    fn solve_default_constructor_advertises_schema() {
        let tool = solve_tool();
        assert_eq!(tool.name(), "solve");
        let schema = tool.input_schema();
        assert!(schema.required.contains(&"program".to_string()));
        assert!(schema.properties.contains_key("max_models"));
    }
}
