//! Subprocess-based Clingo solver implementation.
//!
//! [`ClingoSubprocessSolver`] shells out to the `clingo` executable,
//! communicating via stdin/stdout with JSON output (`--outf=2`).

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::process::Command;
use tracing::debug;

use crate::error::AspError;
use crate::solver::{AnswerSet, AspSolver, Atom};

/// An ASP solver that invokes the Clingo executable as a subprocess.
///
/// The solver writes the logic program to Clingo's stdin and parses the
/// JSON output produced by `--outf=2`.
///
/// # Examples
///
/// ```no_run
/// use cqels_asp::{ClingoSubprocessSolver, AspSolver};
///
/// # async fn example() -> Result<(), cqels_asp::AspError> {
/// let solver = ClingoSubprocessSolver::new();
/// let results = solver.solve("a :- not b. b :- not a.", 0).await?;
/// for answer_set in &results {
///     println!("{:?}", answer_set);
/// }
/// # Ok(())
/// # }
/// ```
pub struct ClingoSubprocessSolver {
    clingo_path: String,
}

impl ClingoSubprocessSolver {
    /// Creates a new solver that looks for `clingo` on `$PATH`.
    pub fn new() -> Self {
        Self {
            clingo_path: "clingo".to_string(),
        }
    }

    /// Creates a new solver with a custom path to the Clingo executable.
    pub fn with_path(path: impl Into<String>) -> Self {
        Self {
            clingo_path: path.into(),
        }
    }
}

impl Default for ClingoSubprocessSolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AspSolver for ClingoSubprocessSolver {
    async fn solve(&self, program: &str, max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        let output = Command::new(&self.clingo_path)
            .arg("--outf=2")
            .arg(format!("--models={max_models}"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    AspError::SolverNotFound
                } else {
                    AspError::Io(e)
                }
            })?;

        // Write program to stdin
        use tokio::io::AsyncWriteExt;
        let mut child = output;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(program.as_bytes()).await?;
            drop(stdin); // Close stdin to signal EOF
        }

        let output = child.wait_with_output().await?;
        let exit_code = output.status.code().unwrap_or(-1);

        debug!(
            exit_code,
            stdout_len = output.stdout.len(),
            "clingo process completed"
        );

        // Clingo exit codes:
        // 10 = SATISFIABLE (all models enumerated)
        // 20 = UNSATISFIABLE
        // 30 = SATISFIABLE (model limit reached, i.e., more models may exist)
        match exit_code {
            10 | 30 => {
                // SAT -- parse the JSON output
                let json_str = String::from_utf8_lossy(&output.stdout);
                parse_clingo_json(&json_str)
            }
            20 => {
                // UNSAT -- no answer sets
                Ok(vec![])
            }
            _ => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                Err(AspError::SolverFailed {
                    message: stderr.to_string(),
                    exit_code,
                })
            }
        }
    }
}

/// Parses the JSON output produced by `clingo --outf=2`.
///
/// The expected structure is:
/// ```json
/// {
///   "Call": [
///     {
///       "Witnesses": [
///         { "Value": ["atom1", "atom2(a,b)"] }
///       ]
///     }
///   ],
///   "Result": "SATISFIABLE"
/// }
/// ```
pub(crate) fn parse_clingo_json(json_str: &str) -> Result<Vec<AnswerSet>, AspError> {
    let root: JsonValue = serde_json::from_str(json_str).map_err(|e| AspError::ParseError {
        message: format!("invalid JSON from clingo: {e}"),
    })?;

    let calls =
        root.get("Call")
            .and_then(|v| v.as_array())
            .ok_or_else(|| AspError::ParseError {
                message: "missing 'Call' array in clingo JSON output".into(),
            })?;

    let mut answer_sets = Vec::new();

    for call in calls {
        let witnesses = match call.get("Witnesses").and_then(|v| v.as_array()) {
            Some(w) => w,
            None => continue,
        };

        for witness in witnesses {
            let values = match witness.get("Value").and_then(|v| v.as_array()) {
                Some(v) => v,
                None => continue,
            };

            let atoms: Vec<Atom> = values
                .iter()
                .filter_map(|v| v.as_str())
                .map(parse_atom_string)
                .collect();

            answer_sets.push(AnswerSet::new(atoms));
        }
    }

    Ok(answer_sets)
}

/// Parses a single atom string like `"edge(a,b)"` or `"fact"` into an [`Atom`].
fn parse_atom_string(s: &str) -> Atom {
    if let Some(paren_pos) = s.find('(') {
        let predicate = &s[..paren_pos];
        // Strip only the final closing parenthesis
        let inner = &s[paren_pos + 1..];
        let args_str = inner.strip_suffix(')').unwrap_or(inner);
        let terms: Vec<String> = split_top_level_args(args_str);
        Atom::new(predicate, terms)
    } else {
        Atom::new(s, vec![])
    }
}

/// Splits arguments at top-level commas, respecting nested parentheses and quotes.
fn split_top_level_args(s: &str) -> Vec<String> {
    if s.is_empty() {
        return vec![];
    }

    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    let mut in_quotes = false;

    for ch in s.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '(' if !in_quotes => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_quotes && depth > 0 => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 && !in_quotes => {
                result.push(current.clone());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        result.push(current);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_clingo_json_satisfiable() {
        let json = r#"{
            "Solver": "clingo version 5.4.0",
            "Call": [
                {
                    "Witnesses": [
                        { "Value": ["a", "edge(a,b)", "node(a)"] }
                    ]
                }
            ],
            "Result": "SATISFIABLE"
        }"#;

        let result = parse_clingo_json(json).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].atoms.len(), 3);
        assert_eq!(result[0].atoms[0].predicate, "a");
        assert!(result[0].atoms[0].terms.is_empty());
        assert_eq!(result[0].atoms[1].predicate, "edge");
        assert_eq!(result[0].atoms[1].terms, vec!["a", "b"]);
        assert_eq!(result[0].atoms[2].predicate, "node");
        assert_eq!(result[0].atoms[2].terms, vec!["a"]);
    }

    #[test]
    fn test_parse_clingo_json_multiple_witnesses() {
        let json = r#"{
            "Call": [
                {
                    "Witnesses": [
                        { "Value": ["a"] },
                        { "Value": ["b"] }
                    ]
                }
            ],
            "Result": "SATISFIABLE"
        }"#;

        let result = parse_clingo_json(json).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].atoms[0].predicate, "a");
        assert_eq!(result[1].atoms[0].predicate, "b");
    }

    #[test]
    fn test_parse_clingo_json_invalid() {
        let result = parse_clingo_json("not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_atom_string_simple() {
        let atom = parse_atom_string("fact");
        assert_eq!(atom.predicate, "fact");
        assert!(atom.terms.is_empty());
    }

    #[test]
    fn test_parse_atom_string_with_args() {
        let atom = parse_atom_string("edge(a,b)");
        assert_eq!(atom.predicate, "edge");
        assert_eq!(atom.terms, vec!["a", "b"]);
    }

    #[test]
    fn test_parse_atom_string_nested() {
        let atom = parse_atom_string(
            "rdf(iri(\"http://ex.org\"),iri(\"http://ex.org/p\"),literal(\"val\"))",
        );
        assert_eq!(atom.predicate, "rdf");
        assert_eq!(atom.terms.len(), 3);
        assert_eq!(atom.terms[0], "iri(\"http://ex.org\")");
        assert_eq!(atom.terms[1], "iri(\"http://ex.org/p\")");
        assert_eq!(atom.terms[2], "literal(\"val\")");
    }

    #[test]
    fn test_clingo_subprocess_solver_default() {
        let solver = ClingoSubprocessSolver::new();
        assert_eq!(solver.clingo_path, "clingo");
    }

    #[test]
    fn test_clingo_subprocess_solver_with_path() {
        let solver = ClingoSubprocessSolver::with_path("/usr/local/bin/clingo");
        assert_eq!(solver.clingo_path, "/usr/local/bin/clingo");
    }
}
