//! Incremental ASP solving session.
//!
//! [`IncrementalAspSession`] manages a stateful solving context that
//! accumulates facts across successive solve calls, supporting
//! incremental additions and deletions via [`AspFactDelta`].

use crate::error::AspError;
use crate::mapper::{AspFactDelta, AspFactMapper};
use crate::solver::{AnswerSet, AspSolver};

/// A stateful session for incremental ASP solving.
///
/// The session maintains a base program and an accumulated set of facts.
/// Each call to [`solve_with_delta`](Self::solve_with_delta) updates the
/// accumulated facts and re-solves the combined program.
///
/// # Examples
///
/// ```no_run
/// use cqels_asp::{IncrementalAspSession, ClingoSubprocessSolver, AspFactDelta};
/// use cqels_model::{Statement, Term, IriTerm, LiteralTerm};
///
/// # async fn example() -> Result<(), cqels_asp::AspError> {
/// let solver = ClingoSubprocessSolver::new();
/// let mut session = IncrementalAspSession::new(solver, "path(X,Y) :- edge(X,Y).".into());
///
/// let delta = AspFactDelta {
///     additions: vec![Statement::new(
///         Term::Iri(IriTerm::new("http://example.org/a")),
///         IriTerm::new("http://example.org/edge"),
///         Term::Iri(IriTerm::new("http://example.org/b")),
///     )],
///     deletions: vec![],
/// };
/// let results = session.solve_with_delta(&delta).await?;
/// # Ok(())
/// # }
/// ```
pub struct IncrementalAspSession<S: AspSolver> {
    solver: S,
    base_program: String,
    accumulated_facts: String,
}

impl<S: AspSolver> IncrementalAspSession<S> {
    /// Creates a new session with the given solver and base program.
    pub fn new(solver: S, base_program: String) -> Self {
        Self {
            solver,
            base_program,
            accumulated_facts: String::new(),
        }
    }

    /// Applies the delta (additions/deletions), re-solves, and returns answer sets.
    ///
    /// Additions are appended to the accumulated facts. Deletions cause
    /// matching facts to be removed from the accumulated set. The full
    /// program (base + accumulated facts) is then sent to the solver.
    pub async fn solve_with_delta(
        &mut self,
        delta: &AspFactDelta,
    ) -> Result<Vec<AnswerSet>, AspError> {
        // Remove deleted facts
        for stmt in &delta.deletions {
            let fact = AspFactMapper::statement_to_fact(stmt);
            self.accumulated_facts = self
                .accumulated_facts
                .lines()
                .filter(|line| *line != fact)
                .collect::<Vec<_>>()
                .join("\n");
        }

        // Add new facts
        for stmt in &delta.additions {
            let fact = AspFactMapper::statement_to_fact(stmt);
            if !self.accumulated_facts.is_empty() {
                self.accumulated_facts.push('\n');
            }
            self.accumulated_facts.push_str(&fact);
        }

        // Combine base program with accumulated facts
        let full_program = if self.base_program.is_empty() {
            self.accumulated_facts.clone()
        } else if self.accumulated_facts.is_empty() {
            self.base_program.clone()
        } else {
            format!("{}\n{}", self.base_program, self.accumulated_facts)
        };

        self.solver.solve(&full_program, 1).await
    }

    /// Resets the accumulated facts, keeping the base program.
    pub fn reset(&mut self) {
        self.accumulated_facts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solver::Atom;
    use async_trait::async_trait;
    use cqels_model::{IriTerm, LiteralTerm, Statement, Term};

    /// A mock solver that captures the program and returns a fixed answer set.
    struct MockSolver {
        response: Vec<AnswerSet>,
    }

    impl MockSolver {
        fn new(response: Vec<AnswerSet>) -> Self {
            Self { response }
        }
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

    fn sample_statement() -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new("http://example.org/alice")),
            IriTerm::new("http://example.org/name"),
            Term::Literal(LiteralTerm::new("Alice")),
        )
    }

    #[tokio::test]
    async fn test_session_solve_with_additions() {
        let mock = MockSolver::new(vec![AnswerSet::new(vec![Atom::new(
            "result",
            vec!["ok".into()],
        )])]);
        let mut session = IncrementalAspSession::new(mock, String::new());

        let delta = AspFactDelta {
            additions: vec![sample_statement()],
            deletions: vec![],
        };

        let results = session.solve_with_delta(&delta).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].atoms[0].predicate, "result");
    }

    #[tokio::test]
    async fn test_session_reset() {
        let mock = MockSolver::new(vec![]);
        let mut session = IncrementalAspSession::new(mock, "base.".into());

        let delta = AspFactDelta {
            additions: vec![sample_statement()],
            deletions: vec![],
        };

        let _ = session.solve_with_delta(&delta).await.unwrap();
        assert!(!session.accumulated_facts.is_empty());

        session.reset();
        assert!(session.accumulated_facts.is_empty());
    }

    #[tokio::test]
    async fn test_session_solve_with_deletions() {
        let mock = MockSolver::new(vec![]);
        let mut session = IncrementalAspSession::new(mock, String::new());

        let stmt = sample_statement();

        // Add first
        let delta_add = AspFactDelta {
            additions: vec![stmt.clone()],
            deletions: vec![],
        };
        let _ = session.solve_with_delta(&delta_add).await.unwrap();
        assert!(!session.accumulated_facts.is_empty());

        // Then delete
        let delta_del = AspFactDelta {
            additions: vec![],
            deletions: vec![stmt],
        };
        let _ = session.solve_with_delta(&delta_del).await.unwrap();
        assert!(
            session.accumulated_facts.is_empty() || session.accumulated_facts.trim().is_empty()
        );
    }
}
