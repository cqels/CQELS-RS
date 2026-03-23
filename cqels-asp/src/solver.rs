//! Core ASP solver trait and answer-set types.
//!
//! Defines [`Atom`], [`AnswerSet`], and the [`AspSolver`] trait that all
//! solver backends (subprocess, library-linked, etc.) must implement.

use std::fmt;

use async_trait::async_trait;

use crate::error::AspError;

/// A ground atom in an answer set, consisting of a predicate name and a list of terms.
///
/// # Examples
///
/// ```
/// use cqels_asp::Atom;
///
/// let atom = Atom::new("edge", vec!["a".into(), "b".into()]);
/// assert_eq!(atom.to_string(), "edge(a,b)");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Atom {
    /// The predicate name.
    pub predicate: String,
    /// The list of term arguments.
    pub terms: Vec<String>,
}

impl Atom {
    /// Creates a new atom with the given predicate and term arguments.
    pub fn new(predicate: impl Into<String>, terms: Vec<String>) -> Self {
        Self {
            predicate: predicate.into(),
            terms,
        }
    }

    /// Returns `true` if this atom is ground (contains no variables).
    ///
    /// Currently always returns `true` as the solver only produces ground atoms.
    pub fn is_ground(&self) -> bool {
        true
    }
}

impl fmt::Display for Atom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.terms.is_empty() {
            write!(f, "{}", self.predicate)
        } else {
            write!(f, "{}({})", self.predicate, self.terms.join(","))
        }
    }
}

/// A single answer set returned by an ASP solver.
///
/// An answer set is a set of ground atoms representing a stable model
/// of the logic program.
///
/// # Examples
///
/// ```
/// use cqels_asp::{Atom, AnswerSet};
///
/// let atoms = vec![
///     Atom::new("node", vec!["a".into()]),
///     Atom::new("edge", vec!["a".into(), "b".into()]),
/// ];
/// let answer_set = AnswerSet::new(atoms);
/// assert!(!answer_set.is_empty());
///
/// let edges = answer_set.query_predicate("edge");
/// assert_eq!(edges.len(), 1);
/// ```
#[derive(Clone, Debug, Default)]
pub struct AnswerSet {
    /// The atoms in this answer set.
    pub atoms: Vec<Atom>,
}

impl AnswerSet {
    /// Creates a new answer set from the given atoms.
    pub fn new(atoms: Vec<Atom>) -> Self {
        Self { atoms }
    }

    /// Returns `true` if this answer set contains no atoms.
    pub fn is_empty(&self) -> bool {
        self.atoms.is_empty()
    }

    /// Returns all atoms in this answer set whose predicate matches the given name.
    pub fn query_predicate(&self, predicate: &str) -> Vec<&Atom> {
        self.atoms
            .iter()
            .filter(|a| a.predicate == predicate)
            .collect()
    }
}

/// Trait for ASP solver backends.
///
/// Implementations accept a logic program as a string and return
/// zero or more answer sets.
#[async_trait]
pub trait AspSolver: Send + Sync {
    /// Solves the given ASP program and returns up to `max_models` answer sets.
    ///
    /// # Arguments
    ///
    /// * `program` -- The logic program in ASP-Core-2 syntax.
    /// * `max_models` -- Maximum number of answer sets to enumerate (0 = all).
    async fn solve(&self, program: &str, max_models: usize) -> Result<Vec<AnswerSet>, AspError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atom_display_with_terms() {
        let atom = Atom::new("edge", vec!["a".into(), "b".into()]);
        assert_eq!(atom.to_string(), "edge(a,b)");
    }

    #[test]
    fn test_atom_display_no_terms() {
        let atom = Atom::new("fact", vec![]);
        assert_eq!(atom.to_string(), "fact");
    }

    #[test]
    fn test_atom_is_ground() {
        let atom = Atom::new("p", vec!["x".into()]);
        assert!(atom.is_ground());
    }

    #[test]
    fn test_answer_set_empty() {
        let empty = AnswerSet::default();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_answer_set_query_predicate() {
        let answer_set = AnswerSet::new(vec![
            Atom::new("node", vec!["a".into()]),
            Atom::new("edge", vec!["a".into(), "b".into()]),
            Atom::new("node", vec!["b".into()]),
        ]);

        let nodes = answer_set.query_predicate("node");
        assert_eq!(nodes.len(), 2);

        let edges = answer_set.query_predicate("edge");
        assert_eq!(edges.len(), 1);

        let missing = answer_set.query_predicate("missing");
        assert!(missing.is_empty());
    }
}
