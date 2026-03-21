use std::collections::HashMap;
use std::fmt;

use cqels_model::{Statement, Term};

use crate::pattern::TriplePattern;

/// An alpha node performs single-pattern matching against incoming statements.
#[derive(Clone, Debug)]
pub struct AlphaNode {
    pattern: TriplePattern,
    pattern_index: usize,
}

impl AlphaNode {
    pub fn new(pattern: TriplePattern, pattern_index: usize) -> Self {
        Self {
            pattern,
            pattern_index,
        }
    }

    /// Evaluates this node against a statement, returning bindings if matched.
    pub fn evaluate(&self, stmt: &Statement) -> Option<HashMap<String, Term>> {
        self.pattern.match_statement(stmt)
    }

    pub fn pattern(&self) -> &TriplePattern {
        &self.pattern
    }

    pub fn pattern_index(&self) -> usize {
        self.pattern_index
    }
}

impl PartialEq for AlphaNode {
    fn eq(&self, other: &Self) -> bool {
        self.pattern_index == other.pattern_index && self.pattern == other.pattern
    }
}

impl Eq for AlphaNode {}

impl std::hash::Hash for AlphaNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pattern_index.hash(state);
    }
}

/// Result of an alpha node evaluation.
#[derive(Debug)]
pub struct AlphaMatch {
    pub node: AlphaNode,
    pub bindings: HashMap<String, Term>,
}

/// The alpha network evaluates all alpha nodes against incoming statements.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::{AlphaNetwork, TriplePattern, PatternTerm};
/// use cqels_model::{Term, IriTerm, Statement};
///
/// let pattern = TriplePattern::new(
///     PatternTerm::var("s"),
///     PatternTerm::constant(Term::Iri(IriTerm::new("http://ex.org/type"))),
///     PatternTerm::var("o"),
/// );
/// let network = AlphaNetwork::compile(vec![pattern]);
/// assert_eq!(network.size(), 1);
///
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://ex.org/alice")),
///     IriTerm::new("http://ex.org/type"),
///     Term::Iri(IriTerm::new("http://ex.org/Person")),
/// );
/// let matches = network.evaluate(&stmt);
/// assert_eq!(matches.len(), 1);
/// ```
pub struct AlphaNetwork {
    nodes: Vec<AlphaNode>,
}

impl AlphaNetwork {
    pub fn new(nodes: Vec<AlphaNode>) -> Self {
        Self { nodes }
    }

    /// Evaluates a statement against all alpha nodes, returning matches.
    pub fn evaluate(&self, stmt: &Statement) -> Vec<AlphaMatch> {
        let mut matches = Vec::new();
        for node in &self.nodes {
            if let Some(bindings) = node.evaluate(stmt) {
                matches.push(AlphaMatch {
                    node: node.clone(),
                    bindings,
                });
            }
        }
        matches
    }

    pub fn nodes(&self) -> &[AlphaNode] {
        &self.nodes
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    /// Compiles an alpha network from a set of unique triple patterns.
    pub fn compile(patterns: impl IntoIterator<Item = TriplePattern>) -> Self {
        let nodes: Vec<AlphaNode> = patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| AlphaNode::new(pattern, i))
            .collect();
        Self::new(nodes)
    }
}

impl fmt::Debug for AlphaNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlphaNetwork")
            .field("nodes_count", &self.nodes.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::PatternTerm;
    use cqels_model::term::IriTerm;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    #[test]
    fn test_alpha_node_match() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let node = AlphaNode::new(pattern, 0);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Person"),
        );
        let bindings = node.evaluate(&stmt).unwrap();
        assert_eq!(bindings.get("s"), Some(&iri("http://ex.org/alice")));
    }

    #[test]
    fn test_alpha_node_no_match() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let node = AlphaNode::new(pattern, 0);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/knows"),
            iri("http://ex.org/bob"),
        );
        assert!(node.evaluate(&stmt).is_none());
    }

    #[test]
    fn test_alpha_network_multiple_matches() {
        let p1 = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::var("o"),
        );
        let p2 = TriplePattern::new(
            PatternTerm::var("x"),
            PatternTerm::var("y"),
            PatternTerm::var("z"),
        );

        let network = AlphaNetwork::compile(vec![p1, p2]);
        assert_eq!(network.size(), 2);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Person"),
        );
        let matches = network.evaluate(&stmt);
        assert_eq!(matches.len(), 2); // Both patterns match
    }

    #[test]
    fn test_alpha_network_selective_match() {
        let p1 = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let p2 = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/knows")),
            PatternTerm::var("o"),
        );

        let network = AlphaNetwork::compile(vec![p1, p2]);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Person"),
        );
        let matches = network.evaluate(&stmt);
        assert_eq!(matches.len(), 1); // Only p1 matches
        assert_eq!(matches[0].node.pattern_index(), 0);
    }

    #[test]
    fn test_alpha_node_accessors() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let node = AlphaNode::new(pattern.clone(), 7);
        assert_eq!(node.pattern_index(), 7);
        assert_eq!(*node.pattern(), pattern);
    }

    #[test]
    fn test_alpha_node_equality() {
        let p = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let n1 = AlphaNode::new(p.clone(), 0);
        let n2 = AlphaNode::new(p.clone(), 0);
        let n3 = AlphaNode::new(p, 1);
        assert_eq!(n1, n2);
        assert_ne!(n1, n3);
    }

    #[test]
    fn test_alpha_node_hash() {
        use std::collections::HashSet;
        let p = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let n1 = AlphaNode::new(p.clone(), 0);
        let n2 = AlphaNode::new(p, 0);
        let mut set = HashSet::new();
        set.insert(n1);
        set.insert(n2);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_alpha_network_empty() {
        let network = AlphaNetwork::new(vec![]);
        assert_eq!(network.size(), 0);
        assert!(network.nodes().is_empty());

        let stmt = Statement::new(
            iri("http://ex.org/a"),
            IriTerm::new("http://ex.org/p"),
            iri("http://ex.org/b"),
        );
        let matches = network.evaluate(&stmt);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_alpha_network_compile() {
        let patterns = vec![
            TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::var("o"),
            ),
            TriplePattern::new(
                PatternTerm::var("a"),
                PatternTerm::var("b"),
                PatternTerm::var("c"),
            ),
        ];
        let network = AlphaNetwork::compile(patterns);
        assert_eq!(network.size(), 2);
        assert_eq!(network.nodes()[0].pattern_index(), 0);
        assert_eq!(network.nodes()[1].pattern_index(), 1);
    }

    #[test]
    fn test_alpha_network_debug() {
        let network = AlphaNetwork::compile(vec![TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        )]);
        let debug = format!("{:?}", network);
        assert!(debug.contains("AlphaNetwork"));
        assert!(debug.contains("nodes_count"));
    }

    #[test]
    fn test_alpha_match_bindings() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let network = AlphaNetwork::compile(vec![pattern]);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/knows"),
            iri("http://ex.org/bob"),
        );
        let matches = network.evaluate(&stmt);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].bindings.get("s"),
            Some(&iri("http://ex.org/alice"))
        );
        assert_eq!(
            matches[0].bindings.get("p"),
            Some(&iri("http://ex.org/knows"))
        );
        assert_eq!(
            matches[0].bindings.get("o"),
            Some(&iri("http://ex.org/bob"))
        );
    }

    #[test]
    fn test_alpha_node_no_match_on_object() {
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let node = AlphaNode::new(pattern, 0);

        let stmt = Statement::new(
            iri("http://ex.org/alice"),
            IriTerm::new("http://ex.org/type"),
            iri("http://ex.org/Animal"), // Doesn't match Person
        );
        assert!(node.evaluate(&stmt).is_none());
    }
}
