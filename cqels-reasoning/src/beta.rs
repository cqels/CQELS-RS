use std::collections::HashMap;
use std::fmt;

use cqels_model::{Statement, Term};

use crate::alpha::AlphaNode;
use crate::production::Activation;
use crate::rule::RuleSet;
use crate::alpha::AlphaNetwork;

/// A beta node joins bindings from two alpha nodes using shared variables.
///
/// Maintains left and right memories indexed by a join key built from shared variables.
pub struct BetaNode {
    shared_variables: Vec<String>,
    left_pattern_index: usize,
    right_pattern_index: usize,
    left_memory: HashMap<String, Vec<HashMap<String, Term>>>,
    right_memory: HashMap<String, Vec<HashMap<String, Term>>>,
}

impl BetaNode {
    pub fn new(
        shared_variables: Vec<String>,
        left_pattern_index: usize,
        right_pattern_index: usize,
    ) -> Self {
        Self {
            shared_variables,
            left_pattern_index,
            right_pattern_index,
            left_memory: HashMap::new(),
            right_memory: HashMap::new(),
        }
    }

    /// Adds bindings to the left memory and joins with right memory.
    pub fn add_left(&mut self, bindings: HashMap<String, Term>) -> Vec<HashMap<String, Term>> {
        let key = self.build_join_key(&bindings);
        let results = self.join_with_right(&bindings);
        self.left_memory
            .entry(key)
            .or_default()
            .push(bindings);
        results
    }

    /// Adds bindings to the right memory and joins with left memory.
    pub fn add_right(&mut self, bindings: HashMap<String, Term>) -> Vec<HashMap<String, Term>> {
        let key = self.build_join_key(&bindings);
        let results = self.join_with_left(&bindings);
        self.right_memory
            .entry(key)
            .or_default()
            .push(bindings);
        results
    }

    fn join_with_right(&self, left_bindings: &HashMap<String, Term>) -> Vec<HashMap<String, Term>> {
        let key = self.build_join_key(left_bindings);
        if let Some(right_entries) = self.right_memory.get(&key) {
            let mut results = Vec::with_capacity(right_entries.len());
            for right in right_entries {
                if let Some(merged) = merge_bindings(left_bindings, right) {
                    results.push(merged);
                }
            }
            results
        } else {
            Vec::new()
        }
    }

    fn join_with_left(&self, right_bindings: &HashMap<String, Term>) -> Vec<HashMap<String, Term>> {
        let key = self.build_join_key(right_bindings);
        if let Some(left_entries) = self.left_memory.get(&key) {
            let mut results = Vec::with_capacity(left_entries.len());
            for left in left_entries {
                if let Some(merged) = merge_bindings(left, right_bindings) {
                    results.push(merged);
                }
            }
            results
        } else {
            Vec::new()
        }
    }

    fn build_join_key(&self, bindings: &HashMap<String, Term>) -> String {
        use std::fmt::Write;
        let mut key = String::with_capacity(self.shared_variables.len() * 32);
        for (i, var) in self.shared_variables.iter().enumerate() {
            if i > 0 {
                key.push('|');
            }
            if let Some(t) = bindings.get(var) {
                let _ = write!(key, "{t}");
            }
        }
        key
    }

    pub fn clear(&mut self) {
        self.left_memory.clear();
        self.right_memory.clear();
    }

    pub fn left_pattern_index(&self) -> usize {
        self.left_pattern_index
    }

    pub fn right_pattern_index(&self) -> usize {
        self.right_pattern_index
    }

    pub fn shared_variables(&self) -> &[String] {
        &self.shared_variables
    }

    pub fn left_memory_size(&self) -> usize {
        self.left_memory.values().map(|v| v.len()).sum()
    }

    pub fn right_memory_size(&self) -> usize {
        self.right_memory.values().map(|v| v.len()).sum()
    }
}

impl fmt::Debug for BetaNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BetaNode")
            .field("shared_variables", &self.shared_variables)
            .field("left_pattern_index", &self.left_pattern_index)
            .field("right_pattern_index", &self.right_pattern_index)
            .field("left_memory_size", &self.left_memory_size())
            .field("right_memory_size", &self.right_memory_size())
            .finish()
    }
}

/// Merges two sets of bindings. Returns `None` if they are inconsistent
/// (same variable mapped to different values).
///
/// Optimized: checks compatibility *first* without allocating, then merges
/// only if consistent.
pub fn merge_bindings(
    left: &HashMap<String, Term>,
    right: &HashMap<String, Term>,
) -> Option<HashMap<String, Term>> {
    // Fast compatibility check — no allocations
    for (key, value) in right {
        if let Some(existing) = left.get(key) {
            if existing != value {
                return None; // Inconsistent — bail early
            }
        }
    }
    // Allocate once with the right capacity
    let mut merged = HashMap::with_capacity(left.len() + right.len());
    merged.extend(left.iter().map(|(k, v)| (k.clone(), v.clone())));
    for (key, value) in right {
        if !merged.contains_key(key) {
            merged.insert(key.clone(), value.clone());
        }
    }
    Some(merged)
}

/// Entry mapping an alpha node to a rule's beta chain.
struct BetaEntry {
    rule_index: usize,
    chain_position: usize,
    is_left: bool,
}

/// The beta network coordinates join operations across rules.
///
/// For each rule, it maintains a chain of beta nodes that incrementally join
/// alpha node outputs. Single-pattern rules bypass the beta network entirely.
/// Uses rule indices (into the parent `RuleSet`) to avoid lifetime issues.
pub struct BetaNetwork {
    /// rule_index → ordered list of beta nodes for joining patterns
    rule_chains: Vec<Vec<BetaNode>>,
    /// pattern_index → list of (rule_index, chain_position, is_left) entries
    alpha_to_rule_map: HashMap<usize, Vec<BetaEntry>>,
    /// rule_index → number of patterns in that rule
    rule_pattern_counts: Vec<usize>,
}

impl BetaNetwork {
    /// Propagates alpha match results through the beta network, returning activations.
    ///
    /// Requires a reference to the `RuleSet` for filter checking.
    pub fn propagate(
        &mut self,
        alpha_node: &AlphaNode,
        bindings: HashMap<String, Term>,
        premises: Vec<Statement>,
        timestamp: i64,
        rule_set: &RuleSet,
    ) -> Vec<Activation> {
        let pattern_index = alpha_node.pattern_index();
        let mut activations = Vec::new();

        let entries: Vec<(usize, usize, bool)> = self
            .alpha_to_rule_map
            .get(&pattern_index)
            .map(|entries| {
                entries
                    .iter()
                    .map(|e| (e.rule_index, e.chain_position, e.is_left))
                    .collect()
            })
            .unwrap_or_default();

        for (rule_index, chain_position, is_left) in entries {
            let pattern_count = self.rule_pattern_counts[rule_index];

            if pattern_count == 1 {
                // Single-pattern rule: directly create activation
                let rule = &rule_set.rules()[rule_index];
                if rule.condition().passes_filters(&bindings) {
                    activations.push(Activation::new(
                        rule_index,
                        bindings.clone(),
                        premises.clone(),
                        timestamp,
                    ));
                }
                continue;
            }

            // Multi-pattern rule: feed into beta chain
            let chain = &mut self.rule_chains[rule_index];
            if chain_position >= chain.len() {
                continue;
            }

            let join_results = if is_left {
                chain[chain_position].add_left(bindings.clone())
            } else {
                chain[chain_position].add_right(bindings.clone())
            };

            // Propagate join results down the remaining chain
            for merged in join_results {
                self.propagate_down_chain(
                    rule_index,
                    chain_position + 1,
                    merged,
                    &premises,
                    timestamp,
                    &mut activations,
                    rule_set,
                );
            }
        }

        activations
    }

    #[allow(clippy::too_many_arguments)]
    fn propagate_down_chain(
        &mut self,
        rule_index: usize,
        start_position: usize,
        bindings: HashMap<String, Term>,
        premises: &[Statement],
        timestamp: i64,
        activations: &mut Vec<Activation>,
        rule_set: &RuleSet,
    ) {
        let chain_len = self.rule_chains[rule_index].len();

        if start_position >= chain_len {
            // Reached end of chain — create activation
            let rule = &rule_set.rules()[rule_index];
            if rule.condition().passes_filters(&bindings) {
                activations.push(Activation::new(
                    rule_index,
                    bindings,
                    premises.to_vec(),
                    timestamp,
                ));
            }
            return;
        }

        // Continue propagating through the chain (add to left side)
        let join_results = self.rule_chains[rule_index][start_position].add_left(bindings);
        for merged in join_results {
            self.propagate_down_chain(
                rule_index,
                start_position + 1,
                merged,
                premises,
                timestamp,
                activations,
                rule_set,
            );
        }
    }

    pub fn clear(&mut self) {
        for chain in &mut self.rule_chains {
            for node in chain {
                node.clear();
            }
        }
    }

    /// Compiles a beta network from a rule set and alpha network.
    pub fn compile(rule_set: &RuleSet, alpha_network: &AlphaNetwork) -> Self {
        let mut rule_chains = Vec::new();
        let mut alpha_to_rule_map: HashMap<usize, Vec<BetaEntry>> = HashMap::new();
        let mut rule_pattern_counts = Vec::new();

        for (rule_index, rule) in rule_set.rules().iter().enumerate() {
            let patterns = rule.condition().patterns();
            rule_pattern_counts.push(patterns.len());

            if patterns.is_empty() {
                rule_chains.push(Vec::new());
                continue;
            }

            // Find the alpha node index for each pattern
            let pattern_indices: Vec<usize> = patterns
                .iter()
                .filter_map(|p| {
                    alpha_network
                        .nodes()
                        .iter()
                        .find(|n| n.pattern() == p)
                        .map(|n| n.pattern_index())
                })
                .collect();

            if patterns.len() == 1 {
                // Single-pattern rule: alpha feeds directly to production
                rule_chains.push(Vec::new());
                if let Some(&pi) = pattern_indices.first() {
                    alpha_to_rule_map
                        .entry(pi)
                        .or_default()
                        .push(BetaEntry {
                            rule_index,
                            chain_position: 0,
                            is_left: true,
                        });
                }
                continue;
            }

            // Multi-pattern rule: create beta chain
            let mut chain = Vec::new();
            for i in 1..pattern_indices.len() {
                let left_idx = if i == 1 { pattern_indices[0] } else { usize::MAX };
                let right_idx = pattern_indices[i];

                // Find shared variables between left patterns and the new right pattern
                let left_vars: std::collections::HashSet<String> = if i == 1 {
                    patterns[0].variable_names().into_iter().collect()
                } else {
                    patterns[..i]
                        .iter()
                        .flat_map(|p| p.variable_names())
                        .collect()
                };
                let right_vars: std::collections::HashSet<String> =
                    patterns[i].variable_names().into_iter().collect();
                let shared: Vec<String> = left_vars.intersection(&right_vars).cloned().collect();

                chain.push(BetaNode::new(shared, left_idx, right_idx));
            }

            // Map first pattern's alpha to the first beta node (left side)
            alpha_to_rule_map
                .entry(pattern_indices[0])
                .or_default()
                .push(BetaEntry {
                    rule_index,
                    chain_position: 0,
                    is_left: true,
                });

            // Map subsequent patterns' alpha nodes to their beta nodes (right side)
            for (i, &pi) in pattern_indices.iter().enumerate().skip(1) {
                alpha_to_rule_map
                    .entry(pi)
                    .or_default()
                    .push(BetaEntry {
                        rule_index,
                        chain_position: i - 1,
                        is_left: false,
                    });
            }

            rule_chains.push(chain);
        }

        Self {
            rule_chains,
            alpha_to_rule_map,
            rule_pattern_counts,
        }
    }
}

impl fmt::Debug for BetaNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BetaNetwork")
            .field("rule_chains_count", &self.rule_chains.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::IriTerm;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    #[test]
    fn test_beta_node_join() {
        let mut node = BetaNode::new(vec!["x".to_string()], 0, 1);

        let mut left = HashMap::new();
        left.insert("x".to_string(), iri("http://ex.org/a"));
        left.insert("y".to_string(), iri("http://ex.org/b"));

        // No right entries yet — should produce no results
        let results = node.add_left(left);
        assert!(results.is_empty());

        // Add right with matching join key
        let mut right = HashMap::new();
        right.insert("x".to_string(), iri("http://ex.org/a"));
        right.insert("z".to_string(), iri("http://ex.org/c"));

        let results = node.add_right(right);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("x"), Some(&iri("http://ex.org/a")));
        assert_eq!(results[0].get("y"), Some(&iri("http://ex.org/b")));
        assert_eq!(results[0].get("z"), Some(&iri("http://ex.org/c")));
    }

    #[test]
    fn test_beta_node_no_match() {
        let mut node = BetaNode::new(vec!["x".to_string()], 0, 1);

        let mut left = HashMap::new();
        left.insert("x".to_string(), iri("http://ex.org/a"));
        node.add_left(left);

        let mut right = HashMap::new();
        right.insert("x".to_string(), iri("http://ex.org/different"));

        let results = node.add_right(right);
        assert!(results.is_empty());
    }

    #[test]
    fn test_merge_bindings_consistent() {
        let mut left = HashMap::new();
        left.insert("x".to_string(), iri("http://ex.org/a"));
        left.insert("y".to_string(), iri("http://ex.org/b"));

        let mut right = HashMap::new();
        right.insert("x".to_string(), iri("http://ex.org/a"));
        right.insert("z".to_string(), iri("http://ex.org/c"));

        let merged = merge_bindings(&left, &right).unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_merge_bindings_inconsistent() {
        let mut left = HashMap::new();
        left.insert("x".to_string(), iri("http://ex.org/a"));

        let mut right = HashMap::new();
        right.insert("x".to_string(), iri("http://ex.org/different"));

        assert!(merge_bindings(&left, &right).is_none());
    }

    #[test]
    fn test_beta_node_clear() {
        let mut node = BetaNode::new(vec!["x".to_string()], 0, 1);

        let mut left = HashMap::new();
        left.insert("x".to_string(), iri("http://ex.org/a"));
        node.add_left(left);

        assert_eq!(node.left_memory_size(), 1);
        node.clear();
        assert_eq!(node.left_memory_size(), 0);
        assert_eq!(node.right_memory_size(), 0);
    }
}
