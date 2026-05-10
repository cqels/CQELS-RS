//! Compile-time self-join detection.
//!
//! Mirrors Java PR #25's optimization where two stream patterns over the
//! same stream sharing at least one variable are rewritten to use an
//! indexed hash join instead of a nested-loop cross-product.
//!
//! This module currently produces *hints* — descriptive metadata that
//! downstream runtime code can use to substitute
//! [`crate::operator::join::WindowedSelfJoinState`] for the default
//! nested-loop join. Auto-substitution inside `pipeline.rs` is a
//! follow-up; see [`detect_self_joins`] for the detection algorithm.

use std::collections::{HashMap, HashSet};

use crate::parser::ast::{CqelsPatternGroup, CqelsQueryDefinition, TriplePattern};

/// One detected self-join between two stream patterns over the same stream.
///
/// `join_keys` lists the variable names (without the leading `?`/`$`)
/// shared by both sides. `pattern_indices` are the positions in
/// `query_def.pattern_groups` where the two `Stream` groups live, in
/// declaration order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelfJoinHint {
    pub source: String,
    pub join_keys: Vec<String>,
    pub pattern_indices: (usize, usize),
}

/// Detects compile-time self-join opportunities in the query's WHERE clause.
///
/// A self-join is reported when two `Stream { source, patterns }` pattern
/// groups share the same `source` and at least one variable that appears
/// in both sides' triple patterns.
///
/// Time complexity: O(N²) over the number of Stream pattern groups in the
/// query, which is typically tiny.
pub fn detect_self_joins(query_def: &CqelsQueryDefinition) -> Vec<SelfJoinHint> {
    let stream_groups: Vec<(usize, &String, &Vec<TriplePattern>)> = query_def
        .pattern_groups
        .iter()
        .enumerate()
        .filter_map(|(i, group)| match group {
            CqelsPatternGroup::Stream { source, patterns } => Some((i, source, patterns)),
            _ => None,
        })
        .collect();

    let mut hints = Vec::new();
    for (i, &(idx_l, src_l, pats_l)) in stream_groups.iter().enumerate() {
        for &(idx_r, src_r, pats_r) in stream_groups.iter().skip(i + 1) {
            if src_l != src_r {
                continue;
            }
            let vars_l = collect_variables_in_patterns(pats_l);
            let vars_r = collect_variables_in_patterns(pats_r);
            let mut shared: Vec<String> = vars_l.intersection(&vars_r).cloned().collect();
            shared.sort();
            if shared.is_empty() {
                continue;
            }
            hints.push(SelfJoinHint {
                source: src_l.clone(),
                join_keys: shared,
                pattern_indices: (idx_l, idx_r),
            });
        }
    }
    hints
}

fn collect_variables_in_patterns(patterns: &[TriplePattern]) -> HashSet<String> {
    let mut vars = HashSet::new();
    for tp in patterns {
        for term in [&tp.subject, &tp.predicate, &tp.object] {
            if is_variable(term) {
                vars.insert(strip_var_prefix(term));
            }
        }
    }
    vars
}

fn is_variable(term: &str) -> bool {
    term.starts_with('?') || term.starts_with('$')
}

fn strip_var_prefix(term: &str) -> String {
    term.trim_start_matches(['?', '$']).to_string()
}

/// Bins detected self-joins by stream source — useful when the same stream
/// is involved in multiple self-joins (e.g. a 3-way self-join).
pub fn self_join_hints_by_source(
    query_def: &CqelsQueryDefinition,
) -> HashMap<String, Vec<SelfJoinHint>> {
    let mut grouped: HashMap<String, Vec<SelfJoinHint>> = HashMap::new();
    for hint in detect_self_joins(query_def) {
        grouped.entry(hint.source.clone()).or_default().push(hint);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::CqelsQlParser;

    fn parse(query: &str) -> CqelsQueryDefinition {
        CqelsQlParser::parse(query).expect("parse")
    }

    fn explicit(query: &str) -> CqelsQueryDefinition {
        // Use the parser path; queries are written with explicit STREAM { ... }
        // blocks so implicit-binding doesn't interfere.
        parse(query)
    }

    #[test]
    fn detects_two_stream_blocks_on_same_source_with_shared_var() {
        let query = r#"
            SELECT ?driver ?passenger
            FROM STREAM rides [RANGE 10s]
            WHERE {
                STREAM rides { ?driver <http://ex.org/in> ?car . }
                STREAM rides { ?passenger <http://ex.org/in> ?car . }
            }
        "#;
        let def = explicit(query);
        let hints = detect_self_joins(&def);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].source, "rides");
        assert!(hints[0].join_keys.contains(&"car".to_string()));
    }

    #[test]
    fn ignores_streams_on_different_sources() {
        let query = r#"
            SELECT ?a ?b
            FROM STREAM s1 [RANGE 5s]
            FROM STREAM s2 [RANGE 5s]
            WHERE {
                STREAM s1 { ?a <http://ex.org/p> ?x . }
                STREAM s2 { ?b <http://ex.org/p> ?x . }
            }
        "#;
        let def = explicit(query);
        assert!(detect_self_joins(&def).is_empty());
    }

    #[test]
    fn ignores_same_source_without_shared_variable() {
        let query = r#"
            SELECT ?a ?b
            FROM STREAM events [RANGE 5s]
            WHERE {
                STREAM events { ?a <http://ex.org/p> "left" . }
                STREAM events { ?b <http://ex.org/q> "right" . }
            }
        "#;
        let def = explicit(query);
        assert!(
            detect_self_joins(&def).is_empty(),
            "no shared variable → Cartesian, not a self-join"
        );
    }

    #[test]
    fn returns_empty_for_single_stream_block() {
        let query = r#"
            SELECT ?a ?b
            FROM STREAM s [RANGE 5s]
            WHERE {
                STREAM s { ?a <http://ex.org/p> ?b . }
            }
        "#;
        let def = explicit(query);
        assert!(detect_self_joins(&def).is_empty());
    }

    #[test]
    fn detects_three_way_self_join_pairwise() {
        // 3 stream blocks on same source pair-wise share variables — should
        // report all 3 pairs.
        let query = r#"
            SELECT ?a ?b ?c
            FROM STREAM rides [RANGE 10s]
            WHERE {
                STREAM rides { ?a <http://ex.org/in> ?car . }
                STREAM rides { ?b <http://ex.org/in> ?car . }
                STREAM rides { ?c <http://ex.org/in> ?car . }
            }
        "#;
        let def = explicit(query);
        let hints = detect_self_joins(&def);
        assert_eq!(hints.len(), 3, "C(3,2) = 3 pairs expected");
    }

    #[test]
    fn detects_multiple_shared_variables() {
        let query = r#"
            SELECT ?a ?b
            FROM STREAM events [RANGE 5s]
            WHERE {
                STREAM events { ?a <http://ex.org/x> ?u . ?a <http://ex.org/y> ?v . }
                STREAM events { ?b <http://ex.org/x> ?u . ?b <http://ex.org/y> ?v . }
            }
        "#;
        let def = explicit(query);
        let hints = detect_self_joins(&def);
        assert_eq!(hints.len(), 1);
        let keys: std::collections::HashSet<_> = hints[0].join_keys.iter().cloned().collect();
        assert!(keys.contains("u"));
        assert!(keys.contains("v"));
    }

    #[test]
    fn binned_by_source_groups_three_way_join() {
        let query = r#"
            SELECT ?a ?b ?c
            FROM STREAM rides [RANGE 10s]
            WHERE {
                STREAM rides { ?a <http://ex.org/in> ?car . }
                STREAM rides { ?b <http://ex.org/in> ?car . }
                STREAM rides { ?c <http://ex.org/in> ?car . }
            }
        "#;
        let def = explicit(query);
        let grouped = self_join_hints_by_source(&def);
        assert_eq!(grouped.get("rides").map(|v| v.len()), Some(3));
    }
}
