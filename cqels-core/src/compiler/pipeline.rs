//! Shared pipeline utilities for query compilation and execution.
//!
//! This module provides the building blocks used by both
//! [`CompiledCqelsQuery`](super::compiled::CompiledCqelsQuery) and
//! [`CompiledCypherQuery`](super::compiled::CompiledCypherQuery) during
//! query execution. The pipeline processes a stream of [`BindingSet`]
//! values through the following phases:
//!
//! 1. **Pattern matching** — [`match_triple_pattern`] / [`match_cypher_pattern`]:
//!    Matches incoming RDF statements against query patterns, producing initial
//!    variable bindings.
//! 2. **Filtering** — [`apply_filters`]: Evaluates FILTER/WHERE expressions
//!    and drops non-matching bindings.
//! 3. **Binding** — [`apply_binds`]: Computes BIND/RETURN expressions and
//!    inserts new variable values.
//! 4. **OPTIONAL** — [`apply_optional`]: Left-outer-join with optional pattern
//!    groups. Extends bindings when patterns match, passes through unchanged
//!    otherwise.
//! 5. **UNION** — [`apply_union`]: Duplicates bindings across matching union
//!    branches.
//! 6. **MINUS** — [`apply_minus`]: Anti-join that removes bindings matching
//!    MINUS patterns.
//! 7. **Aggregation** — [`apply_group_by_aggregates`]: Groups bindings by
//!    GROUP BY keys and applies aggregate functions (SUM, COUNT, AVG, etc.).
//! 8. **Ordering & limiting** — [`apply_order_and_limit`]: Sorts results by
//!    ORDER BY expressions and applies LIMIT.
//! 9. **Projection** — [`apply_projection`]: Retains only SELECT/RETURN
//!    variables, discarding internal bindings.
//! 10. **Distinct** — [`apply_distinct`]: Deduplicates result bindings.
//!
//! Each function operates on `Pin<Box<dyn Stream<Item = BindingSet> + Send>>`
//! and returns the same type, allowing composable pipeline construction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;

use futures::{Stream, StreamExt};

use cqels_model::term::IriTerm;
use cqels_model::{BindingSet, Statement, Term, Value};

use crate::expression::ast::{AggregateExprFunction, Expression};
use crate::expression::evaluator::ExpressionEvaluator;
use crate::parser::ast::{
    AggregateFunction, CqelsPatternGroup, CypherPattern, NodePattern, RelDirection, SortDirection,
    TriplePattern,
};

/// Specification for an aggregate in the pipeline.
#[derive(Clone, Debug)]
pub struct PipelineAggregateSpec {
    pub function: AggregateExprFunction,
    pub argument: Expression,
    pub alias: String,
    pub distinct: bool,
    /// Optional separator for GROUP_CONCAT (default ",").
    pub separator: Option<String>,
}

/// Attempts to match a triple pattern against a statement, producing
/// a `BindingSet` with variable bindings if successful.
///
/// Variables in the pattern (starting with `?` or `$`) are bound to the
/// corresponding statement components. Constants must match exactly.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use cqels_core::compiler::pipeline::match_triple_pattern;
/// use cqels_core::parser::ast::TriplePattern;
/// use cqels_model::{Statement, Term, Value};
/// use cqels_model::term::IriTerm;
///
/// let pattern = TriplePattern {
///     subject: "?s".into(),
///     predicate: "<http://ex.org/type>".into(),
///     object: "?o".into(),
/// };
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://ex.org/alice")),
///     IriTerm::new("http://ex.org/type"),
///     Term::Iri(IriTerm::new("http://ex.org/Person")),
/// );
/// let prefixes = HashMap::new();
/// let bs = match_triple_pattern(&pattern, &stmt, &prefixes, 1000).unwrap();
/// assert!(bs.get("s").is_some());
/// assert!(bs.get("o").is_some());
/// ```
pub fn match_triple_pattern(
    pattern: &crate::parser::ast::TriplePattern,
    stmt: &Statement,
    prefixes: &HashMap<String, String>,
    timestamp: i64,
) -> Option<BindingSet> {
    let mut bindings = BindingSet::new(timestamp);

    // Match subject
    if !match_term_component(&pattern.subject, &stmt.subject, prefixes, &mut bindings) {
        return None;
    }

    // Match predicate
    let pred_term = Term::Iri(IriTerm::new(stmt.predicate.as_str()));
    if !match_term_component(&pattern.predicate, &pred_term, prefixes, &mut bindings) {
        return None;
    }

    // Match object
    if !match_term_component(&pattern.object, &stmt.object, prefixes, &mut bindings) {
        return None;
    }

    Some(bindings)
}

/// Matches a single pattern component against a statement term.
///
/// Returns `true` if the match succeeds and variable bindings are added.
fn match_term_component(
    pattern_term: &str,
    actual_term: &Term,
    prefixes: &HashMap<String, String>,
    bindings: &mut BindingSet,
) -> bool {
    if is_variable(pattern_term) {
        let var_name = pattern_term
            .strip_prefix('?')
            .or_else(|| pattern_term.strip_prefix('$'))
            .unwrap_or(pattern_term);

        let value = term_to_value(actual_term);

        // Check consistency: if already bound, must match
        if let Some(existing) = bindings.get(var_name) {
            return *existing == value;
        }

        bindings.insert(var_name, value);
        true
    } else {
        // Constant: must match the actual term
        let resolved = resolve_term(pattern_term, prefixes);
        match_constant(&resolved, actual_term)
    }
}

/// Checks if a pattern term string is a variable.
fn is_variable(term: &str) -> bool {
    term.starts_with('?') || term.starts_with('$')
}

/// Converts an RDF Term to a [`Value`], lifting typed RDF literals to native
/// numeric/boolean/string variants (xsd:integer/int/long → `Value::Integer`,
/// xsd:double/float/decimal → `Value::Float`, xsd:boolean → `Value::Boolean`,
/// otherwise `Value::String`; IRIs and blank nodes stay `Value::Term`).
///
/// This is the binding-construction lifter used throughout pattern matching, so
/// the bindings reaching the expression evaluator hold native `Value` variants
/// rather than `Value::Term(Literal)`. Exposed `pub` so the parity
/// expression-evaluation adapter can build binding sets the same way production
/// does (otherwise arithmetic on RDF-typed bindings spuriously yields null).
pub fn term_to_value(term: &Term) -> Value {
    match term {
        Term::Iri(iri) => Value::Term(Term::Iri(iri.clone())),
        Term::BlankNode(bn) => Value::Term(Term::BlankNode(bn.clone())),
        Term::Literal(lit) => {
            // Lift only literals whose DATATYPE is numeric/boolean. A plain or
            // xsd:string literal stays a string even when its lexical form
            // parses as a number: per SPARQL 1.1 §17.2.2 the EBV of "0" is
            // true (nonempty), and arithmetic on it is a type error — lifting
            // it to Integer(0) silently changed both. (parity unit 5)
            let val = lit.value();
            if let Some(dt) = lit.datatype() {
                match dt {
                    // The full XSD integer-derived hierarchy (XSD 1.1 §3.4) —
                    // codex P2 on cqels-rs#94: enumerating only integer/int/long
                    // dropped xsd:short, xsd:byte, the (non)Negative/Positive
                    // family, and the unsigned family to Value::String, breaking
                    // FILTER/arithmetic over standard stream datatypes. Values
                    // whose magnitude exceeds i64 (e.g. large xsd:unsignedLong)
                    // fall through to String, same as out-of-range xsd:integer.
                    "http://www.w3.org/2001/XMLSchema#integer"
                    | "http://www.w3.org/2001/XMLSchema#int"
                    | "http://www.w3.org/2001/XMLSchema#long"
                    | "http://www.w3.org/2001/XMLSchema#short"
                    | "http://www.w3.org/2001/XMLSchema#byte"
                    | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
                    | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
                    | "http://www.w3.org/2001/XMLSchema#negativeInteger"
                    | "http://www.w3.org/2001/XMLSchema#positiveInteger"
                    | "http://www.w3.org/2001/XMLSchema#unsignedLong"
                    | "http://www.w3.org/2001/XMLSchema#unsignedInt"
                    | "http://www.w3.org/2001/XMLSchema#unsignedShort"
                    | "http://www.w3.org/2001/XMLSchema#unsignedByte" => {
                        if let Ok(i) = val.parse::<i64>() {
                            return Value::Integer(i);
                        }
                    }
                    "http://www.w3.org/2001/XMLSchema#double"
                    | "http://www.w3.org/2001/XMLSchema#float"
                    | "http://www.w3.org/2001/XMLSchema#decimal" => {
                        if let Ok(f) = val.parse::<f64>() {
                            return Value::Float(f);
                        }
                    }
                    "http://www.w3.org/2001/XMLSchema#boolean" => {
                        return Value::Boolean(val == "true" || val == "1");
                    }
                    _ => {}
                }
            }
            Value::String(val.to_string())
        }
        _ => Value::Null,
    }
}

/// Resolves a pattern term (IRI or prefixed name) using prefix mappings.
fn resolve_term(term: &str, prefixes: &HashMap<String, String>) -> String {
    // Handle 'a' → rdf:type
    if term == "a" {
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string();
    }

    // Handle full IRI
    if term.starts_with('<') && term.ends_with('>') {
        return term[1..term.len() - 1].to_string();
    }

    // Handle prefixed name
    if let Some(colon_pos) = term.find(':') {
        let prefix = &term[..colon_pos];
        let local = &term[colon_pos + 1..];
        if let Some(uri) = prefixes.get(prefix) {
            return format!("{uri}{local}");
        }
    }

    term.to_string()
}

/// Checks if a resolved constant matches an RDF term.
fn match_constant(resolved: &str, term: &Term) -> bool {
    match term {
        Term::Iri(iri) => iri.as_str() == resolved,
        Term::Literal(lit) => lit.value() == resolved,
        Term::BlankNode(bn) => bn.id() == resolved,
        _ => false,
    }
}

/// Applies FILTER expressions to a stream of binding sets.
///
/// Each binding set is retained only if **all** filter expressions evaluate
/// to `true`. Filters that reference unbound variables evaluate to `false`.
pub fn apply_filters(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    filters: &[Expression],
    evaluator: &ExpressionEvaluator,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let filters = filters.to_vec();
    let evaluator = evaluator.clone();

    Box::pin(stream.filter(move |bs| {
        let pass = filters.iter().all(|f| evaluator.evaluate_as_bool(f, bs));
        futures::future::ready(pass)
    }))
}

/// Applies BIND expressions to a stream of binding sets.
///
/// For each `(expression, variable)` pair, evaluates the expression against
/// the current bindings and inserts the result under the given variable name.
pub fn apply_binds(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    binds: &[(Expression, String)],
    evaluator: &ExpressionEvaluator,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let binds = binds.to_vec();
    let evaluator = evaluator.clone();

    Box::pin(stream.map(move |mut bs| {
        for (expr, var) in &binds {
            let val = evaluator.evaluate(expr, &bs);
            let var_name = var
                .strip_prefix('?')
                .or_else(|| var.strip_prefix('$'))
                .unwrap_or(var);
            bs.insert(var_name, val);
        }
        bs
    }))
}

/// Applies OPTIONAL pattern groups (left-outer-join semantics).
///
/// For each incoming `BindingSet`, tries to match optional patterns against
/// current bindings. If patterns match, extends the `BindingSet` with new
/// variable bindings. If not, passes through unchanged.
///
/// # Streaming-mode limitation
///
/// In streaming mode without a materialized dataset, OPTIONAL matching
/// checks variable overlap only — it verifies that the optional pattern
/// shares at least one bound variable with the current bindings.
/// Constant values in patterns are not checked against actual data.
/// When an RDF store is available, static patterns should be resolved
/// there first (see Phase 1b in the compiled query pipeline).
pub fn apply_optional(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    optional_groups: &[Vec<CqelsPatternGroup>],
    prefixes: &HashMap<String, String>,
    evaluator: &ExpressionEvaluator,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let optional_groups = optional_groups.to_vec();
    let prefixes = prefixes.clone();
    let evaluator = evaluator.clone();

    Box::pin(stream.map(move |bs| {
        let mut result = bs;
        for optional_block in &optional_groups {
            result = try_match_optional_block(&result, optional_block, &prefixes, &evaluator);
        }
        result
    }))
}

/// Tries to match a single OPTIONAL block against a binding set.
/// Returns the extended binding set if patterns match, or the original unchanged.
fn try_match_optional_block(
    bs: &BindingSet,
    groups: &[CqelsPatternGroup],
    prefixes: &HashMap<String, String>,
    evaluator: &ExpressionEvaluator,
) -> BindingSet {
    // Collect triple patterns and filters/binds from the optional block
    let mut triple_patterns = Vec::new();
    let mut filter_exprs = Vec::new();
    let mut bind_exprs = Vec::new();

    for group in groups {
        match group {
            CqelsPatternGroup::Stream { patterns, .. }
            | CqelsPatternGroup::Static { patterns }
            | CqelsPatternGroup::Default { patterns }
            | CqelsPatternGroup::NamedGraph { patterns, .. } => {
                triple_patterns.extend(patterns.iter().cloned());
            }
            CqelsPatternGroup::Filter { expression } => {
                if let Ok(expr) =
                    crate::expression::parser::ExpressionParser::parse_cqelsql(expression)
                {
                    filter_exprs.push(expr);
                }
            }
            CqelsPatternGroup::Bind {
                expression,
                variable,
            } => {
                if let Ok(expr) =
                    crate::expression::parser::ExpressionParser::parse_cqelsql(expression)
                {
                    bind_exprs.push((expr, variable.clone()));
                }
            }
            _ => {}
        }
    }

    // Try to match triple patterns against current bindings.
    // For intra-element OPTIONAL, we check if the current bindings already
    // satisfy the optional patterns (variable consistency check).
    let mut extended = bs.clone();
    let mut all_matched = true;

    for pattern in &triple_patterns {
        if !try_satisfy_pattern_from_bindings(pattern, &extended, prefixes) {
            all_matched = false;
            break;
        }
    }

    if all_matched && !triple_patterns.is_empty() {
        // Apply filters from the optional block
        let filters_pass = filter_exprs
            .iter()
            .all(|f| evaluator.evaluate_as_bool(f, &extended));
        if filters_pass {
            // Apply binds from the optional block
            for (expr, var) in &bind_exprs {
                let val = evaluator.evaluate(expr, &extended);
                let var_name = var
                    .strip_prefix('?')
                    .or_else(|| var.strip_prefix('$'))
                    .unwrap_or(var);
                extended.insert(var_name, val);
            }
            return extended;
        }
    }

    // Pattern didn't match — return original bindings unchanged
    bs.clone()
}

/// Checks if a triple pattern can be satisfied from existing bindings.
///
/// Returns `true` only when at least one variable in the pattern is already
/// bound in the binding set (i.e. the optional pattern is "connected" to the
/// main query via a shared variable).  Disconnected optional patterns (no
/// shared variables) return `false`, which causes the caller to pass through
/// the original bindings unchanged — correct left-outer-join semantics.
///
/// # Streaming-mode limitation
///
/// Only variable presence is checked, not actual data matching against a
/// materialized dataset. This is correct for pure streaming mode where no
/// stored triples exist to query against.
fn try_satisfy_pattern_from_bindings(
    pattern: &TriplePattern,
    bs: &BindingSet,
    _prefixes: &HashMap<String, String>,
) -> bool {
    let components = [&pattern.subject, &pattern.predicate, &pattern.object];
    let mut has_shared_variable = false;

    for comp in &components {
        if is_variable(comp) {
            let var_name = comp
                .strip_prefix('?')
                .or_else(|| comp.strip_prefix('$'))
                .unwrap_or(comp);
            if bs.get(var_name).is_some() {
                has_shared_variable = true;
            }
        }
        // Constants are checked during actual pattern matching, not here
    }

    has_shared_variable
}

/// Applies UNION blocks to a stream.
///
/// For each incoming `BindingSet`, tries matching both left and right pattern
/// branches. Emits results from either or both branches (true union).
///
/// # Streaming-mode limitation
///
/// Branch matching uses variable-overlap heuristics (`check_pattern_consistency`)
/// rather than evaluating against a materialized dataset. Each branch "matches"
/// if its patterns share at least one bound variable with the current bindings.
pub fn apply_union(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    union_blocks: &[(Vec<CqelsPatternGroup>, Vec<CqelsPatternGroup>)],
    prefixes: &HashMap<String, String>,
    evaluator: &ExpressionEvaluator,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let union_blocks = union_blocks.to_vec();
    let prefixes = prefixes.clone();
    let evaluator = evaluator.clone();

    Box::pin(stream.flat_map(move |bs| {
        let mut results = Vec::new();

        for (left_patterns, right_patterns) in &union_blocks {
            let left_match = try_match_pattern_groups(&bs, left_patterns, &prefixes, &evaluator);
            let right_match = try_match_pattern_groups(&bs, right_patterns, &prefixes, &evaluator);

            match (left_match, right_match) {
                (Some(l), Some(r)) => {
                    results.push(l);
                    results.push(r);
                }
                (Some(l), None) => results.push(l),
                (None, Some(r)) => results.push(r),
                (None, None) => {
                    // Neither branch matched — pass through original
                    results.push(bs.clone());
                }
            }
        }

        if results.is_empty() {
            results.push(bs.clone());
        }

        futures::stream::iter(results)
    }))
}

/// Tries to match a set of pattern groups against current bindings.
fn try_match_pattern_groups(
    bs: &BindingSet,
    groups: &[CqelsPatternGroup],
    prefixes: &HashMap<String, String>,
    evaluator: &ExpressionEvaluator,
) -> Option<BindingSet> {
    let mut result = bs.clone();
    let mut has_patterns = false;

    for group in groups {
        match group {
            CqelsPatternGroup::Stream { patterns, .. }
            | CqelsPatternGroup::Static { patterns }
            | CqelsPatternGroup::Default { patterns }
            | CqelsPatternGroup::NamedGraph { patterns, .. } => {
                has_patterns = true;
                for pattern in patterns {
                    if !check_pattern_consistency(pattern, &result, prefixes) {
                        return None;
                    }
                }
            }
            CqelsPatternGroup::Filter { expression } => {
                if let Ok(expr) =
                    crate::expression::parser::ExpressionParser::parse_cqelsql(expression)
                {
                    if !evaluator.evaluate_as_bool(&expr, &result) {
                        return None;
                    }
                }
            }
            CqelsPatternGroup::Bind {
                expression,
                variable,
            } => {
                if let Ok(expr) =
                    crate::expression::parser::ExpressionParser::parse_cqelsql(expression)
                {
                    let val = evaluator.evaluate(&expr, &result);
                    let var_name = variable
                        .strip_prefix('?')
                        .or_else(|| variable.strip_prefix('$'))
                        .unwrap_or(variable);
                    result.insert(var_name, val);
                }
            }
            _ => {}
        }
    }

    if has_patterns {
        Some(result)
    } else {
        None
    }
}

/// Checks that a pattern is connected to the current bindings.
///
/// Returns `true` only when the pattern shares at least one bound variable
/// with the binding set.  This ensures UNION branches only "match" when
/// they are connected to the main query's bindings.
///
/// Note: in streaming mode without a separate dataset, we cannot verify
/// constant values against actual data — we only check variable overlap.
fn check_pattern_consistency(
    pattern: &TriplePattern,
    bs: &BindingSet,
    _prefixes: &HashMap<String, String>,
) -> bool {
    let components = [&pattern.subject, &pattern.predicate, &pattern.object];
    let mut has_shared_variable = false;

    for comp in &components {
        if is_variable(comp) {
            let var_name = comp
                .strip_prefix('?')
                .or_else(|| comp.strip_prefix('$'))
                .unwrap_or(comp);
            if bs.get(var_name).is_some() {
                has_shared_variable = true;
            }
        }
    }

    has_shared_variable
}

/// Applies MINUS blocks to a stream (anti-join semantics).
///
/// For each incoming `BindingSet`, checks if the MINUS patterns can be
/// satisfied. If they match, the binding set is **filtered out**.
///
/// # Streaming-mode limitation
///
/// MINUS matching uses variable-overlap heuristics — a binding is excluded
/// if it shares at least one variable with the MINUS pattern. Actual value
/// comparison against a MINUS dataset is deferred to when multi-dataset
/// support is added.
pub fn apply_minus(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    minus_blocks: &[Vec<TriplePattern>],
    prefixes: &HashMap<String, String>,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let minus_blocks = minus_blocks.to_vec();
    let prefixes = prefixes.clone();

    Box::pin(stream.filter(move |bs| {
        // Keep the binding set only if NONE of the minus blocks match
        let should_keep = !minus_blocks
            .iter()
            .any(|patterns| minus_patterns_match(patterns, bs, &prefixes));
        futures::future::ready(should_keep)
    }))
}

/// Checks if MINUS patterns match against a binding set.
///
/// In streaming MINUS (without a separate dataset to join against), the
/// semantic is: a MINUS block "matches" when the binding set shares at
/// least one variable with the MINUS pattern.  This causes the binding
/// to be excluded — any solution that is "compatible" with the MINUS
/// pattern (i.e. has overlapping variable names) is filtered out.
///
/// When no variables are shared the MINUS block does not apply and the
/// binding passes through, which is correct per SPARQL semantics (MINUS
/// only removes solutions where shared variables have compatible values,
/// and with no shared variables there is nothing to compare).
fn minus_patterns_match(
    patterns: &[TriplePattern],
    bs: &BindingSet,
    _prefixes: &HashMap<String, String>,
) -> bool {
    for pattern in patterns {
        let components = [&pattern.subject, &pattern.predicate, &pattern.object];
        let mut has_shared_variable = false;
        // Track whether all shared variables are compatible.
        // Currently always true in streaming mode (presence = compatibility),
        // but kept mutable for future multi-dataset MINUS support where
        // actual value comparison would set this to false on mismatch.
        #[allow(unused_mut)]
        let mut all_shared_match = true;

        for comp in &components {
            if is_variable(comp) {
                let var_name = comp
                    .strip_prefix('?')
                    .or_else(|| comp.strip_prefix('$'))
                    .unwrap_or(comp);
                if bs.get(var_name).is_some() {
                    has_shared_variable = true;
                    // In streaming mode the variable is present — that counts
                    // as compatible. With a separate MINUS dataset, we would
                    // compare the bound value against the dataset value here
                    // and set `all_shared_match = false` on mismatch.
                }
            }
            // Constants in the MINUS pattern are not compared against bound
            // variables in streaming mode — they describe the MINUS dataset
            // which is not materialised here.
        }

        if has_shared_variable && all_shared_match {
            return true;
        }
    }
    false
}

/// Matches a Cypher graph pattern against a set of RDF statements,
/// producing a `BindingSet` with all node/relationship variable bindings.
///
/// Implements recursive chain matching: for each relationship in the pattern,
/// finds a matching statement, then recursively matches the rest of the chain.
pub fn match_cypher_pattern(
    pattern: &CypherPattern,
    stmts: &[Statement],
    timestamp: i64,
) -> Vec<BindingSet> {
    if pattern.relationships.is_empty() {
        // No relationships — just single node binding
        if pattern.nodes.is_empty() {
            return vec![];
        }
        // For a single node with no relationships, bind from any statement's subject
        let mut results = Vec::new();
        for stmt in stmts {
            let mut bs = BindingSet::new(timestamp);
            if let Some(node) = pattern.nodes.first() {
                if let Some(var) = &node.variable {
                    bs.insert(var.as_str(), term_to_value(&stmt.subject));
                    // Check node property constraints
                    if check_node_properties(node, &bs) {
                        results.push(bs);
                    }
                }
            }
        }
        return results;
    }

    // Build a map of node patterns by variable name
    let node_map: HashMap<String, &NodePattern> = pattern
        .nodes
        .iter()
        .filter_map(|n| n.variable.as_ref().map(|v| (v.clone(), n)))
        .collect();

    // Recursive chain matching
    let mut results = Vec::new();
    let initial_bs = BindingSet::new(timestamp);
    match_chain_recursive(
        &pattern.relationships,
        0,
        &initial_bs,
        stmts,
        &node_map,
        &pattern.nodes,
        timestamp,
        &mut results,
    );

    results
}

/// Recursively matches relationship chain patterns against statements.
#[allow(clippy::too_many_arguments, clippy::only_used_in_recursion)]
fn match_chain_recursive(
    relationships: &[crate::parser::ast::RelationshipPattern],
    rel_idx: usize,
    current_bs: &BindingSet,
    stmts: &[Statement],
    node_map: &HashMap<String, &NodePattern>,
    nodes: &[NodePattern],
    timestamp: i64,
    results: &mut Vec<BindingSet>,
) {
    if rel_idx >= relationships.len() {
        // All relationships matched — extract node properties and emit result
        let mut final_bs = current_bs.clone();
        let snapshot = final_bs.clone();
        extract_node_properties(&snapshot, stmts, node_map, &mut final_bs);
        results.push(final_bs);
        return;
    }

    let rel = &relationships[rel_idx];

    // Variable-length path handling
    if let Some(ref path_length) = rel.path_length {
        use crate::operator::join::{GraphPatternJoinState, VariableLengthPathOperator};

        let min = path_length.min_length.unwrap_or(1) as usize;
        let max = path_length.max_length.unwrap_or(10) as usize;

        let mut graph = GraphPatternJoinState::new();
        for stmt in stmts {
            graph.add_statement(stmt, timestamp);
        }

        let mut op = VariableLengthPathOperator::new(min, max);
        if !rel.types.is_empty() {
            // Find the full predicate matching the type suffix (same logic as single-hop).
            // GraphPatternJoinState stores predicate via to_string() (e.g., "<http://...>"),
            // so we must resolve the same form.
            let type_name = &rel.types[0];
            if let Some(full_pred) = stmts.iter().find_map(|s| {
                let p = s.predicate.as_str();
                if p.ends_with(type_name.as_str()) || p == type_name.as_str() {
                    Some(s.predicate.to_string())
                } else {
                    None
                }
            }) {
                op = op.with_relationship_type(full_pred);
            } else {
                // No matching predicate found — no results possible
                return;
            }
        }

        // Determine start nodes
        let start_var = rel
            .start_node
            .as_ref()
            .or_else(|| nodes.get(rel_idx).and_then(|n| n.variable.as_ref()));
        let end_var = rel
            .end_node
            .as_ref()
            .or_else(|| nodes.get(rel_idx + 1).and_then(|n| n.variable.as_ref()));

        let start_nodes: Vec<String> = if let Some(sv) = start_var {
            if let Some(existing) = current_bs.get(sv) {
                vec![existing.to_string()]
            } else {
                graph.all_subjects()
            }
        } else {
            graph.all_subjects()
        };

        for start in &start_nodes {
            let paths = op.find_paths(start, &graph);
            for path in paths {
                if path.len() < 2 {
                    continue;
                }
                let mut bs = current_bs.clone();
                let path_start = &path[0];
                let path_end = &path[path.len() - 1];

                if let Some(sv) = start_var {
                    if let Some(existing) = bs.get(sv) {
                        if existing.to_string() != *path_start {
                            continue;
                        }
                    } else {
                        bs.insert(sv.as_str(), Value::String(path_start.clone()));
                    }
                }
                if let Some(ev) = end_var {
                    if let Some(existing) = bs.get(ev) {
                        if existing.to_string() != *path_end {
                            continue;
                        }
                    } else {
                        bs.insert(ev.as_str(), Value::String(path_end.clone()));
                    }
                }

                // Recurse to next relationship in chain
                match_chain_recursive(
                    relationships,
                    rel_idx + 1,
                    &bs,
                    stmts,
                    node_map,
                    nodes,
                    timestamp,
                    results,
                );
            }
        }
        return;
    }

    for stmt in stmts {
        // Check relationship type filter
        if !rel.types.is_empty() {
            let pred = stmt.predicate.as_str();
            let type_matches = rel
                .types
                .iter()
                .any(|t| pred.ends_with(t) || pred == t.as_str());
            if !type_matches {
                continue;
            }
        }

        let mut bs = current_bs.clone();

        // Determine start/end nodes based on relationship direction
        let (start_term, end_term) = match rel.direction {
            RelDirection::Outgoing | RelDirection::Undirected => (&stmt.subject, &stmt.object),
            RelDirection::Incoming => (&stmt.object, &stmt.subject),
        };

        // Bind start node variable (use term_to_value for consistency with end-node binding)
        let start_var = rel
            .start_node
            .as_ref()
            .or_else(|| nodes.get(rel_idx).and_then(|n| n.variable.as_ref()));
        if let Some(start_v) = start_var {
            let start_val = term_to_value(start_term);
            if let Some(existing) = bs.get(start_v) {
                // Already bound — must match
                if *existing != start_val {
                    continue;
                }
            } else {
                bs.insert(start_v.as_str(), start_val);
            }
        }

        // Bind end node variable
        let end_var = rel
            .end_node
            .as_ref()
            .or_else(|| nodes.get(rel_idx + 1).and_then(|n| n.variable.as_ref()));
        if let Some(end_v) = end_var {
            let end_val = term_to_value(end_term);
            if let Some(existing) = bs.get(end_v) {
                // Already bound — must match
                if *existing != end_val {
                    continue;
                }
            } else {
                bs.insert(end_v.as_str(), end_val);
            }
        }

        // Bind relationship variable
        if let Some(rel_var) = &rel.variable {
            bs.insert(
                rel_var.as_str(),
                Value::String(stmt.predicate.as_str().to_string()),
            );
        }

        // Check node label constraints
        let start_ok = start_var
            .and_then(|v| node_map.get(v.as_str()))
            .map(|node| check_node_labels(node, start_term, stmts))
            .unwrap_or(true);
        let end_ok = end_var
            .and_then(|v| node_map.get(v.as_str()))
            .map(|node| check_node_labels(node, end_term, stmts))
            .unwrap_or(true);

        if !start_ok || !end_ok {
            continue;
        }

        // Check inline node property constraints
        let start_props_ok = start_var
            .and_then(|v| node_map.get(v.as_str()))
            .map(|node| check_node_properties(node, &bs))
            .unwrap_or(true);
        let end_props_ok = end_var
            .and_then(|v| node_map.get(v.as_str()))
            .map(|node| check_node_properties(node, &bs))
            .unwrap_or(true);

        if !start_props_ok || !end_props_ok {
            continue;
        }

        // Recursively match next relationship
        match_chain_recursive(
            relationships,
            rel_idx + 1,
            &bs,
            stmts,
            node_map,
            nodes,
            timestamp,
            results,
        );
    }
}

/// Checks if a node's label constraints are satisfied by looking for rdf:type triples.
fn check_node_labels(node: &NodePattern, node_term: &Term, stmts: &[Statement]) -> bool {
    if node.labels.is_empty() {
        return true;
    }
    let node_str = term_to_string(node_term);
    for label in &node.labels {
        let has_label = stmts.iter().any(|s| {
            let subj_str = term_to_string(&s.subject);
            subj_str == node_str
                && (s.predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                    || s.predicate.as_str().ends_with("type"))
                && (s.object == Term::Iri(cqels_model::term::IriTerm::new(label))
                    || term_to_string(&s.object) == *label
                    || term_to_string(&s.object).ends_with(label))
        });
        if !has_label {
            return false;
        }
    }
    true
}

/// Checks inline node property constraints: `(n:Sensor {location: "NYC"})`.
fn check_node_properties(node: &NodePattern, bs: &BindingSet) -> bool {
    if node.properties.is_empty() {
        return true;
    }
    if let Some(var) = &node.variable {
        for (prop_key, prop_value) in &node.properties {
            let bound_key = format!("{}.{}", var, prop_key);
            match bs.get(&bound_key) {
                Some(val) => {
                    let val_str = val.to_string();
                    // Compare with property value (strip quotes if present)
                    let expected = prop_value.trim_matches('"').trim_matches('\'');
                    if val_str != expected && val_str != *prop_value {
                        return false;
                    }
                }
                None => {
                    // Property not yet bound — can't verify constraint yet,
                    // but don't fail (it may be bound later via property extraction)
                }
            }
        }
    }
    true
}

/// Extracts "var.property" bindings for all bound node variables.
fn extract_node_properties(
    bs: &BindingSet,
    stmts: &[Statement],
    node_map: &HashMap<String, &NodePattern>,
    result: &mut BindingSet,
) {
    for var in node_map.keys() {
        if let Some(node_val) = bs.get(var.as_str()) {
            // Extract a comparable string from the node value, regardless of
            // whether it's stored as Value::Term or Value::String.
            let node_str = value_to_node_string(node_val);
            for stmt in stmts {
                let subj_str = term_to_string(&stmt.subject);
                if subj_str == node_str {
                    let pred_str = stmt.predicate.as_str();
                    let prop_name = pred_str
                        .rsplit('/')
                        .next()
                        .or_else(|| pred_str.rsplit('#').next())
                        .unwrap_or(pred_str);
                    let obj_val = term_to_value(&stmt.object);
                    result.insert(format!("{}.{}", var, prop_name), obj_val);
                }
            }
        }
    }
}

/// Extracts a bare string from a Value for node comparison.
/// Works consistently regardless of whether the value was stored as
/// `Value::Term(Term::Iri(...))` or `Value::String(...)`.
fn value_to_node_string(val: &Value) -> String {
    match val {
        Value::Term(t) => term_to_string(t),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Converts a Term to a display string.
fn term_to_string(term: &Term) -> String {
    match term {
        Term::Iri(iri) => iri.as_str().to_string(),
        Term::BlankNode(bn) => bn.id().to_string(),
        Term::Literal(lit) => lit.value().to_string(),
        _ => String::new(),
    }
}

/// Applies GROUP BY and aggregate functions to a batch of binding sets.
pub fn apply_group_by_aggregates(
    elements: Vec<BindingSet>,
    group_by: &[String],
    aggregates: &[PipelineAggregateSpec],
    evaluator: &ExpressionEvaluator,
) -> Vec<BindingSet> {
    if elements.is_empty() {
        return vec![];
    }

    // Group elements by group-by variables.
    //
    // `BTreeMap`, not `HashMap`: rust's default `HashMap` randomizes
    // its hash seed per process, so iterating the map below produces
    // a different group order across runs. That was the root cause
    // of HiveIntel/cqels-rs#70 — the `count-aggregate` parity fixture
    // would flip between match and mismatch on consecutive runs of
    // the same input. `BTreeMap` iterates in sorted-key order which
    // is deterministic. The key type is `Vec<String>` which already
    // implements `Ord`, so this is a drop-in. SPARQL 1.1 §17.4
    // doesn't promise group order absent ORDER BY, but reproducibility
    // across runs of the same query+data is a reasonable expectation
    // — especially when the caller pipes the output into anything
    // order-sensitive like `head -N` or a regression diff.
    //
    // `BTreeMap` is `O(log N)` vs `HashMap`'s amortized `O(1)`, but
    // for the cardinalities a streaming-window aggregate sees (group
    // counts in the hundreds at most before window eviction), the
    // constant-factor cost is negligible compared to the determinism
    // guarantee.
    let mut groups: BTreeMap<Vec<String>, Vec<BindingSet>> = BTreeMap::new();

    for bs in &elements {
        let key: Vec<String> = group_by
            .iter()
            .map(|var| {
                let var_name = var
                    .strip_prefix('?')
                    .or_else(|| var.strip_prefix('$'))
                    .unwrap_or(var);
                bs.get(var_name)
                    .map(|v| format!("{:?}", v)) // Use Debug format to preserve type info
                    .unwrap_or_else(|| "Null".to_string())
            })
            .collect();
        groups.entry(key).or_default().push(bs.clone());
    }

    let mut results = Vec::new();

    for (_key, group) in groups {
        let mut result = BindingSet::new(group.iter().map(|bs| bs.timestamp()).max().unwrap_or(0));

        // Copy group-by variable values
        if let Some(first) = group.first() {
            for var in group_by {
                let var_name = var
                    .strip_prefix('?')
                    .or_else(|| var.strip_prefix('$'))
                    .unwrap_or(var);
                if let Some(val) = first.get(var_name) {
                    result.insert(var_name, val.clone());
                }
            }
        }

        // Compute aggregates
        for agg in aggregates {
            let alias = agg
                .alias
                .strip_prefix('?')
                .or_else(|| agg.alias.strip_prefix('$'))
                .unwrap_or(&agg.alias);

            let values: Vec<Value> = if agg.distinct {
                let mut seen = HashSet::new();
                group
                    .iter()
                    .map(|bs| evaluator.evaluate(&agg.argument, bs))
                    .filter(|v| !v.is_null())
                    .filter(|v| {
                        let key = v.to_string();
                        seen.insert(key)
                    })
                    .collect()
            } else {
                group
                    .iter()
                    .map(|bs| evaluator.evaluate(&agg.argument, bs))
                    .filter(|v| !v.is_null())
                    .collect()
            };

            let agg_result = compute_aggregate(agg.function, &values, agg.separator.as_deref());
            result.insert(alias, agg_result);
        }

        results.push(result);
    }

    results
}

/// Returns `true` if all aggregate functions are SWAG-compatible.
pub fn is_swag_compatible(aggregates: &[PipelineAggregateSpec]) -> bool {
    aggregates.iter().all(|a| {
        matches!(
            a.function,
            AggregateExprFunction::Count
                | AggregateExprFunction::Sum
                | AggregateExprFunction::Avg
                | AggregateExprFunction::Min
                | AggregateExprFunction::Max
        )
    })
}

/// Extracts an `f64` from a [`Value`], returning `0.0` for non-numeric values.
fn value_as_f64(v: &Value) -> f64 {
    v.as_numeric().unwrap_or(0.0)
}

/// Computes an aggregate using the SWAG (Sliding Window AGgregation) backend.
///
/// Supports COUNT, SUM, AVG, MIN, and MAX via `TwoStacksLiteWindow`.
fn compute_aggregate_swag(function: AggregateExprFunction, values: &[Value]) -> Value {
    use crate::operator::swag::*;

    match function {
        AggregateExprFunction::Count => {
            let op = SwagCountOp::<Value>::new();
            let mut window = TwoStacksLiteWindow::new(op);
            for v in values {
                window.push(v);
            }
            Value::Integer(window.query())
        }
        AggregateExprFunction::Sum => {
            if values.is_empty() {
                return Value::Integer(0);
            }
            let all_integers = values.iter().all(|v| matches!(v, Value::Integer(_)));
            let op = SwagSumOp::new(value_as_f64);
            let mut window = TwoStacksLiteWindow::new(op);
            for v in values {
                window.push(v);
            }
            let sum = window.query();
            if all_integers {
                Value::Integer(sum as i64)
            } else {
                Value::Float(sum)
            }
        }
        AggregateExprFunction::Avg => {
            if values.is_empty() {
                return Value::Null;
            }
            let op = SwagMeanOp::new(value_as_f64);
            let mut window = TwoStacksLiteWindow::new(op);
            for v in values {
                window.push(v);
            }
            Value::Float(window.query())
        }
        AggregateExprFunction::Min => {
            if values.is_empty() {
                return Value::Null;
            }
            let all_integers = values.iter().all(|v| matches!(v, Value::Integer(_)));
            let op = SwagMinOp::new(value_as_f64);
            let mut window = TwoStacksLiteWindow::new(op);
            for v in values {
                window.push(v);
            }
            let min = window.query();
            if min == f64::INFINITY {
                return Value::Null;
            }
            if all_integers {
                Value::Integer(min as i64)
            } else {
                Value::Float(min)
            }
        }
        AggregateExprFunction::Max => {
            if values.is_empty() {
                return Value::Null;
            }
            let all_integers = values.iter().all(|v| matches!(v, Value::Integer(_)));
            let op = SwagMaxOp::new(value_as_f64);
            let mut window = TwoStacksLiteWindow::new(op);
            for v in values {
                window.push(v);
            }
            let max = window.query();
            if max == f64::NEG_INFINITY {
                return Value::Null;
            }
            if all_integers {
                Value::Integer(max as i64)
            } else {
                Value::Float(max)
            }
        }
        _ => unreachable!("non-SWAG function passed to compute_aggregate_swag"),
    }
}

/// Computes a single aggregate value.
///
/// SWAG-compatible functions (COUNT, SUM, AVG, MIN, MAX) are routed through
/// [`compute_aggregate_swag`] which uses the `TwoStacksLiteWindow` for
/// amortized O(1) sliding-window aggregation. Collect and GroupConcat use
/// the legacy path.
fn compute_aggregate(
    function: AggregateExprFunction,
    values: &[Value],
    separator: Option<&str>,
) -> Value {
    // Use SWAG for compatible functions
    match function {
        AggregateExprFunction::Count
        | AggregateExprFunction::Sum
        | AggregateExprFunction::Avg
        | AggregateExprFunction::Min
        | AggregateExprFunction::Max => return compute_aggregate_swag(function, values),
        _ => {}
    }

    // Legacy path for Collect and GroupConcat
    match function {
        AggregateExprFunction::Collect => {
            let strs: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            Value::String(format!("[{}]", strs.join(", ")))
        }
        AggregateExprFunction::GroupConcat => {
            let sep = separator.unwrap_or(",");
            let strs: Vec<String> = values
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            Value::String(strs.join(sep))
        }
        _ => unreachable!(),
    }
}

/// Applies ORDER BY and LIMIT to a collected batch of binding sets.
///
/// When the query has a single ORDER BY key and a LIMIT, uses `TopKOperator`
/// for O(N log K) performance instead of a full O(N log N) sort. For multi-key
/// ORDER BY or no LIMIT, falls back to standard sort + truncate.
pub fn apply_order_and_limit(
    mut elements: Vec<BindingSet>,
    order_by: &[(Expression, SortDirection)],
    evaluator: &ExpressionEvaluator,
    limit: Option<u64>,
) -> Vec<BindingSet> {
    // TopK optimization: single ORDER BY key + LIMIT
    if let (1, Some(limit_val)) = (order_by.len(), limit) {
        let k = limit_val as usize;
        let (ref expr, direction) = order_by[0];
        let evaluator_ref = evaluator;
        let topk_direction = match direction {
            SortDirection::Ascending => crate::operator::ranking::SortDirection::Ascending,
            SortDirection::Descending => crate::operator::ranking::SortDirection::Descending,
        };

        let mut topk = crate::operator::ranking::TopKOperator::new(
            k,
            |bs: &BindingSet| evaluator_ref.evaluate(expr, bs).as_numeric().unwrap_or(0.0),
            topk_direction,
        );

        for bs in elements {
            topk.add(bs);
        }

        return topk.get_top_k().into_iter().cloned().collect();
    }

    // Full sort for multi-key ORDER BY or no LIMIT
    if !order_by.is_empty() {
        elements.sort_by(|a, b| {
            for (expr, direction) in order_by {
                let val_a = evaluator.evaluate(expr, a);
                let val_b = evaluator.evaluate(expr, b);

                let cmp = val_a
                    .partial_cmp(&val_b)
                    .unwrap_or(std::cmp::Ordering::Equal);

                let cmp = match direction {
                    SortDirection::Ascending => cmp,
                    SortDirection::Descending => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(limit) = limit {
        elements.truncate(limit as usize);
    }

    elements
}

/// Applies SELECT/RETURN projection to a stream of binding sets.
///
/// Retains only the specified variables in each binding set, discarding
/// all internal/intermediate bindings. Timestamps are preserved.
pub fn apply_projection(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    select_vars: &[String],
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let vars: Vec<String> = select_vars
        .iter()
        .map(|v| {
            v.strip_prefix('?')
                .or_else(|| v.strip_prefix('$'))
                .unwrap_or(v)
                .to_string()
        })
        .collect();

    Box::pin(stream.map(move |bs| {
        let mut projected = BindingSet::new(bs.timestamp());
        for var in &vars {
            if let Some(val) = bs.get(var) {
                projected.insert(var.as_str(), val.clone());
            }
        }
        projected
    }))
}

/// Applies RSP-QL stream semantics to a binding set stream.
///
/// - **IStream** (default): pass through all bindings as-is.
/// - **DStream**: track the previous batch and emit bindings that were removed.
/// - **RStream**: accumulate all bindings and emit the full set on each update.
pub fn apply_stream_semantics(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    semantics: crate::parser::ast::StreamSemantics,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    use crate::parser::ast::StreamSemantics;
    match semantics {
        StreamSemantics::IStream => stream,
        StreamSemantics::DStream => {
            // Track previous batch keys and emit bindings present in previous but not current.
            let state = std::sync::Arc::new(std::sync::Mutex::new(
                Vec::<String>::new(), // previous batch keys
            ));
            let prev_bindings =
                std::sync::Arc::new(std::sync::Mutex::new(Vec::<BindingSet>::new()));

            Box::pin(stream.flat_map(move |bs| {
                let key = deterministic_binding_key(&bs);
                let mut prev_keys = state.lock().unwrap_or_else(|e| e.into_inner());
                let mut prev_bs = prev_bindings.lock().unwrap_or_else(|e| e.into_inner());

                // Check if this binding was in the previous set
                let was_present = prev_keys.contains(&key);

                // Update state: remove from previous if still present, add new
                if was_present {
                    if let Some(pos) = prev_keys.iter().position(|k| k == &key) {
                        prev_keys.remove(pos);
                        prev_bs.remove(pos);
                    }
                }

                // Emit expired bindings (those still in prev_keys after processing)
                // We do this on each new binding arrival as a proxy
                let expired: Vec<BindingSet> = if !was_present {
                    // New binding arrived - emit any remaining old bindings as expired
                    let result = prev_bs.drain(..).collect();
                    prev_keys.clear();
                    result
                } else {
                    vec![]
                };

                // Store current binding for next comparison
                prev_keys.push(key);
                prev_bs.push(bs);

                futures::stream::iter(expired)
            }))
        }
        StreamSemantics::RStream => {
            // Accumulate all bindings, emit full set on each new arrival.
            let accumulated = std::sync::Arc::new(std::sync::Mutex::new(Vec::<BindingSet>::new()));

            Box::pin(stream.flat_map(move |bs| {
                let mut acc = accumulated.lock().unwrap_or_else(|e| e.into_inner());
                // Add new binding if not already present
                let key = deterministic_binding_key(&bs);
                let already = acc
                    .iter()
                    .any(|existing| deterministic_binding_key(existing) == key);
                if !already {
                    acc.push(bs);
                }
                let snapshot: Vec<BindingSet> = acc.clone();
                futures::stream::iter(snapshot)
            }))
        }
    }
}

/// Applies DISTINCT to a stream of binding sets.
///
/// Deduplicates bindings using a deterministic string key derived from
/// sorted variable names and values. Maintains a `HashSet` in memory
/// for the lifetime of the stream.
pub fn apply_distinct(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(HashSet::<String>::new()));

    Box::pin(stream.filter(move |bs| {
        // Build a deterministic key by sorting binding keys
        let key = deterministic_binding_key(bs);
        let is_new = seen.lock().unwrap_or_else(|e| e.into_inner()).insert(key);
        futures::future::ready(is_new)
    }))
}

/// Produces a deterministic string key for a [`BindingSet`] by sorting
/// variable names and emitting length-prefixed values.
///
/// The encoding is `name=<valueLen>:<value>` per binding, joined with `,`,
/// where `<valueLen>` is the Unicode scalar count of the stringified value.
/// Length-prefixing guarantees injectivity: RDF terms can legally contain
/// `,` and `=` in their `Display` form (e.g. a blank node `_:foo,b=_:bar`
/// would otherwise collide with a two-binding `{a → _:foo, b → _:bar}`
/// because [`cqels_model::term::BlankNodeTerm`]'s `_:id` Display has no
/// closing delimiter).
///
/// Mirrors the Java side
/// (`org.cqels.lang.common.computation.SolutionDistinct#solutionKey` in
/// cqels/claude) which uses the same length-prefix shape with `String#length`
/// (UTF-16 code units). The two are byte-identical on all-ASCII content,
/// which covers the entire parity fixture corpus and all SPARQL grammar
/// productions exercised by the unit-4 spec. Supplementary-plane Unicode
/// content would diverge by ratio 1:2; no fixture exercises it.
///
/// See `parity/specs/distinct-operator.parity.md` D-D1 for the cross-engine
/// contract.
fn deterministic_binding_key(bs: &BindingSet) -> String {
    let mut pairs: Vec<(String, String)> = bs
        .variables()
        .map(|var| {
            (
                var.to_string(),
                bs.get(var).map(|v| v.to_string()).unwrap_or_default(),
            )
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut key = String::new();
    for (k, v) in &pairs {
        if !key.is_empty() {
            key.push(',');
        }
        key.push_str(k);
        key.push('=');
        key.push_str(&v.chars().count().to_string());
        key.push(':');
        key.push_str(v);
    }
    key
}

/// Substitutes variables in a CONSTRUCT template using bindings to produce statements.
///
/// For each template triple, substitutes `?var` references from the binding set.
/// Triples where any variable is unbound are skipped.
pub fn apply_construct_template(
    bindings: &BindingSet,
    template: &[crate::parser::ast::TriplePattern],
    prefixes: &HashMap<String, String>,
) -> Vec<Statement> {
    let mut results = Vec::new();

    for tp in template {
        let subject_str = resolve_construct_term(&tp.subject, bindings, prefixes);
        let predicate_str = resolve_construct_term(&tp.predicate, bindings, prefixes);
        let object_str = resolve_construct_term(&tp.object, bindings, prefixes);

        // Skip triples where any variable is unbound
        let (Some(s), Some(p), Some(o)) = (subject_str, predicate_str, object_str) else {
            continue;
        };

        let subject = if s.starts_with("http://") || s.starts_with("https://") {
            Term::Iri(IriTerm::new(&s))
        } else {
            Term::Literal(cqels_model::term::LiteralTerm::new(&s))
        };

        let predicate = IriTerm::new(p.trim_start_matches('<').trim_end_matches('>'));

        let object = if o.starts_with("http://") || o.starts_with("https://") {
            Term::Iri(IriTerm::new(&o))
        } else {
            Term::Literal(cqels_model::term::LiteralTerm::new(&o))
        };

        results.push(Statement::new(subject, predicate, object));
    }

    results
}

/// Resolves a construct term: if it's a variable, look up in bindings; otherwise resolve as IRI/literal.
fn resolve_construct_term(
    term: &str,
    bindings: &BindingSet,
    prefixes: &HashMap<String, String>,
) -> Option<String> {
    if crate::parser::ast::TriplePattern::is_variable(term) {
        let var_name = term
            .strip_prefix('?')
            .or_else(|| term.strip_prefix('$'))
            .unwrap_or(term);
        bindings.get(var_name).map(|v| v.to_string())
    } else if term.starts_with('<') && term.ends_with('>') {
        Some(
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string(),
        )
    } else if let Some(colon_pos) = term.find(':') {
        let prefix = &term[..colon_pos];
        let local = &term[colon_pos + 1..];
        if let Some(base) = prefixes.get(prefix) {
            Some(format!("{base}{local}"))
        } else {
            Some(term.to_string())
        }
    } else {
        Some(term.to_string())
    }
}

/// Converts a [`Value`] to a [`Term`], preserving type information.
///
/// Delegates to [`Value::to_term()`] for correct handling of IRIs, typed
/// literals, and booleans. Returns an empty-string literal for `Value::Null`.
pub(crate) fn value_to_term(val: &Value) -> Term {
    val.to_term()
        .unwrap_or_else(|| Term::Literal(cqels_model::term::LiteralTerm::new("")))
}

/// Converts parser AggregateFunction to expression AggregateExprFunction.
pub(crate) fn convert_aggregate_function(f: AggregateFunction) -> AggregateExprFunction {
    match f {
        AggregateFunction::Count => AggregateExprFunction::Count,
        AggregateFunction::Sum => AggregateExprFunction::Sum,
        AggregateFunction::Avg => AggregateExprFunction::Avg,
        AggregateFunction::Min => AggregateExprFunction::Min,
        AggregateFunction::Max => AggregateExprFunction::Max,
        AggregateFunction::Collect => AggregateExprFunction::Collect,
        AggregateFunction::GroupConcat => AggregateExprFunction::GroupConcat,
    }
}

/// Simple string hash for generating query IDs.
pub(crate) fn hash_string(s: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    hash
}

/// Joins two sets of binding sets using natural join semantics.
///
/// For each pair `(l, r)` where `l ∈ left` and `r ∈ right`, produces
/// `l.join(r)` if compatible (shared variables have equal values).
/// Returns all successful joins.
///
/// # Examples
///
/// ```
/// use cqels_core::compiler::pipeline::join_binding_sets;
/// use cqels_model::{BindingSet, Value};
///
/// let mut l = BindingSet::new(0);
/// l.insert("s", Value::String("alice".into()));
/// l.insert("type", Value::String("Person".into()));
///
/// let mut r = BindingSet::new(0);
/// r.insert("s", Value::String("alice".into()));
/// r.insert("age", Value::Integer(30));
///
/// let results = join_binding_sets(&[l], &[r]);
/// assert_eq!(results.len(), 1);
/// assert!(results[0].get("type").is_some());
/// assert!(results[0].get("age").is_some());
/// ```
pub fn join_binding_sets(left: &[BindingSet], right: &[BindingSet]) -> Vec<BindingSet> {
    let mut results = Vec::new();
    for l in left {
        for r in right {
            if let Some(joined) = l.join(r) {
                results.push(joined);
            }
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::LiteralTerm;

    fn make_statement(subject: &str, predicate: &str, object: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Literal(LiteralTerm::new(object)),
        )
    }

    fn make_triple_pattern(
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> crate::parser::ast::TriplePattern {
        crate::parser::ast::TriplePattern {
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object: object.to_string(),
        }
    }

    #[test]
    fn test_term_to_value_lifts_xsd_integer_derived_subtypes() {
        // codex P2 on cqels-rs#94: enumerating only integer/int/long dropped
        // the derived subtypes to Value::String after the untyped-numeric
        // fallback was removed, breaking FILTER/arithmetic over standard
        // stream datatypes.
        for dt in [
            "http://www.w3.org/2001/XMLSchema#short",
            "http://www.w3.org/2001/XMLSchema#byte",
            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
            "http://www.w3.org/2001/XMLSchema#negativeInteger",
            "http://www.w3.org/2001/XMLSchema#positiveInteger",
            "http://www.w3.org/2001/XMLSchema#unsignedLong",
            "http://www.w3.org/2001/XMLSchema#unsignedInt",
            "http://www.w3.org/2001/XMLSchema#unsignedShort",
            "http://www.w3.org/2001/XMLSchema#unsignedByte",
        ] {
            let term = Term::Literal(LiteralTerm::new("31").with_datatype(dt));
            assert_eq!(term_to_value(&term), Value::Integer(31), "datatype {dt}");
        }
    }

    #[test]
    fn test_term_to_value_plain_and_string_literals_stay_strings() {
        // The other half of the unit-5 contract (commit 6f3524e): plain and
        // xsd:string literals must NOT lift numerically even when the lexical
        // form parses — SPARQL §17.2.2 EBV of "0" is true (nonempty string).
        let plain = Term::Literal(LiteralTerm::new("0"));
        assert_eq!(term_to_value(&plain), Value::String("0".to_string()));
        let typed = Term::Literal(
            LiteralTerm::new("0").with_datatype("http://www.w3.org/2001/XMLSchema#string"),
        );
        assert_eq!(term_to_value(&typed), Value::String("0".to_string()));
        // Out-of-i64-range magnitude falls through to String (consistent with
        // pre-existing xsd:integer behavior).
        let huge = Term::Literal(
            LiteralTerm::new("18446744073709551615")
                .with_datatype("http://www.w3.org/2001/XMLSchema#unsignedLong"),
        );
        assert_eq!(
            term_to_value(&huge),
            Value::String("18446744073709551615".to_string())
        );
    }

    #[test]
    fn test_match_triple_pattern_all_variables() {
        let pattern = make_triple_pattern("?s", "?p", "?o");
        let stmt = make_statement(
            "http://example.org/sensor1",
            "http://example.org/temp",
            "42",
        );
        let prefixes = HashMap::new();
        let result = match_triple_pattern(&pattern, &stmt, &prefixes, 1000);
        assert!(result.is_some());
        let bs = result.unwrap();
        assert!(bs.contains("s"));
        assert!(bs.contains("p"));
        assert!(bs.contains("o"));
    }

    #[test]
    fn test_match_triple_pattern_fixed_predicate() {
        let pattern = make_triple_pattern("?s", "<http://example.org/temp>", "?o");
        let stmt = make_statement(
            "http://example.org/sensor1",
            "http://example.org/temp",
            "42",
        );
        let prefixes = HashMap::new();
        let result = match_triple_pattern(&pattern, &stmt, &prefixes, 1000);
        assert!(result.is_some());
    }

    #[test]
    fn test_match_triple_pattern_mismatch() {
        let pattern = make_triple_pattern("?s", "<http://example.org/other>", "?o");
        let stmt = make_statement(
            "http://example.org/sensor1",
            "http://example.org/temp",
            "42",
        );
        let prefixes = HashMap::new();
        let result = match_triple_pattern(&pattern, &stmt, &prefixes, 1000);
        assert!(result.is_none());
    }

    #[test]
    fn test_match_triple_pattern_with_prefix() {
        let pattern = make_triple_pattern("?s", "ex:temp", "?o");
        let stmt = make_statement(
            "http://example.org/sensor1",
            "http://example.org/temp",
            "42",
        );
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());
        let result = match_triple_pattern(&pattern, &stmt, &prefixes, 1000);
        assert!(result.is_some());
    }

    #[test]
    fn test_match_triple_pattern_a_keyword() {
        let pattern = make_triple_pattern("?s", "a", "?o");
        let stmt = make_statement(
            "http://example.org/sensor1",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "Sensor",
        );
        let prefixes = HashMap::new();
        let result = match_triple_pattern(&pattern, &stmt, &prefixes, 1000);
        assert!(result.is_some());
    }

    #[test]
    fn test_apply_group_by_aggregates() {
        let evaluator = ExpressionEvaluator::new();

        let elements = vec![
            {
                let mut bs = BindingSet::new(0);
                bs.insert("city", Value::String("NYC".into()));
                bs.insert("temp", Value::Integer(30));
                bs
            },
            {
                let mut bs = BindingSet::new(0);
                bs.insert("city", Value::String("NYC".into()));
                bs.insert("temp", Value::Integer(32));
                bs
            },
            {
                let mut bs = BindingSet::new(0);
                bs.insert("city", Value::String("LA".into()));
                bs.insert("temp", Value::Integer(25));
                bs
            },
        ];

        let group_by = vec!["city".to_string()];
        let aggregates = vec![PipelineAggregateSpec {
            function: AggregateExprFunction::Avg,
            argument: Expression::Variable("temp".to_string()),
            alias: "avg_temp".to_string(),
            distinct: false,
            separator: None,
        }];

        let results = apply_group_by_aggregates(elements, &group_by, &aggregates, &evaluator);
        assert_eq!(results.len(), 2);

        for result in &results {
            let city = result.get("city").unwrap();
            let avg = result.get("avg_temp").unwrap();
            match city.as_string().unwrap() {
                "NYC" => assert_eq!(*avg, Value::Float(31.0)),
                "LA" => assert_eq!(*avg, Value::Float(25.0)),
                _ => panic!("unexpected city"),
            }
        }
    }

    /// Regression for HiveIntel/cqels-rs#70 — non-deterministic group
    /// order. The previous implementation used `HashMap<Vec<String>,
    /// _>` whose randomized hash seed produced different group orders
    /// across runs even of identical input. The fix is `BTreeMap`,
    /// which iterates in sorted-key order. This test runs the
    /// function 100 times on identical input and asserts every run
    /// produces byte-identical output ordering — would have failed
    /// reliably on the pre-fix `HashMap` implementation.
    #[test]
    fn group_by_aggregates_emit_in_deterministic_order() {
        let evaluator = ExpressionEvaluator::new();
        // Two distinct sensor groups so HashMap's bucket layout has
        // something to scramble. Reproduces the count-aggregate
        // parity fixture's data shape (3 readings for s1, 2 for s2).
        let make_elements = || {
            vec![
                {
                    let mut bs = BindingSet::new(1000);
                    bs.insert("sensor", Value::String("s1".into()));
                    bs.insert("temp", Value::Integer(21));
                    bs
                },
                {
                    let mut bs = BindingSet::new(1100);
                    bs.insert("sensor", Value::String("s2".into()));
                    bs.insert("temp", Value::Integer(22));
                    bs
                },
                {
                    let mut bs = BindingSet::new(1200);
                    bs.insert("sensor", Value::String("s1".into()));
                    bs.insert("temp", Value::Integer(23));
                    bs
                },
                {
                    let mut bs = BindingSet::new(1300);
                    bs.insert("sensor", Value::String("s1".into()));
                    bs.insert("temp", Value::Integer(24));
                    bs
                },
                {
                    let mut bs = BindingSet::new(1400);
                    bs.insert("sensor", Value::String("s2".into()));
                    bs.insert("temp", Value::Integer(25));
                    bs
                },
            ]
        };

        let group_by = vec!["sensor".to_string()];
        let aggregates = vec![PipelineAggregateSpec {
            function: AggregateExprFunction::Count,
            argument: Expression::Variable("temp".to_string()),
            alias: "n".to_string(),
            distinct: false,
            separator: None,
        }];

        let first = apply_group_by_aggregates(make_elements(), &group_by, &aggregates, &evaluator);

        // 100 iterations: every run must produce byte-identical
        // output. On the old HashMap implementation, this test would
        // typically fail within the first ~5 iterations.
        for i in 0..100 {
            let next =
                apply_group_by_aggregates(make_elements(), &group_by, &aggregates, &evaluator);
            assert_eq!(
                first.len(),
                next.len(),
                "iteration {i}: group count drifted"
            );
            for (a, b) in first.iter().zip(next.iter()) {
                assert_eq!(
                    a.get("sensor"),
                    b.get("sensor"),
                    "iteration {i}: emission order drifted on sensor"
                );
                assert_eq!(
                    a.get("n"),
                    b.get("n"),
                    "iteration {i}: count drifted alongside drift"
                );
            }
        }

        // Spot-check the actual values too — alphabetical sort by
        // group key means s1 comes before s2.
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].get("sensor").unwrap().as_string().unwrap(), "s1");
        assert_eq!(first[0].get("n").unwrap(), &Value::Integer(3));
        assert_eq!(first[1].get("sensor").unwrap().as_string().unwrap(), "s2");
        assert_eq!(first[1].get("n").unwrap(), &Value::Integer(2));
    }

    #[test]
    fn test_apply_order_and_limit() {
        let evaluator = ExpressionEvaluator::new();

        let elements = vec![
            {
                let mut bs = BindingSet::new(0);
                bs.insert("val", Value::Integer(30));
                bs
            },
            {
                let mut bs = BindingSet::new(0);
                bs.insert("val", Value::Integer(10));
                bs
            },
            {
                let mut bs = BindingSet::new(0);
                bs.insert("val", Value::Integer(20));
                bs
            },
        ];

        let order_by = vec![(
            Expression::Variable("val".to_string()),
            SortDirection::Ascending,
        )];

        let sorted = apply_order_and_limit(elements, &order_by, &evaluator, Some(2));
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0].get("val"), Some(&Value::Integer(10)));
        assert_eq!(sorted[1].get("val"), Some(&Value::Integer(20)));
    }

    #[test]
    fn test_compute_aggregate_count() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Count, &values, None),
            Value::Integer(3)
        );
    }

    #[test]
    fn test_compute_aggregate_sum() {
        let values = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Sum, &values, None),
            Value::Integer(60)
        );
    }

    #[test]
    fn test_compute_aggregate_avg() {
        let values = vec![Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Avg, &values, None),
            Value::Float(20.0)
        );
    }

    #[test]
    fn test_resolve_term_prefix() {
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());
        assert_eq!(
            resolve_term("ex:name", &prefixes),
            "http://example.org/name"
        );
    }

    #[test]
    fn test_resolve_term_iri() {
        let prefixes = HashMap::new();
        assert_eq!(
            resolve_term("<http://example.org/name>", &prefixes),
            "http://example.org/name"
        );
    }

    #[test]
    fn test_resolve_term_a() {
        let prefixes = HashMap::new();
        assert_eq!(
            resolve_term("a", &prefixes),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
    }

    // ── Phase 2 unit tests ──────────────────────────────────────────────

    #[test]
    fn test_minus_patterns_match_shared_variable() {
        // MINUS pattern shares ?s with the binding set → match (exclude)
        let pattern = make_triple_pattern("?s", "?p", "?o");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));
        let prefixes = HashMap::new();
        assert!(minus_patterns_match(&[pattern], &bs, &prefixes));
    }

    #[test]
    fn test_minus_patterns_match_no_shared_variable() {
        // MINUS pattern has ?x, ?y, ?z but binding has ?s → no shared → no match
        let pattern = make_triple_pattern("?x", "?y", "?z");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));
        let prefixes = HashMap::new();
        assert!(!minus_patterns_match(&[pattern], &bs, &prefixes));
    }

    #[test]
    fn test_minus_patterns_match_constants_only() {
        // MINUS pattern has only constants — no shared variables → no match
        let pattern = make_triple_pattern(
            "<http://example.org/s>",
            "<http://example.org/p>",
            "<http://example.org/o>",
        );
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("something".into()));
        let prefixes = HashMap::new();
        assert!(!minus_patterns_match(&[pattern], &bs, &prefixes));
    }

    #[test]
    fn test_try_satisfy_pattern_bound_variable() {
        // Pattern has ?s which is bound in bs → connected → true
        let pattern = make_triple_pattern("?s", "<http://example.org/p>", "?o");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));
        let prefixes = HashMap::new();
        assert!(try_satisfy_pattern_from_bindings(&pattern, &bs, &prefixes));
    }

    #[test]
    fn test_try_satisfy_pattern_unbound_variable() {
        // Pattern has ?x, ?y, ?z but binding has ?s → disconnected → false
        let pattern = make_triple_pattern("?x", "?y", "?z");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));
        let prefixes = HashMap::new();
        assert!(!try_satisfy_pattern_from_bindings(&pattern, &bs, &prefixes));
    }

    #[test]
    fn test_check_pattern_consistency_shared() {
        // Pattern ?s matches binding with ?s → consistent
        let pattern = make_triple_pattern("?s", "?p", "?o");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));
        let prefixes = HashMap::new();
        assert!(check_pattern_consistency(&pattern, &bs, &prefixes));
    }

    #[test]
    fn test_check_pattern_consistency_no_shared() {
        // Pattern ?x, ?y, ?z — no overlap with binding ?s → false
        let pattern = make_triple_pattern("?x", "?y", "?z");
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::String("val".into()));
        let prefixes = HashMap::new();
        assert!(!check_pattern_consistency(&pattern, &bs, &prefixes));
    }

    #[tokio::test]
    async fn test_apply_optional_passes_through_unmatched() {
        use crate::parser::ast::CqelsPatternGroup;

        let evaluator = ExpressionEvaluator::new();
        let prefixes = HashMap::new();

        let mut bs = BindingSet::new(100);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));

        let stream = Box::pin(futures::stream::iter(vec![bs.clone()]));

        // Optional block with disconnected variables — should pass through unchanged
        let optional_groups = vec![vec![CqelsPatternGroup::Default {
            patterns: vec![make_triple_pattern("?x", "?y", "?z")],
        }]];

        let result: Vec<_> = apply_optional(stream, &optional_groups, &prefixes, &evaluator)
            .collect()
            .await;

        assert_eq!(result.len(), 1);
        // Original bindings preserved unchanged
        assert_eq!(
            result[0].get("s"),
            Some(&Value::String("http://example.org/sensor1".into()))
        );
        // Disconnected variables NOT added
        assert!(result[0].get("x").is_none());
    }

    #[tokio::test]
    async fn test_apply_union_both_branches() {
        use crate::parser::ast::CqelsPatternGroup;

        let evaluator = ExpressionEvaluator::new();
        let prefixes = HashMap::new();

        let mut bs = BindingSet::new(100);
        bs.insert("s", Value::String("http://example.org/sensor1".into()));

        let stream = Box::pin(futures::stream::iter(vec![bs.clone()]));

        // Both branches share ?s with the binding → both match
        let union_blocks = vec![(
            vec![CqelsPatternGroup::Default {
                patterns: vec![make_triple_pattern("?s", "?p1", "?o1")],
            }],
            vec![CqelsPatternGroup::Default {
                patterns: vec![make_triple_pattern("?s", "?p2", "?o2")],
            }],
        )];

        let result: Vec<_> = apply_union(stream, &union_blocks, &prefixes, &evaluator)
            .collect()
            .await;

        // Both branches match → 2 results
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_apply_minus_filters_shared() {
        let prefixes = HashMap::new();

        let mut bs1 = BindingSet::new(100);
        bs1.insert("s", Value::String("http://example.org/sensor1".into()));

        let mut bs2 = BindingSet::new(200);
        bs2.insert("x", Value::String("http://example.org/other".into()));

        let stream = Box::pin(futures::stream::iter(vec![bs1, bs2]));

        // MINUS pattern shares ?s → filters out bs1, keeps bs2
        let minus_blocks = vec![vec![make_triple_pattern("?s", "?p", "?o")]];

        let result: Vec<_> = apply_minus(stream, &minus_blocks, &prefixes)
            .collect()
            .await;

        assert_eq!(result.len(), 1);
        assert!(result[0].get("x").is_some());
        assert!(result[0].get("s").is_none());
    }

    #[test]
    fn test_compute_aggregate_group_concat_default_separator() {
        let values = vec![
            Value::String("a".into()),
            Value::String("b".into()),
            Value::String("c".into()),
        ];
        let result = compute_aggregate(AggregateExprFunction::GroupConcat, &values, None);
        assert_eq!(result, Value::String("a,b,c".into()));
    }

    #[test]
    fn test_compute_aggregate_group_concat_custom_separator() {
        let values = vec![
            Value::String("x".into()),
            Value::String("y".into()),
            Value::String("z".into()),
        ];
        let result = compute_aggregate(AggregateExprFunction::GroupConcat, &values, Some("; "));
        assert_eq!(result, Value::String("x; y; z".into()));
    }

    #[test]
    fn test_compute_aggregate_group_concat_empty() {
        let values: Vec<Value> = vec![];
        let result = compute_aggregate(AggregateExprFunction::GroupConcat, &values, None);
        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn test_match_cypher_pattern_single_relationship() {
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![crate::parser::ast::RelationshipPattern {
                variable: None,
                types: vec!["KNOWS".into()],
                direction: RelDirection::Outgoing,
                properties: HashMap::new(),
                start_node: Some("a".into()),
                end_node: Some("b".into()),
                path_length: None,
            }],
        };

        let stmts = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/Alice")),
            IriTerm::new("http://ex.org/KNOWS"),
            Term::Iri(IriTerm::new("http://ex.org/Bob")),
        )];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        assert_eq!(results.len(), 1);
        assert!(results[0].get("a").is_some());
        assert!(results[0].get("b").is_some());
    }

    #[test]
    fn test_match_cypher_pattern_chain() {
        // (a)-[:KNOWS]->(b)-[:LIKES]->(c)
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("c".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![
                crate::parser::ast::RelationshipPattern {
                    variable: None,
                    types: vec!["KNOWS".into()],
                    direction: RelDirection::Outgoing,
                    properties: HashMap::new(),
                    start_node: Some("a".into()),
                    end_node: Some("b".into()),
                    path_length: None,
                },
                crate::parser::ast::RelationshipPattern {
                    variable: None,
                    types: vec!["LIKES".into()],
                    direction: RelDirection::Outgoing,
                    properties: HashMap::new(),
                    start_node: Some("b".into()),
                    end_node: Some("c".into()),
                    path_length: None,
                },
            ],
        };

        let stmts = vec![
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/Alice")),
                IriTerm::new("http://ex.org/KNOWS"),
                Term::Iri(IriTerm::new("http://ex.org/Bob")),
            ),
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/Bob")),
                IriTerm::new("http://ex.org/LIKES"),
                Term::Iri(IriTerm::new("http://ex.org/Post1")),
            ),
        ];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        assert_eq!(results.len(), 1);
        assert!(results[0].get("a").is_some());
        assert!(results[0].get("b").is_some());
        assert!(results[0].get("c").is_some());
    }

    #[test]
    fn test_match_cypher_pattern_no_match() {
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![crate::parser::ast::RelationshipPattern {
                variable: None,
                types: vec!["FOLLOWS".into()],
                direction: RelDirection::Outgoing,
                properties: HashMap::new(),
                start_node: Some("a".into()),
                end_node: Some("b".into()),
                path_length: None,
            }],
        };

        let stmts = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/Alice")),
            IriTerm::new("http://ex.org/KNOWS"),
            Term::Iri(IriTerm::new("http://ex.org/Bob")),
        )];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        assert!(results.is_empty());
    }

    #[test]
    fn test_check_node_properties_matching() {
        let mut props = HashMap::new();
        props.insert("location".into(), "\"NYC\"".into());

        let node = NodePattern {
            variable: Some("n".into()),
            labels: vec![],
            properties: props,
        };

        let mut bs = BindingSet::new(0);
        bs.insert("n", Value::String("http://ex.org/Sensor1".into()));
        bs.insert("n.location", Value::String("NYC".into()));

        assert!(check_node_properties(&node, &bs));
    }

    #[test]
    fn test_check_node_properties_non_matching() {
        let mut props = HashMap::new();
        props.insert("location".into(), "\"NYC\"".into());

        let node = NodePattern {
            variable: Some("n".into()),
            labels: vec![],
            properties: props,
        };

        let mut bs = BindingSet::new(0);
        bs.insert("n", Value::String("http://ex.org/Sensor1".into()));
        bs.insert("n.location", Value::String("LA".into()));

        assert!(!check_node_properties(&node, &bs));
    }

    // ── Phase 13: SWAG equivalence tests ──────────────────────────────────

    #[test]
    fn test_swag_count_equivalence() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Count, &values, None),
            Value::Integer(3)
        );
    }

    #[test]
    fn test_swag_sum_equivalence() {
        // Integer sum
        let ints = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Sum, &ints, None),
            Value::Integer(60)
        );
        // Float sum
        let floats = vec![Value::Float(1.5), Value::Float(2.5), Value::Float(3.0)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Sum, &floats, None),
            Value::Float(7.0)
        );
    }

    #[test]
    fn test_swag_avg_equivalence() {
        let values = vec![Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Avg, &values, None),
            Value::Float(20.0)
        );
    }

    #[test]
    fn test_swag_min_max_equivalence() {
        let ints = vec![Value::Integer(5), Value::Integer(1), Value::Integer(9)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Min, &ints, None),
            Value::Integer(1)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Max, &ints, None),
            Value::Integer(9)
        );

        let floats = vec![Value::Float(3.5), Value::Float(1.25), Value::Float(2.75)];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Min, &floats, None),
            Value::Float(1.25)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Max, &floats, None),
            Value::Float(3.5)
        );
    }

    #[test]
    fn test_swag_empty_values() {
        let empty: Vec<Value> = vec![];
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Count, &empty, None),
            Value::Integer(0)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Sum, &empty, None),
            Value::Integer(0)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Avg, &empty, None),
            Value::Null
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Min, &empty, None),
            Value::Null
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Max, &empty, None),
            Value::Null
        );
    }

    #[test]
    fn test_swag_mixed_types() {
        let mixed = vec![Value::Integer(10), Value::Float(20.5), Value::Integer(30)];
        // Mixed types → float path
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Sum, &mixed, None),
            Value::Float(60.5)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Avg, &mixed, None),
            Value::Float(60.5 / 3.0)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Min, &mixed, None),
            Value::Float(10.0)
        );
        assert_eq!(
            compute_aggregate(AggregateExprFunction::Max, &mixed, None),
            Value::Float(30.0)
        );
    }

    #[test]
    fn test_is_swag_compatible() {
        use crate::expression::ast::Expression;

        let make_spec = |func| PipelineAggregateSpec {
            function: func,
            argument: Expression::Variable("x".to_string()),
            alias: "a".to_string(),
            distinct: false,
            separator: None,
        };

        // All SWAG-compatible
        let swag_aggs = vec![
            make_spec(AggregateExprFunction::Count),
            make_spec(AggregateExprFunction::Sum),
            make_spec(AggregateExprFunction::Avg),
            make_spec(AggregateExprFunction::Min),
            make_spec(AggregateExprFunction::Max),
        ];
        assert!(is_swag_compatible(&swag_aggs));

        // Contains non-SWAG function
        let mixed_aggs = vec![
            make_spec(AggregateExprFunction::Count),
            make_spec(AggregateExprFunction::GroupConcat),
        ];
        assert!(!is_swag_compatible(&mixed_aggs));

        // Empty list is trivially compatible
        assert!(is_swag_compatible(&[]));
    }

    // ── Variable-length path tests ──────────────────────────────────────

    #[test]
    fn test_variable_length_path_multi_hop() {
        // (a)-[*1..3]->(b) over A->B->C chain
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![crate::parser::ast::RelationshipPattern {
                variable: None,
                types: vec![],
                direction: RelDirection::Outgoing,
                properties: HashMap::new(),
                start_node: Some("a".into()),
                end_node: Some("b".into()),
                path_length: Some(crate::parser::ast::PathLength {
                    min_length: Some(1),
                    max_length: Some(3),
                }),
            }],
        };

        let stmts = vec![
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/A")),
                IriTerm::new("http://ex.org/KNOWS"),
                Term::Iri(IriTerm::new("http://ex.org/B")),
            ),
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/B")),
                IriTerm::new("http://ex.org/KNOWS"),
                Term::Iri(IriTerm::new("http://ex.org/C")),
            ),
        ];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        // Should find: A->B (1 hop), A->C (2 hops), B->C (1 hop)
        assert!(
            results.len() >= 3,
            "expected >= 3 results, got {}",
            results.len()
        );
    }

    #[test]
    fn test_variable_length_path_with_type_filter() {
        // (a)-[:KNOWS *1..2]->(b) — only KNOWS edges
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![crate::parser::ast::RelationshipPattern {
                variable: None,
                types: vec!["KNOWS".into()],
                direction: RelDirection::Outgoing,
                properties: HashMap::new(),
                start_node: Some("a".into()),
                end_node: Some("b".into()),
                path_length: Some(crate::parser::ast::PathLength {
                    min_length: Some(1),
                    max_length: Some(2),
                }),
            }],
        };

        let stmts = vec![
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/A")),
                IriTerm::new("http://ex.org/KNOWS"),
                Term::Iri(IriTerm::new("http://ex.org/B")),
            ),
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/B")),
                IriTerm::new("http://ex.org/LIKES"),
                Term::Iri(IriTerm::new("http://ex.org/C")),
            ),
        ];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        // Only A->B via KNOWS (LIKES is filtered out)
        assert!(!results.is_empty());
        for r in &results {
            // End node should never be C since LIKES edge is filtered
            if let Some(end) = r.get("b") {
                assert_ne!(
                    end.to_string(),
                    "http://ex.org/C",
                    "should not traverse LIKES edges"
                );
            }
        }
    }

    #[test]
    fn test_single_hop_pattern_still_works() {
        // Regression: single-hop without path_length must still work
        let pattern = CypherPattern {
            nodes: vec![
                NodePattern {
                    variable: Some("a".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
                NodePattern {
                    variable: Some("b".into()),
                    labels: vec![],
                    properties: HashMap::new(),
                },
            ],
            relationships: vec![crate::parser::ast::RelationshipPattern {
                variable: None,
                types: vec!["KNOWS".into()],
                direction: RelDirection::Outgoing,
                properties: HashMap::new(),
                start_node: Some("a".into()),
                end_node: Some("b".into()),
                path_length: None,
            }],
        };

        let stmts = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/Alice")),
            IriTerm::new("http://ex.org/KNOWS"),
            Term::Iri(IriTerm::new("http://ex.org/Bob")),
        )];

        let results = match_cypher_pattern(&pattern, &stmts, 1000);
        assert_eq!(results.len(), 1);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // deterministic_binding_key + apply_distinct — parity-root unit 4 / D-D1
    // ─────────────────────────────────────────────────────────────────────────

    use cqels_model::term::BlankNodeTerm;

    fn bs_with(pairs: &[(&str, Value)]) -> BindingSet {
        let mut bs = BindingSet::new(0);
        for (name, value) in pairs {
            bs.insert(*name, value.clone());
        }
        bs
    }

    fn iri(v: &str) -> Value {
        Value::Term(Term::Iri(IriTerm::new(v)))
    }

    fn bnode(id: &str) -> Value {
        Value::Term(Term::BlankNode(BlankNodeTerm::new(id)))
    }

    #[test]
    fn deterministic_binding_key_empty_emits_empty_string() {
        assert_eq!(deterministic_binding_key(&BindingSet::new(0)), "");
    }

    #[test]
    fn deterministic_binding_key_sorts_by_name_regardless_of_insertion_order() {
        let r1 = bs_with(&[("b", iri("urn:y")), ("a", iri("urn:x"))]);
        let r2 = bs_with(&[("a", iri("urn:x")), ("b", iri("urn:y"))]);
        assert_eq!(
            deterministic_binding_key(&r1),
            deterministic_binding_key(&r2)
        );
        // Pin the canonical shape: name=<valueLen>:<value> joined by ','.
        // Rust IRI Display is `<urn:x>` (7 chars including angle brackets).
        assert_eq!(deterministic_binding_key(&r1), "a=7:<urn:x>,b=7:<urn:y>");
    }

    #[test]
    fn deterministic_binding_key_injective_blank_node_containing_comma_and_equals() {
        // Regression mirror of the Java test
        // `solutionKey_injective_iriContainingCommaAndEquals` (cqels/claude#143
        // Codex P2 fix). Rust hits the collision through BlankNodeTerm rather
        // than IRI — IriTerm Display brackets the value, but BlankNodeTerm's
        // `_:id` Display has no closing delimiter, so a blank node ID
        // `foo,b=_:bar` produces the same naive `name=value`-joined key as
        // a two-binding `{a → _:foo, b → _:bar}`. Length-prefixing breaks the
        // tie.
        let two_bindings = bs_with(&[("a", bnode("foo")), ("b", bnode("bar"))]);
        let one_binding_colliding = bs_with(&[("a", bnode("foo,b=_:bar"))]);
        assert_ne!(
            deterministic_binding_key(&two_bindings),
            deterministic_binding_key(&one_binding_colliding),
        );
    }

    #[tokio::test]
    async fn apply_distinct_multiset_collapse_first_seen_wins() {
        // D1 — multiset → set collapse: identical solutions dedup to one,
        // first-seen order preserved.
        let a = bs_with(&[("x", iri("urn:a"))]);
        let a_dup = bs_with(&[("x", iri("urn:a"))]);
        let b = bs_with(&[("x", iri("urn:b"))]);

        let input = futures::stream::iter(vec![a.clone(), a_dup, b.clone(), a.clone()]);
        let out: Vec<BindingSet> = apply_distinct(Box::pin(input)).collect().await;

        assert_eq!(out, vec![a, b]);
    }

    #[tokio::test]
    async fn apply_distinct_blank_node_collision_both_kept() {
        // End-to-end version of the injectivity regression: both solutions
        // are observably distinct, must both survive `apply_distinct`.
        let two_bindings = bs_with(&[("a", bnode("foo")), ("b", bnode("bar"))]);
        let one_binding_colliding = bs_with(&[("a", bnode("foo,b=_:bar"))]);

        let input =
            futures::stream::iter(vec![two_bindings.clone(), one_binding_colliding.clone()]);
        let out: Vec<BindingSet> = apply_distinct(Box::pin(input)).collect().await;

        assert_eq!(out.len(), 2);
        assert!(out.contains(&two_bindings));
        assert!(out.contains(&one_binding_colliding));
    }
}
