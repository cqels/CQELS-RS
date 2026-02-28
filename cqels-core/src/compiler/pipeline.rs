//! Shared pipeline utilities for query compilation and execution.
//!
//! Functions that are common to both CqelsQL and CypherQL query compilers:
//! triple pattern matching, filtering, binding, aggregation, ordering,
//! projection, and distinct.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use futures::{Stream, StreamExt};

use cqels_model::term::IriTerm;
use cqels_model::{BindingSet, Statement, Term, Value};

use crate::expression::ast::{AggregateExprFunction, Expression};
use crate::expression::evaluator::ExpressionEvaluator;
use crate::parser::ast::SortDirection;

/// Specification for an aggregate in the pipeline.
#[derive(Clone, Debug)]
pub struct PipelineAggregateSpec {
    pub function: AggregateExprFunction,
    pub argument: Expression,
    pub alias: String,
    pub distinct: bool,
}

/// Attempts to match a triple pattern against a statement, producing
/// a `BindingSet` with variable bindings if successful.
///
/// Variables in the pattern (starting with `?` or `$`) are bound to the
/// corresponding statement components. Constants must match exactly.
pub fn match_triple_pattern(
    pattern: &crate::parser::ast::TriplePattern,
    stmt: &Statement,
    prefixes: &HashMap<String, String>,
    timestamp: i64,
) -> Option<BindingSet> {
    let mut bindings = BindingSet::new(timestamp);

    // Match subject
    if !match_term_component(
        &pattern.subject,
        &stmt.subject,
        prefixes,
        &mut bindings,
    ) {
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

/// Converts an RDF Term to a Value.
fn term_to_value(term: &Term) -> Value {
    match term {
        Term::Iri(iri) => Value::Term(Term::Iri(iri.clone())),
        Term::BlankNode(bn) => Value::Term(Term::BlankNode(bn.clone())),
        Term::Literal(lit) => {
            // Try to parse numeric literals
            let val = lit.value();
            if let Some(dt) = lit.datatype() {
                match dt {
                    "http://www.w3.org/2001/XMLSchema#integer"
                    | "http://www.w3.org/2001/XMLSchema#int"
                    | "http://www.w3.org/2001/XMLSchema#long" => {
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
            // Try numeric parsing for untyped literals
            if let Ok(i) = val.parse::<i64>() {
                return Value::Integer(i);
            }
            if let Ok(f) = val.parse::<f64>() {
                return Value::Float(f);
            }
            Value::String(val.to_string())
        }
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
    }
}

/// Applies filter expressions to a stream of binding sets.
pub fn apply_filters(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
    filters: &[Expression],
    evaluator: &ExpressionEvaluator,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let filters = filters.to_vec();
    let evaluator = evaluator.clone();

    Box::pin(stream.filter(move |bs| {
        let pass = filters
            .iter()
            .all(|f| evaluator.evaluate_as_bool(f, bs));
        futures::future::ready(pass)
    }))
}

/// Applies bind expressions to a stream of binding sets.
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

    // Group elements by group-by variables
    let mut groups: HashMap<Vec<String>, Vec<BindingSet>> = HashMap::new();

    for bs in &elements {
        let key: Vec<String> = group_by
            .iter()
            .map(|var| {
                let var_name = var
                    .strip_prefix('?')
                    .or_else(|| var.strip_prefix('$'))
                    .unwrap_or(var);
                bs.get(var_name)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect();
        groups.entry(key).or_default().push(bs.clone());
    }

    let mut results = Vec::new();

    for (_key, group) in groups {
        let mut result = BindingSet::new(
            group.iter().map(|bs| bs.timestamp()).max().unwrap_or(0),
        );

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

            let agg_result = compute_aggregate(agg.function, &values);
            result.insert(alias, agg_result);
        }

        results.push(result);
    }

    results
}

/// Computes a single aggregate value.
fn compute_aggregate(function: AggregateExprFunction, values: &[Value]) -> Value {
    match function {
        AggregateExprFunction::Count => Value::Integer(values.len() as i64),
        AggregateExprFunction::Sum => {
            let sum: f64 = values
                .iter()
                .filter_map(|v| v.as_numeric())
                .sum();
            if values.iter().all(|v| matches!(v, Value::Integer(_))) {
                Value::Integer(sum as i64)
            } else {
                Value::Float(sum)
            }
        }
        AggregateExprFunction::Avg => {
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| v.as_numeric())
                .collect();
            if nums.is_empty() {
                Value::Null
            } else {
                Value::Float(nums.iter().sum::<f64>() / nums.len() as f64)
            }
        }
        AggregateExprFunction::Min => {
            values
                .iter()
                .filter_map(|v| v.as_numeric())
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::Float)
                .unwrap_or(Value::Null)
        }
        AggregateExprFunction::Max => {
            values
                .iter()
                .filter_map(|v| v.as_numeric())
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::Float)
                .unwrap_or(Value::Null)
        }
        AggregateExprFunction::Collect => {
            // COLLECT returns a string representation of all values
            let strs: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            Value::String(format!("[{}]", strs.join(", ")))
        }
    }
}

/// Applies ORDER BY and LIMIT to a batch of binding sets.
pub fn apply_order_and_limit(
    mut elements: Vec<BindingSet>,
    order_by: &[(Expression, SortDirection)],
    evaluator: &ExpressionEvaluator,
    limit: Option<u64>,
) -> Vec<BindingSet> {
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

/// Applies SELECT projection to a stream of binding sets.
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

/// Applies DISTINCT to a stream of binding sets.
pub fn apply_distinct(
    stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>>,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(HashSet::<String>::new()));

    Box::pin(stream.filter(move |bs| {
        // Use display representation as hash key
        let key = bs.to_string();
        let is_new = seen.lock().unwrap().insert(key);
        futures::future::ready(is_new)
    }))
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

    fn make_triple_pattern(subject: &str, predicate: &str, object: &str) -> crate::parser::ast::TriplePattern {
        crate::parser::ast::TriplePattern {
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object: object.to_string(),
        }
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
        assert_eq!(compute_aggregate(AggregateExprFunction::Count, &values), Value::Integer(3));
    }

    #[test]
    fn test_compute_aggregate_sum() {
        let values = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)];
        assert_eq!(compute_aggregate(AggregateExprFunction::Sum, &values), Value::Integer(60));
    }

    #[test]
    fn test_compute_aggregate_avg() {
        let values = vec![Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)];
        assert_eq!(compute_aggregate(AggregateExprFunction::Avg, &values), Value::Float(20.0));
    }

    #[test]
    fn test_resolve_term_prefix() {
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());
        assert_eq!(resolve_term("ex:name", &prefixes), "http://example.org/name");
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
}
