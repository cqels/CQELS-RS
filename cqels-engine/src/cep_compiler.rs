//! Compiler from declarative `FILTER(SEQ(...))` (Java PR #36) to the
//! programmatic [`Pattern`] API.
//!
//! Maps a [`SeqConstraint`](cqels_core::parser::ast::SeqConstraint) embedded
//! in a [`CqelsQueryDefinition`] into a [`Pattern<RdfStreamElement>`] that
//! [`crate::NfaPatternProcessor`] can execute:
//!
//! - Event variables → Pattern states.
//! - SEQ ordering → `followed_by` (or `not_followed_by` for `NOT ?e`).
//! - SEQ quantifiers (`+`, `*`, `?`, `{N}`, `{N,M}`) → `Pattern::times*`/`optional`.
//! - Triple patterns on the event subject → `where_cond` predicate against
//!   the matching `RdfStreamElement`'s statement.
//! - First stream's `RANGE` window spec → `.within(duration)`.
//!
//! Mirrors Java's `org.cqels.engine.cep.CepPatternCompiler`. Single-event
//! FILTER predicates and cross-event `where_context` guards are not yet
//! ported — those require wiring an expression evaluator and are tracked as
//! follow-up.

use std::collections::HashMap;
use std::sync::Arc;

use cqels_core::expression::ast::Expression;
use cqels_core::expression::evaluator::ExpressionEvaluator;
use cqels_core::expression::parser::ExpressionParser;
use cqels_core::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, SeqArg, TriplePattern, SEQ_UNBOUNDED,
};
use cqels_core::stream::RdfStreamElement;
use cqels_model::{BindingSet, Term, Value};

use crate::cep::Pattern;

/// Errors returned by [`CepPatternCompiler::compile`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CepCompileError {
    /// The query definition has no `FILTER(SEQ(...))` constraint.
    #[error("query has no SEQ constraint")]
    NoSeqConstraint,
    /// SEQ was found but its argument list is empty.
    #[error("SEQ has no event arguments")]
    EmptySeq,
}

/// Compiles `FILTER(SEQ(...))` query definitions into CEP patterns.
pub struct CepPatternCompiler;

impl CepPatternCompiler {
    /// Compiles a query definition containing a SEQ constraint into a
    /// [`Pattern<RdfStreamElement>`] ready for [`crate::NfaPatternProcessor`].
    pub fn compile(
        query_def: &CqelsQueryDefinition,
    ) -> Result<Pattern<RdfStreamElement>, CepCompileError> {
        let seq = query_def
            .seq_constraint
            .as_ref()
            .ok_or(CepCompileError::NoSeqConstraint)?;
        if seq.args.is_empty() {
            return Err(CepCompileError::EmptySeq);
        }

        let event_triples = group_triples_by_subject(&query_def.pattern_groups);
        let prefixes = query_def.prefixes.clone();
        let var_to_event = build_variable_to_event_map(&event_triples);

        // Classify all FILTER expressions in the WHERE clause.
        let (single_event_filters, cross_event_filters) = classify_filters(
            &query_def.pattern_groups,
            &var_to_event,
            seq.args.as_slice(),
        );

        let event_triples_shared: Arc<HashMap<String, Vec<TriplePattern>>> =
            Arc::new(event_triples.clone());
        let prefixes_shared: Arc<HashMap<String, String>> = Arc::new(prefixes.clone());

        let mut pattern: Option<Pattern<RdfStreamElement>> = None;
        for (idx, arg) in seq.args.iter().enumerate() {
            let state_name = arg.alias.clone().unwrap_or_else(|| arg.variable.clone());
            let subject_key = format!("?{}", arg.variable);
            let triples = event_triples.get(&subject_key).cloned().unwrap_or_default();
            let triples_for_cond = triples.clone();
            let condition = build_event_condition(triples_for_cond, prefixes.clone());

            let mut stage = match (pattern.take(), arg.negated) {
                (None, _) => Pattern::begin(state_name).where_cond(condition),
                (Some(prev), false) => prev.followed_by(state_name).where_cond(condition),
                (Some(prev), true) => prev.not_followed_by(state_name).where_cond(condition),
            };

            // Apply single-event FILTER predicates for this event variable.
            let filters_here = single_event_filters
                .get(&arg.variable)
                .cloned()
                .unwrap_or_default();
            if !filters_here.is_empty() {
                let triples_for_filter = triples.clone();
                let event_var = arg.variable.clone();
                let prefixes_for_filter = prefixes_shared.clone();
                stage = stage.where_cond(move |element| {
                    let bindings = bind_event_variables(
                        &triples_for_filter,
                        element,
                        &event_var,
                        &prefixes_for_filter,
                    );
                    let evaluator = ExpressionEvaluator::new();
                    filters_here
                        .iter()
                        .all(|expr| evaluator.evaluate_as_bool(expr, &bindings))
                });
            }

            // Apply cross-event FILTER predicates that become applicable at
            // this position (i.e., the latest event they reference is `idx`).
            let cross_filters_here: Vec<_> = cross_event_filters
                .iter()
                .filter(|f| f.latest_event_index == idx)
                .cloned()
                .collect();
            if !cross_filters_here.is_empty() {
                let triples_map = event_triples_shared.clone();
                let prefixes_for_filter = prefixes_shared.clone();
                let seq_args = seq.args.clone();
                let current_idx = idx;
                stage = stage.where_context(move |current_event, previous| {
                    let mut bindings = BindingSet::new(current_event.timestamp);
                    for (i, prev_event) in previous.iter().enumerate().take(current_idx) {
                        let prev_var = &seq_args[i].variable;
                        let triples = triples_map
                            .get(&format!("?{prev_var}"))
                            .cloned()
                            .unwrap_or_default();
                        merge_event_variables(
                            &triples,
                            prev_event,
                            prev_var,
                            &prefixes_for_filter,
                            &mut bindings,
                        );
                    }
                    let current_var = &seq_args[current_idx].variable;
                    let current_triples = triples_map
                        .get(&format!("?{current_var}"))
                        .cloned()
                        .unwrap_or_default();
                    merge_event_variables(
                        &current_triples,
                        current_event,
                        current_var,
                        &prefixes_for_filter,
                        &mut bindings,
                    );
                    let evaluator = ExpressionEvaluator::new();
                    cross_filters_here
                        .iter()
                        .all(|f| evaluator.evaluate_as_bool(&f.expression, &bindings))
                });
            }

            pattern = Some(apply_quantifier(stage, arg));
        }

        let mut pattern = pattern.ok_or(CepCompileError::EmptySeq)?;
        if let Some(stream) = query_def.streams.first() {
            if let Some(duration) = stream.window.duration {
                pattern = pattern.within(duration);
            }
        }
        Ok(pattern)
    }
}

/// Map every variable bound by any triple pattern back to the event
/// variable (subject) that binds it.
fn build_variable_to_event_map(
    event_triples: &HashMap<String, Vec<TriplePattern>>,
) -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    for (event_var_key, triples) in event_triples {
        let event_var = event_var_key.trim_start_matches(['?', '$']).to_string();
        mapping.insert(event_var.clone(), event_var.clone());
        for tp in triples {
            if is_likely_variable(&tp.object) {
                let var = strip_var_prefix(&tp.object);
                mapping.insert(var, event_var.clone());
            }
            if is_likely_variable(&tp.predicate) {
                let var = strip_var_prefix(&tp.predicate);
                mapping.insert(var, event_var.clone());
            }
        }
    }
    mapping
}

#[derive(Clone, Debug)]
struct CrossEventFilter {
    expression: Expression,
    latest_event_index: usize,
}

/// Classify each WHERE-level FILTER expression as single-event (refers to a
/// single event variable's bound variables) or cross-event (spans multiple).
///
/// Filters whose variables map to no event (static only) are ignored, as in
/// the Java compiler.
fn classify_filters(
    pattern_groups: &[CqelsPatternGroup],
    var_to_event: &HashMap<String, String>,
    seq_args: &[SeqArg],
) -> (HashMap<String, Vec<Expression>>, Vec<CrossEventFilter>) {
    let mut single: HashMap<String, Vec<Expression>> = HashMap::new();
    let mut cross: Vec<CrossEventFilter> = Vec::new();
    let event_index: HashMap<&str, usize> = seq_args
        .iter()
        .enumerate()
        .map(|(i, arg)| (arg.variable.as_str(), i))
        .collect();

    for group in pattern_groups {
        let CqelsPatternGroup::Filter { expression } = group else {
            continue;
        };
        let Ok(expr) = ExpressionParser::parse_cqelsql(expression) else {
            continue;
        };
        let referenced_vars = collect_variables(&expr);
        let mut referenced_events: Vec<&str> = referenced_vars
            .iter()
            .filter_map(|v| var_to_event.get(v).map(|s| s.as_str()))
            .collect();
        referenced_events.sort();
        referenced_events.dedup();

        match referenced_events.len() {
            0 => continue,
            1 => {
                let evt = referenced_events[0].to_string();
                single.entry(evt).or_default().push(expr);
            }
            _ => {
                let latest = referenced_events
                    .iter()
                    .filter_map(|e| event_index.get(*e).copied())
                    .max()
                    .unwrap_or(0);
                cross.push(CrossEventFilter {
                    expression: expr,
                    latest_event_index: latest,
                });
            }
        }
    }

    (single, cross)
}

/// Walks an [`Expression`] tree and returns every variable name referenced
/// (without the leading `?`/`$`).
fn collect_variables(expr: &Expression) -> Vec<String> {
    let mut out = Vec::new();
    walk_expr(expr, &mut out);
    out
}

fn walk_expr(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        Expression::Variable(name) | Expression::Bound(name) => {
            out.push(strip_var_prefix(name));
        }
        Expression::PropertyAccess { variable, .. } => {
            out.push(strip_var_prefix(variable));
        }
        Expression::BinaryOp { left, right, .. } => {
            walk_expr(left, out);
            walk_expr(right, out);
        }
        Expression::UnaryOp { operand, .. } => walk_expr(operand, out),
        Expression::FunctionCall { args, .. } => {
            for arg in args {
                walk_expr(arg, out);
            }
        }
        Expression::If {
            condition,
            then_expr,
            else_expr,
        } => {
            walk_expr(condition, out);
            walk_expr(then_expr, out);
            walk_expr(else_expr, out);
        }
        Expression::Aggregate { argument, .. } => walk_expr(argument, out),
        Expression::Literal(_) => {}
        _ => {}
    }
}

fn strip_var_prefix(term: &str) -> String {
    term.trim_start_matches(['?', '$']).to_string()
}

/// Build a fresh [`BindingSet`] for a single event by applying its triple
/// patterns to the matched [`RdfStreamElement`].
fn bind_event_variables(
    triples: &[TriplePattern],
    element: &RdfStreamElement,
    event_var: &str,
    prefixes: &HashMap<String, String>,
) -> BindingSet {
    let mut bindings = BindingSet::new(element.timestamp);
    merge_event_variables(triples, element, event_var, prefixes, &mut bindings);
    bindings
}

/// Same as [`bind_event_variables`] but merges into an existing
/// `BindingSet` — used when bindings accumulate across previous events.
fn merge_event_variables(
    triples: &[TriplePattern],
    element: &RdfStreamElement,
    event_var: &str,
    _prefixes: &HashMap<String, String>,
    bindings: &mut BindingSet,
) {
    bindings.insert(event_var, term_to_value(&element.statement.subject));
    for tp in triples {
        if is_likely_variable(&tp.object) {
            let obj_var = strip_var_prefix(&tp.object);
            bindings.insert(obj_var, term_to_value(&element.statement.object));
        }
        if is_likely_variable(&tp.predicate) {
            let pred_var = strip_var_prefix(&tp.predicate);
            bindings.insert(
                pred_var,
                Value::Term(Term::Iri(element.statement.predicate.clone())),
            );
        }
    }
}

/// Convert an oxrdf-style [`Term`] into a typed [`Value`] for expression
/// evaluation. Numeric / boolean / string literals are coerced into their
/// typed variants so SPARQL comparisons work as expected.
fn term_to_value(term: &Term) -> Value {
    match term {
        Term::Literal(lit) => {
            let raw = lit.value();
            if let Ok(i) = raw.parse::<i64>() {
                Value::Integer(i)
            } else if let Ok(f) = raw.parse::<f64>() {
                Value::Float(f)
            } else if raw == "true" {
                Value::Boolean(true)
            } else if raw == "false" {
                Value::Boolean(false)
            } else {
                Value::String(raw.to_string())
            }
        }
        other => Value::Term(other.clone()),
    }
}

fn apply_quantifier(stage: Pattern<RdfStreamElement>, arg: &SeqArg) -> Pattern<RdfStreamElement> {
    if arg.is_single() {
        return stage;
    }
    if arg.is_optional() {
        return stage.optional();
    }
    if arg.is_one_or_more() {
        return stage.one_or_more();
    }
    if arg.is_zero_or_more() {
        return stage.times_range(0, usize::MAX);
    }
    let min = arg.min_occurrences as usize;
    if arg.max_occurrences == SEQ_UNBOUNDED {
        return stage.times_or_more(min);
    }
    let max = arg.max_occurrences as usize;
    if min == max {
        stage.times(min)
    } else {
        stage.times_range(min, max)
    }
}

fn group_triples_by_subject(
    pattern_groups: &[CqelsPatternGroup],
) -> HashMap<String, Vec<TriplePattern>> {
    let mut map: HashMap<String, Vec<TriplePattern>> = HashMap::new();
    for group in pattern_groups {
        let patterns: &[TriplePattern] = match group {
            CqelsPatternGroup::Stream { patterns, .. }
            | CqelsPatternGroup::Default { patterns }
            | CqelsPatternGroup::Static { patterns }
            | CqelsPatternGroup::NamedGraph { patterns, .. }
            | CqelsPatternGroup::Minus { patterns } => patterns.as_slice(),
            _ => continue,
        };
        for tp in patterns {
            map.entry(tp.subject.clone()).or_default().push(tp.clone());
        }
    }
    map
}

fn build_event_condition(
    triples: Vec<TriplePattern>,
    prefixes: HashMap<String, String>,
) -> impl Fn(&RdfStreamElement) -> bool + Send + Sync + 'static {
    move |element: &RdfStreamElement| {
        if triples.is_empty() {
            return true;
        }
        // Use the first triple pattern as the primary matching condition
        // (Java parity: buildEventCondition falls back to first triple).
        let primary = &triples[0];
        let stmt = &element.statement;

        if !is_likely_variable(&primary.predicate) {
            let resolved = resolve_prefix(&primary.predicate, &prefixes);
            if stmt.predicate.as_str() != resolved {
                return false;
            }
        }

        if !is_likely_variable(&primary.object) {
            let resolved = resolve_prefix(&primary.object, &prefixes);
            if format_term(&stmt.object) != resolved {
                return false;
            }
        }

        true
    }
}

fn is_likely_variable(term: &str) -> bool {
    term.starts_with('?') || term.starts_with('$')
}

fn resolve_prefix(term: &str, prefixes: &HashMap<String, String>) -> String {
    let trimmed = term.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') {
        return trimmed[1..trimmed.len() - 1].to_string();
    }
    if let Some((prefix, local)) = trimmed.split_once(':') {
        if let Some(base) = prefixes.get(prefix) {
            return format!("{base}{local}");
        }
    }
    trimmed.to_string()
}

fn format_term(term: &cqels_model::Term) -> String {
    use cqels_model::Term;
    match term {
        Term::Iri(iri) => iri.as_str().to_string(),
        Term::Literal(lit) => lit.value().to_string(),
        Term::BlankNode(b) => b.to_string(),
        _ => term.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NfaPatternProcessor;
    use cqels_core::parser::ast::{
        CqelsQueryType, CqelsStreamDefinition, OperatorHints, SelectElement, SeqConstraint,
        StreamSemantics, WindowSpec,
    };
    use cqels_model::{IriTerm, Statement, Term};

    fn make_def_with_seq(args: Vec<SeqArg>, triples: Vec<TriplePattern>) -> CqelsQueryDefinition {
        CqelsQueryDefinition {
            name: None,
            description: None,
            query_type: CqelsQueryType::Select,
            prefixes: HashMap::new(),
            streams: vec![CqelsStreamDefinition {
                name: "events".to_string(),
                window: WindowSpec::now(),
            }],
            named_windows: vec![],
            static_graphs: vec![],
            named_graphs: vec![],
            select_elements: vec![SelectElement::Variable("?e1".to_string())],
            distinct: false,
            pattern_groups: vec![CqelsPatternGroup::Stream {
                source: "events".to_string(),
                patterns: triples,
            }],
            aggregates: vec![],
            group_by_variables: vec![],
            order_by_conditions: vec![],
            limit: None,
            operator_hints: OperatorHints::default(),
            stream_semantics: StreamSemantics::default(),
            construct_template: vec![],
            seq_constraint: Some(SeqConstraint { args }),
        }
    }

    fn typed_event(subject: &str, type_iri: &str, ts: i64) -> RdfStreamElement {
        RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new(subject)),
                IriTerm::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                Term::Iri(IriTerm::new(type_iri)),
            ),
            ts,
        )
    }

    #[test]
    fn no_seq_constraint_returns_error() {
        let mut def = make_def_with_seq(vec![SeqArg::single("e1")], vec![]);
        def.seq_constraint = None;
        assert!(matches!(
            CepPatternCompiler::compile(&def),
            Err(CepCompileError::NoSeqConstraint)
        ));
    }

    fn find_state<'a>(
        head: &'a Pattern<RdfStreamElement>,
        name: &str,
    ) -> &'a Pattern<RdfStreamElement> {
        let mut node = head;
        loop {
            if node.name() == name {
                return node;
            }
            node = node
                .previous()
                .unwrap_or_else(|| panic!("state {name} not found"));
        }
    }

    #[test]
    fn compiles_two_event_sequence_to_pattern() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Alpha>".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Beta>".to_string(),
            },
        ];
        let def = make_def_with_seq(args, triples);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        // Pattern chain is built tail-first; the head returned is the LAST state.
        assert_eq!(pattern.name(), "e2");
        assert_eq!(pattern.previous().map(|p| p.name()), Some("e1"));
        assert!(pattern.previous().unwrap().previous().is_none());
    }

    #[test]
    fn alias_overrides_state_name() {
        let arg1 = SeqArg {
            variable: "e1".to_string(),
            negated: false,
            min_occurrences: 1,
            max_occurrences: 1,
            alias: Some("start".to_string()),
        };
        let arg2 = SeqArg::single("e2");
        let def = make_def_with_seq(vec![arg1, arg2], vec![]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        let begin = pattern.previous().expect("expected previous (e1 alias)");
        assert_eq!(begin.name(), "start");
    }

    async fn run_pattern(
        pattern: Pattern<RdfStreamElement>,
        events: Vec<RdfStreamElement>,
    ) -> Vec<crate::PatternMatch<RdfStreamElement>> {
        use futures::stream::StreamExt;
        let processor = NfaPatternProcessor::new(pattern);
        let stream = Box::pin(futures::stream::iter(events));
        processor.process(stream).collect().await
    }

    #[tokio::test]
    async fn sequence_matches_two_typed_events_in_order() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Alpha>".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Beta>".to_string(),
            },
        ];
        let def = make_def_with_seq(args, triples);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            typed_event("http://ex.org/a", "http://ex.org/Alpha", 100),
            typed_event("http://ex.org/b", "http://ex.org/Beta", 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert_eq!(matches.len(), 1, "expected exactly one match");
        assert_eq!(matches[0].size(), 2);
    }

    #[tokio::test]
    async fn sequence_rejects_wrong_order() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Alpha>".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
                object: "<http://ex.org/Beta>".to_string(),
            },
        ];
        let def = make_def_with_seq(args, triples);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            typed_event("http://ex.org/b", "http://ex.org/Beta", 100),
            typed_event("http://ex.org/a", "http://ex.org/Alpha", 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert!(matches.is_empty(), "should not match in wrong order");
    }

    #[test]
    fn quantifier_one_or_more_emits_repeating_state() {
        let mut args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        args[1].min_occurrences = 1;
        args[1].max_occurrences = SEQ_UNBOUNDED;
        let def = make_def_with_seq(args, vec![]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        let e2 = find_state(&pattern, "e2");
        assert_eq!(e2.quantifier().min_occurrences, 1);
        assert_eq!(e2.quantifier().max_occurrences, usize::MAX);
    }

    #[test]
    fn quantifier_times_range_emits_bounded_state() {
        let mut args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        args[1].min_occurrences = 2;
        args[1].max_occurrences = 5;
        let def = make_def_with_seq(args, vec![]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        let e2 = find_state(&pattern, "e2");
        assert_eq!(e2.quantifier().min_occurrences, 2);
        assert_eq!(e2.quantifier().max_occurrences, 5);
    }

    #[test]
    fn negated_arg_uses_not_followed_by() {
        let mut args = vec![
            SeqArg::single("e1"),
            SeqArg::single("e2"),
            SeqArg::single("e3"),
        ];
        args[1].negated = true;
        let def = make_def_with_seq(args, vec![]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        let e2 = find_state(&pattern, "e2");
        assert!(
            e2.is_negated(),
            "e2 should be a negated (not_followed_by) state"
        );
    }

    #[test]
    fn window_range_propagates_to_pattern_within() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let mut def = make_def_with_seq(args, vec![]);
        def.streams[0].window = WindowSpec::range(std::time::Duration::from_secs(10));
        let pattern = CepPatternCompiler::compile(&def).expect("compile");
        assert_eq!(
            pattern.time_window(),
            Some(std::time::Duration::from_secs(10)),
            "RANGE 10s should map to .within(10s)"
        );
    }

    // ─── FILTER predicate wiring (Java parity, follow-up) ─────────────────────

    fn numeric_event(subject: &str, predicate: &str, n: i64, ts: i64) -> RdfStreamElement {
        RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new(subject)),
                IriTerm::new(predicate),
                Term::Literal(cqels_model::LiteralTerm::new(n.to_string())),
            ),
            ts,
        )
    }

    fn make_def_with_filter(
        args: Vec<SeqArg>,
        triples: Vec<TriplePattern>,
        filter_expressions: Vec<&str>,
    ) -> CqelsQueryDefinition {
        let mut def = make_def_with_seq(args, triples);
        for expr in filter_expressions {
            def.pattern_groups.push(CqelsPatternGroup::Filter {
                expression: expr.to_string(),
            });
        }
        def
    }

    #[tokio::test]
    async fn single_event_filter_keeps_match_when_predicate_true() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s1".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s2".to_string(),
            },
        ];
        let def = make_def_with_filter(args, triples, vec!["?s1 > 50"]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            numeric_event("http://ex.org/a", "http://ex.org/speed", 80, 100),
            numeric_event("http://ex.org/b", "http://ex.org/speed", 30, 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert_eq!(matches.len(), 1, "first event (speed=80) passes filter");
    }

    #[tokio::test]
    async fn single_event_filter_drops_match_when_predicate_false() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s1".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s2".to_string(),
            },
        ];
        let def = make_def_with_filter(args, triples, vec!["?s1 > 100"]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            numeric_event("http://ex.org/a", "http://ex.org/speed", 80, 100),
            numeric_event("http://ex.org/b", "http://ex.org/speed", 30, 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert!(matches.is_empty(), "predicate fails → no match");
    }

    #[tokio::test]
    async fn cross_event_filter_keeps_match_when_increasing() {
        // Cross-event: ?s2 > ?s1 — speed is increasing.
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s1".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s2".to_string(),
            },
        ];
        let def = make_def_with_filter(args, triples, vec!["?s2 > ?s1"]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            numeric_event("http://ex.org/a", "http://ex.org/speed", 50, 100),
            numeric_event("http://ex.org/b", "http://ex.org/speed", 80, 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert_eq!(matches.len(), 1, "speed increased — pattern matches");
    }

    #[tokio::test]
    async fn cross_event_filter_drops_match_when_decreasing() {
        let args = vec![SeqArg::single("e1"), SeqArg::single("e2")];
        let triples = vec![
            TriplePattern {
                subject: "?e1".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s1".to_string(),
            },
            TriplePattern {
                subject: "?e2".to_string(),
                predicate: "<http://ex.org/speed>".to_string(),
                object: "?s2".to_string(),
            },
        ];
        let def = make_def_with_filter(args, triples, vec!["?s2 > ?s1"]);
        let pattern = CepPatternCompiler::compile(&def).expect("compile");

        let events = vec![
            numeric_event("http://ex.org/a", "http://ex.org/speed", 80, 100),
            numeric_event("http://ex.org/b", "http://ex.org/speed", 50, 200),
        ];
        let matches = run_pattern(pattern, events).await;
        assert!(
            matches.is_empty(),
            "speed decreased — cross-event filter rejects match"
        );
    }

    #[test]
    fn collect_variables_finds_all_vars_in_binary_op() {
        let expr = Expression::BinaryOp {
            op: cqels_core::expression::ast::BinaryOp::Gt,
            left: Box::new(Expression::Variable("?s2".to_string())),
            right: Box::new(Expression::Variable("?s1".to_string())),
        };
        let mut vars = collect_variables(&expr);
        vars.sort();
        assert_eq!(vars, vec!["s1".to_string(), "s2".to_string()]);
    }
}
