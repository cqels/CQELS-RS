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

use cqels_core::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, SeqArg, TriplePattern, SEQ_UNBOUNDED,
};
use cqels_core::stream::RdfStreamElement;

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

        let mut pattern: Option<Pattern<RdfStreamElement>> = None;
        for arg in &seq.args {
            let state_name = arg.alias.clone().unwrap_or_else(|| arg.variable.clone());
            let subject_key = format!("?{}", arg.variable);
            let triples = event_triples.get(&subject_key).cloned().unwrap_or_default();
            let condition = build_event_condition(triples, prefixes.clone());

            let stage = match (pattern.take(), arg.negated) {
                (None, _) => Pattern::begin(state_name).where_cond(condition),
                (Some(prev), false) => prev.followed_by(state_name).where_cond(condition),
                (Some(prev), true) => prev.not_followed_by(state_name).where_cond(condition),
            };
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
}
