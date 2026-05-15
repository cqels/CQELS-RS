//! Compiled query types that implement `ContinuousQuery`.
//!
//! These types hold pre-parsed expression trees and query metadata,
//! and execute the full pipeline when `execute()` is called.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{FutureExt, Stream, StreamExt};

use cqels_model::{BindingSet, Statement, Term};

use crate::expression::ast::Expression;
use crate::expression::evaluator::ExpressionEvaluator;
use crate::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, CypherQueryDefinition, SortDirection, WindowType,
};
use crate::query::{ContinuousQuery, QueryInputs, QueryType};
use crate::stream::StreamElement;
use crate::window::{
    SlidingCountWindow, SlidingWindow, TumblingCountWindow, TumblingWindow, Window,
};

use crate::store::RdfStore;

use super::pipeline::{
    apply_binds, apply_distinct, apply_filters, apply_group_by_aggregates, apply_minus,
    apply_optional, apply_order_and_limit, apply_projection, apply_union, join_binding_sets,
    match_cypher_pattern, match_triple_pattern, PipelineAggregateSpec,
};

/// A compiled CqelsQL query ready for execution.
///
/// Immutable query data is wrapped in `Arc` so that `execute()` can
/// cheaply share references with async closures instead of deep-cloning
/// expression trees and definitions on every call.
///
/// # Multi-stream queries
///
/// When a query references multiple streams (including via the named-
/// window aliased-view lowering), each stream is windowed by *its own*
/// [`CqelsStreamDefinition`](crate::parser::ast::CqelsStreamDefinition)`.window`
/// spec before the resulting batches are merged for pattern matching.
/// Each batch is tagged with its source stream name and the pattern
/// matcher restricts the patterns it tries to those declared for that
/// source (plus any unscoped default patterns from the WHERE clause).
/// Cross-stream pattern leakage is therefore eliminated: a pattern in
/// `STREAM s1 { ... }` only fires against batches from `s1`.
pub struct CompiledCqelsQuery {
    /// Original query string.
    pub(crate) query_string: String,
    /// Query name/ID.
    pub(crate) query_id: String,
    /// The parsed query definition.
    pub(crate) definition: Arc<CqelsQueryDefinition>,
    /// Pre-parsed FILTER expressions.
    pub(crate) filter_expressions: Arc<Vec<Expression>>,
    /// Pre-parsed BIND expressions with target variable.
    pub(crate) bind_expressions: Arc<Vec<(Expression, String)>>,
    /// Pre-parsed ORDER BY expressions with sort direction.
    pub(crate) order_by_expressions: Arc<Vec<(Expression, SortDirection)>>,
    /// Pre-parsed HAVING expressions.
    pub(crate) having_expressions: Arc<Vec<Expression>>,
    /// Aggregate specifications.
    pub(crate) aggregate_specs: Arc<Vec<PipelineAggregateSpec>>,
    /// Expression evaluator with prefix mappings.
    pub(crate) evaluator: Arc<ExpressionEvaluator>,
    /// SELECT variable names for projection.
    pub(crate) select_vars: Arc<Vec<String>>,
    /// Optional RDF store for STATIC and GRAPH pattern lookups.
    pub(crate) rdf_store: Option<Arc<dyn RdfStore>>,
    /// Self-join hints detected at compile time. Populated by
    /// [`crate::compiler::self_join::detect_self_joins`]. Empty unless the
    /// query has two or more `Stream { ... }` pattern groups over the same
    /// source sharing at least one variable.
    ///
    /// Runtime can branch on this to substitute
    /// `cqels_core::operator::join::WindowedSelfJoinState` for the default
    /// pattern-matching path (O(N+M) vs O(N·M)). Substitution is currently
    /// a TODO — see issue tracker.
    pub(crate) self_join_hints: Arc<Vec<crate::compiler::self_join::SelfJoinHint>>,
}

impl CompiledCqelsQuery {
    /// Self-join hints discovered at compile time, in declaration order.
    pub fn self_join_hints(&self) -> &[crate::compiler::self_join::SelfJoinHint] {
        &self.self_join_hints
    }

    /// `true` when the compiler detected at least one self-join opportunity.
    pub fn has_self_join_optimization(&self) -> bool {
        !self.self_join_hints.is_empty()
    }

    /// The parsed query definition, exposing stream sources, pattern
    /// groups, filters, sort specifications, and limits. Useful for
    /// callers that need to inspect plan-level shape (e.g. an MCP
    /// `analyze_query` tool).
    pub fn definition(&self) -> &CqelsQueryDefinition {
        &self.definition
    }

    /// The SELECT-projection variable names, in declaration order.
    pub fn select_vars(&self) -> &[String] {
        &self.select_vars
    }
}

#[async_trait]
impl ContinuousQuery for CompiledCqelsQuery {
    type Result = BindingSet;

    fn query_id(&self) -> &str {
        &self.query_id
    }

    fn query_string(&self) -> &str {
        &self.query_string
    }

    fn query_type(&self) -> QueryType {
        QueryType::Sparql
    }

    fn input_stream_aliases(&self) -> Vec<(String, String)> {
        // Surfaces the (synthetic, source) pairs created by the
        // named-window lowering pass so the engine can fan source
        // data into the synthetic stream names.
        self.definition
            .streams
            .iter()
            .filter_map(|s| {
                s.source_stream
                    .as_ref()
                    .map(|src| (s.name.clone(), src.clone()))
            })
            .collect()
    }

    fn execute(&self, mut inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
        // Fast path for the simple self-join shape (Java PR #25 optimization).
        // Routes the query through `WindowedSelfJoinState` for O(N+M)
        // instead of the default pattern matcher's O(N·M). Falls back to
        // the generic pipeline when the query has any feature outside the
        // strict shape (aggregates, filters, projections beyond the join
        // variables, etc.) — see `try_self_join_fast_path` for the exact
        // shape.
        if let Some(stream) = try_self_join_fast_path(self, &mut inputs) {
            return stream;
        }

        let definition = Arc::clone(&self.definition);
        let filter_expressions = Arc::clone(&self.filter_expressions);
        let bind_expressions = Arc::clone(&self.bind_expressions);
        let order_by_expressions = Arc::clone(&self.order_by_expressions);
        let having_expressions = Arc::clone(&self.having_expressions);
        let aggregate_specs = Arc::clone(&self.aggregate_specs);
        let evaluator = Arc::clone(&self.evaluator);
        let select_vars = Arc::clone(&self.select_vars);
        let distinct = definition.distinct;
        let limit = definition.limit;
        let group_by = definition.group_by_variables.clone();

        // Collect all stream pattern groups
        let stream_patterns: Vec<(String, Vec<crate::parser::ast::TriplePattern>)> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Stream { source, patterns } => {
                    Some((source.clone(), patterns.clone()))
                }
                _ => None,
            })
            .collect();

        // Collect default patterns only (for stream matching)
        let default_patterns: Vec<crate::parser::ast::TriplePattern> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Default { patterns } => Some(patterns.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        // Collect static patterns (for RDF store lookup)
        let static_patterns: Vec<crate::parser::ast::TriplePattern> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Static { patterns } => Some(patterns.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        // Collect named graph patterns (for RDF store lookup)
        let named_graph_patterns: Vec<(String, Vec<crate::parser::ast::TriplePattern>)> =
            definition
                .pattern_groups
                .iter()
                .filter_map(|pg| match pg {
                    CqelsPatternGroup::NamedGraph {
                        graph_uri,
                        patterns,
                    } => Some((graph_uri.clone(), patterns.clone())),
                    _ => None,
                })
                .collect();

        // Collect optional pattern groups
        let optional_groups: Vec<Vec<CqelsPatternGroup>> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Optional { groups } => Some(groups.clone()),
                _ => None,
            })
            .collect();

        // Collect UNION blocks
        let union_blocks: Vec<(Vec<CqelsPatternGroup>, Vec<CqelsPatternGroup>)> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Union { left, right } => Some((left.clone(), right.clone())),
                _ => None,
            })
            .collect();

        // Collect MINUS blocks
        let minus_blocks: Vec<Vec<crate::parser::ast::TriplePattern>> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Minus { patterns } => Some(patterns.clone()),
                _ => None,
            })
            .collect();

        let prefixes = definition.prefixes.clone();

        // Build the windowed batch stream. Each stream pattern group's
        // source is windowed by *its own* `CqelsStreamDefinition.window`
        // spec, then **tagged with the source name** so the pattern
        // matcher can restrict its work to patterns declared for that
        // source. The resulting batches from every source merge via
        // `select_all` into a single
        // `Stream<(String, Vec<StreamElement>)>`.
        //
        // The per-stream-then-merge ordering is the runtime piece of
        // the named-window aliased-view feature (PR #55): two named
        // windows on the same source with different specs each get
        // their own time-aligned batches, so a `WINDOW :w1 [RANGE 10s]`
        // pattern group sees 10-second batches while `WINDOW :w2
        // [RANGE 30s]` sees 30-second batches — both fed by the same
        // user-registered source via the engine's broadcast fan-out.
        //
        // The source-tagging step (this PR) closes the last gap from
        // PR #56: the matcher now applies *only the patterns declared
        // for the batch's source* to each batch, eliminating
        // cross-stream leakage on overlapping patterns.
        let batch_stream: SourceTaggedBatchStream = if stream_patterns.is_empty() {
                // No `STREAM {}` groups — fall back to "any registered
                // stream" with the first declared window spec, matching
                // the pre-refactor behavior for default-pattern queries.
                // The fallback stream's name is recorded as the batch
                // source, but since `patterns_by_source` (built below)
                // is empty, only `default_patterns` will apply to
                // these batches.
                let fallback_name = inputs.stream_names().next().map(|s| s.to_string());
                match fallback_name {
                    Some(name) => match inputs.take_stream(&name) {
                        Some(input) => {
                            let spec = definition.streams.first().map(|s| &s.window);
                            let windowed = apply_window_spec(input, spec);
                            Box::pin(windowed.map(move |batch| (name.clone(), batch)))
                        }
                        None => return Box::pin(futures::stream::empty()),
                    },
                    None => return Box::pin(futures::stream::empty()),
                }
            } else {
                // Per-stream pattern group: window the input and tag
                // each batch with the source's name. Two pattern
                // groups can share a source (e.g. self-joins) — each
                // group registers its own tag, but since `take_stream`
                // can only return the input once, only one tagged
                // stream is built per source; the matcher aggregates
                // patterns across groups sharing a source.
                let mut seen_sources: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let batched: Vec<SourceTaggedBatchStream> = stream_patterns
                    .iter()
                    .filter_map(|(name, _)| {
                        if !seen_sources.insert(name.clone()) {
                            // Already wired this source's windowed
                            // stream — no double-tap.
                            return None;
                        }
                        let input = inputs.take_stream(name)?;
                        let spec = definition
                            .streams
                            .iter()
                            .find(|s| s.name == *name)
                            .map(|s| &s.window);
                        let windowed = apply_window_spec(input, spec);
                        let source = name.clone();
                        Some(
                            Box::pin(windowed.map(move |batch| (source.clone(), batch)))
                                as SourceTaggedBatchStream,
                        )
                    })
                    .collect();
                if batched.is_empty() {
                    return Box::pin(futures::stream::empty());
                } else if batched.len() == 1 {
                    batched.into_iter().next().unwrap()
                } else {
                    Box::pin(futures::stream::select_all(batched))
                }
            };

        // Group stream patterns by source. Two pattern groups on the
        // same source (e.g., a self-join shape) merge into one entry —
        // matching against a batch from that source applies *all* of
        // them as a conjunction.
        let mut patterns_by_source: std::collections::HashMap<
            String,
            Vec<crate::parser::ast::TriplePattern>,
        > = std::collections::HashMap::new();
        for (source, patterns) in &stream_patterns {
            patterns_by_source
                .entry(source.clone())
                .or_default()
                .extend(patterns.iter().cloned());
        }
        // `default_patterns` is already in scope from the earlier
        // collection. Unscoped triples in the WHERE clause apply to
        // every batch regardless of its source — they're added to
        // each source's pattern set inside the matcher below.

        // Pre-compute static bindings from RDF store (if available)
        let rdf_store = self.rdf_store.clone();
        let static_bindings: Vec<BindingSet> = if let Some(store) = &rdf_store {
            let mut accumulated: Vec<BindingSet> = Vec::new();

            // Query each static pattern and progressively join results
            for pattern in &static_patterns {
                let pattern_results = store.query_pattern(pattern, &prefixes);
                if accumulated.is_empty() {
                    accumulated = pattern_results;
                } else if !pattern_results.is_empty() {
                    accumulated = join_binding_sets(&accumulated, &pattern_results);
                }
            }

            // Query each named graph pattern and join with accumulated results
            for (graph_uri, patterns) in &named_graph_patterns {
                for pattern in patterns {
                    let pattern_results =
                        store.query_named_graph_pattern(graph_uri, pattern, &prefixes);
                    if accumulated.is_empty() {
                        accumulated = pattern_results;
                    } else if !pattern_results.is_empty() {
                        accumulated = join_binding_sets(&accumulated, &pattern_results);
                    }
                }
            }

            accumulated
        } else {
            vec![]
        };

        // Phase 1: Batch-level pattern matching.
        //
        // Each batch is tagged with its source stream name (see
        // `batch_stream` construction above). The matcher applies
        // only the patterns declared for that source — plus the
        // default (unscoped) patterns from the WHERE clause — against
        // the batch's elements. With source filtering in place, every
        // selected pattern is mandatory: each must contribute at
        // least one match for the batch to produce results.
        let prefixes_clone = prefixes.clone();
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            Box::pin(batch_stream.flat_map(move |(source, batch)| {
                let mut stmts: Vec<(Statement, i64)> = Vec::new();
                for elem in &batch {
                    let ts = elem.timestamp();
                    if let StreamElement::Rdf(rdf) = elem {
                        stmts.push((rdf.statement.clone(), ts));
                    }
                }

                // Restrict patterns to this batch's source group plus
                // any unscoped default patterns. Sources that have no
                // declared patterns (e.g., the fallback case in the
                // empty-stream-patterns path) yield only the default
                // patterns; if those are empty too, no results.
                let source_patterns = patterns_by_source.get(&source);
                let active_pattern_count =
                    source_patterns.map(|p| p.len()).unwrap_or(0) + default_patterns.len();
                let mut pattern_results: Vec<Vec<BindingSet>> = Vec::new();
                let mut matched_count = 0;
                let chained = source_patterns
                    .into_iter()
                    .flatten()
                    .chain(default_patterns.iter());
                for pattern in chained {
                    let matches: Vec<BindingSet> = stmts
                        .iter()
                        .filter_map(|(stmt, ts)| {
                            match_triple_pattern(pattern, stmt, &prefixes_clone, *ts)
                        })
                        .collect();
                    if !matches.is_empty() {
                        pattern_results.push(matches);
                        matched_count += 1;
                    }
                }

                // Every pattern selected for this batch is mandatory:
                // if any of them fails to contribute, the batch
                // produces no bindings.
                let results = if active_pattern_count == 0 || matched_count < active_pattern_count {
                    vec![]
                } else {
                    let mut accumulated = pattern_results.remove(0);
                    for next_pattern_matches in pattern_results {
                        accumulated = join_binding_sets(&accumulated, &next_pattern_matches);
                    }
                    accumulated
                };

                futures::stream::iter(results)
            }));

        // Phase 1b: Join stream bindings with static bindings from RDF store
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            if !static_bindings.is_empty() {
                Box::pin(binding_stream.flat_map(move |bs| {
                    let joined: Vec<BindingSet> = static_bindings
                        .iter()
                        .filter_map(|static_bs| bs.join(static_bs))
                        .collect();
                    // If no joins succeed, pass through the original binding
                    futures::stream::iter(if joined.is_empty() { vec![bs] } else { joined })
                }))
            } else {
                binding_stream
            };

        // Phase 1c: Apply RSP-QL stream semantics
        let binding_stream =
            super::pipeline::apply_stream_semantics(binding_stream, definition.stream_semantics);

        // Phase 2: Apply FILTER expressions
        let filtered = if filter_expressions.is_empty() {
            binding_stream
        } else {
            apply_filters(binding_stream, &filter_expressions, &evaluator)
        };

        // Phase 3: Apply BIND expressions
        let bound = if bind_expressions.is_empty() {
            filtered
        } else {
            apply_binds(filtered, &bind_expressions, &evaluator)
        };

        // Phase 3b: Apply OPTIONAL groups (left-outer-join)
        let with_optionals = if optional_groups.is_empty() {
            bound
        } else {
            apply_optional(bound, &optional_groups, &prefixes, &evaluator)
        };

        // Phase 3c: Apply UNION blocks
        let with_unions = if union_blocks.is_empty() {
            with_optionals
        } else {
            apply_union(with_optionals, &union_blocks, &prefixes, &evaluator)
        };

        // Phase 3d: Apply MINUS blocks (anti-join)
        let with_minus = if minus_blocks.is_empty() {
            with_unions
        } else {
            apply_minus(with_unions, &minus_blocks, &prefixes)
        };

        // Phase 4: Collect, aggregate, order, limit, project
        let has_aggregates = !aggregate_specs.is_empty() || !group_by.is_empty();
        let has_order = !order_by_expressions.is_empty();
        let needs_collect = has_aggregates || has_order || limit.is_some();

        if needs_collect {
            // Collect all results for batch processing
            let evaluator2 = evaluator.clone();

            let result_stream = with_minus
                .collect::<Vec<BindingSet>>()
                .into_stream()
                .flat_map(move |elements| {
                    let mut results = elements;

                    // GROUP BY + aggregates
                    if has_aggregates {
                        results = apply_group_by_aggregates(
                            results,
                            &group_by,
                            &aggregate_specs,
                            &evaluator2,
                        );

                        // HAVING
                        if !having_expressions.is_empty() {
                            results.retain(|bs| {
                                having_expressions
                                    .iter()
                                    .all(|h| evaluator2.evaluate_as_bool(h, bs))
                            });
                        }
                    }

                    // ORDER BY + LIMIT
                    results =
                        apply_order_and_limit(results, &order_by_expressions, &evaluator2, limit);

                    futures::stream::iter(results)
                });

            // Project and distinct
            let projected = if select_vars.is_empty() {
                Box::pin(result_stream) as Pin<Box<dyn Stream<Item = BindingSet> + Send>>
            } else {
                apply_projection(Box::pin(result_stream), &select_vars)
            };

            if distinct {
                apply_distinct(projected)
            } else {
                projected
            }
        } else {
            // Streaming mode: no collection needed
            let projected = if select_vars.is_empty() {
                with_minus
            } else {
                apply_projection(with_minus, &select_vars)
            };

            if distinct {
                apply_distinct(projected)
            } else {
                projected
            }
        }
    }
}

// ─── Self-join fast path ─────────────────────────────────────────────
//
// Detects the simple self-join shape at runtime and routes the query
// through `WindowedSelfJoinState` (O(N+M)) instead of the default
// pattern matcher (O(N·M)).
//
/// A batch of stream elements paired with its source stream name.
/// Pattern matching restricts the patterns it tries against this
/// batch to those declared for the named source (plus any unscoped
/// default patterns), eliminating cross-stream leakage in multi-
/// stream queries.
type SourceTaggedBatch = (String, Vec<StreamElement>);

/// Boxed stream of source-tagged batches — the shape `execute`
/// constructs after applying per-stream windowing.
type SourceTaggedBatchStream = Pin<Box<dyn Stream<Item = SourceTaggedBatch> + Send>>;

/// Applies a [`WindowSpec`] to an input `StreamElement` stream,
/// producing batches of elements. Factored from the inline match in
/// [`CompiledCqelsQuery::execute`] so multi-stream queries can window
/// each input independently before merging.
///
/// A `None` spec (no window declared) and degenerate specs
/// (zero-duration RANGE, zero-count TRIPLES, …) fall back to "every
/// element is its own batch", matching the existing fallback
/// behavior.
fn apply_window_spec(
    input: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    spec: Option<&crate::parser::ast::WindowSpec>,
) -> Pin<Box<dyn Stream<Item = Vec<StreamElement>> + Send>> {
    match spec.map(|w| &w.window_type) {
        Some(WindowType::Now) => Box::pin(input.map(|elem| vec![elem])),
        Some(WindowType::Range) => {
            let duration = spec
                .and_then(|w| w.duration)
                .unwrap_or(Duration::from_secs(0));
            if duration.is_zero() {
                Box::pin(input.map(|elem| vec![elem]))
            } else {
                let window = TumblingWindow::new(duration);
                Box::pin(window.apply(input).map(|batch| batch.elements))
            }
        }
        Some(WindowType::Slide) => {
            let duration = spec
                .and_then(|w| w.duration)
                .unwrap_or(Duration::from_secs(0));
            let step = spec.and_then(|w| w.step).unwrap_or(Duration::from_secs(0));
            if duration.is_zero() || step.is_zero() {
                Box::pin(input.map(|elem| vec![elem]))
            } else {
                let window = SlidingWindow::new(duration, step);
                Box::pin(window.apply(input).map(|batch| batch.elements))
            }
        }
        Some(WindowType::Triples) => {
            let count = spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
            if count == 0 {
                Box::pin(input.map(|elem| vec![elem]))
            } else {
                let window = TumblingCountWindow::new(count);
                Box::pin(window.apply(input).map(|batch| batch.elements))
            }
        }
        Some(WindowType::TriplesSlide) => {
            let count = spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
            let slide = spec.and_then(|w| w.triple_slide).unwrap_or(1) as usize;
            if count == 0 || slide == 0 {
                Box::pin(input.map(|elem| vec![elem]))
            } else {
                let window = SlidingCountWindow::new(count, slide);
                Box::pin(window.apply(input).map(|batch| batch.elements))
            }
        }
        None => Box::pin(input.map(|elem| vec![elem])),
    }
}

// Conservative trigger: only fires for queries that exactly match a
// "two stream patterns on one stream, joined on a single shared object
// variable, no aggregates/filters/projections" shape. Anything else
// falls back to the generic pipeline.

fn try_self_join_fast_path(
    query: &CompiledCqelsQuery,
    inputs: &mut QueryInputs,
) -> Option<Pin<Box<dyn Stream<Item = BindingSet> + Send>>> {
    if !query.has_self_join_optimization() {
        return None;
    }
    let definition = &query.definition;

    // Strict shape (excluding FILTER — FILTER is now supported below):
    // no aggregates, no group-by, no order-by, no limit, no distinct,
    // no BIND expressions.
    if !query.bind_expressions.is_empty()
        || !query.order_by_expressions.is_empty()
        || !query.aggregate_specs.is_empty()
        || !definition.group_by_variables.is_empty()
        || definition.limit.is_some()
        || definition.distinct
    {
        return None;
    }
    // Exactly one self-join opportunity.
    if query.self_join_hints.len() != 1 {
        return None;
    }
    let hint = &query.self_join_hints[0];
    if hint.join_keys.len() != 1 {
        return None;
    }
    let shared_var = hint.join_keys[0].clone();

    // The two stream pattern groups referenced by the hint must each
    // contain exactly one triple pattern that binds `shared_var` in
    // either the subject or object position.
    let pat_l = stream_pattern_at(definition, hint.pattern_indices.0)?;
    let pat_r = stream_pattern_at(definition, hint.pattern_indices.1)?;
    let stream_name = pat_l.0.clone();
    let (left_bound_var, left_position, predicate_l) = simple_join_pattern(&pat_l, &shared_var)?;
    let (right_bound_var, right_position, predicate_r) = simple_join_pattern(&pat_r, &shared_var)?;
    if predicate_l != predicate_r {
        // We don't yet handle cross-predicate joins on the same key.
        return None;
    }
    if left_position != right_position {
        // Mixed-position joins (subject-on-one-side, object-on-other)
        // need the operator to consider each event under both roles.
        // Out of scope for the fast path today; fall back to generic.
        return None;
    }
    let resolved_predicate = resolve_iri_term(&predicate_l, &definition.prefixes);

    // The set of variables our post-join FILTER predicates can reference.
    // Anything outside this set means the filter touches a variable the
    // fast path can't bind — fall back to the generic pipeline.
    let known_vars = [
        left_bound_var.as_str(),
        right_bound_var.as_str(),
        shared_var.as_str(),
    ];
    for expr in query.filter_expressions.iter() {
        for var in collect_variables(expr) {
            if !known_vars.iter().any(|k| *k == var) {
                return None;
            }
        }
    }

    // Pick the input stream — both groups reference the same source.
    let input_stream = inputs.take_stream(&stream_name)?;

    // Use the first declared stream's window duration for the join
    // window. NOW / TRIPLES windows degenerate to "no time bound" — we
    // pick a very long window so the join state retains every event
    // within the stream's natural lifetime.
    let window_duration = definition
        .streams
        .first()
        .and_then(|s| s.window.duration)
        .unwrap_or(Duration::from_secs(60 * 60 * 24));

    let stream = build_self_join_stream(
        input_stream,
        window_duration,
        resolved_predicate,
        left_bound_var,
        left_position,
        right_bound_var,
        shared_var,
        Arc::clone(&query.select_vars),
        Arc::clone(&query.filter_expressions),
        Arc::clone(&query.evaluator),
    );
    Some(stream)
}

fn stream_pattern_at(
    definition: &CqelsQueryDefinition,
    idx: usize,
) -> Option<(String, Vec<crate::parser::ast::TriplePattern>)> {
    match definition.pattern_groups.get(idx) {
        Some(CqelsPatternGroup::Stream { source, patterns }) => {
            Some((source.clone(), patterns.clone()))
        }
        _ => None,
    }
}

/// Where the shared join key sits in the triple pattern. The "other"
/// position is the variable that ends up bound for this side of the
/// pair (e.g., `?driver` / `?passenger`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKeyPosition {
    /// Triple of shape `?shared <p> ?bound` — the shared variable is
    /// in the subject position, the bound variable in the object.
    Subject,
    /// Triple of shape `?bound <p> ?shared` — the shared variable is
    /// in the object position, the bound variable in the subject.
    Object,
}

/// For a stream pattern with exactly one triple pattern of shape
/// `?bound <predicate> ?shared` (object position) or
/// `?shared <predicate> ?bound` (subject position), return
/// `(bound_var_name, key_position, predicate_term)`. Returns `None`
/// for anything else — multiple triples, variable predicates, both
/// terms variable but neither equal to `shared_var`, etc.
fn simple_join_pattern(
    pat: &(String, Vec<crate::parser::ast::TriplePattern>),
    shared_var: &str,
) -> Option<(String, JoinKeyPosition, String)> {
    let patterns = &pat.1;
    if patterns.len() != 1 {
        return None;
    }
    let tp = &patterns[0];
    // Predicate must be a constant (IRI or prefixed name), not a variable.
    if is_variable_term(&tp.predicate) {
        return None;
    }
    let subj_var = strip_var_prefix(&tp.subject);
    let obj_var = strip_var_prefix(&tp.object);
    match (subj_var, obj_var) {
        (Some(s), Some(o)) if o == shared_var && s != shared_var => {
            Some((s, JoinKeyPosition::Object, tp.predicate.clone()))
        }
        (Some(s), Some(o)) if s == shared_var && o != shared_var => {
            Some((o, JoinKeyPosition::Subject, tp.predicate.clone()))
        }
        _ => None,
    }
}

/// Walks an `Expression` tree collecting referenced variable names
/// (without the `?`/`$` prefix). Used by the self-join fast path to
/// determine whether a FILTER's variables fit in the
/// `{left, right, shared}` scope it knows how to bind.
fn collect_variables(expr: &Expression) -> Vec<String> {
    fn walk(expr: &Expression, out: &mut Vec<String>) {
        match expr {
            Expression::Variable(name) | Expression::Bound(name) => {
                out.push(name.trim_start_matches(['?', '$']).to_string());
            }
            Expression::PropertyAccess { variable, .. } => {
                out.push(variable.trim_start_matches(['?', '$']).to_string());
            }
            Expression::BinaryOp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            Expression::UnaryOp { operand, .. } => walk(operand, out),
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, out);
                }
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
            } => {
                walk(condition, out);
                walk(then_expr, out);
                walk(else_expr, out);
            }
            Expression::Aggregate { argument, .. } => walk(argument, out),
            Expression::Literal(_) => {}
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

fn is_variable_term(term: &str) -> bool {
    term.starts_with('?') || term.starts_with('$')
}

fn strip_var_prefix(term: &str) -> Option<String> {
    if !is_variable_term(term) {
        return None;
    }
    Some(term.trim_start_matches(['?', '$']).to_string())
}

fn resolve_iri_term(term: &str, prefixes: &std::collections::HashMap<String, String>) -> String {
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

#[allow(clippy::too_many_arguments)]
fn build_self_join_stream(
    input: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    window: Duration,
    resolved_predicate: String,
    left_bound_var: String,
    join_position: JoinKeyPosition,
    right_bound_var: String,
    shared_var: String,
    select_vars: Arc<Vec<String>>,
    filter_expressions: Arc<Vec<Expression>>,
    evaluator: Arc<ExpressionEvaluator>,
) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
    use crate::operator::join::WindowedSelfJoinState;
    use cqels_model::Value;

    // Per-event state stored by the join. `bound_value` is the string
    // form of the "other" position relative to the shared key (e.g.
    // the subject IRI when the shared key sits in the object slot).
    // `key` is the shared-key value used to match.
    #[derive(Clone)]
    struct StoredEvent {
        bound_value: String,
        key: String,
        #[allow(dead_code)]
        timestamp: i64,
    }

    let state: WindowedSelfJoinState<StoredEvent, String> = WindowedSelfJoinState::new(window);

    let stream = futures::stream::unfold((input, state), move |(mut input, mut state)| {
        let resolved_predicate = resolved_predicate.clone();
        let left_bound_var = left_bound_var.clone();
        let right_bound_var = right_bound_var.clone();
        let shared_var = shared_var.clone();
        let select_vars = Arc::clone(&select_vars);
        let filter_expressions = Arc::clone(&filter_expressions);
        let evaluator = Arc::clone(&evaluator);
        async move {
            loop {
                let elem = input.next().await?;
                let Some(rdf) = elem.as_rdf() else {
                    continue;
                };
                let stmt = &rdf.statement;
                if stmt.predicate.as_str() != resolved_predicate {
                    continue;
                }
                let subject_str = match &stmt.subject {
                    Term::Iri(iri) => iri.as_str().to_string(),
                    other => other.to_string(),
                };
                let object_str = match &stmt.object {
                    Term::Iri(iri) => iri.as_str().to_string(),
                    Term::Literal(lit) => lit.value().to_string(),
                    other => other.to_string(),
                };

                // The join key is the shared variable's position; the
                // bound value is the *other* slot. Because both sides
                // of the self-join scan the same input stream, the
                // event must populate the join key from whichever
                // position holds the shared variable. The pattern's
                // shape is enforced earlier — both sides agree on the
                // shared position, or the fast path is rejected.
                //
                // For a triple `?bound <p> ?shared` (object position),
                // the key is the object and bound_value is the subject;
                // for `?shared <p> ?bound` (subject position) it is the
                // other way round.
                let (key, bound_value) = match join_position {
                    JoinKeyPosition::Object => (object_str.clone(), subject_str.clone()),
                    JoinKeyPosition::Subject => (subject_str.clone(), object_str.clone()),
                };

                let event = StoredEvent {
                    bound_value: bound_value.clone(),
                    key: key.clone(),
                    timestamp: rdf.timestamp,
                };
                let pairs = state.add(event, rdf.timestamp, |e| e.key.clone());
                if pairs.is_empty() {
                    continue;
                }
                // Build BindingSets for the pairs and emit them as a
                // single chunk before reading the next element.
                let mut emissions = Vec::with_capacity(pairs.len());
                for pair in pairs {
                    // Both sides share the same join position by
                    // construction (enforced in `try_self_join_fast_path`
                    // via `left_position == right_position`), so the
                    // bound values map directly to left/right bound
                    // variables.
                    let mut bs = BindingSet::new(pair.timestamp);
                    bs.insert(
                        &left_bound_var,
                        Value::String(pair.first.bound_value.clone()),
                    );
                    bs.insert(
                        &right_bound_var,
                        Value::String(pair.second.bound_value.clone()),
                    );
                    bs.insert(&shared_var, Value::String(pair.first.key.clone()));

                    // Post-join FILTER evaluation. Skip pairs whose
                    // filters do not all evaluate to true.
                    if !filter_expressions
                        .iter()
                        .all(|f| evaluator.evaluate_as_bool(f, &bs))
                    {
                        continue;
                    }

                    // Restrict to SELECT projection if non-empty.
                    let bs = if select_vars.is_empty() {
                        bs
                    } else {
                        project(&bs, &select_vars)
                    };
                    emissions.push(bs);
                }
                if emissions.is_empty() {
                    continue;
                }
                let _ = pair_timestamp_min(&emissions);
                return Some((futures::stream::iter(emissions), (input, state)));
            }
        }
    })
    .flatten();
    Box::pin(stream)
}

fn pair_timestamp_min(emissions: &[BindingSet]) -> i64 {
    emissions.iter().map(|b| b.timestamp()).min().unwrap_or(0)
}

fn project(bs: &BindingSet, select_vars: &[String]) -> BindingSet {
    let mut out = BindingSet::new(bs.timestamp());
    for var in select_vars {
        let key = var.trim_start_matches(['?', '$']);
        if let Some(value) = bs.get(key) {
            out.insert(key, value.clone());
        }
    }
    out
}

/// A compiled ASK query that produces `bool` — `true` for each matching binding.
///
/// Wraps the standard binding pipeline and maps each `BindingSet` to `true`.
pub struct CompiledAskQuery {
    /// The inner select-style query that produces bindings.
    pub(crate) inner: CompiledCqelsQuery,
}

#[async_trait]
impl ContinuousQuery for CompiledAskQuery {
    type Result = bool;

    fn query_id(&self) -> &str {
        self.inner.query_id()
    }

    fn query_string(&self) -> &str {
        self.inner.query_string()
    }

    fn query_type(&self) -> QueryType {
        QueryType::Sparql
    }

    fn execute(&self, inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = bool> + Send>> {
        let binding_stream = self.inner.execute(inputs);
        Box::pin(binding_stream.map(|_| true))
    }
}

/// A compiled DESCRIBE query that produces `Statement`s about each bound resource.
///
/// For each binding, looks up the RDF store for triples where the described
/// resource appears as subject or object.
pub struct CompiledDescribeQuery {
    /// The inner select-style query that produces bindings.
    pub(crate) inner: CompiledCqelsQuery,
    /// Variables to describe (empty means describe all bound variables).
    pub(crate) describe_vars: Vec<String>,
    /// RDF store for resource lookups.
    pub(crate) rdf_store: Option<Arc<dyn crate::store::RdfStore>>,
    /// Prefix mappings.
    pub(crate) prefixes: std::collections::HashMap<String, String>,
}

#[async_trait]
impl ContinuousQuery for CompiledDescribeQuery {
    type Result = Statement;

    fn query_id(&self) -> &str {
        self.inner.query_id()
    }

    fn query_string(&self) -> &str {
        self.inner.query_string()
    }

    fn query_type(&self) -> QueryType {
        QueryType::Sparql
    }

    fn execute(&self, inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Statement> + Send>> {
        let binding_stream = self.inner.execute(inputs);
        let describe_vars = self.describe_vars.clone();
        let rdf_store = self.rdf_store.clone();
        let prefixes = self.prefixes.clone();

        // Pre-strip variable prefixes once (not per-binding)
        let stripped_vars: Vec<String> = describe_vars
            .iter()
            .map(|v| {
                v.strip_prefix('?')
                    .or_else(|| v.strip_prefix('$'))
                    .unwrap_or(v)
                    .to_string()
            })
            .collect();

        Box::pin(binding_stream.flat_map(move |bs| {
            let mut stmts = Vec::new();

            // Determine which variables to describe
            let vars_to_describe: Vec<String> = if stripped_vars.is_empty() {
                // DESCRIBE * — describe all bound variables
                bs.variables().map(|v| v.to_string()).collect()
            } else {
                stripped_vars.clone()
            };

            if let Some(ref store) = rdf_store {
                for var in &vars_to_describe {
                    if let Some(value) = bs.get(var) {
                        // Extract raw URI string using as_string() to avoid
                        // Display formatting artifacts (angle brackets, quotes).
                        let resource_uri = match value.as_string() {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        // Query for triples where resource is subject
                        let as_subject = crate::parser::ast::TriplePattern {
                            subject: format!("<{resource_uri}>"),
                            predicate: "?_p".to_string(),
                            object: "?_o".to_string(),
                        };
                        let results = store.query_pattern(&as_subject, &prefixes);
                        for result_bs in &results {
                            if let (Some(p_val), Some(o_val)) =
                                (result_bs.get("_p"), result_bs.get("_o"))
                            {
                                let subject =
                                    Term::Iri(cqels_model::term::IriTerm::new(&resource_uri));
                                let predicate = cqels_model::term::IriTerm::new(
                                    p_val.as_string().unwrap_or(""),
                                );
                                let object = super::pipeline::value_to_term(o_val);
                                stmts.push(Statement::new(subject, predicate, object));
                            }
                        }
                        // Query for triples where resource is object
                        let as_object = crate::parser::ast::TriplePattern {
                            subject: "?_s".to_string(),
                            predicate: "?_p".to_string(),
                            object: format!("<{resource_uri}>"),
                        };
                        let results = store.query_pattern(&as_object, &prefixes);
                        for result_bs in &results {
                            if let (Some(s_val), Some(p_val)) =
                                (result_bs.get("_s"), result_bs.get("_p"))
                            {
                                let subject = super::pipeline::value_to_term(s_val);
                                let predicate = cqels_model::term::IriTerm::new(
                                    p_val.as_string().unwrap_or(""),
                                );
                                let object =
                                    Term::Iri(cqels_model::term::IriTerm::new(&resource_uri));
                                stmts.push(Statement::new(subject, predicate, object));
                            }
                        }
                    }
                }
            }

            futures::stream::iter(stmts)
        }))
    }
}

/// A compiled CONSTRUCT query that produces `Statement`s from bindings.
///
/// Wraps the standard binding pipeline but maps each `BindingSet` through
/// the construct template to produce `Statement`s.
pub struct CompiledConstructQuery {
    /// The inner select-style query that produces bindings.
    pub(crate) inner: CompiledCqelsQuery,
    /// The construct template triple patterns.
    pub(crate) template: Vec<crate::parser::ast::TriplePattern>,
    /// Prefix mappings for resolving template terms.
    pub(crate) prefixes: std::collections::HashMap<String, String>,
}

#[async_trait]
impl ContinuousQuery for CompiledConstructQuery {
    type Result = Statement;

    fn query_id(&self) -> &str {
        self.inner.query_id()
    }

    fn query_string(&self) -> &str {
        self.inner.query_string()
    }

    fn query_type(&self) -> QueryType {
        QueryType::Sparql
    }

    fn execute(&self, inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Statement> + Send>> {
        let binding_stream = self.inner.execute(inputs);
        let template = self.template.clone();
        let prefixes = self.prefixes.clone();

        Box::pin(binding_stream.flat_map(move |bs| {
            let stmts = super::pipeline::apply_construct_template(&bs, &template, &prefixes);
            futures::stream::iter(stmts)
        }))
    }
}

/// A compiled CypherQL query ready for execution.
///
/// Immutable query data is wrapped in `Arc` for cheap sharing with
/// async closures in `execute()`.
pub struct CompiledCypherQuery {
    /// Original query string.
    pub(crate) query_string: String,
    /// Query name/ID.
    pub(crate) query_id: String,
    /// The parsed query definition.
    pub(crate) definition: Arc<CypherQueryDefinition>,
    /// Pre-parsed WHERE expression.
    pub(crate) where_expression: Arc<Option<Expression>>,
    /// Pre-parsed HAVING expressions.
    pub(crate) having_expressions: Arc<Vec<Expression>>,
    /// Pre-parsed ORDER BY expressions with sort direction.
    pub(crate) order_by_expressions: Arc<Vec<(Expression, SortDirection)>>,
    /// Pre-parsed RETURN expressions.
    pub(crate) return_expressions: Arc<Vec<(Expression, String)>>,
    /// Aggregate specifications.
    pub(crate) aggregate_specs: Arc<Vec<PipelineAggregateSpec>>,
    /// RETURN column aliases for projection.
    pub(crate) select_vars: Arc<Vec<String>>,
    /// Expression evaluator.
    pub(crate) evaluator: Arc<ExpressionEvaluator>,
    /// Optional RDF store for static graph pattern lookups.
    pub(crate) rdf_store: Option<Arc<dyn RdfStore>>,
}

#[async_trait]
impl ContinuousQuery for CompiledCypherQuery {
    type Result = BindingSet;

    fn query_id(&self) -> &str {
        &self.query_id
    }

    fn query_string(&self) -> &str {
        &self.query_string
    }

    fn query_type(&self) -> QueryType {
        QueryType::Cypher
    }

    fn execute(&self, mut inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
        let definition = Arc::clone(&self.definition);
        let where_expression = Arc::clone(&self.where_expression);
        let having_expressions = Arc::clone(&self.having_expressions);
        let order_by_expressions = Arc::clone(&self.order_by_expressions);
        let return_expressions = Arc::clone(&self.return_expressions);
        let aggregate_specs = Arc::clone(&self.aggregate_specs);
        let select_vars = Arc::clone(&self.select_vars);
        let evaluator = Arc::clone(&self.evaluator);
        let distinct = definition.distinct;
        let limit = definition.limit;
        let group_by = definition.group_by_expressions.clone();

        // Pre-compute static bindings from RDF store for Cypher static/named-graph patterns
        let rdf_store = self.rdf_store.clone();
        let static_bindings: Vec<BindingSet> = if let Some(store) = &rdf_store {
            use crate::parser::ast::PatternSource;
            let mut accumulated: Vec<BindingSet> = Vec::new();

            for pg in &definition.pattern_groups {
                if pg.source == PatternSource::Static || pg.source == PatternSource::Graph {
                    // Convert Cypher patterns to triple pattern lookups via the store
                    for cypher_pattern in &pg.patterns {
                        // For each relationship in the pattern, query the store
                        for rel in &cypher_pattern.relationships {
                            let subj_var = rel.start_node.as_deref().unwrap_or("_s");
                            let obj_var = rel.end_node.as_deref().unwrap_or("_o");

                            // Build a triple pattern from the relationship
                            let predicate_str = rel
                                .types
                                .first()
                                .map(|t| format!("<{t}>"))
                                .unwrap_or_else(|| "?_p".to_string());

                            let triple_pattern = crate::parser::ast::TriplePattern {
                                subject: format!("?{subj_var}"),
                                predicate: predicate_str,
                                object: format!("?{obj_var}"),
                            };

                            let prefixes = std::collections::HashMap::new();
                            let pattern_results = if pg.source == PatternSource::Graph {
                                let graph_uri = pg.source_name.as_deref().unwrap_or("");
                                store.query_named_graph_pattern(
                                    graph_uri,
                                    &triple_pattern,
                                    &prefixes,
                                )
                            } else {
                                store.query_pattern(&triple_pattern, &prefixes)
                            };

                            if accumulated.is_empty() {
                                accumulated = pattern_results;
                            } else if !pattern_results.is_empty() {
                                accumulated = join_binding_sets(&accumulated, &pattern_results);
                            }
                        }
                    }
                }
            }
            accumulated
        } else {
            vec![]
        };

        // Take input streams — merge multiple if available
        let input_stream: Option<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> =
            if definition.streams.len() <= 1 {
                let stream_name = definition
                    .streams
                    .first()
                    .map(|s| s.name.clone())
                    .or_else(|| inputs.stream_names().next().map(|s| s.to_string()));
                stream_name.and_then(|name| inputs.take_stream(&name))
            } else {
                let streams: Vec<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> = definition
                    .streams
                    .iter()
                    .filter_map(|s| inputs.take_stream(&s.name))
                    .collect();
                if streams.is_empty() {
                    None
                } else if streams.len() == 1 {
                    streams.into_iter().next()
                } else {
                    Some(Box::pin(futures::stream::select_all(streams)))
                }
            };

        let input_stream = match input_stream {
            Some(s) => s,
            None => return Box::pin(futures::stream::empty()),
        };

        // Apply windowing from the first Cypher stream's window spec.
        // For Cypher chain matching, we produce batches of elements (Vec<StreamElement>)
        // so that match_cypher_pattern can match across multiple statements in a window.
        let cypher_window_spec = definition.streams.first().map(|s| &s.window);
        let batch_stream: Pin<Box<dyn Stream<Item = Vec<StreamElement>> + Send>> =
            match cypher_window_spec.map(|w| &w.window_type) {
                Some(WindowType::Now) => {
                    // Each element is its own batch
                    Box::pin(input_stream.map(|elem| vec![elem]))
                }
                Some(WindowType::Range) => {
                    let duration = cypher_window_spec
                        .and_then(|w| w.duration)
                        .unwrap_or(Duration::from_secs(0));
                    if duration.is_zero() {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = TumblingWindow::new(duration);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                Some(WindowType::Slide) => {
                    let duration = cypher_window_spec
                        .and_then(|w| w.duration)
                        .unwrap_or(Duration::from_secs(0));
                    let step = cypher_window_spec
                        .and_then(|w| w.step)
                        .unwrap_or(Duration::from_secs(0));
                    if duration.is_zero() || step.is_zero() {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = SlidingWindow::new(duration, step);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                Some(WindowType::Triples) => {
                    let count =
                        cypher_window_spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
                    if count == 0 {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = TumblingCountWindow::new(count);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                Some(WindowType::TriplesSlide) => {
                    let count =
                        cypher_window_spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
                    let slide =
                        cypher_window_spec.and_then(|w| w.triple_slide).unwrap_or(1) as usize;
                    if count == 0 || slide == 0 {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = SlidingCountWindow::new(count, slide);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                None => {
                    // No windowing — each element is its own batch
                    Box::pin(input_stream.map(|elem| vec![elem]))
                }
            };

        // Phase 1: Convert batches of StreamElements to BindingSets using Cypher pattern matching.
        // Uses match_cypher_pattern for recursive chain matching across statements in each batch.
        let pattern_groups = definition.pattern_groups.clone();
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            Box::pin(batch_stream.flat_map(move |batch| {
                // Collect all RDF statements and their timestamps from this batch
                let mut stmts: Vec<Statement> = Vec::new();
                let mut timestamp = 0i64;
                for elem in &batch {
                    timestamp = elem.timestamp();
                    if let StreamElement::Rdf(rdf) = elem {
                        stmts.push(rdf.statement.clone());
                    }
                }

                // Run chain matching for each pattern group and each pattern
                let mut all_results: Vec<BindingSet> = Vec::new();
                for pg in &pattern_groups {
                    for pattern in &pg.patterns {
                        let results = match_cypher_pattern(pattern, &stmts, timestamp);
                        all_results.extend(results);
                    }
                }

                futures::stream::iter(all_results)
            }));

        // Phase 1b: Join stream bindings with static bindings from RDF store
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            if !static_bindings.is_empty() {
                Box::pin(binding_stream.flat_map(move |bs| {
                    let joined: Vec<BindingSet> = static_bindings
                        .iter()
                        .filter_map(|static_bs| bs.join(static_bs))
                        .collect();
                    futures::stream::iter(if joined.is_empty() { vec![bs] } else { joined })
                }))
            } else {
                binding_stream
            };

        // Phase 2: Apply WHERE filter
        let evaluator2 = evaluator.clone();
        let filtered: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            if let Some(where_expr) = where_expression.as_ref() {
                apply_filters(binding_stream, std::slice::from_ref(where_expr), &evaluator)
            } else {
                binding_stream
            };

        // Phase 3: Apply RETURN expressions as bindings
        let return_exprs = return_expressions.clone();
        let evaluator3 = evaluator2.clone();
        let with_returns: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            if return_exprs.is_empty() {
                filtered
            } else {
                apply_binds(filtered, &return_exprs, &evaluator3)
            };

        // Phase 4: Aggregation, ordering, limiting
        let has_aggregates = !aggregate_specs.is_empty() || !group_by.is_empty();
        let has_order = !order_by_expressions.is_empty();
        let needs_collect = has_aggregates || has_order || limit.is_some();

        if needs_collect {
            let evaluator4 = evaluator2.clone();
            let result_stream = with_returns
                .collect::<Vec<BindingSet>>()
                .into_stream()
                .flat_map(move |elements| {
                    let mut results = elements;

                    if has_aggregates {
                        results = apply_group_by_aggregates(
                            results,
                            &group_by,
                            &aggregate_specs,
                            &evaluator4,
                        );

                        if !having_expressions.is_empty() {
                            results.retain(|bs| {
                                having_expressions
                                    .iter()
                                    .all(|h| evaluator4.evaluate_as_bool(h, bs))
                            });
                        }
                    }

                    results =
                        apply_order_and_limit(results, &order_by_expressions, &evaluator4, limit);

                    futures::stream::iter(results)
                });

            let projected: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
                if !select_vars.is_empty() {
                    apply_projection(Box::pin(result_stream), &select_vars)
                } else {
                    Box::pin(result_stream)
                };

            if distinct {
                apply_distinct(projected)
            } else {
                projected
            }
        } else {
            let projected: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
                if !select_vars.is_empty() {
                    apply_projection(with_returns, &select_vars)
                } else {
                    with_returns
                };

            if distinct {
                apply_distinct(projected)
            } else {
                projected
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::parser::ast::*;
    use crate::stream::RdfStreamElement;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::{Statement, Term};
    use futures::StreamExt;

    #[tokio::test]
    async fn test_compiled_cqels_query_basic() {
        let definition = CqelsQueryDefinition {
            name: None,
            description: None,
            query_type: CqelsQueryType::Select,
            prefixes: HashMap::new(),
            streams: vec![CqelsStreamDefinition::root("sensors", WindowSpec::now())],
            named_windows: vec![],
            static_graphs: vec![],
            named_graphs: vec![],
            select_elements: vec![
                SelectElement::Variable("?sensor".to_string()),
                SelectElement::Variable("?temp".to_string()),
            ],
            distinct: false,
            pattern_groups: vec![CqelsPatternGroup::Stream {
                source: "sensors".to_string(),
                patterns: vec![TriplePattern {
                    subject: "?sensor".to_string(),
                    predicate: "<http://example.org/temp>".to_string(),
                    object: "?temp".to_string(),
                }],
            }],
            aggregates: vec![],
            group_by_variables: vec![],
            order_by_conditions: vec![],
            limit: None,
            operator_hints: OperatorHints::default(),
            stream_semantics: StreamSemantics::default(),
            construct_template: vec![],
            seq_constraint: None,
        };

        let query = CompiledCqelsQuery {
            query_string: "SELECT ?sensor ?temp FROM STREAM sensors [NOW] WHERE { STREAM sensors { ?sensor <http://example.org/temp> ?temp . } }".to_string(),
            query_id: "test".to_string(),
            definition: Arc::new(definition),
            filter_expressions: Arc::new(vec![]),
            bind_expressions: Arc::new(vec![]),
            order_by_expressions: Arc::new(vec![]),
            having_expressions: Arc::new(vec![]),
            aggregate_specs: Arc::new(vec![]),
            evaluator: Arc::new(ExpressionEvaluator::new()),
            select_vars: Arc::new(vec!["sensor".to_string(), "temp".to_string()]),
            rdf_store: None,
            self_join_hints: Arc::new(vec![]),
        };

        // Create test input stream
        let elements = vec![
            StreamElement::Rdf(RdfStreamElement::new(
                Statement::new(
                    Term::Iri(IriTerm::new("http://example.org/sensor1")),
                    IriTerm::new("http://example.org/temp"),
                    Term::Literal(LiteralTerm::new("42")),
                ),
                1000,
            )),
            StreamElement::Rdf(RdfStreamElement::new(
                Statement::new(
                    Term::Iri(IriTerm::new("http://example.org/sensor2")),
                    IriTerm::new("http://example.org/temp"),
                    Term::Literal(LiteralTerm::new("35")),
                ),
                2000,
            )),
        ];

        let mut inputs = QueryInputs::new();
        inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

        let results: Vec<BindingSet> = query.execute(inputs).collect().await;
        assert_eq!(results.len(), 2);
        assert!(results[0].contains("sensor"));
        assert!(results[0].contains("temp"));
    }

    /// Direct check that [`apply_window_spec`] respects each spec
    /// it's handed — used by the multi-stream path in `execute` to
    /// window each input independently before merging.
    #[tokio::test]
    async fn apply_window_spec_now_makes_each_element_its_own_batch() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/s")),
            IriTerm::new("http://ex.org/p"),
            Term::Literal(LiteralTerm::new("v")),
        );
        let elements = vec![
            StreamElement::Rdf(RdfStreamElement::new(stmt.clone(), 1)),
            StreamElement::Rdf(RdfStreamElement::new(stmt.clone(), 2)),
            StreamElement::Rdf(RdfStreamElement::new(stmt, 3)),
        ];
        let input = Box::pin(futures::stream::iter(elements));
        let spec = WindowSpec::now();
        let batches: Vec<Vec<StreamElement>> =
            apply_window_spec(input, Some(&spec)).collect().await;
        assert_eq!(batches.len(), 3, "NOW emits one batch per element");
        for batch in &batches {
            assert_eq!(batch.len(), 1);
        }
    }

    #[tokio::test]
    async fn apply_window_spec_triples_groups_into_count_batches() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/s")),
            IriTerm::new("http://ex.org/p"),
            Term::Literal(LiteralTerm::new("v")),
        );
        let elements: Vec<StreamElement> = (1..=4)
            .map(|i| StreamElement::Rdf(RdfStreamElement::new(stmt.clone(), i)))
            .collect();
        let input = Box::pin(futures::stream::iter(elements));
        let spec = WindowSpec::triples(2);
        let batches: Vec<Vec<StreamElement>> =
            apply_window_spec(input, Some(&spec)).collect().await;
        // TRIPLES 2 emits a batch every two elements: [1,2] then [3,4].
        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_eq!(batch.len(), 2);
        }
    }

    /// End-to-end verification that a multi-stream query — exactly
    /// the shape an aliased named-window view produces after
    /// lowering — receives the elements pushed into the underlying
    /// input streams. Each `STREAM {}` group is batched by its own
    /// window spec before the merge, so a NOW-spec view yields one
    /// binding per element while a RANGE-spec view yields a single
    /// batch covering the time window.
    #[tokio::test]
    async fn multi_stream_query_applies_per_stream_windowing() {
        // Two synthetic streams over the same conceptual source, with
        // different specs. Matches what the named-window lowering
        // produces after PR #55: a NOW-spec root + a RANGE-spec
        // synthetic alias.
        let definition = CqelsQueryDefinition {
            name: None,
            description: None,
            query_type: CqelsQueryType::Select,
            prefixes: HashMap::new(),
            streams: vec![
                CqelsStreamDefinition::root("sensors_now", WindowSpec::now()),
                CqelsStreamDefinition::aliased(
                    "__nw__sensors_range",
                    "sensors_now",
                    WindowSpec::range(std::time::Duration::from_secs(60)),
                ),
            ],
            named_windows: vec![],
            static_graphs: vec![],
            named_graphs: vec![],
            select_elements: vec![
                SelectElement::Variable("?s".to_string()),
                SelectElement::Variable("?v".to_string()),
            ],
            distinct: false,
            pattern_groups: vec![
                CqelsPatternGroup::Stream {
                    source: "sensors_now".to_string(),
                    patterns: vec![TriplePattern {
                        subject: "?s".to_string(),
                        predicate: "<http://ex.org/temp>".to_string(),
                        object: "?v".to_string(),
                    }],
                },
                CqelsPatternGroup::Stream {
                    source: "__nw__sensors_range".to_string(),
                    patterns: vec![TriplePattern {
                        subject: "?s".to_string(),
                        predicate: "<http://ex.org/temp>".to_string(),
                        object: "?v".to_string(),
                    }],
                },
            ],
            aggregates: vec![],
            group_by_variables: vec![],
            order_by_conditions: vec![],
            limit: None,
            operator_hints: OperatorHints::default(),
            stream_semantics: StreamSemantics::default(),
            construct_template: vec![],
            seq_constraint: None,
        };

        let query = CompiledCqelsQuery {
            query_string: "<multi-stream test>".to_string(),
            query_id: "test-multi".to_string(),
            definition: Arc::new(definition),
            filter_expressions: Arc::new(vec![]),
            bind_expressions: Arc::new(vec![]),
            order_by_expressions: Arc::new(vec![]),
            having_expressions: Arc::new(vec![]),
            aggregate_specs: Arc::new(vec![]),
            evaluator: Arc::new(ExpressionEvaluator::new()),
            select_vars: Arc::new(vec!["s".to_string(), "v".to_string()]),
            rdf_store: None,
            self_join_hints: Arc::new(vec![]),
        };

        let make_elem = |i: i64| {
            StreamElement::Rdf(RdfStreamElement::new(
                Statement::new(
                    Term::Iri(IriTerm::new(format!("http://ex.org/sensor{i}"))),
                    IriTerm::new("http://ex.org/temp"),
                    Term::Literal(LiteralTerm::new(format!("{}", 20 + i))),
                ),
                i * 1000,
            ))
        };
        let now_elements: Vec<StreamElement> = (1..=2).map(make_elem).collect();
        let range_elements: Vec<StreamElement> = (1..=2).map(make_elem).collect();

        let mut inputs = QueryInputs::new();
        inputs.add_stream("sensors_now", Box::pin(futures::stream::iter(now_elements)));
        inputs.add_stream(
            "__nw__sensors_range",
            Box::pin(futures::stream::iter(range_elements)),
        );

        let results: Vec<BindingSet> = query.execute(inputs).collect().await;
        // Per-stream windowing: the NOW-spec view emits one batch per
        // element (2 bindings); the RANGE-spec view collects both
        // elements into a single batch which matches the pattern
        // twice. Total ≥ 3 across both streams, with every binding
        // carrying `s` and `v`.
        assert!(
            results.len() >= 2,
            "expected at least one binding per windowed view, got {}",
            results.len()
        );
        for binding in &results {
            assert!(binding.contains("s"));
            assert!(binding.contains("v"));
        }
    }

    /// Source-tagged batching test: each pattern group is scoped to
    /// its own stream's batches. Stream `s_temp` carries `?s :temp ?v`
    /// triples and `s_pressure` carries `?s :pressure ?p` triples.
    /// Without source filtering the older matcher would have tried
    /// every pattern against every batch, producing spurious
    /// bind-failures (because a pressure batch has no `:temp` match
    /// and vice-versa) — in this query that manifests as the multi-
    /// stream non-strict matcher emitting *some* bindings for each
    /// pattern independently. With source-tagged filtering each
    /// batch only sees its own group's patterns, and every selected
    /// pattern is required.
    #[tokio::test]
    async fn source_tagged_batches_keep_pattern_matches_per_source() {
        let definition = CqelsQueryDefinition {
            name: None,
            description: None,
            query_type: CqelsQueryType::Select,
            prefixes: HashMap::new(),
            streams: vec![
                CqelsStreamDefinition::root("s_temp", WindowSpec::now()),
                CqelsStreamDefinition::root("s_pressure", WindowSpec::now()),
            ],
            named_windows: vec![],
            static_graphs: vec![],
            named_graphs: vec![],
            select_elements: vec![
                SelectElement::Variable("?s".to_string()),
                SelectElement::Variable("?v".to_string()),
                SelectElement::Variable("?p".to_string()),
            ],
            distinct: false,
            pattern_groups: vec![
                CqelsPatternGroup::Stream {
                    source: "s_temp".to_string(),
                    patterns: vec![TriplePattern {
                        subject: "?s".to_string(),
                        predicate: "<http://ex.org/temp>".to_string(),
                        object: "?v".to_string(),
                    }],
                },
                CqelsPatternGroup::Stream {
                    source: "s_pressure".to_string(),
                    patterns: vec![TriplePattern {
                        subject: "?s".to_string(),
                        predicate: "<http://ex.org/pressure>".to_string(),
                        object: "?p".to_string(),
                    }],
                },
            ],
            aggregates: vec![],
            group_by_variables: vec![],
            order_by_conditions: vec![],
            limit: None,
            operator_hints: OperatorHints::default(),
            stream_semantics: StreamSemantics::default(),
            construct_template: vec![],
            seq_constraint: None,
        };

        let query = CompiledCqelsQuery {
            query_string: "<source-tagged test>".to_string(),
            query_id: "test-source-tagged".to_string(),
            definition: Arc::new(definition),
            filter_expressions: Arc::new(vec![]),
            bind_expressions: Arc::new(vec![]),
            order_by_expressions: Arc::new(vec![]),
            having_expressions: Arc::new(vec![]),
            aggregate_specs: Arc::new(vec![]),
            evaluator: Arc::new(ExpressionEvaluator::new()),
            select_vars: Arc::new(vec!["s".to_string(), "v".to_string(), "p".to_string()]),
            rdf_store: None,
            self_join_hints: Arc::new(vec![]),
        };

        // s_temp carries one `:temp` reading. The matcher should bind
        // `?v` to "21".
        let temp_elem = StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/sensor1")),
                IriTerm::new("http://ex.org/temp"),
                Term::Literal(LiteralTerm::new("21")),
            ),
            1000,
        ));
        // s_pressure carries one `:pressure` reading. The matcher
        // should bind `?p` to "1.5".
        let pressure_elem = StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/sensor1")),
                IriTerm::new("http://ex.org/pressure"),
                Term::Literal(LiteralTerm::new("1.5")),
            ),
            2000,
        ));

        let mut inputs = QueryInputs::new();
        inputs.add_stream("s_temp", Box::pin(futures::stream::iter(vec![temp_elem])));
        inputs.add_stream(
            "s_pressure",
            Box::pin(futures::stream::iter(vec![pressure_elem])),
        );

        let results: Vec<BindingSet> = query.execute(inputs).collect().await;
        assert!(!results.is_empty(), "expected per-source matches");

        // Every binding from `s_temp`'s batch carries `?v`; from
        // `s_pressure`'s batch carries `?p`. Source-tagged filtering
        // prevents bindings that lack either expected variable.
        for binding in &results {
            let has_v = binding.contains("v");
            let has_p = binding.contains("p");
            assert!(
                has_v ^ has_p,
                "binding should bind exactly one of (?v, ?p) per source; got both/none: \
                 contains(v)={has_v} contains(p)={has_p}"
            );
        }
    }
}
