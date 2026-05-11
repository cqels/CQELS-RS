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
/// # Limitations
///
/// **Multi-stream merging (#5):** When a query references multiple streams,
/// they are merged via `select_all` before windowing. All streams share the
/// first stream's window spec and source identity is lost after merge.
/// Per-stream windowing and cross-stream joins require a more sophisticated
/// execution model.
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

    fn execute(&self, mut inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
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

        // Take input streams — merge multiple if available
        let input_stream: Option<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> =
            if stream_patterns.len() <= 1 {
                // Single stream — use existing logic
                let stream_name = stream_patterns
                    .first()
                    .map(|(name, _)| name.clone())
                    .or_else(|| inputs.stream_names().next().map(|s| s.to_string()));
                stream_name.and_then(|name| inputs.take_stream(&name))
            } else {
                // LIMITATION (#5): Multiple streams are merged before windowing.
                // All streams share the first stream's window spec, and source
                // identity is lost after merge. Per-stream windowing and
                // cross-stream joins require a more sophisticated execution model.
                let streams: Vec<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> =
                    stream_patterns
                        .iter()
                        .filter_map(|(name, _)| inputs.take_stream(name))
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

        // Apply windowing from the first stream's window spec.
        // Produces batches of elements (Vec<StreamElement>) so that pattern
        // matching can operate across multiple statements in a window.
        let window_spec = definition.streams.first().map(|s| &s.window);
        let batch_stream: Pin<Box<dyn Stream<Item = Vec<StreamElement>> + Send>> =
            match window_spec.map(|w| &w.window_type) {
                Some(WindowType::Now) => {
                    // NOW: each element is its own batch
                    Box::pin(input_stream.map(|elem| vec![elem]))
                }
                Some(WindowType::Range) => {
                    let duration = window_spec
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
                    let duration = window_spec
                        .and_then(|w| w.duration)
                        .unwrap_or(Duration::from_secs(0));
                    let step = window_spec
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
                    let count = window_spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
                    if count == 0 {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = TumblingCountWindow::new(count);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                Some(WindowType::TriplesSlide) => {
                    let count = window_spec.and_then(|w| w.triple_count).unwrap_or(1) as usize;
                    let slide = window_spec.and_then(|w| w.triple_slide).unwrap_or(1) as usize;
                    if count == 0 || slide == 0 {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = SlidingCountWindow::new(count, slide);
                        Box::pin(window.apply(input_stream).map(|batch| batch.elements))
                    }
                }
                None => Box::pin(input_stream.map(|elem| vec![elem])),
            };

        // All stream + default patterns for matching against streaming data
        let all_patterns: Vec<crate::parser::ast::TriplePattern> = stream_patterns
            .iter()
            .flat_map(|(_, patterns)| patterns.clone())
            .chain(default_patterns)
            .collect();

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

        // Phase 1: Batch-level pattern matching — match across all statements
        // in each window batch so that multi-pattern queries can join across elements.
        let prefixes_clone = prefixes.clone();
        // For single-stream queries, all patterns form a mandatory conjunction:
        // every pattern must have at least one match for the batch to produce
        // results. For multi-stream queries, batches may only contain elements
        // from one source stream, so we cannot require all patterns to match
        // (this is the documented multi-stream limitation, #5).
        let require_all_patterns = stream_patterns.len() <= 1;
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            Box::pin(batch_stream.flat_map(move |batch| {
                let mut stmts: Vec<(Statement, i64)> = Vec::new();
                for elem in &batch {
                    let ts = elem.timestamp();
                    if let StreamElement::Rdf(rdf) = elem {
                        stmts.push((rdf.statement.clone(), ts));
                    }
                }

                // For each pattern, collect bindings from all matching statements.
                let total_patterns = all_patterns.len();
                let mut pattern_results: Vec<Vec<BindingSet>> = Vec::new();
                let mut matched_count = 0;
                for pattern in &all_patterns {
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

                // When all patterns are from the same stream, enforce that
                // every mandatory pattern contributes at least one match.
                let results = if pattern_results.is_empty()
                    || (require_all_patterns && matched_count < total_patterns)
                {
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
            streams: vec![CqelsStreamDefinition {
                name: "sensors".to_string(),
                window: WindowSpec::now(),
            }],
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
}
