//! Compiled query types that implement `ContinuousQuery`.
//!
//! These types hold pre-parsed expression trees and query metadata,
//! and execute the full pipeline when `execute()` is called.

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::{FutureExt, Stream, StreamExt};

use cqels_model::{BindingSet, Statement};

use crate::expression::ast::Expression;
use crate::expression::evaluator::ExpressionEvaluator;
use crate::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, CypherQueryDefinition, SortDirection, WindowType,
};
use crate::query::{ContinuousQuery, QueryInputs, QueryType};
use crate::stream::StreamElement;
use crate::window::{SlidingWindow, TumblingCountWindow, TumblingWindow, Window};

use super::pipeline::{
    apply_binds, apply_distinct, apply_filters, apply_group_by_aggregates, apply_minus,
    apply_optional, apply_order_and_limit, apply_projection, apply_union, match_cypher_pattern,
    match_triple_pattern, PipelineAggregateSpec,
};

/// A compiled CqelsQL query ready for execution.
pub struct CompiledCqelsQuery {
    /// Original query string.
    pub(crate) query_string: String,
    /// Query name/ID.
    pub(crate) query_id: String,
    /// The parsed query definition.
    pub(crate) definition: CqelsQueryDefinition,
    /// Pre-parsed FILTER expressions.
    pub(crate) filter_expressions: Vec<Expression>,
    /// Pre-parsed BIND expressions with target variable.
    pub(crate) bind_expressions: Vec<(Expression, String)>,
    /// Pre-parsed ORDER BY expressions with sort direction.
    pub(crate) order_by_expressions: Vec<(Expression, SortDirection)>,
    /// Pre-parsed HAVING expressions.
    pub(crate) having_expressions: Vec<Expression>,
    /// Aggregate specifications.
    pub(crate) aggregate_specs: Vec<PipelineAggregateSpec>,
    /// Expression evaluator with prefix mappings.
    pub(crate) evaluator: ExpressionEvaluator,
    /// SELECT variable names for projection.
    pub(crate) select_vars: Vec<String>,
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

    fn execute(
        &self,
        mut inputs: QueryInputs,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
        let definition = self.definition.clone();
        let filter_expressions = self.filter_expressions.clone();
        let bind_expressions = self.bind_expressions.clone();
        let order_by_expressions = self.order_by_expressions.clone();
        let having_expressions = self.having_expressions.clone();
        let aggregate_specs = self.aggregate_specs.clone();
        let evaluator = self.evaluator.clone();
        let select_vars = self.select_vars.clone();
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

        // Collect default/static patterns (for basic matching)
        let default_patterns: Vec<crate::parser::ast::TriplePattern> = definition
            .pattern_groups
            .iter()
            .filter_map(|pg| match pg {
                CqelsPatternGroup::Default { patterns } => Some(patterns.clone()),
                CqelsPatternGroup::Static { patterns } => Some(patterns.clone()),
                _ => None,
            })
            .flatten()
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
                CqelsPatternGroup::Union { left, right } => {
                    Some((left.clone(), right.clone()))
                }
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
                // Multiple streams — merge all using select_all (round-robin fair interleaving)
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

        // Apply windowing from the first stream's window spec
        let window_spec = definition.streams.first().map(|s| &s.window);
        let input_stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>> =
            match window_spec.map(|w| &w.window_type) {
                Some(WindowType::Now) => {
                    // NOW: each element is its own "window" — pass through as-is
                    input_stream
                }
                Some(WindowType::Range) => {
                    let duration = window_spec
                        .and_then(|w| w.duration)
                        .unwrap_or(Duration::from_secs(0));
                    if duration.is_zero() {
                        input_stream
                    } else {
                        let window = TumblingWindow::new(duration);
                        Box::pin(
                            window
                                .apply(input_stream)
                                .flat_map(|batch| futures::stream::iter(batch.elements)),
                        )
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
                        input_stream
                    } else {
                        let window = SlidingWindow::new(duration, step);
                        Box::pin(
                            window
                                .apply(input_stream)
                                .flat_map(|batch| futures::stream::iter(batch.elements)),
                        )
                    }
                }
                Some(WindowType::Triples) => {
                    let count = window_spec
                        .and_then(|w| w.triple_count)
                        .unwrap_or(1) as usize;
                    if count == 0 {
                        input_stream
                    } else {
                        let window = TumblingCountWindow::new(count);
                        Box::pin(
                            window
                                .apply(input_stream)
                                .flat_map(|batch| futures::stream::iter(batch.elements)),
                        )
                    }
                }
                None => input_stream,
            };

        // All patterns (stream + default) for matching
        let all_patterns: Vec<crate::parser::ast::TriplePattern> = stream_patterns
            .iter()
            .flat_map(|(_, patterns)| patterns.clone())
            .chain(default_patterns)
            .collect();

        // Phase 1: Pattern matching — convert StreamElements to BindingSets
        let prefixes_clone = prefixes.clone();
        let binding_stream: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            Box::pin(input_stream.filter_map(move |elem| {
                let timestamp = elem.timestamp();
                let stmt = match &elem {
                    StreamElement::Rdf(rdf) => Some(rdf.statement.clone()),
                    _ => None,
                };

                let result = stmt.and_then(|stmt| {
                    // Try to match against each pattern and join results
                    let mut combined: Option<BindingSet> = None;
                    for pattern in &all_patterns {
                        if let Some(bs) =
                            match_triple_pattern(pattern, &stmt, &prefixes_clone, timestamp)
                        {
                            combined = Some(match combined {
                                Some(existing) => {
                                    existing.join(&bs).unwrap_or(existing)
                                }
                                None => bs,
                            });
                        }
                    }
                    combined
                });

                futures::future::ready(result)
            }));

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

            let result_stream = with_minus.collect::<Vec<BindingSet>>().into_stream().flat_map(
                move |elements| {
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
                            results = results
                                .into_iter()
                                .filter(|bs| {
                                    having_expressions
                                        .iter()
                                        .all(|h| evaluator2.evaluate_as_bool(h, bs))
                                })
                                .collect();
                        }
                    }

                    // ORDER BY + LIMIT
                    results = apply_order_and_limit(
                        results,
                        &order_by_expressions,
                        &evaluator2,
                        limit,
                    );

                    futures::stream::iter(results)
                },
            );

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

/// A compiled CypherQL query ready for execution.
pub struct CompiledCypherQuery {
    /// Original query string.
    pub(crate) query_string: String,
    /// Query name/ID.
    pub(crate) query_id: String,
    /// The parsed query definition.
    pub(crate) definition: CypherQueryDefinition,
    /// Pre-parsed WHERE expression.
    pub(crate) where_expression: Option<Expression>,
    /// Pre-parsed HAVING expressions.
    pub(crate) having_expressions: Vec<Expression>,
    /// Pre-parsed ORDER BY expressions with sort direction.
    pub(crate) order_by_expressions: Vec<(Expression, SortDirection)>,
    /// Pre-parsed RETURN expressions.
    pub(crate) return_expressions: Vec<(Expression, String)>,
    /// Aggregate specifications.
    pub(crate) aggregate_specs: Vec<PipelineAggregateSpec>,
    /// RETURN column aliases for projection.
    pub(crate) select_vars: Vec<String>,
    /// Expression evaluator.
    pub(crate) evaluator: ExpressionEvaluator,
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

    fn execute(
        &self,
        mut inputs: QueryInputs,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
        let definition = self.definition.clone();
        let where_expression = self.where_expression.clone();
        let having_expressions = self.having_expressions.clone();
        let order_by_expressions = self.order_by_expressions.clone();
        let return_expressions = self.return_expressions.clone();
        let aggregate_specs = self.aggregate_specs.clone();
        let select_vars = self.select_vars.clone();
        let evaluator = self.evaluator.clone();
        let distinct = definition.distinct;
        let limit = definition.limit;
        let group_by = definition.group_by_expressions.clone();

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
                    let count = cypher_window_spec
                        .and_then(|w| w.triple_count)
                        .unwrap_or(1) as usize;
                    if count == 0 {
                        Box::pin(input_stream.map(|elem| vec![elem]))
                    } else {
                        let window = TumblingCountWindow::new(count);
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
                        let results =
                            match_cypher_pattern(pattern, &stmts, timestamp);
                        all_results.extend(results);
                    }
                }

                futures::stream::iter(all_results)
            }));

        // Phase 2: Apply WHERE filter
        let evaluator2 = evaluator.clone();
        let filtered: Pin<Box<dyn Stream<Item = BindingSet> + Send>> =
            if let Some(where_expr) = where_expression {
                apply_filters(binding_stream, &[where_expr], &evaluator)
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
            let result_stream =
                with_returns
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
                                results = results
                                    .into_iter()
                                    .filter(|bs| {
                                        having_expressions
                                            .iter()
                                            .all(|h| evaluator4.evaluate_as_bool(h, bs))
                                    })
                                    .collect();
                            }
                        }

                        results = apply_order_and_limit(
                            results,
                            &order_by_expressions,
                            &evaluator4,
                            limit,
                        );

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
        };

        let query = CompiledCqelsQuery {
            query_string: "SELECT ?sensor ?temp FROM STREAM sensors [NOW] WHERE { STREAM sensors { ?sensor <http://example.org/temp> ?temp . } }".to_string(),
            query_id: "test".to_string(),
            definition,
            filter_expressions: vec![],
            bind_expressions: vec![],
            order_by_expressions: vec![],
            having_expressions: vec![],
            aggregate_specs: vec![],
            evaluator: ExpressionEvaluator::new(),
            select_vars: vec!["sensor".to_string(), "temp".to_string()],
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
