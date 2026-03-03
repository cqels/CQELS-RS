//! CqelsQL query compiler.
//!
//! Compiles a parsed `CqelsQueryDefinition` into a `CompiledCqelsQuery` that
//! can be executed against input streams. Pre-parses all expression strings
//! (filters, binds, order-by) into `Expression` trees at compile time.

use std::sync::Arc;

use crate::expression::ast::Expression;
use crate::expression::evaluator::ExpressionEvaluator;
use crate::expression::parser::ExpressionParser;
use crate::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, SelectElement,
};
use crate::parser::ParseResult;
use crate::store::RdfStore;

use super::compiled::CompiledCqelsQuery;
use super::pipeline::{convert_aggregate_function, hash_string, PipelineAggregateSpec};

/// Compiler for CqelsQL (SPARQL-style) queries.
pub struct CqelsQueryCompiler;

impl CqelsQueryCompiler {
    /// Compiles a CqelsQL query definition into an executable query.
    ///
    /// Parses all embedded expression strings into `Expression` trees.
    /// Returns a `ParseError` if any expression fails to parse.
    pub fn compile(
        query_string: &str,
        definition: CqelsQueryDefinition,
    ) -> ParseResult<CompiledCqelsQuery> {
        Self::compile_with_store(query_string, definition, None)
    }

    /// Compiles a CqelsQL query definition with an optional RDF store for
    /// resolving `STATIC { ... }` and `GRAPH <uri> { ... }` patterns.
    pub fn compile_with_store(
        query_string: &str,
        definition: CqelsQueryDefinition,
        rdf_store: Option<Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledCqelsQuery> {
        let evaluator = ExpressionEvaluator::with_prefixes(definition.prefixes.clone());

        // Parse FILTER expressions
        let mut filter_expressions = Vec::new();
        for group in &definition.pattern_groups {
            if let CqelsPatternGroup::Filter { expression } = group {
                let expr = ExpressionParser::parse_cqelsql(expression)?;
                filter_expressions.push(expr);
            }
        }

        // Parse BIND expressions
        let mut bind_expressions = Vec::new();
        for group in &definition.pattern_groups {
            if let CqelsPatternGroup::Bind {
                expression,
                variable,
            } = group
            {
                let expr = ExpressionParser::parse_cqelsql(expression)?;
                bind_expressions.push((expr, variable.clone()));
            }
        }

        // Parse ORDER BY expressions
        let mut order_by_expressions = Vec::new();
        for cond in &definition.order_by_conditions {
            let expr = ExpressionParser::parse_cqelsql(&cond.expression)?;
            order_by_expressions.push((expr, cond.direction));
        }

        // Parse aggregate specifications
        let mut aggregate_specs = Vec::new();
        for agg in &definition.aggregates {
            let argument = ExpressionParser::parse_cqelsql(&agg.argument)?;
            let function = convert_aggregate_function(agg.function);
            aggregate_specs.push(PipelineAggregateSpec {
                function,
                argument,
                alias: agg.alias.clone(),
                distinct: false,
                separator: None,
            });
        }

        // Also check SELECT expressions for aggregates
        for elem in &definition.select_elements {
            if let SelectElement::Expression {
                expression, alias, ..
            } = elem
            {
                let expr = ExpressionParser::parse_cqelsql(expression)?;
                // If it's an aggregate expression, add to aggregate_specs
                if let Expression::Aggregate {
                    function,
                    argument,
                    distinct,
                } = &expr
                {
                    aggregate_specs.push(PipelineAggregateSpec {
                        function: *function,
                        argument: *argument.clone(),
                        alias: alias.clone(),
                        distinct: *distinct,
                        separator: None,
                    });
                } else {
                    // Regular computed expression — add as BIND
                    bind_expressions.push((expr, alias.clone()));
                }
            }
        }

        // Build SELECT variable list for projection
        let select_vars: Vec<String> = definition
            .select_elements
            .iter()
            .map(|elem| match elem {
                SelectElement::Variable(v) => v.clone(),
                SelectElement::Expression { alias, .. } => alias.clone(),
            })
            .collect();

        // Query ID
        let query_id = definition
            .name
            .clone()
            .unwrap_or_else(|| format!("cqels-query-{}", hash_string(query_string)));

        Ok(CompiledCqelsQuery {
            query_string: query_string.to_string(),
            query_id,
            definition: Arc::new(definition),
            filter_expressions: Arc::new(filter_expressions),
            bind_expressions: Arc::new(bind_expressions),
            order_by_expressions: Arc::new(order_by_expressions),
            having_expressions: Arc::new(vec![]), // CqelsQL doesn't have HAVING in the grammar
            aggregate_specs: Arc::new(aggregate_specs),
            evaluator: Arc::new(evaluator),
            select_vars: Arc::new(select_vars),
            rdf_store,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;
    use std::collections::HashMap;

    fn make_basic_definition() -> CqelsQueryDefinition {
        CqelsQueryDefinition {
            name: Some("test-query".to_string()),
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
            pattern_groups: vec![
                CqelsPatternGroup::Stream {
                    source: "sensors".to_string(),
                    patterns: vec![TriplePattern {
                        subject: "?sensor".to_string(),
                        predicate: "<http://example.org/temp>".to_string(),
                        object: "?temp".to_string(),
                    }],
                },
                CqelsPatternGroup::Filter {
                    expression: "?temp > 30".to_string(),
                },
            ],
            aggregates: vec![],
            group_by_variables: vec![],
            order_by_conditions: vec![],
            limit: None,
            operator_hints: OperatorHints::default(),
        }
    }

    #[test]
    fn test_compile_basic_query() {
        let def = make_basic_definition();
        let result = CqelsQueryCompiler::compile(
            "SELECT ?sensor ?temp FROM STREAM sensors [NOW] WHERE { ... }",
            def,
        );
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.query_id, "test-query");
        assert_eq!(compiled.filter_expressions.len(), 1);
        assert_eq!(compiled.select_vars.len(), 2);
    }

    #[test]
    fn test_compile_with_bind() {
        let mut def = make_basic_definition();
        def.pattern_groups.push(CqelsPatternGroup::Bind {
            expression: "?temp * 9 / 5 + 32".to_string(),
            variable: "?tempF".to_string(),
        });

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.bind_expressions.len(), 1);
    }

    #[test]
    fn test_compile_with_order_by() {
        let mut def = make_basic_definition();
        def.order_by_conditions.push(OrderCondition {
            expression: "?temp".to_string(),
            direction: SortDirection::Descending,
        });
        def.limit = Some(5);

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.order_by_expressions.len(), 1);
    }

    #[test]
    fn test_compile_with_aggregates() {
        let mut def = make_basic_definition();
        def.aggregates.push(AggregateSpec {
            function: AggregateFunction::Avg,
            argument: "?temp".to_string(),
            alias: "?avg_temp".to_string(),
        });
        def.group_by_variables.push("?sensor".to_string());

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.aggregate_specs.len(), 1);
    }

    #[test]
    fn test_compile_invalid_filter_expression() {
        let mut def = make_basic_definition();
        def.pattern_groups = vec![CqelsPatternGroup::Filter {
            expression: "??? invalid @@@ expr".to_string(),
        }];

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_with_prefixes() {
        let mut def = make_basic_definition();
        def.prefixes.insert("ex".to_string(), "http://example.org/".to_string());

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert!(compiled.evaluator.prefixes().contains_key("ex"));
    }
}
