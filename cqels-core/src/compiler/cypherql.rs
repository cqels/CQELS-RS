//! CypherQL query compiler.
//!
//! Compiles a parsed `CypherQueryDefinition` into a `CompiledCypherQuery` that
//! can be executed against input streams. Pre-parses all expression strings
//! (WHERE, HAVING, ORDER BY, RETURN) into `Expression` trees at compile time.

use std::sync::Arc;

use crate::expression::ast::Expression;
use crate::expression::evaluator::ExpressionEvaluator;
use crate::expression::parser::ExpressionParser;
use crate::parser::ast::CypherQueryDefinition;
use crate::parser::ParseResult;
use crate::store::RdfStore;

use super::compiled::CompiledCypherQuery;
use super::pipeline::{convert_aggregate_function, hash_string, PipelineAggregateSpec};

/// Compiler for CypherQL queries.
pub struct CypherQueryCompiler;

impl CypherQueryCompiler {
    /// Compiles a CypherQL query definition into an executable query.
    ///
    /// Parses all embedded expression strings into `Expression` trees.
    /// Returns a `ParseError` if any expression fails to parse.
    pub fn compile(
        query_string: &str,
        definition: CypherQueryDefinition,
    ) -> ParseResult<CompiledCypherQuery> {
        Self::compile_with_store(query_string, definition, None)
    }

    /// Compiles a CypherQL query definition with an optional RDF store for
    /// resolving static and named graph patterns.
    pub fn compile_with_store(
        query_string: &str,
        definition: CypherQueryDefinition,
        rdf_store: Option<Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledCypherQuery> {
        let evaluator = ExpressionEvaluator::new();

        // Parse WHERE expression
        let where_expression = match &definition.where_expression {
            Some(expr_str) => Some(ExpressionParser::parse_cypherql(expr_str)?),
            None => None,
        };

        // Parse HAVING expressions
        let mut having_expressions = Vec::new();
        for filter in &definition.having_filters {
            let expr = ExpressionParser::parse_cypherql(filter)?;
            having_expressions.push(expr);
        }

        // Parse ORDER BY expressions
        let mut order_by_expressions = Vec::new();
        for cond in &definition.order_by_conditions {
            let expr = ExpressionParser::parse_cypherql(&cond.expression)?;
            order_by_expressions.push((expr, cond.direction));
        }

        // Parse RETURN expressions and detect aggregates
        let mut return_expressions = Vec::new();
        let mut aggregate_specs = Vec::new();

        for ret_expr in &definition.return_expressions {
            let parsed = ExpressionParser::parse_cypherql(&ret_expr.expression)?;
            let alias = ret_expr
                .alias
                .clone()
                .unwrap_or_else(|| ret_expr.expression.clone());

            // Check if this is an aggregate — aggregates go to aggregate_specs only,
            // not return_expressions (they are computed during the aggregation phase)
            let is_aggregate = if let Some(agg_fn) = &ret_expr.aggregate_function {
                let function = convert_aggregate_function(*agg_fn);
                aggregate_specs.push(PipelineAggregateSpec {
                    function,
                    argument: parsed.clone(),
                    alias: alias.clone(),
                    distinct: ret_expr.distinct,
                    separator: None,
                });
                true
            } else if let Expression::Aggregate {
                function,
                argument,
                distinct,
            } = &parsed
            {
                aggregate_specs.push(PipelineAggregateSpec {
                    function: *function,
                    argument: *argument.clone(),
                    alias: alias.clone(),
                    distinct: *distinct,
                    separator: None,
                });
                true
            } else {
                false
            };

            // Only add non-aggregate expressions to return_expressions
            if !is_aggregate {
                return_expressions.push((parsed, alias));
            }
        }

        // Build select_vars from RETURN aliases (for projection)
        let select_vars: Vec<String> = definition
            .return_expressions
            .iter()
            .map(|ret_expr| {
                ret_expr
                    .alias
                    .clone()
                    .unwrap_or_else(|| ret_expr.expression.clone())
            })
            .collect();

        // Query ID
        let query_id = definition
            .name
            .clone()
            .unwrap_or_else(|| format!("cypher-query-{}", hash_string(query_string)));

        Ok(CompiledCypherQuery {
            query_string: query_string.to_string(),
            query_id,
            definition: Arc::new(definition),
            where_expression: Arc::new(where_expression),
            having_expressions: Arc::new(having_expressions),
            order_by_expressions: Arc::new(order_by_expressions),
            return_expressions: Arc::new(return_expressions),
            aggregate_specs: Arc::new(aggregate_specs),
            select_vars: Arc::new(select_vars),
            evaluator: Arc::new(evaluator),
            rdf_store,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;
    use std::time::Duration;

    fn make_basic_definition() -> CypherQueryDefinition {
        CypherQueryDefinition {
            name: Some("test-cypher".to_string()),
            query_type: CypherQueryType::Select,
            streams: vec![CypherStreamDefinition {
                name: "events".to_string(),
                window: WindowSpec::range(Duration::from_secs(30)),
            }],
            static_graphs: vec![],
            named_graphs: vec![],
            pattern_groups: vec![PatternGroup {
                source: PatternSource::Stream,
                source_name: Some("events".to_string()),
                patterns: vec![CypherPattern {
                    nodes: vec![
                        NodePattern {
                            variable: Some("n".to_string()),
                            labels: vec!["Sensor".to_string()],
                            properties: std::collections::HashMap::new(),
                        },
                        NodePattern {
                            variable: Some("m".to_string()),
                            labels: vec![],
                            properties: std::collections::HashMap::new(),
                        },
                    ],
                    relationships: vec![RelationshipPattern {
                        variable: Some("r".to_string()),
                        types: vec!["MEASURES".to_string()],
                        direction: RelDirection::Outgoing,
                        properties: std::collections::HashMap::new(),
                        start_node: Some("n".to_string()),
                        end_node: Some("m".to_string()),
                        path_length: None,
                    }],
                }],
            }],
            where_expression: Some("n.temp > 30".to_string()),
            return_expressions: vec![
                ReturnExpression {
                    expression: "n.name".to_string(),
                    alias: Some("name".to_string()),
                    aggregate_function: None,
                    distinct: false,
                },
                ReturnExpression {
                    expression: "n.temp".to_string(),
                    alias: Some("temperature".to_string()),
                    aggregate_function: None,
                    distinct: false,
                },
            ],
            distinct: false,
            group_by_expressions: vec![],
            having_filters: vec![],
            order_by_conditions: vec![],
            limit: None,
        }
    }

    #[test]
    fn test_compile_basic_cypher_query() {
        let def = make_basic_definition();
        let result = CypherQueryCompiler::compile(
            "FROM STREAM events [RANGE 30s] MATCH ...",
            def,
        );
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.query_id, "test-cypher");
        assert!(compiled.where_expression.is_some());
        assert_eq!(compiled.return_expressions.len(), 2);
    }

    #[test]
    fn test_compile_cypher_with_order_by() {
        let mut def = make_basic_definition();
        def.order_by_conditions.push(OrderCondition {
            expression: "n.temp".to_string(),
            direction: SortDirection::Descending,
        });
        def.limit = Some(10);

        let result = CypherQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.order_by_expressions.len(), 1);
    }

    #[test]
    fn test_compile_cypher_with_aggregate() {
        let mut def = make_basic_definition();
        def.return_expressions = vec![ReturnExpression {
            expression: "n.temp".to_string(),
            alias: Some("avg_temp".to_string()),
            aggregate_function: Some(AggregateFunction::Avg),
            distinct: false,
        }];
        def.group_by_expressions.push("n.city".to_string());

        let result = CypherQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.aggregate_specs.len(), 1);
    }

    #[test]
    fn test_compile_cypher_with_having() {
        let mut def = make_basic_definition();
        def.having_filters.push("avg_temp > 25".to_string());

        let result = CypherQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.having_expressions.len(), 1);
    }

    #[test]
    fn test_compile_invalid_where_expression() {
        let mut def = make_basic_definition();
        def.where_expression = Some("@@@ invalid ###".to_string());

        let result = CypherQueryCompiler::compile("...", def);
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_no_where() {
        let mut def = make_basic_definition();
        def.where_expression = None;

        let result = CypherQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert!(compiled.where_expression.is_none());
    }
}
