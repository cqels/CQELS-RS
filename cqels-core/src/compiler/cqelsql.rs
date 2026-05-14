//! CqelsQL query compiler.
//!
//! Compiles a parsed `CqelsQueryDefinition` into a `CompiledCqelsQuery` that
//! can be executed against input streams. Pre-parses all expression strings
//! (filters, binds, order-by) into `Expression` trees at compile time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static CQELS_QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

use crate::expression::ast::Expression;
use crate::expression::evaluator::ExpressionEvaluator;
use crate::expression::parser::ExpressionParser;
use crate::parser::ast::{CqelsPatternGroup, CqelsQueryDefinition, SelectElement};
use crate::parser::ParseResult;
use crate::store::RdfStore;

use super::compiled::{
    CompiledAskQuery, CompiledConstructQuery, CompiledCqelsQuery, CompiledDescribeQuery,
};
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

    /// Compiles a CONSTRUCT query definition into an executable query producing Statements.
    pub fn compile_construct(
        query_string: &str,
        definition: CqelsQueryDefinition,
        rdf_store: Option<std::sync::Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledConstructQuery> {
        let template = definition.construct_template.clone();
        let prefixes = definition.prefixes.clone();
        let inner = Self::compile_with_store(query_string, definition, rdf_store)?;
        Ok(CompiledConstructQuery {
            inner,
            template,
            prefixes,
        })
    }

    /// Compiles an ASK query definition into an executable query producing booleans.
    pub fn compile_ask(
        query_string: &str,
        definition: CqelsQueryDefinition,
        rdf_store: Option<std::sync::Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledAskQuery> {
        let inner = Self::compile_with_store(query_string, definition, rdf_store)?;
        Ok(CompiledAskQuery { inner })
    }

    /// Compiles a DESCRIBE query definition into an executable query producing Statements.
    pub fn compile_describe(
        query_string: &str,
        definition: CqelsQueryDefinition,
        rdf_store: Option<std::sync::Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledDescribeQuery> {
        let describe_vars: Vec<String> = definition
            .select_elements
            .iter()
            .map(|elem| match elem {
                SelectElement::Variable(v) => v.clone(),
                SelectElement::Expression { alias, .. } => alias.clone(),
            })
            .collect();
        let prefixes = definition.prefixes.clone();
        let store_clone = rdf_store.clone();
        let inner = Self::compile_with_store(query_string, definition, rdf_store)?;
        Ok(CompiledDescribeQuery {
            inner,
            describe_vars,
            rdf_store: store_clone,
            prefixes,
        })
    }

    /// Compiles a CqelsQL query definition with an optional RDF store for
    /// resolving `STATIC { ... }` and `GRAPH <uri> { ... }` patterns.
    pub fn compile_with_store(
        query_string: &str,
        mut definition: CqelsQueryDefinition,
        rdf_store: Option<Arc<dyn RdfStore>>,
    ) -> ParseResult<CompiledCqelsQuery> {
        // Lower RSP-QL named windows into ordinary stream entries +
        // `Stream {}` pattern groups before any other compilation
        // stage runs. The rest of the pipeline (pipeline planning,
        // self-join detection, pattern projection) only understands
        // ordinary streams, so this is the one place named windows
        // need to be acknowledged.
        super::named_window::lower_named_windows(&mut definition)?;

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
        let query_id = definition.name.clone().unwrap_or_else(|| {
            let n = CQELS_QUERY_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("cqels-query-{}-{n}", hash_string(query_string))
        });

        // Detect self-join optimization opportunities. Populated whether
        // or not the runtime currently consumes them — see
        // `CompiledCqelsQuery::self_join_hints` doc for the planned
        // substitution path.
        let self_join_hints = crate::compiler::self_join::detect_self_joins(&definition);

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
            self_join_hints: Arc::new(self_join_hints),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;
    use crate::query::ContinuousQuery;
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
            named_windows: vec![],
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
            stream_semantics: StreamSemantics::default(),
            construct_template: vec![],
            seq_constraint: None,
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
    fn test_compile_ask_query() {
        let mut def = make_basic_definition();
        def.query_type = CqelsQueryType::Ask;
        def.select_elements.clear();

        let result = CqelsQueryCompiler::compile_ask("ASK ...", def, None);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert!(!compiled.query_id().is_empty());
    }

    #[test]
    fn test_compile_describe_query() {
        let mut def = make_basic_definition();
        def.query_type = CqelsQueryType::Describe;

        let result = CqelsQueryCompiler::compile_describe("DESCRIBE ...", def, None);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert!(!compiled.query_id().is_empty());
    }

    #[test]
    fn test_compile_with_prefixes() {
        let mut def = make_basic_definition();
        def.prefixes
            .insert("ex".to_string(), "http://example.org/".to_string());

        let result = CqelsQueryCompiler::compile("...", def);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert!(compiled.evaluator.prefixes().contains_key("ex"));
    }

    /// Compiling a non-self-join query records zero hints — the
    /// runtime falls through to the default pattern-matching path.
    #[test]
    fn test_compile_basic_query_has_no_self_join_hints() {
        let def = make_basic_definition();
        let compiled = CqelsQueryCompiler::compile("...", def).expect("compile");
        assert!(!compiled.has_self_join_optimization());
        assert!(compiled.self_join_hints().is_empty());
    }

    /// Compiling a self-join query (two Stream blocks on the same source
    /// with a shared variable) populates `self_join_hints` from the
    /// compile-time detector. Verifies that detection output flows
    /// through compilation into the executable artifact.
    #[test]
    fn test_compile_self_join_query_records_hint() {
        let query = r#"
            SELECT ?driver ?passenger
            FROM STREAM rides [RANGE 10s]
            WHERE {
                STREAM rides { ?driver <http://ex.org/in> ?car . }
                STREAM rides { ?passenger <http://ex.org/in> ?car . }
            }
        "#;
        let def = crate::parser::CqelsQlParser::parse(query).expect("parse");
        let compiled = CqelsQueryCompiler::compile(query, def).expect("compile");
        assert!(
            compiled.has_self_join_optimization(),
            "self-join query should produce at least one hint"
        );
        let hints = compiled.self_join_hints();
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].source, "rides");
        assert!(hints[0].join_keys.contains(&"car".to_string()));
    }

    // ─── named windows: end-to-end compile + execute ──────────────────

    /// A query that declares a named window and references it via
    /// `WINDOW :W { ... }` compiles cleanly: the lowering pass
    /// rewrites it into an ordinary stream + `STREAM {}` group, and
    /// the rest of the compiler / planner sees nothing exotic.
    #[test]
    fn compiles_named_window_query_via_lowering() {
        let query = r#"
            SELECT ?sensor ?temp
            FROM NAMED WINDOW <http://ex.org/w> ON STREAM sensors [RANGE 10s]
            WHERE {
                WINDOW <http://ex.org/w> {
                    ?sensor <http://ex.org/temp> ?temp .
                }
            }
        "#;
        let def = crate::parser::CqelsQlParser::parse(query).expect("parse");
        let compiled = CqelsQueryCompiler::compile(query, def).expect("compile");

        // After lowering, the compiled definition should look like a
        // plain `FROM STREAM sensors` query — no named windows left,
        // and the `WINDOW {}` group is now a `Stream` group on
        // `sensors`.
        let def = compiled.definition();
        assert!(def.named_windows.is_empty());
        assert_eq!(def.streams.len(), 1);
        assert_eq!(def.streams[0].name, "sensors");
        let stream_sources: Vec<&str> = def
            .pattern_groups
            .iter()
            .filter_map(|g| match g {
                CqelsPatternGroup::Stream { source, .. } => Some(source.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(stream_sources, vec!["sensors"]);
    }

    /// A compile-time `WINDOW :W { ... }` that references an
    /// undeclared IRI surfaces a clean `Semantic` parse error.
    #[test]
    fn rejects_window_reference_to_undeclared_iri_at_compile_time() {
        // Note: an undeclared `WINDOW {}` group with NO `FROM NAMED
        // WINDOW` declarations triggers the parser's stream-inference
        // heuristic which can rewrite it to `Stream`, so include a
        // separate declared window IRI to keep the lowering pass on
        // the error path.
        let query = r#"
            SELECT ?s
            FROM NAMED WINDOW <http://ex.org/declared> ON STREAM events [RANGE 5s]
            WHERE {
                WINDOW <http://ex.org/undeclared> {
                    ?s <http://ex.org/p> ?o .
                }
            }
        "#;
        let def = crate::parser::CqelsQlParser::parse(query).expect("parse");
        let err = match CqelsQueryCompiler::compile(query, def) {
            Err(e) => e,
            Ok(_) => panic!("expected compile error for undeclared named window"),
        };
        assert!(err.to_string().contains("undeclared named window"));
    }

    /// Two named windows on the same source stream with different
    /// specs are explicitly unsupported. The compiler must reject
    /// rather than silently picking one.
    #[test]
    fn rejects_conflicting_named_window_specs_at_compile_time() {
        let query = r#"
            SELECT ?s
            FROM NAMED WINDOW <http://ex.org/w1> ON STREAM sensors [RANGE 10s]
            FROM NAMED WINDOW <http://ex.org/w2> ON STREAM sensors [RANGE 30s]
            WHERE {
                WINDOW <http://ex.org/w1> { ?s <http://ex.org/p> ?o . }
            }
        "#;
        let def = crate::parser::CqelsQlParser::parse(query).expect("parse");
        let err = match CqelsQueryCompiler::compile(query, def) {
            Err(e) => e,
            Ok(_) => panic!("expected compile error for conflicting window specs"),
        };
        assert!(err.to_string().contains("multiple distinct window specs"));
    }
}
