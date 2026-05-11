//! CypherQL parser implementation.
//!
//! Parses CypherQL query strings (Cypher-based continuous queries with streaming
//! extensions) into `CypherQueryDefinition` ASTs.

use std::collections::HashMap;
use std::time::Duration;

use pest::Parser;
use pest_derive::Parser;

use super::ast::*;
use super::{ParseError, ParseResult};

#[derive(Parser)]
#[grammar = "parser/cypherql.pest"]
struct CypherQlPestParser;

/// Parser for CypherQL queries.
pub struct CypherQlParser;

impl CypherQlParser {
    /// Parses a CypherQL query string into a `CypherQueryDefinition`.
    pub fn parse(input: &str) -> ParseResult<CypherQueryDefinition> {
        let pairs = CypherQlPestParser::parse(Rule::query, input)
            .map_err(|e| ParseError::Syntax(e.to_string()))?;

        let mut builder = CypherQueryDefinition::builder();

        for pair in pairs {
            if pair.as_rule() == Rule::query {
                for inner in pair.into_inner() {
                    match inner.as_rule() {
                        Rule::register_query => {
                            builder = parse_register_query(inner, builder)?;
                        }
                        Rule::continuous_query => {
                            builder = parse_continuous_query(inner, builder)?;
                        }
                        Rule::EOI => {}
                        _ => {}
                    }
                }
            }
        }

        Ok(builder.build())
    }
}

fn parse_register_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    let mut inner = pair.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected query name".into()))?
        .as_str()
        .to_string();
    builder = builder.name(name);

    if let Some(cq) = inner.next() {
        builder = parse_continuous_query(cq, builder)?;
    }

    Ok(builder)
}

fn parse_continuous_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::from_clauses => {
                builder = parse_from_clauses(inner, builder)?;
            }
            Rule::match_clause => {
                builder = parse_match_clause(inner, builder)?;
            }
            Rule::where_clause => {
                let expr = inner
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_default();
                builder = builder.where_expression(expr);
            }
            Rule::group_by_clause => {
                builder = parse_group_by(inner, builder)?;
            }
            Rule::having_clause | Rule::post_return_having_clause => {
                let expr = inner
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_default();
                builder = builder.add_having_filter(expr);
            }
            Rule::return_clause => {
                builder = parse_return_clause(inner, builder)?;
            }
            Rule::order_by_clause => {
                builder = parse_order_by(inner, builder)?;
            }
            Rule::limit_clause => {
                builder = parse_limit(inner, builder)?;
            }
            _ => {}
        }
    }
    Ok(builder)
}

fn parse_from_clauses(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::from_clause {
            for clause in inner.into_inner() {
                match clause.as_rule() {
                    Rule::from_stream => {
                        builder = parse_from_stream(clause, builder)?;
                    }
                    Rule::from_static => {
                        builder = parse_from_static(clause, builder)?;
                    }
                    Rule::from_graph => {
                        builder = parse_from_graph(clause, builder)?;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(builder)
}

fn parse_from_stream(
    pair: pest::iterators::Pair<Rule>,
    builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    let mut inner = pair.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected stream name".into()))?
        .as_str()
        .to_string();
    let window = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected window spec".into()))?;
    let window_spec = parse_window_spec(window)?;

    Ok(builder.add_stream(CypherStreamDefinition {
        name,
        window: window_spec,
    }))
}

fn parse_from_static(
    pair: pest::iterators::Pair<Rule>,
    builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    let mut uri = String::new();
    let mut depth = None;
    let mut cache = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::string_literal => {
                uri = unquote_string(inner.as_str());
            }
            Rule::integer_literal if depth.is_none() => {
                depth = inner.as_str().parse().ok();
            }
            Rule::cache_spec => {
                cache = Some(parse_duration_from_pair(inner)?);
            }
            _ => {}
        }
    }

    Ok(builder.add_static_graph(GraphDefinition {
        uri,
        depth,
        cache_duration: cache,
    }))
}

fn parse_from_graph(
    pair: pest::iterators::Pair<Rule>,
    builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    let mut uri = String::new();
    let mut depth = None;
    let mut cache = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::string_literal => {
                uri = unquote_string(inner.as_str());
            }
            Rule::integer_literal if depth.is_none() => {
                depth = inner.as_str().parse().ok();
            }
            Rule::cache_spec => {
                cache = Some(parse_duration_from_pair(inner)?);
            }
            _ => {}
        }
    }

    Ok(builder.add_named_graph(GraphDefinition {
        uri,
        depth,
        cache_duration: cache,
    }))
}

fn parse_window_spec(pair: pest::iterators::Pair<Rule>) -> ParseResult<WindowSpec> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::window_now => return Ok(WindowSpec::now()),
            Rule::window_range => {
                let mut durations = Vec::new();
                for d in inner.into_inner() {
                    if d.as_rule() == Rule::duration {
                        durations.push(parse_duration_value(d)?);
                    }
                }
                if durations.len() >= 2 {
                    return Ok(WindowSpec::slide(durations[0], durations[1]));
                }
                return Ok(WindowSpec::range(
                    durations
                        .into_iter()
                        .next()
                        .unwrap_or(Duration::from_secs(0)),
                ));
            }
            Rule::window_triples => {
                let mut integers = Vec::new();
                for d in inner.into_inner() {
                    if d.as_rule() == Rule::integer_literal {
                        let val: u64 = d
                            .as_str()
                            .parse()
                            .map_err(|e| ParseError::Syntax(format!("invalid count: {e}")))?;
                        integers.push(val);
                    }
                }
                if integers.len() >= 2 {
                    return Ok(WindowSpec::triples_slide(integers[0], integers[1]));
                }
                if let Some(&count) = integers.first() {
                    return Ok(WindowSpec::triples(count));
                }
                return Err(ParseError::Syntax("missing triple count".into()));
            }
            Rule::window_slide => {
                let mut durations = Vec::new();
                for d in inner.into_inner() {
                    if d.as_rule() == Rule::duration {
                        durations.push(parse_duration_value(d)?);
                    }
                }
                if durations.len() < 2 {
                    return Err(ParseError::Syntax(
                        "SLIDE requires duration and step".into(),
                    ));
                }
                return Ok(WindowSpec::slide(durations[0], durations[1]));
            }
            _ => {}
        }
    }
    Err(ParseError::Syntax("invalid window specification".into()))
}

fn parse_duration_value(pair: pest::iterators::Pair<Rule>) -> ParseResult<Duration> {
    let mut value: u64 = 0;
    let mut unit = "s";

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::integer_literal => {
                value = inner
                    .as_str()
                    .parse()
                    .map_err(|e| ParseError::Syntax(format!("invalid duration value: {e}")))?;
            }
            Rule::time_unit => {
                unit = inner.as_str();
            }
            _ => {}
        }
    }

    let unit_lower = unit.to_lowercase();
    match unit_lower.as_str() {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        "h" => Ok(Duration::from_secs(value * 3600)),
        "d" => Ok(Duration::from_secs(value * 86400)),
        _ => Err(ParseError::Syntax(format!("unknown time unit: {unit}"))),
    }
}

fn parse_duration_from_pair(pair: pest::iterators::Pair<Rule>) -> ParseResult<Duration> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::duration {
            return parse_duration_value(inner);
        }
    }
    Err(ParseError::Syntax("expected duration".into()))
}

fn parse_match_clause(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern_group {
            let group = parse_pattern_group(inner)?;
            builder = builder.add_pattern_group(group);
        }
    }
    Ok(builder)
}

fn parse_pattern_group(pair: pest::iterators::Pair<Rule>) -> ParseResult<PatternGroup> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::stream_pattern_group => {
                let mut it = inner.into_inner();
                let source_name = it
                    .next()
                    .ok_or_else(|| ParseError::Syntax("expected stream name".into()))?
                    .as_str()
                    .to_string();
                let mut patterns = Vec::new();
                for p in it {
                    if p.as_rule() == Rule::pattern {
                        patterns.extend(parse_cypher_patterns(p)?);
                    }
                }
                return Ok(PatternGroup {
                    source: PatternSource::Stream,
                    source_name: Some(source_name),
                    patterns,
                });
            }
            Rule::static_pattern_group => {
                let mut patterns = Vec::new();
                for p in inner.into_inner() {
                    if p.as_rule() == Rule::pattern {
                        patterns.extend(parse_cypher_patterns(p)?);
                    }
                }
                return Ok(PatternGroup {
                    source: PatternSource::Static,
                    source_name: None,
                    patterns,
                });
            }
            Rule::graph_pattern_group => {
                let mut it = inner.into_inner();
                let graph_uri = it
                    .next()
                    .map(|p| unquote_string(p.as_str()))
                    .unwrap_or_default();
                let mut patterns = Vec::new();
                for p in it {
                    if p.as_rule() == Rule::pattern {
                        patterns.extend(parse_cypher_patterns(p)?);
                    }
                }
                return Ok(PatternGroup {
                    source: PatternSource::Graph,
                    source_name: Some(graph_uri),
                    patterns,
                });
            }
            Rule::pattern => {
                let patterns = parse_cypher_patterns(inner)?;
                return Ok(PatternGroup {
                    source: PatternSource::Default,
                    source_name: None,
                    patterns,
                });
            }
            _ => {}
        }
    }
    Err(ParseError::Syntax("invalid pattern group".into()))
}

fn parse_cypher_patterns(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<CypherPattern>> {
    let mut patterns = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern_part {
            let pattern = parse_pattern_part(inner)?;
            patterns.push(pattern);
        }
    }

    Ok(patterns)
}

fn parse_pattern_part(pair: pest::iterators::Pair<Rule>) -> ParseResult<CypherPattern> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                // Named pattern variable, skip for now
            }
            Rule::anonymous_pattern_part => {
                let child = inner.into_inner().next().ok_or_else(|| {
                    ParseError::Syntax("expected pattern element in anonymous pattern part".into())
                })?;
                parse_pattern_element(child, &mut nodes, &mut relationships)?;
            }
            Rule::pattern_element => {
                parse_pattern_element(inner, &mut nodes, &mut relationships)?;
            }
            _ => {}
        }
    }

    Ok(CypherPattern {
        nodes,
        relationships,
    })
}

fn parse_pattern_element(
    pair: pest::iterators::Pair<Rule>,
    nodes: &mut Vec<NodePattern>,
    relationships: &mut Vec<RelationshipPattern>,
) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => {
                nodes.push(parse_node_pattern(inner)?);
            }
            Rule::pattern_element_chain => {
                for chain_inner in inner.into_inner() {
                    match chain_inner.as_rule() {
                        Rule::relationship_pattern => {
                            let start = nodes.last().and_then(|n| n.variable.clone());
                            let rel = parse_relationship_pattern(chain_inner, start)?;
                            relationships.push(rel);
                        }
                        Rule::node_pattern => {
                            let node = parse_node_pattern(chain_inner)?;
                            // Update end_node on last relationship
                            if let Some(last_rel) = relationships.last_mut() {
                                last_rel.end_node = node.variable.clone();
                            }
                            nodes.push(node);
                        }
                        _ => {}
                    }
                }
            }
            Rule::pattern_element => {
                parse_pattern_element(inner, nodes, relationships)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_node_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = HashMap::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                variable = Some(inner.as_str().to_string());
            }
            Rule::node_labels => {
                for label in inner.into_inner() {
                    if label.as_rule() == Rule::node_label {
                        for l in label.into_inner() {
                            if l.as_rule() == Rule::identifier {
                                labels.push(l.as_str().to_string());
                            }
                        }
                    }
                }
            }
            Rule::properties => {
                properties = parse_properties(inner)?;
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
    })
}

fn parse_relationship_pattern(
    pair: pest::iterators::Pair<Rule>,
    start_node: Option<String>,
) -> ParseResult<RelationshipPattern> {
    let mut variable = None;
    let mut types = Vec::new();
    let mut direction = RelDirection::Undirected;
    let mut properties = HashMap::new();
    let mut path_length = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::incoming_rel => {
                direction = RelDirection::Incoming;
                for detail in inner.into_inner() {
                    if detail.as_rule() == Rule::relationship_detail {
                        parse_rel_detail(
                            detail,
                            &mut variable,
                            &mut types,
                            &mut path_length,
                            &mut properties,
                        )?;
                    }
                }
            }
            Rule::outgoing_rel => {
                direction = RelDirection::Outgoing;
                for detail in inner.into_inner() {
                    if detail.as_rule() == Rule::relationship_detail {
                        parse_rel_detail(
                            detail,
                            &mut variable,
                            &mut types,
                            &mut path_length,
                            &mut properties,
                        )?;
                    }
                }
            }
            Rule::undirected_rel => {
                direction = RelDirection::Undirected;
                for detail in inner.into_inner() {
                    if detail.as_rule() == Rule::relationship_detail {
                        parse_rel_detail(
                            detail,
                            &mut variable,
                            &mut types,
                            &mut path_length,
                            &mut properties,
                        )?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(RelationshipPattern {
        variable,
        types,
        direction,
        properties,
        start_node,
        end_node: None,
        path_length,
    })
}

fn parse_rel_detail(
    pair: pest::iterators::Pair<Rule>,
    variable: &mut Option<String>,
    types: &mut Vec<String>,
    path_length: &mut Option<PathLength>,
    properties: &mut HashMap<String, String>,
) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier if variable.is_none() => {
                *variable = Some(inner.as_str().to_string());
            }
            Rule::relationship_types => {
                for t in inner.into_inner() {
                    if t.as_rule() == Rule::identifier {
                        types.push(t.as_str().to_string());
                    }
                }
            }
            Rule::path_length => {
                *path_length = Some(parse_path_length(inner)?);
            }
            Rule::properties => {
                *properties = parse_properties(inner)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_path_length(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathLength> {
    let mut ints: Vec<u32> = Vec::new();
    let text = pair.as_str();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::integer_literal {
            if let Ok(v) = inner.as_str().parse() {
                ints.push(v);
            }
        }
    }

    if ints.is_empty() {
        Ok(PathLength {
            min_length: None,
            max_length: None,
        })
    } else if ints.len() == 1 {
        if text.contains("..") {
            if text.starts_with("*..") || text.starts_with("* ..") {
                Ok(PathLength {
                    min_length: None,
                    max_length: Some(ints[0]),
                })
            } else {
                Ok(PathLength {
                    min_length: Some(ints[0]),
                    max_length: None,
                })
            }
        } else {
            Ok(PathLength {
                min_length: Some(ints[0]),
                max_length: Some(ints[0]),
            })
        }
    } else {
        Ok(PathLength {
            min_length: Some(ints[0]),
            max_length: Some(ints[1]),
        })
    }
}

fn parse_properties(pair: pest::iterators::Pair<Rule>) -> ParseResult<HashMap<String, String>> {
    let mut props = HashMap::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_key_value {
            let mut it = inner.into_inner();
            let key = it
                .next()
                .map(|p| p.as_str().to_string())
                .unwrap_or_default();
            let value = it
                .next()
                .map(|p| p.as_str().to_string())
                .unwrap_or_default();
            props.insert(key, value);
        }
    }

    Ok(props)
}

fn parse_return_clause(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct_kw => {
                builder = builder.distinct(true);
            }
            Rule::return_items => {
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::return_item {
                        let ret_expr = parse_return_item(item)?;
                        builder = builder.add_return_expression(ret_expr);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(builder)
}

fn parse_return_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<ReturnExpression> {
    let mut expression = String::new();
    let mut alias = None;

    let mut inner_pairs: Vec<_> = pair.into_inner().collect();

    // Last identifier after AS is the alias
    if inner_pairs.len() >= 2 {
        let last = &inner_pairs[inner_pairs.len() - 1];
        if last.as_rule() == Rule::identifier {
            alias = Some(last.as_str().to_string());
            inner_pairs.pop();
        }
    }

    if let Some(expr_pair) = inner_pairs.first() {
        expression = expr_pair.as_str().to_string();
    }

    // Detect aggregate functions
    let aggregate_function = detect_aggregate(&expression);

    Ok(ReturnExpression {
        expression,
        alias,
        aggregate_function,
        distinct: false,
    })
}

fn detect_aggregate(expr: &str) -> Option<AggregateFunction> {
    let upper = expr.trim().to_uppercase();
    let funcs = [
        ("COUNT(", AggregateFunction::Count),
        ("SUM(", AggregateFunction::Sum),
        ("AVG(", AggregateFunction::Avg),
        ("MIN(", AggregateFunction::Min),
        ("MAX(", AggregateFunction::Max),
        ("COLLECT(", AggregateFunction::Collect),
    ];

    for (prefix, func) in &funcs {
        if upper.starts_with(prefix) {
            return Some(*func);
        }
    }
    None
}

fn parse_group_by(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::group_item {
            builder = builder.add_group_by(inner.as_str().to_string());
        }
    }
    Ok(builder)
}

fn parse_order_by(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::order_item {
            let mut expr = String::new();
            let mut direction = SortDirection::Ascending;

            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::expression => {
                        expr = part.as_str().to_string();
                    }
                    Rule::desc_kw => {
                        direction = SortDirection::Descending;
                    }
                    Rule::asc_kw => {
                        direction = SortDirection::Ascending;
                    }
                    _ => {}
                }
            }

            builder = builder.add_order_by(OrderCondition {
                expression: expr,
                direction,
            });
        }
    }
    Ok(builder)
}

fn parse_limit(
    pair: pest::iterators::Pair<Rule>,
    builder: CypherQueryDefinitionBuilder,
) -> ParseResult<CypherQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::integer_literal {
            let limit: u64 = inner
                .as_str()
                .parse()
                .map_err(|e| ParseError::Syntax(format!("invalid limit: {e}")))?;
            return Ok(builder.limit(limit));
        }
    }
    Err(ParseError::Syntax("expected limit value".into()))
}

fn unquote_string(s: &str) -> String {
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_cypher_query() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            RETURN n.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.streams.len(), 1);
        assert_eq!(result.streams[0].name, "events");
        assert_eq!(result.streams[0].window, WindowSpec::now());
        assert_eq!(result.pattern_groups.len(), 1);
        assert_eq!(result.return_expressions.len(), 1);
    }

    #[test]
    fn test_parse_register_cypher_query() {
        let query = r#"
            REGISTER QUERY myQ AS
            FROM STREAM events [NOW]
            MATCH (n:Person)
            RETURN n.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.name, Some("myQ".to_string()));
    }

    #[test]
    fn test_parse_relationship_pattern() {
        let query = r#"
            FROM STREAM social [RANGE 30s]
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            RETURN a.name, b.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::range(Duration::from_secs(30))
        );
        assert_eq!(result.pattern_groups.len(), 1);
        let group = &result.pattern_groups[0];
        assert_eq!(group.source, PatternSource::Default);
        assert!(!group.patterns.is_empty());

        let pattern = &group.patterns[0];
        assert_eq!(pattern.nodes.len(), 2);
        assert_eq!(pattern.nodes[0].labels, vec!["Person"]);
        assert_eq!(pattern.nodes[1].labels, vec!["Person"]);
        assert_eq!(pattern.relationships.len(), 1);
        assert_eq!(pattern.relationships[0].direction, RelDirection::Outgoing);
        assert_eq!(pattern.relationships[0].types, vec!["KNOWS"]);
    }

    #[test]
    fn test_parse_window_specs() {
        let query = r#"
            FROM STREAM s1 [TRIPLES 50]
            MATCH (n)
            RETURN n
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.streams[0].window, WindowSpec::triples(50));

        let query = r#"
            FROM STREAM s1 [TRIPLES 50 SLIDE 25]
            MATCH (n)
            RETURN n
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.streams[0].window, WindowSpec::triples_slide(50, 25));

        let query = r#"
            FROM STREAM s1 [SLIDE 2m STEP 30s]
            MATCH (n)
            RETURN n
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::slide(Duration::from_secs(120), Duration::from_secs(30))
        );
    }

    #[test]
    fn test_parse_where_clause() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            WHERE n.age > 18
            RETURN n.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert!(result.where_expression.is_some());
        let expr = result.where_expression.unwrap();
        assert!(expr.contains("n.age"));
    }

    #[test]
    fn test_parse_group_by_order_by_limit() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            GROUP BY n.city
            RETURN n.city, count(n) AS cnt
            ORDER BY cnt DESC
            LIMIT 10
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert!(!result.group_by_expressions.is_empty());
        assert!(!result.order_by_conditions.is_empty());
        assert_eq!(
            result.order_by_conditions[0].direction,
            SortDirection::Descending
        );
        assert_eq!(result.limit, Some(10));
    }

    #[test]
    fn test_parse_distinct() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            RETURN DISTINCT n.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert!(result.distinct);
    }

    #[test]
    fn test_parse_stream_pattern_group() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH STREAM events { (n:Event) }
            RETURN n.type
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 1);
        assert_eq!(result.pattern_groups[0].source, PatternSource::Stream);
        assert_eq!(
            result.pattern_groups[0].source_name,
            Some("events".to_string())
        );
    }

    #[test]
    fn test_parse_multiple_sources() {
        let query = r#"
            FROM STREAM events [NOW]
            FROM STATIC 'http://example.org/static'
            FROM GRAPH 'http://example.org/graph'
            MATCH (n:Person)
            RETURN n.name
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.streams.len(), 1);
        assert_eq!(result.static_graphs.len(), 1);
        assert_eq!(result.static_graphs[0].uri, "http://example.org/static");
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].uri, "http://example.org/graph");
    }

    #[test]
    fn test_parse_error_invalid_query() {
        let result = CypherQlParser::parse("NOT A VALID QUERY");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_node_with_properties() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person {name: "Alice"})
            RETURN n
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        let pattern = &result.pattern_groups[0].patterns[0];
        assert_eq!(pattern.nodes[0].labels, vec!["Person"]);
        assert!(pattern.nodes[0].properties.contains_key("name"));
    }

    #[test]
    fn test_order_by_with_property_access() {
        // ORDER BY with simple identifier
        let q1 = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            RETURN p
            ORDER BY p DESC
        "#;
        let r1 = CypherQlParser::parse(q1).unwrap();
        assert_eq!(r1.order_by_conditions.len(), 1);
        assert_eq!(
            r1.order_by_conditions[0].direction,
            SortDirection::Descending
        );

        // ORDER BY with property access expression
        let q2 = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            RETURN p.name, p.age
            ORDER BY p.age DESC
            LIMIT 10
        "#;
        let r2 = CypherQlParser::parse(q2).unwrap();
        assert_eq!(r2.return_expressions.len(), 2);
        assert_eq!(r2.order_by_conditions.len(), 1);
        assert_eq!(
            r2.order_by_conditions[0].direction,
            SortDirection::Descending
        );
        assert_eq!(r2.limit, Some(10));

        // ORDER BY with multiple sort keys
        let q3 = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            RETURN p.name, p.age, p.city
            ORDER BY p.city ASC, p.age DESC
        "#;
        let r3 = CypherQlParser::parse(q3).unwrap();
        assert_eq!(r3.order_by_conditions.len(), 2);
        assert_eq!(
            r3.order_by_conditions[0].direction,
            SortDirection::Ascending
        );
        assert_eq!(
            r3.order_by_conditions[1].direction,
            SortDirection::Descending
        );
    }

    #[test]
    fn test_parse_return_with_alias() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            RETURN n.name AS personName
        "#;

        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.return_expressions.len(), 1);
        assert_eq!(
            result.return_expressions[0].alias,
            Some("personName".to_string())
        );
    }

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_parse_error_empty_input() {
        assert!(CypherQlParser::parse("").is_err());
    }

    #[test]
    fn test_parse_error_missing_from() {
        let result = CypherQlParser::parse("MATCH (n) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_missing_return() {
        let result = CypherQlParser::parse("FROM STREAM s [NOW] MATCH (n:Person)");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_window() {
        let result = CypherQlParser::parse("FROM STREAM s [] MATCH (n) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incoming_relationship() {
        let query = r#"
            FROM STREAM s [NOW]
            MATCH (a:Person)<-[:FOLLOWS]-(b:Person)
            RETURN a.name, b.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        let rel = &result.pattern_groups[0].patterns[0].relationships[0];
        assert_eq!(rel.direction, RelDirection::Incoming);
        assert_eq!(rel.types, vec!["FOLLOWS"]);
    }

    #[test]
    fn test_parse_undirected_relationship() {
        let query = r#"
            FROM STREAM s [NOW]
            MATCH (a:Person)-[:KNOWS]-(b:Person)
            RETURN a.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        let rel = &result.pattern_groups[0].patterns[0].relationships[0];
        assert_eq!(rel.direction, RelDirection::Undirected);
    }

    #[test]
    fn test_parse_variable_length_path() {
        let query = r#"
            FROM STREAM s [NOW]
            MATCH (a)-[:FOLLOWS*1..5]->(b)
            RETURN a, b
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        let rel = &result.pattern_groups[0].patterns[0].relationships[0];
        assert!(rel.path_length.is_some());
        let pl = rel.path_length.as_ref().unwrap();
        assert_eq!(pl.min_length, Some(1));
        assert_eq!(pl.max_length, Some(5));
    }

    #[test]
    fn test_parse_multiple_labels() {
        let query = r#"
            FROM STREAM s [NOW]
            MATCH (n:Person:Employee)
            RETURN n.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        let node = &result.pattern_groups[0].patterns[0].nodes[0];
        assert_eq!(node.labels, vec!["Person", "Employee"]);
    }

    #[test]
    fn test_parse_having_clause() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (n:Person)
            GROUP BY n.city
            HAVING count(n) > 10
            RETURN n.city, count(n) AS cnt
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert!(!result.having_filters.is_empty());
    }

    #[test]
    fn test_parse_where_comparison_operators() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            WHERE p.age >= 18
            RETURN p.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert!(result.where_expression.is_some());
        let expr = result.where_expression.unwrap();
        assert!(expr.contains("p.age") && expr.contains("18"));
    }

    #[test]
    fn test_parse_where_and_expression() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            WHERE p.age > 18 AND p.active = true
            RETURN p.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert!(result.where_expression.is_some());
    }

    #[test]
    fn test_parse_where_or_expression() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            WHERE p.city = "Berlin" OR p.city = "Munich"
            RETURN p.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert!(result.where_expression.is_some());
    }

    #[test]
    fn test_parse_aggregate_functions() {
        let query = r#"
            FROM STREAM events [NOW]
            MATCH (p:Person)
            RETURN count(p) AS total, sum(p.age) AS ageSum, avg(p.age) AS avgAge
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.return_expressions.len(), 3);
        assert_eq!(
            result.return_expressions[0].aggregate_function,
            Some(AggregateFunction::Count)
        );
        assert_eq!(
            result.return_expressions[1].aggregate_function,
            Some(AggregateFunction::Sum)
        );
        assert_eq!(
            result.return_expressions[2].aggregate_function,
            Some(AggregateFunction::Avg)
        );
    }

    #[test]
    fn test_parse_window_range_with_step() {
        let query = r#"
            FROM STREAM s [RANGE 10m STEP 5m]
            MATCH (n) RETURN n
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::slide(Duration::from_secs(600), Duration::from_secs(300))
        );
    }

    #[test]
    fn test_parse_multiple_pattern_groups() {
        let query = r#"
            FROM STREAM events [NOW]
            FROM STATIC 'http://kb.org/data'
            MATCH
                STREAM events { (e:Event)-[:TRIGGERED_BY]->(s:Sensor) },
                STATIC { (s:Sensor)-[:LOCATED_IN]->(l:Location) }
            RETURN e.type, l.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 2);
        assert_eq!(result.pattern_groups[0].source, PatternSource::Stream);
        assert_eq!(result.pattern_groups[1].source, PatternSource::Static);
    }

    #[test]
    fn test_parse_chain_three_nodes() {
        let query = r#"
            FROM STREAM s [NOW]
            MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City)
            RETURN a.name, c.name
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        let pattern = &result.pattern_groups[0].patterns[0];
        assert_eq!(pattern.nodes.len(), 3);
        assert_eq!(pattern.relationships.len(), 2);
    }

    #[test]
    fn test_parse_static_with_depth_and_cache() {
        let query = r#"
            FROM STREAM s [NOW]
            FROM STATIC 'http://kb.org/data' WITH DEPTH 3 CACHE 5m
            MATCH (n) RETURN n
        "#;
        let result = CypherQlParser::parse(query).unwrap();
        assert_eq!(result.static_graphs.len(), 1);
        assert_eq!(result.static_graphs[0].depth, Some(3));
        assert_eq!(
            result.static_graphs[0].cache_duration,
            Some(Duration::from_secs(300))
        );
    }
}
