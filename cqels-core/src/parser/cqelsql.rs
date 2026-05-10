//! CqelsQL parser implementation.
//!
//! Parses CQELS-QL query strings (SPARQL-based continuous queries with streaming
//! extensions) into `CqelsQueryDefinition` ASTs.

use std::time::Duration;

use pest::Parser;
use pest_derive::Parser;

use super::ast::*;
use super::{ParseError, ParseResult};

#[derive(Parser)]
#[grammar = "parser/cqelsql.pest"]
struct CqelsQlPestParser;

/// Parser for CqelsQL queries.
pub struct CqelsQlParser;

impl CqelsQlParser {
    /// Parses a CqelsQL query string into a `CqelsQueryDefinition`.
    pub fn parse(input: &str) -> ParseResult<CqelsQueryDefinition> {
        let pairs = CqelsQlPestParser::parse(Rule::query, input)
            .map_err(|e| ParseError::Syntax(e.to_string()))?;

        let mut builder = CqelsQueryDefinition::builder();

        for pair in pairs {
            if pair.as_rule() == Rule::query {
                for inner in pair.into_inner() {
                    match inner.as_rule() {
                        Rule::prefix_decl => {
                            let (prefix, uri) = parse_prefix_decl(inner)?;
                            builder = builder.add_prefix(prefix, uri);
                        }
                        Rule::register_query => {
                            builder = parse_register_query(inner, builder)?;
                        }
                        Rule::select_query => {
                            builder = parse_select_query(inner, builder)?;
                        }
                        Rule::construct_query => {
                            builder = parse_construct_query(inner, builder)?;
                        }
                        Rule::ask_query => {
                            builder = parse_ask_query(inner, builder)?;
                        }
                        Rule::describe_query => {
                            builder = parse_describe_query(inner, builder)?;
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

fn parse_prefix_decl(pair: pest::iterators::Pair<Rule>) -> ParseResult<(String, String)> {
    let mut inner = pair.into_inner();
    let prefix = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected prefix name".into()))?
        .as_str()
        .trim_end_matches(':')
        .to_string();
    let uri = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected IRI".into()))?
        .as_str()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string();
    Ok((prefix, uri))
}

fn parse_register_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    let mut inner = pair.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| ParseError::Syntax("expected query name".into()))?
        .as_str()
        .to_string();
    builder = builder.name(name);

    if let Some(select) = inner.next() {
        builder = parse_select_query(select, builder)?;
    }

    Ok(builder)
}

fn parse_select_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    builder = builder.query_type(CqelsQueryType::Select);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::stream_semantics_kw => {
                builder = parse_stream_semantics(inner, builder)?;
            }
            Rule::select_list => {
                builder = parse_select_list(inner, builder)?;
            }
            Rule::from_clauses => {
                builder = parse_from_clauses(inner, builder)?;
            }
            Rule::where_clause => {
                builder = parse_where_clause(inner, builder)?;
            }
            Rule::group_by_clause => {
                builder = parse_group_by(inner, builder)?;
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

fn parse_construct_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    builder = builder.query_type(CqelsQueryType::Construct);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::construct_template => {
                for tp in inner.into_inner() {
                    if tp.as_rule() == Rule::triple_pattern {
                        for pattern in parse_triple_patterns(tp)? {
                            builder = builder.add_construct_template(pattern);
                        }
                    }
                }
            }
            Rule::from_clauses => {
                builder = parse_from_clauses(inner, builder)?;
            }
            Rule::where_clause => {
                builder = parse_where_clause(inner, builder)?;
            }
            _ => {}
        }
    }

    Ok(builder)
}

fn parse_ask_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    builder = builder.query_type(CqelsQueryType::Ask);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::from_clauses => {
                builder = parse_from_clauses(inner, builder)?;
            }
            Rule::where_clause => {
                builder = parse_where_clause(inner, builder)?;
            }
            _ => {}
        }
    }

    Ok(builder)
}

fn parse_describe_query(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    builder = builder.query_type(CqelsQueryType::Describe);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::describe_target => {
                for target in inner.into_inner() {
                    match target.as_rule() {
                        Rule::variable => {
                            builder = builder.add_select_element(SelectElement::Variable(
                                target.as_str().to_string(),
                            ));
                        }
                        Rule::star => {
                            // Star means describe all variables — leave select_elements empty
                        }
                        _ => {}
                    }
                }
            }
            Rule::from_clauses => {
                builder = parse_from_clauses(inner, builder)?;
            }
            Rule::where_clause => {
                builder = parse_where_clause(inner, builder)?;
            }
            _ => {}
        }
    }

    Ok(builder)
}

fn parse_stream_semantics(
    pair: pest::iterators::Pair<Rule>,
    builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::istream_kw => return Ok(builder.stream_semantics(StreamSemantics::IStream)),
            Rule::dstream_kw => return Ok(builder.stream_semantics(StreamSemantics::DStream)),
            Rule::rstream_kw => return Ok(builder.stream_semantics(StreamSemantics::RStream)),
            _ => {}
        }
    }
    Ok(builder)
}

fn parse_select_list(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct_kw => {
                builder = builder.distinct(true);
            }
            Rule::select_element => {
                for elem_inner in inner.into_inner() {
                    match elem_inner.as_rule() {
                        Rule::variable => {
                            builder = builder.add_select_element(SelectElement::Variable(
                                elem_inner.as_str().to_string(),
                            ));
                        }
                        Rule::expression_alias => {
                            let mut parts = elem_inner.into_inner();
                            let expr = parts
                                .next()
                                .map(|p| p.as_str().to_string())
                                .unwrap_or_default();
                            let alias = parts
                                .next()
                                .map(|p| p.as_str().to_string())
                                .unwrap_or_default();

                            // Check if this is an aggregate
                            if let Some(agg) = try_parse_aggregate(&expr, &alias) {
                                builder = builder.add_aggregate(agg);
                            }

                            builder = builder.add_select_element(SelectElement::Expression {
                                expression: expr,
                                alias,
                            });
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(builder)
}

fn try_parse_aggregate(expr: &str, alias: &str) -> Option<AggregateSpec> {
    let upper = expr.trim().to_uppercase();
    let funcs = [
        ("GROUP_CONCAT", AggregateFunction::GroupConcat),
        ("COUNT", AggregateFunction::Count),
        ("AVG", AggregateFunction::Avg),
        ("SUM", AggregateFunction::Sum),
        ("MIN", AggregateFunction::Min),
        ("MAX", AggregateFunction::Max),
    ];

    for (name, func) in &funcs {
        if upper.starts_with(name) && upper.contains('(') {
            let start = upper.find('(')? + 1;
            let end = upper.rfind(')')?;
            let arg = expr[start..end].trim().to_string();
            return Some(AggregateSpec {
                function: *func,
                argument: arg,
                alias: alias.to_string(),
            });
        }
    }
    None
}

fn parse_from_clauses(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
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
                    Rule::from_named => {
                        builder = parse_from_named(clause, builder)?;
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
    builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
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

    Ok(builder.add_stream(CqelsStreamDefinition {
        name,
        window: window_spec,
    }))
}

fn parse_from_static(
    pair: pest::iterators::Pair<Rule>,
    builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    let mut uri = String::new();
    let mut depth = None;
    let mut cache = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::iri_ref => {
                uri = inner
                    .as_str()
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string();
            }
            Rule::integer => {
                if depth.is_none() {
                    depth = inner.as_str().parse().ok();
                }
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

fn parse_from_named(
    pair: pest::iterators::Pair<Rule>,
    builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    let mut uri = String::new();
    let mut depth = None;
    let mut cache = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::iri_ref => {
                uri = inner
                    .as_str()
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string();
            }
            Rule::integer => {
                if depth.is_none() {
                    depth = inner.as_str().parse().ok();
                }
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
                    // RANGE with STEP becomes SLIDE
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
                    if d.as_rule() == Rule::integer {
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
            Rule::integer => {
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

    match unit {
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

fn parse_where_clause(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    let mut groups: Vec<CqelsPatternGroup> = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern_group {
            groups.push(parse_pattern_group(inner)?);
        }
    }

    apply_implicit_stream_binding(&mut groups, &builder);

    for group in groups {
        builder = builder.add_pattern_group(group);
    }
    Ok(builder)
}

/// Implicit stream binding (Java PR #31, Phase 1).
///
/// When the query has exactly one `FROM STREAM`, no FROM-level static/named
/// graph declarations, and no explicit `STREAM { ... }` blocks in the WHERE,
/// any bare triple patterns are auto-bound to that single stream — matching
/// what the user would have written as `STREAM <s> { ... }` explicitly.
fn apply_implicit_stream_binding(
    groups: &mut [CqelsPatternGroup],
    builder: &CqelsQueryDefinitionBuilder,
) {
    let has_explicit_stream = groups
        .iter()
        .any(|g| matches!(g, CqelsPatternGroup::Stream { .. }));
    let has_default = groups
        .iter()
        .any(|g| matches!(g, CqelsPatternGroup::Default { .. }));

    let eligible = has_default
        && !has_explicit_stream
        && builder.streams().len() == 1
        && builder.static_graphs().is_empty()
        && builder.named_graphs().is_empty();

    if !eligible {
        return;
    }

    let stream_name = builder.streams()[0].name.clone();
    for group in groups.iter_mut() {
        if let CqelsPatternGroup::Default { patterns } = group {
            let patterns = std::mem::take(patterns);
            *group = CqelsPatternGroup::Stream {
                source: stream_name.clone(),
                patterns,
            };
        }
    }
}

fn parse_pattern_group(pair: pest::iterators::Pair<Rule>) -> ParseResult<CqelsPatternGroup> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::stream_pattern_group => {
                let mut it = inner.into_inner();
                let source = it
                    .next()
                    .ok_or_else(|| ParseError::Syntax("expected stream name".into()))?
                    .as_str()
                    .to_string();
                let mut patterns = Vec::new();
                for tp in it {
                    if tp.as_rule() == Rule::triple_pattern {
                        patterns.extend(parse_triple_patterns(tp)?);
                    }
                }
                return Ok(CqelsPatternGroup::Stream { source, patterns });
            }
            Rule::static_pattern_group => {
                let mut patterns = Vec::new();
                for tp in inner.into_inner() {
                    if tp.as_rule() == Rule::triple_pattern {
                        patterns.extend(parse_triple_patterns(tp)?);
                    }
                }
                return Ok(CqelsPatternGroup::Static { patterns });
            }
            Rule::named_graph_pattern_group => {
                let mut it = inner.into_inner();
                let graph_uri = it
                    .next()
                    .ok_or_else(|| ParseError::Syntax("expected graph IRI".into()))?
                    .as_str()
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string();
                let mut patterns = Vec::new();
                for tp in it {
                    if tp.as_rule() == Rule::triple_pattern {
                        patterns.extend(parse_triple_patterns(tp)?);
                    }
                }
                return Ok(CqelsPatternGroup::NamedGraph {
                    graph_uri,
                    patterns,
                });
            }
            Rule::filter_constraint => {
                let expr = inner
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_default();
                return Ok(CqelsPatternGroup::Filter { expression: expr });
            }
            Rule::bind_pattern => {
                let mut it = inner.into_inner();
                let expression = it
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_default();
                let variable = it
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_default();
                return Ok(CqelsPatternGroup::Bind {
                    expression,
                    variable,
                });
            }
            Rule::optional_pattern => {
                let mut groups = Vec::new();
                for pg in inner.into_inner() {
                    if pg.as_rule() == Rule::pattern_group {
                        groups.push(parse_pattern_group(pg)?);
                    }
                }
                return Ok(CqelsPatternGroup::Optional { groups });
            }
            Rule::union_pattern => {
                // union_pattern = { "{" ~ pattern_group+ ~ "}" ~ "UNION" ~ "{" ~ pattern_group+ ~ "}" }
                // The pest rule captures two groups of pattern_groups separated by UNION keyword.
                // We collect all pattern_groups and split them into left/right halves.
                let all_groups: Vec<pest::iterators::Pair<Rule>> = inner
                    .into_inner()
                    .filter(|p| p.as_rule() == Rule::pattern_group)
                    .collect();
                // The grammar produces pattern_group+ UNION pattern_group+
                // We need to find the split point. Since pest flattens, we parse
                // the raw text to find where the split is.
                // Actually, the pest grammar "{" ~ pattern_group+ ~ "}" ~ ^"union" ~ "{" ~ pattern_group+ ~ "}"
                // will produce all pattern_groups in sequence. We need to split them.
                // We'll use a different approach: track line positions.
                // Simpler: parse the span to find "UNION" and split groups accordingly.
                //
                // Actually the simplest approach: since we know the structure is
                // { left_groups } UNION { right_groups }, and pest will capture
                // left_groups and right_groups in order, we can look at the spans
                // to determine which groups are before/after UNION.
                //
                // But actually there's an even simpler way: reconstruct from the
                // original pest pair structure. Let me use a state machine approach.
                // Since all pattern_groups come in sequence and we can't easily
                // distinguish left from right, let's split them evenly or use a
                // different grammar approach.
                //
                // Better approach: Split at the midpoint where the "UNION" keyword
                // appears in the source. The left groups have spans before UNION,
                // right groups have spans after UNION.
                if all_groups.is_empty() {
                    return Err(ParseError::Syntax("empty UNION pattern".into()));
                }
                // Find where UNION appears in the source
                // Each group's span position tells us which side it's on
                // Since the grammar is { left+ } UNION { right+ }, we split
                // at the first group whose start position is after the UNION keyword
                let mut left = Vec::new();
                let mut right = Vec::new();
                let mut in_right = false;
                // Count braces to detect the transition
                // Actually, a simpler observation: the grammar defines
                // two brace-delimited blocks. Pest parses pattern_groups
                // from both blocks. We can detect the boundary by looking
                // at span gaps between consecutive groups.
                //
                // Even simpler: just split in half if there are only 2 groups,
                // or use the following heuristic: look at the source text
                // between groups for the word "union".
                for (i, pg) in all_groups.iter().enumerate() {
                    if i > 0 && !in_right {
                        let prev_end = all_groups[i - 1].as_span().end();
                        let cur_start = pg.as_span().start();
                        let between = &pg.as_span().get_input()[prev_end..cur_start];
                        if between.to_uppercase().contains("UNION") {
                            in_right = true;
                        }
                    }
                    if in_right {
                        right.push(parse_pattern_group(pg.clone())?);
                    } else {
                        left.push(parse_pattern_group(pg.clone())?);
                    }
                }
                if right.is_empty() && left.len() >= 2 {
                    // Fallback: split in half
                    let mid = left.len() / 2;
                    right = left.split_off(mid);
                }
                return Ok(CqelsPatternGroup::Union { left, right });
            }
            Rule::minus_pattern => {
                let mut patterns = Vec::new();
                for tp in inner.into_inner() {
                    if tp.as_rule() == Rule::triple_pattern {
                        patterns.extend(parse_triple_patterns(tp)?);
                    }
                }
                return Ok(CqelsPatternGroup::Minus { patterns });
            }
            Rule::triple_pattern => {
                let patterns = parse_triple_patterns(inner)?;
                return Ok(CqelsPatternGroup::Default { patterns });
            }
            _ => {}
        }
    }
    Err(ParseError::Syntax("invalid pattern group".into()))
}

fn parse_triple_patterns(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<TriplePattern>> {
    let mut results = Vec::new();
    let mut subject = String::new();
    let mut pred_obj_lists = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::subject => {
                subject = inner.as_str().to_string();
            }
            Rule::predicate_object_list => {
                pred_obj_lists.push(inner);
            }
            _ => {}
        }
    }

    for pol in pred_obj_lists {
        let mut predicate = String::new();
        let mut objects = Vec::new();

        for inner in pol.into_inner() {
            match inner.as_rule() {
                Rule::predicate => {
                    // Handle 'a' as rdf:type shorthand
                    let pred_text = inner.as_str().trim();
                    if pred_text == "a" {
                        predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string();
                    } else {
                        predicate = pred_text.to_string();
                    }
                }
                Rule::object_list => {
                    for obj in inner.into_inner() {
                        if obj.as_rule() == Rule::object {
                            objects.push(obj.as_str().to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        for obj in objects {
            results.push(TriplePattern {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: obj,
            });
        }
    }

    Ok(results)
}

fn parse_group_by(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            builder = builder.add_group_by(inner.as_str().to_string());
        }
    }
    Ok(builder)
}

fn parse_order_by(
    pair: pest::iterators::Pair<Rule>,
    mut builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::order_condition {
            let mut expr = String::new();
            let mut direction = SortDirection::Ascending;

            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::variable => {
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
    builder: CqelsQueryDefinitionBuilder,
) -> ParseResult<CqelsQueryDefinitionBuilder> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::integer {
            let limit: u64 = inner
                .as_str()
                .parse()
                .map_err(|e| ParseError::Syntax(format!("invalid limit: {e}")))?;
            return Ok(builder.limit(limit));
        }
    }
    Err(ParseError::Syntax("expected limit value".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM sensor [NOW]
            WHERE {
                ?x <http://example.org/value> ?y .
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.query_type, CqelsQueryType::Select);
        assert_eq!(result.streams.len(), 1);
        assert_eq!(result.streams[0].name, "sensor");
        assert_eq!(result.streams[0].window, WindowSpec::now());
        assert_eq!(result.select_elements.len(), 2);
        assert_eq!(result.pattern_groups.len(), 1);
    }

    #[test]
    fn test_parse_with_prefix() {
        let query = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?s ?o
            FROM STREAM data [RANGE 10s]
            WHERE {
                ?s ex:prop ?o .
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert!(result.prefixes.contains_key("ex"));
        assert_eq!(result.prefixes["ex"], "http://example.org/");
        assert_eq!(
            result.streams[0].window,
            WindowSpec::range(Duration::from_secs(10))
        );
    }

    #[test]
    fn test_parse_register_query() {
        let query = r#"
            REGISTER QUERY myQuery AS
            SELECT ?x
            FROM STREAM events [NOW]
            WHERE {
                ?x <http://example.org/type> ?y .
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.name, Some("myQuery".to_string()));
    }

    #[test]
    fn test_parse_window_specs() {
        // Test RANGE
        let query = r#"
            SELECT ?x
            FROM STREAM s1 [RANGE 30s]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::range(Duration::from_secs(30))
        );

        // Test TRIPLES
        let query = r#"
            SELECT ?x
            FROM STREAM s1 [TRIPLES 100]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.streams[0].window, WindowSpec::triples(100));

        // Test TRIPLES with SLIDE
        let query = r#"
            SELECT ?x
            FROM STREAM s1 [TRIPLES 100 SLIDE 50]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.streams[0].window, WindowSpec::triples_slide(100, 50));

        // Test SLIDE
        let query = r#"
            SELECT ?x
            FROM STREAM s1 [SLIDE 1m STEP 30s]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::slide(Duration::from_secs(60), Duration::from_secs(30))
        );
    }

    #[test]
    fn test_parse_multiple_sources() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM events [NOW]
            FROM <http://example.org/static>
            FROM NAMED <http://example.org/named>
            WHERE {
                ?x <http://ex.org/p> ?y .
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.streams.len(), 1);
        assert_eq!(result.static_graphs.len(), 1);
        assert_eq!(result.static_graphs[0].uri, "http://example.org/static");
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].uri, "http://example.org/named");
    }

    #[test]
    fn test_parse_group_by_order_by_limit() {
        let query = r#"
            SELECT ?type (COUNT(?x) AS ?cnt)
            FROM STREAM data [NOW]
            WHERE {
                ?x <http://ex.org/type> ?type .
            }
            GROUP BY ?type
            ORDER BY ?cnt DESC
            LIMIT 10
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert!(result.has_group_by());
        assert_eq!(result.group_by_variables, vec!["?type"]);
        assert!(result.has_order_by());
        assert_eq!(result.order_by_conditions[0].expression, "?cnt");
        assert_eq!(
            result.order_by_conditions[0].direction,
            SortDirection::Descending
        );
        assert!(result.has_limit());
        assert_eq!(result.limit, Some(10));
    }

    #[test]
    fn test_parse_distinct() {
        let query = r#"
            SELECT DISTINCT ?x
            FROM STREAM s [NOW]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert!(result.distinct);
    }

    #[test]
    fn test_parse_stream_pattern_group() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM sensor [NOW]
            WHERE {
                STREAM sensor {
                    ?x <http://ex.org/reading> ?y .
                }
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 1);
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Stream { source, patterns } => {
                assert_eq!(source, "sensor");
                assert_eq!(patterns.len(), 1);
            }
            _ => panic!("expected stream pattern group"),
        }
    }

    #[test]
    fn test_parse_multiple_triple_patterns() {
        let query = r#"
            SELECT ?name ?age
            FROM STREAM people [NOW]
            WHERE {
                ?p <http://ex.org/name> ?name .
                ?p <http://ex.org/age> ?age .
            }
        "#;

        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 2);
    }

    #[test]
    fn test_parse_error_invalid_query() {
        let result = CqelsQlParser::parse("NOT A VALID QUERY");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rdf_type_shorthand() {
        let query = r#"
            SELECT ?x
            FROM STREAM s [NOW]
            WHERE {
                ?x a <http://ex.org/Person> .
            }
        "#;

        // Implicit stream binding (Java PR #31): bare triples + single FROM
        // STREAM + no graphs → bound to that stream.
        let result = CqelsQlParser::parse(query).unwrap();
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Stream { source, patterns } => {
                assert_eq!(source, "s");
                assert_eq!(
                    patterns[0].predicate,
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                );
            }
            other => panic!("expected stream pattern group, got {other:?}"),
        }
    }

    // ─── Implicit stream binding (Java PR #31, Phase 1) ───────────────────────

    #[test]
    fn implicit_binding_single_stream_wraps_bare_triples() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM sensor [NOW]
            WHERE {
                ?x <http://ex.org/reading> ?y .
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 1);
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Stream { source, patterns } => {
                assert_eq!(source, "sensor");
                assert_eq!(patterns.len(), 1);
            }
            other => panic!("expected implicit stream binding, got {other:?}"),
        }
    }

    #[test]
    fn implicit_binding_multiple_bare_triples_all_bound() {
        let query = r#"
            SELECT ?p ?name ?age
            FROM STREAM people [NOW]
            WHERE {
                ?p <http://ex.org/name> ?name .
                ?p <http://ex.org/age> ?age .
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.pattern_groups.len(), 2);
        for group in &result.pattern_groups {
            match group {
                CqelsPatternGroup::Stream { source, .. } => assert_eq!(source, "people"),
                other => panic!("expected all groups bound to 'people', got {other:?}"),
            }
        }
    }

    #[test]
    fn implicit_binding_skipped_with_explicit_stream_block() {
        let query = r#"
            SELECT ?x ?y ?z
            FROM STREAM s [NOW]
            WHERE {
                STREAM s { ?x <http://ex.org/p> ?y . }
                ?x <http://ex.org/q> ?z .
            }
        "#;
        // Explicit STREAM block in WHERE disables implicit binding; bare
        // triples remain Default (Java parity: streamPatterns non-empty).
        let result = CqelsQlParser::parse(query).unwrap();
        let mut saw_stream = false;
        let mut saw_default = false;
        for group in &result.pattern_groups {
            match group {
                CqelsPatternGroup::Stream { .. } => saw_stream = true,
                CqelsPatternGroup::Default { .. } => saw_default = true,
                _ => {}
            }
        }
        assert!(saw_stream, "explicit STREAM block should be preserved");
        assert!(
            saw_default,
            "bare triples should NOT be auto-bound when an explicit STREAM block exists"
        );
    }

    #[test]
    fn implicit_binding_skipped_with_multiple_streams() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM a [NOW]
            FROM STREAM b [NOW]
            WHERE {
                ?x <http://ex.org/p> ?y .
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        // Two streams → ambiguous which to bind to → keep as Default.
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Default { .. } => {}
            other => panic!("expected Default group with multiple streams, got {other:?}"),
        }
    }

    #[test]
    fn implicit_binding_skipped_when_static_graph_declared() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM s [NOW]
            FROM <http://example.org/static>
            WHERE {
                ?x <http://ex.org/p> ?y .
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        // FROM-level static graph disables implicit binding (Java parity).
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Default { .. } => {}
            other => panic!("expected Default group when static graph declared, got {other:?}"),
        }
    }

    #[test]
    fn implicit_binding_skipped_when_named_graph_declared() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM s [NOW]
            FROM NAMED <http://example.org/g>
            WHERE {
                ?x <http://ex.org/p> ?y .
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        match &result.pattern_groups[0] {
            CqelsPatternGroup::Default { .. } => {}
            other => panic!("expected Default group when named graph declared, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_ask_query() {
        let query = r#"
            ASK FROM STREAM s [NOW]
            WHERE { ?x <http://example.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.query_type, CqelsQueryType::Ask);
        assert!(result.select_elements.is_empty());
        assert_eq!(result.streams.len(), 1);
        assert_eq!(result.streams[0].name, "s");
        assert_eq!(result.pattern_groups.len(), 1);
    }

    #[test]
    fn test_parse_ask_missing_where() {
        let result = CqelsQlParser::parse("ASK FROM STREAM s [NOW]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_describe_with_variable() {
        let query = r#"
            DESCRIBE ?x FROM STREAM s [NOW]
            WHERE { ?x <http://example.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.query_type, CqelsQueryType::Describe);
        assert_eq!(result.select_elements.len(), 1);
        match &result.select_elements[0] {
            SelectElement::Variable(v) => assert_eq!(v, "?x"),
            _ => panic!("expected variable"),
        }
    }

    #[test]
    fn test_parse_describe_star() {
        let query = r#"
            DESCRIBE * FROM STREAM s [NOW]
            WHERE { ?x <http://example.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.query_type, CqelsQueryType::Describe);
        assert!(result.select_elements.is_empty());
    }

    #[test]
    fn test_parse_describe_multiple_variables() {
        let query = r#"
            DESCRIBE ?x ?y FROM STREAM s [NOW]
            WHERE { ?x <http://example.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.query_type, CqelsQueryType::Describe);
        assert_eq!(result.select_elements.len(), 2);
    }

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_parse_error_missing_where() {
        let result = CqelsQlParser::parse("SELECT ?x FROM STREAM s [NOW]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_empty_input() {
        let result = CqelsQlParser::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_incomplete_window() {
        let result =
            CqelsQlParser::parse("SELECT ?x FROM STREAM s [] WHERE { ?x <http://ex.org/p> ?y . }");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_window_range_with_step() {
        let query = r#"
            SELECT ?x
            FROM STREAM s [RANGE 10s STEP 5s]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(
            result.streams[0].window,
            WindowSpec::slide(Duration::from_secs(10), Duration::from_secs(5))
        );
    }

    #[test]
    fn test_parse_window_time_units() {
        // Milliseconds
        let q = r#"SELECT ?x FROM STREAM s [RANGE 500ms] WHERE { ?x <http://p> ?y . }"#;
        let r = CqelsQlParser::parse(q).unwrap();
        assert_eq!(
            r.streams[0].window,
            WindowSpec::range(Duration::from_millis(500))
        );

        // Minutes
        let q = r#"SELECT ?x FROM STREAM s [RANGE 2m] WHERE { ?x <http://p> ?y . }"#;
        let r = CqelsQlParser::parse(q).unwrap();
        assert_eq!(
            r.streams[0].window,
            WindowSpec::range(Duration::from_secs(120))
        );

        // Hours
        let q = r#"SELECT ?x FROM STREAM s [RANGE 1h] WHERE { ?x <http://p> ?y . }"#;
        let r = CqelsQlParser::parse(q).unwrap();
        assert_eq!(
            r.streams[0].window,
            WindowSpec::range(Duration::from_secs(3600))
        );
    }

    #[test]
    fn test_parse_aggregate_count_star() {
        let query = r#"
            SELECT (COUNT(*) AS ?cnt)
            FROM STREAM s [NOW]
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.select_elements.len(), 1);
    }

    #[test]
    fn test_parse_multiple_aggregates() {
        let query = r#"
            SELECT (SUM(?v) AS ?total) (AVG(?v) AS ?mean) (MIN(?v) AS ?lo) (MAX(?v) AS ?hi)
            FROM STREAM s [NOW]
            WHERE { ?x <http://ex.org/value> ?v . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.select_elements.len(), 4);
    }

    #[test]
    fn test_parse_multiple_prefixes() {
        let query = r#"
            PREFIX ex: <http://example.org/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            SELECT ?name
            FROM STREAM s [NOW]
            WHERE { ?x foaf:name ?name . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.prefixes.len(), 3);
        assert!(result.prefixes.contains_key("ex"));
        assert!(result.prefixes.contains_key("foaf"));
        assert!(result.prefixes.contains_key("xsd"));
    }

    #[test]
    fn test_parse_order_by_ascending() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM s [NOW]
            WHERE { ?x <http://ex.org/p> ?y . }
            ORDER BY ?x ASC
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(
            result.order_by_conditions[0].direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn test_parse_multiple_order_by() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM s [NOW]
            WHERE { ?x <http://ex.org/p> ?y . }
            ORDER BY ?x ASC, ?y DESC
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.order_by_conditions.len(), 2);
        assert_eq!(
            result.order_by_conditions[0].direction,
            SortDirection::Ascending
        );
        assert_eq!(
            result.order_by_conditions[1].direction,
            SortDirection::Descending
        );
    }

    #[test]
    fn test_parse_filter_expression() {
        let query = r#"
            SELECT ?x ?y
            FROM STREAM s [NOW]
            WHERE {
                ?x <http://ex.org/value> ?y .
                FILTER(?y > 100)
            }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        // Should have 2 pattern groups: one triple + one filter
        assert!(result.pattern_groups.len() >= 2);
    }

    #[test]
    fn test_parse_static_with_depth() {
        let query = r#"
            SELECT ?x
            FROM STREAM s [NOW]
            FROM STATIC <http://example.org/kb> WITH DEPTH 3
            WHERE { ?x <http://ex.org/p> ?y . }
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.static_graphs.len(), 1);
    }

    #[test]
    fn test_parse_complex_multistream() {
        let query = r#"
            PREFIX ex: <http://example.org/>
            REGISTER QUERY alertQuery AS
            SELECT ?sensor ?temp
            FROM STREAM sensors [RANGE 10s]
            FROM STREAM events [SLIDE 5m STEP 1m]
            WHERE {
                STREAM sensors {
                    ?sensor ex:temperature ?temp .
                }
                STREAM events {
                    ?sensor ex:alert ?alert .
                }
            }
            ORDER BY ?temp DESC
            LIMIT 100
        "#;
        let result = CqelsQlParser::parse(query).unwrap();
        assert_eq!(result.name, Some("alertQuery".to_string()));
        assert_eq!(result.streams.len(), 2);
        assert_eq!(result.streams[0].name, "sensors");
        assert_eq!(result.streams[1].name, "events");
        assert!(result.has_order_by());
        assert_eq!(result.limit, Some(100));
    }
}
