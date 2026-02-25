//! Property-based tests for CqelsQL and CypherQL parsers.
//!
//! Uses proptest to generate random/edge-case inputs and verify:
//! - Parsers never panic on arbitrary input
//! - Successfully parsed queries produce valid AST structures
//! - Round-trip invariants hold where applicable

use proptest::prelude::*;

use cqels_core::parser::{CqelsQlParser, CypherQlParser};

// ---------------------------------------------------------------------------
// Strategies for generating query fragments
// ---------------------------------------------------------------------------

fn variable_name() -> impl Strategy<Value = String> {
    "[a-z][a-zA-Z0-9_]{0,10}".prop_map(|s| format!("?{s}"))
}

fn iri_string() -> impl Strategy<Value = String> {
    "[a-z][a-zA-Z0-9/._-]{0,30}".prop_map(|s| format!("<http://example.org/{s}>"))
}

fn prefix_name() -> impl Strategy<Value = String> {
    "[a-z]{1,5}"
}

fn window_spec() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("[NOW]".to_string()),
        (1u64..3600).prop_map(|n| format!("[RANGE {n}s]")),
        (1u64..3600, 1u64..600).prop_map(|(r, s)| format!("[SLIDE {r}s STEP {s}s]")),
        (1u64..10000).prop_map(|n| format!("[TRIPLES {n}]")),
    ]
}

fn stream_name() -> impl Strategy<Value = String> {
    "[a-z][a-zA-Z0-9_]{1,10}".prop_filter("must not be a reserved word", |s| not_reserved(s))
}

fn not_reserved(s: &str) -> bool {
    let lower = s.to_lowercase();
    !matches!(
        lower.as_str(),
        "or" | "and" | "not" | "in" | "as" | "by" | "asc" | "desc"
            | "match" | "where" | "group" | "order" | "limit" | "with"
            | "from" | "null" | "true" | "false" | "case" | "when" | "then"
            | "else" | "end" | "now" | "range" | "slide" | "step"
            | "return" | "stream" | "static" | "graph" | "having" | "distinct"
            | "register" | "query" | "cache" | "depth" | "triples"
            | "starts" | "ends" | "contains"
    )
}

fn cypher_label() -> impl Strategy<Value = String> {
    "[A-Z][a-zA-Z]{2,10}".prop_filter("must not be a reserved word", |s| not_reserved(s))
}

fn cypher_property() -> impl Strategy<Value = String> {
    "[a-z][a-zA-Z0-9_]{1,10}".prop_filter("must not be a reserved word", |s| not_reserved(s))
}

/// Variable names that are not CypherQL reserved words.
fn cypher_var_name() -> impl Strategy<Value = String> {
    "[a-z]{1,5}".prop_filter("must not be a reserved word", |s| not_reserved(s))
}

// ---------------------------------------------------------------------------
// 1. Parsers never panic on arbitrary input
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn cqelsql_parser_never_panics(input in "\\PC{0,200}") {
        // Parser may return Ok or Err, but must never panic
        let _ = CqelsQlParser::parse(&input);
    }

    #[test]
    fn cypherql_parser_never_panics(input in "\\PC{0,200}") {
        let _ = CypherQlParser::parse(&input);
    }
}

// ---------------------------------------------------------------------------
// 2. Well-formed CqelsQL queries always parse successfully
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn cqelsql_simple_select_always_parses(
        var1 in variable_name(),
        var2 in variable_name(),
        stream in stream_name(),
        win in window_spec(),
        pred in iri_string(),
    ) {
        let query = format!(
            "SELECT {var1} {var2}\nFROM STREAM {stream} {win}\nWHERE {{\n    {var1} {pred} {var2} .\n}}"
        );
        let result = CqelsQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        prop_assert_eq!(def.streams.len(), 1);
        prop_assert_eq!(&def.streams[0].name, &stream);
        prop_assert!(def.select_elements.len() >= 2);
    }

    #[test]
    fn cqelsql_with_prefix_always_parses(
        prefix in prefix_name(),
        var in variable_name(),
        stream in stream_name(),
        win in window_spec(),
    ) {
        let query = format!(
            "PREFIX {prefix}: <http://example.org/{prefix}/>\n\
             SELECT {var}\n\
             FROM STREAM {stream} {win}\n\
             WHERE {{\n    {var} {prefix}:prop {var} .\n}}"
        );
        let result = CqelsQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        prop_assert!(def.prefixes.contains_key(&prefix));
    }

    #[test]
    fn cqelsql_with_order_limit_always_parses(
        var in variable_name(),
        stream in stream_name(),
        win in window_spec(),
        pred in iri_string(),
        limit in 1u64..1000,
        desc in any::<bool>(),
    ) {
        let dir = if desc { "DESC" } else { "ASC" };
        let query = format!(
            "SELECT {var}\nFROM STREAM {stream} {win}\nWHERE {{\n    {var} {pred} {var} .\n}}\nORDER BY {var} {dir}\nLIMIT {limit}"
        );
        let result = CqelsQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        prop_assert!(def.has_order_by());
        prop_assert_eq!(def.limit, Some(limit));
    }
}

// ---------------------------------------------------------------------------
// 3. Well-formed CypherQL queries always parse successfully
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn cypherql_simple_match_always_parses(
        node_var in cypher_var_name(),
        label in cypher_label(),
        stream in stream_name(),
        win in window_spec(),
        prop in cypher_property(),
    ) {
        let query = format!(
            "FROM STREAM {stream} {win}\nMATCH ({node_var}:{label})\nRETURN {node_var}.{prop}"
        );
        let result = CypherQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        prop_assert_eq!(def.streams.len(), 1);
        prop_assert_eq!(&def.streams[0].name, &stream);
    }

    #[test]
    fn cypherql_relationship_always_parses(
        a_var in cypher_var_name(),
        b_var in cypher_var_name(),
        a_label in cypher_label(),
        b_label in cypher_label(),
        rel_type in "[A-Z][A-Z_]{0,10}".prop_filter("must not be a reserved word", |s| not_reserved(s)),
        stream in stream_name(),
        win in window_spec(),
    ) {
        let query = format!(
            "FROM STREAM {stream} {win}\nMATCH ({a_var}:{a_label})-[:{rel_type}]->({b_var}:{b_label})\nRETURN {a_var}, {b_var}"
        );
        let result = CypherQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        let pattern = &def.pattern_groups[0].patterns[0];
        prop_assert_eq!(pattern.nodes.len(), 2);
        prop_assert_eq!(pattern.relationships.len(), 1);
    }

    #[test]
    fn cypherql_with_where_and_order_always_parses(
        var in cypher_var_name(),
        label in cypher_label(),
        prop in cypher_property(),
        stream in stream_name(),
        win in window_spec(),
        limit in 1u64..1000,
    ) {
        let query = format!(
            "FROM STREAM {stream} {win}\n\
             MATCH ({var}:{label})\n\
             WHERE {var}.{prop} > 0\n\
             RETURN {var}.{prop}\n\
             ORDER BY {var}.{prop} DESC\n\
             LIMIT {limit}"
        );
        let result = CypherQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed to parse: {}\nError: {:?}", query, result.err());

        let def = result.unwrap();
        prop_assert!(def.where_expression.is_some());
        prop_assert_eq!(def.limit, Some(limit));
    }
}

// ---------------------------------------------------------------------------
// 4. Parser invariants: parsed structure consistency
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn cqelsql_stream_count_matches_from_clauses(
        stream1 in stream_name(),
        stream2 in stream_name(),
        win1 in window_spec(),
        win2 in window_spec(),
        var in variable_name(),
        pred in iri_string(),
    ) {
        let query = format!(
            "SELECT {var}\n\
             FROM STREAM {stream1} {win1}\n\
             FROM STREAM {stream2} {win2}\n\
             WHERE {{\n\
                 STREAM {stream1} {{\n    {var} {pred} {var} .\n}}\n\
                 STREAM {stream2} {{\n    {var} {pred} {var} .\n}}\n\
             }}"
        );
        let result = CqelsQlParser::parse(&query);
        prop_assert!(result.is_ok(), "Failed: {}\nError: {:?}", query, result.err());
        prop_assert_eq!(result.unwrap().streams.len(), 2);
    }
}
