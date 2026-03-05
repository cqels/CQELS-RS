//! Example: CypherQL query parsing
//!
//! Demonstrates parsing Cypher-based continuous queries with streaming extensions.
//!
//! Run with: `cargo run --example cypher_query`

use cqels_core::parser::ast::PatternSource;
use cqels_core::parser::CypherQlParser;

fn main() {
    println!("=== CypherQL Query Parsing Example ===\n");

    // Example 1: Simple pattern match with ORDER BY
    println!("--- Example 1: Simple Pattern Match ---");
    let query1 = r#"
        FROM STREAM social [NOW]
        MATCH (p:Person)
        WHERE p.age > 18
        RETURN p.name, p.age
        ORDER BY p.age DESC
        LIMIT 10
    "#;

    match CypherQlParser::parse(query1) {
        Ok(def) => {
            println!(
                "  Streams: {:?}",
                def.streams.iter().map(|s| &s.name).collect::<Vec<_>>()
            );
            println!("  Pattern groups: {}", def.pattern_groups.len());
            println!("  WHERE: {:?}", def.where_expression);
            println!("  RETURN items: {}", def.return_expressions.len());
            println!(
                "  ORDER BY: {:?}",
                def.order_by_conditions
                    .iter()
                    .map(|o| &o.expression)
                    .collect::<Vec<_>>()
            );
            println!("  LIMIT: {:?}", def.limit);
        }
        Err(e) => println!("  Parse error: {e}"),
    }
    println!();

    // Example 2: Relationship pattern with aggregation
    println!("--- Example 2: Relationship Pattern ---");
    let query2 = r#"
        FROM STREAM social [RANGE 5m]
        MATCH (a:Person)-[:FOLLOWS]->(b:Person)
        GROUP BY a.city
        RETURN a.city, count(b) AS followers
        ORDER BY followers DESC
        LIMIT 5
    "#;

    match CypherQlParser::parse(query2) {
        Ok(def) => {
            println!("  Window: {}", def.streams[0].window);
            println!("  Groups: {}", def.group_by_expressions.len());
            for group in &def.pattern_groups {
                println!("  Pattern source: {:?}", group.source);
                for pattern in &group.patterns {
                    println!("    Nodes: {}", pattern.nodes.len());
                    println!("    Relationships: {}", pattern.relationships.len());
                    for rel in &pattern.relationships {
                        println!(
                            "      Direction: {:?}, Types: {:?}",
                            rel.direction, rel.types
                        );
                    }
                }
            }
        }
        Err(e) => println!("  Parse error: {e}"),
    }
    println!();

    // Example 3: Registered query with multiple sources
    println!("--- Example 3: Multi-Source Registered Query ---");
    let query3 = r#"
        REGISTER QUERY alertMonitor AS
        FROM STREAM events [SLIDE 1m STEP 10s]
        FROM STATIC 'http://knowledge.org/base'
        MATCH
            STREAM events { (e:Event)-[:TRIGGERED_BY]->(s:Sensor) },
            STATIC { (s:Sensor)-[:LOCATED_IN]->(l:Location) }
        RETURN e.type, s.id, l.name
    "#;

    match CypherQlParser::parse(query3) {
        Ok(def) => {
            println!("  Query name: {:?}", def.name);
            println!("  Streams: {}", def.streams.len());
            println!("  Static graphs: {}", def.static_graphs.len());
            for group in &def.pattern_groups {
                let source_label = match group.source {
                    PatternSource::Stream => "STREAM",
                    PatternSource::Static => "STATIC",
                    PatternSource::Graph => "GRAPH",
                    PatternSource::Default => "DEFAULT",
                    _ => "UNKNOWN",
                };
                println!(
                    "  Pattern group [{}]: {} patterns",
                    source_label,
                    group.patterns.len()
                );
            }
        }
        Err(e) => println!("  Parse error: {e}"),
    }
    println!();

    // Example 4: Node with properties
    println!("--- Example 4: Node Properties ---");
    let query4 = r#"
        FROM STREAM events [TRIPLES 100]
        MATCH (n:Person {name: "Alice"})
        RETURN n
    "#;

    match CypherQlParser::parse(query4) {
        Ok(def) => {
            println!("  Window: {}", def.streams[0].window);
            for group in &def.pattern_groups {
                for pattern in &group.patterns {
                    for node in &pattern.nodes {
                        println!("  Node: {node}");
                        println!("    Variable: {:?}", node.variable);
                        println!("    Labels: {:?}", node.labels);
                        println!("    Properties: {:?}", node.properties);
                    }
                }
            }
        }
        Err(e) => println!("  Parse error: {e}"),
    }

    println!("\n=== Done ===");
}
