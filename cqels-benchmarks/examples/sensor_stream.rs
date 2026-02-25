//! Example: Sensor stream processing with CQELS
//!
//! Demonstrates:
//! - Creating RDF stream elements from sensor data
//! - Parsing a CqelsQL continuous query
//! - Setting up RETE reasoning rules for inference
//! - Processing a stream of sensor events
//!
//! Run with: `cargo run --example sensor_stream`

use std::time::Duration;

use cqels_benchmarks::generate_sensor_readings;
use cqels_core::parser::CqelsQlParser;
use cqels_reasoning::{
    PatternTerm, ReasoningConfig, ReteNetwork, Rule, RuleSet, TriplePattern, TripleTemplate,
};
use cqels_model::term::{IriTerm, Term};

fn main() {
    println!("=== CQELS Sensor Stream Example ===\n");

    // 1. Parse a CqelsQL query
    println!("1. Parsing CqelsQL query...");
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor ?temp
        FROM STREAM sensors [RANGE 10s]
        WHERE {
            ?sensor ex:temperature ?temp .
        }
        ORDER BY ?temp DESC
        LIMIT 5
    "#;

    let query_def = CqelsQlParser::parse(query_str).expect("failed to parse query");
    println!("   Query name: {:?}", query_def.name);
    println!("   Query type: {:?}", query_def.query_type);
    println!("   Streams: {:?}", query_def.streams.iter().map(|s| &s.name).collect::<Vec<_>>());
    println!("   Select elements: {}", query_def.select_elements.len());
    println!("   Has ORDER BY: {}", query_def.has_order_by());
    println!("   Limit: {:?}", query_def.limit);
    println!();

    // 2. Set up RETE reasoning rules
    println!("2. Setting up RETE reasoning rules...");

    // Rule: If a sensor has a temperature reading, infer it has an alert
    let high_temp_rule = Rule::builder()
        .name("high_temperature_alert")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
            PatternTerm::Variable("temp".into()),
        ))
        .template(TripleTemplate::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasAlert"))),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/HighTemperature"))),
        ))
        .build();

    // Rule: If a sensor has humidity and temperature, infer a complete reading
    let reading_rule = Rule::builder()
        .name("complete_reading")
        .priority(2)
        .pattern(TriplePattern::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
            PatternTerm::Variable("temp".into()),
        ))
        .pattern(TriplePattern::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/humidity"))),
            PatternTerm::Variable("hum".into()),
        ))
        .template(TripleTemplate::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasCompleteReading"))),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/Complete"))),
        ))
        .build();

    let rule_set = RuleSet::builder()
        .add_rule(high_temp_rule)
        .add_rule(reading_rule)
        .build();

    println!("   Rules defined: {}", rule_set.rules().len());
    println!("   Total patterns: {}", rule_set.all_patterns().len());
    println!();

    // 3. Process sensor stream through RETE network
    println!("3. Processing sensor stream...");

    let config = ReasoningConfig::builder()
        .rule_set(rule_set)
        .default_window(Duration::from_secs(30))
        .enable_recursive_inference(false)
        .build();

    let mut network = ReteNetwork::compile(config);

    // Generate 500 sensor readings
    let readings = generate_sensor_readings(500);
    let mut total_inferred = 0;
    let mut alert_count = 0;
    let mut complete_count = 0;

    for reading in &readings {
        let inferred = network.process_element(reading);
        for inf in &inferred {
            total_inferred += 1;
            let pred = inf.statement.predicate.as_str();
            if pred == "http://example.org/hasAlert" {
                alert_count += 1;
            } else if pred == "http://example.org/hasCompleteReading" {
                complete_count += 1;
            }
        }
    }

    println!("   Input elements processed: {}", readings.len());
    println!("   Total inferred triples: {total_inferred}");
    println!("   High temperature alerts: {alert_count}");
    println!("   Complete readings: {complete_count}");
    println!("   Working memory size: {}", network.working_memory().size());
    println!();

    println!("=== Done ===");
}
