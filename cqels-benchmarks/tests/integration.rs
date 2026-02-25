//! Integration tests composing multiple CQELS subsystems end-to-end.
//!
//! These tests verify that parsing, windowing, filtering, aggregation,
//! joining, ranking, and reasoning work together correctly.

use std::time::Duration;

use futures::StreamExt;

// Model
use cqels_model::term::{IriTerm, LiteralTerm, Term};
use cqels_model::{BindingSet, Statement, Value};

// Core – parsing
use cqels_core::parser::{CqelsQlParser, CypherQlParser};

// Core – streams & windows
use cqels_core::stream::{RdfStreamElement, StreamElement, Timestamped};
use cqels_core::window::{
    SessionWindow, SlidingWindow, TumblingCountWindow, TumblingWindow, Window,
};

// Core – operators
use cqels_core::operator::aggregate::{
    AggregateFunction, AvgAggregate, CountAggregate, GroupKey, MaxAggregate, MinAggregate,
    SumAggregate, WindowedAggregateOperator,
};
use cqels_core::operator::filter::FilterOperator;
use cqels_core::operator::bind::BindOperator;
use cqels_core::operator::join::GraphPatternJoinState;
use cqels_core::operator::ranking::{SortDirection, TopKOperator};

// Engine – CEP
use cqels_engine::{NfaPatternProcessor, Pattern};

// Reasoning
use cqels_reasoning::{
    PatternTerm, ReasoningConfig, ReteNetwork, Rule, RuleSet, TriplePattern, TripleTemplate,
};

// Generators
use cqels_benchmarks::{generate_sensor_readings, generate_social_events};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn iri(s: &str) -> Term {
    Term::Iri(IriTerm::new(s))
}

fn iri_pred(s: &str) -> IriTerm {
    IriTerm::new(s)
}

fn lit(s: &str) -> Term {
    Term::Literal(LiteralTerm::new(s))
}

fn stmt(subj: &str, pred: &str, obj: &str) -> Statement {
    Statement::new(iri(subj), iri_pred(pred), iri(obj))
}

fn make_rdf_element(subj: &str, pred: &str, obj: &str, ts: i64) -> RdfStreamElement {
    RdfStreamElement::new(stmt(subj, pred, obj), ts)
}

fn make_rdf_literal_element(subj: &str, pred: &str, obj_val: &str, ts: i64) -> RdfStreamElement {
    RdfStreamElement::new(
        Statement::new(iri(subj), iri_pred(pred), lit(obj_val)),
        ts,
    )
}

fn term_as_str(t: &Term) -> &str {
    match t {
        Term::Iri(iri) => iri.as_str(),
        Term::Literal(lit) => lit.value(),
        Term::BlankNode(bn) => bn.id(),
    }
}

// ---------------------------------------------------------------------------
// 1. Parse → Window → Aggregate pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_parse_then_window_then_aggregate() {
    // Step 1: Parse a CqelsQL query
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

    let parsed = CqelsQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams.len(), 1);
    assert_eq!(parsed.streams[0].name, "sensors");
    assert!(parsed.has_order_by());
    assert_eq!(parsed.limit, Some(5));

    // Step 2: Generate sensor data and apply the window from the parsed query
    let readings = generate_sensor_readings(100);
    let stream = Box::pin(futures::stream::iter(readings));
    let window = TumblingWindow::new(Duration::from_secs(10));
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert!(!batches.is_empty(), "should produce at least one window batch");

    // Step 3: Apply aggregation to each window batch
    let count_op = WindowedAggregateOperator::new(
        CountAggregate,
        |_: &RdfStreamElement| None, // no grouping
        10_000,
    );

    let mut total_elements = 0i64;
    for batch in &batches {
        let results = count_op.process_batch(&batch.elements);
        for res in &results {
            assert!(res.value > 0, "each window should have at least one element");
            total_elements += res.value;
        }
    }
    assert_eq!(total_elements, 100, "all 100 readings should be accounted for");
}

// ---------------------------------------------------------------------------
// 2. Parse → Filter → Bind pipeline using BindingSets
// ---------------------------------------------------------------------------

#[test]
fn test_parse_then_filter_then_bind() {
    // Step 1: Parse a CypherQL query with WHERE and RETURN
    let query_str = r#"
        FROM STREAM social [RANGE 5m]
        MATCH (p:Person)-[:FOLLOWS]->(q:Person)
        WHERE p.age > 18
        RETURN p.name, p.age, p.age * 2 AS double_age
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams.len(), 1);
    assert!(parsed.where_expression.is_some());
    assert_eq!(parsed.return_expressions.len(), 3);

    // Step 2: Simulate binding sets from query execution
    let bindings: Vec<BindingSet> = vec![
        {
            let mut bs = BindingSet::new(1000);
            bs.insert("name".to_string(), Value::String("Alice".to_string()));
            bs.insert("age".to_string(), Value::Integer(25));
            bs
        },
        {
            let mut bs = BindingSet::new(2000);
            bs.insert("name".to_string(), Value::String("Bob".to_string()));
            bs.insert("age".to_string(), Value::Integer(16));
            bs
        },
        {
            let mut bs = BindingSet::new(3000);
            bs.insert("name".to_string(), Value::String("Carol".to_string()));
            bs.insert("age".to_string(), Value::Integer(30));
            bs
        },
    ];

    // Step 3: Apply filter (age > 18)
    let filter = FilterOperator::new(|bs: &BindingSet| {
        bs.get("age")
            .and_then(|v| v.as_integer())
            .is_some_and(|age| age > 18)
    });

    let filtered: Vec<_> = bindings
        .into_iter()
        .filter(|bs| filter.evaluate(bs))
        .collect();

    assert_eq!(filtered.len(), 2);

    // Step 4: Apply bind (double_age = age * 2)
    let bind = BindOperator::new("double_age", |bs: &BindingSet| {
        bs.get("age")
            .and_then(|v| v.as_integer())
            .map(|age| Value::Integer(age * 2))
    });

    let mut enriched = filtered;
    for bs in &mut enriched {
        bind.evaluate(bs);
    }

    // Verify: Alice (age=25) -> double_age=50, Carol (age=30) -> double_age=60
    assert_eq!(
        enriched[0].get("double_age").and_then(|v| v.as_integer()),
        Some(50)
    );
    assert_eq!(
        enriched[1].get("double_age").and_then(|v| v.as_integer()),
        Some(60)
    );
}

// ---------------------------------------------------------------------------
// 3. Window → Filter → TopK ranking pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_window_then_filter_then_rank() {
    // Generate social events and window them
    let events = generate_social_events(200);
    let stream = Box::pin(futures::stream::iter(events));
    let window = TumblingWindow::new(Duration::from_secs(10));
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert!(!batches.is_empty());

    // For each window, apply a top-5 ranking by timestamp (most recent first)
    for batch in &batches {
        let mut top_k = TopKOperator::new(
            5,
            |e: &RdfStreamElement| e.timestamp() as f64,
            SortDirection::Descending,
        );

        for elem in &batch.elements {
            top_k.add(elem.clone());
        }

        let top = top_k.get_top_k();
        assert!(top.len() <= 5);

        // Verify descending order
        for pair in top.windows(2) {
            assert!(pair[0].timestamp() >= pair[1].timestamp());
        }
    }
}

// ---------------------------------------------------------------------------
// 4. Graph pattern join with parsed query structure
// ---------------------------------------------------------------------------

#[test]
fn test_parse_cypher_then_graph_pattern_join() {
    // Step 1: Parse a Cypher query with relationship patterns
    let query_str = r#"
        FROM STREAM social [NOW]
        MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIKES]->(c:Post)
        RETURN a, b, c
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.pattern_groups.len(), 1);
    let pattern = &parsed.pattern_groups[0].patterns[0];
    assert_eq!(pattern.nodes.len(), 3);
    assert_eq!(pattern.relationships.len(), 2);

    // Step 2: Build a graph from RDF statements
    let mut graph = GraphPatternJoinState::new();

    graph.add_statement(&stmt("http://ex.org/Alice", "http://ex.org/follows", "http://ex.org/Bob"), 100);
    graph.add_statement(&stmt("http://ex.org/Bob", "http://ex.org/follows", "http://ex.org/Carol"), 200);
    graph.add_statement(&stmt("http://ex.org/Bob", "http://ex.org/likes", "http://ex.org/Post1"), 300);

    // Note: GraphPatternJoinState stores keys using Display format, so IRIs are wrapped in <...>
    // Step 3: Verify graph structure matches the parsed query's pattern
    assert!(graph.has_edge("<http://ex.org/Alice>", "<http://ex.org/follows>"));
    let bob_targets = graph.get_objects("<http://ex.org/Alice>", "<http://ex.org/follows>");
    assert!(bob_targets.contains(&"<http://ex.org/Bob>"));

    assert!(graph.has_edge("<http://ex.org/Bob>", "<http://ex.org/likes>"));
    let liked = graph.get_objects("<http://ex.org/Bob>", "<http://ex.org/likes>");
    assert!(liked.contains(&"<http://ex.org/Post1>"));

    let followers = graph.get_subjects("<http://ex.org/Bob>", "<http://ex.org/follows>");
    assert!(followers.contains(&"<http://ex.org/Alice>"));
}

// ---------------------------------------------------------------------------
// 5. Parse → Reason → Infer new facts pipeline
// ---------------------------------------------------------------------------

#[test]
fn test_parse_then_reason() {
    // Step 1: Parse a query that references a stream
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?x ?alert
        FROM STREAM sensors [RANGE 30s]
        WHERE {
            ?x ex:hasAlert ?alert .
        }
    "#;

    let parsed = CqelsQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams[0].name, "sensors");

    // Step 2: Set up reasoning rules that generate the alerts
    let alert_rule = Rule::builder()
        .id("temp-alert")
        .name("high_temperature_alert")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::var("sensor"),
            PatternTerm::constant(iri("http://example.org/temperature")),
            PatternTerm::var("temp"),
        ))
        .template(TripleTemplate::new(
            PatternTerm::var("sensor"),
            PatternTerm::constant(iri("http://example.org/hasAlert")),
            PatternTerm::constant(iri("http://example.org/HighTemperature")),
        ))
        .build();

    let rule_set = RuleSet::new(vec![alert_rule]);
    let config = ReasoningConfig::default_config(rule_set);
    let mut network = ReteNetwork::compile(config);

    // Step 3: Process sensor readings through reasoning
    let readings = generate_sensor_readings(50);
    let mut alert_count = 0;

    for reading in &readings {
        let inferred = network.process_element(reading);
        for inf in &inferred {
            if inf.statement.predicate.as_str() == "http://example.org/hasAlert" {
                alert_count += 1;
            }
        }
    }

    let temp_readings = readings
        .iter()
        .filter(|r| r.statement.predicate.as_str() == "http://example.org/temperature")
        .count();
    assert!(alert_count > 0, "reasoning should produce alerts");
    assert!(
        alert_count <= temp_readings,
        "alerts should not exceed temperature readings (dedup)"
    );
}

// ---------------------------------------------------------------------------
// 6. CEP + Reasoning combined: detect pattern, then reason
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TempEvent {
    sensor: String,
    value: f64,
    timestamp: i64,
}

impl Timestamped for TempEvent {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[tokio::test]
async fn test_cep_then_reason() {
    // Step 1: Detect a rising temperature pattern using CEP
    let pattern = Pattern::begin("warm")
        .where_cond(|e: &TempEvent| e.value > 30.0)
        .followed_by("hot")
        .where_cond(|e: &TempEvent| e.value > 50.0)
        .within(Duration::from_secs(10));

    let events = vec![
        TempEvent { sensor: "s1".into(), value: 25.0, timestamp: 1000 },
        TempEvent { sensor: "s1".into(), value: 35.0, timestamp: 2000 },
        TempEvent { sensor: "s1".into(), value: 40.0, timestamp: 3000 },
        TempEvent { sensor: "s1".into(), value: 55.0, timestamp: 4000 },
        TempEvent { sensor: "s2".into(), value: 60.0, timestamp: 5000 },
    ];

    let processor = NfaPatternProcessor::new(pattern);
    let stream = Box::pin(futures::stream::iter(events));
    let matches: Vec<_> = processor.process(stream).collect().await;

    assert!(!matches.is_empty(), "should detect rising temperature pattern");

    // Step 2: For each match, create an RDF event and run through reasoning
    let alert_rule = Rule::builder()
        .id("rising-temp")
        .pattern(TriplePattern::new(
            PatternTerm::var("sensor"),
            PatternTerm::constant(iri("http://ex.org/risingTemp")),
            PatternTerm::var("level"),
        ))
        .template(TripleTemplate::new(
            PatternTerm::var("sensor"),
            PatternTerm::constant(iri("http://ex.org/status")),
            PatternTerm::constant(iri("http://ex.org/CriticalAlert")),
        ))
        .build();

    let rule_set = RuleSet::new(vec![alert_rule]);
    let config = ReasoningConfig::default_config(rule_set);
    let mut network = ReteNetwork::compile(config);

    let mut critical_alerts = 0;
    for m in &matches {
        let first_event = &m.events()[0];
        let rdf_elem = RdfStreamElement::new(
            Statement::new(
                iri(&format!("http://ex.org/{}", first_event.sensor)),
                iri_pred("http://ex.org/risingTemp"),
                iri("http://ex.org/High"),
            ),
            m.start_timestamp(),
        );

        let inferred = network.process_element(&rdf_elem);
        for inf in &inferred {
            if inf.statement.predicate.as_str() == "http://ex.org/status" {
                critical_alerts += 1;
            }
        }
    }

    assert!(critical_alerts > 0, "reasoning should produce critical alerts from CEP matches");
}

// ---------------------------------------------------------------------------
// 7. Window → Grouped aggregation → TopK pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_window_grouped_aggregate_then_rank() {
    let readings = generate_sensor_readings(200);

    // Step 1: Window into 10s batches
    let stream = Box::pin(futures::stream::iter(readings));
    let window = TumblingWindow::new(Duration::from_secs(10));
    let batches: Vec<_> = window.apply(stream).collect().await;
    assert!(!batches.is_empty());

    // Step 2: Aggregate count per predicate per window
    let count_op = WindowedAggregateOperator::new(
        CountAggregate,
        |e: &RdfStreamElement| {
            Some(GroupKey::single(e.statement.predicate.as_str().to_string()))
        },
        10_000,
    );

    for batch in &batches {
        let results = count_op.process_batch(&batch.elements);
        assert!(!results.is_empty(), "each window should have aggregate results");

        // Step 3: Rank aggregation results by count descending
        let mut ranked: Vec<_> = results.iter().collect();
        ranked.sort_by(|a, b| b.value.cmp(&a.value));

        if ranked.len() > 1 {
            assert!(ranked[0].value >= ranked[1].value);
        }
    }
}

// ---------------------------------------------------------------------------
// 8. Session window + multiple aggregates pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_session_window_with_aggregates() {
    // Create events with clear session gaps
    let events: Vec<RdfStreamElement> = (0..30)
        .map(|i| {
            let session = i / 10;
            let ts = session * 15000 + (i % 10) * 500;
            make_rdf_literal_element(
                &format!("http://ex.org/sensor/{}", i % 3),
                "http://ex.org/value",
                &format!("{}", 20.0 + (i as f64) * 0.5),
                ts,
            )
        })
        .collect();

    // Step 1: Apply session window
    let stream = Box::pin(futures::stream::iter(events));
    let session = SessionWindow::new(Duration::from_secs(6));
    let batches: Vec<_> = session.apply(stream).collect().await;

    assert!(
        batches.len() >= 2,
        "should detect multiple sessions, got {}",
        batches.len()
    );

    // Step 2: For each session, compute multiple aggregates
    let sum_agg = SumAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let avg_agg = AvgAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let min_agg = MinAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let max_agg = MaxAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);

    for batch in &batches {
        let mut sum_acc = sum_agg.create_accumulator();
        let mut avg_acc = avg_agg.create_accumulator();
        let mut min_acc = min_agg.create_accumulator();
        let mut max_acc = max_agg.create_accumulator();

        for elem in &batch.elements {
            sum_acc = sum_agg.add(elem, sum_acc);
            avg_acc = avg_agg.add(elem, avg_acc);
            min_acc = min_agg.add(elem, min_acc);
            max_acc = max_agg.add(elem, max_acc);
        }

        let avg = avg_agg.get_result(&avg_acc);
        let min_ts = min_agg.get_result(&min_acc);
        let max_ts = max_agg.get_result(&max_acc);

        assert!(batch.size() > 0, "session should have elements");
        assert!(max_ts >= min_ts, "max should be >= min");
        assert!(
            avg >= batch.window_start as f64,
            "average timestamp should be within window bounds"
        );
    }
}

// ---------------------------------------------------------------------------
// 9. Multi-rule reasoning chain
// ---------------------------------------------------------------------------

#[test]
fn test_reasoning_chain_with_generated_data() {
    // Rule 1: sensor hasTemperature temp → sensor hasReading Complete
    let rule1 = Rule::builder()
        .id("reading-complete")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://example.org/temperature")),
            PatternTerm::var("t"),
        ))
        .template(TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://example.org/hasReading")),
            PatternTerm::constant(iri("http://example.org/Complete")),
        ))
        .build();

    // Rule 2: sensor hasReading Complete → sensor status Active
    let rule2 = Rule::builder()
        .id("sensor-active")
        .priority(2)
        .pattern(TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://example.org/hasReading")),
            PatternTerm::constant(iri("http://example.org/Complete")),
        ))
        .template(TripleTemplate::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://example.org/status")),
            PatternTerm::constant(iri("http://example.org/Active")),
        ))
        .build();

    let rule_set = RuleSet::new(vec![rule1, rule2]);
    let config = ReasoningConfig::builder()
        .rule_set(rule_set)
        .default_window(Duration::from_secs(60))
        .enable_recursive_inference(true)
        .build();
    let mut network = ReteNetwork::compile(config);

    let readings = generate_sensor_readings(20);
    let mut reading_count = 0;
    let mut active_count = 0;

    for r in &readings {
        let inferred = network.process_element(r);
        for inf in &inferred {
            let pred = inf.statement.predicate.as_str();
            if pred == "http://example.org/hasReading" {
                reading_count += 1;
            } else if pred == "http://example.org/status" {
                active_count += 1;
            }
        }
    }

    assert!(reading_count > 0, "rule1 should fire");
    assert!(active_count > 0, "rule2 should fire (chained from rule1)");
}

// ---------------------------------------------------------------------------
// 10. Full parse → window → aggregate → rank pipeline (end-to-end)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_cqelsql_pipeline() {
    // Step 1: Parse query
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor (COUNT(?temp) AS ?reading_count) (AVG(?temp) AS ?avg_temp)
        FROM STREAM sensors [RANGE 10s]
        WHERE {
            ?sensor ex:temperature ?temp .
        }
        GROUP BY ?sensor
        ORDER BY ?avg_temp DESC
        LIMIT 3
    "#;

    let parsed = CqelsQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams.len(), 1);
    assert!(!parsed.group_by_variables.is_empty());
    assert!(!parsed.aggregates.is_empty());
    assert!(parsed.has_order_by());
    assert_eq!(parsed.limit, Some(3));

    // Step 2: Generate and window data
    let readings = generate_sensor_readings(200);
    let stream = Box::pin(futures::stream::iter(readings));
    let window = TumblingWindow::new(Duration::from_secs(10));
    let batches: Vec<_> = window.apply(stream).collect().await;

    // Step 3: Apply grouped aggregation per subject
    let count_op = WindowedAggregateOperator::new(
        CountAggregate,
        |e: &RdfStreamElement| {
            Some(GroupKey::single(term_as_str(&e.statement.subject).to_string()))
        },
        10_000,
    );

    // Step 4: Collect, rank, and limit results
    let mut all_results = Vec::new();
    for batch in &batches {
        let results = count_op.process_batch(&batch.elements);
        all_results.extend(results);
    }

    // Sort descending by count, take top 3 (matching LIMIT 3)
    all_results.sort_by(|a, b| b.value.cmp(&a.value));
    let top_3: Vec<_> = all_results.into_iter().take(3).collect();

    assert!(
        top_3.len() <= 3,
        "limit should restrict to at most 3 results"
    );

    for pair in top_3.windows(2) {
        assert!(pair[0].value >= pair[1].value);
    }
}

// ---------------------------------------------------------------------------
// 11. CypherQL parse + graph pattern + variable-length path
// ---------------------------------------------------------------------------

#[test]
fn test_cypher_parse_then_variable_length_path() {
    use cqels_core::operator::join::VariableLengthPathOperator;

    let query_str = r#"
        FROM STREAM social [NOW]
        MATCH (a:Person)-[:FOLLOWS*1..3]->(b:Person)
        RETURN a, b
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    let pattern = &parsed.pattern_groups[0].patterns[0];
    assert_eq!(pattern.relationships.len(), 1);

    // Build graph: Alice -> Bob -> Carol -> Dave -> Eve
    let mut graph = GraphPatternJoinState::new();
    let people = ["Alice", "Bob", "Carol", "Dave", "Eve"];
    for i in 0..people.len() - 1 {
        graph.add_statement(
            &stmt(
                &format!("http://ex.org/{}", people[i]),
                "http://ex.org/follows",
                &format!("http://ex.org/{}", people[i + 1]),
            ),
            (i as i64) * 1000,
        );
    }

    // Find paths from Alice with 1..3 hops
    // Note: GraphPatternJoinState stores keys in Display format (<...> for IRIs)
    let path_op = VariableLengthPathOperator::new(1, 3)
        .with_relationship_type("<http://ex.org/follows>");
    let paths = path_op.find_paths("<http://ex.org/Alice>", &graph);

    // Should find: Alice→Bob (1), Alice→Bob→Carol (2), Alice→Bob→Carol→Dave (3)
    assert!(paths.len() >= 3, "should find paths of length 1, 2, and 3; got {}", paths.len());

    let one_hop: Vec<_> = paths.iter().filter(|p| p.len() == 2).collect();
    assert!(!one_hop.is_empty(), "should have 1-hop path");
    assert_eq!(one_hop[0][0], "<http://ex.org/Alice>");
    assert_eq!(one_hop[0][1], "<http://ex.org/Bob>");

    let three_hop: Vec<_> = paths.iter().filter(|p| p.len() == 4).collect();
    assert!(!three_hop.is_empty(), "should have 3-hop path");
    assert_eq!(*three_hop[0].last().unwrap(), "<http://ex.org/Dave>");
}

// ---------------------------------------------------------------------------
// 12. Sliding window overlap verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sliding_window_overlap_consistency() {
    let readings = generate_sensor_readings(100);
    let total_count = readings.len();

    let stream = Box::pin(futures::stream::iter(readings));
    let sliding = SlidingWindow::new(Duration::from_secs(15), Duration::from_secs(5));
    let batches: Vec<_> = sliding.apply(stream).collect().await;

    assert!(batches.len() > 1, "sliding window should produce multiple batches");

    let total_in_windows: usize = batches.iter().map(|b| b.size()).sum();
    assert!(
        total_in_windows >= total_count,
        "overlapping windows should cover at least all elements: {} >= {}",
        total_in_windows,
        total_count
    );

    for batch in &batches {
        assert_eq!(
            batch.window_end - batch.window_start,
            15000,
            "each window should span exactly 15s"
        );
    }
}

// ---------------------------------------------------------------------------
// 13. Engine lifecycle: register stream → receive events
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_engine_register_and_process() {
    use cqels_engine::{ReactiveStreamEngine, StreamEngine};

    let engine = ReactiveStreamEngine::new();

    let events: Vec<StreamElement> = generate_sensor_readings(10)
        .into_iter()
        .map(StreamElement::Rdf)
        .collect();

    let stream = Box::pin(futures::stream::iter(events));
    engine
        .register_stream("sensors", stream)
        .await
        .expect("register stream failed");

    assert!(!engine.is_running());
    engine.start().await.expect("start failed");
    assert!(engine.is_running());
    engine.stop().await.expect("stop failed");
    assert!(!engine.is_running());
}

// ---------------------------------------------------------------------------
// 14. Count window + aggregate (non-time-based windowing)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_count_window_then_aggregate() {
    let readings = generate_sensor_readings(50);

    let stream = Box::pin(futures::stream::iter(readings));
    let window = TumblingCountWindow::new(10);
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert_eq!(batches.len(), 5, "50 elements / 10 per batch = 5 batches");

    for batch in &batches {
        assert_eq!(batch.size(), 10, "each count-window batch should have exactly 10 elements");
    }

    let min_agg = MinAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let max_agg = MaxAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);

    for batch in &batches {
        let mut min_acc = min_agg.create_accumulator();
        let mut max_acc = max_agg.create_accumulator();

        for elem in &batch.elements {
            min_acc = min_agg.add(elem, min_acc);
            max_acc = max_agg.add(elem, max_acc);
        }

        let min_ts = min_agg.get_result(&min_acc);
        let max_ts = max_agg.get_result(&max_acc);

        assert!(
            max_ts >= min_ts,
            "max timestamp should be >= min timestamp within a batch"
        );
    }
}

// ---------------------------------------------------------------------------
// 15. Graph eviction with windowed reasoning
// ---------------------------------------------------------------------------

#[test]
fn test_graph_eviction_then_reason() {
    let mut graph = GraphPatternJoinState::new();

    // Old edge (timestamp 100)
    graph.add_statement(&stmt("http://ex.org/A", "http://ex.org/knows", "http://ex.org/B"), 100);

    // Recent edge (timestamp 5000)
    graph.add_statement(&stmt("http://ex.org/C", "http://ex.org/knows", "http://ex.org/D"), 5000);

    // Note: GraphPatternJoinState stores keys in Display format (<...> for IRIs)
    assert!(graph.has_edge("<http://ex.org/A>", "<http://ex.org/knows>"));
    assert!(graph.has_edge("<http://ex.org/C>", "<http://ex.org/knows>"));

    // Evict old edges (before timestamp 1000)
    graph.evict_before(1000);

    assert!(
        !graph.has_edge("<http://ex.org/A>", "<http://ex.org/knows>"),
        "old edge should be evicted"
    );
    assert!(
        graph.has_edge("<http://ex.org/C>", "<http://ex.org/knows>"),
        "recent edge should remain"
    );

    // Now reason over the remaining graph
    let rule = Rule::builder()
        .id("knows-reachable")
        .pattern(TriplePattern::new(
            PatternTerm::var("x"),
            PatternTerm::constant(iri("http://ex.org/knows")),
            PatternTerm::var("y"),
        ))
        .template(TripleTemplate::new(
            PatternTerm::var("x"),
            PatternTerm::constant(iri("http://ex.org/reachable")),
            PatternTerm::var("y"),
        ))
        .build();

    let rule_set = RuleSet::new(vec![rule]);
    let config = ReasoningConfig::default_config(rule_set);
    let mut network = ReteNetwork::compile(config);

    let recent_elem = make_rdf_element("http://ex.org/C", "http://ex.org/knows", "http://ex.org/D", 5000);
    let inferred = network.process_element(&recent_elem);

    assert!(!inferred.is_empty(), "should infer from recent data");
    assert_eq!(
        inferred[0].statement.predicate.as_str(),
        "http://ex.org/reachable"
    );
}
