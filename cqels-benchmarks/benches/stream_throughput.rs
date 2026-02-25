//! Stream throughput benchmarks for the CQELS engine.
//!
//! Measures the throughput of core stream processing operations:
//! - RDF stream element creation and access
//! - Window operator throughput (tumbling, sliding, count)
//! - RETE network processing throughput
//! - CqelsQL/CypherQL parser throughput

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;

use cqels_benchmarks::{generate_rdf_stream_batch, generate_sensor_readings, generate_social_events};
use cqels_core::operator::aggregate::{
    AggregateFunction, CountAggregate, SumAggregate, WindowedAggregateOperator, GroupKey,
};
use cqels_core::parser::{CqelsQlParser, CypherQlParser};
use cqels_core::stream::{RdfStreamElement, Timestamped};
use cqels_core::window::{SlidingWindow, TumblingCountWindow, TumblingWindow, Window};
use cqels_engine::{NfaPatternProcessor, Pattern};
use cqels_model::statement::Statement;
use cqels_model::term::{IriTerm, LiteralTerm, Term};
use cqels_reasoning::{
    ReasoningConfig, ReteNetwork, Rule, RuleSet,
    PatternTerm, TriplePattern, TripleTemplate,
};

// ─── Stream element creation benchmarks ──────────────────────────────────────

fn bench_rdf_element_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_element_creation");

    for &count in &[100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |b, &count| {
                b.iter(|| {
                    black_box(generate_rdf_stream_batch(count as usize));
                });
            },
        );
    }

    group.finish();
}

fn bench_statement_construction(c: &mut Criterion) {
    c.bench_function("statement_construction", |b| {
        b.iter(|| {
            let subject = Term::Iri(IriTerm::new("http://example.org/s1"));
            let predicate = IriTerm::new("http://example.org/p1");
            let object = Term::Literal(
                LiteralTerm::new("42.0")
                    .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
            );
            let stmt = Statement::new(subject, predicate, object);
            black_box(RdfStreamElement::new(stmt, 1000));
        });
    });
}

// ─── Batch generation benchmarks ─────────────────────────────────────────────

fn bench_batch_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_generation");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("sensor_readings", count),
            &count,
            |b, &count| {
                b.iter(|| black_box(generate_sensor_readings(count as usize)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("social_events", count),
            &count,
            |b, &count| {
                b.iter(|| black_box(generate_social_events(count as usize)));
            },
        );
    }

    group.finish();
}

// ─── RETE network benchmarks ─────────────────────────────────────────────────

fn make_single_pattern_ruleset() -> RuleSet {
    let rule = Rule::builder()
        .name("temp_rule")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::Variable("s".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
            PatternTerm::Variable("val".into()),
        ))
        .template(TripleTemplate::new(
            PatternTerm::Variable("s".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasReading"))),
            PatternTerm::Variable("val".into()),
        ))
        .build();
    RuleSet::builder().add_rule(rule).build()
}

fn bench_rete_single_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("rete_processing");

    for &count in &[100, 1000, 5000] {
        group.throughput(Throughput::Elements(count));

        let elements = generate_sensor_readings(count as usize);

        group.bench_with_input(
            BenchmarkId::new("single_pattern", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(make_single_pattern_ruleset())
                        .build();
                    let mut network = ReteNetwork::compile(config);
                    let mut total = 0usize;
                    for elem in elements {
                        total += network.process_element(elem).len();
                    }
                    black_box(total);
                });
            },
        );
    }

    group.finish();
}

fn make_join_pattern_ruleset() -> RuleSet {
    let rule = Rule::builder()
        .name("social_rule")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::Variable("a".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://social.org/follows"))),
            PatternTerm::Variable("b".into()),
        ))
        .pattern(TriplePattern::new(
            PatternTerm::Variable("a".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://social.org/likes"))),
            PatternTerm::Variable("b".into()),
        ))
        .template(TripleTemplate::new(
            PatternTerm::Variable("a".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://social.org/friendsWith"))),
            PatternTerm::Variable("b".into()),
        ))
        .build();
    RuleSet::builder().add_rule(rule).build()
}

fn bench_rete_join_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("rete_join");

    for &count in &[100, 500, 2000] {
        group.throughput(Throughput::Elements(count));

        let elements = generate_social_events(count as usize);

        group.bench_with_input(
            BenchmarkId::new("two_pattern_join", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(make_join_pattern_ruleset())
                        .default_window(Duration::from_secs(60))
                        .build();
                    let mut network = ReteNetwork::compile(config);
                    let mut total = 0usize;
                    for elem in elements {
                        total += network.process_element(elem).len();
                    }
                    black_box(total);
                });
            },
        );
    }

    group.finish();
}

// ─── Parser benchmarks ──────────────────────────────────────────────────────

fn bench_cqelsql_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("cqelsql_parser");

    let simple_query = r#"
        SELECT ?x ?y
        FROM STREAM sensor [NOW]
        WHERE {
            ?x <http://example.org/value> ?y .
        }
    "#;

    let complex_query = r#"
        PREFIX ex: <http://example.org/>
        REGISTER QUERY sensorAnalytics AS
        SELECT ?type (AVG(?val) AS ?avgVal) (COUNT(?s) AS ?cnt)
        FROM STREAM sensor [RANGE 30s]
        FROM <http://example.org/static>
        WHERE {
            STREAM sensor {
                ?s ex:reading ?val .
                ?s ex:type ?type .
            }
            STATIC {
                ?s ex:location ?loc .
            }
        }
        GROUP BY ?type
        ORDER BY ?avgVal DESC
        LIMIT 10
    "#;

    group.bench_function("simple_query", |b| {
        b.iter(|| black_box(CqelsQlParser::parse(simple_query).unwrap()));
    });

    group.bench_function("complex_query", |b| {
        b.iter(|| black_box(CqelsQlParser::parse(complex_query).unwrap()));
    });

    group.finish();
}

fn bench_cypherql_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("cypherql_parser");

    let simple_query = r#"
        FROM STREAM events [NOW]
        MATCH (n:Person)
        RETURN n.name
    "#;

    let complex_query = r#"
        REGISTER QUERY socialAnalysis AS
        FROM STREAM social [RANGE 1m]
        FROM STATIC 'http://example.org/static'
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.age > 18
        GROUP BY a.city
        RETURN a.city, count(b) AS connections
        ORDER BY connections DESC
        LIMIT 20
    "#;

    group.bench_function("simple_query", |b| {
        b.iter(|| black_box(CypherQlParser::parse(simple_query).unwrap()));
    });

    group.bench_function("complex_query", |b| {
        b.iter(|| black_box(CypherQlParser::parse(complex_query).unwrap()));
    });

    group.finish();
}

// ─── Window operator benchmarks ─────────────────────────────────────────────

fn bench_tumbling_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tumbling_window");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_sensor_readings(count as usize);

        group.bench_with_input(
            BenchmarkId::new("time_10s", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = Box::pin(futures::stream::iter(elements.clone()));
                        let window = TumblingWindow::new(Duration::from_secs(10));
                        let batches: Vec<_> = window.apply(stream).collect().await;
                        black_box(batches.len());
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_sliding_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_window");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_sensor_readings(count as usize);

        group.bench_with_input(
            BenchmarkId::new("range15s_slide5s", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = Box::pin(futures::stream::iter(elements.clone()));
                        let window = SlidingWindow::new(
                            Duration::from_secs(15),
                            Duration::from_secs(5),
                        );
                        let batches: Vec<_> = window.apply(stream).collect().await;
                        black_box(batches.len());
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_count_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("count_window");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_sensor_readings(count as usize);

        group.bench_with_input(
            BenchmarkId::new("batch_100", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = Box::pin(futures::stream::iter(elements.clone()));
                        let window = TumblingCountWindow::new(100);
                        let batches: Vec<_> = window.apply(stream).collect().await;
                        black_box(batches.len());
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Windowed aggregation benchmarks ────────────────────────────────────────

fn bench_windowed_aggregation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("windowed_aggregation");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_sensor_readings(count as usize);

        // Window + count aggregate
        group.bench_with_input(
            BenchmarkId::new("count_grouped", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = Box::pin(futures::stream::iter(elements.clone()));
                        let window = TumblingWindow::new(Duration::from_secs(10));
                        let batches: Vec<_> = window.apply(stream).collect().await;

                        let count_op = WindowedAggregateOperator::new(
                            CountAggregate,
                            |e: &RdfStreamElement| {
                                Some(GroupKey::single(e.statement.predicate.as_str().to_string()))
                            },
                            10_000,
                        );

                        let mut total = 0;
                        for batch in &batches {
                            total += count_op.process_batch(&batch.elements).len();
                        }
                        black_box(total);
                    });
                });
            },
        );

        // Window + sum aggregate (no grouping)
        group.bench_with_input(
            BenchmarkId::new("sum_ungrouped", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let stream = Box::pin(futures::stream::iter(elements.clone()));
                        let window = TumblingWindow::new(Duration::from_secs(10));
                        let batches: Vec<_> = window.apply(stream).collect().await;

                        let sum_agg = SumAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
                        let mut total = 0.0f64;
                        for batch in &batches {
                            let mut acc = sum_agg.create_accumulator();
                            for elem in &batch.elements {
                                acc = sum_agg.add(elem, acc);
                            }
                            total += sum_agg.get_result(&acc);
                        }
                        black_box(total);
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── CEP pattern matching benchmarks ────────────────────────────────────────

/// A lightweight event for CEP benchmarking.
#[derive(Debug, Clone)]
struct BenchEvent {
    name: String,
    value: f64,
    timestamp: i64,
}

impl Timestamped for BenchEvent {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

fn generate_bench_events(count: usize) -> Vec<BenchEvent> {
    (0..count)
        .map(|i| {
            let name = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
            BenchEvent {
                name: name.to_string(),
                value: (i as f64) * 1.5,
                timestamp: (i as i64) * 100,
            }
        })
        .collect()
}

fn bench_cep_simple_sequence(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cep_processing");

    for &count in &[100, 1000, 5000] {
        group.throughput(Throughput::Elements(count));
        let events = generate_bench_events(count as usize);

        // Simple two-state sequence: A followed by B
        group.bench_with_input(
            BenchmarkId::new("two_state", count),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let pattern = Pattern::begin("first")
                            .where_cond(|e: &BenchEvent| e.name == "A")
                            .followed_by("second")
                            .where_cond(|e: &BenchEvent| e.name == "B");

                        let processor = NfaPatternProcessor::new(pattern);
                        let stream = Box::pin(futures::stream::iter(events.clone()));
                        let matches: Vec<_> = processor.process(stream).collect().await;
                        black_box(matches.len());
                    });
                });
            },
        );

        // Three-state sequence: A → B → C
        group.bench_with_input(
            BenchmarkId::new("three_state", count),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let pattern = Pattern::begin("first")
                            .where_cond(|e: &BenchEvent| e.name == "A")
                            .followed_by("second")
                            .where_cond(|e: &BenchEvent| e.name == "B")
                            .followed_by("third")
                            .where_cond(|e: &BenchEvent| e.name == "C");

                        let processor = NfaPatternProcessor::new(pattern);
                        let stream = Box::pin(futures::stream::iter(events.clone()));
                        let matches: Vec<_> = processor.process(stream).collect().await;
                        black_box(matches.len());
                    });
                });
            },
        );

        // Context-sensitive: increasing values
        group.bench_with_input(
            BenchmarkId::new("context_sensitive", count),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let pattern = Pattern::begin("first")
                            .where_cond(|e: &BenchEvent| e.name == "A")
                            .followed_by("second")
                            .where_context(|e: &BenchEvent, prev: &[BenchEvent]| {
                                prev.last().is_some_and(|p| e.value > p.value)
                            });

                        let processor = NfaPatternProcessor::new(pattern);
                        let stream = Box::pin(futures::stream::iter(events.clone()));
                        let matches: Vec<_> = processor.process(stream).collect().await;
                        black_box(matches.len());
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_cep_with_time_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cep_time_window");

    for &count in &[100, 1000, 5000] {
        group.throughput(Throughput::Elements(count));
        let events = generate_bench_events(count as usize);

        group.bench_with_input(
            BenchmarkId::new("within_1s", count),
            &events,
            |b, events| {
                b.iter(|| {
                    rt.block_on(async {
                        let pattern = Pattern::begin("first")
                            .where_cond(|e: &BenchEvent| e.name == "A")
                            .followed_by("second")
                            .where_cond(|e: &BenchEvent| e.name == "B")
                            .within(Duration::from_secs(1));

                        let processor = NfaPatternProcessor::new(pattern);
                        let stream = Box::pin(futures::stream::iter(events.clone()));
                        let matches: Vec<_> = processor.process(stream).collect().await;
                        black_box(matches.len());
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Entry point ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_rdf_element_creation,
    bench_statement_construction,
    bench_batch_generation,
    bench_rete_single_pattern,
    bench_rete_join_pattern,
    bench_cqelsql_parser,
    bench_cypherql_parser,
    bench_tumbling_window,
    bench_sliding_window,
    bench_count_window,
    bench_windowed_aggregation,
    bench_cep_simple_sequence,
    bench_cep_with_time_window,
);
criterion_main!(benches);
