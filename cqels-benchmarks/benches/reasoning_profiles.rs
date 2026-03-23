//! Reasoning profile throughput benchmarks.
//!
//! Compares RETE inference throughput across different reasoning profiles
//! (RDFS, OWL-Lite, OWL 2 RL) using profile-specific stream data.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use cqels_benchmarks::generators::{
    generate_rdfs_stream, generate_symmetric_relations, generate_transitive_chain,
};
use cqels_benchmarks::profiles::{owl2_rl_scenario, owl_lite_scenario, rdfs_scenario};
use cqels_reasoning::{ReasoningConfig, ReteNetwork};

// ─── RDFS profile benchmarks ────────────────────────────────────────────────

fn bench_rdfs_profile(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdfs_profile");

    for &count in &[500, 2000, 5000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_rdfs_stream(count as usize, 5);
        let scenario = rdfs_scenario();

        group.bench_with_input(
            BenchmarkId::new("rdfs_subclass", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(scenario.profile.rule_set())
                        .build();
                    let mut network = ReteNetwork::compile(config);

                    // Load schema triples first
                    for schema_stmt in &scenario.schema {
                        let schema_elem =
                            cqels_core::stream::RdfStreamElement::new(schema_stmt.clone(), 0);
                        network.process_element(&schema_elem);
                    }

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

// ─── OWL-Lite profile benchmarks ────────────────────────────────────────────

fn bench_owl_lite_transitive(c: &mut Criterion) {
    let mut group = c.benchmark_group("owl_lite_transitive");

    for &count in &[200, 1000, 3000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_transitive_chain(count as usize, 10);
        let scenario = owl_lite_scenario();

        group.bench_with_input(
            BenchmarkId::new("transitive_chain", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(scenario.profile.rule_set())
                        .build();
                    let mut network = ReteNetwork::compile(config);

                    for schema_stmt in &scenario.schema {
                        let schema_elem =
                            cqels_core::stream::RdfStreamElement::new(schema_stmt.clone(), 0);
                        network.process_element(&schema_elem);
                    }

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

fn bench_owl_lite_symmetric(c: &mut Criterion) {
    let mut group = c.benchmark_group("owl_lite_symmetric");

    for &count in &[200, 1000, 3000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_symmetric_relations(count as usize);
        let scenario = owl_lite_scenario();

        group.bench_with_input(
            BenchmarkId::new("symmetric_relations", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(scenario.profile.rule_set())
                        .build();
                    let mut network = ReteNetwork::compile(config);

                    for schema_stmt in &scenario.schema {
                        let schema_elem =
                            cqels_core::stream::RdfStreamElement::new(schema_stmt.clone(), 0);
                        network.process_element(&schema_elem);
                    }

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

// ─── OWL 2 RL profile benchmark ────────────────────────────────────────────

fn bench_owl2_rl_profile(c: &mut Criterion) {
    let mut group = c.benchmark_group("owl2_rl_profile");

    for &count in &[200, 1000, 3000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_rdfs_stream(count as usize, 5);
        let scenario = owl2_rl_scenario();

        group.bench_with_input(
            BenchmarkId::new("owl2_rl_full", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    let config = ReasoningConfig::builder()
                        .rule_set(scenario.profile.rule_set())
                        .build();
                    let mut network = ReteNetwork::compile(config);

                    for schema_stmt in &scenario.schema {
                        let schema_elem =
                            cqels_core::stream::RdfStreamElement::new(schema_stmt.clone(), 0);
                        network.process_element(&schema_elem);
                    }

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

// ─── Entry point ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_rdfs_profile,
    bench_owl_lite_transitive,
    bench_owl_lite_symmetric,
    bench_owl2_rl_profile,
);
criterion_main!(benches);
