//! End-to-end CqelsEngine throughput benchmarks via the DataStream push API.
//!
//! Measures the overhead of the facade layer when pushing stream elements
//! through the CqelsEngine.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use cqels_benchmarks::generate_rdf_stream_batch;
use cqels_engine::element_factory;

// ─── Element factory benchmarks ──────────────────────────────────────────────

fn bench_element_factory(c: &mut Criterion) {
    let mut group = c.benchmark_group("element_factory");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("triple_uri", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    for i in 0..count as usize {
                        black_box(element_factory::triple_uri(
                            &format!("http://example.org/s{i}"),
                            "http://example.org/p",
                            &format!("http://example.org/o{i}"),
                        ));
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("triple_f64", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    for i in 0..count as usize {
                        black_box(element_factory::triple_f64(
                            &format!("http://sensor/{}", i % 100),
                            "http://example.org/value",
                            i as f64 * 1.5,
                        ));
                    }
                });
            },
        );
    }

    group.finish();
}

// ─── DataStream push throughput ──────────────────────────────────────────────

fn bench_data_stream_push(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("data_stream_push");

    for &count in &[1000, 5000] {
        group.throughput(Throughput::Elements(count));
        let elements = generate_rdf_stream_batch(count as usize);

        group.bench_with_input(
            BenchmarkId::new("push_batch", count),
            &elements,
            |b, elements| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut engine = cqels_engine::CqelsEngine::builder()
                            .id("bench")
                            .broadcast_capacity(count as usize * 2)
                            .build()
                            .unwrap();
                        let stream = engine.create_stream("bench_stream").await.unwrap();
                        for elem in elements {
                            let _ = stream.push(elem.clone()).await;
                        }
                        black_box(());
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Batch generation comparison ────────────────────────────────────────────

fn bench_profile_data_generation(c: &mut Criterion) {
    use cqels_benchmarks::generators::{
        generate_property_stream, generate_rdfs_stream, generate_transitive_chain,
    };

    let mut group = c.benchmark_group("profile_data_generation");

    for &count in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("rdfs_stream", count),
            &count,
            |b, &count| {
                b.iter(|| black_box(generate_rdfs_stream(count as usize, 5)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("property_stream", count),
            &count,
            |b, &count| {
                b.iter(|| black_box(generate_property_stream(count as usize, 10)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("transitive_chain", count),
            &count,
            |b, &count| {
                b.iter(|| black_box(generate_transitive_chain(count as usize, 10)));
            },
        );
    }

    group.finish();
}

// ─── Entry point ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_element_factory,
    bench_data_stream_push,
    bench_profile_data_generation,
);
criterion_main!(benches);
