//! Criterion benchmarks that load each parity-test fixture and drive
//! it through the Rust engine end-to-end.
//!
//! Each fixture under `parity-tests/fixtures/` becomes one Criterion
//! benchmark inside the `parity_fixtures` group, with throughput
//! reported in events/sec (the size of the workload's `streams.jsonl`).
//! The same workloads are intended to be driven through `cqels-java`'s
//! Maven-based runner under `parity-tests/runner-java/` when that
//! exists, so the resulting numbers compare apples-to-apples.
//!
//! The fixture format mirrors what the standalone parity runner
//! consumes; see `parity-tests/README.md` for the spec.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;

use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_engine::listener::listener_from_fn;
use cqels_engine::CqelsEngine;
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::{BindingSet, Statement, Term};

// ─── Fixture loading ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Metadata {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    #[allow(dead_code)]
    ground_truth: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Event {
    stream: String,
    ts: i64,
    s: String,
    p: String,
    o: String,
}

struct Workload {
    name: String,
    query: String,
    events: Vec<Event>,
    expected_bindings: usize,
}

fn fixtures_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `cqels-benchmarks/` at compile time —
    // navigate up one level to the workspace root, then into
    // `parity-tests/fixtures/`. Resolved at bench-build time so a
    // moved checkout doesn't break things.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().join("parity-tests/fixtures")
}

fn load_workload(dir: &Path) -> Workload {
    let metadata_str = fs::read_to_string(dir.join("metadata.toml"))
        .unwrap_or_else(|e| panic!("read metadata: {e}"));
    let metadata: Metadata =
        toml::from_str(&metadata_str).unwrap_or_else(|e| panic!("parse metadata: {e}"));
    let query =
        fs::read_to_string(dir.join("query.cqels")).unwrap_or_else(|e| panic!("read query: {e}"));

    let streams_str = fs::read_to_string(dir.join("streams.jsonl"))
        .unwrap_or_else(|e| panic!("read streams: {e}"));
    let mut events = Vec::new();
    for line in streams_str.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        events.push(
            serde_json::from_str::<Event>(line).unwrap_or_else(|e| panic!("parse event: {e}")),
        );
    }

    let expected_str = fs::read_to_string(dir.join("expected.jsonl"))
        .unwrap_or_else(|e| panic!("read expected: {e}"));
    let expected_bindings = expected_str
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim().starts_with('#'))
        .filter(|l| serde_json::from_str::<JsonValue>(l.trim()).is_ok())
        .count();

    Workload {
        name: metadata.name,
        query,
        events,
        expected_bindings,
    }
}

fn discover_workloads() -> Vec<Workload> {
    let root = fixtures_root();
    let mut dirs: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("read fixtures root {}: {e}", root.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs.iter().map(|d| load_workload(d)).collect()
}

// ─── Term parsing (matches runner-rust's heuristic) ─────────────────────────

fn parse_term(s: &str) -> Term {
    let t = s.trim();
    if t.starts_with('<') && t.ends_with('>') && t.len() >= 2 {
        return Term::Iri(IriTerm::new(&t[1..t.len() - 1]));
    }
    let looks_like_iri = t.starts_with("http://")
        || t.starts_with("https://")
        || t.starts_with("urn:")
        || t.starts_with("file://");
    if looks_like_iri {
        Term::Iri(IriTerm::new(t))
    } else {
        Term::Literal(LiteralTerm::new(t))
    }
}

// ─── End-to-end run on the engine ───────────────────────────────────────────

async fn run_workload(workload: &Workload) -> usize {
    let mut engine = CqelsEngine::builder()
        .build()
        .unwrap_or_else(|e| panic!("build engine: {e}"));

    let stream_names: BTreeMap<String, ()> = workload
        .events
        .iter()
        .map(|e| (e.stream.clone(), ()))
        .collect();
    let mut data_streams = Vec::with_capacity(stream_names.len());
    for name in stream_names.keys() {
        let ds = engine
            .create_stream(name)
            .await
            .unwrap_or_else(|e| panic!("create_stream({name}): {e}"));
        data_streams.push((name.clone(), ds));
    }

    let captured: Arc<Mutex<Vec<BindingSet>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_listener = captured.clone();
    let listener = listener_from_fn(move |bs: BindingSet| {
        captured_for_listener.lock().unwrap().push(bs);
    });
    let _query_id = engine
        .register_cqelsql_query(&workload.query, listener)
        .await
        .unwrap_or_else(|e| panic!("register_cqelsql_query: {e}"));

    engine
        .start()
        .await
        .unwrap_or_else(|e| panic!("start: {e}"));

    for event in &workload.events {
        let stmt = Statement::new(
            parse_term(&event.s),
            IriTerm::new(&event.p),
            parse_term(&event.o),
        );
        let elem = StreamElement::Rdf(RdfStreamElement::new(stmt, event.ts));
        let ds = data_streams
            .iter()
            .find(|(n, _)| n == &event.stream)
            .map(|(_, ds)| ds)
            .unwrap_or_else(|| panic!("event references undeclared stream `{}`", event.stream));
        ds.push(elem).await.unwrap_or_else(|e| panic!("push: {e}"));
    }

    // Soft-close every input so the watermark-driven tumbling window
    // gets a chance to flush its final batch before we measure stop.
    let names: Vec<String> = data_streams.iter().map(|(n, _)| n.clone()).collect();
    data_streams.clear();
    for name in &names {
        engine
            .close_stream(name)
            .await
            .unwrap_or_else(|e| panic!("close_stream({name}): {e}"));
    }

    // Give the binding pipeline up to 500 ms to land the final
    // bindings. The soft-close above is synchronous on the
    // forwarding task, but the binding stream is processed on a
    // separate task — bounded wait keeps the benchmark deterministic.
    let target = workload.expected_bindings;
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    loop {
        if captured.lock().unwrap().len() >= target || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    engine.stop().await.unwrap_or_else(|e| panic!("stop: {e}"));
    let count = captured.lock().unwrap().len();
    count
}

// ─── Criterion harness ──────────────────────────────────────────────────────

fn bench_parity_fixtures(c: &mut Criterion) {
    let workloads = discover_workloads();
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("parity_fixtures");
    for workload in &workloads {
        let event_count = workload.events.len() as u64;
        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(&workload.name),
            workload,
            |b, w| {
                b.iter(|| {
                    let bindings = rt.block_on(run_workload(w));
                    // Sanity: parity correctness is verified by the
                    // separate runner; here we just ensure each
                    // iteration produces the expected count so
                    // regressions in correctness fail the bench loudly
                    // instead of silently lowering reported throughput.
                    assert_eq!(
                        bindings, w.expected_bindings,
                        "workload `{}` produced {bindings} bindings, expected {}",
                        w.name, w.expected_bindings
                    );
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_parity_fixtures);
criterion_main!(benches);
