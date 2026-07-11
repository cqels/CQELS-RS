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
    /// Drain window (ms) to wait for async bindings after closing the streams.
    /// Mirrors the parity runner's metadata field (#83); absent → 1000 ms.
    drain_timeout_ms: Option<u64>,
}

/// Default drain window when a fixture's `metadata.toml` omits
/// `drain_timeout_ms`. Matches the Rust parity runner's default.
const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 1000;

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
    /// Contents of the fixture's `static.trig`, if present, loaded into the
    /// engine's RDF store before the query is registered (#83).
    static_trig: Option<String>,
    /// Drain window to wait for async bindings (from metadata; #83).
    drain_timeout_ms: u64,
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

    // Optional static-graph data — only some fixtures ship a `static.trig`.
    let static_trig = match fs::read_to_string(dir.join("static.trig")) {
        Ok(s) => Some(s),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => panic!("read static.trig: {e}"),
    };

    Workload {
        name: metadata.name,
        query,
        events,
        expected_bindings,
        static_trig,
        drain_timeout_ms: metadata
            .drain_timeout_ms
            .unwrap_or(DEFAULT_DRAIN_TIMEOUT_MS),
    }
}

/// Parses a fixture's `static.trig` and seeds the engine's RDF store, mirroring
/// the Rust parity runner's loader: default-graph triples go through
/// `load_statements`, named-graph triples through `load_named_graph`. Returns
/// the number of statements loaded so callers can confirm static data landed.
///
/// Errors (malformed `static.trig`, store insert failures) are returned rather
/// than panicked so `bench_parity_fixtures` can skip the offending fixture
/// instead of aborting the whole benchmark.
///
/// `#[allow(deprecated)]`: oxigraph 0.4 deprecates `DatasetParser` in favor of
/// `oxrdfio::RdfParser`, but the runner uses the same one-file consumer and
/// pulling in another crate isn't worth it here.
#[allow(deprecated)]
fn load_static_data(engine: &CqelsEngine, trig: &str) -> Result<usize, String> {
    use oxigraph::io::{DatasetFormat, DatasetParser};
    use std::collections::HashMap;

    let parser = DatasetParser::from_format(DatasetFormat::TriG);
    let mut by_graph: HashMap<Option<String>, Vec<Statement>> = HashMap::new();
    for quad in parser.read_quads(trig.as_bytes()) {
        let quad = quad.map_err(|e| format!("static.trig parse: {e}"))?;
        // Default graph → None; a named/blank graph → its IRI string (the store
        // treats blank-node graphs as opaque IRIs, matching the runner).
        let graph_key = match &quad.graph_name {
            oxigraph::model::GraphName::DefaultGraph => None,
            other => Some(other.to_string()),
        };
        by_graph.entry(graph_key).or_default().push(quad.into());
    }

    let mut loaded = 0;
    for (graph, stmts) in by_graph {
        loaded += stmts.len();
        match graph {
            None => engine
                .load_statements(&stmts)
                .map_err(|e| format!("load_statements: {e}"))?,
            Some(iri) => {
                // `DatasetParser` renders a `<iri>` graph context wrapped in
                // angle brackets; strip them so the store sees the plain IRI
                // that `query_named_graph_pattern` expects.
                let plain = iri
                    .strip_prefix('<')
                    .and_then(|s| s.strip_suffix('>'))
                    .unwrap_or(&iri)
                    .to_string();
                engine
                    .load_named_graph(&plain, &stmts)
                    .map_err(|e| format!("load_named_graph({plain}): {e}"))?;
            }
        }
    }
    Ok(loaded)
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

async fn run_workload(workload: &Workload) -> Result<usize, String> {
    let engine = CqelsEngine::builder()
        .build()
        .map_err(|e| format!("build engine: {e}"))?;

    // Seed static-graph data BEFORE registering the query so the compiled
    // query's static-pattern resolution sees it (#83). Without this, static
    // fixtures were benchmarked against an empty store.
    if let Some(trig) = &workload.static_trig {
        load_static_data(&engine, trig)?;
    }

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
            .map_err(|e| format!("create_stream({name}): {e}"))?;
        data_streams.push((name.clone(), ds));
    }

    let captured: Arc<Mutex<Vec<BindingSet>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_listener = captured.clone();
    let listener = listener_from_fn(move |bs: BindingSet| {
        captured_for_listener.lock().unwrap().push(bs);
    });
    engine
        .register_cqelsql_query(&workload.query, listener)
        .await
        .map_err(|e| format!("register_cqelsql_query: {e}"))?;

    engine.start().await.map_err(|e| format!("start: {e}"))?;

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
            .ok_or_else(|| format!("event references undeclared stream `{}`", event.stream))?;
        ds.push(elem).await.map_err(|e| format!("push: {e}"))?;
    }

    // Soft-close every input so the watermark-driven tumbling window
    // gets a chance to flush its final batch before we measure stop.
    let names: Vec<String> = data_streams.iter().map(|(n, _)| n.clone()).collect();
    data_streams.clear();
    for name in &names {
        engine
            .close_stream(name)
            .await
            .map_err(|e| format!("close_stream({name}): {e}"))?;
    }

    // Wait the fixture's drain window for the final bindings to land. The
    // soft-close above is synchronous on the forwarding task, but the binding
    // stream is processed on a separate task. We wait the FULL window for
    // zero-expected fixtures (where `len() >= 0` is immediately true) so late
    // — and erroneous — outputs are still observed (#83); otherwise we can exit
    // as soon as the expected count is reached.
    let target = workload.expected_bindings;
    let deadline = std::time::Instant::now() + Duration::from_millis(workload.drain_timeout_ms);
    loop {
        let have = captured.lock().unwrap().len();
        if std::time::Instant::now() >= deadline || (target > 0 && have >= target) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    engine.stop().await.map_err(|e| format!("stop: {e}"))?;
    let count = captured.lock().unwrap().len();
    Ok(count)
}

// ─── Criterion harness ──────────────────────────────────────────────────────

/// Fixtures the Rust engine cannot currently run to a benchmarkable result,
/// with the reason. These are skipped in preflight; **any other** fixture that
/// fails preflight panics, so a newly-regressed fixture can't silently drop out
/// and let the benchmark "pass" with reduced coverage (#83 review). Remove
/// entries here as the underlying engine gaps close.
const KNOWN_UNBENCHMARKABLE: &[(&str, &str)] = &[
    // CqelsQL parser gaps — the query fails to register.
    ("optional-extra-property", "OPTIONAL not yet parsed"),
    ("order-by-limit", "query not yet parsed by CqelsQL"),
    ("union-two-properties", "UNION not yet parsed"),
    // Engine result divergences — Rust currently emits 0 of the expected rows.
    (
        "stream-static-lookup-join",
        "GRAPH named-graph join emits 0 rows on Rust",
    ),
    ("triples-filter-numeric", "emits 0 rows on Rust"),
];

fn bench_parity_fixtures(c: &mut Criterion) {
    let workloads = discover_workloads();
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("parity_fixtures");
    for workload in &workloads {
        // Trial run once before measuring; only benchmark fixtures that produce
        // their expected (positive) binding count, where the drain loop exits
        // early and measures real throughput. A fixture that produces the wrong
        // count or no output would make every iteration wait out the full drain
        // timeout, so we don't benchmark it (#83 review). Such a fixture must be
        // an explicitly-listed known gap — otherwise it's a regression and we
        // panic, so coverage can't silently shrink.
        let result = rt.block_on(run_workload(workload));
        let benchmarkable =
            matches!(&result, Ok(count) if *count == workload.expected_bindings && *count > 0);
        if !benchmarkable {
            let detail = match &result {
                Ok(count) => format!(
                    "produced {count} bindings, expected {}",
                    workload.expected_bindings
                ),
                Err(e) => e.clone(),
            };
            match KNOWN_UNBENCHMARKABLE
                .iter()
                .find(|(n, _)| *n == workload.name)
            {
                Some((_, why)) => {
                    eprintln!(
                        "skipping known-unbenchmarkable parity fixture `{}`: {why} ({detail})",
                        workload.name
                    );
                    continue;
                }
                None => panic!(
                    "parity fixture `{}` is not benchmarkable ({detail}); if this is an accepted \
                     engine gap add it to KNOWN_UNBENCHMARKABLE, otherwise it is a regression",
                    workload.name
                ),
            }
        }

        let event_count = workload.events.len() as u64;
        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(&workload.name),
            workload,
            |b, w| {
                b.iter(|| {
                    let count = rt
                        .block_on(run_workload(w))
                        .unwrap_or_else(|e| panic!("workload `{}` failed mid-bench: {e}", w.name));
                    // The preflight only checks the first run. Re-assert per
                    // iteration so a flaky drain (count drops below the target,
                    // making the iteration measure the timeout) fails loudly
                    // instead of being recorded as throughput (#83 review).
                    assert_eq!(
                        count, w.expected_bindings,
                        "workload `{}` produced {count} bindings mid-bench, expected {}",
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
