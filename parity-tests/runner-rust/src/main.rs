//! Rust runner for the language-neutral cqels parity fixtures.
//!
//! Drives a fixture's query + input events through the local
//! `cqels-engine` and compares produced bindings against the
//! fixture's `expected.jsonl`. Exit codes match the README:
//!
//! - 0: all bindings match expected, in order
//! - 1: mismatch (a unified diff is printed)
//! - 2: engine error while running the workload
//! - 3: fixture failed to load (malformed JSON, missing files, ...)
//!
//! Build + run from this directory:
//!
//! ```bash
//! cargo run --release -- ../fixtures/simple-select-now
//! cargo run --release -- --all ../fixtures
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_engine::listener::listener_from_fn;
use cqels_engine::CqelsEngine;
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::value::Value;
use cqels_model::{BindingSet, Statement, Term};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use similar::TextDiff;
use std::sync::{Arc, Mutex};

const EXIT_OK: u8 = 0;
const EXIT_MISMATCH: u8 = 1;
const EXIT_ENGINE_ERROR: u8 = 2;
const EXIT_LOAD_ERROR: u8 = 3;

#[derive(Debug, Deserialize)]
struct Metadata {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    #[allow(dead_code)]
    ground_truth: Option<String>,
    /// Optional override of how long to wait for the last expected
    /// binding to arrive after pushing all events. Defaults to 1s.
    /// Useful for windowed aggregations that flush on a tumbling
    /// boundary.
    #[serde(default)]
    drain_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct Event {
    stream: String,
    ts: i64,
    s: String,
    p: String,
    o: String,
}

/// One expected binding line — variables as a flat string map, plus
/// an optional timestamp. `_ts` in the source JSON moves to the
/// dedicated field; everything else stays in `bindings`.
#[derive(Debug, Clone, Default)]
struct ExpectedBinding {
    bindings: BTreeMap<String, String>,
    timestamp: Option<i64>,
}

#[derive(Debug)]
enum LoadError {
    Io(std::io::Error, PathBuf),
    Toml(toml::de::Error, PathBuf),
    Json(serde_json::Error, PathBuf, usize),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io(e, p) => write!(f, "{} reading {}", e, p.display()),
            LoadError::Toml(e, p) => write!(f, "{} parsing {}", e, p.display()),
            LoadError::Json(e, p, line) => {
                write!(f, "{} parsing {} line {}", e, p.display(), line)
            }
        }
    }
}

struct Workload {
    metadata: Metadata,
    query: String,
    events: Vec<Event>,
    expected: Vec<ExpectedBinding>,
}

fn load_workload(dir: &Path) -> Result<Workload, LoadError> {
    let metadata_path = dir.join("metadata.toml");
    let metadata_str =
        fs::read_to_string(&metadata_path).map_err(|e| LoadError::Io(e, metadata_path.clone()))?;
    let metadata: Metadata =
        toml::from_str(&metadata_str).map_err(|e| LoadError::Toml(e, metadata_path))?;

    let query_path = dir.join("query.cqels");
    let query = fs::read_to_string(&query_path).map_err(|e| LoadError::Io(e, query_path))?;

    let streams_path = dir.join("streams.jsonl");
    let streams_str =
        fs::read_to_string(&streams_path).map_err(|e| LoadError::Io(e, streams_path.clone()))?;
    let mut events = Vec::new();
    for (i, line) in streams_str.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let event: Event = serde_json::from_str(line)
            .map_err(|e| LoadError::Json(e, streams_path.clone(), i + 1))?;
        events.push(event);
    }

    let expected_path = dir.join("expected.jsonl");
    let expected_str =
        fs::read_to_string(&expected_path).map_err(|e| LoadError::Io(e, expected_path.clone()))?;
    let mut expected = Vec::new();
    for (i, line) in expected_str.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let json: JsonValue = serde_json::from_str(line)
            .map_err(|e| LoadError::Json(e, expected_path.clone(), i + 1))?;
        let mut binding = ExpectedBinding::default();
        if let Some(obj) = json.as_object() {
            for (k, v) in obj {
                if k == "_ts" {
                    binding.timestamp = v.as_i64();
                    continue;
                }
                binding
                    .bindings
                    .insert(strip_var_prefix(k), value_to_string(v));
            }
        }
        expected.push(binding);
    }

    Ok(Workload {
        metadata,
        query,
        events,
        expected,
    })
}

fn strip_var_prefix(name: &str) -> String {
    name.trim_start_matches('?').trim_start_matches('$').into()
}

fn value_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Heuristic: bracketed `<...>` or known IRI prefixes are IRIs;
/// everything else becomes a plain literal. Matches the convention
/// the README documents.
fn parse_term(s: &str) -> Term {
    let trimmed = s.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') && trimmed.len() >= 2 {
        return Term::Iri(IriTerm::new(&trimmed[1..trimmed.len() - 1]));
    }
    let looks_like_iri = trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("urn:")
        || trimmed.starts_with("file://");
    if looks_like_iri {
        Term::Iri(IriTerm::new(trimmed))
    } else {
        Term::Literal(LiteralTerm::new(trimmed))
    }
}

/// Converts a BindingSet into the same `BTreeMap<String, String>`
/// shape the expected lines load into so comparisons are straight-
/// forward.
fn binding_to_map(bs: &BindingSet) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for (var, value) in bs.iter() {
        map.insert(strip_var_prefix(var), value_display(value));
    }
    map
}

fn value_display(v: &Value) -> String {
    match v {
        Value::Term(t) => match t {
            Term::Iri(iri) => iri.as_str().to_string(),
            Term::Literal(lit) => lit.value().to_string(),
            other => other.to_string(),
        },
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
        // `Value` is non-exhaustive; surface any new variant as
        // its `Display` form rather than panicking.
        other => other.to_string(),
    }
}

async fn run_workload(workload: &Workload) -> Result<Vec<(BTreeMap<String, String>, i64)>, String> {
    let mut engine = CqelsEngine::builder()
        .build()
        .map_err(|e| format!("build engine: {e}"))?;

    // Pre-create every stream referenced by the workload's events.
    let mut stream_names: Vec<String> = workload
        .events
        .iter()
        .map(|e| e.stream.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if stream_names.is_empty() {
        return Err("workload defines no input streams".to_string());
    }
    stream_names.sort();
    let mut data_streams = Vec::new();
    for name in &stream_names {
        let ds = engine
            .create_stream(name)
            .await
            .map_err(|e| format!("create_stream({name}): {e}"))?;
        data_streams.push((name.clone(), ds));
    }

    // Register the query, capturing bindings into a shared vec.
    let captured: Arc<Mutex<Vec<(BindingSet, i64)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_listener = captured.clone();
    let listener = listener_from_fn(move |bs: BindingSet| {
        let ts = bs.timestamp();
        captured_for_listener.lock().unwrap().push((bs, ts));
    });
    let _query_id = engine
        .register_cqelsql_query(&workload.query, listener)
        .await
        .map_err(|e| format!("register_cqelsql_query: {e}"))?;

    engine.start().await.map_err(|e| format!("start: {e}"))?;

    // Push every event in declaration order, honoring its declared
    // timestamp by constructing the RdfStreamElement directly.
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

    // Close every input. The engine holds its own clone of each
    // stream's mpsc sender, so just dropping the runner's
    // `DataStream` handles isn't enough — `close_stream` removes
    // the engine's internal clone too and aborts the forwarding
    // task. With every sender dropped, the broadcast subscribers
    // see end-of-stream, which lets event-driven operators (notably
    // `TumblingTimeWindowStream`) flush their final open batch.
    let stream_names: Vec<String> = data_streams.iter().map(|(n, _)| n.clone()).collect();
    data_streams.clear();
    for name in &stream_names {
        engine
            .close_stream(name)
            .await
            .map_err(|e| format!("close_stream({name}): {e}"))?;
    }

    // Drain. Bindings flow through tokio tasks, so wait a bounded
    // amount of time for the last one to land. The metadata can
    // override the default (1s) for slow tumbling-window workloads.
    let drain_ms = workload.metadata.drain_timeout_ms.unwrap_or(1000);
    let drain_until = std::time::Instant::now() + Duration::from_millis(drain_ms);
    let target = workload.expected.len();
    loop {
        let count = captured.lock().unwrap().len();
        if count >= target {
            break;
        }
        if std::time::Instant::now() >= drain_until {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    engine.stop().await.map_err(|e| format!("stop: {e}"))?;

    let captured = captured.lock().unwrap();
    let out: Vec<_> = captured
        .iter()
        .map(|(bs, ts)| (binding_to_map(bs), *ts))
        .collect();
    Ok(out)
}

/// Returns a one-line-per-binding representation suitable for
/// `similar`'s text diff.
fn render(bindings: &[(BTreeMap<String, String>, i64)], include_ts: bool) -> String {
    let mut out = String::new();
    for (b, ts) in bindings {
        let mut line = serde_json::Map::new();
        for (k, v) in b {
            line.insert(k.clone(), JsonValue::String(v.clone()));
        }
        if include_ts {
            line.insert("_ts".into(), JsonValue::Number((*ts).into()));
        }
        out.push_str(&serde_json::to_string(&line).unwrap());
        out.push('\n');
    }
    out
}

fn compare(
    actual: &[(BTreeMap<String, String>, i64)],
    expected: &[ExpectedBinding],
) -> Result<(), String> {
    // Determine whether any expected line specified `_ts`. If so we
    // include timestamps in the rendered comparison; otherwise we
    // strip them so the test is timestamp-agnostic.
    let any_ts = expected.iter().any(|e| e.timestamp.is_some());

    let expected_lines = expected
        .iter()
        .map(|e| {
            let mut bs = (e.bindings.clone(), e.timestamp.unwrap_or(0));
            if !any_ts {
                bs.1 = 0;
            }
            bs
        })
        .collect::<Vec<_>>();

    let actual_rendered = render(actual, any_ts);
    let expected_rendered = render(&expected_lines, any_ts);

    if actual_rendered == expected_rendered {
        return Ok(());
    }

    let diff = TextDiff::from_lines(&expected_rendered, &actual_rendered);
    let mut report = String::from("bindings mismatch (- expected, + actual):\n");
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            similar::ChangeTag::Delete => "-",
            similar::ChangeTag::Insert => "+",
            similar::ChangeTag::Equal => " ",
        };
        report.push_str(&format!("{sign}{}", change));
    }
    Err(report)
}

async fn run_one(dir: &Path) -> u8 {
    let workload = match load_workload(dir) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[{}] load error: {e}", dir.display());
            return EXIT_LOAD_ERROR;
        }
    };
    println!("running `{}` ...", workload.metadata.name);
    match run_workload(&workload).await {
        Ok(actual) => match compare(&actual, &workload.expected) {
            Ok(()) => {
                println!(
                    "  ok ({} binding{})",
                    actual.len(),
                    if actual.len() == 1 { "" } else { "s" }
                );
                EXIT_OK
            }
            Err(diff) => {
                eprintln!("  FAIL\n{diff}");
                EXIT_MISMATCH
            }
        },
        Err(e) => {
            eprintln!("  engine error: {e}");
            EXIT_ENGINE_ERROR
        }
    }
}

async fn run_all(root: &Path) -> u8 {
    let entries = match fs::read_dir(root) {
        Ok(it) => it,
        Err(e) => {
            eprintln!("read fixtures root {}: {e}", root.display());
            return EXIT_LOAD_ERROR;
        }
    };
    let mut dirs: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    dirs.sort();
    if dirs.is_empty() {
        eprintln!("no fixture directories under {}", root.display());
        return EXIT_LOAD_ERROR;
    }
    let mut worst = EXIT_OK;
    for dir in dirs {
        let code = run_one(&dir).await;
        if code > worst {
            worst = code;
        }
    }
    worst
}

/// Runs a fixture and emits the captured bindings as JSONL to stdout,
/// one binding per line, with no status messages or framing. Used by
/// `cargo xtask parity diff` / `parity capture` to compare engine
/// outputs against each other (or capture one engine's output as the
/// fixture's `expected.jsonl`) without needing a hand-spec oracle.
///
/// Timestamps are omitted — the differential workflow targets binding
/// content; if a future variant of this needs `_ts`, expose a
/// `--with-ts` flag rather than changing the default.
async fn dump_one(dir: &Path) -> u8 {
    let workload = match load_workload(dir) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[{}] load error: {e}", dir.display());
            return EXIT_LOAD_ERROR;
        }
    };
    match run_workload(&workload).await {
        Ok(actual) => {
            print!("{}", render(&actual, false));
            EXIT_OK
        }
        Err(e) => {
            eprintln!("engine error: {e}");
            EXIT_ENGINE_ERROR
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let exit = match args.as_slice() {
        [flag, dir] if flag == "--dump" => dump_one(Path::new(dir)).await,
        [flag, root] if flag == "--all" => run_all(Path::new(root)).await,
        [single] if single != "--all" && single != "--dump" => run_one(Path::new(single)).await,
        _ => {
            eprintln!("usage: cqels-parity-runner <fixture-dir>");
            eprintln!("       cqels-parity-runner --all <fixtures-root>");
            eprintln!("       cqels-parity-runner --dump <fixture-dir>");
            EXIT_LOAD_ERROR
        }
    };
    ExitCode::from(exit)
}
