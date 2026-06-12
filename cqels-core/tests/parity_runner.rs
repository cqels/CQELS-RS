//! Rust side of the cross-language parity harness.
//!
//! This binary is the Rust entry point for the parity fixture harness at
//! `<parity-root>/parity/fixtures`. It runs the `_meta/` self-tests (which pin that this side's
//! RDFC-1.0 canonical output matches the Java side's, byte-for-byte) plus any per-unit corpora
//! that have been wired with a real operator adapter.
//!
//! The harness and the oxrdf-backed `RdfBackend` are vendored under `tests/parity/` to keep this
//! file small. They are loaded via `#[path]` to avoid cargo treating them as separate test bins.

#[path = "parity/parity_fixture_harness.rs"]
mod parity_fixture_harness;

#[path = "parity/oxrdf_backend.rs"]
mod oxrdf_backend;

#[path = "parity/tumbling_window_adapter.rs"]
mod tumbling_window_adapter;

#[path = "parity/sliding_window_adapter.rs"]
mod sliding_window_adapter;

#[path = "parity/solution_codec.rs"]
mod solution_codec;

#[path = "parity/minus_operator_adapter.rs"]
mod minus_operator_adapter;

#[path = "parity/distinct_operator_adapter.rs"]
mod distinct_operator_adapter;

#[path = "parity/expression_eval_adapter.rs"]
mod expression_eval_adapter;

use std::path::{Path, PathBuf};

use distinct_operator_adapter::DistinctOperatorAdapter;
use expression_eval_adapter::ExpressionEvalAdapter;
use minus_operator_adapter::MinusOperatorAdapter;
use oxrdf_backend::OxRdfBackend;
use parity_fixture_harness::{FixtureOperator, Harness, Terminal};
use sliding_window_adapter::SlidingWindowAdapter;
use tumbling_window_adapter::TumblingWindowAdapter;

/// Path from this crate to the parity-root. With the submodule layout the Rust repo lives at
/// `<parity-root>/rust`, so the fixtures are at `../../parity/fixtures`. Returns `None` in a
/// standalone cqels-rs checkout where neither `PARITY_FIXTURES` is set nor the default path
/// resolves — callers (the parity tests) skip with a printed note in that case so a normal
/// `cargo test` on cqels-rs does not fail just because the parity-root fixtures are absent.
/// Set `PARITY_FIXTURES=/abs/path/to/parity/fixtures` to opt back in.
fn fixtures_dir() -> Option<PathBuf> {
    let candidate = if let Ok(p) = std::env::var("PARITY_FIXTURES") {
        PathBuf::from(p)
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../parity/fixtures")
    };
    // Canonicalize uniformly so a misconfigured PARITY_FIXTURES also triggers
    // the skip path rather than panicking later in read_dir.
    candidate.canonicalize().ok()
}

fn case_files(subdir: &str) -> Vec<PathBuf> {
    let Some(dir) = fixtures_dir() else {
        return Vec::new();
    };
    let dir = dir.join(subdir);
    let mut v: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".case.json"))
        .collect();
    v.sort();
    v
}

/// Skip-with-explanation helper for the three parity tests. Returns `true` if the parity-root
/// fixtures aren't reachable from this checkout; callers `return` immediately so `cargo test`
/// shows the test as passing (cargo has no "skipped" state for non-#[ignore] tests, but
/// printing the note keeps the rationale visible in test output).
fn skip_if_no_fixtures(test_name: &str) -> bool {
    if fixtures_dir().is_none() {
        eprintln!(
            "[parity_runner] skipping {test_name}: parity-root fixtures not found. \
             Set PARITY_FIXTURES=/abs/path/to/parity/fixtures, or run via the parity-root \
             submodule layout (https://github.com/HiveIntel/parity-root)."
        );
        true
    } else {
        false
    }
}

/// Comparator self-tests. Keep this green — it pins that the Rust side's RDFC-1.0 canonicalizer
/// matches the Java side's. Runs against an echo operator, not any real one.
#[test]
fn meta_self_tests() {
    if skip_if_no_fixtures("meta_self_tests") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("_meta");
    assert!(
        !cases.is_empty(),
        "no _meta self-tests found in parity/fixtures/_meta"
    );
    for f in cases {
        let mut echo = EchoOperator::default();
        h.run_self_test(&f, &mut echo)
            .unwrap_or_else(|e| panic!("_meta self-test failed: {} :: {e}", f.display()));
    }
}

/// Parity corpus for `graph.stream.WindowOperator` (tumbling time-window over RDF quads).
/// Runs every `*.case.json` under `parity/fixtures/window-operator/` against a fresh
/// [`TumblingWindowAdapter`] so per-case state never leaks.
#[test]
fn window_operator_parity() {
    if skip_if_no_fixtures("window_operator_parity") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("window-operator");
    assert!(!cases.is_empty(), "no window-operator cases found");
    for f in cases {
        let mut adapter = TumblingWindowAdapter::new();
        h.run_case(&f, &mut adapter)
            .unwrap_or_else(|e| panic!("parity case failed: {} :: {}", f.display(), e.0));
    }
}

/// Parity corpus for `graph.stream.SlidingWindowOperator`. Same template as
/// `window_operator_parity`, but each case carries `window_size_ms + slide_ms`
/// config and the adapter routes records to all overlapping windows.
#[test]
fn sliding_window_operator_parity() {
    if skip_if_no_fixtures("sliding_window_operator_parity") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("sliding-window");
    assert!(!cases.is_empty(), "no sliding-window cases found");
    for f in cases {
        let mut adapter = SlidingWindowAdapter::new();
        h.run_case(&f, &mut adapter)
            .unwrap_or_else(|e| panic!("parity case failed: {} :: {}", f.display(), e.0));
    }
}

/// Parity corpus for `sparql.operator.MinusOperator`. Uses the solutions-payload
/// dispatch (`run_solution_case`) rather than the quads dispatch — every case
/// under `parity/fixtures/minus-operator/` must declare `"payload_kind": "solutions"`.
#[test]
fn minus_operator_parity() {
    if skip_if_no_fixtures("minus_operator_parity") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("minus-operator");
    assert!(!cases.is_empty(), "no minus-operator cases found");
    for f in cases {
        let mut adapter = MinusOperatorAdapter::new();
        h.run_solution_case(&f, &mut adapter)
            .unwrap_or_else(|e| panic!("parity case failed: {} :: {}", f.display(), e.0));
    }
}

/// Parity corpus for `sparql.operator.Distinct` (multiset → set collapse over
/// solution mappings). Same dispatch shape as `minus_operator_parity` but with
/// `DistinctOperatorAdapter`. The corpus is partial today (autonomous Stage B
/// — LL1-safe fixtures only); literal-heavy cases follow once the
/// canonicalization-symmetry decision lands. If no cases exist yet, the test
/// no-ops with a printed skip so it doesn't fail-fast.
#[test]
fn distinct_operator_parity() {
    if skip_if_no_fixtures("distinct_operator_parity") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("distinct-operator");
    if cases.is_empty() {
        eprintln!(
            "[parity_runner] distinct_operator_parity: no fixtures under \
             parity/fixtures/distinct-operator/ yet; skipping."
        );
        return;
    }
    for f in cases {
        let mut adapter = DistinctOperatorAdapter::new();
        h.run_solution_case(&f, &mut adapter)
            .unwrap_or_else(|e| panic!("parity case failed: {} :: {}", f.display(), e.0));
    }
}

/// Unit 5: SPARQL expression evaluation. Drives the `expression` payload kind
/// (`run_expression_case`) with `ExpressionEvalAdapter` — each `do_evaluate`
/// step parses an expression string, evaluates it against a binding set, and
/// compares the canonical result. If no cases exist yet, the test no-ops with a
/// printed skip.
#[test]
fn expression_evaluation_parity() {
    if skip_if_no_fixtures("expression_evaluation_parity") {
        return;
    }
    let backend = OxRdfBackend;
    let h = Harness::new(&backend);
    let cases = case_files("expression-evaluation");
    if cases.is_empty() {
        eprintln!(
            "[parity_runner] expression_evaluation_parity: no fixtures under \
             parity/fixtures/expression-evaluation/ yet; skipping."
        );
        return;
    }
    for f in cases {
        let mut adapter = ExpressionEvalAdapter::new();
        h.run_expression_case(&f, &mut adapter)
            .unwrap_or_else(|e| panic!("parity case failed: {} :: {}", f.display(), e.0));
    }
}

/// Minimal echo operator for the `_meta` self-tests: stores everything that comes in via
/// `emit()` and flushes on `complete()`. Not a real operator — only exercises the harness.
#[derive(Default)]
struct EchoOperator {
    pending: Vec<String>,
    out: std::collections::VecDeque<String>,
    terminal: Option<Terminal>,
}

impl FixtureOperator for EchoOperator {
    fn configure(&mut self, _c: &serde_json::Value) {}
    fn emit(&mut self, _t: Option<i64>, q: &str) {
        self.pending.push(q.to_string());
    }
    fn advance_watermark(&mut self, _ms: i64) {}
    fn advance_time(&mut self, _by: i64) {}
    fn cancel(&mut self) {}
    fn complete(&mut self) {
        for p in self.pending.drain(..) {
            self.out.push_back(p);
        }
        self.terminal = Some(Terminal {
            kind: "complete".into(),
            error_type: None,
            message: None,
        });
    }
    fn error(&mut self, t: &str, m: Option<&str>) {
        self.terminal = Some(Terminal {
            kind: "error".into(),
            error_type: Some(t.into()),
            message: m.map(String::from),
        });
    }
    fn drain_emissions(&mut self) -> Vec<String> {
        self.out.drain(..).collect()
    }
    fn terminal_or_none(&self) -> Option<Terminal> {
        self.terminal.clone()
    }
}
