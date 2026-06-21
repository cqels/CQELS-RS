//! Bridges `cqels_core::compiler::pipeline::apply_binds` to the parity fixture
//! harness's [`SolutionFixtureOperator`] interface (the `solutions` payload
//! kind, unit 7 — SPARQL 1.1 §10.2 `BIND(expr AS ?var)`).
//!
//! The bind program rides in `config.binds`: an ordered list of
//! `{ "expr": "<string>", "var": "<bare-name>" }`. `configure` parses each
//! `expr` with the engine's own parser ([`ExpressionParser::parse_cqelsql`])
//! into a `(Expression, String)` pair; `apply_binds` applies the whole slice
//! **sequentially** to each `BindingSet`, so a later bind reads an earlier
//! bind's output (spec B5).
//!
//! Lifecycle mirrors `DistinctOperatorAdapter`: lazy cold pipeline built on the
//! first `emit_solution` (or `complete`), an `mpsc::unbounded` channel piped
//! through `apply_binds`, drained non-blocking via `drain_ready` and to-end via
//! `drain_to_end`.
//!
//! ## Binding lift (REQUIRED)
//!
//! Solution terms parse to `Value::Term` via [`solution_codec::parse_term_string`].
//! `apply_binds`'s evaluator only does arithmetic on native `Value::Integer` /
//! `Value::Float`, so a `"2"^^xsd:integer` binding left as `Value::Term` would
//! make B4 arithmetic spuriously yield null. We lift each term through the
//! engine's own [`pipeline::term_to_value`] exactly as the production pipeline
//! and the `ExpressionEvalAdapter` (unit 5) do.
//!
//! ## Faithful present-null surfacing (CRITICAL — do not mask the bug)
//!
//! Per the spec convergence section, a pre-fix `apply_binds` inserts
//! `Value::Null` unconditionally on an error/unbound bind result. The shared
//! `solution_codec::value_to_canonical_nt` `unreachable!`s on `Value::Null`, so
//! reusing `bindingset_to_solution` would PANIC and mask the divergence. This
//! adapter instead serializes a `Value::Null` binding to an observable,
//! comparison-distinct sentinel string (`"<<unbound>>"`). The B2/B3 fixtures
//! have the var ABSENT in `expected`, so a present-null sentinel surfaces as a
//! RED multiset mismatch — that is the point. After the Rust production fix
//! (skip-null in `bind_insert`), `apply_binds` emits no null, the sentinel path
//! is never taken, and B2/B3 go green.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use cqels_core::compiler::pipeline::{apply_binds, term_to_value};
use cqels_core::expression::{Expression, ExpressionEvaluator, ExpressionParser};
use cqels_model::{BindingSet, Value};
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{Solution, SolutionFixtureOperator, Terminal};
use crate::solution_codec::{parse_term_string, value_to_canonical_nt};

type SolutionStream = Pin<Box<dyn Stream<Item = BindingSet> + Send>>;

/// Observable sentinel for a present-`Value::Null` bound output. Distinct from
/// any legal canonical N-Triples term string, so it never collides with a real
/// binding and always loses the multiset comparison against an absent variable.
const UNBOUND_SENTINEL: &str = "<<unbound>>";

pub(crate) struct BindOperatorAdapter {
    binds: Vec<(Expression, String)>,
    evaluator: ExpressionEvaluator,
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl BindOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
            binds: Vec::new(),
            evaluator: ExpressionEvaluator::new(),
            sender: None,
            output: None,
            pending: VecDeque::new(),
            terminal: None,
            cancelled: false,
        }
    }

    fn ensure_pipeline(&mut self) {
        if self.sender.is_some() || self.cancelled || self.terminal.is_some() {
            return;
        }
        let (tx, rx) = mpsc::unbounded::<BindingSet>();
        // apply_binds takes ownership of the input stream and returns a new
        // stream that folds the bind slice over each BindingSet in order.
        let output = apply_binds(Box::pin(rx), &self.binds, &self.evaluator);
        self.sender = Some(tx);
        self.output = Some(output);
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending`. `apply_binds` uses `Stream::map` with a synchronous
    /// closure, so `Poll::Pending` means the upstream channel is empty.
    fn drain_ready(&mut self) {
        let Some(output) = self.output.as_mut() else {
            return;
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        loop {
            match output.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(bs)) => {
                    self.pending.push_back(bindingset_to_solution_faithful(&bs));
                }
                Poll::Ready(None) => {
                    self.output = None;
                    return;
                }
                Poll::Pending => return,
            }
        }
    }

    fn drain_to_end(&mut self) {
        let Some(output) = self.output.take() else {
            return;
        };
        for bs in block_on_stream(output) {
            self.pending.push_back(bindingset_to_solution_faithful(&bs));
        }
    }
}

impl SolutionFixtureOperator for BindOperatorAdapter {
    fn configure(&mut self, config: &JsonValue) {
        // config.binds = ordered [ { "expr": "...", "var": "..." }, ... ].
        // Each expr is parsed with the engine's own parser; a malformed expr is
        // a fixture-authoring bug surfaced loudly (the fixture corpus is
        // hand-authored and schema-validated, so a parse failure here means the
        // fixture is wrong, not an engine behavior under test).
        let binds_json = config
            .get("binds")
            .and_then(JsonValue::as_array)
            .unwrap_or_else(|| {
                panic!("BindOperatorAdapter requires config.binds (ordered array) — fixture bug")
            });
        let mut binds = Vec::with_capacity(binds_json.len());
        for (i, item) in binds_json.iter().enumerate() {
            let expr = item
                .get("expr")
                .and_then(JsonValue::as_str)
                .unwrap_or_else(|| panic!("config.binds[{i}] missing string 'expr' — fixture bug"));
            let var = item
                .get("var")
                .and_then(JsonValue::as_str)
                .unwrap_or_else(|| panic!("config.binds[{i}] missing string 'var' — fixture bug"));
            let ast = ExpressionParser::parse_cqelsql(expr).unwrap_or_else(|e| {
                panic!("config.binds[{i}] expr {expr:?} failed to parse — fixture bug: {e:?}")
            });
            // apply_binds strips a leading ?/$ itself; pass the var through as
            // written (the fixture uses a bare name, D-B4).
            binds.push((ast, var.to_string()));
        }
        self.binds = binds;
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        // BIND has no exclusion side. A non-empty call is a fixture-authoring
        // bug — debug_assert loudly; silent no-op in release (mirrors Distinct).
        debug_assert!(
            solutions.is_empty(),
            "BindOperatorAdapter::set_exclusions called with {} solutions; \
             BIND has no exclusion side — fixture script bug",
            solutions.len()
        );
    }

    fn emit_solution(&mut self, solution: &Solution) {
        debug_assert!(
            self.terminal.is_none() && !self.cancelled,
            "emit_solution after terminal/cancel — fixture script bug"
        );
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        if let Err(e) = sender.unbounded_send(solution_to_lifted_bindingset(solution)) {
            eprintln!("BindOperatorAdapter: emit_solution send failed: {e}");
        }
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec B-S2: cancellation halts emission, no terminal signal.
        self.cancelled = true;
        self.sender = None;
        self.output = None;
    }

    fn complete(&mut self) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        self.sender = None;
        self.drain_to_end();
        self.terminal = Some(Terminal {
            kind: "complete".into(),
            error_type: None,
            message: None,
        });
    }

    fn error(&mut self, error_type: &str, message: Option<&str>) {
        // Spec deviation D-B3: Rust's Stream<Item = BindingSet> has no error
        // item, so the adapter records the terminal locally. No fixture in the
        // BIND corpus drives this path (error-on-main is Java-only).
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.sender = None;
        self.output = None;
        self.terminal = Some(Terminal {
            kind: "error".into(),
            error_type: Some(error_type.to_string()),
            message: message.map(str::to_string),
        });
    }

    fn drain_solutions(&mut self) -> Vec<Solution> {
        self.drain_ready();
        self.pending.drain(..).collect()
    }

    fn terminal_or_none(&self) -> Option<Terminal> {
        self.terminal.clone()
    }
}

/// Converts a fixture solution to a `BindingSet`, LIFTING each typed RDF literal
/// to its native `Value` variant via `term_to_value` (REQUIRED so typed-literal
/// arithmetic in B4 works — see module docs). A malformed term is a fixture bug
/// surfaced as a panic, matching `solution_codec::solution_to_bindingset`.
fn solution_to_lifted_bindingset(sol: &Solution) -> BindingSet {
    let mut bs = BindingSet::new(0);
    for (var, term_str) in sol {
        let value = parse_term_string(term_str).unwrap_or_else(|err| {
            panic!("parse failed for variable '{var}' = {term_str:?}: {err}")
        });
        match value {
            Value::Term(t) => bs.insert(var.as_str(), term_to_value(&t)),
            other => bs.insert(var.as_str(), other),
        };
    }
    bs
}

/// Serializes a `BindingSet` to the Solution wire format, FAITHFULLY surfacing a
/// present-`Value::Null` bound output as the [`UNBOUND_SENTINEL`] rather than
/// dropping it, absent-ifying it, or letting `value_to_canonical_nt` panic on
/// the null (the CRITICAL adapter rule). After the production null-skip fix no
/// null reaches here, so the sentinel arm is dead in the green state.
fn bindingset_to_solution_faithful(bs: &BindingSet) -> Solution {
    bs.iter()
        .map(|(var, val)| {
            let term = if val.is_null() {
                UNBOUND_SENTINEL.to_string()
            } else {
                value_to_canonical_nt(val)
            };
            (var.to_string(), term)
        })
        .collect()
}
