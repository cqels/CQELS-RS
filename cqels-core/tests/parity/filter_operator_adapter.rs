//! Bridges `cqels_core::compiler::pipeline::apply_filters` to the parity
//! fixture harness's [`SolutionFixtureOperator`] interface (unit 6).
//!
//! Lifecycle (lazy pipeline): on the first `emit_solution` (or on `complete`),
//! the adapter constructs a `futures::channel::mpsc::unbounded` channel and
//! pipes the receiver through `apply_filters(stream, &[predicate],
//! &evaluator)`. Same lazy-pipeline + drain_ready/drain_to_end pattern as the
//! Distinct adapter.
//!
//! Parity surface (spec D-F1): the closure-bound
//! `cqels_core::operator::filter::FilterOperator<F>` cannot consume a
//! string-typed predicate, so the adapter goes through the pipeline-level
//! `apply_filters` — the same function the SPARQL compiler emits when
//! lowering a SELECT … FILTER. Cross-engine behavior is locked at the
//! evaluator-driven path, which mirrors Java's
//! `FilterOperator(ctx, /*enableSpatialPushdown=*/ false)`.
//!
//! `set_exclusions` is asserted-empty: FILTER has no right-hand side.
//! A fixture that includes `do_set_exclusions` on a Filter case is a
//! fixture-authoring bug (silently no-ops in release builds).
//!
//! Mirrors `java/.../parity/fixtures/FilterOperatorAdapter.java`.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use cqels_core::compiler::pipeline::{apply_filters, term_to_value};
use cqels_core::expression::{Expression, ExpressionEvaluator, ExpressionParser};
use cqels_model::{BindingSet, Value};
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{Solution, SolutionFixtureOperator, Terminal};
use crate::solution_codec::{bindingset_to_solution, parse_term_string};

type SolutionStream = Pin<Box<dyn Stream<Item = BindingSet> + Send>>;

pub(crate) struct FilterOperatorAdapter {
    predicate: Option<Expression>,
    evaluator: ExpressionEvaluator,
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl FilterOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
            predicate: None,
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
        let predicate = self
            .predicate
            .clone()
            .expect("FilterOperatorAdapter pipeline started before configure(filter=…) ran");
        let (tx, rx) = mpsc::unbounded::<BindingSet>();
        let filters = vec![predicate];
        let output: SolutionStream = apply_filters(Box::pin(rx), &filters, &self.evaluator);
        self.sender = Some(tx);
        self.output = Some(output);
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending` so the caller can resume later.
    ///
    /// **Load-bearing assumption** (same as Distinct adapter): `apply_filters`
    /// uses `Stream::filter` with a synchronous predicate, so `Poll::Pending`
    /// means the upstream channel is empty (not "wake me later").
    fn drain_ready(&mut self) {
        let Some(output) = self.output.as_mut() else {
            return;
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        loop {
            match output.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(bs)) => {
                    self.pending.push_back(bindingset_to_solution(&bs));
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
            self.pending.push_back(bindingset_to_solution(&bs));
        }
    }
}

impl SolutionFixtureOperator for FilterOperatorAdapter {
    fn configure(&mut self, config: &JsonValue) {
        // The shared filter corpus keys the predicate under `config.predicate`
        // (matching the parity-root fixtures + the Java adapter), NOT
        // `config.filter` — reading the wrong key panicked on every real
        // fixture, masked only because standalone CI has no corpus to run.
        let filter_str = config
            .get("predicate")
            .and_then(JsonValue::as_str)
            .expect("FilterOperatorAdapter requires config.predicate (non-empty string)");
        assert!(
            !filter_str.trim().is_empty(),
            "FilterOperatorAdapter requires config.predicate to be non-empty"
        );
        let ast = ExpressionParser::parse_cqelsql(filter_str)
            .unwrap_or_else(|e| panic!("failed to parse FILTER expression {filter_str:?}: {e}"));
        self.predicate = Some(ast);
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        debug_assert!(
            solutions.is_empty(),
            "FilterOperatorAdapter::set_exclusions called with {} solutions; \
             FILTER has no exclusion side — fixture script bug",
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
        // Lift typed RDF literals to native Value variants (Integer/Float/etc)
        // before sending downstream — without this, the evaluator's arithmetic
        // and comparison paths (which only operate on the native variants)
        // would see Value::Term(Literal) and spuriously yield null. Same
        // recipe as ExpressionEvalAdapter; the shared `solution_to_bindingset`
        // is term-equality-only and is reused by Minus/Distinct unchanged.
        let bs = lift_solution(solution);
        if let Err(e) = sender.unbounded_send(bs) {
            eprintln!("FilterOperatorAdapter: emit_solution send failed: {e}");
        }
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec F-S2: cancellation halts emission, no terminal signal.
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
        // Spec D-F3: M-S2-equivalent is Java-only. Rust's
        // Stream<Item = BindingSet> has no error item, so the adapter records
        // the terminal locally rather than synthesizing one through the pipeline.
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

/// Parses each solution binding through `parse_term_string` and lifts a
/// resulting `Value::Term` to the native numeric/boolean/string variants the
/// evaluator operates on via `term_to_value`. Other parse results (already-
/// native Values produced by the codec) pass through unchanged. A malformed
/// term-string is a fixture-authoring bug — panic loudly so it surfaces in
/// the failing-case diagnostic rather than silently skipping the row.
///
/// Mirrors the lifting in `ExpressionEvalAdapter::evaluate`. When a third
/// evaluator-driven solutions-payload unit lands (Bind, HAVING), this should
/// migrate to a shared helper in `solution_codec`.
fn lift_solution(solution: &Solution) -> BindingSet {
    let mut bs = BindingSet::new(0);
    for (var, term_str) in solution {
        let lifted = match parse_term_string(term_str) {
            Ok(Value::Term(t)) => term_to_value(&t),
            Ok(other) => other,
            Err(err) => panic!("parse failed for variable '{var}' = {term_str:?}: {err}"),
        };
        bs.insert(var.as_str(), lifted);
    }
    bs
}
