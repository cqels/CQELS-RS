//! Bridges `cqels_core::compiler::pipeline::apply_distinct` to the parity fixture
//! harness's [`SolutionFixtureOperator`] interface.
//!
//! Lifecycle (lazy pipeline): on the first `emit_solution` (or on `complete`),
//! the adapter constructs a `futures::channel::mpsc::unbounded` channel and
//! pipes the receiver through `apply_distinct`. The function takes ownership
//! of the input stream and returns a new stream with deduplication state owned
//! internally (an `Arc<Mutex<HashSet<String>>>` keyed on
//! `deterministic_binding_key`), so no operator instance needs to outlive the
//! pipeline — no `Box::leak`, no Arc juggling required.
//!
//! Unit 3's adapter discovery: keep the lazy-pipeline + drain_ready /
//! drain_to_end pattern (`noop_waker_ref` + `poll_next_unpin`), reuse the same
//! Solution↔BindingSet conversion helpers.
//!
//! `set_exclusions` is asserted-empty: Distinct has no right-hand side.
//! A fixture that includes `do_set_exclusions` on a Distinct case is a
//! fixture-authoring bug (silently no-ops in release builds).

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use cqels_core::compiler::pipeline::apply_distinct;
use cqels_model::BindingSet;
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{Solution, SolutionFixtureOperator, Terminal};
use crate::solution_codec::{bindingset_to_solution, solution_to_bindingset};

type SolutionStream = Pin<Box<dyn Stream<Item = BindingSet> + Send>>;

pub(crate) struct DistinctOperatorAdapter {
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl DistinctOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
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
        // apply_distinct takes ownership of the input stream and returns a new
        // stream whose dedup state is held inside an Arc<Mutex<HashSet>> owned
        // by the returned stream itself. No leak, no &'static gymnastics.
        let output = apply_distinct(Box::pin(rx));
        self.sender = Some(tx);
        self.output = Some(output);
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending` so the caller can resume later.
    ///
    /// **Load-bearing assumption**: `apply_distinct` uses `Stream::filter` with
    /// a synchronous predicate, so `Poll::Pending` means the upstream channel
    /// is empty (not "wake me later"). A future async-predicate refactor of
    /// apply_distinct would strand items here under the `noop_waker`.
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

impl SolutionFixtureOperator for DistinctOperatorAdapter {
    fn configure(&mut self, _config: &JsonValue) {
        // Distinct carries no config.
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        // Distinct has no exclusion side. A non-empty call here is a
        // fixture-authoring bug — debug_assert loudly; silent no-op in release.
        debug_assert!(
            solutions.is_empty(),
            "DistinctOperatorAdapter::set_exclusions called with {} solutions; \
             Distinct has no exclusion side — fixture script bug",
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
        if let Err(e) = sender.unbounded_send(solution_to_bindingset(solution)) {
            eprintln!("DistinctOperatorAdapter: emit_solution send failed: {e}");
        }
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec D-S4: cancellation halts emission, no terminal signal.
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
        // Spec deviation D-D1 (proposed, mirroring MinusOperator's D4): Rust's
        // `Stream<Item = BindingSet>` has no error item, so the adapter
        // records the terminal locally rather than synthesizing one through
        // the pipeline. Java-only fixture exercises this path.
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

// Conversions live in `crate::solution_codec` — shared across every
// solutions-payload adapter. See the LL1 note there for the eventual
// canonicalization-symmetry fix.
