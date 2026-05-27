//! Bridges `cqels_core::operator::minus::MinusOperator` to the parity fixture harness's
//! [`SolutionFixtureOperator`] interface.
//!
//! Lifecycle (lazy pipeline): exclusions accumulate via `set_exclusions`; on the first
//! `emit_solution` (or on `complete`), the adapter constructs an `Arc<MinusOperator>` plus a
//! `futures::channel::mpsc::unbounded` channel and inlines the body of
//! [`MinusOperator::apply`] (a `Stream::filter` over [`MinusOperator::is_excluded`]) so the
//! operator can be cloned into the filter closure without needing a `&'static` borrow. The
//! parity surface is the SPARQL 1.1 §8.3 compatibility check inside `is_excluded`, not the
//! `apply` call site — both are exercised here.
//!
//! Term I/O: each fixture solution carries variable→N-Triples-style strings. Parsing wraps
//! each term in a stub triple `<urn:s> <urn:p> {term} .` and runs `oxttl::NTriplesParser`,
//! producing an `oxrdf::Term`. The existing `From<oxrdf::Term>` for `cqels_model::Term`
//! folds it into the model type; we wrap in `Value::Term` to retain the canonical form.
//!
//! `xsd:string`-typed literals are emitted as bare `"foo"` rather than
//! `"foo"^^<http://...XMLSchema#string>` so the round-trip lines up with the RDF 1.1
//! N-Triples canonical form used by the fixture authors.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use cqels_core::operator::minus::MinusOperator;
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

pub(crate) struct MinusOperatorAdapter {
    exclusions: Vec<BindingSet>,
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl MinusOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
            exclusions: Vec::new(),
            sender: None,
            output: None,
            pending: VecDeque::new(),
            terminal: None,
            cancelled: false,
        }
    }

    /// Construct the pipeline on demand. Empty-exclusion (spec M4) fixtures
    /// share this code path so the first `emit_solution` (or `complete`)
    /// flips an idle adapter into a running one.
    fn ensure_pipeline(&mut self) {
        if self.sender.is_some() || self.cancelled || self.terminal.is_some() {
            return;
        }
        let (tx, rx) = mpsc::unbounded::<BindingSet>();
        // `MinusOperator::apply` borrows `&'a self`, which forces the operator
        // to live as long as the returned stream. Rather than `Box::leak`-ing
        // for a `'static` borrow, hold the operator in an `Arc` and inline
        // `apply`'s body — `Stream::filter` over `is_excluded`. Same code path
        // is exercised; the Arc drops with the closure when the stream ends.
        let op = Arc::new(MinusOperator::excluding(self.exclusions.clone()));
        let op_for_filter = Arc::clone(&op);
        let filtered = rx.filter(move |bs| futures::future::ready(!op_for_filter.is_excluded(bs)));
        self.sender = Some(tx);
        self.output = Some(Box::pin(filtered));
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending` so the caller can resume later.
    ///
    /// **Load-bearing assumption**: `MinusOperator`'s filter predicate is
    /// synchronous (`futures::future::ready(...)`), so every queued item on
    /// the upstream channel resolves to `Poll::Ready(Some(_))` in a single
    /// poll under the `noop_waker`. A `Poll::Pending` therefore means the
    /// upstream channel is empty, not that we owe a wakeup. If a future
    /// operator change ever introduces an async predicate, this loop will
    /// silently strand items in Pending — replace `noop_waker_ref` with a
    /// proper executor at that point.
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

    /// Synchronously drain everything after `complete` has closed the input.
    fn drain_to_end(&mut self) {
        let Some(output) = self.output.take() else {
            return;
        };
        for bs in block_on_stream(output) {
            self.pending.push_back(bindingset_to_solution(&bs));
        }
    }
}

impl SolutionFixtureOperator for MinusOperatorAdapter {
    fn configure(&mut self, _config: &JsonValue) {
        // MinusOperator carries no config (spec D3: Java's Builder API is style-only).
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        debug_assert!(
            self.sender.is_none(),
            "set_exclusions called after the pipeline was constructed — \
             the leaked-into-closure MinusOperator already snapshotted the \
             old exclusions and new values would be silently ignored. \
             The harness should reject this at script level."
        );
        self.exclusions = solutions.iter().map(solution_to_bindingset).collect();
    }

    fn emit_solution(&mut self, solution: &Solution) {
        debug_assert!(
            self.terminal.is_none() && !self.cancelled,
            "emit_solution called after a terminal (complete/error) or cancel — \
             fixture script bug. Adapter silently no-ops in release builds."
        );
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        if let Err(e) = sender.unbounded_send(solution_to_bindingset(solution)) {
            // The receiver lives inside our own `self.output` filter stream;
            // a send failure here means the operator dropped its receiver
            // unexpectedly. Surface to test output so the diagnostic isn't
            // hidden behind a later multiset mismatch.
            eprintln!("MinusOperatorAdapter: emit_solution send failed: {e}");
        }
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec M-S4: cancellation halts emission, no terminal signal.
        self.cancelled = true;
        self.sender = None;
        self.output = None;
    }

    fn complete(&mut self) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        // Drop the sender so the filter stream sees EOF, then drain.
        self.sender = None;
        self.drain_to_end();
        self.terminal = Some(Terminal {
            kind: "complete".into(),
            error_type: None,
            message: None,
        });
    }

    fn error(&mut self, error_type: &str, message: Option<&str>) {
        // Spec M-S2: error on the main stream propagates verbatim. Rust's
        // `Stream<Item = BindingSet>` has no error item, so the adapter
        // records the terminal locally and tears down the pipeline. Unconsumed
        // pending emissions are intentionally left for the harness's
        // end-of-script exhaustiveness check to flag.
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
