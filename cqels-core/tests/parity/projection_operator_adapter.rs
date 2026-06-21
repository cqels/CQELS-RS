//! Bridges `cqels_core::compiler::pipeline::apply_projection` to the parity
//! fixture harness's [`SolutionFixtureOperator`] interface.
//!
//! Lifecycle (lazy pipeline): on the first `emit_solution` (or on `complete`),
//! the adapter constructs a `futures::channel::mpsc::unbounded` channel and
//! pipes the receiver through `apply_projection(stream, &project_vars)`. The
//! function takes ownership of the input stream and returns a new stream that
//! restricts each binding set to the configured variable list — stateless, no
//! seen-set, no Arc juggling. Same drain_ready / drain_to_end pattern as the
//! Distinct adapter.
//!
//! **Projection vars** ride in `config.project` (ordered bare variable names).
//! The adapter strips a leading `?`/`$` defensively; `apply_projection` also
//! strips internally, so a `?name` fixture would still work, but fixtures use
//! the bare form per spec.
//!
//! **Raw terms, no `term_to_value` lift.** `apply_projection` clones the
//! existing `Value` for each retained variable (`bs.get(var).clone()`); it does
//! no expression evaluation. The codec parses fixture term-strings into
//! `Value::Term`, projection keeps those `Value::Term`s, and
//! `value_to_canonical_nt` canonicalizes them — so the raw-term path through
//! `solution_to_bindingset` is the faithful production input here. (Contrast
//! the expression-eval unit, which needs the native-value lift.)
//!
//! `set_exclusions` is asserted-empty: Projection has no right-hand side.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use cqels_core::compiler::pipeline::apply_projection;
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

pub(crate) struct ProjectionOperatorAdapter {
    project_vars: Vec<String>,
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl ProjectionOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
            project_vars: Vec::new(),
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
        // apply_projection takes ownership of the input stream and returns a new
        // stream that restricts each binding set to the configured vars. It's
        // stateless, so no leak / &'static gymnastics — same shape as Distinct.
        let output = apply_projection(Box::pin(rx), &self.project_vars);
        self.sender = Some(tx);
        self.output = Some(output);
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending` so the caller can resume later.
    ///
    /// **Load-bearing assumption**: `apply_projection` uses `Stream::map` with a
    /// synchronous closure, so `Poll::Pending` means the upstream channel is
    /// empty (not "wake me later"). Mirrors the Distinct adapter's noop-waker
    /// drain contract.
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

impl SolutionFixtureOperator for ProjectionOperatorAdapter {
    fn configure(&mut self, config: &JsonValue) {
        // `config.project`: ordered list of bare variable names. Strip a leading
        // ?/$ defensively (apply_projection also strips, so this is belt-and-
        // suspenders). A missing/empty list projects to the empty solution,
        // matching apply_projection's &[] behavior.
        self.project_vars = config
            .get("project")
            .and_then(JsonValue::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(JsonValue::as_str)
                    .map(|v| {
                        v.strip_prefix('?')
                            .or_else(|| v.strip_prefix('$'))
                            .unwrap_or(v)
                            .to_string()
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        // Projection has no exclusion side. A non-empty call here is a
        // fixture-authoring bug — debug_assert loudly; silent no-op in release.
        debug_assert!(
            solutions.is_empty(),
            "ProjectionOperatorAdapter::set_exclusions called with {} solutions; \
             Projection has no exclusion side — fixture script bug",
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
            eprintln!("ProjectionOperatorAdapter: emit_solution send failed: {e}");
        }
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec P-S2: cancellation halts emission, no terminal signal.
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
        // Spec deviation D-P2 (mirroring DistinctOperator D-D2): Rust's
        // `Stream<Item = BindingSet>` has no error item, so the adapter records
        // the terminal locally rather than synthesizing one through the
        // pipeline. No fixture exercises this path.
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
