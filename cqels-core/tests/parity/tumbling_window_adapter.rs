//! Bridges `TumblingWindow<TimedQuad>` to the parity fixture harness's stepwise
//! [`FixtureOperator`] interface.
//!
//! The adapter is a thin live-channel bridge over the production
//! [`Window::apply_events_dedup_result`] API (added by T3 + B2 + B6):
//!
//!   1. `configure` opens an unbounded futures channel and hands its receiver
//!      to `window.apply_events_dedup_result(config, ...)`, storing the
//!      `Stream<Result<WindowedBatch<T>, WindowError>>` output.
//!   2. `emit` / `advance_watermark` push `StreamEvent`s into the channel.
//!   3. After every push, the adapter non-blockingly polls the output
//!      (`poll_next_unpin` + noop waker). Ok batches become pending N-Quads;
//!      Err items become the terminal — adopted from production verbatim
//!      (spec B6 closes the fixture-gap from codex T6 re-run).
//!   4. `complete` drops the channel sender (triggering FLUSH-ON-COMPLETE
//!      in production) and synchronously drains the rest.
//!   5. `cancel` drops the sender + output without draining (spec B8).
//!   6. `error` pushes `StreamEvent::Error` through the channel; production
//!      surfaces it as the next `Err(WindowError)` item, which `drain_ready`
//!      adopts as the terminal. The adapter no longer synthesizes errors.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use cqels_core::stream::{StreamEvent, Timestamped};
use cqels_core::window::{TumblingWindow, Window, WindowConfig, WindowError, WindowedBatch};
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use oxrdf::Quad;
use oxttl::NQuadsParser;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{FixtureOperator, Terminal};

/// Minimal `Timestamped` wrapper around `oxrdf::Quad` — the adapter's stream payload.
///
/// Equality and hashing intentionally ignore `ts` and compare only the inner
/// [`Quad`] (RDF term-equality on subject / predicate / object / graph) so that
/// dedup via [`Window::apply_events_dedup`] matches spec B2's "set of quads in
/// a window" semantics. Without this, two emits of the same quad with
/// different per-emit timestamps would *not* collapse — which is what the
/// `duplicate-quad-dedup` fixture explicitly forbids.
#[derive(Clone)]
struct TimedQuad {
    quad: Quad,
    ts: i64,
}

impl Timestamped for TimedQuad {
    fn timestamp(&self) -> i64 {
        self.ts
    }
}

impl PartialEq for TimedQuad {
    fn eq(&self, other: &Self) -> bool {
        self.quad == other.quad
    }
}
impl Eq for TimedQuad {}
impl std::hash::Hash for TimedQuad {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.quad.hash(state);
    }
}

type BatchStream =
    Pin<Box<dyn Stream<Item = Result<WindowedBatch<TimedQuad>, WindowError>> + Send>>;

pub(crate) struct TumblingWindowAdapter {
    sender: Option<mpsc::UnboundedSender<StreamEvent<TimedQuad>>>,
    output: Option<BatchStream>,
    pending: VecDeque<String>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl TumblingWindowAdapter {
    pub(crate) fn new() -> Self {
        Self {
            sender: None,
            output: None,
            pending: VecDeque::new(),
            terminal: None,
            cancelled: false,
        }
    }

    /// Pull anything immediately ready from the output stream into `pending`
    /// (Ok batches) or `terminal` (Err items, observed verbatim from
    /// production). Stops at the first Pending poll so this never blocks.
    fn drain_ready(&mut self) {
        let Some(output) = self.output.as_mut() else {
            return;
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        loop {
            match output.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.pending.push_back(batch_to_nquads(&batch));
                }
                Poll::Ready(Some(Err(err))) => {
                    // Spec B6: production has surfaced an upstream error.
                    // Adopt it as our terminal — kind/message verbatim.
                    if self.terminal.is_none() && !self.cancelled {
                        self.terminal = Some(Terminal {
                            kind: "error".into(),
                            error_type: Some(err.kind),
                            message: err.message,
                        });
                    }
                }
                Poll::Ready(None) => {
                    self.output = None;
                    return;
                }
                Poll::Pending => return,
            }
        }
    }

    /// Synchronously drain everything from the output stream after the input
    /// has been closed. Used on `complete()` to collect any FLUSH-ON-COMPLETE
    /// batches before setting the terminal.
    fn drain_to_end(&mut self) {
        let Some(output) = self.output.take() else {
            return;
        };
        for item in block_on_stream(output) {
            match item {
                Ok(batch) => self.pending.push_back(batch_to_nquads(&batch)),
                Err(err) => {
                    if self.terminal.is_none() && !self.cancelled {
                        self.terminal = Some(Terminal {
                            kind: "error".into(),
                            error_type: Some(err.kind),
                            message: err.message,
                        });
                    }
                }
            }
        }
    }
}

fn batch_to_nquads(batch: &WindowedBatch<TimedQuad>) -> String {
    use std::fmt::Write;
    let mut nq = String::new();
    for el in &batch.elements {
        // oxrdf::Quad Display omits the trailing `.` — N-Quads needs it on every line.
        writeln!(nq, "{} .", el.quad).expect("write to String never fails");
    }
    nq
}

impl FixtureOperator for TumblingWindowAdapter {
    fn configure(&mut self, config: &JsonValue) {
        let size_ms = config
            .get("window_size_ms")
            .and_then(|v| v.as_i64())
            .expect("window_size_ms required");
        let lateness = config
            .get("allowed_lateness_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let (tx, rx) = mpsc::unbounded::<StreamEvent<TimedQuad>>();
        self.sender = Some(tx);

        let window = TumblingWindow::new(Duration::from_millis(size_ms as u64));
        // apply_events_dedup_result: per-window set semantics (B2) AND
        // observable terminal error (B6 — the stream yields Err(WindowError)
        // when upstream signals an error, so the adapter doesn't have to
        // synthesize the terminal locally). TimedQuad's PartialEq + Hash
        // compare on the inner Quad only.
        self.output =
            Some(window.apply_events_dedup_result(WindowConfig::new(lateness), Box::pin(rx)));
    }

    fn emit(&mut self, event_time_ms: Option<i64>, nquads: &str) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        let ts = event_time_ms.expect("TumblingWindow requires event_time_ms on every emit");
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        for quad in NQuadsParser::new().for_reader(nquads.as_bytes()) {
            let q = quad.expect("fixture N-Quads must parse");
            let _ = sender.unbounded_send(StreamEvent::record(TimedQuad { quad: q, ts }, ts));
        }
        self.drain_ready();
    }

    fn advance_watermark(&mut self, ms: i64) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        if let Some(sender) = self.sender.as_ref() {
            let _ = sender.unbounded_send(StreamEvent::watermark(ms));
        }
        self.drain_ready();
    }

    fn advance_time(&mut self, _by_ms: i64) {
        // Event-time operator — wall-clock advance has no effect.
    }

    fn cancel(&mut self) {
        self.cancelled = true;
        // Drop the sender so the operator sees EOF, but don't drain any
        // FLUSH-ON-COMPLETE batches — spec B8 says cancellation halts
        // emission, no terminal signal.
        self.sender = None;
        self.output = None;
    }

    fn complete(&mut self) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        // Drop the sender to signal upstream completion; apply_events flushes
        // any open windows in ascending order (spec B4 FLUSH-ON-COMPLETE).
        self.sender = None;
        self.drain_to_end();
        self.terminal = Some(Terminal {
            kind: "complete".into(),
            error_type: None,
            message: None,
        });
    }

    fn error(&mut self, error_type: &str, message: Option<&str>) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        // Spec B6: push StreamEvent::Error into the channel. The production
        // operator (TumblingTimeWindowEventStream) clears its open-bucket
        // state and emits exactly one Err(WindowError { kind, message })
        // on the output stream, then ends with None — no FLUSH-ON-COMPLETE.
        //
        // drain_ready picks up the Err item and adopts it as the terminal,
        // so the terminal kind/message come from PRODUCTION's response to
        // our error event, not from a local fallback. This closes codex T6
        // B6 fixture-gap: the production code is genuinely exercised.
        if let Some(sender) = self.sender.as_ref() {
            let _ = sender.unbounded_send(StreamEvent::error(
                error_type.to_string(),
                message.map(|s| s.to_string()),
            ));
        }
        self.sender = None;
        // drain_ready pulls the Err(WindowError) production emits in response
        // and adopts it as `self.terminal` verbatim. If production fails to
        // surface a terminal (e.g. silently dropped the error event), we
        // intentionally do NOT fall back to a local terminal — the harness's
        // `expect: terminal` step in error-propagates.case.json will then
        // fail loudly, which is the correct outcome. Codex local-review
        // tumbling-round-N flagged the previous fallback as adapter-side
        // masking that could hide a production regression.
        //
        // Likewise, `pending` is not cleared: if production incorrectly
        // flushed an open window before observing the error, those batches
        // remain visible and the harness's exhaustiveness check at script
        // end will flag the leak.
        self.drain_ready();
        self.output = None;
    }

    fn drain_emissions(&mut self) -> Vec<String> {
        self.drain_ready();
        self.pending.drain(..).collect()
    }

    fn terminal_or_none(&self) -> Option<Terminal> {
        self.terminal.clone()
    }
}
