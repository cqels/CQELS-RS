//! Bridges `SlidingWindow<TimedQuad>` to the parity fixture harness's stepwise
//! [`FixtureOperator`] interface — sister of [`TumblingWindowAdapter`].
//!
//! Implementation is identical in shape to `TumblingWindowAdapter`:
//! futures channel for input, `apply_events_dedup_result` for output,
//! `poll_next_unpin + noop_waker` for non-blocking drain, `block_on_stream` on
//! complete, `StreamEvent::Error` push on error so production surfaces the
//! terminal verbatim. The only difference is the operator constructor takes
//! `(size_ms, slide_ms)` and produces overlapping windows.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use cqels_core::stream::{StreamEvent, Timestamped};
use cqels_core::window::{SlidingWindow, Window, WindowConfig, WindowError, WindowedBatch};
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use oxrdf::Quad;
use oxttl::NQuadsParser;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{FixtureOperator, Terminal};

/// Same `TimedQuad` shape as the Tumbling adapter: equality on the inner
/// `Quad` only (not `ts`), so spec B2 dedup applies regardless of per-emit
/// timestamp differences.
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

pub(crate) struct SlidingWindowAdapter {
    sender: Option<mpsc::UnboundedSender<StreamEvent<TimedQuad>>>,
    output: Option<BatchStream>,
    pending: VecDeque<String>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl SlidingWindowAdapter {
    pub(crate) fn new() -> Self {
        Self {
            sender: None,
            output: None,
            pending: VecDeque::new(),
            terminal: None,
            cancelled: false,
        }
    }

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

impl FixtureOperator for SlidingWindowAdapter {
    fn configure(&mut self, config: &JsonValue) {
        let size_ms = config
            .get("window_size_ms")
            .and_then(|v| v.as_i64())
            .expect("window_size_ms required");
        let slide_ms = config
            .get("slide_ms")
            .and_then(|v| v.as_i64())
            .expect("slide_ms required for SlidingWindow");
        let lateness = config
            .get("allowed_lateness_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let (tx, rx) = mpsc::unbounded::<StreamEvent<TimedQuad>>();
        self.sender = Some(tx);

        let window = SlidingWindow::new(
            Duration::from_millis(size_ms as u64),
            Duration::from_millis(slide_ms as u64),
        );
        self.output =
            Some(window.apply_events_dedup_result(WindowConfig::new(lateness), Box::pin(rx)));
    }

    fn emit(&mut self, event_time_ms: Option<i64>, nquads: &str) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        let ts = event_time_ms.expect("SlidingWindow requires event_time_ms on every emit");
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

    fn advance_time(&mut self, _by_ms: i64) {}

    fn cancel(&mut self) {
        self.cancelled = true;
        self.sender = None;
        self.output = None;
    }

    fn complete(&mut self) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
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
        if let Some(sender) = self.sender.as_ref() {
            let _ = sender.unbounded_send(StreamEvent::error(
                error_type.to_string(),
                message.map(|s| s.to_string()),
            ));
        }
        self.sender = None;
        // See TumblingWindowAdapter::error for the rationale: no local
        // terminal fallback, no pending.clear() — let production's Err
        // surface (or its absence) be visible to the harness.
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
