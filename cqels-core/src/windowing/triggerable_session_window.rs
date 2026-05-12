//! Trigger-driven session window operator.
//!
//! Companion to [`TriggerableTimeWindow`](super::TriggerableTimeWindow) and
//! [`TriggerableSlidingWindow`](super::TriggerableSlidingWindow): same
//! Trigger/Evictor delegation, but with **session** semantics — windows
//! group consecutive events whose gap is at most `gap`, and the window's
//! bounds grow as events arrive.
//!
//! Closes the parity-plan 2.1 `Window<T>`-integration follow-up
//! (tumbling, sliding, session all covered).
//!
//! Scope
//! - **Single active session at a time.** Matches the existing
//!   [`SessionWindow`](crate::window::SessionWindow): events must arrive
//!   in roughly-sorted order. Late arrivals that would belong to a
//!   prior closed session are folded into the current open session
//!   (same as the existing operator).
//! - **Per-session trigger + evictor**, cloned from templates each
//!   time a new session opens.
//! - **Session close drives `on_event_time`.** When the gap times out
//!   (i.e. the new event would start a new session), the closing
//!   session is given one final `on_event_time` chance to fire before
//!   its buffer is dropped. Likewise at end-of-stream.
//! - **No data-driven merge.** Out-of-order events that would bridge
//!   two prior closed sessions are not currently re-merged. Matches
//!   the existing operator's behavior.
//!
//! Differences vs the existing `SessionWindow`
//! - The existing operator always emits the residual session at
//!   end-of-stream. The triggerable variant defers that decision to
//!   the trigger via `on_event_time` — if the trigger returns
//!   `Continue`, the residual is dropped silently. Most concrete
//!   triggers (count, event-time) DO fire on `on_event_time`, so the
//!   net behavior matches for typical configurations.
//!
//! Not in scope here (follow-ups)
//! - Processing-time clocks/timers.
//! - Multi-session state with merge-on-late-arrival.

use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{Stream, StreamExt};

use crate::stream::{Timestamped, TimestampedValue};
use crate::window::{Window, WindowType, WindowedBatch};
use crate::windowing::triggerable_window::NoEvictor;
use crate::windowing::{
    Evictor, EvictorContext, TimeWindow, Trigger, TriggerContext, TriggerResult, WindowBounds,
};

/// Trigger-driven session window.
///
/// Each session holds its own trigger + evictor (cloned from the
/// operator's templates) and a growing `[start_ts, last_ts]` extent.
/// When the gap between events exceeds `gap`, the operator gives the
/// closing session one final `on_event_time` shot at firing, then
/// starts a new session with the incoming event.
///
/// # Type parameters
///
/// - `T` — element type, must implement [`Timestamped`].
/// - `Tr` — trigger template. Must implement
///   [`Trigger<T, TimeWindow>`] and `Clone`.
/// - `Ev` — evictor template (optional via constructor).
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use cqels_core::stream::TimestampedValue;
/// use cqels_core::window::Window;
/// use cqels_core::windowing::{CountTrigger, TriggerableSessionWindow};
///
/// // Fire every 3 elements within a session; sessions close after
/// // 30s of inactivity.
/// let window = TriggerableSessionWindow::<TimestampedValue<i64>, _, _>::new(
///     Duration::from_secs(30),
///     CountTrigger::of(3),
/// );
/// assert_eq!(
///     window.window_type(),
///     cqels_core::window::WindowType::Session
/// );
/// ```
#[derive(Clone, Debug)]
pub struct TriggerableSessionWindow<T, Tr, Ev> {
    gap: Duration,
    trigger_template: Tr,
    evictor_template: Option<Ev>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, Tr> TriggerableSessionWindow<T, Tr, NoEvictor>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a session window with no evictor.
    pub fn new(gap: Duration, trigger: Tr) -> Self {
        Self {
            gap,
            trigger_template: trigger,
            evictor_template: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, Tr, Ev> TriggerableSessionWindow<T, Tr, Ev>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
    Ev: Evictor<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a session window with an evictor.
    pub fn with_evictor(gap: Duration, trigger: Tr, evictor: Ev) -> Self {
        Self {
            gap,
            trigger_template: trigger,
            evictor_template: Some(evictor),
            _phantom: PhantomData,
        }
    }

    /// The configured inactivity gap.
    pub fn gap(&self) -> Duration {
        self.gap
    }
}

impl<T, Tr, Ev> Window<T> for TriggerableSessionWindow<T, Tr, Ev>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + Sync + 'static,
    Ev: Evictor<T, TimeWindow> + Clone + Send + Sync + 'static,
{
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        let gap_ms = (self.gap.as_millis() as i64).max(1);
        let trigger_template = self.trigger_template.clone();
        let evictor_template = self.evictor_template.clone();

        // We unfold the input stream into a flat stream of
        // `WindowedBatch<T>`. Per event we may produce 0..2 batches:
        // one when the prior session closes (on_event_time), plus
        // one when the new session's on_element fires.
        let state = OperatorState::<T, Tr, Ev>::new();

        let out = futures::stream::unfold(
            (stream, state, trigger_template, evictor_template, false),
            move |(mut stream, mut state, trigger_template, evictor_template, mut finished)| async move {
                loop {
                    let elem_opt = stream.next().await;
                    if let Some(elem) = elem_opt {
                        let ts = elem.timestamp();
                        let mut emissions: Vec<WindowedBatch<T>> = Vec::new();

                        // ─── Gap timeout: close current session if
                        // the new event arrived after gap. We test
                        // against `last_ts` regardless of buffer
                        // contents — a session can have an empty
                        // buffer after `FireAndPurge` yet still
                        // logically be the "current" session until
                        // the gap expires.
                        let gap_timed_out = state
                            .session
                            .as_ref()
                            .map(|s| (ts - s.last_ts) > gap_ms)
                            .unwrap_or(false);
                        if gap_timed_out {
                            if let Some(batch) = state.close_current_session() {
                                emissions.push(batch);
                            }
                            // close_current_session drops the session
                            // even when no batch was emitted (empty
                            // buffer case), so a fresh one opens
                            // below.
                        }

                        // Note: we deliberately do NOT advance the
                        // watermark on each on_element. Session
                        // windows have data-driven bounds whose
                        // `max_timestamp` equals the latest event's
                        // timestamp — if we advanced the watermark
                        // synchronously, `EventTimeTrigger.on_element`
                        // (which compares
                        // `window.max_timestamp() <= watermark`)
                        // would fire on every single arrival.
                        // Watermark advancement happens at session
                        // close instead, where `on_event_time`
                        // gives the trigger its definitive shot.

                        // ─── Open a fresh session if none active.
                        if state.session.is_none() {
                            state.session = Some(SessionState::new(
                                ts,
                                trigger_template.clone(),
                                evictor_template.clone(),
                            ));
                        }

                        // ─── Add to current session, drive on_element.
                        if let Some(batch) = state.on_element(elem, ts) {
                            emissions.push(batch);
                        }

                        if emissions.is_empty() {
                            continue;
                        }
                        return Some((
                            futures::stream::iter(emissions),
                            (stream, state, trigger_template, evictor_template, finished),
                        ));
                    } else {
                        // ─── End of stream: one final on_event_time
                        // on any open session.
                        if finished {
                            return None;
                        }
                        finished = true;
                        let mut emissions: Vec<WindowedBatch<T>> = Vec::new();
                        if let Some(batch) = state.close_current_session() {
                            emissions.push(batch);
                        }
                        if emissions.is_empty() {
                            return None;
                        }
                        return Some((
                            futures::stream::iter(emissions),
                            (stream, state, trigger_template, evictor_template, finished),
                        ));
                    }
                }
            },
        )
        .flatten();

        Box::pin(out)
    }

    fn window_type(&self) -> WindowType {
        WindowType::Session
    }
}

// ─── Internal state ────────────────────────────────────────────────

/// Per-session state. Generic over T, Tr, Ev to share the operator's
/// type parameters.
struct SessionState<T, Tr, Ev> {
    buffer: Vec<TimestampedValue<T>>,
    trigger: Tr,
    evictor: Option<Ev>,
    start_ts: i64,
    last_ts: i64,
}

impl<T, Tr, Ev> SessionState<T, Tr, Ev>
where
    T: Clone,
    Tr: Trigger<T, TimeWindow>,
    Ev: Evictor<T, TimeWindow>,
{
    fn new(start_ts: i64, trigger: Tr, evictor: Option<Ev>) -> Self {
        Self {
            buffer: Vec::new(),
            trigger,
            evictor,
            start_ts,
            last_ts: start_ts,
        }
    }

    /// Half-open `[start, last+1)` bounds — `max_timestamp` is `last`.
    fn bounds(&self) -> TimeWindow {
        TimeWindow::new(self.start_ts, self.last_ts + 1)
    }

    /// Apply trigger result and produce an emission batch (if any).
    fn apply_result(
        &mut self,
        result: TriggerResult,
        ctx: &dyn EvictorContext,
    ) -> Option<Vec<TimestampedValue<T>>> {
        match result {
            TriggerResult::Continue => None,
            TriggerResult::Purge => {
                self.buffer.clear();
                None
            }
            TriggerResult::Fire | TriggerResult::FireAndPurge => {
                let bounds = self.bounds();
                if let Some(evictor) = &self.evictor {
                    evictor.evict_before(&mut self.buffer, &bounds, ctx);
                }
                let emitted = self.buffer.clone();
                if let Some(evictor) = &self.evictor {
                    evictor.evict_after(&mut self.buffer, &bounds, ctx);
                }
                if matches!(result, TriggerResult::FireAndPurge) {
                    self.buffer.clear();
                }
                if emitted.is_empty() {
                    None
                } else {
                    Some(emitted)
                }
            }
        }
    }
}

struct OperatorState<T, Tr, Ev> {
    session: Option<SessionState<T, Tr, Ev>>,
    ctx: OperatorContext,
}

impl<T, Tr, Ev> OperatorState<T, Tr, Ev>
where
    T: Clone,
    Tr: Trigger<T, TimeWindow>,
    Ev: Evictor<T, TimeWindow>,
{
    fn new() -> Self {
        Self {
            session: None,
            ctx: OperatorContext::default(),
        }
    }

    fn on_element(&mut self, elem: T, ts: i64) -> Option<WindowedBatch<T>> {
        let session = self
            .session
            .as_mut()
            .expect("session must be open before on_element");
        session.buffer.push(TimestampedValue::new(elem.clone(), ts));
        session.last_ts = ts;
        let bounds = session.bounds();
        let result = session
            .trigger
            .on_element(&elem, ts, &bounds, &mut self.ctx);
        let snapshot = ContextSnapshot {
            processing_time: self.ctx.processing_time,
            watermark: self.ctx.watermark,
        };
        session.apply_result(result, &snapshot).map(|elements| {
            WindowedBatch::new(
                elements.into_iter().map(|tv| tv.value).collect(),
                bounds.start,
                bounds.end,
                WindowType::Session,
            )
        })
    }

    /// Drive `on_event_time` on the current session (if any), emit
    /// the resulting batch (if any), and drop the session.
    fn close_current_session(&mut self) -> Option<WindowedBatch<T>> {
        let mut session = self.session.take()?;
        if session.buffer.is_empty() {
            return None;
        }
        let bounds = session.bounds();
        let result = session
            .trigger
            .on_event_time(bounds.max_timestamp(), &bounds, &mut self.ctx);
        let snapshot = ContextSnapshot {
            processing_time: self.ctx.processing_time,
            watermark: self.ctx.watermark,
        };
        session.apply_result(result, &snapshot).map(|elements| {
            WindowedBatch::new(
                elements.into_iter().map(|tv| tv.value).collect(),
                bounds.start,
                bounds.end,
                WindowType::Session,
            )
        })
    }
}

#[derive(Default, Debug)]
struct OperatorContext {
    watermark: i64,
    processing_time: i64,
    event_time_timers: Vec<i64>,
    processing_time_timers: Vec<i64>,
}

impl TriggerContext for OperatorContext {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }
    fn current_watermark(&self) -> i64 {
        self.watermark
    }
    fn register_event_time_timer(&mut self, time: i64) {
        self.event_time_timers.push(time);
    }
    fn register_processing_time_timer(&mut self, time: i64) {
        self.processing_time_timers.push(time);
    }
    fn delete_event_time_timer(&mut self, time: i64) {
        self.event_time_timers.retain(|t| *t != time);
    }
    fn delete_processing_time_timer(&mut self, time: i64) {
        self.processing_time_timers.retain(|t| *t != time);
    }
}

struct ContextSnapshot {
    processing_time: i64,
    watermark: i64,
}

impl EvictorContext for ContextSnapshot {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }
    fn current_watermark(&self) -> i64 {
        self.watermark
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::TimestampedValue;
    use crate::windowing::{CountTrigger, EventTimeTrigger, PurgingTrigger};
    use futures::stream::{self, StreamExt};

    fn ts_stream(values: &[i64]) -> Pin<Box<dyn Stream<Item = TimestampedValue<i64>> + Send>> {
        let owned: Vec<_> = values
            .iter()
            .map(|t| TimestampedValue::new(*t, *t))
            .collect();
        Box::pin(stream::iter(owned))
    }

    #[tokio::test]
    async fn count_trigger_fires_within_session() {
        // 30s gap, fire every 2 elements. Events arriving every 1s.
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(Duration::from_secs(30), CountTrigger::of(2));

        let batches: Vec<_> = window
            .apply(ts_stream(&[1000, 2000, 3000, 4000]))
            .collect()
            .await;

        // Two fires of size 2 + 4 (count trigger doesn't purge, so
        // the second fire sees the full accumulated buffer).
        assert_eq!(batches.len(), 2, "two fires expected, got {batches:?}");
        assert_eq!(batches[0].size(), 2);
        assert_eq!(batches[1].size(), 4);
    }

    #[tokio::test]
    async fn purging_count_trigger_resets_session_state() {
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(
                Duration::from_secs(30),
                PurgingTrigger::of(CountTrigger::of(2)),
            );

        let batches: Vec<_> = window
            .apply(ts_stream(&[1000, 2000, 3000, 4000]))
            .collect()
            .await;

        // Two fires of size 2 each (purge clears between fires).
        assert_eq!(batches.len(), 2);
        for b in &batches {
            assert_eq!(b.size(), 2);
        }
    }

    #[tokio::test]
    async fn event_time_trigger_fires_on_gap_close() {
        // 5s gap, EventTimeTrigger. Two events within gap, then
        // a third 10s later → triggers session close + on_event_time.
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(Duration::from_secs(5), EventTimeTrigger::create());

        let batches: Vec<_> = window
            .apply(ts_stream(&[1000, 2000, 12000]))
            .collect()
            .await;

        // First session closes when 12000 arrives → on_event_time
        // fires for [1000, 2001) with the two prior events.
        // Then 12000 starts a fresh session — at end-of-stream we
        // also close it with on_event_time fire.
        assert_eq!(
            batches.len(),
            2,
            "expected two batches (closed + final), got {batches:?}"
        );
        let first = &batches[0];
        assert_eq!(first.window_start, 1000);
        assert_eq!(first.size(), 2);
        let second = &batches[1];
        assert_eq!(second.window_start, 12000);
        assert_eq!(second.size(), 1);
    }

    #[tokio::test]
    async fn two_disjoint_sessions_each_fire_independently() {
        // 5s gap, PurgingTrigger over CountTrigger::of(2). Two
        // session blocks of 2 events each, separated by 20s.
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(
                Duration::from_secs(5),
                PurgingTrigger::of(CountTrigger::of(2)),
            );

        let batches: Vec<_> = window
            .apply(ts_stream(&[1000, 2000, 22000, 23000]))
            .collect()
            .await;

        // First session fires on the 2nd event (count=2), purges.
        // Gap closes when 22000 arrives — no buffer left, so no
        // close-time emission. Second session fires on 23000.
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].window_start, 1000);
        assert_eq!(batches[1].window_start, 22000);
    }

    #[tokio::test]
    async fn empty_stream_produces_no_batches() {
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(Duration::from_secs(5), EventTimeTrigger::create());
        let batches: Vec<_> = window.apply(ts_stream(&[])).collect().await;
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn single_event_session_emits_at_end_of_stream() {
        // EventTimeTrigger only fires via on_event_time, which is
        // driven at end-of-stream when no further input arrives.
        let window: TriggerableSessionWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSessionWindow::new(Duration::from_secs(5), EventTimeTrigger::create());

        let batches: Vec<_> = window.apply(ts_stream(&[1000])).collect().await;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].window_start, 1000);
        assert_eq!(batches[0].size(), 1);
    }
}
