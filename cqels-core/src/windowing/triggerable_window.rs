//! Trigger-driven time-window operator.
//!
//! Bridges the [`Trigger`]/[`Evictor`] framework in `crate::windowing` to
//! the [`Window<T>`](crate::window::Window) trait used by the rest of the
//! engine. The existing concrete window operators
//! ([`TumblingWindow`](crate::window::TumblingWindow),
//! [`SlidingWindow`](crate::window::SlidingWindow),
//! [`SessionWindow`](crate::window::SessionWindow)) bake their emission
//! rule into each `impl`; this operator delegates emission to a
//! caller-supplied trigger and discard policy to an optional evictor.
//!
//! Scope (MVP):
//! - **Tumbling time-window assignment.** Each element is routed to a
//!   single window based on `timestamp / size_ms`. Sliding and session
//!   variants are follow-ups.
//! - **Per-window trigger + evictor state.** A fresh trigger and
//!   evictor are cloned from the operator's template each time a new
//!   window opens. State is kept inline on the trigger struct (matching
//!   the rest of `windowing`).
//! - **Event-time advancement.** When an incoming element's timestamp
//!   pushes the watermark forward, every open window with
//!   `max_timestamp() <= new_watermark` is driven through
//!   `trigger.on_event_time(...)` in ascending order. Late arrivals
//!   (timestamp < current watermark) still route to their natural
//!   window and the trigger decides what to do.
//! - **Emission ordering.** Emissions from a single input element are
//!   flushed in window-key order: prior windows' event-time fires
//!   first, then the current window's on-element fire (if any).
//!
//! Not in scope here (deferred):
//! - Sliding / session window assignment.
//! - Processing-time clocks and timers — `on_processing_time` is not
//!   driven by this operator (a wall-clock-driven version is a
//!   follow-up).
//! - Window expiration. Windows stay in the operator's state map for
//!   the lifetime of the stream; bounded grace + GC is a follow-up.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{Stream, StreamExt};

use crate::stream::{Timestamped, TimestampedValue};
use crate::window::{Window, WindowType, WindowedBatch};
use crate::windowing::{
    Evictor, EvictorContext, TimeWindow, Trigger, TriggerContext, TriggerableWindowProcessor,
    WindowBounds,
};

/// No-op evictor used as the default `Ev` type parameter when the
/// caller doesn't supply one. Required because
/// [`TriggerableWindowProcessor`] bounds `Ev: Evictor<T, W>` even when
/// no evictor is installed.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoEvictor;

impl<T, W: WindowBounds> Evictor<T, W> for NoEvictor {
    fn evict_before(&self, _: &mut Vec<TimestampedValue<T>>, _: &W, _: &dyn EvictorContext) {}
    fn evict_after(&self, _: &mut Vec<TimestampedValue<T>>, _: &W, _: &dyn EvictorContext) {}
}

/// Trigger-driven tumbling time window.
///
/// Configured with a window size, a trigger template, and an optional
/// evictor template. Each open window gets its own cloned trigger
/// (and evictor, if any) — necessary because triggers like
/// [`CountTrigger`](crate::windowing::CountTrigger) keep state inline
/// per instance.
///
/// # Type parameters
///
/// - `T` — element type. Must implement [`Timestamped`] so the operator
///   can assign each event to a window.
/// - `Tr` — trigger template. Must implement
///   [`Trigger<T, TimeWindow>`] and `Clone` so per-window instances
///   can be minted.
/// - `Ev` — evictor template (optional via constructor). Must
///   implement [`Evictor<T, TimeWindow>`] and `Clone`.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use cqels_core::stream::TimestampedValue;
/// use cqels_core::window::Window;
/// use cqels_core::windowing::{CountTrigger, TriggerableTimeWindow};
///
/// // Fire every 3 elements; tumbling 5s windows.
/// let window = TriggerableTimeWindow::<TimestampedValue<i64>, _, _>::new(
///     Duration::from_secs(5),
///     CountTrigger::of(3),
/// );
/// assert_eq!(window.window_type(), cqels_core::window::WindowType::TumblingTime);
/// ```
#[derive(Clone, Debug)]
pub struct TriggerableTimeWindow<T, Tr, Ev> {
    size: Duration,
    trigger_template: Tr,
    evictor_template: Option<Ev>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, Tr> TriggerableTimeWindow<T, Tr, NoEvictor>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a trigger-driven tumbling window with no evictor.
    ///
    /// `size` controls window assignment (each event lands in the
    /// window `[k*size, (k+1)*size)` where `k = ts / size_ms`).
    pub fn new(size: Duration, trigger: Tr) -> Self {
        Self {
            size,
            trigger_template: trigger,
            evictor_template: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, Tr, Ev> TriggerableTimeWindow<T, Tr, Ev>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
    Ev: Evictor<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a trigger-driven tumbling window with an evictor.
    pub fn with_evictor(size: Duration, trigger: Tr, evictor: Ev) -> Self {
        Self {
            size,
            trigger_template: trigger,
            evictor_template: Some(evictor),
            _phantom: PhantomData,
        }
    }

    /// The window size used for tumbling assignment.
    pub fn size(&self) -> Duration {
        self.size
    }
}

impl<T, Tr, Ev> Window<T> for TriggerableTimeWindow<T, Tr, Ev>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + Sync + 'static,
    Ev: Evictor<T, TimeWindow> + Clone + Send + Sync + 'static,
{
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        let size_ms = (self.size.as_millis() as i64).max(1);
        let trigger_template = self.trigger_template.clone();
        let evictor_template = self.evictor_template.clone();

        // Per-window state map keyed by window key (= ts / size_ms).
        // Holds the trigger/evictor instance + the watermark seen so
        // far. The map is intentionally never pruned in the MVP —
        // window GC is a separate concern.
        type WindowMap<T, Tr, Ev> =
            BTreeMap<i64, TriggerableWindowProcessor<T, TimeWindow, Tr, Ev>>;
        let processors: WindowMap<T, Tr, Ev> = BTreeMap::new();

        // A real `TriggerContext`/`EvictorContext` that tracks the
        // operator's watermark. Triggers may register event-time
        // timers; we treat any such registration as a hint to
        // re-check on the next element (we don't run a wall-clock
        // timer driver in the MVP).
        let ctx = OperatorContext::default();

        let out = futures::stream::unfold(
            (stream, processors, ctx, trigger_template, evictor_template),
            move |(mut stream, mut processors, mut ctx, trigger_template, evictor_template)| {
                async move {
                    loop {
                        let elem = stream.next().await?;
                        let ts = elem.timestamp();
                        let window_key = ts.div_euclid(size_ms);
                        let bounds =
                            TimeWindow::new(window_key * size_ms, (window_key + 1) * size_ms);

                        // Advance the watermark monotonically (we only
                        // move it forward — late arrivals don't lower it).
                        if ts > ctx.watermark {
                            ctx.watermark = ts;
                        }

                        let mut emissions: Vec<WindowedBatch<T>> = Vec::new();

                        // Fire `on_event_time` for any open window
                        // whose end has been crossed by the new
                        // watermark. Visit them in ascending key order
                        // so emissions stay time-ordered.
                        let due_keys: Vec<i64> = processors
                            .iter()
                            .filter(|(k, _)| {
                                let b = TimeWindow::new(*k * size_ms, (*k + 1) * size_ms);
                                b.max_timestamp() <= ctx.watermark && **k != window_key
                            })
                            .map(|(k, _)| *k)
                            .collect();
                        for k in due_keys {
                            let b = TimeWindow::new(k * size_ms, (k + 1) * size_ms);
                            // Snapshot evictor-relevant state before
                            // calling so we can hold a mutable borrow
                            // of `ctx` for the trigger callback.
                            let evictor_snapshot = ContextSnapshot {
                                processing_time: ctx.processing_time,
                                watermark: ctx.watermark,
                            };
                            // SAFETY: we just collected this key from
                            // the map; nothing has removed it since.
                            let proc = processors
                                .get_mut(&k)
                                .expect("due key still in processor map");
                            // Java semantics: the framework fires
                            // `on_event_time(timer_time)` exactly at the
                            // timestamp the timer was registered for —
                            // which for an `EventTimeTrigger` is
                            // `window.max_timestamp()`. Passing the raw
                            // watermark here makes triggers that
                            // compare `time == window.max_timestamp()`
                            // (the canonical pattern) silently return
                            // `Continue`.
                            let emission =
                                proc.on_event_time(b.max_timestamp(), &mut ctx, &evictor_snapshot);
                            if !emission.elements.is_empty() {
                                emissions.push(WindowedBatch::new(
                                    emission.elements.into_iter().map(|tv| tv.value).collect(),
                                    b.start,
                                    b.end,
                                    WindowType::TumblingTime,
                                ));
                            }
                        }

                        // Route the new element into its window's
                        // processor (constructing one if absent), then
                        // collect any FIRE emission.
                        let proc = processors.entry(window_key).or_insert_with(|| {
                            TriggerableWindowProcessor::new(
                                bounds,
                                trigger_template.clone(),
                                evictor_template.clone(),
                            )
                        });
                        let emission = proc.on_element(elem, ts, &mut ctx);
                        if !emission.elements.is_empty() {
                            emissions.push(WindowedBatch::new(
                                emission.elements.into_iter().map(|tv| tv.value).collect(),
                                bounds.start,
                                bounds.end,
                                WindowType::TumblingTime,
                            ));
                        }

                        if emissions.is_empty() {
                            continue;
                        }
                        return Some((
                            futures::stream::iter(emissions),
                            (stream, processors, ctx, trigger_template, evictor_template),
                        ));
                    }
                }
            },
        )
        .flatten();

        Box::pin(out)
    }

    fn window_type(&self) -> WindowType {
        WindowType::TumblingTime
    }
}

/// Internal `TriggerContext` / `EvictorContext` for the trigger-driven
/// window operator. Tracks event-time watermark and processing-time
/// snapshot; timer registrations are recorded but not separately
/// driven (the operator re-evaluates open windows on every incoming
/// element, which subsumes timer firing for the tumbling MVP).
#[derive(Default, Debug)]
struct OperatorContext {
    watermark: i64,
    processing_time: i64,
    /// Event-time timers registered by triggers. Not currently fired
    /// independently — we re-check open windows on every incoming
    /// element, which subsumes timer firing for the tumbling MVP.
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

impl EvictorContext for OperatorContext {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }

    fn current_watermark(&self) -> i64 {
        self.watermark
    }
}

/// Immutable snapshot of an `OperatorContext` used to satisfy
/// [`TriggerableWindowProcessor::on_event_time`]'s `&dyn EvictorContext`
/// argument without conflicting with the live `&mut TriggerContext`
/// borrow.
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
    use crate::windowing::{CountEvictor, CountTrigger, EventTimeTrigger, PurgingTrigger};
    use futures::stream::{self, StreamExt};

    fn ts_stream(values: &[i64]) -> Pin<Box<dyn Stream<Item = TimestampedValue<i64>> + Send>> {
        let owned: Vec<_> = values
            .iter()
            .map(|t| TimestampedValue::new(*t, *t))
            .collect();
        Box::pin(stream::iter(owned))
    }

    #[tokio::test]
    async fn count_trigger_fires_every_n_elements_in_same_window() {
        let window: TriggerableTimeWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableTimeWindow::new(Duration::from_secs(60), CountTrigger::of(2));

        // 4 elements within one 60s window — expect 2 FIREs of size 2.
        let batches: Vec<_> = window
            .apply(ts_stream(&[100, 200, 300, 400]))
            .collect()
            .await;

        assert_eq!(batches.len(), 2, "two fires expected, got {batches:?}");
        for batch in &batches {
            // CountTrigger emits Fire (not FireAndPurge), so the buffer
            // grows: first fire sees [100,200], second sees [100,200,300,400].
            assert!(batch.window_start <= 100);
            assert!(batch.window_end >= 400);
        }
        assert_eq!(batches[0].size(), 2);
        assert_eq!(batches[1].size(), 4);
    }

    #[tokio::test]
    async fn purging_count_trigger_clears_between_fires() {
        let window: TriggerableTimeWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableTimeWindow::new(
                Duration::from_secs(60),
                PurgingTrigger::of(CountTrigger::of(2)),
            );

        let batches: Vec<_> = window
            .apply(ts_stream(&[100, 200, 300, 400]))
            .collect()
            .await;

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].size(), 2);
        assert_eq!(
            batches[1].size(),
            2,
            "purge should clear buffer between fires"
        );
    }

    #[tokio::test]
    async fn event_time_trigger_fires_when_next_window_advances_watermark() {
        // 1s windows; first window [0, 1000). EventTimeTrigger fires
        // when watermark >= window.max_timestamp() (= 999). The next
        // event at ts=1500 advances watermark past 999, so window 0
        // should fire with both prior elements.
        let window: TriggerableTimeWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableTimeWindow::new(Duration::from_secs(1), EventTimeTrigger::create());

        let batches: Vec<_> = window.apply(ts_stream(&[100, 500, 1500])).collect().await;

        assert!(
            !batches.is_empty(),
            "event-time trigger should fire on watermark advance"
        );
        // The first emitted batch corresponds to window [0, 1000) and
        // contains the first two elements (timestamps 100, 500).
        let first = &batches[0];
        assert_eq!(first.window_start, 0);
        assert_eq!(first.window_end, 1000);
        assert_eq!(first.size(), 2);
    }

    #[tokio::test]
    async fn evictor_caps_emitted_batch_size() {
        // CountTrigger fires every 3 elements; CountEvictor caps the
        // buffer to 2 before each fire. We expect each emitted batch
        // to have at most 2 elements.
        let window = TriggerableTimeWindow::<TimestampedValue<i64>, _, _>::with_evictor(
            Duration::from_secs(60),
            CountTrigger::of(3),
            CountEvictor::of(2),
        );

        let batches: Vec<_> = window
            .apply(ts_stream(&[100, 200, 300, 400, 500, 600]))
            .collect()
            .await;

        assert_eq!(batches.len(), 2, "two fires expected");
        for b in &batches {
            assert!(
                b.size() <= 2,
                "CountEvictor::of(2) should cap batch size, got {}",
                b.size()
            );
        }
    }

    #[tokio::test]
    async fn empty_stream_produces_no_batches() {
        let window: TriggerableTimeWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableTimeWindow::new(Duration::from_secs(1), EventTimeTrigger::create());
        let batches: Vec<_> = window.apply(ts_stream(&[])).collect().await;
        assert!(batches.is_empty());
    }
}
