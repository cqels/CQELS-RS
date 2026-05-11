//! Trigger-driven sliding time-window operator.
//!
//! Companion to [`TriggerableTimeWindow`](super::TriggerableTimeWindow): same
//! Trigger/Evictor delegation, but with sliding (multi-window) assignment.
//! Each incoming event lands in every window whose half-open interval
//! `[start, start + size)` contains its timestamp; window starts step by
//! `slide`, mirroring the canonical CQELS sliding semantics.
//!
//! Scope (matches the tumbling MVP):
//! - **Sliding time-window assignment.** Each element fans out into
//!   `ceil(size / slide)` windows on average. A fresh trigger + evictor
//!   are cloned from templates per new window.
//! - **Event-time advancement.** When the watermark crosses an open
//!   window's `max_timestamp()`, that window is driven through
//!   `trigger.on_event_time(...)` in ascending start order.
//! - **Emission ordering.** Per input event, prior windows' event-time
//!   fires come first (oldest first), then the per-window on-element
//!   fires for the windows that contain the new event (oldest first).
//!
//! Not in scope (deferred follow-ups, same as the tumbling MVP):
//! - Processing-time clocks/timers.
//! - Window expiration. Old windows stay in the operator's state map
//!   for the lifetime of the stream.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{Stream, StreamExt};

use crate::stream::Timestamped;
use crate::window::{Window, WindowType, WindowedBatch};
use crate::windowing::triggerable_window::NoEvictor;
use crate::windowing::{
    Evictor, EvictorContext, TimeWindow, Trigger, TriggerContext, TriggerableWindowProcessor,
    WindowBounds,
};

/// Trigger-driven sliding time window.
///
/// Configured with a `size` (window length), a `slide` interval
/// (how far window starts step apart), a trigger template, and an
/// optional evictor template. Each open window gets its own cloned
/// trigger/evictor — necessary because triggers like
/// [`CountTrigger`](crate::windowing::CountTrigger) keep state inline
/// per instance.
///
/// # Type parameters
///
/// - `T` — element type. Must implement [`Timestamped`].
/// - `Tr` — trigger template. Must implement
///   [`Trigger<T, TimeWindow>`] and `Clone`.
/// - `Ev` — evictor template (optional via constructor). Must
///   implement [`Evictor<T, TimeWindow>`] and `Clone`.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use cqels_core::stream::TimestampedValue;
/// use cqels_core::window::Window;
/// use cqels_core::windowing::{CountTrigger, TriggerableSlidingWindow};
///
/// // 10s windows that step every 5s; fire every 3 elements.
/// let window = TriggerableSlidingWindow::<TimestampedValue<i64>, _, _>::new(
///     Duration::from_secs(10),
///     Duration::from_secs(5),
///     CountTrigger::of(3),
/// );
/// assert_eq!(
///     window.window_type(),
///     cqels_core::window::WindowType::SlidingTime
/// );
/// ```
#[derive(Clone, Debug)]
pub struct TriggerableSlidingWindow<T, Tr, Ev> {
    size: Duration,
    slide: Duration,
    trigger_template: Tr,
    evictor_template: Option<Ev>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, Tr> TriggerableSlidingWindow<T, Tr, NoEvictor>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a sliding trigger-driven window with no evictor.
    pub fn new(size: Duration, slide: Duration, trigger: Tr) -> Self {
        Self {
            size,
            slide,
            trigger_template: trigger,
            evictor_template: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, Tr, Ev> TriggerableSlidingWindow<T, Tr, Ev>
where
    T: Timestamped + Clone + Send + 'static,
    Tr: Trigger<T, TimeWindow> + Clone + Send + 'static,
    Ev: Evictor<T, TimeWindow> + Clone + Send + 'static,
{
    /// Builds a sliding trigger-driven window with an evictor.
    pub fn with_evictor(size: Duration, slide: Duration, trigger: Tr, evictor: Ev) -> Self {
        Self {
            size,
            slide,
            trigger_template: trigger,
            evictor_template: Some(evictor),
            _phantom: PhantomData,
        }
    }

    /// The window size.
    pub fn size(&self) -> Duration {
        self.size
    }

    /// The slide interval.
    pub fn slide(&self) -> Duration {
        self.slide
    }
}

impl<T, Tr, Ev> Window<T> for TriggerableSlidingWindow<T, Tr, Ev>
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
        let slide_ms = (self.slide.as_millis() as i64).max(1);
        let trigger_template = self.trigger_template.clone();
        let evictor_template = self.evictor_template.clone();

        // Per-window state map keyed by window_start. Cleanup of old
        // windows is a deferred follow-up — for now they accumulate.
        type WindowMap<T, Tr, Ev> =
            BTreeMap<i64, TriggerableWindowProcessor<T, TimeWindow, Tr, Ev>>;
        let processors: WindowMap<T, Tr, Ev> = BTreeMap::new();
        let ctx = SlidingOperatorContext::default();

        let out = futures::stream::unfold(
            (stream, processors, ctx, trigger_template, evictor_template),
            move |(mut stream, mut processors, mut ctx, trigger_template, evictor_template)| {
                async move {
                    loop {
                        let elem = stream.next().await?;
                        let ts = elem.timestamp();

                        // Advance watermark monotonically.
                        if ts > ctx.watermark {
                            ctx.watermark = ts;
                        }

                        // Compute all window starts this element belongs
                        // to. Mirrors `SlidingWindow::apply` in
                        // `crate::window`.
                        let first_start = (((ts - size_ms) / slide_ms) + 1) * slide_ms;
                        let mut starts: Vec<i64> = Vec::new();
                        let mut window_start = first_start.max(0);
                        while window_start <= ts {
                            let window_end = window_start + size_ms;
                            if ts < window_end {
                                starts.push(window_start);
                            }
                            window_start += slide_ms;
                        }

                        let mut emissions: Vec<WindowedBatch<T>> = Vec::new();

                        // ─── Event-time fires for open windows whose
                        // end has been crossed by the new watermark.
                        // Don't refire windows the new event is about
                        // to land in (those will get on_element below).
                        let target_set: std::collections::BTreeSet<i64> =
                            starts.iter().copied().collect();
                        let due_starts: Vec<i64> = processors
                            .keys()
                            .copied()
                            .filter(|s| {
                                let b = TimeWindow::new(*s, *s + size_ms);
                                b.max_timestamp() <= ctx.watermark && !target_set.contains(s)
                            })
                            .collect();
                        for s in due_starts {
                            let b = TimeWindow::new(s, s + size_ms);
                            let snapshot = ContextSnapshot {
                                processing_time: ctx.processing_time,
                                watermark: ctx.watermark,
                            };
                            let proc = processors.get_mut(&s).expect("due start in map");
                            // Java semantics: fire the trigger with
                            // `window.max_timestamp()`, not the raw
                            // watermark — `EventTimeTrigger` compares
                            // `time == window.max_timestamp()`.
                            let emission =
                                proc.on_event_time(b.max_timestamp(), &mut ctx, &snapshot);
                            if !emission.elements.is_empty() {
                                emissions.push(WindowedBatch::new(
                                    emission.elements.into_iter().map(|tv| tv.value).collect(),
                                    b.start,
                                    b.end,
                                    WindowType::SlidingTime,
                                ));
                            }
                        }

                        // ─── on_element fan-out: every window the new
                        // event lands in. Visit in start order so
                        // emissions stay time-ordered.
                        for s in &starts {
                            let b = TimeWindow::new(*s, *s + size_ms);
                            let proc = processors.entry(*s).or_insert_with(|| {
                                TriggerableWindowProcessor::new(
                                    b,
                                    trigger_template.clone(),
                                    evictor_template.clone(),
                                )
                            });
                            let emission = proc.on_element(elem.clone(), ts, &mut ctx);
                            if !emission.elements.is_empty() {
                                emissions.push(WindowedBatch::new(
                                    emission.elements.into_iter().map(|tv| tv.value).collect(),
                                    b.start,
                                    b.end,
                                    WindowType::SlidingTime,
                                ));
                            }
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
        WindowType::SlidingTime
    }
}

/// Internal `TriggerContext` / `EvictorContext` for the sliding
/// trigger-driven operator. Same shape as the tumbling variant's
/// `OperatorContext`; defined here to keep the modules independent.
#[derive(Default, Debug)]
struct SlidingOperatorContext {
    watermark: i64,
    processing_time: i64,
    event_time_timers: Vec<i64>,
    processing_time_timers: Vec<i64>,
}

impl TriggerContext for SlidingOperatorContext {
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

impl EvictorContext for SlidingOperatorContext {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }
    fn current_watermark(&self) -> i64 {
        self.watermark
    }
}

/// Immutable snapshot used to satisfy the
/// `&dyn EvictorContext` arg of
/// [`TriggerableWindowProcessor::on_event_time`] when a mutable
/// borrow of the live context is already held.
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
    async fn purging_count_trigger_fires_per_window_assignment() {
        // 10s windows sliding every 5s. With PurgingTrigger wrapping
        // CountTrigger::of(2), each window fires when it has seen 2
        // elements and then resets.
        //
        // Events at ts = 0, 4, 8, 12 land in windows as follows
        // (window starts must be in [ts-size+1, ts] and divisible by
        // slide; size=10000, slide=5000):
        //   ts=0     → [0]               (only window starting at 0)
        //   ts=4000  → [0]               (start=5000 is > ts=4000)
        //   ts=8000  → [0, 5000]
        //   ts=12000 → [5000, 10000]
        //
        // Window [0,10000): sees ts=0, ts=4000, ts=8000 — fires after 2.
        // Window [5000,15000): sees ts=8000, ts=12000 — fires after 2.
        // Window [10000,20000): sees ts=12000 — does not fire (1 < 2).
        let window = TriggerableSlidingWindow::<TimestampedValue<i64>, _, NoEvictor>::new(
            Duration::from_secs(10),
            Duration::from_secs(5),
            PurgingTrigger::of(CountTrigger::of(2)),
        );

        let batches: Vec<_> = window
            .apply(ts_stream(&[0, 4000, 8000, 12000]))
            .collect()
            .await;

        // Two fires expected.
        assert_eq!(batches.len(), 2, "expected two fires, got {batches:?}");

        // Both batches should have exactly 2 elements (PurgingTrigger
        // resets after each fire).
        for b in &batches {
            assert_eq!(b.size(), 2);
        }

        // First fire is window [0, 10000), second is window
        // [5000, 15000). Confirm start coordinates.
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[1].window_start, 5000);
    }

    #[tokio::test]
    async fn element_fans_out_into_overlapping_windows() {
        // 4s window sliding every 1s. An event at ts=3000 should
        // land in windows [0,4), [1,5), [2,6), [3,7) — 4 windows.
        // A 1-element count trigger fires immediately on each
        // on_element, so we expect 4 emissions for that single event.
        let window = TriggerableSlidingWindow::<TimestampedValue<i64>, _, NoEvictor>::new(
            Duration::from_secs(4),
            Duration::from_secs(1),
            CountTrigger::of(1),
        );

        let batches: Vec<_> = window.apply(ts_stream(&[3000])).collect().await;

        assert_eq!(batches.len(), 4, "single event should fire 4 windows");
        // Window starts in ascending order: 0, 1000, 2000, 3000.
        let starts: Vec<_> = batches.iter().map(|b| b.window_start).collect();
        assert_eq!(starts, vec![0, 1000, 2000, 3000]);
        // Each batch contains exactly the one event.
        for b in &batches {
            assert_eq!(b.size(), 1);
        }
    }

    #[tokio::test]
    async fn event_time_trigger_fires_on_watermark_advance() {
        // 4s window sliding every 2s. EventTimeTrigger fires when
        // the watermark crosses `max_timestamp()` (= start + 3999).
        //
        // Use timestamps comfortably above `size` so the assignment
        // formula `((ts - size_ms) / slide_ms + 1) * slide_ms` (which
        // matches the existing `SlidingWindow` and uses truncating
        // integer division) behaves intuitively:
        //
        //   ts=10000 lands in [8000,12000) and [10000,14000)
        //   ts=11000 lands in [8000,12000) and [10000,14000)
        //   ts=15000 advances the watermark past
        //   [8000,12000)'s max_timestamp (11999), so that window
        //   fires `on_event_time` with both prior events.
        let window = TriggerableSlidingWindow::<TimestampedValue<i64>, _, NoEvictor>::new(
            Duration::from_secs(4),
            Duration::from_secs(2),
            EventTimeTrigger::create(),
        );

        let batches: Vec<_> = window
            .apply(ts_stream(&[10000, 11000, 15000]))
            .collect()
            .await;

        let first = batches
            .iter()
            .find(|b| b.window_start == 8000)
            .expect("window [8000, 12000) must fire on event-time");
        assert_eq!(first.window_end, 12000);
        assert_eq!(first.size(), 2);
    }

    #[tokio::test]
    async fn empty_stream_produces_no_batches() {
        let window: TriggerableSlidingWindow<TimestampedValue<i64>, _, NoEvictor> =
            TriggerableSlidingWindow::new(
                Duration::from_secs(4),
                Duration::from_secs(2),
                EventTimeTrigger::create(),
            );
        let batches: Vec<_> = window.apply(ts_stream(&[])).collect().await;
        assert!(batches.is_empty());
    }
}
