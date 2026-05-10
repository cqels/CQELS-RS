//! Trigger and evictor framework for window evaluation.
//!
//! Mirrors Java's `org.cqels.windowing.{triggers,evictors,windows}` packages.
//!
//! - [`WindowBounds`] — minimal metadata about a window's time extent.
//! - [`TriggerResult`] — `Continue`/`Fire`/`Purge`/`FireAndPurge`.
//! - [`Trigger`] — decides when to emit a window's contents based on
//!   incoming elements, event-time, and processing-time signals.
//! - [`Evictor`] — discards elements from a window's contents before or
//!   after emission.
//!
//! # Scope vs Java
//!
//! Ported in this module:
//! - [`EventTimeTrigger`], [`ProcessingTimeTrigger`], [`CountTrigger`],
//!   [`PurgingTrigger`], [`CountEvictor`], [`TimeEvictor`].
//! - A [`MockTriggerContext`] / [`MockEvictorContext`] for testing.
//!
//! Deferred (require separate `DeltaFunction` machinery / continuous-time
//! state plumbing): `DeltaTrigger`, `DeltaEvictor`, `ContinuousEventTimeTrigger`,
//! `ContinuousProcessingTimeTrigger`, `ProcessingTimeoutTrigger`.
//!
//! Note: triggers hold state inline (one trigger instance per window) rather
//! than relying on Java's "framework-managed partitioned state" model. The
//! existing window operators in [`crate::window`] don't yet consume these
//! traits — integration is a follow-up.

use std::time::Duration;

use crate::stream::TimestampedValue;

pub mod delta;
pub mod processor;
pub mod timeout;

pub use delta::{DeltaEvictor, DeltaFunction, DeltaTrigger};
pub use processor::{TriggerableWindowProcessor, WindowEmission};
pub use timeout::ProcessingTimeoutTrigger;

// ─── Window bounds ──────────────────────────────────────────────────────────

/// Minimal window-bounds trait used by triggers and evictors.
///
/// Mirrors Java's `org.cqels.windowing.windows.Window#maxTimestamp()`.
pub trait WindowBounds {
    /// Returns the largest timestamp that still belongs to this window.
    fn max_timestamp(&self) -> i64;
}

/// Half-open time window `[start, end)`.
///
/// Mirrors Java's `org.cqels.windowing.windows.TimeWindow`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub start: i64,
    pub end: i64,
}

impl TimeWindow {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }
}

impl WindowBounds for TimeWindow {
    fn max_timestamp(&self) -> i64 {
        self.end - 1
    }
}

/// A single global window that contains all elements.
///
/// Mirrors Java's `org.cqels.windowing.windows.GlobalWindow`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct GlobalWindow;

impl WindowBounds for GlobalWindow {
    fn max_timestamp(&self) -> i64 {
        i64::MAX
    }
}

// ─── Trigger result ─────────────────────────────────────────────────────────

/// Outcome of a [`Trigger`] callback.
///
/// Mirrors Java's `TriggerResult` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TriggerResult {
    /// Keep the window open; emit nothing.
    Continue,
    /// Emit window contents but keep the window open.
    Fire,
    /// Discard window contents without emitting.
    Purge,
    /// Emit window contents and discard them afterwards.
    FireAndPurge,
}

impl TriggerResult {
    pub fn is_fire(&self) -> bool {
        matches!(self, TriggerResult::Fire | TriggerResult::FireAndPurge)
    }

    pub fn is_purge(&self) -> bool {
        matches!(self, TriggerResult::Purge | TriggerResult::FireAndPurge)
    }

    /// Wraps a result so that any FIRE becomes FIRE_AND_PURGE.
    pub(crate) fn into_purging(self) -> Self {
        match self {
            TriggerResult::Fire => TriggerResult::FireAndPurge,
            other => other,
        }
    }
}

// ─── Trigger context ────────────────────────────────────────────────────────

/// Context passed to [`Trigger`] callbacks.
///
/// Provides access to current time signals and timer registration. Mirrors
/// Java's `Trigger.TriggerContext` (without the partitioned-state hook —
/// Rust triggers hold their own state inline).
pub trait TriggerContext {
    fn current_processing_time(&self) -> i64;
    fn current_watermark(&self) -> i64;
    fn register_processing_time_timer(&mut self, time: i64);
    fn register_event_time_timer(&mut self, time: i64);
    fn delete_processing_time_timer(&mut self, time: i64);
    fn delete_event_time_timer(&mut self, time: i64);
}

/// Mock trigger context for unit testing.
#[derive(Default, Debug)]
pub struct MockTriggerContext {
    pub processing_time: i64,
    pub watermark: i64,
    pub processing_time_timers: Vec<i64>,
    pub event_time_timers: Vec<i64>,
}

impl TriggerContext for MockTriggerContext {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }

    fn current_watermark(&self) -> i64 {
        self.watermark
    }

    fn register_processing_time_timer(&mut self, time: i64) {
        self.processing_time_timers.push(time);
    }

    fn register_event_time_timer(&mut self, time: i64) {
        self.event_time_timers.push(time);
    }

    fn delete_processing_time_timer(&mut self, time: i64) {
        self.processing_time_timers.retain(|t| *t != time);
    }

    fn delete_event_time_timer(&mut self, time: i64) {
        self.event_time_timers.retain(|t| *t != time);
    }
}

// ─── Trigger trait ──────────────────────────────────────────────────────────

/// Callback-based window-emission policy.
///
/// Mirrors Java's `Trigger<T, W>`. Triggers may be stateful (e.g. a count
/// trigger increments a counter) — Rust modeled with `&mut self` and inline
/// fields rather than Java's framework-managed partitioned state.
pub trait Trigger<T, W: WindowBounds> {
    fn on_element(
        &mut self,
        element: &T,
        timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult;

    fn on_event_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult;

    fn on_processing_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult;

    fn clear(&mut self, window: &W, ctx: &mut dyn TriggerContext);
}

// ─── Concrete triggers ──────────────────────────────────────────────────────

/// Fires when the watermark crosses the window's max timestamp.
///
/// Mirrors Java's `EventTimeTrigger`.
#[derive(Clone, Copy, Debug, Default)]
pub struct EventTimeTrigger;

impl EventTimeTrigger {
    pub fn create() -> Self {
        Self
    }
}

impl<T, W: WindowBounds> Trigger<T, W> for EventTimeTrigger {
    fn on_element(
        &mut self,
        _element: &T,
        _timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if window.max_timestamp() <= ctx.current_watermark() {
            TriggerResult::Fire
        } else {
            ctx.register_event_time_timer(window.max_timestamp());
            TriggerResult::Continue
        }
    }

    fn on_event_time(
        &mut self,
        time: i64,
        window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if time == window.max_timestamp() {
            TriggerResult::Fire
        } else {
            TriggerResult::Continue
        }
    }

    fn on_processing_time(
        &mut self,
        _time: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        TriggerResult::Continue
    }

    fn clear(&mut self, window: &W, ctx: &mut dyn TriggerContext) {
        ctx.delete_event_time_timer(window.max_timestamp());
    }
}

/// Fires when processing time crosses the window's max timestamp.
///
/// Mirrors Java's `ProcessingTimeTrigger`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessingTimeTrigger;

impl ProcessingTimeTrigger {
    pub fn create() -> Self {
        Self
    }
}

impl<T, W: WindowBounds> Trigger<T, W> for ProcessingTimeTrigger {
    fn on_element(
        &mut self,
        _element: &T,
        _timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        ctx.register_processing_time_timer(window.max_timestamp());
        TriggerResult::Continue
    }

    fn on_event_time(
        &mut self,
        _time: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        TriggerResult::Continue
    }

    fn on_processing_time(
        &mut self,
        _time: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        TriggerResult::Fire
    }

    fn clear(&mut self, window: &W, ctx: &mut dyn TriggerContext) {
        ctx.delete_processing_time_timer(window.max_timestamp());
    }
}

/// Fires when the element count reaches a threshold, then resets.
///
/// Mirrors Java's `CountTrigger`. Note: Java keeps the count in
/// framework-managed partitioned state keyed by window; Rust keeps it inline
/// on the trigger struct — so each window must use its own trigger instance.
#[derive(Clone, Copy, Debug)]
pub struct CountTrigger {
    max_count: u64,
    count: u64,
}

impl CountTrigger {
    pub fn of(max_count: u64) -> Self {
        Self {
            max_count,
            count: 0,
        }
    }
}

impl<T, W: WindowBounds> Trigger<T, W> for CountTrigger {
    fn on_element(
        &mut self,
        _element: &T,
        _timestamp: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        self.count += 1;
        if self.count >= self.max_count {
            self.count = 0;
            TriggerResult::Fire
        } else {
            TriggerResult::Continue
        }
    }

    fn on_event_time(
        &mut self,
        _time: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        TriggerResult::Continue
    }

    fn on_processing_time(
        &mut self,
        _time: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        TriggerResult::Continue
    }

    fn clear(&mut self, _window: &W, _ctx: &mut dyn TriggerContext) {
        self.count = 0;
    }
}

/// Wraps another trigger so every FIRE becomes FIRE_AND_PURGE.
///
/// Mirrors Java's `PurgingTrigger`.
#[derive(Clone, Debug)]
pub struct PurgingTrigger<I> {
    inner: I,
}

impl<I> PurgingTrigger<I> {
    pub fn of(inner: I) -> Self {
        Self { inner }
    }
}

impl<T, W: WindowBounds, I: Trigger<T, W>> Trigger<T, W> for PurgingTrigger<I> {
    fn on_element(
        &mut self,
        element: &T,
        timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        self.inner
            .on_element(element, timestamp, window, ctx)
            .into_purging()
    }

    fn on_event_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        self.inner.on_event_time(time, window, ctx).into_purging()
    }

    fn on_processing_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        self.inner
            .on_processing_time(time, window, ctx)
            .into_purging()
    }

    fn clear(&mut self, window: &W, ctx: &mut dyn TriggerContext) {
        self.inner.clear(window, ctx);
    }
}

// ─── Evictor ────────────────────────────────────────────────────────────────

/// Context passed to [`Evictor`] callbacks.
pub trait EvictorContext {
    fn current_processing_time(&self) -> i64;
    fn current_watermark(&self) -> i64;
}

/// Mock evictor context for unit testing.
#[derive(Default, Debug)]
pub struct MockEvictorContext {
    pub processing_time: i64,
    pub watermark: i64,
}

impl EvictorContext for MockEvictorContext {
    fn current_processing_time(&self) -> i64 {
        self.processing_time
    }
    fn current_watermark(&self) -> i64 {
        self.watermark
    }
}

/// Discards elements from a window's buffer before or after firing.
///
/// Mirrors Java's `Evictor<T, W>`. The buffer is passed as `&mut Vec<...>`;
/// removal order is "from the front" to match Java's iterator-based logic.
pub trait Evictor<T, W: WindowBounds> {
    fn evict_before(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        window: &W,
        ctx: &dyn EvictorContext,
    );

    fn evict_after(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        window: &W,
        ctx: &dyn EvictorContext,
    );
}

/// Keeps the most recent `max_count` elements.
///
/// Mirrors Java's `CountEvictor`. Defaults to evicting *before* firing;
/// pass `evict_after = true` to invert.
#[derive(Clone, Copy, Debug)]
pub struct CountEvictor {
    max_count: usize,
    evict_after: bool,
}

impl CountEvictor {
    pub fn of(max_count: usize) -> Self {
        Self {
            max_count,
            evict_after: false,
        }
    }

    pub fn of_evict_after(max_count: usize) -> Self {
        Self {
            max_count,
            evict_after: true,
        }
    }
}

impl<T, W: WindowBounds> Evictor<T, W> for CountEvictor {
    fn evict_before(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if !self.evict_after {
            evict_oldest(elements, self.max_count);
        }
    }

    fn evict_after(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if self.evict_after {
            evict_oldest(elements, self.max_count);
        }
    }
}

fn evict_oldest<T>(elements: &mut Vec<TimestampedValue<T>>, max_count: usize) {
    if elements.len() <= max_count {
        return;
    }
    let drop_count = elements.len() - max_count;
    elements.drain(..drop_count);
}

/// Keeps elements whose timestamps are within `window_size` of the newest.
///
/// Mirrors Java's `TimeEvictor`.
#[derive(Clone, Copy, Debug)]
pub struct TimeEvictor {
    window_size_ms: i64,
    evict_after: bool,
}

impl TimeEvictor {
    pub fn of(window_size: Duration) -> Self {
        Self {
            window_size_ms: window_size.as_millis() as i64,
            evict_after: false,
        }
    }

    pub fn of_evict_after(window_size: Duration) -> Self {
        Self {
            window_size_ms: window_size.as_millis() as i64,
            evict_after: true,
        }
    }
}

impl<T, W: WindowBounds> Evictor<T, W> for TimeEvictor {
    fn evict_before(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if !self.evict_after {
            evict_by_time(elements, self.window_size_ms);
        }
    }

    fn evict_after(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if self.evict_after {
            evict_by_time(elements, self.window_size_ms);
        }
    }
}

fn evict_by_time<T>(elements: &mut Vec<TimestampedValue<T>>, window_size_ms: i64) {
    let Some(max_ts) = elements.iter().map(|e| e.timestamp).max() else {
        return;
    };
    let cutoff = max_ts - window_size_ms;
    elements.retain(|e| e.timestamp > cutoff);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> MockTriggerContext {
        MockTriggerContext::default()
    }

    fn evictor_ctx() -> MockEvictorContext {
        MockEvictorContext::default()
    }

    // ── TimeWindow + GlobalWindow ──

    #[test]
    fn time_window_max_timestamp_is_end_minus_one() {
        let w = TimeWindow::new(1000, 2000);
        assert_eq!(w.max_timestamp(), 1999);
    }

    #[test]
    fn global_window_max_timestamp_is_i64_max() {
        assert_eq!(GlobalWindow.max_timestamp(), i64::MAX);
    }

    // ── EventTimeTrigger ──

    #[test]
    fn event_time_trigger_fires_when_watermark_past_window_end() {
        let mut t = EventTimeTrigger::create();
        let window = TimeWindow::new(0, 1000);
        let mut c = ctx();
        c.watermark = 2000;
        let r = <EventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 100, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Fire);
    }

    #[test]
    fn event_time_trigger_continues_and_registers_timer_when_watermark_before_end() {
        let mut t = EventTimeTrigger::create();
        let window = TimeWindow::new(0, 1000);
        let mut c = ctx();
        c.watermark = 500;
        let r = <EventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 100, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Continue);
        assert_eq!(c.event_time_timers, vec![window.max_timestamp()]);
    }

    #[test]
    fn event_time_trigger_clear_removes_timer() {
        let mut t = EventTimeTrigger::create();
        let window = TimeWindow::new(0, 1000);
        let mut c = ctx();
        c.event_time_timers.push(window.max_timestamp());
        <EventTimeTrigger as Trigger<i32, TimeWindow>>::clear(&mut t, &window, &mut c);
        assert!(c.event_time_timers.is_empty());
    }

    // ── ProcessingTimeTrigger ──

    #[test]
    fn processing_time_trigger_on_processing_time_fires() {
        let mut t = ProcessingTimeTrigger::create();
        let window = TimeWindow::new(0, 1000);
        let mut c = ctx();
        let r = <ProcessingTimeTrigger as Trigger<i32, TimeWindow>>::on_processing_time(
            &mut t, 500, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Fire);
    }

    #[test]
    fn processing_time_trigger_on_element_registers_timer_only() {
        let mut t = ProcessingTimeTrigger::create();
        let window = TimeWindow::new(0, 1000);
        let mut c = ctx();
        let r = <ProcessingTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 50, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Continue);
        assert_eq!(c.processing_time_timers, vec![window.max_timestamp()]);
    }

    // ── CountTrigger ──

    #[test]
    fn count_trigger_fires_when_count_reaches_threshold() {
        let mut t = CountTrigger::of(3);
        let window = GlobalWindow;
        let mut c = ctx();
        assert_eq!(
            <CountTrigger as Trigger<i32, GlobalWindow>>::on_element(
                &mut t, &0, 0, &window, &mut c
            ),
            TriggerResult::Continue
        );
        assert_eq!(
            <CountTrigger as Trigger<i32, GlobalWindow>>::on_element(
                &mut t, &0, 0, &window, &mut c
            ),
            TriggerResult::Continue
        );
        assert_eq!(
            <CountTrigger as Trigger<i32, GlobalWindow>>::on_element(
                &mut t, &0, 0, &window, &mut c
            ),
            TriggerResult::Fire
        );
    }

    #[test]
    fn count_trigger_resets_after_firing() {
        let mut t = CountTrigger::of(2);
        let window = GlobalWindow;
        let mut c = ctx();
        for _ in 0..2 {
            <CountTrigger as Trigger<i32, GlobalWindow>>::on_element(
                &mut t, &0, 0, &window, &mut c,
            );
        }
        // After firing, the next element starts a fresh count.
        let r = <CountTrigger as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Continue);
    }

    // ── PurgingTrigger ──

    #[test]
    fn purging_trigger_converts_fire_to_fire_and_purge() {
        let mut t = PurgingTrigger::of(CountTrigger::of(1));
        let window = GlobalWindow;
        let mut c = ctx();
        let r = <PurgingTrigger<CountTrigger> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::FireAndPurge);
    }

    #[test]
    fn purging_trigger_preserves_continue() {
        let mut t = PurgingTrigger::of(CountTrigger::of(2));
        let window = GlobalWindow;
        let mut c = ctx();
        let r = <PurgingTrigger<CountTrigger> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut c,
        );
        assert_eq!(r, TriggerResult::Continue);
    }

    // ── TriggerResult ──

    #[test]
    fn trigger_result_flags() {
        assert!(TriggerResult::Fire.is_fire() && !TriggerResult::Fire.is_purge());
        assert!(TriggerResult::Purge.is_purge() && !TriggerResult::Purge.is_fire());
        assert!(TriggerResult::FireAndPurge.is_fire() && TriggerResult::FireAndPurge.is_purge());
        assert!(!TriggerResult::Continue.is_fire() && !TriggerResult::Continue.is_purge());
    }

    // ── CountEvictor ──

    #[test]
    fn count_evictor_keeps_most_recent_n() {
        let evictor = CountEvictor::of(3);
        let mut buf: Vec<TimestampedValue<i32>> = (1..=5)
            .map(|i| TimestampedValue::new(i, i as i64 * 100))
            .collect();
        let window = GlobalWindow;
        let c = evictor_ctx();
        <CountEvictor as Evictor<i32, GlobalWindow>>::evict_before(&evictor, &mut buf, &window, &c);
        // Java evicts from the FRONT, keeping the last 3.
        assert_eq!(buf.len(), 3);
        assert_eq!(buf[0].value, 3);
        assert_eq!(buf[2].value, 5);
    }

    #[test]
    fn count_evictor_evict_before_default_does_not_apply_after() {
        let evictor = CountEvictor::of(2);
        let mut buf: Vec<TimestampedValue<i32>> = (1..=5)
            .map(|i| TimestampedValue::new(i, i as i64))
            .collect();
        let window = GlobalWindow;
        let c = evictor_ctx();
        <CountEvictor as Evictor<i32, GlobalWindow>>::evict_after(&evictor, &mut buf, &window, &c);
        assert_eq!(
            buf.len(),
            5,
            "default evictor should not run on evict_after"
        );
    }

    #[test]
    fn count_evictor_evict_after_variant_runs_only_on_evict_after() {
        let evictor = CountEvictor::of_evict_after(2);
        let mut buf: Vec<TimestampedValue<i32>> = (1..=5)
            .map(|i| TimestampedValue::new(i, i as i64))
            .collect();
        let window = GlobalWindow;
        let c = evictor_ctx();
        <CountEvictor as Evictor<i32, GlobalWindow>>::evict_before(&evictor, &mut buf, &window, &c);
        assert_eq!(buf.len(), 5);
        <CountEvictor as Evictor<i32, GlobalWindow>>::evict_after(&evictor, &mut buf, &window, &c);
        assert_eq!(buf.len(), 2);
    }

    // ── TimeEvictor ──

    #[test]
    fn time_evictor_keeps_recent_window() {
        let evictor = TimeEvictor::of(Duration::from_millis(500));
        let mut buf: Vec<TimestampedValue<i32>> = vec![
            TimestampedValue::new(1, 100),
            TimestampedValue::new(2, 500),
            TimestampedValue::new(3, 900),
            TimestampedValue::new(4, 1000),
        ];
        let window = GlobalWindow;
        let c = evictor_ctx();
        <TimeEvictor as Evictor<i32, GlobalWindow>>::evict_before(&evictor, &mut buf, &window, &c);
        // Cutoff = 1000 - 500 = 500. Java's TimeEvictor removes elements
        // where ts <= cutoff, i.e. timestamps 100 and 500 → kept: 900, 1000.
        assert_eq!(buf.iter().map(|e| e.value).collect::<Vec<_>>(), vec![3, 4]);
    }

    #[test]
    fn time_evictor_empty_input_is_noop() {
        let evictor = TimeEvictor::of(Duration::from_secs(1));
        let mut buf: Vec<TimestampedValue<i32>> = Vec::new();
        let window = GlobalWindow;
        let c = evictor_ctx();
        <TimeEvictor as Evictor<i32, GlobalWindow>>::evict_before(&evictor, &mut buf, &window, &c);
        assert!(buf.is_empty());
    }
}
