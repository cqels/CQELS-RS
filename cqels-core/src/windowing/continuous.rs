//! Continuous (periodic) variants of the event-time and processing-time
//! triggers.
//!
//! Mirrors Java's `ContinuousEventTimeTrigger` and
//! `ContinuousProcessingTimeTrigger`. Both fire periodically at a fixed
//! interval: every `interval_ms` of event time / processing time, the
//! trigger emits a FIRE result and registers the next firing.
//!
//! Inline state mirrors the existing Rust trigger model (one instance per
//! window) — Java uses framework-managed `ReducingState<Long>` to coordinate
//! across multiple windows.

use super::{Trigger, TriggerContext, TriggerResult, WindowBounds};

/// Fires periodically every `interval_ms` of event time, and also at the
/// window's max timestamp.
///
/// Mirrors Java's `ContinuousEventTimeTrigger`.
#[derive(Clone, Debug)]
pub struct ContinuousEventTimeTrigger {
    interval_ms: i64,
    next_fire: Option<i64>,
}

impl ContinuousEventTimeTrigger {
    pub fn of(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            next_fire: None,
        }
    }
}

impl<T, W: WindowBounds> Trigger<T, W> for ContinuousEventTimeTrigger {
    fn on_element(
        &mut self,
        _element: &T,
        timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if window.max_timestamp() <= ctx.current_watermark() {
            return TriggerResult::Fire;
        }
        ctx.register_event_time_timer(window.max_timestamp());

        if self.next_fire.is_none() {
            // Align to interval boundary.
            let base = timestamp - (timestamp.rem_euclid(self.interval_ms.max(1)));
            let candidate = (base + self.interval_ms).min(window.max_timestamp());
            ctx.register_event_time_timer(candidate);
            self.next_fire = Some(candidate);
        }

        TriggerResult::Continue
    }

    fn on_event_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if time == window.max_timestamp() {
            return TriggerResult::Fire;
        }
        if Some(time) == self.next_fire {
            self.next_fire = None;
            let candidate = (time + self.interval_ms).min(window.max_timestamp());
            ctx.register_event_time_timer(candidate);
            self.next_fire = Some(candidate);
            return TriggerResult::Fire;
        }
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

    fn clear(&mut self, _window: &W, ctx: &mut dyn TriggerContext) {
        if let Some(next) = self.next_fire.take() {
            ctx.delete_event_time_timer(next);
        }
    }
}

/// Fires periodically every `interval_ms` of processing time.
///
/// Mirrors Java's `ContinuousProcessingTimeTrigger`.
#[derive(Clone, Debug)]
pub struct ContinuousProcessingTimeTrigger {
    interval_ms: i64,
    next_fire: Option<i64>,
}

impl ContinuousProcessingTimeTrigger {
    pub fn of(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            next_fire: None,
        }
    }
}

impl<T, W: WindowBounds> Trigger<T, W> for ContinuousProcessingTimeTrigger {
    fn on_element(
        &mut self,
        _element: &T,
        _timestamp: i64,
        _window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if self.next_fire.is_none() {
            let candidate = ctx.current_processing_time() + self.interval_ms;
            ctx.register_processing_time_timer(candidate);
            self.next_fire = Some(candidate);
        }
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
        time: i64,
        _window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        if Some(time) == self.next_fire {
            self.next_fire = None;
            let candidate = time + self.interval_ms;
            ctx.register_processing_time_timer(candidate);
            self.next_fire = Some(candidate);
            return TriggerResult::Fire;
        }
        TriggerResult::Continue
    }

    fn clear(&mut self, _window: &W, ctx: &mut dyn TriggerContext) {
        if let Some(next) = self.next_fire.take() {
            ctx.delete_processing_time_timer(next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowing::{GlobalWindow, MockTriggerContext, TimeWindow};

    #[test]
    fn continuous_event_time_registers_periodic_timer() {
        let mut t = ContinuousEventTimeTrigger::of(1_000);
        let window = TimeWindow::new(0, 10_000);
        let mut ctx = MockTriggerContext::default();
        let r = <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 250, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
        // Two timers should be registered: window.max_timestamp() (9_999)
        // and the periodic boundary (1_000).
        assert!(ctx.event_time_timers.contains(&window.max_timestamp()));
        assert!(ctx.event_time_timers.contains(&1_000));
    }

    #[test]
    fn continuous_event_time_fires_when_watermark_crosses_max() {
        let mut t = ContinuousEventTimeTrigger::of(1_000);
        let window = TimeWindow::new(0, 1_000);
        let mut ctx = MockTriggerContext {
            watermark: 1_500,
            ..Default::default()
        };
        let r = <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 100, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Fire);
    }

    #[test]
    fn continuous_event_time_fires_on_periodic_callback() {
        let mut t = ContinuousEventTimeTrigger::of(1_000);
        let window = TimeWindow::new(0, 10_000);
        let mut ctx = MockTriggerContext::default();
        <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 250, &window, &mut ctx,
        );
        // First periodic boundary = 1_000.
        let r = <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::on_event_time(
            &mut t, 1_000, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Fire);
        // After firing, a new timer should be registered for 2_000.
        assert!(ctx.event_time_timers.contains(&2_000));
    }

    #[test]
    fn continuous_processing_time_registers_initial_timer() {
        let mut t = ContinuousProcessingTimeTrigger::of(500);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext {
            processing_time: 100,
            ..Default::default()
        };
        let r = <ContinuousProcessingTimeTrigger as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
        assert_eq!(ctx.processing_time_timers, vec![600]);
    }

    #[test]
    fn continuous_processing_time_fires_periodically() {
        let mut t = ContinuousProcessingTimeTrigger::of(500);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <ContinuousProcessingTimeTrigger as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        let r = <ContinuousProcessingTimeTrigger as Trigger<i32, GlobalWindow>>::on_processing_time(
            &mut t, 500, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Fire);
        assert!(
            ctx.processing_time_timers.contains(&1_000),
            "next periodic timer should be registered after firing",
        );
    }

    #[test]
    fn continuous_processing_time_ignores_unmatched_timer() {
        let mut t = ContinuousProcessingTimeTrigger::of(500);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <ContinuousProcessingTimeTrigger as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        let r = <ContinuousProcessingTimeTrigger as Trigger<i32, GlobalWindow>>::on_processing_time(
            &mut t, 999, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
    }

    #[test]
    fn continuous_event_time_clear_removes_pending_timer() {
        let mut t = ContinuousEventTimeTrigger::of(1_000);
        let window = TimeWindow::new(0, 10_000);
        let mut ctx = MockTriggerContext::default();
        <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::on_element(
            &mut t, &0, 250, &window, &mut ctx,
        );
        <ContinuousEventTimeTrigger as Trigger<i32, TimeWindow>>::clear(&mut t, &window, &mut ctx);
        assert!(
            !ctx.event_time_timers.contains(&1_000),
            "clear should remove the periodic timer"
        );
    }
}
