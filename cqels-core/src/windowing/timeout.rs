//! `ProcessingTimeoutTrigger` — fires when the nested trigger fires *or*
//! a processing-time timeout expires.
//!
//! Mirrors Java's `org.cqels.windowing.triggers.ProcessingTimeoutTrigger`.
//! The timeout is anchored relative to processing time; when a new element
//! arrives, the trigger may reset the timer (matching Java's
//! `resetTimerOnNewRecord` flag).

use super::{Trigger, TriggerContext, TriggerResult, WindowBounds};

/// Trigger combinator that adds a processing-time deadline to a nested
/// trigger.
///
/// If the nested trigger fires, the outer trigger fires (and clears its
/// pending timer). Otherwise, a processing-time timer is registered; when
/// the timer fires, the outer trigger fires regardless of the nested
/// state.
pub struct ProcessingTimeoutTrigger<T, W: WindowBounds, Inner: Trigger<T, W>> {
    inner: Inner,
    interval_ms: i64,
    reset_timer_on_new_record: bool,
    clear_on_timeout: bool,
    pending_timeout: Option<i64>,
    _marker: std::marker::PhantomData<(T, W)>,
}

impl<T, W: WindowBounds, Inner: Trigger<T, W>> ProcessingTimeoutTrigger<T, W, Inner> {
    pub fn of(inner: Inner, interval_ms: i64) -> Self {
        Self {
            inner,
            interval_ms,
            reset_timer_on_new_record: false,
            clear_on_timeout: true,
            pending_timeout: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn reset_timer_on_new_record(mut self, reset: bool) -> Self {
        self.reset_timer_on_new_record = reset;
        self
    }

    pub fn clear_on_timeout(mut self, clear: bool) -> Self {
        self.clear_on_timeout = clear;
        self
    }
}

impl<T, W: WindowBounds, Inner: Trigger<T, W>> Trigger<T, W>
    for ProcessingTimeoutTrigger<T, W, Inner>
{
    fn on_element(
        &mut self,
        element: &T,
        timestamp: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        let inner_result = self.inner.on_element(element, timestamp, window, ctx);
        if inner_result.is_fire() {
            if let Some(pending) = self.pending_timeout.take() {
                ctx.delete_processing_time_timer(pending);
            }
            self.inner.clear(window, ctx);
            return inner_result;
        }

        let next_fire = ctx.current_processing_time() + self.interval_ms;
        if let Some(pending) = self.pending_timeout {
            if self.reset_timer_on_new_record {
                ctx.delete_processing_time_timer(pending);
                self.pending_timeout = None;
            }
        }
        if self.pending_timeout.is_none() {
            ctx.register_processing_time_timer(next_fire);
            self.pending_timeout = Some(next_fire);
        }

        inner_result
    }

    fn on_event_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        let inner_result = self.inner.on_event_time(time, window, ctx);
        if self.clear_on_timeout {
            self.clear(window, ctx);
        }
        if inner_result.is_purge() {
            TriggerResult::FireAndPurge
        } else {
            TriggerResult::Fire
        }
    }

    fn on_processing_time(
        &mut self,
        time: i64,
        window: &W,
        ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        let inner_result = self.inner.on_processing_time(time, window, ctx);
        if self.clear_on_timeout {
            self.clear(window, ctx);
        }
        if inner_result.is_purge() {
            TriggerResult::FireAndPurge
        } else {
            TriggerResult::Fire
        }
    }

    fn clear(&mut self, window: &W, ctx: &mut dyn TriggerContext) {
        if let Some(pending) = self.pending_timeout.take() {
            ctx.delete_processing_time_timer(pending);
        }
        self.inner.clear(window, ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowing::{CountTrigger, GlobalWindow, MockTriggerContext};

    #[test]
    fn timeout_registers_timer_on_first_element() {
        let mut t = ProcessingTimeoutTrigger::of(CountTrigger::of(100), 5_000);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext {
            processing_time: 1_000,
            ..Default::default()
        };
        let r = <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
        assert_eq!(ctx.processing_time_timers, vec![6_000]);
    }

    #[test]
    fn timeout_fires_on_processing_time_callback() {
        let mut t = ProcessingTimeoutTrigger::of(CountTrigger::of(100), 5_000);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext {
            processing_time: 1_000,
            ..Default::default()
        };
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        let r =
            <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_processing_time(
                &mut t, 6_000, &window, &mut ctx,
            );
        assert_eq!(r, TriggerResult::Fire);
    }

    #[test]
    fn timeout_inner_fire_clears_pending_timer() {
        let mut t = ProcessingTimeoutTrigger::of(CountTrigger::of(2), 10_000);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        // First element registers timer, no fire.
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(ctx.processing_time_timers.len(), 1);
        // Second element triggers inner CountTrigger → fires, deletes timer.
        let r = <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Fire);
        assert!(
            ctx.processing_time_timers.is_empty(),
            "inner fire should delete pending timeout timer"
        );
    }

    #[test]
    fn reset_timer_on_new_record_replaces_pending_timer() {
        let mut t = ProcessingTimeoutTrigger::of(CountTrigger::of(100), 1_000)
            .reset_timer_on_new_record(true);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext {
            processing_time: 0,
            ..Default::default()
        };
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(ctx.processing_time_timers, vec![1_000]);
        ctx.processing_time = 500;
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        // Old timer deleted, new one at 500 + 1_000 = 1_500.
        assert_eq!(ctx.processing_time_timers, vec![1_500]);
    }

    #[test]
    fn clear_removes_pending_timer() {
        let mut t = ProcessingTimeoutTrigger::of(CountTrigger::of(100), 5_000);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(ctx.processing_time_timers.len(), 1);
        <ProcessingTimeoutTrigger<i32, _, _> as Trigger<i32, GlobalWindow>>::clear(
            &mut t, &window, &mut ctx,
        );
        assert!(ctx.processing_time_timers.is_empty());
    }
}
