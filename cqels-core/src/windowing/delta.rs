//! Delta-based triggers and evictors.
//!
//! Mirrors Java's `DeltaTrigger` and `DeltaEvictor`. A [`DeltaFunction`]
//! computes a scalar "distance" between two element values; a delta trigger
//! fires when the distance from the last-seen element exceeds a threshold,
//! and a delta evictor discards elements whose distance from the newest
//! exceeds the threshold.

use super::{Evictor, EvictorContext, Trigger, TriggerContext, TriggerResult, WindowBounds};
use crate::stream::TimestampedValue;

/// Computes a scalar delta between two element values.
///
/// Mirrors Java's `org.cqels.windowing.functions.DeltaFunction`.
pub trait DeltaFunction<T>: Send + Sync {
    fn delta(&self, prior: &T, current: &T) -> f64;
}

impl<T, F> DeltaFunction<T> for F
where
    F: Fn(&T, &T) -> f64 + Send + Sync,
{
    fn delta(&self, prior: &T, current: &T) -> f64 {
        self(prior, current)
    }
}

/// Trigger that fires when the delta between the last-fired element and a
/// new element exceeds the configured threshold.
///
/// Mirrors Java's `DeltaTrigger`. Inline state matches the Rust trigger
/// model (one trigger instance per window) instead of Java's
/// framework-managed `ValueState`.
pub struct DeltaTrigger<T: Clone, D: DeltaFunction<T>> {
    delta_fn: D,
    threshold: f64,
    last_element: Option<T>,
}

impl<T: Clone, D: DeltaFunction<T>> DeltaTrigger<T, D> {
    pub fn of(threshold: f64, delta_fn: D) -> Self {
        Self {
            delta_fn,
            threshold,
            last_element: None,
        }
    }
}

impl<T: Clone, W: WindowBounds, D: DeltaFunction<T>> Trigger<T, W> for DeltaTrigger<T, D> {
    fn on_element(
        &mut self,
        element: &T,
        _timestamp: i64,
        _window: &W,
        _ctx: &mut dyn TriggerContext,
    ) -> TriggerResult {
        match &self.last_element {
            None => {
                self.last_element = Some(element.clone());
                TriggerResult::Continue
            }
            Some(prior) => {
                if self.delta_fn.delta(prior, element) > self.threshold {
                    self.last_element = Some(element.clone());
                    TriggerResult::Fire
                } else {
                    TriggerResult::Continue
                }
            }
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
        self.last_element = None;
    }
}

/// Evictor that discards elements whose delta from the newest element is
/// at or above the threshold.
///
/// Mirrors Java's `DeltaEvictor`.
pub struct DeltaEvictor<T, D: DeltaFunction<T>> {
    delta_fn: D,
    threshold: f64,
    evict_after: bool,
    _marker: std::marker::PhantomData<T>,
}

impl<T, D: DeltaFunction<T>> DeltaEvictor<T, D> {
    pub fn of(threshold: f64, delta_fn: D) -> Self {
        Self {
            delta_fn,
            threshold,
            evict_after: false,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn of_evict_after(threshold: f64, delta_fn: D) -> Self {
        Self {
            delta_fn,
            threshold,
            evict_after: true,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Clone, W: WindowBounds, D: DeltaFunction<T>> Evictor<T, W> for DeltaEvictor<T, D> {
    fn evict_before(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if !self.evict_after {
            evict_by_delta(elements, &self.delta_fn, self.threshold);
        }
    }

    fn evict_after(
        &self,
        elements: &mut Vec<TimestampedValue<T>>,
        _window: &W,
        _ctx: &dyn EvictorContext,
    ) {
        if self.evict_after {
            evict_by_delta(elements, &self.delta_fn, self.threshold);
        }
    }
}

fn evict_by_delta<T: Clone, D: DeltaFunction<T>>(
    elements: &mut Vec<TimestampedValue<T>>,
    delta_fn: &D,
    threshold: f64,
) {
    let Some(last_value) = elements.last().map(|e| e.value.clone()) else {
        return;
    };
    elements.retain(|e| delta_fn.delta(&e.value, &last_value) < threshold);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowing::{GlobalWindow, MockEvictorContext, MockTriggerContext};

    fn abs_delta(a: &i64, b: &i64) -> f64 {
        (b - a).abs() as f64
    }

    #[test]
    fn delta_trigger_does_not_fire_within_threshold() {
        let mut t = DeltaTrigger::of(10.0, abs_delta);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        assert_eq!(
            <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
                &mut t, &0, 0, &window, &mut ctx
            ),
            TriggerResult::Continue
        );
        assert_eq!(
            <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
                &mut t, &5, 1, &window, &mut ctx
            ),
            TriggerResult::Continue
        );
    }

    #[test]
    fn delta_trigger_fires_when_threshold_exceeded() {
        let mut t = DeltaTrigger::of(10.0, abs_delta);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        assert_eq!(
            <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
                &mut t, &50, 1, &window, &mut ctx
            ),
            TriggerResult::Fire
        );
    }

    #[test]
    fn delta_trigger_updates_baseline_on_fire() {
        // After firing, the baseline becomes the firing element, so a
        // near-by element does not re-fire.
        let mut t = DeltaTrigger::of(10.0, abs_delta);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &0, 0, &window, &mut ctx,
        );
        <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &50, 1, &window, &mut ctx,
        );
        // Element at 53 is within 10 of the new baseline (50) → no fire.
        let r = <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &53, 2, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
    }

    #[test]
    fn delta_trigger_clear_resets_baseline() {
        let mut t = DeltaTrigger::of(10.0, abs_delta);
        let window = GlobalWindow;
        let mut ctx = MockTriggerContext::default();
        <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &100, 0, &window, &mut ctx,
        );
        <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::clear(&mut t, &window, &mut ctx);
        // After clear, first element again continues without firing.
        let r = <DeltaTrigger<i64, _> as Trigger<i64, GlobalWindow>>::on_element(
            &mut t, &200, 1, &window, &mut ctx,
        );
        assert_eq!(r, TriggerResult::Continue);
    }

    #[test]
    fn delta_evictor_drops_far_elements() {
        let evictor = DeltaEvictor::of(10.0, abs_delta);
        let mut buf: Vec<TimestampedValue<i64>> = vec![
            TimestampedValue::new(0, 100),
            TimestampedValue::new(3, 200),
            TimestampedValue::new(95, 300),
            TimestampedValue::new(100, 400),
        ];
        let window = GlobalWindow;
        let ctx = MockEvictorContext::default();
        <DeltaEvictor<i64, _> as Evictor<i64, GlobalWindow>>::evict_before(
            &evictor, &mut buf, &window, &ctx,
        );
        // Last is 100. Threshold is 10. Keep only elements with delta < 10:
        // 95 (delta 5) and 100 (delta 0). Drop 0 and 3.
        let values: Vec<_> = buf.iter().map(|e| e.value).collect();
        assert_eq!(values, vec![95, 100]);
    }

    #[test]
    fn delta_evictor_empty_input_is_noop() {
        let evictor = DeltaEvictor::of(1.0, abs_delta);
        let mut buf: Vec<TimestampedValue<i64>> = Vec::new();
        let window = GlobalWindow;
        let ctx = MockEvictorContext::default();
        <DeltaEvictor<i64, _> as Evictor<i64, GlobalWindow>>::evict_before(
            &evictor, &mut buf, &window, &ctx,
        );
        assert!(buf.is_empty());
    }
}
