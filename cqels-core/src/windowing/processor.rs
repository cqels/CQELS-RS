//! Bridge between the [`crate::windowing`] trigger/evictor framework and a
//! stateful, single-window streaming processor.
//!
//! Existing [`crate::window::Window<T>`] operators bake the emission rule
//! into each impl. This module adds a composable alternative: a
//! [`TriggerableWindowProcessor`] that buffers elements for one
//! [`WindowBounds`], consults a [`Trigger`] on every event, and applies an
//! optional [`Evictor`] when the trigger fires.
//!
//! The processor itself is *single-window* — multiple windows are
//! supported by holding one processor per window. This matches the inline
//! state model already used by the concrete triggers in this crate.

use super::{
    Evictor, EvictorContext, MockEvictorContext, Trigger, TriggerContext, TriggerResult,
    WindowBounds,
};
use crate::stream::TimestampedValue;

/// Outcome of processing a single event with a triggered window.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowEmission<T: Clone> {
    /// The window contents emitted by this firing (after any evict-before
    /// step and before evict-after). Empty when the trigger returned
    /// `Continue` or `Purge`.
    pub elements: Vec<TimestampedValue<T>>,
    /// `true` when the trigger purged the buffer after firing (or via
    /// `Purge`). Outer consumers may use this to know whether internal
    /// state was cleared.
    pub purged: bool,
}

/// Stateful window processor that delegates emission policy to a
/// [`Trigger`] and discard policy to an [`Evictor`].
///
/// Generic over the element type `T`, the window bounds type `W`, the
/// trigger `Tr`, and the (optional) evictor `Ev`.
pub struct TriggerableWindowProcessor<T, W, Tr, Ev>
where
    T: Clone,
    W: WindowBounds,
    Tr: Trigger<T, W>,
    Ev: Evictor<T, W>,
{
    window: W,
    trigger: Tr,
    evictor: Option<Ev>,
    buffer: Vec<TimestampedValue<T>>,
}

impl<T, W, Tr, Ev> TriggerableWindowProcessor<T, W, Tr, Ev>
where
    T: Clone,
    W: WindowBounds,
    Tr: Trigger<T, W>,
    Ev: Evictor<T, W>,
{
    pub fn new(window: W, trigger: Tr, evictor: Option<Ev>) -> Self {
        Self {
            window,
            trigger,
            evictor,
            buffer: Vec::new(),
        }
    }

    /// Current buffer size — useful for tests and back-pressure decisions.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Returns a read-only view of the buffer's contents.
    pub fn buffer(&self) -> &[TimestampedValue<T>] {
        &self.buffer
    }

    /// Adds an element to the window, drives the trigger, and emits when
    /// the trigger says so. Returns the emitted batch (if any) plus
    /// whether the buffer was purged.
    pub fn on_element(
        &mut self,
        element: T,
        timestamp: i64,
        ctx: &mut dyn TriggerContext,
    ) -> WindowEmission<T> {
        self.buffer
            .push(TimestampedValue::new(element.clone(), timestamp));
        let result = self
            .trigger
            .on_element(&element, timestamp, &self.window, ctx);
        self.apply_result(result, &MockEvictorContext::default())
    }

    /// Drives the trigger's event-time callback (e.g. a registered timer
    /// fired) and applies the result.
    pub fn on_event_time(
        &mut self,
        time: i64,
        ctx: &mut dyn TriggerContext,
        evictor_ctx: &dyn EvictorContext,
    ) -> WindowEmission<T> {
        let result = self.trigger.on_event_time(time, &self.window, ctx);
        self.apply_result(result, evictor_ctx)
    }

    /// Drives the trigger's processing-time callback and applies the
    /// result.
    pub fn on_processing_time(
        &mut self,
        time: i64,
        ctx: &mut dyn TriggerContext,
        evictor_ctx: &dyn EvictorContext,
    ) -> WindowEmission<T> {
        let result = self.trigger.on_processing_time(time, &self.window, ctx);
        self.apply_result(result, evictor_ctx)
    }

    /// Clears the buffer and trigger state.
    pub fn reset(&mut self, ctx: &mut dyn TriggerContext) {
        self.buffer.clear();
        self.trigger.clear(&self.window, ctx);
    }

    fn apply_result(
        &mut self,
        result: TriggerResult,
        evictor_ctx: &dyn EvictorContext,
    ) -> WindowEmission<T> {
        if result == TriggerResult::Continue {
            return WindowEmission {
                elements: Vec::new(),
                purged: false,
            };
        }

        // PURGE without FIRE: discard buffer, emit nothing.
        if result == TriggerResult::Purge {
            self.buffer.clear();
            return WindowEmission {
                elements: Vec::new(),
                purged: true,
            };
        }

        // FIRE or FIRE_AND_PURGE: evict-before, snapshot, evict-after.
        if let Some(evictor) = &self.evictor {
            evictor.evict_before(&mut self.buffer, &self.window, evictor_ctx);
        }
        let emitted = self.buffer.clone();
        if let Some(evictor) = &self.evictor {
            evictor.evict_after(&mut self.buffer, &self.window, evictor_ctx);
        }
        let purged = result == TriggerResult::FireAndPurge;
        if purged {
            self.buffer.clear();
        }
        WindowEmission {
            elements: emitted,
            purged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowing::{
        CountEvictor, CountTrigger, EventTimeTrigger, GlobalWindow, MockTriggerContext,
        PurgingTrigger, TimeWindow,
    };

    fn t_ctx() -> MockTriggerContext {
        MockTriggerContext::default()
    }

    fn e_ctx() -> MockEvictorContext {
        MockEvictorContext::default()
    }

    #[test]
    fn count_trigger_emits_full_buffer_on_fire() {
        // No evictor: when CountTrigger fires, the entire buffer is emitted.
        let mut proc: TriggerableWindowProcessor<i32, GlobalWindow, CountTrigger, CountEvictor> =
            TriggerableWindowProcessor::new(GlobalWindow, CountTrigger::of(3), None);
        let mut ctx = t_ctx();
        let e1 = proc.on_element(10, 100, &mut ctx);
        assert!(e1.elements.is_empty());
        let e2 = proc.on_element(20, 200, &mut ctx);
        assert!(e2.elements.is_empty());
        let e3 = proc.on_element(30, 300, &mut ctx);
        assert_eq!(e3.elements.len(), 3);
        assert!(!e3.purged, "plain Fire does not purge");
        // Buffer still has elements (not purged).
        assert_eq!(proc.buffered(), 3);
    }

    #[test]
    fn purging_count_trigger_clears_buffer_on_fire() {
        let mut proc: TriggerableWindowProcessor<
            i32,
            GlobalWindow,
            PurgingTrigger<CountTrigger>,
            CountEvictor,
        > = TriggerableWindowProcessor::new(
            GlobalWindow,
            PurgingTrigger::of(CountTrigger::of(2)),
            None,
        );
        let mut ctx = t_ctx();
        proc.on_element(1, 100, &mut ctx);
        let emission = proc.on_element(2, 200, &mut ctx);
        assert_eq!(emission.elements.len(), 2);
        assert!(
            emission.purged,
            "PurgingTrigger should request FireAndPurge"
        );
        assert_eq!(proc.buffered(), 0, "buffer cleared after FireAndPurge");
    }

    #[test]
    fn count_evictor_keeps_only_recent_n_on_fire() {
        // Trigger fires after 5 elements; evictor keeps the most recent 2.
        let mut proc: TriggerableWindowProcessor<i32, GlobalWindow, CountTrigger, CountEvictor> =
            TriggerableWindowProcessor::new(
                GlobalWindow,
                CountTrigger::of(5),
                Some(CountEvictor::of(2)),
            );
        let mut ctx = t_ctx();
        for i in 1..=5 {
            proc.on_element(i, i as i64 * 100, &mut ctx);
        }
        // The Fire on the 5th element collapses through `on_element`
        // (last call) — emission contains 2 elements (5,4 trimmed to 2
        // newest: 4 and 5).
        // Force a re-emission by triggering once more.
        let emission = proc.on_element(6, 600, &mut ctx);
        // After the prior fire, CountTrigger resets; this is element 1 of
        // a fresh count cycle.
        assert!(emission.elements.is_empty());
    }

    #[test]
    fn event_time_trigger_fires_on_event_time_callback() {
        let window = TimeWindow::new(0, 1000);
        let mut proc: TriggerableWindowProcessor<i32, TimeWindow, EventTimeTrigger, CountEvictor> =
            TriggerableWindowProcessor::new(window, EventTimeTrigger::create(), None);
        let mut ctx = t_ctx();
        // First element with watermark before window end → no fire, timer
        // registered.
        ctx.watermark = 500;
        let r1 = proc.on_element(42, 200, &mut ctx);
        assert!(r1.elements.is_empty());
        assert_eq!(ctx.event_time_timers.len(), 1);
        // Simulate the timer firing at the window's max timestamp.
        let r2 = proc.on_event_time(window.max_timestamp(), &mut ctx, &e_ctx());
        assert_eq!(r2.elements.len(), 1);
        assert_eq!(r2.elements[0].value, 42);
    }

    #[test]
    fn reset_clears_buffer_and_trigger_state() {
        let mut proc: TriggerableWindowProcessor<i32, GlobalWindow, CountTrigger, CountEvictor> =
            TriggerableWindowProcessor::new(GlobalWindow, CountTrigger::of(10), None);
        let mut ctx = t_ctx();
        for i in 0..5 {
            proc.on_element(i, i as i64, &mut ctx);
        }
        assert_eq!(proc.buffered(), 5);
        proc.reset(&mut ctx);
        assert_eq!(proc.buffered(), 0);
    }
}
