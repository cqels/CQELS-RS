use std::marker::PhantomData;

/// Core SWAG (Sliding Window Aggregation) operation trait.
///
/// Represents a monoidal aggregation with identity, lift, combine, and lower operations.
/// Maps to Java's `SwagOp<IN, PARTIAL, OUT>`.
pub trait SwagOp<In, Partial: Clone, Out>: Send + Sync {
    /// Returns the identity element for the monoid.
    fn identity(&self) -> Partial;

    /// Lifts an input value to a partial aggregate.
    fn lift(&self, value: &In) -> Partial;

    /// Combines two partial aggregates (must be associative).
    fn combine(&self, a: &Partial, b: &Partial) -> Partial;

    /// Lowers a partial aggregate to an output value.
    fn lower(&self, aggregate: &Partial) -> Out;

    /// Returns the inverse of a partial aggregate, if the operation is invertible.
    fn invert(&self, _value: &Partial) -> Option<Partial> {
        None
    }

    /// Returns `true` if this operation supports efficient inversion.
    fn is_invertible(&self) -> bool {
        false
    }

    /// Returns a descriptive name for this operation.
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Concrete SWAG operations
// ---------------------------------------------------------------------------

/// SWAG count operation — counts elements. Invertible.
pub struct SwagCountOp<T>(PhantomData<T>);

impl<T> SwagCountOp<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for SwagCountOp<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync> SwagOp<T, i64, i64> for SwagCountOp<T> {
    fn identity(&self) -> i64 {
        0
    }
    fn lift(&self, _value: &T) -> i64 {
        1
    }
    fn combine(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }
    fn lower(&self, aggregate: &i64) -> i64 {
        *aggregate
    }
    fn invert(&self, value: &i64) -> Option<i64> {
        Some(-value)
    }
    fn is_invertible(&self) -> bool {
        true
    }
    fn name(&self) -> &str {
        "COUNT"
    }
}

/// SWAG sum operation — sums f64 values. Invertible.
pub struct SwagSumOp<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> SwagSumOp<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> SwagOp<T, f64, f64> for SwagSumOp<T, F> {
    fn identity(&self) -> f64 {
        0.0
    }
    fn lift(&self, value: &T) -> f64 {
        (self.extractor)(value)
    }
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        a + b
    }
    fn lower(&self, aggregate: &f64) -> f64 {
        *aggregate
    }
    fn invert(&self, value: &f64) -> Option<f64> {
        Some(-value)
    }
    fn is_invertible(&self) -> bool {
        true
    }
    fn name(&self) -> &str {
        "SUM"
    }
}

/// Partial aggregate for mean computation.
#[derive(Clone, Debug, Default)]
pub struct MeanPartial {
    pub sum: f64,
    pub count: i64,
}

/// SWAG mean operation. Invertible.
pub struct SwagMeanOp<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> SwagMeanOp<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> SwagOp<T, MeanPartial, f64>
    for SwagMeanOp<T, F>
{
    fn identity(&self) -> MeanPartial {
        MeanPartial::default()
    }
    fn lift(&self, value: &T) -> MeanPartial {
        MeanPartial {
            sum: (self.extractor)(value),
            count: 1,
        }
    }
    fn combine(&self, a: &MeanPartial, b: &MeanPartial) -> MeanPartial {
        MeanPartial {
            sum: a.sum + b.sum,
            count: a.count + b.count,
        }
    }
    fn lower(&self, aggregate: &MeanPartial) -> f64 {
        if aggregate.count == 0 {
            0.0
        } else {
            aggregate.sum / aggregate.count as f64
        }
    }
    fn invert(&self, value: &MeanPartial) -> Option<MeanPartial> {
        Some(MeanPartial {
            sum: -value.sum,
            count: -value.count,
        })
    }
    fn is_invertible(&self) -> bool {
        true
    }
    fn name(&self) -> &str {
        "MEAN"
    }
}

/// SWAG max operation. NOT invertible.
pub struct SwagMaxOp<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> SwagMaxOp<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> SwagOp<T, f64, f64> for SwagMaxOp<T, F> {
    fn identity(&self) -> f64 {
        f64::NEG_INFINITY
    }
    fn lift(&self, value: &T) -> f64 {
        (self.extractor)(value)
    }
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        a.max(*b)
    }
    fn lower(&self, aggregate: &f64) -> f64 {
        *aggregate
    }
    fn name(&self) -> &str {
        "MAX"
    }
}

/// SWAG min operation. NOT invertible.
pub struct SwagMinOp<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> SwagMinOp<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> SwagOp<T, f64, f64> for SwagMinOp<T, F> {
    fn identity(&self) -> f64 {
        f64::INFINITY
    }
    fn lift(&self, value: &T) -> f64 {
        (self.extractor)(value)
    }
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        a.min(*b)
    }
    fn lower(&self, aggregate: &f64) -> f64 {
        *aggregate
    }
    fn name(&self) -> &str {
        "MIN"
    }
}

// ---------------------------------------------------------------------------
// Two-Stacks-Lite FIFO window algorithm
// ---------------------------------------------------------------------------

/// Two-Stacks-Lite FIFO window for efficient sliding window aggregation.
///
/// Provides amortized O(1) push, pop, and query operations.
/// Maps to Java's `TwoStacksLiteWindow`.
///
/// The back stack stores individual lifted partials along with cumulative prefix
/// aggregates. The front stack stores suffix aggregates (from each element to the
/// top of the stack). This enables correct flip operations for non-invertible monoids.
pub struct TwoStacksLiteWindow<In, Partial: Clone, Out, Op: SwagOp<In, Partial, Out>> {
    /// Front stack: suffix aggregates (element i combined with all above it).
    /// Top of vec = oldest element in the window.
    front_agg: Vec<Partial>,
    /// Back stack: individual lifted partials (in push order, oldest first).
    back_vals: Vec<Partial>,
    /// Back stack: cumulative prefix aggregate (combine of all elements from bottom to this point).
    back_agg: Vec<Partial>,
    op: Op,
    _phantom: PhantomData<(In, Out)>,
}

impl<In, Partial: Clone, Out, Op: SwagOp<In, Partial, Out>>
    TwoStacksLiteWindow<In, Partial, Out, Op>
{
    pub fn new(op: Op) -> Self {
        Self {
            front_agg: Vec::new(),
            back_vals: Vec::new(),
            back_agg: Vec::new(),
            op,
            _phantom: PhantomData,
        }
    }

    /// Pushes a value into the back of the window.
    pub fn push(&mut self, value: &In) {
        let partial = self.op.lift(value);
        let cumulative = self
            .back_agg
            .last()
            .map(|last| self.op.combine(last, &partial))
            .unwrap_or_else(|| partial.clone());
        self.back_vals.push(partial);
        self.back_agg.push(cumulative);
    }

    /// Pops the oldest value from the front of the window.
    pub fn pop(&mut self) {
        if self.front_agg.is_empty() {
            self.flip();
        }
        self.front_agg.pop();
    }

    /// Queries the current aggregate over the window.
    pub fn query(&self) -> Out {
        let front = self
            .front_agg
            .last()
            .cloned()
            .unwrap_or_else(|| self.op.identity());
        let back = self
            .back_agg
            .last()
            .cloned()
            .unwrap_or_else(|| self.op.identity());
        let combined = self.op.combine(&front, &back);
        self.op.lower(&combined)
    }

    /// Returns the number of elements in the window.
    pub fn len(&self) -> usize {
        self.front_agg.len() + self.back_vals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.front_agg.is_empty() && self.back_vals.is_empty()
    }

    fn flip(&mut self) {
        // Move all individual partials from back to front, building suffix aggregates.
        // The back is in push order (oldest first). We reverse so the oldest element
        // ends up at the top of the front stack (popped first).
        let vals: Vec<Partial> = self.back_vals.drain(..).collect();
        self.back_agg.clear();

        self.front_agg.clear();
        // Build suffix aggregates: iterate in reverse (newest to oldest).
        // front_agg[0] = newest individual partial
        // front_agg[n-1] = combine(oldest, ..., newest) = suffix from oldest
        let mut suffix_aggs: Vec<Partial> = Vec::with_capacity(vals.len());
        for val in vals.iter().rev() {
            let agg = if let Some(prev) = suffix_aggs.last() {
                self.op.combine(val, prev)
            } else {
                val.clone()
            };
            suffix_aggs.push(agg);
        }
        // Reverse so that the last element of front_agg is the oldest element's suffix
        // (i.e., the full aggregate from oldest to newest — which is what we pop last).
        // Actually, we want: front_agg.pop() removes the oldest element.
        // front_agg[top] = suffix aggregate starting from oldest element = full aggregate.
        // So front_agg should be: [newest_only, combine(2nd_newest, newest), ..., combine(oldest, ..., newest)]
        // That's exactly suffix_aggs in order.
        self.front_agg = suffix_aggs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_swag_count() {
        let op = SwagCountOp::<i32>::new();
        assert_eq!(op.identity(), 0);
        assert_eq!(op.lift(&42), 1);
        assert_eq!(op.combine(&3, &5), 8);
        assert_eq!(op.lower(&10), 10);
        assert!(op.is_invertible());
    }

    #[test]
    fn test_swag_sum() {
        let op = SwagSumOp::new(|x: &f64| *x);
        assert!((op.identity() - 0.0).abs() < f64::EPSILON);
        assert!((op.lift(&3.0) - 3.0).abs() < f64::EPSILON);
        assert!((op.combine(&2.0, &3.0) - 5.0).abs() < f64::EPSILON);
        assert!(op.is_invertible());
    }

    #[test]
    fn test_swag_mean() {
        let op = SwagMeanOp::new(|x: &f64| *x);
        let p1 = op.lift(&10.0);
        let p2 = op.lift(&20.0);
        let combined = op.combine(&p1, &p2);
        assert!((op.lower(&combined) - 15.0).abs() < f64::EPSILON);
        assert!(op.is_invertible());
    }

    #[test]
    fn test_swag_max() {
        let op = SwagMaxOp::new(|x: &f64| *x);
        let p1 = op.lift(&5.0);
        let p2 = op.lift(&10.0);
        assert!((op.combine(&p1, &p2) - 10.0).abs() < f64::EPSILON);
        assert!(!op.is_invertible());
    }

    #[test]
    fn test_swag_min() {
        let op = SwagMinOp::new(|x: &f64| *x);
        let p1 = op.lift(&5.0);
        let p2 = op.lift(&10.0);
        assert!((op.combine(&p1, &p2) - 5.0).abs() < f64::EPSILON);
        assert!(!op.is_invertible());
    }

    #[test]
    fn test_two_stacks_lite_basic() {
        let op = SwagSumOp::new(|x: &f64| *x);
        let mut window = TwoStacksLiteWindow::new(op);

        window.push(&1.0);
        window.push(&2.0);
        window.push(&3.0);
        assert_eq!(window.len(), 3);
        assert!((window.query() - 6.0).abs() < f64::EPSILON);

        window.pop();
        assert_eq!(window.len(), 2);
        assert!((window.query() - 5.0).abs() < f64::EPSILON);

        window.pop();
        assert!((window.query() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_two_stacks_lite_sliding() {
        let op = SwagCountOp::<i32>::new();
        let mut window = TwoStacksLiteWindow::new(op);

        // Simulate a sliding window of size 3
        for i in 0..5 {
            window.push(&i);
            if window.len() > 3 {
                window.pop();
            }
        }
        assert_eq!(window.len(), 3);
        assert_eq!(window.query(), 3);
    }

    #[test]
    fn test_two_stacks_lite_empty() {
        let op = SwagSumOp::new(|x: &f64| *x);
        let window = TwoStacksLiteWindow::new(op);
        assert!(window.is_empty());
        assert!((window.query() - 0.0).abs() < f64::EPSILON);
    }
}
