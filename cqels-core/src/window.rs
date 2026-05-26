use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use futures::Stream;

use crate::stream::{StreamEvent, Timestamped};

/// Supported window types that classify how a stream is partitioned into
/// finite batches.
///
/// Each variant corresponds to a distinct windowing strategy. Time-based
/// windows partition by elapsed time; count-based windows partition by the
/// number of elements; session windows partition by activity gaps.
///
/// Maps to Java's `WindowType` enum in CQELS 2.0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WindowType {
    TumblingTime,
    SlidingTime,
    TumblingCount,
    SlidingCount,
    Session,
    Custom,
}

impl fmt::Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::TumblingTime => write!(f, "TUMBLING_TIME"),
            WindowType::SlidingTime => write!(f, "SLIDING_TIME"),
            WindowType::TumblingCount => write!(f, "TUMBLING_COUNT"),
            WindowType::SlidingCount => write!(f, "SLIDING_COUNT"),
            WindowType::Session => write!(f, "SESSION"),
            WindowType::Custom => write!(f, "CUSTOM"),
        }
    }
}

/// A batch of stream elements collected within a single window evaluation.
///
/// Carries the elements together with the window's time boundaries and type,
/// enabling downstream operators (e.g., aggregation) to process the batch
/// in its temporal context.
///
/// Maps to Java's `WindowedBatch<T>` in CQELS 2.0.
///
/// # Examples
///
/// ```
/// use cqels_core::window::{WindowedBatch, WindowType};
///
/// let batch = WindowedBatch::new(vec![1, 2, 3], 0, 5000, WindowType::TumblingTime);
/// assert_eq!(batch.size(), 3);
/// assert!(!batch.is_empty());
/// assert_eq!(batch.window_start, 0);
/// assert_eq!(batch.window_end, 5000);
/// ```
#[derive(Clone, Debug)]
pub struct WindowedBatch<T> {
    pub elements: Vec<T>,
    pub window_start: i64,
    pub window_end: i64,
    pub window_type: WindowType,
}

impl<T> WindowedBatch<T> {
    pub fn new(
        elements: Vec<T>,
        window_start: i64,
        window_end: i64,
        window_type: WindowType,
    ) -> Self {
        Self {
            elements,
            window_start,
            window_end,
            window_type,
        }
    }

    pub fn size(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl<T: fmt::Debug> fmt::Display for WindowedBatch<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WindowedBatch[type={}, start={}, end={}, size={}]",
            self.window_type,
            self.window_start,
            self.window_end,
            self.size()
        )
    }
}

/// A declarative window specification parsed from CQELS-QL queries.
///
/// `WindowSpec` captures the *parameters* of a window (size, slide, count)
/// without applying them to a stream. It is typically produced by the query
/// parser and later converted into a concrete [`Window`] implementation.
///
/// Maps to Java's `WindowSpec` in the CQELS 2.0 query language.
///
/// # Variants
///
/// | Variant | CQELS-QL Syntax | Meaning |
/// |---------|----------------|---------|
/// | `Now` | `[NOW]` | Process only the latest element |
/// | `Range(d)` | `[RANGE d]` | Time-based tumbling window of duration `d` |
/// | `RangeSlide(d, s)` | `[RANGE d SLIDE s]` | Time-based sliding window |
/// | `Rows(n)` | `[ROWS n]` | Count-based tumbling window |
/// | `RowsSlide(n, s)` | `[ROWS n SLIDE s]` | Count-based sliding window |
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum WindowSpec {
    /// Process only the most recent element (NOW window).
    Now,
    /// Time-based tumbling window with the given duration.
    Range(Duration),
    /// Time-based sliding window: `(size, slide)`.
    RangeSlide(Duration, Duration),
    /// Count-based tumbling window holding at most `n` elements.
    Rows(usize),
    /// Count-based sliding window: `(window_size, slide_size)`.
    RowsSlide(usize, usize),
}

impl WindowSpec {
    pub fn window_type(&self) -> WindowType {
        match self {
            WindowSpec::Now => WindowType::TumblingTime,
            WindowSpec::Range(_) => WindowType::TumblingTime,
            WindowSpec::RangeSlide(_, _) => WindowType::SlidingTime,
            WindowSpec::Rows(_) => WindowType::TumblingCount,
            WindowSpec::RowsSlide(_, _) => WindowType::SlidingCount,
        }
    }
}

/// Configuration for the event-aware [`Window::apply_events`] method.
///
/// Carries policy parameters that are not part of the window's identity
/// (size, slide) but govern how it reacts to watermarks. The parity spec
/// `graph.stream.WindowOperator` covers `allowed_lateness_ms` (B7).
///
/// Window-size lives on the concrete window (`TumblingWindow::size`,
/// `SlidingWindow::size`, …) and is not duplicated here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WindowConfig {
    /// Lateness budget. After a window has fired at watermark `w`, a late
    /// record with `t < w` is admitted (and re-fires the window) only while
    /// the watermark is strictly less than `window_end + allowed_lateness_ms`.
    /// Past that boundary, late records are dropped silently.
    ///
    /// `0` means "drop any record arriving after its window has fired"
    /// (the default).
    pub allowed_lateness_ms: i64,
}

impl WindowConfig {
    /// Builds a new `WindowConfig`. Panics if `allowed_lateness_ms < 0` —
    /// negative lateness has no defined meaning under the parity spec and
    /// Java's `WindowConfig` constructor throws `IllegalArgumentException`
    /// for the same input; this assert keeps the two surfaces aligned.
    pub const fn new(allowed_lateness_ms: i64) -> Self {
        assert!(
            allowed_lateness_ms >= 0,
            "WindowConfig::new: allowed_lateness_ms must be >= 0"
        );
        Self {
            allowed_lateness_ms,
        }
    }
}

/// Terminal error surfaced by the event-aware window operator on the
/// Result-bearing output stream ([`Window::apply_events_result`] /
/// [`Window::apply_events_dedup_result`]).
///
/// Carries the upstream `kind` and optional `message` verbatim from the
/// `StreamEvent::Error` that triggered termination. After this item, the
/// stream returns `None` and emits no further batches — spec
/// `graph.stream.WindowOperator` B6.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WindowError {
    pub kind: String,
    pub message: Option<String>,
}

impl WindowError {
    pub fn new(kind: impl Into<String>, message: Option<impl Into<String>>) -> Self {
        Self {
            kind: kind.into(),
            message: message.map(Into::into),
        }
    }
}

impl fmt::Display for WindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(m) => write!(f, "{}: {}", self.kind, m),
            None => write!(f, "{}", self.kind),
        }
    }
}

impl std::error::Error for WindowError {}

/// Trait for window operators that transform an unbounded stream of
/// timestamped elements into a stream of finite [`WindowedBatch`] segments.
///
/// Implementors define the windowing strategy (tumbling, sliding, session,
/// count-based) by consuming the input stream and emitting batches when
/// window boundaries are reached.
///
/// Maps to Java's `Window<T extends StreamElement>` interface in CQELS 2.0.
///
/// # Type Parameters
///
/// * `T` -- The stream element type. Must implement [`Timestamped`] so the
///   window can inspect event times.
///
/// # `apply` vs `apply_events`
///
/// The trait carries two entry points. [`Window::apply`] is the original
/// record-only operator (no watermarks, no lateness) used by the bulk of
/// the engine and most tests. [`Window::apply_events`] is the
/// watermark-aware variant introduced for the parity contract — it consumes
/// [`StreamEvent`]s (records interleaved with watermark advances) and
/// honors `WindowConfig::allowed_lateness_ms`.
///
/// Implementations may override `apply_events` for native watermark
/// handling (see [`TumblingWindow`]'s override). The default impl strips
/// watermarks and delegates to `apply`, so any existing `Window<T>`
/// keeps working without modification.
pub trait Window<T: Timestamped + Clone + Send + 'static>: Send + Sync {
    /// Applies this window operator to the given input stream, producing a
    /// new stream of [`WindowedBatch`] values.
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>;

    /// Returns the [`WindowType`] that classifies this window.
    fn window_type(&self) -> WindowType;

    /// Event-aware variant of [`apply`](Self::apply): consumes
    /// [`StreamEvent`]s (records and watermark advances) and produces
    /// batches that fire on watermark progression rather than on
    /// later-keyed record arrival.
    ///
    /// The default implementation strips watermarks and delegates to
    /// `apply`, so existing windows compile unchanged but ignore the
    /// watermark + lateness contract from the parity spec. Window types
    /// that participate in event-time semantics (currently
    /// [`TumblingWindow`]) should override this method.
    fn apply_events(
        &self,
        _config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        // Strip Watermarks. Spec B6: on upstream Error, end the inner
        // stream immediately via take_while so apply() sees no further
        // records and downstream output ends without batches. (For the
        // default impl this is best-effort — windows that buffer
        // internally via apply() may still flush at upstream-complete.
        // TumblingWindow's override handles Error precisely with no flush.)
        let records = stream
            .take_while(|e| {
                let stop = e.is_error();
                async move { !stop }
            })
            .filter_map(|e| async move {
                match e {
                    StreamEvent::Record { value, .. } => Some(value),
                    StreamEvent::Watermark { .. } | StreamEvent::Error { .. } => None,
                }
            });
        self.apply(Box::pin(records))
    }

    /// Dedup-aware variant of [`apply_events`](Self::apply_events): same
    /// stream-event semantics, but emitted batches contain each element
    /// at most once under `T`'s `PartialEq + Hash` equality (parity spec
    /// `graph.stream.WindowOperator` B2 — RDF stream operators must
    /// enforce set semantics within a window).
    ///
    /// Bound: `T: Eq + std::hash::Hash` because dedup needs hashing.
    /// Existing record-only `Window<T>` impls compile unchanged; the
    /// default implementation here delegates to `apply_events` and skips
    /// dedup, so unspecialized windows behave as a bag.
    /// [`TumblingWindow`] overrides this method.
    fn apply_events_dedup(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        self.apply_events(config, stream)
    }

    /// Result-bearing variant of [`apply_events`](Self::apply_events) — same
    /// stream-event semantics, but the output is
    /// `Stream<Result<WindowedBatch<T>, WindowError>>`. Upstream errors
    /// signaled via [`StreamEvent::Error`] surface as one `Err(WindowError)`
    /// item followed by `None`, *without* flushing open windows (parity spec
    /// `graph.stream.WindowOperator` B6).
    ///
    /// The default implementation delegates to `apply_events` and wraps each
    /// batch as `Ok` — adequate for unspecialized windows that have no
    /// notion of upstream errors. [`TumblingWindow`] overrides this method
    /// to actually emit `Err` on the error path.
    fn apply_events_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>> {
        use futures::StreamExt;
        Box::pin(self.apply_events(config, stream).map(Ok))
    }

    /// Result-bearing dedup variant — combines
    /// [`apply_events_dedup`](Self::apply_events_dedup) and
    /// [`apply_events_result`](Self::apply_events_result).
    ///
    /// The default implementation delegates to
    /// [`apply_events_result`](Self::apply_events_result) (so terminal
    /// `Err(WindowError)` items propagate verbatim — spec B6) and applies
    /// set semantics POST-EMISSION on each `Ok` batch by deduplicating
    /// the batch's elements vector under `T`'s `Eq + Hash`, preserving
    /// first-seen order. Override (as [`TumblingWindow`] and
    /// [`SlidingWindow`] do) for the more efficient
    /// dedup-at-construction path that never builds the duplicates in
    /// the first place.
    ///
    /// Earlier this method wrapped
    /// `apply_events_dedup(...).map(Ok)`, which silently discarded
    /// upstream errors for any `Window<T>` that overrode only
    /// `apply_events_result` and relied on the default here — flagged
    /// by the codex bot review on cqels-rs PR #74.
    fn apply_events_dedup_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        use futures::StreamExt;
        Box::pin(self.apply_events_result(config, stream).map(|res| {
            res.map(|mut batch| {
                let mut seen = std::collections::HashSet::with_capacity(batch.elements.len());
                batch.elements.retain(|el| seen.insert(el.clone()));
                batch
            })
        }))
    }
}

/// Non-overlapping fixed-duration time window.
///
/// A tumbling window divides the timeline into consecutive, non-overlapping
/// intervals of the given `size`. Every stream element falls into exactly
/// one window based on its timestamp.
///
/// For example, a 5-second tumbling window produces batches
/// `[0, 5000)`, `[5000, 10000)`, `[10000, 15000)`, and so on.
///
/// Maps to Java's `TumblingWindow` in CQELS 2.0.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::{TumblingWindow, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = TumblingWindow::new(Duration::from_secs(5));
/// assert_eq!(
///     <TumblingWindow as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::TumblingTime,
/// );
/// ```
#[derive(Clone, Debug)]
pub struct TumblingWindow {
    /// The fixed duration of each window.
    pub size: Duration,
}

impl TumblingWindow {
    pub fn new(size: Duration) -> Self {
        Self { size }
    }
}

impl<T: Timestamped + Clone + Send + 'static> Window<T> for TumblingWindow {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        // Watermark-driven emission: maintain the currently-open
        // window's bucket key, push elements into it as they arrive,
        // and flush the batch as soon as an event with a *later*
        // window key crosses the boundary. On stream end, drain the
        // final open window. This means low-volume streams emit
        // promptly (the previous `chunks(1024)` implementation
        // buffered up to 1024 elements before any window could be
        // observed — a problem for low-rate sources like sensor data).
        //
        // Out-of-order events (ts older than the current window) are
        // grouped with the current open window rather than dropped.
        // CQELS-rs streams are documented as in-order; this is the
        // conservative interpretation.
        let size_ms = self.size.as_millis() as i64;
        Box::pin(TumblingTimeWindowStream::new(stream, size_ms))
    }

    fn window_type(&self) -> WindowType {
        WindowType::TumblingTime
    }

    /// Watermark-aware tumbling-window evaluation (parity spec
    /// `graph.stream.WindowOperator` B4/B7). Each record is routed to its
    /// natural window key (`floor(t / size_ms)`, spec I1). Buckets fire on
    /// watermark advance — when an incoming `Watermark { timestamp: w }` has
    /// `w >= window_end`, every not-yet-fired bucket with `end <= w` is
    /// emitted in ascending key order.
    ///
    /// Late records (arriving after their window has fired) are admitted iff
    /// `current_watermark < window_end + allowed_lateness_ms` (strict —
    /// equality means the lateness budget is just consumed). Within budget,
    /// the bucket is re-fired with the augmented contents; past budget, the
    /// record is dropped silently. On upstream completion, all remaining
    /// open buckets flush in ascending key order (FLUSH-ON-COMPLETE).
    fn apply_events(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        // The underlying stream is Result-bearing (it's the same stream
        // apply_events_result uses); apply_events discards any error item
        // for backward compat with callers that don't care about the
        // terminal error type. To observe the error, use apply_events_result.
        let inner = self.apply_events_result(config, stream);
        Box::pin(inner.filter_map(|r| async move { r.ok() }))
    }

    /// Dedup-aware tumbling-window evaluation (parity spec
    /// `graph.stream.WindowOperator` B2). Same semantics as
    /// [`apply_events`](Self::apply_events) plus: every emitted batch's
    /// elements are unique under `T`'s `PartialEq` equality. Duplicates
    /// that land in the same window (or a re-fired window after a late
    /// record matches one already present) collapse to a single entry;
    /// first-seen order within a bucket is preserved.
    fn apply_events_dedup(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        use futures::StreamExt;
        let inner = self.apply_events_dedup_result(config, stream);
        Box::pin(inner.filter_map(|r| async move { r.ok() }))
    }

    fn apply_events_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>> {
        let size_ms = self.size.as_millis() as i64;
        Box::pin(TumblingTimeWindowEventStream::<T, NoDedup>::new(
            stream,
            size_ms,
            config.allowed_lateness_ms,
        ))
    }

    fn apply_events_dedup_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        let size_ms = self.size.as_millis() as i64;
        Box::pin(TumblingTimeWindowEventStream::<T, EqDedup>::new(
            stream,
            size_ms,
            config.allowed_lateness_ms,
        ))
    }
}

/// Record-only tumbling time window stream — routes every element to its
/// natural window key (`floor(t / size_ms)`) and flushes a window when a
/// strictly-greater-keyed element arrives, or when the input ends.
///
/// # Out-of-order behavior
///
/// Each element belongs to the window `floor(t / size_ms)`, always —
/// out-of-order events go to their natural bucket, not the current open
/// one (parity spec `graph.stream.WindowOperator` I1). Previously this
/// stream merged late events into whichever bucket was currently open,
/// which violated I1 and could only be observed under
/// out-of-order input; the fix preserves correct assignment.
///
/// Because this stream consumes plain `T` (no watermark channel),
/// emission timing is approximated by "later-keyed element arrival":
/// when an element with key `K` arrives, all currently-open buckets
/// with key `< K` are flushed in ascending key order. Buckets opened
/// late (after a higher key has already fired) stay open until the
/// stream ends, at which point all remaining buckets are drained in
/// ascending key order.
///
/// This means strict spec I2 ("emission order is ascending by
/// start_ms") holds only for in-order or monotonically-non-decreasing
/// streams. For genuinely out-of-order streams, all elements still
/// land in the correct windows (I1), but a late-opened bucket may be
/// emitted after a higher bucket has already fired. Callers needing
/// strict I2 plus a real lateness policy should use
/// [`Window::apply_events`] with a [`WindowConfig`].
struct TumblingTimeWindowStream<T: Timestamped + Clone> {
    inner: Pin<Box<dyn Stream<Item = T> + Send>>,
    size_ms: i64,
    /// All open buckets, keyed by window key (`floor(t / size_ms)`),
    /// ordered ascending. Each bucket holds the elements assigned to
    /// that window so far.
    open_batches: std::collections::BTreeMap<i64, Vec<T>>,
    pending: std::collections::VecDeque<WindowedBatch<T>>,
    inner_done: bool,
}

impl<T: Timestamped + Clone> TumblingTimeWindowStream<T> {
    fn new(inner: Pin<Box<dyn Stream<Item = T> + Send>>, size_ms: i64) -> Self {
        Self {
            inner,
            size_ms,
            open_batches: std::collections::BTreeMap::new(),
            pending: std::collections::VecDeque::new(),
            inner_done: false,
        }
    }

    fn batch_from(&self, key: i64, elements: Vec<T>) -> WindowedBatch<T> {
        let window_start = key * self.size_ms;
        let window_end = window_start + self.size_ms;
        WindowedBatch::new(elements, window_start, window_end, WindowType::TumblingTime)
    }
}

// All fields are Unpin (Pin<Box<dyn _ + Send>>, primitives, BTreeMap,
// VecDeque, bool), so the struct itself is Unpin — no projection
// pin-tooling required.
impl<T: Timestamped + Clone + Send> Unpin for TumblingTimeWindowStream<T> {}

impl<T: Timestamped + Clone + Send + 'static> Stream for TumblingTimeWindowStream<T> {
    type Item = WindowedBatch<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        loop {
            // Drain ready batches first so callers see them before
            // we touch the inner stream again.
            if let Some(batch) = this.pending.pop_front() {
                return Poll::Ready(Some(batch));
            }
            if this.inner_done {
                // Drain all remaining open buckets in ascending key order.
                if let Some((key, elements)) = this.open_batches.pop_first() {
                    return Poll::Ready(Some(this.batch_from(key, elements)));
                }
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(elem)) => {
                    let ts = elem.timestamp();
                    let key = ts.div_euclid(this.size_ms);
                    // Snapshot the largest open key BEFORE inserting, so we
                    // can detect "this element advances past every open
                    // bucket" without confusing it with re-entering its own
                    // bucket.
                    let prior_max = this.open_batches.keys().next_back().copied();
                    this.open_batches.entry(key).or_default().push(elem);
                    if let Some(max) = prior_max {
                        if key > max {
                            // The new element opens a window strictly later
                            // than any currently-open one — flush every
                            // open bucket with key < this new key, in
                            // ascending order.
                            while let Some(entry) = this.open_batches.first_entry() {
                                if *entry.key() >= key {
                                    break;
                                }
                                let (k, elements) = entry.remove_entry();
                                let batch = this.batch_from(k, elements);
                                this.pending.push_back(batch);
                            }
                        }
                    }
                }
                Poll::Ready(None) => {
                    this.inner_done = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Per-bucket insertion strategy parameterizing [`TumblingTimeWindowEventStream`].
///
/// A zero-sized marker that decides whether incoming records into the same
/// window-bucket are kept as a bag ([`NoDedup`]) or collapsed under
/// `PartialEq` equality ([`EqDedup`], for parity spec B2). Selected by the
/// `Window<T>` entry point: [`Window::apply_events`] uses [`NoDedup`],
/// [`Window::apply_events_dedup`] uses [`EqDedup`].
trait DedupPolicy<T>: Default {
    /// Insert `value` into `bucket`. Returns `true` iff the bucket actually
    /// grew — callers (notably the late-record re-fire path) use this to
    /// short-circuit a no-op re-emission when a duplicate is dropped.
    fn push(bucket: &mut Vec<T>, value: T) -> bool;
}

/// Bag insertion: every record is appended unconditionally.
#[derive(Default)]
struct NoDedup;
impl<T> DedupPolicy<T> for NoDedup {
    fn push(bucket: &mut Vec<T>, value: T) -> bool {
        bucket.push(value);
        true
    }
}

/// Set insertion under `PartialEq`: a record is only appended if no existing
/// element in the bucket compares equal to it. First-seen order within the
/// bucket is preserved. O(n) per insert (linear scan); fine for the
/// per-window cardinalities typical of RDF stream processing.
#[derive(Default)]
struct EqDedup;
impl<T: PartialEq> DedupPolicy<T> for EqDedup {
    fn push(bucket: &mut Vec<T>, value: T) -> bool {
        if bucket.contains(&value) {
            return false;
        }
        bucket.push(value);
        true
    }
}

/// Watermark-aware tumbling-window stream — consumes [`StreamEvent`]s
/// (records interleaved with watermark advances) and produces
/// `Result<WindowedBatch<T>, WindowError>` items that fire on watermark
/// progression, honor `allowed_lateness_ms`, and surface upstream errors.
///
/// This is the native event-time implementation of the parity spec
/// `graph.stream.WindowOperator` — B4 watermark-driven emission, B7 lateness
/// drop/re-emit, B1 flush-on-complete, and B6 terminal error propagation.
/// Each record is routed to its natural window key `floor(t / size_ms)`
/// (I1); watermark advances close any not-yet-fired bucket whose end ≤
/// watermark, in ascending key order (toward I2). A
/// [`StreamEvent::Error`] from upstream emits exactly one
/// `Err(WindowError { kind, message })` and then ends the stream — no flush
/// (B6).
///
/// Generic over `P: DedupPolicy<T>` — parameterizes per-bucket insertion
/// as bag ([`NoDedup`]) or set ([`EqDedup`], for spec B2).
struct TumblingTimeWindowEventStream<T: Timestamped + Clone, P: DedupPolicy<T>> {
    inner: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    size_ms: i64,
    allowed_lateness_ms: i64,
    /// Open and fired-but-still-in-lateness buckets, keyed by window key.
    open_batches: std::collections::BTreeMap<i64, Vec<T>>,
    /// Highest window-key that has been fired at least once (`None` until
    /// the first emission).
    fired_through: Option<i64>,
    /// Largest watermark observed so far (`i64::MIN` until the first
    /// watermark arrives).
    current_watermark: i64,
    /// Batches queued for emission (oldest first).
    pending: std::collections::VecDeque<WindowedBatch<T>>,
    /// A terminal error that must be surfaced as the next item, then the
    /// stream ends. `Some` only between observing `StreamEvent::Error` and
    /// the consumer pulling the corresponding `Err` item.
    pending_error: Option<WindowError>,
    inner_done: bool,
    _policy: std::marker::PhantomData<P>,
}

impl<T: Timestamped + Clone, P: DedupPolicy<T>> TumblingTimeWindowEventStream<T, P> {
    fn new(
        inner: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
        size_ms: i64,
        allowed_lateness_ms: i64,
    ) -> Self {
        Self {
            inner,
            size_ms,
            allowed_lateness_ms,
            open_batches: std::collections::BTreeMap::new(),
            fired_through: None,
            current_watermark: i64::MIN,
            pending: std::collections::VecDeque::new(),
            pending_error: None,
            inner_done: false,
            _policy: std::marker::PhantomData,
        }
    }

    fn batch_from(&self, key: i64, elements: Vec<T>) -> WindowedBatch<T> {
        let window_start = key * self.size_ms;
        let window_end = window_start + self.size_ms;
        WindowedBatch::new(elements, window_start, window_end, WindowType::TumblingTime)
    }

    /// Fire bucket `key` (snapshot its contents) and bump `fired_through`.
    /// The bucket entry stays in `open_batches` so subsequent late records
    /// can re-fire it within the lateness budget; the caller is responsible
    /// for purging it once the budget is exhausted.
    fn fire(&mut self, key: i64) {
        if let Some(elements) = self.open_batches.get(&key) {
            if !elements.is_empty() {
                let batch = self.batch_from(key, elements.clone());
                self.pending.push_back(batch);
            }
        }
        self.fired_through = Some(self.fired_through.map_or(key, |f| f.max(key)));
    }

    fn purge_expired(&mut self) {
        let cw = self.current_watermark;
        let size = self.size_ms;
        let lateness = self.allowed_lateness_ms;
        self.open_batches
            .retain(|k, _| (k + 1) * size + lateness > cw);
    }
}

impl<T: Timestamped + Clone + Send, P: DedupPolicy<T>> Unpin
    for TumblingTimeWindowEventStream<T, P>
{
}

impl<T: Timestamped + Clone + Send + 'static, P: DedupPolicy<T>> Stream
    for TumblingTimeWindowEventStream<T, P>
{
    type Item = Result<WindowedBatch<T>, WindowError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        loop {
            // Pending error takes precedence over any queued batches —
            // they were already cleared on the Error path; this is just
            // belt-and-suspenders.
            if let Some(err) = this.pending_error.take() {
                this.inner_done = true;
                return Poll::Ready(Some(Err(err)));
            }
            if let Some(batch) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if this.inner_done {
                // FLUSH-ON-COMPLETE (spec B4): flush any not-yet-fired bucket
                // in ascending key order, then we're done.
                let next_unfired = this
                    .open_batches
                    .keys()
                    .copied()
                    .find(|k| this.fired_through.is_none_or(|f| *k > f));
                if let Some(key) = next_unfired {
                    this.fire(key);
                    // fire() pushed onto pending; loop to drain.
                    continue;
                }
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => match event {
                    StreamEvent::Record { value, timestamp } => {
                        let key = timestamp.div_euclid(this.size_ms);
                        let window_end = (key + 1) * this.size_ms;

                        // Spec B7: lateness check is purely watermark-based —
                        // does NOT depend on whether we previously opened or
                        // fired a bucket for this window. Fixes codex unit-2
                        // round-2 B7 finding (watermark closes empty window,
                        // late record then leaks through fired_through being
                        // None). Applies to both tumbling and sliding.
                        if this.current_watermark >= window_end + this.allowed_lateness_ms {
                            continue; // dropped
                        }

                        // already_fired = the window's normal fire-time has
                        // passed. If so, this record is late but within
                        // budget — we insert and immediately re-fire.
                        let already_fired = this.current_watermark >= window_end;
                        let grew = P::push(this.open_batches.entry(key).or_default(), value);
                        if already_fired && grew {
                            // Re-fire (or first-fire for the empty-late case)
                            // only if the bucket actually changed — a
                            // duplicate-under-dedup would produce a
                            // spec-forbidden no-op re-emission. Mirrors Java
                            // EventProcessor#pushInto's short-circuit.
                            this.fire(key);
                        }
                    }
                    StreamEvent::Watermark { timestamp } => {
                        this.current_watermark = this.current_watermark.max(timestamp);
                        // Fire every not-yet-fired bucket whose end ≤ watermark.
                        let to_fire: Vec<i64> = this
                            .open_batches
                            .iter()
                            .take_while(|(k, _)| (**k + 1) * this.size_ms <= timestamp)
                            .filter(|(k, _)| this.fired_through.is_none_or(|f| **k > f))
                            .map(|(k, _)| *k)
                            .collect();
                        for key in to_fire {
                            this.fire(key);
                        }
                        this.purge_expired();
                    }
                    StreamEvent::Error { kind, message } => {
                        // Spec B6: terminal error without flushing any open windows.
                        // Drop all open-bucket state and any queued batches, then
                        // surface ONE Err item carrying the upstream kind/message
                        // verbatim; the next poll returns None and the stream
                        // ends.
                        this.open_batches.clear();
                        this.pending.clear();
                        this.pending_error = Some(WindowError { kind, message });
                        this.inner_done = true;
                        // Loop so the next iteration drains pending_error.
                        continue;
                    }
                },
                Poll::Ready(None) => {
                    this.inner_done = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Overlapping time-based sliding window.
///
/// A sliding window of `size` advances by `slide` increments, producing
/// overlapping batches. Each element may appear in multiple windows.
///
/// For example, a window with `size = 5s` and `slide = 2s` produces
/// windows `[0, 5000)`, `[2000, 7000)`, `[4000, 9000)`, etc.
///
/// Maps to Java's `SlidingWindow` in CQELS 2.0.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::SlidingWindow;
///
/// let window = SlidingWindow::new(
///     Duration::from_secs(10),  // window size
///     Duration::from_secs(5),   // slide interval
/// );
/// assert_eq!(window.size, Duration::from_secs(10));
/// assert_eq!(window.slide, Duration::from_secs(5));
/// ```
#[derive(Clone, Debug)]
pub struct SlidingWindow {
    /// The total duration each window spans.
    pub size: Duration,
    /// The interval between successive window starts.
    pub slide: Duration,
}

impl SlidingWindow {
    pub fn new(size: Duration, slide: Duration) -> Self {
        Self { size, slide }
    }
}

impl<T: Timestamped + Clone + Send + 'static> Window<T> for SlidingWindow {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        let size_ms = self.size.as_millis() as i64;
        let slide_ms = self.slide.as_millis() as i64;

        let stream = stream.chunks(1024).flat_map(move |chunk| {
            let mut windows: std::collections::BTreeMap<i64, Vec<T>> =
                std::collections::BTreeMap::new();
            for elem in chunk {
                let ts = elem.timestamp();
                // Determine all windows this element belongs to. Uses
                // div_euclid (floor division) instead of Rust's truncating
                // `/` so the formula behaves correctly for ts < size_ms.
                // Without this, ts=1500 with size=2000, slide=1000 would
                // start at 1000 and skip window [0, 2000) — a parity bug
                // vs. Java SlidingWindow.apply, flagged by codex unit-2
                // round 1 (BS1 cross-impl-mismatch). The event-aware
                // surface (SlidingTimeWindowEventStream::windows_for) had
                // the same fix from the start.
                let first_window_start = ((ts - size_ms).div_euclid(slide_ms) + 1) * slide_ms;
                let mut window_start = first_window_start.max(0);
                while window_start <= ts {
                    let window_end = window_start + size_ms;
                    if ts < window_end {
                        windows.entry(window_start).or_default().push(elem.clone());
                    }
                    window_start += slide_ms;
                }
            }
            futures::stream::iter(windows.into_iter().map(move |(start, elements)| {
                WindowedBatch::new(elements, start, start + size_ms, WindowType::SlidingTime)
            }))
        });
        Box::pin(stream)
    }

    fn window_type(&self) -> WindowType {
        WindowType::SlidingTime
    }

    /// Watermark-aware sliding-window evaluation (parity spec
    /// `graph.stream.SlidingWindowOperator` B4S/B7/BS1/BS2). Each record is
    /// assigned to **every** window whose interval `[k*slide, k*slide+size)`
    /// contains its timestamp — typically `ceil(size/slide)` windows. Buckets
    /// fire on watermark advance in ascending `start_ms` order; late records
    /// honor `allowed_lateness_ms`; complete flushes remaining buckets.
    fn apply_events(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        let inner = self.apply_events_result(config, stream);
        Box::pin(inner.filter_map(|r| async move { r.ok() }))
    }

    fn apply_events_dedup(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        use futures::StreamExt;
        let inner = self.apply_events_dedup_result(config, stream);
        Box::pin(inner.filter_map(|r| async move { r.ok() }))
    }

    fn apply_events_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>> {
        let size_ms = self.size.as_millis() as i64;
        let slide_ms = self.slide.as_millis() as i64;
        Box::pin(SlidingTimeWindowEventStream::<T, NoDedup>::new(
            stream,
            size_ms,
            slide_ms,
            config.allowed_lateness_ms,
        ))
    }

    fn apply_events_dedup_result(
        &self,
        config: WindowConfig,
        stream: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Result<WindowedBatch<T>, WindowError>> + Send>>
    where
        T: Eq + std::hash::Hash,
    {
        let size_ms = self.size.as_millis() as i64;
        let slide_ms = self.slide.as_millis() as i64;
        Box::pin(SlidingTimeWindowEventStream::<T, EqDedup>::new(
            stream,
            size_ms,
            slide_ms,
            config.allowed_lateness_ms,
        ))
    }
}

/// Watermark-aware sliding-window stream — same Result-bearing item type as
/// [`TumblingTimeWindowEventStream`], but each record is routed to every
/// window whose interval `[k*slide_ms, k*slide_ms + size_ms)` contains its
/// timestamp (spec BS1, typically `ceil(size/slide)` windows). Watermark
/// advances close any not-yet-fired bucket whose `start + size <= w`, in
/// ascending start order. Late records past `allowed_lateness_ms` are
/// dropped per spec B7; within budget, they push into every affected
/// bucket and re-fire any that grew (B7 short-circuit on no-op).
/// `StreamEvent::Error` surfaces as one `Err(WindowError)` and ends the
/// stream without flushing — spec B6.
struct SlidingTimeWindowEventStream<T: Timestamped + Clone, P: DedupPolicy<T>> {
    inner: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
    size_ms: i64,
    slide_ms: i64,
    allowed_lateness_ms: i64,
    /// Open and fired-but-still-in-lateness buckets, keyed by window-start.
    open_batches: std::collections::BTreeMap<i64, Vec<T>>,
    fired_through: Option<i64>,
    current_watermark: i64,
    pending: std::collections::VecDeque<WindowedBatch<T>>,
    pending_error: Option<WindowError>,
    inner_done: bool,
    _policy: std::marker::PhantomData<P>,
}

impl<T: Timestamped + Clone, P: DedupPolicy<T>> SlidingTimeWindowEventStream<T, P> {
    fn new(
        inner: Pin<Box<dyn Stream<Item = StreamEvent<T>> + Send>>,
        size_ms: i64,
        slide_ms: i64,
        allowed_lateness_ms: i64,
    ) -> Self {
        Self {
            inner,
            size_ms,
            slide_ms,
            allowed_lateness_ms,
            open_batches: std::collections::BTreeMap::new(),
            fired_through: None,
            current_watermark: i64::MIN,
            pending: std::collections::VecDeque::new(),
            pending_error: None,
            inner_done: false,
            _policy: std::marker::PhantomData,
        }
    }

    fn batch_from(&self, start: i64, elements: Vec<T>) -> WindowedBatch<T> {
        WindowedBatch::new(
            elements,
            start,
            start + self.size_ms,
            WindowType::SlidingTime,
        )
    }

    /// All window-starts whose interval `[start, start + size_ms)` contains `ts`.
    /// Window starts are non-negative multiples of `slide_ms`.
    ///
    /// Uses `div_euclid` (floor division) because Rust's `/` truncates toward
    /// zero, which yields the wrong first-window for `ts < size_ms`. For
    /// `ts=7000, size=10000, slide=5000`: floor((-3000)/5000) = -1, so the
    /// first start is `(-1+1)*5000 = 0`, correctly putting t=7000 into both
    /// `[0, 10000)` and `[5000, 15000)`. Plain truncation would skip
    /// `[0, 10000)` entirely.
    fn windows_for(&self, ts: i64) -> Vec<i64> {
        let mut out = Vec::new();
        let mut start = ((ts - self.size_ms).div_euclid(self.slide_ms) + 1) * self.slide_ms;
        if start < 0 {
            start = 0;
        }
        while start <= ts {
            if ts < start + self.size_ms {
                out.push(start);
            }
            start += self.slide_ms;
        }
        out
    }

    fn fire(&mut self, start: i64) {
        if let Some(elements) = self.open_batches.get(&start) {
            if !elements.is_empty() {
                let batch = self.batch_from(start, elements.clone());
                self.pending.push_back(batch);
            }
        }
        self.fired_through = Some(self.fired_through.map_or(start, |f| f.max(start)));
    }

    fn purge_expired(&mut self) {
        let cw = self.current_watermark;
        let size = self.size_ms;
        let lateness = self.allowed_lateness_ms;
        self.open_batches
            .retain(|start, _| start + size + lateness > cw);
    }
}

impl<T: Timestamped + Clone + Send, P: DedupPolicy<T>> Unpin
    for SlidingTimeWindowEventStream<T, P>
{
}

impl<T: Timestamped + Clone + Send + 'static, P: DedupPolicy<T>> Stream
    for SlidingTimeWindowEventStream<T, P>
{
    type Item = Result<WindowedBatch<T>, WindowError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        loop {
            if let Some(err) = this.pending_error.take() {
                this.inner_done = true;
                return Poll::Ready(Some(Err(err)));
            }
            if let Some(batch) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if this.inner_done {
                // FLUSH-ON-COMPLETE: any not-yet-fired bucket in ascending start.
                let next_unfired = this
                    .open_batches
                    .keys()
                    .copied()
                    .find(|s| this.fired_through.is_none_or(|f| *s > f));
                if let Some(start) = next_unfired {
                    this.fire(start);
                    continue;
                }
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => match event {
                    StreamEvent::Record { value, timestamp } => {
                        let starts = this.windows_for(timestamp);
                        for start in starts {
                            let window_end = start + this.size_ms;

                            // Spec B7: lateness check is purely watermark-based.
                            // Mirrors the tumbling fix for the empty-window-late
                            // bug (codex unit-2 round-2): a window closed by a
                            // watermark with no records was previously treated
                            // as never-fired, so late records leaked through.
                            if this.current_watermark >= window_end + this.allowed_lateness_ms {
                                continue;
                            }

                            let already_fired = this.current_watermark >= window_end;
                            let grew =
                                P::push(this.open_batches.entry(start).or_default(), value.clone());
                            if already_fired && grew {
                                this.fire(start);
                            }
                        }
                    }
                    StreamEvent::Watermark { timestamp } => {
                        this.current_watermark = this.current_watermark.max(timestamp);
                        let to_fire: Vec<i64> = this
                            .open_batches
                            .iter()
                            .take_while(|(s, _)| **s + this.size_ms <= timestamp)
                            .filter(|(s, _)| this.fired_through.is_none_or(|f| **s > f))
                            .map(|(s, _)| *s)
                            .collect();
                        for start in to_fire {
                            this.fire(start);
                        }
                        this.purge_expired();
                    }
                    StreamEvent::Error { kind, message } => {
                        this.open_batches.clear();
                        this.pending.clear();
                        this.pending_error = Some(WindowError { kind, message });
                        this.inner_done = true;
                        continue;
                    }
                },
                Poll::Ready(None) => {
                    this.inner_done = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Activity-based session window.
///
/// A session window groups consecutive events that are separated by less
/// than the specified `gap` duration. When a gap exceeding the threshold
/// occurs, the current session is closed and emitted as a batch.
///
/// Session windows are useful for detecting bursts of activity, such as
/// a user interaction session or a sensor anomaly episode.
///
/// Maps to Java's `SessionWindow` in CQELS 2.0.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::SessionWindow;
///
/// // Close the session after 30 seconds of inactivity
/// let window = SessionWindow::new(Duration::from_secs(30));
/// assert_eq!(window.gap, Duration::from_secs(30));
/// ```
#[derive(Clone, Debug)]
pub struct SessionWindow {
    /// Maximum allowed inactivity gap between consecutive events. If the
    /// gap between two events exceeds this duration, the session is closed.
    pub gap: Duration,
}

impl SessionWindow {
    pub fn new(gap: Duration) -> Self {
        Self { gap }
    }
}

impl<T: Timestamped + Clone + Send + 'static> Window<T> for SessionWindow {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        use std::sync::{Arc, Mutex};

        let gap_ms = self.gap.as_millis() as i64;

        struct SessionState<T> {
            current: Vec<T>,
            last_ts: i64,
            start_ts: i64,
        }

        // Shared state to flush the final session when the stream ends
        let final_state: Arc<Mutex<Option<SessionState<T>>>> = Arc::new(Mutex::new(None));
        let final_state_writer = final_state.clone();

        let stream = stream
            .scan(
                SessionState::<T> {
                    current: Vec::new(),
                    last_ts: i64::MIN,
                    start_ts: 0,
                },
                move |state, elem| {
                    let ts = elem.timestamp();
                    let completed = if !state.current.is_empty() && (ts - state.last_ts) > gap_ms {
                        let batch = WindowedBatch::new(
                            std::mem::take(&mut state.current),
                            state.start_ts,
                            state.last_ts + gap_ms,
                            WindowType::Session,
                        );
                        state.start_ts = ts;
                        Some(batch)
                    } else if state.current.is_empty() {
                        state.start_ts = ts;
                        None
                    } else {
                        None
                    };
                    state.current.push(elem);
                    state.last_ts = ts;

                    // Snapshot state for final flush
                    {
                        let mut guard = final_state_writer.lock().unwrap();
                        *guard = Some(SessionState {
                            current: state.current.clone(),
                            last_ts: state.last_ts,
                            start_ts: state.start_ts,
                        });
                    }

                    futures::future::ready(Some(completed))
                },
            )
            .filter_map(futures::future::ready);

        // Chain the final session flush
        let final_flush = futures::stream::once(async move {
            let guard = final_state.lock().unwrap();
            guard.as_ref().and_then(|state| {
                if state.current.is_empty() {
                    None
                } else {
                    Some(WindowedBatch::new(
                        state.current.clone(),
                        state.start_ts,
                        state.last_ts + gap_ms,
                        WindowType::Session,
                    ))
                }
            })
        })
        .filter_map(futures::future::ready);

        Box::pin(stream.chain(final_flush))
    }

    fn window_type(&self) -> WindowType {
        WindowType::Session
    }
}

/// Non-overlapping count-based tumbling window.
///
/// Collects exactly `count` elements into each batch. The window boundaries
/// are determined by element arrival order rather than timestamps.
///
/// If the stream ends before filling a batch, the remaining elements are
/// emitted as a partial (smaller) batch.
///
/// Maps to Java's `TumblingCountWindow` in CQELS 2.0.
#[derive(Clone, Debug)]
pub struct TumblingCountWindow {
    /// Number of elements per batch.
    pub count: usize,
}

impl TumblingCountWindow {
    pub fn new(count: usize) -> Self {
        assert!(count > 0, "count must be positive");
        Self { count }
    }
}

impl<T: Timestamped + Clone + Send + 'static> Window<T> for TumblingCountWindow {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        let count = self.count;

        let stream = stream.chunks(count).map(move |chunk| {
            let min_ts = chunk.iter().map(|e| e.timestamp()).min().unwrap_or(0);
            let max_ts = chunk.iter().map(|e| e.timestamp()).max().unwrap_or(0);
            WindowedBatch::new(chunk, min_ts, max_ts, WindowType::TumblingCount)
        });
        Box::pin(stream)
    }

    fn window_type(&self) -> WindowType {
        WindowType::TumblingCount
    }
}

/// Overlapping count-based sliding window.
///
/// Collects `count` elements per batch and advances by `slide` elements.
/// When `slide < count`, batches overlap (elements appear in multiple windows).
///
/// For example, with `count = 4` and `slide = 2`, a stream of 6 elements
/// produces batches `[e0,e1,e2,e3]`, `[e2,e3,e4,e5]`.
///
/// Maps to Java's `SlidingCountWindow` in CQELS 2.0.
#[derive(Clone, Debug)]
pub struct SlidingCountWindow {
    /// Number of elements per batch.
    pub count: usize,
    /// Number of elements to advance between batches.
    pub slide: usize,
}

impl SlidingCountWindow {
    pub fn new(count: usize, slide: usize) -> Self {
        assert!(count > 0, "count must be positive");
        assert!(slide > 0, "slide must be positive");
        assert!(slide <= count, "slide must not exceed count");
        Self { count, slide }
    }
}

impl<T: Timestamped + Clone + Send + 'static> Window<T> for SlidingCountWindow {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>> {
        use futures::StreamExt;
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};

        let count = self.count;
        let slide = self.slide;

        // Shared state for final flush of remaining elements
        let final_state: Arc<Mutex<Option<VecDeque<T>>>> = Arc::new(Mutex::new(None));
        let final_state_writer = final_state.clone();

        let stream = stream
            .scan(VecDeque::<T>::new(), move |buffer, elem| {
                buffer.push_back(elem);

                let batch = if buffer.len() >= count {
                    let elements: Vec<T> = buffer.iter().take(count).cloned().collect();
                    let min_ts = elements.iter().map(|e| e.timestamp()).min().unwrap_or(0);
                    let max_ts = elements.iter().map(|e| e.timestamp()).max().unwrap_or(0);
                    // Advance by slide
                    for _ in 0..slide {
                        buffer.pop_front();
                    }
                    Some(WindowedBatch::new(
                        elements,
                        min_ts,
                        max_ts,
                        WindowType::SlidingCount,
                    ))
                } else {
                    None
                };

                // Snapshot buffer for final flush
                {
                    let mut guard = final_state_writer.lock().unwrap();
                    *guard = Some(buffer.clone());
                }

                futures::future::ready(Some(batch))
            })
            .filter_map(futures::future::ready);

        // Flush remaining elements when stream ends
        let final_flush = futures::stream::once(async move {
            let guard = final_state.lock().unwrap();
            guard.as_ref().and_then(|buffer| {
                if buffer.is_empty() {
                    None
                } else {
                    let elements: Vec<T> = buffer.iter().cloned().collect();
                    let min_ts = elements.iter().map(|e| e.timestamp()).min().unwrap_or(0);
                    let max_ts = elements.iter().map(|e| e.timestamp()).max().unwrap_or(0);
                    Some(WindowedBatch::new(
                        elements,
                        min_ts,
                        max_ts,
                        WindowType::SlidingCount,
                    ))
                }
            })
        })
        .filter_map(futures::future::ready);

        Box::pin(stream.chain(final_flush))
    }

    fn window_type(&self) -> WindowType {
        WindowType::SlidingCount
    }
}

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

/// Creates a [`TumblingWindow`] with the given duration.
///
/// Convenience factory matching Java's `Window.tumbling(size)`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::{tumbling, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = tumbling(Duration::from_secs(5));
/// assert_eq!(window.size, Duration::from_secs(5));
/// assert_eq!(
///     <_ as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::TumblingTime,
/// );
/// ```
pub fn tumbling(size: Duration) -> TumblingWindow {
    TumblingWindow::new(size)
}

/// Creates a [`SlidingWindow`] with the given window size and slide interval.
///
/// Convenience factory matching Java's `Window.sliding(size, slide)`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::{sliding, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = sliding(Duration::from_secs(10), Duration::from_secs(5));
/// assert_eq!(
///     <_ as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::SlidingTime,
/// );
/// ```
pub fn sliding(size: Duration, slide: Duration) -> SlidingWindow {
    SlidingWindow::new(size, slide)
}

/// Creates a [`SessionWindow`] with the given inactivity gap.
///
/// Convenience factory matching Java's `Window.session(gap)`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_core::window::{session, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = session(Duration::from_secs(30));
/// assert_eq!(
///     <_ as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::Session,
/// );
/// ```
pub fn session(gap: Duration) -> SessionWindow {
    SessionWindow::new(gap)
}

/// Creates a [`TumblingCountWindow`] that batches the given number of elements.
///
/// Convenience factory matching Java's `Window.tumblingCount(count)`.
///
/// # Examples
///
/// ```
/// use cqels_core::window::{tumbling_count, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = tumbling_count(100);
/// assert_eq!(
///     <_ as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::TumblingCount,
/// );
/// ```
pub fn tumbling_count(count: usize) -> TumblingCountWindow {
    TumblingCountWindow::new(count)
}

/// Creates a [`SlidingCountWindow`] with the given window size and slide.
///
/// Convenience factory matching Java's `Window.slidingCount(count, slide)`.
///
/// # Examples
///
/// ```
/// use cqels_core::window::{sliding_count, Window, WindowType};
/// use cqels_core::stream::TimestampedValue;
///
/// let window = sliding_count(100, 50);
/// assert_eq!(
///     <_ as Window<TimestampedValue<i64>>>::window_type(&window),
///     WindowType::SlidingCount,
/// );
/// ```
pub fn sliding_count(count: usize, slide: usize) -> SlidingCountWindow {
    SlidingCountWindow::new(count, slide)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::TimestampedValue;
    use futures::StreamExt;

    fn make_elements(timestamps: &[i64]) -> Vec<TimestampedValue<i64>> {
        timestamps
            .iter()
            .map(|&ts| TimestampedValue::new(ts, ts))
            .collect()
    }

    #[tokio::test]
    async fn test_tumbling_window() {
        let elements = make_elements(&[0, 1000, 2000, 3000, 4000, 5000, 6000]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingWindow::new(Duration::from_secs(3));
        let batches: Vec<_> = window.apply(stream).collect().await;

        // Elements should be grouped: [0,1000,2000] in window [0,3000), [3000,4000,5000] in [3000,6000), [6000] in [6000,9000)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].elements.len(), 3);
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[0].window_end, 3000);
        assert_eq!(batches[1].elements.len(), 3);
        assert_eq!(batches[1].window_start, 3000);
        assert_eq!(batches[2].elements.len(), 1);
    }

    /// Low-volume regression: a handful of events should still emit
    /// promptly. The previous `chunks(1024)`-based implementation
    /// buffered up to 1024 elements before producing any batch,
    /// which silently broke any RANGE-windowed CqelsQL query whose
    /// source emitted fewer than 1024 events before the engine
    /// stopped. With the watermark-driven impl, a single completed
    /// stream of three elements yields a batch as soon as either a
    /// later-window event arrives (a "watermark") or the input ends.
    #[tokio::test]
    async fn tumbling_window_emits_for_low_volume_stream_at_end() {
        let elements = make_elements(&[1000, 1500, 2000]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingWindow::new(Duration::from_secs(10));
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 1, "single window flushed on stream end");
        assert_eq!(batches[0].elements.len(), 3);
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[0].window_end, 10_000);
    }

    #[tokio::test]
    async fn tumbling_window_flushes_open_batch_when_watermark_advances() {
        // Three events in window [0, 1000), then one event in [1000, 2000).
        // The fourth event should flush the first batch immediately —
        // we observe that by reading the first batch off the stream
        // and asserting its content before the iterator finishes.
        let elements = make_elements(&[100, 200, 300, 1500]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let mut stream = window.apply(stream);
        let first = stream.next().await.expect("first batch");
        assert_eq!(first.window_start, 0);
        assert_eq!(first.window_end, 1000);
        assert_eq!(first.elements.len(), 3);
        let second = stream.next().await.expect("second batch");
        assert_eq!(second.window_start, 1000);
        assert_eq!(second.window_end, 2000);
        assert_eq!(second.elements.len(), 1);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_tumbling_count_window() {
        let elements = make_elements(&[100, 200, 300, 400, 500]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingCountWindow::new(2);
        let batches: Vec<_> = window.apply(stream).collect().await;

        // [100,200], [300,400], [500]
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].size(), 2);
        assert_eq!(batches[1].size(), 2);
        assert_eq!(batches[2].size(), 1);
    }

    #[tokio::test]
    async fn test_sliding_window() {
        let elements = make_elements(&[0, 2000, 4000, 6000, 8000]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingWindow::new(Duration::from_secs(5), Duration::from_secs(3));
        let batches: Vec<_> = window.apply(stream).collect().await;

        // With size=5000ms, slide=3000ms:
        // Window [0, 5000): elements at 0, 2000, 4000
        // Window [3000, 8000): elements at 4000, 6000
        // Window [6000, 11000): elements at 6000, 8000
        assert!(!batches.is_empty());
        // Each element should appear in at least one window
        let total_elements: usize = batches.iter().map(|b| b.size()).sum();
        assert!(total_elements >= 5); // elements may appear in multiple windows
    }

    #[tokio::test]
    async fn test_session_window() {
        // Elements with a gap between 3000 and 8000 (gap > 2000ms)
        let elements = make_elements(&[1000, 2000, 3000, 8000, 9000]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SessionWindow::new(Duration::from_secs(2));
        let batches: Vec<_> = window.apply(stream).collect().await;

        // Session 1: [1000, 2000, 3000] (gap from 3000 to 8000 is > 2000ms)
        // Session 2: [8000, 9000] (flushed when stream ends)
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].elements.len(), 3);
        assert_eq!(batches[1].elements.len(), 2);
    }

    #[tokio::test]
    async fn test_windowed_batch_display() {
        let batch: WindowedBatch<i32> =
            WindowedBatch::new(vec![1, 2, 3], 0, 1000, WindowType::TumblingTime);
        let display = format!("{batch}");
        assert!(display.contains("TUMBLING_TIME"));
        assert!(display.contains("size=3"));
    }

    #[test]
    fn test_window_spec() {
        assert_eq!(WindowSpec::Now.window_type(), WindowType::TumblingTime);
        assert_eq!(
            WindowSpec::Range(Duration::from_secs(10)).window_type(),
            WindowType::TumblingTime
        );
        assert_eq!(
            WindowSpec::RangeSlide(Duration::from_secs(10), Duration::from_secs(5)).window_type(),
            WindowType::SlidingTime
        );
        assert_eq!(
            WindowSpec::Rows(100).window_type(),
            WindowType::TumblingCount
        );
        assert_eq!(
            WindowSpec::RowsSlide(10, 5).window_type(),
            WindowType::SlidingCount
        );
    }

    #[test]
    fn test_factory_functions() {
        type TV = TimestampedValue<i64>;

        let tw = tumbling(Duration::from_secs(5));
        assert_eq!(
            <TumblingWindow as Window<TV>>::window_type(&tw),
            WindowType::TumblingTime
        );

        let sw = sliding(Duration::from_secs(10), Duration::from_secs(5));
        assert_eq!(
            <SlidingWindow as Window<TV>>::window_type(&sw),
            WindowType::SlidingTime
        );

        let sess = session(Duration::from_secs(30));
        assert_eq!(
            <SessionWindow as Window<TV>>::window_type(&sess),
            WindowType::Session
        );

        let tc = tumbling_count(100);
        assert_eq!(
            <TumblingCountWindow as Window<TV>>::window_type(&tc),
            WindowType::TumblingCount
        );

        let sc = sliding_count(10, 5);
        assert_eq!(
            <SlidingCountWindow as Window<TV>>::window_type(&sc),
            WindowType::SlidingCount
        );
    }

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_tumbling_window_empty_stream() {
        let stream = Box::pin(futures::stream::iter(Vec::<TimestampedValue<i64>>::new()));
        let window = TumblingWindow::new(Duration::from_secs(5));
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_tumbling_window_single_element() {
        let elements = make_elements(&[500]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingWindow::new(Duration::from_secs(5));
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].elements.len(), 1);
    }

    /// Regression for parity-spec I1 (`graph.stream.WindowOperator`): an
    /// out-of-order element must land in *its own* window, not be merged
    /// into the current open batch. Previously the late-event branch
    /// in `TumblingTimeWindowStream::poll_next` did the merge, which
    /// could only be observed under out-of-order input.
    #[tokio::test]
    async fn test_tumbling_window_out_of_order_routes_to_natural_bucket() {
        // 1s windows; events arrive in order 500ms, 1500ms, 200ms. The
        // 200ms event is out-of-order — its natural window [0, 1000)
        // has already been fired by the 1500ms arrival.
        let elements = make_elements(&[500, 1500, 200]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window.apply(stream).collect().await;

        // I1: every element lives in the batch whose [start, end) contains its timestamp.
        // This is the bug-catching assertion — the pre-fix code put the 200ms element into
        // window [1000, 2000) because that bucket was "current" when it arrived.
        for batch in &batches {
            for el in &batch.elements {
                assert!(
                    el.timestamp >= batch.window_start && el.timestamp < batch.window_end,
                    "element ts={} routed to wrong window [{}, {}); spec I1 violation",
                    el.timestamp,
                    batch.window_start,
                    batch.window_end,
                );
            }
        }

        // No element is lost or duplicated.
        let mut all: Vec<i64> = batches
            .iter()
            .flat_map(|b| b.elements.iter().map(|e| e.timestamp))
            .collect();
        all.sort();
        assert_eq!(all, vec![200, 500, 1500]);
    }

    // ─── apply_events: watermark + lateness (parity spec B4 / B7) ────────────

    fn make_record(ts: i64) -> StreamEvent<TimestampedValue<i64>> {
        StreamEvent::record(TimestampedValue::new(ts, ts), ts)
    }

    fn wm(ts: i64) -> StreamEvent<TimestampedValue<i64>> {
        StreamEvent::watermark(ts)
    }

    /// Spec B4: watermark advance past `window_end` fires the window;
    /// records arriving *before* the watermark are not yet emitted.
    #[tokio::test]
    async fn apply_events_fires_on_watermark() {
        let events = vec![make_record(100), make_record(200), wm(1000)];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 1, "watermark @ 1000 fires window [0,1000)");
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[0].window_end, 1000);
        let ts: Vec<i64> = batches[0].elements.iter().map(|e| e.timestamp).collect();
        assert_eq!(ts, vec![100, 200]);
    }

    /// Spec B4: a single watermark past multiple window ends fires *all*
    /// closed windows in ascending key order.
    #[tokio::test]
    async fn apply_events_multiple_windows_one_watermark() {
        let events = vec![
            make_record(100),
            make_record(1100),
            make_record(2100),
            wm(3000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 3);
        assert_eq!(
            batches.iter().map(|b| b.window_start).collect::<Vec<_>>(),
            vec![0, 1000, 2000],
        );
    }

    /// Spec B4: FLUSH-ON-COMPLETE — upstream completion flushes the open
    /// window that no watermark has reached.
    #[tokio::test]
    async fn apply_events_flush_on_complete() {
        let events = vec![make_record(100)];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[0].elements.len(), 1);
    }

    /// Spec B7 (lateness = 0): a record arriving after its window has
    /// fired is dropped silently.
    #[tokio::test]
    async fn apply_events_late_data_dropped_when_lateness_zero() {
        let events = vec![
            make_record(100),
            wm(1000),
            make_record(200), // late: window [0,1000) already fired
            wm(2000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(
            batches.len(),
            1,
            "late record dropped; only first window fires"
        );
        assert_eq!(batches[0].elements.len(), 1);
        assert_eq!(batches[0].elements[0].timestamp, 100);
    }

    /// Spec B7 (lateness = 500ms): a late record within budget re-fires
    /// the window with the augmented contents.
    #[tokio::test]
    async fn apply_events_late_data_within_lateness_refires() {
        let events = vec![
            make_record(100),
            wm(1000),
            make_record(200), // late but within budget (wm=1000, end+lateness=1500)
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::new(500), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 2, "two emissions: initial fire and re-fire");
        assert_eq!(batches[0].elements.len(), 1);
        assert_eq!(batches[1].elements.len(), 2);
        let re_ts: Vec<i64> = batches[1].elements.iter().map(|e| e.timestamp).collect();
        assert_eq!(re_ts, vec![100, 200]);
    }

    /// Spec B7 boundary: at `watermark == window_end + allowed_lateness`,
    /// the budget is consumed (strict). A later record is dropped.
    #[tokio::test]
    async fn apply_events_late_data_dropped_at_lateness_boundary() {
        let events = vec![
            make_record(100),
            wm(1000),
            make_record(200), // within budget — re-fires
            wm(1500),         // budget just consumed
            make_record(250), // dropped
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::new(500), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 2);
        // No third emission — t=250 dropped because watermark=1500 ≥ end+lateness=1500.
    }

    /// Spec B2: `apply_events_dedup` collapses term-equal records in the
    /// same window. Pre-fix this would have emitted both copies; the
    /// emitted batch must contain exactly one.
    #[tokio::test]
    async fn apply_events_dedup_collapses_duplicates_in_window() {
        // Custom value type that's Eq + Hash so we can test the bound.
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        struct Q {
            ts: i64,
            payload: &'static str,
        }
        impl crate::stream::Timestamped for Q {
            fn timestamp(&self) -> i64 {
                self.ts
            }
        }

        // Two term-equal Qs (identical fields) + one distinct. Mirrors the
        // RDF case where Quad equality is term-equality on S/P/O/G — two
        // identical quads emitted at the same timestamp should collapse.
        let events = vec![
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ), // duplicate
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "b",
                },
                100,
            ),
            StreamEvent::watermark(1000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events_dedup(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 1);
        // Two distinct payloads survive; the duplicate "a" collapses.
        let payloads: Vec<_> = batches[0].elements.iter().map(|q| q.payload).collect();
        assert_eq!(payloads, vec!["a", "b"]);
    }

    /// Without `_dedup`, the same input emits both copies — confirms
    /// dedup is NOT silently applied to `apply_events` and the two APIs
    /// observably differ.
    #[tokio::test]
    async fn apply_events_no_dedup_keeps_duplicates() {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        struct Q {
            ts: i64,
            payload: &'static str,
        }
        impl crate::stream::Timestamped for Q {
            fn timestamp(&self) -> i64 {
                self.ts
            }
        }

        let events = vec![
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
            StreamEvent::watermark(1000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].elements.len(),
            2,
            "apply_events without _dedup keeps duplicates (bag semantics)"
        );
    }

    /// Spec B7 cross-impl-parity (codex T6 re-run): a late record that
    /// arrives within the lateness budget AND is a duplicate of an existing
    /// element in the bucket must NOT produce a no-op re-emission. Java's
    /// EventProcessor#pushInto returned false in this case; Rust now mirrors
    /// via DedupPolicy::push returning bool.
    #[tokio::test]
    async fn apply_events_dedup_late_duplicate_does_not_refire() {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        struct Q {
            ts: i64,
            payload: &'static str,
        }
        impl crate::stream::Timestamped for Q {
            fn timestamp(&self) -> i64 {
                self.ts
            }
        }

        let events = vec![
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
            StreamEvent::watermark(1000), // fires window [0, 1000)
            // Late record that's a duplicate of "a" already in the bucket.
            // Within lateness budget (wm=1000, end+lateness=1500).
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events_dedup(WindowConfig::new(500), stream)
            .collect()
            .await;

        // Exactly one batch from the initial watermark fire; no re-fire on
        // the duplicate late record.
        assert_eq!(
            batches.len(),
            1,
            "late duplicate must not trigger a no-op re-fire; got {batches:?}"
        );
        assert_eq!(batches[0].elements.len(), 1);
    }

    /// Regression for codex PR #74 review P2: the default
    /// `apply_events_dedup_result` impl must surface upstream errors as
    /// `Err(WindowError)`, not silently drop them. Pre-fix the default
    /// wrapped `apply_events_dedup(...).map(Ok)` and erased the error
    /// path. Now it delegates through `apply_events_result` (which
    /// propagates Err) and deduplicates post-emission.
    ///
    /// Uses a local `Q` type (needs `Eq + Hash + Clone + Timestamped`
    /// per the `_dedup` bound). Tests against TumblingWindow which
    /// overrides this method, but the test would also catch a
    /// regression in the default impl on any other future `Window<T>`.
    #[tokio::test]
    async fn apply_events_dedup_result_surfaces_errors() {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        struct Q {
            ts: i64,
            payload: &'static str,
        }
        impl crate::stream::Timestamped for Q {
            fn timestamp(&self) -> i64 {
                self.ts
            }
        }

        let events = vec![
            StreamEvent::record(
                Q {
                    ts: 100,
                    payload: "a",
                },
                100,
            ),
            StreamEvent::error("DefaultPathFailure", Some("must propagate")),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let items: Vec<_> = window
            .apply_events_dedup_result(WindowConfig::default(), stream)
            .collect()
            .await;
        // Exactly one Err item, no batches (B6 no flush).
        assert_eq!(items.len(), 1, "expected one Err terminal; got {items:?}");
        match &items[0] {
            Err(e) => {
                assert_eq!(e.kind, "DefaultPathFailure");
                assert_eq!(e.message.as_deref(), Some("must propagate"));
            }
            Ok(b) => panic!("expected Err terminal; got Ok batch {b:?}"),
        }
    }

    /// Spec B7 (codex unit-2 round-2): a watermark closes empty windows
    /// before any record arrives; a subsequent record for that window must
    /// be DROPPED at `watermark >= end_ms + allowed_lateness_ms`, not
    /// admitted via the "never-fired" branch. Pre-fix, `fired_through`
    /// stayed `None` so the late record leaked into the bucket and emitted
    /// on the next watermark.
    #[tokio::test]
    async fn apply_events_empty_window_closed_by_watermark_drops_late() {
        let events = vec![
            StreamEvent::watermark(2000), // closes empty [0, 1000) and [1000, 2000)
            make_record(500),             // would belong to [0, 1000) — must be dropped
            make_record(1500),            // would belong to [1000, 2000) — must be dropped
            StreamEvent::watermark(3000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert!(
            batches.is_empty(),
            "late records on empty-closed windows must be dropped; got {batches:?}"
        );
    }

    /// Same B7 fix on the sliding side — empty windows closed by a watermark
    /// must drop subsequent late records (not admit + emit).
    #[tokio::test]
    async fn sliding_apply_events_empty_window_closed_by_watermark_drops_late() {
        let events = vec![
            StreamEvent::watermark(3000), // closes empty [0,2000) AND [1000,3000)
            StreamEvent::record(TimestampedValue::new(1500_i64, 1500), 1500),
            StreamEvent::watermark(4000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = SlidingWindow::new(Duration::from_secs(2), Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        assert!(
            batches.is_empty(),
            "late records on empty-closed sliding windows must be dropped; got {batches:?}"
        );
    }

    /// Spec B6: an upstream `StreamEvent::Error` terminates the output
    /// stream WITHOUT flushing any open windows. The consumer sees no
    /// further batches after the Error event lands, even if there were
    /// records buffered for a still-open window.
    #[tokio::test]
    async fn apply_events_error_terminates_without_flush() {
        let events = vec![
            make_record(100), // window [0, 1000) opens with one record
            make_record(200), // still in same window
            StreamEvent::error("UpstreamFailure", Some("boom")),
            // anything past this should be discarded by take_while semantics
            // — but the stream is already exhausted from the operator's POV.
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;

        assert!(
            batches.is_empty(),
            "spec B6: no flush on upstream error; got {} batch(es): {batches:?}",
            batches.len(),
        );
    }

    /// Spec B7 alignment with Java's WindowConfig constructor: negative
    /// `allowed_lateness_ms` is invalid. Rust panics, Java throws — both
    /// reject the same input.
    #[test]
    #[should_panic(expected = "allowed_lateness_ms must be >= 0")]
    fn window_config_rejects_negative_lateness() {
        let _ = WindowConfig::new(-1);
    }

    /// Spec B6 Result-bearing variant: `apply_events_result` surfaces the
    /// upstream error as exactly one `Err(WindowError { kind, message })`
    /// (kind/message preserved verbatim), then ends the stream with `None`.
    /// No batches flushed — open windows are discarded.
    #[tokio::test]
    async fn apply_events_result_surfaces_terminal_error() {
        let events = vec![
            make_record(100), // would flush-on-complete if not for the error
            StreamEvent::error("UpstreamFailure", Some("boom")),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = TumblingWindow::new(Duration::from_secs(1));
        let items: Vec<_> = window
            .apply_events_result(WindowConfig::default(), stream)
            .collect()
            .await;

        assert_eq!(
            items.len(),
            1,
            "exactly one item — the Err terminal; got {items:?}"
        );
        match &items[0] {
            Err(e) => {
                assert_eq!(e.kind, "UpstreamFailure");
                assert_eq!(e.message.as_deref(), Some("boom"));
            }
            Ok(b) => panic!("expected Err terminal, got Ok batch: {b:?}"),
        }
    }

    /// Regression for codex unit-2 round-1 BS1: the record-only
    /// `SlidingWindow::apply` previously used truncating-toward-zero
    /// division, missing window [0, size_ms) for `ts < size_ms`.
    /// With size=2s, slide=1s, ts=1500 the element must land in BOTH
    /// [0, 2000) and [1000, 3000).
    #[tokio::test]
    async fn sliding_apply_assigns_early_ts_to_both_overlapping_windows() {
        let elements = vec![TimestampedValue::new(1500_i64, 1500)];
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingWindow::new(Duration::from_secs(2), Duration::from_secs(1));
        let batches: Vec<_> = window.apply(stream).collect().await;
        // Two batches expected; chunks(1024) means they all land at stream-end.
        // BTreeMap keeps them in ascending start order.
        assert_eq!(batches.len(), 2);
        let starts: Vec<_> = batches.iter().map(|b| b.window_start).collect();
        assert_eq!(starts, vec![0, 1000]);
        for b in &batches {
            assert_eq!(b.elements.len(), 1);
            assert_eq!(b.elements[0].value, 1500);
        }
    }

    // ─── SlidingWindow::apply_events* (parity unit 2) ────────────────────────

    /// Spec BS1: a record appears in N = ceil(size/slide) consecutive windows
    /// when size > slide. Here size=10s, slide=5s → element at t=7000 appears
    /// in windows [0,10) and [5,15).
    #[tokio::test]
    async fn sliding_apply_events_element_in_n_windows() {
        let events = vec![
            StreamEvent::record(TimestampedValue::new(7000_i64, 7000), 7000),
            StreamEvent::watermark(20_000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = SlidingWindow::new(Duration::from_secs(10), Duration::from_secs(5));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        // Three windows have end <= 20_000 and could contain t=7000:
        //   [0,10) yes,  [5,15) yes,  [10,20) no (start=10000 > 7000).
        // Plus [10,20) end=20000 fires too but contains nothing → no batch.
        // Spec BS2: ascending by start_ms.
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[1].window_start, 5000);
        for b in &batches {
            assert_eq!(b.elements.len(), 1);
            assert_eq!(b.elements[0].value, 7000);
        }
    }

    /// Spec B4S: a single watermark fires all closed sliding windows in
    /// ascending start order. Two records in distinct positions; multiple
    /// overlapping windows fire on one watermark advance.
    #[tokio::test]
    async fn sliding_apply_events_multi_window_one_watermark() {
        let events = vec![
            StreamEvent::record(TimestampedValue::new(2000_i64, 2000), 2000),
            StreamEvent::record(TimestampedValue::new(7000_i64, 7000), 7000),
            StreamEvent::watermark(15_000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = SlidingWindow::new(Duration::from_secs(10), Duration::from_secs(5));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        // Windows with end <= 15_000:
        //   [0,10) → contains t=2000 AND t=7000 → 2 elements
        //   [5,15) → contains t=7000 only (start=5000 > 2000) → 1 element
        // Window [10,20) end=20_000 > 15_000 → not fired.
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].window_start, 0);
        assert_eq!(batches[0].elements.len(), 2);
        assert_eq!(batches[1].window_start, 5000);
        assert_eq!(batches[1].elements.len(), 1);
    }

    /// Spec B7: a late record within budget that adds a new value to an
    /// already-fired sliding window triggers a re-emission of that window
    /// (and any overlapping fired windows the late record also belongs to).
    ///
    /// Lateness=6s so both [0,10) and [5,15) stay alive at watermark=15_000:
    /// budget-exhaust threshold for [0,10) is 10000+6000=16000 (> 15000),
    /// and for [5,15) is 15000+6000=21000 (> 15000).
    #[tokio::test]
    async fn sliding_apply_events_late_data_refires_overlapping_windows() {
        let events = vec![
            StreamEvent::record(TimestampedValue::new(7000_i64, 7000), 7000),
            StreamEvent::watermark(15_000), // fires [0,10) and [5,15)
            // Late record at t=8000 — appears in both [0,10) and [5,15);
            // within lateness budget for both windows.
            StreamEvent::record(TimestampedValue::new(8000_i64, 8000), 8000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = SlidingWindow::new(Duration::from_secs(10), Duration::from_secs(5));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::new(6000), stream)
            .collect()
            .await;
        // Initial: 2 batches ([0,10) with {7000}, [5,15) with {7000}).
        // Late: 2 re-emissions, one per affected fired window.
        assert_eq!(batches.len(), 4);
        assert_eq!(batches[2].window_start, 0);
        assert_eq!(batches[2].elements.len(), 2);
        assert_eq!(batches[3].window_start, 5000);
        assert_eq!(batches[3].elements.len(), 2);
    }

    /// Default impl on a hypothetical record-only Window<T>: when an
    /// implementation does NOT override apply_events, watermarks are
    /// stripped and the record-only `apply` is invoked. Smoke test
    /// against SlidingWindow (no override).
    #[tokio::test]
    async fn apply_events_default_impl_strips_watermarks() {
        let events = vec![
            make_record(0),
            wm(500),
            make_record(1000),
            wm(1500),
            make_record(2000),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let window = SlidingWindow::new(Duration::from_secs(3), Duration::from_secs(1));
        let batches: Vec<_> = window
            .apply_events(WindowConfig::default(), stream)
            .collect()
            .await;
        // Just check it didn't panic and produced at least one batch — the
        // exact count depends on SlidingWindow's record-only semantics.
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn test_tumbling_count_exact_multiple() {
        let elements = make_elements(&[100, 200, 300, 400]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingCountWindow::new(2);
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].size(), 2);
        assert_eq!(batches[1].size(), 2);
    }

    #[tokio::test]
    async fn test_tumbling_count_single_batch_size() {
        let elements = make_elements(&[100, 200, 300]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = TumblingCountWindow::new(1);
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 3);
    }

    #[tokio::test]
    async fn test_session_multiple_sessions() {
        let elements = make_elements(&[100, 200, 300, 2000, 2100, 2200, 4000, 4100]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SessionWindow::new(Duration::from_millis(500));
        let batches: Vec<_> = window.apply(stream).collect().await;
        // Three sessions: [100,200,300], [2000,2100,2200], [4000,4100] (final flush)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].elements.len(), 3);
        assert_eq!(batches[1].elements.len(), 3);
        assert_eq!(batches[2].elements.len(), 2);
    }

    #[tokio::test]
    async fn test_sliding_window_overlap() {
        let elements = make_elements(&[0, 1000, 2000, 3000, 4000]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingWindow::new(Duration::from_secs(3), Duration::from_secs(1));
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_windowed_batch_accessors() {
        let batch: WindowedBatch<i32> =
            WindowedBatch::new(vec![1, 2, 3], 0, 5000, WindowType::TumblingTime);
        assert_eq!(batch.size(), 3);
        assert_eq!(batch.window_start, 0);
        assert_eq!(batch.window_end, 5000);
        assert_eq!(batch.window_type, WindowType::TumblingTime);
    }

    #[tokio::test]
    async fn test_sliding_count_window_basic() {
        // 7 elements, count=3, slide=2 → batches: [0,1,2], [2,3,4], [4,5,6]
        let elements = make_elements(&[100, 200, 300, 400, 500, 600, 700]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingCountWindow::new(3, 2);
        let batches: Vec<_> = window.apply(stream).collect().await;

        // Full batches: [100,200,300], [300,400,500], [500,600,700]
        // After 3rd batch, buffer has [700] which is flushed as partial
        assert_eq!(batches.len(), 4);
        assert_eq!(batches[0].size(), 3);
        assert_eq!(batches[1].size(), 3);
        assert_eq!(batches[2].size(), 3);
        assert_eq!(batches[3].size(), 1); // partial flush
    }

    #[tokio::test]
    async fn test_sliding_count_window_no_overlap() {
        // count == slide → equivalent to tumbling count
        let elements = make_elements(&[100, 200, 300, 400, 500, 600]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingCountWindow::new(3, 3);
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].size(), 3);
        assert_eq!(batches[1].size(), 3);
    }

    #[tokio::test]
    async fn test_sliding_count_window_slide_one() {
        // Slide of 1 → maximum overlap
        let elements = make_elements(&[10, 20, 30, 40]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingCountWindow::new(3, 1);
        let batches: Vec<_> = window.apply(stream).collect().await;

        // Batches: [10,20,30], [20,30,40], then partial flush [30,40]
        assert_eq!(batches.len(), 3); // 2 full + 1 partial
        assert_eq!(batches[0].size(), 3);
        assert_eq!(batches[1].size(), 3);
        assert_eq!(batches[2].size(), 2); // remaining buffer after last slide
    }

    #[tokio::test]
    async fn test_sliding_count_window_empty_stream() {
        let stream = Box::pin(futures::stream::iter(Vec::<TimestampedValue<i64>>::new()));
        let window = SlidingCountWindow::new(3, 1);
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_sliding_count_window_partial() {
        // Fewer elements than window size → single partial batch
        let elements = make_elements(&[100, 200]);
        let stream = Box::pin(futures::stream::iter(elements));
        let window = SlidingCountWindow::new(5, 2);
        let batches: Vec<_> = window.apply(stream).collect().await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].size(), 2);
    }

    #[test]
    fn test_window_spec_debug() {
        let now_dbg = format!("{:?}", WindowSpec::Now);
        assert!(now_dbg.contains("Now"));
        let range_dbg = format!("{:?}", WindowSpec::Range(Duration::from_secs(10)));
        assert!(range_dbg.contains("Range"));
        let rows_dbg = format!("{:?}", WindowSpec::Rows(50));
        assert!(rows_dbg.contains("50"));
    }
}
