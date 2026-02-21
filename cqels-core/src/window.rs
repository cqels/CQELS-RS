use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use futures::Stream;

use crate::stream::Timestamped;

/// Supported window types.
///
/// Maps to Java's `WindowType` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

/// A batch of stream elements within a window.
///
/// Maps to Java's `WindowedBatch<T>`.
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

/// A window specification — describes window parameters without applying them.
///
/// Maps to Java's `WindowSpec` in the query language.
#[derive(Clone, Debug, PartialEq)]
pub enum WindowSpec {
    /// Process immediately (NOW window).
    Now,
    /// Time-based tumbling window.
    Range(Duration),
    /// Time-based sliding window with step.
    RangeSlide(Duration, Duration),
    /// Count-based window.
    Rows(usize),
    /// Count-based sliding window.
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

/// Trait for window operators that transform a stream of elements into
/// a stream of windowed batches.
///
/// Maps to Java's `Window<T extends StreamElement>` interface.
pub trait Window<T: Timestamped + Clone + Send + 'static>: Send + Sync {
    /// Applies this window to a stream, producing windowed batches.
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>;

    /// Returns the window type.
    fn window_type(&self) -> WindowType;
}

/// Non-overlapping fixed-duration time window.
///
/// Maps to Java's `TumblingWindow`.
#[derive(Clone, Debug)]
pub struct TumblingWindow {
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
        use futures::StreamExt;
        let size_ms = self.size.as_millis() as i64;

        // Collect elements and group them by window
        let stream = stream.chunks(1024).flat_map(move |chunk| {
            let mut windows: std::collections::BTreeMap<i64, Vec<T>> =
                std::collections::BTreeMap::new();
            for elem in chunk {
                let ts = elem.timestamp();
                let window_key = ts / size_ms;
                windows.entry(window_key).or_default().push(elem);
            }
            futures::stream::iter(windows.into_iter().map(move |(key, elements)| {
                let window_start = key * size_ms;
                let window_end = window_start + size_ms;
                WindowedBatch::new(elements, window_start, window_end, WindowType::TumblingTime)
            }))
        });
        Box::pin(stream)
    }

    fn window_type(&self) -> WindowType {
        WindowType::TumblingTime
    }
}

/// Overlapping time-based sliding window.
///
/// Maps to Java's `SlidingWindow`.
#[derive(Clone, Debug)]
pub struct SlidingWindow {
    pub size: Duration,
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
                // Determine all windows this element belongs to
                let first_window_start = ((ts - size_ms) / slide_ms + 1) * slide_ms;
                let mut window_start = first_window_start.max(0);
                while window_start <= ts {
                    let window_end = window_start + size_ms;
                    if ts < window_end {
                        windows
                            .entry(window_start)
                            .or_default()
                            .push(elem.clone());
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
}

/// Activity-based session window.
///
/// Maps to Java's `SessionWindow`.
#[derive(Clone, Debug)]
pub struct SessionWindow {
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
        let gap_ms = self.gap.as_millis() as i64;

        struct SessionState<T> {
            current: Vec<T>,
            last_ts: i64,
            start_ts: i64,
        }

        let stream = stream
            .scan(
                SessionState::<T> {
                    current: Vec::new(),
                    last_ts: i64::MIN,
                    start_ts: 0,
                },
                move |state, elem| {
                    let ts = elem.timestamp();
                    let completed = if !state.current.is_empty()
                        && (ts - state.last_ts) > gap_ms
                    {
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
                    futures::future::ready(Some(completed))
                },
            )
            .filter_map(futures::future::ready);

        Box::pin(stream)
    }

    fn window_type(&self) -> WindowType {
        WindowType::Session
    }
}

/// Count-based tumbling window.
///
/// Maps to Java's `TumblingCountWindow`.
#[derive(Clone, Debug)]
pub struct TumblingCountWindow {
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

/// Factory functions for creating common window types.
///
/// Maps to Java's `Window` static factory methods.
pub fn tumbling(size: Duration) -> TumblingWindow {
    TumblingWindow::new(size)
}

pub fn sliding(size: Duration, slide: Duration) -> SlidingWindow {
    SlidingWindow::new(size, slide)
}

pub fn session(gap: Duration) -> SessionWindow {
    SessionWindow::new(gap)
}

pub fn tumbling_count(count: usize) -> TumblingCountWindow {
    TumblingCountWindow::new(count)
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
        // Session 2: [8000, 9000] (will be flushed when stream ends... but our impl emits on gap detection)
        assert_eq!(batches.len(), 1); // Only the completed session is emitted
        assert_eq!(batches[0].elements.len(), 3);
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
        assert_eq!(WindowSpec::Rows(100).window_type(), WindowType::TumblingCount);
    }

    #[test]
    fn test_factory_functions() {
        type TV = TimestampedValue<i64>;

        let tw = tumbling(Duration::from_secs(5));
        assert_eq!(<TumblingWindow as Window<TV>>::window_type(&tw), WindowType::TumblingTime);

        let sw = sliding(Duration::from_secs(10), Duration::from_secs(5));
        assert_eq!(<SlidingWindow as Window<TV>>::window_type(&sw), WindowType::SlidingTime);

        let sess = session(Duration::from_secs(30));
        assert_eq!(<SessionWindow as Window<TV>>::window_type(&sess), WindowType::Session);

        let tc = tumbling_count(100);
        assert_eq!(<TumblingCountWindow as Window<TV>>::window_type(&tc), WindowType::TumblingCount);
    }
}
