use std::collections::HashSet;
use std::hash::Hash;
use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

/// A window update containing elements that were added and removed.
///
/// Used by RSP-QL operators to distinguish between different stream semantics.
#[derive(Clone, Debug)]
pub struct WindowUpdate<T> {
    pub added: Vec<T>,
    pub removed: Vec<T>,
    pub window_start: i64,
    pub window_end: i64,
    pub timestamp: i64,
}

impl<T> WindowUpdate<T> {
    pub fn new(
        added: Vec<T>,
        removed: Vec<T>,
        window_start: i64,
        window_end: i64,
        timestamp: i64,
    ) -> Self {
        Self {
            added,
            removed,
            window_start,
            window_end,
            timestamp,
        }
    }
}

/// A snapshot of a window's current contents.
#[derive(Clone, Debug)]
pub struct WindowSnapshot<T> {
    pub contents: Vec<T>,
    pub window_start: i64,
    pub window_end: i64,
    pub timestamp: i64,
}

/// IStream operator — extracts only newly added elements from window updates.
///
/// Implements the ISTREAM (Insert Stream) semantics from RSP-QL.
pub struct IStreamOperator;

impl IStreamOperator {
    /// Applies IStream semantics to a stream of window updates.
    pub fn apply<T: Clone + Send + 'static>(
        stream: Pin<Box<dyn Stream<Item = WindowUpdate<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = T> + Send>> {
        let stream = stream.flat_map(|update| futures::stream::iter(update.added));
        Box::pin(stream)
    }
}

/// DStream operator — extracts only removed elements from window updates.
///
/// Implements the DSTREAM (Delete Stream) semantics from RSP-QL.
pub struct DStreamOperator;

impl DStreamOperator {
    /// Applies DStream semantics to a stream of window updates.
    pub fn apply<T: Clone + Send + 'static>(
        stream: Pin<Box<dyn Stream<Item = WindowUpdate<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = T> + Send>> {
        let stream = stream.flat_map(|update| futures::stream::iter(update.removed));
        Box::pin(stream)
    }
}

/// RStream operator — emits complete window contents on every update.
///
/// Implements the RSTREAM semantics from RSP-QL.
pub struct RStreamOperator;

impl RStreamOperator {
    /// Applies RStream semantics, tracking cumulative window state.
    pub fn apply<T: Clone + Eq + Hash + Send + 'static>(
        stream: Pin<Box<dyn Stream<Item = WindowUpdate<T>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowSnapshot<T>> + Send>> {
        let stream = stream.scan(HashSet::<T>::new(), |state, update| {
            // Add new elements
            for elem in &update.added {
                state.insert(elem.clone());
            }
            // Remove expired elements
            for elem in &update.removed {
                state.remove(elem);
            }
            let snapshot = WindowSnapshot {
                contents: state.iter().cloned().collect(),
                window_start: update.window_start,
                window_end: update.window_end,
                timestamp: update.timestamp,
            };
            futures::future::ready(Some(snapshot))
        });
        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_istream() {
        let updates = vec![
            WindowUpdate::new(vec![1, 2, 3], vec![], 0, 1000, 100),
            WindowUpdate::new(vec![4], vec![1], 1000, 2000, 200),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = IStreamOperator::apply(stream).collect().await;
        assert_eq!(results, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_dstream() {
        let updates = vec![
            WindowUpdate::new(vec![1, 2, 3], vec![], 0, 1000, 100),
            WindowUpdate::new(vec![4], vec![1, 2], 1000, 2000, 200),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = DStreamOperator::apply(stream).collect().await;
        assert_eq!(results, vec![1, 2]); // only removed elements
    }

    #[tokio::test]
    async fn test_rstream() {
        let updates = vec![
            WindowUpdate::new(vec![1, 2, 3], vec![], 0, 1000, 100),
            WindowUpdate::new(vec![4], vec![1], 1000, 2000, 200),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let snapshots: Vec<WindowSnapshot<i32>> = RStreamOperator::apply(stream).collect().await;

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].contents.len(), 3); // {1, 2, 3}
        assert_eq!(snapshots[1].contents.len(), 3); // {2, 3, 4}

        let mut s2: Vec<i32> = snapshots[1].contents.clone();
        s2.sort();
        assert_eq!(s2, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_istream_empty_update() {
        let updates = vec![
            WindowUpdate::new(vec![], vec![], 0, 1000, 100),
            WindowUpdate::new(vec![1], vec![], 1000, 2000, 200),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = IStreamOperator::apply(stream).collect().await;
        assert_eq!(results, vec![1]);
    }

    #[tokio::test]
    async fn test_dstream_empty_update() {
        let updates = vec![WindowUpdate::new(vec![1, 2], vec![], 0, 1000, 100)];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = DStreamOperator::apply(stream).collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_istream_multiple_updates() {
        let updates = vec![
            WindowUpdate::new(vec![10, 20], vec![5], 0, 1000, 100),
            WindowUpdate::new(vec![30], vec![10], 1000, 2000, 200),
            WindowUpdate::new(vec![40, 50], vec![20, 30], 2000, 3000, 300),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = IStreamOperator::apply(stream).collect().await;
        assert_eq!(results, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_dstream_multiple_updates() {
        let updates = vec![
            WindowUpdate::new(vec![10, 20], vec![5], 0, 1000, 100),
            WindowUpdate::new(vec![30], vec![10], 1000, 2000, 200),
            WindowUpdate::new(vec![40, 50], vec![20, 30], 2000, 3000, 300),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let results: Vec<i32> = DStreamOperator::apply(stream).collect().await;
        assert_eq!(results, vec![5, 10, 20, 30]);
    }

    #[tokio::test]
    async fn test_rstream_empty_stream() {
        let updates: Vec<WindowUpdate<i32>> = vec![];
        let stream = Box::pin(futures::stream::iter(updates));
        let snapshots: Vec<WindowSnapshot<i32>> = RStreamOperator::apply(stream).collect().await;
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_rstream_removes_all() {
        let updates = vec![
            WindowUpdate::new(vec![1, 2, 3], vec![], 0, 1000, 100),
            WindowUpdate::new(vec![], vec![1, 2, 3], 1000, 2000, 200),
        ];
        let stream = Box::pin(futures::stream::iter(updates));
        let snapshots: Vec<WindowSnapshot<i32>> = RStreamOperator::apply(stream).collect().await;
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].contents.len(), 3);
        assert!(snapshots[1].contents.is_empty());
    }

    #[tokio::test]
    async fn test_rstream_window_metadata() {
        let updates = vec![WindowUpdate::new(vec![1], vec![], 5000, 10000, 7500)];
        let stream = Box::pin(futures::stream::iter(updates));
        let snapshots: Vec<WindowSnapshot<i32>> = RStreamOperator::apply(stream).collect().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].window_start, 5000);
        assert_eq!(snapshots[0].window_end, 10000);
        assert_eq!(snapshots[0].timestamp, 7500);
    }

    #[test]
    fn test_window_update_new() {
        let update = WindowUpdate::new(vec![1, 2], vec![3], 100, 200, 150);
        assert_eq!(update.added, vec![1, 2]);
        assert_eq!(update.removed, vec![3]);
        assert_eq!(update.window_start, 100);
        assert_eq!(update.window_end, 200);
        assert_eq!(update.timestamp, 150);
    }

    #[test]
    fn test_window_update_clone() {
        let update = WindowUpdate::new(vec![1], vec![2], 0, 100, 50);
        let cloned = update.clone();
        assert_eq!(cloned.added, vec![1]);
        assert_eq!(cloned.removed, vec![2]);
    }
}
