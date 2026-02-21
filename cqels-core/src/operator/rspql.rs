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
        let stream = stream.scan(
            HashSet::<T>::new(),
            |state, update| {
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
            },
        );
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
}
