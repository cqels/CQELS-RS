//! Async sources of VSS signals.
//!
//! [`VssSignalSource`] is the seam that lets downstream code plug in a
//! real network transport (KUKSA.val WebSocket, Eclipse Sommr, etc.)
//! without coupling this crate to a specific broker. A reference
//! [`ChannelVssSignalSource`] backed by a `tokio::sync::mpsc` channel
//! ships for tests and in-process producers.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::signal::VssSignal;

/// Async source of VSS signals.
///
/// Implementations pull from whatever transport is in use (HTTP/SSE,
/// WebSocket, gRPC, in-memory channel, file replay, ...) and surface
/// signals through `next()`. A return of `None` indicates the source
/// has been closed and no further signals will arrive.
#[async_trait]
pub trait VssSignalSource: Send + Sync {
    /// Returns the next signal, or `None` if the source is closed.
    async fn next(&self) -> Option<VssSignal>;
}

/// A reference [`VssSignalSource`] backed by a `tokio::sync::mpsc`
/// channel. Pair the returned source with the returned [`Sender`] to
/// push signals from a producer task.
pub struct ChannelVssSignalSource {
    receiver: tokio::sync::Mutex<mpsc::Receiver<VssSignal>>,
}

impl ChannelVssSignalSource {
    /// Constructs a new channel-backed source with the supplied
    /// channel capacity. Returns `(source, sender)` so the caller can
    /// push signals.
    pub fn with_capacity(capacity: usize) -> (Self, Sender) {
        let (tx, rx) = mpsc::channel(capacity);
        let source = Self {
            receiver: tokio::sync::Mutex::new(rx),
        };
        (source, Sender { tx })
    }
}

#[async_trait]
impl VssSignalSource for ChannelVssSignalSource {
    async fn next(&self) -> Option<VssSignal> {
        let mut guard = self.receiver.lock().await;
        guard.recv().await
    }
}

/// Producer-side handle returned by
/// [`ChannelVssSignalSource::with_capacity`].
#[derive(Clone)]
pub struct Sender {
    tx: mpsc::Sender<VssSignal>,
}

impl Sender {
    /// Sends a signal to the source. Returns the signal back wrapped
    /// in `Err` if the source has been dropped.
    pub async fn send(&self, signal: VssSignal) -> Result<(), VssSignal> {
        self.tx.send(signal).await.map_err(|e| e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::VssValue;

    #[tokio::test]
    async fn channel_source_returns_pushed_signals_in_order() {
        let (source, sender) = ChannelVssSignalSource::with_capacity(4);
        sender
            .send(VssSignal::new("Vehicle.Speed", VssValue::from(10_i64), 1))
            .await
            .expect("send");
        sender
            .send(VssSignal::new("Vehicle.Speed", VssValue::from(20_i64), 2))
            .await
            .expect("send");

        let first = source.next().await.expect("first signal");
        assert_eq!(first.timestamp, 1);
        let second = source.next().await.expect("second signal");
        assert_eq!(second.timestamp, 2);
    }

    #[tokio::test]
    async fn channel_source_returns_none_after_sender_dropped() {
        let (source, sender) = ChannelVssSignalSource::with_capacity(1);
        drop(sender);
        assert!(source.next().await.is_none());
    }

    #[tokio::test]
    async fn sender_returns_err_after_source_dropped() {
        let (source, sender) = ChannelVssSignalSource::with_capacity(1);
        drop(source);
        let result = sender
            .send(VssSignal::new("Vehicle.Speed", VssValue::from(0_i64), 0))
            .await;
        assert!(result.is_err());
    }
}
