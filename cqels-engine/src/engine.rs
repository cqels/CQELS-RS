use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::Stream;
use tokio::sync::{broadcast, mpsc, Mutex};

use cqels_core::query::{ContinuousQuery, QueryInputs};
use cqels_core::stream::StreamElement;
use cqels_model::CqelsError;

/// Core stream engine trait.
///
/// Maps to Java's `StreamEngine` interface.
#[async_trait]
pub trait StreamEngine: Send + Sync {
    /// Registers a named input stream.
    async fn register_stream(
        &self,
        name: &str,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) -> Result<(), CqelsError>;

    /// Removes a named stream and aborts its forwarding task.
    async fn unregister_stream(&self, name: &str) -> Result<(), CqelsError>;

    /// Registers and executes a continuous query, returning a result stream.
    async fn register_query(
        &self,
        query: Box<dyn ContinuousQuery<Result = StreamElement>>,
    ) -> Result<Pin<Box<dyn Stream<Item = StreamElement> + Send>>, CqelsError>;

    /// Starts the engine, activating all registered streams.
    async fn start(&self) -> Result<(), CqelsError>;

    /// Stops the engine, completing all streams.
    async fn stop(&self) -> Result<(), CqelsError>;

    /// Returns whether the engine is currently running.
    fn is_running(&self) -> bool;
}

/// Shared stream state — a broadcast sender and the forwarding task handle.
struct StreamState {
    sender: broadcast::Sender<StreamElement>,
    _handle: Option<tokio::task::JoinHandle<()>>,
}

/// Reactive stream engine implementation using tokio channels.
///
/// Maps to Java's `ReactiveStreamEngine`.
///
/// Each registered stream is bridged to a `broadcast::Sender`, enabling multiple
/// subscribers (queries) to receive all events. Queries are executed as spawned tasks.
type PendingStream = (String, Pin<Box<dyn Stream<Item = StreamElement> + Send>>);

pub struct ReactiveStreamEngine {
    streams: Arc<Mutex<HashMap<String, StreamState>>>,
    pending: Arc<Mutex<Vec<PendingStream>>>,
    running: AtomicBool,
    broadcast_capacity: usize,
}

impl Default for ReactiveStreamEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ReactiveStreamEngine {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(Vec::new())),
            running: AtomicBool::new(false),
            broadcast_capacity: 4096,
        }
    }

    pub fn with_capacity(broadcast_capacity: usize) -> Self {
        Self {
            broadcast_capacity,
            ..Self::new()
        }
    }

    /// Gets a broadcast receiver for a named stream.
    pub async fn get_stream_receiver(
        &self,
        name: &str,
    ) -> Option<broadcast::Receiver<StreamElement>> {
        let streams = self.streams.lock().await;
        streams.get(name).map(|s| s.sender.subscribe())
    }

    /// Builds QueryInputs from all currently registered streams.
    async fn build_query_inputs(&self) -> QueryInputs {
        let mut inputs = QueryInputs::new();
        let streams = self.streams.lock().await;

        for name in streams.keys() {
            let rx = streams[name].sender.subscribe();
            let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
            let stream = futures::StreamExt::filter_map(stream, |result| {
                futures::future::ready(result.ok())
            });
            inputs.add_stream(name.clone(), Box::pin(stream));
        }

        inputs
    }

    async fn activate_pending(&self) {
        use futures::StreamExt;

        let mut pending = self.pending.lock().await;
        let streams_to_activate: Vec<_> = pending.drain(..).collect();
        drop(pending);

        let mut streams = self.streams.lock().await;
        for (name, mut input_stream) in streams_to_activate {
            if let Some(state) = streams.get_mut(&name) {
                let tx = state.sender.clone();
                let handle = tokio::spawn(async move {
                    while let Some(element) = input_stream.next().await {
                        let _ = tx.send(element);
                    }
                });
                state._handle = Some(handle);
            }
        }
    }
}

#[async_trait]
impl StreamEngine for ReactiveStreamEngine {
    async fn register_stream(
        &self,
        name: &str,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) -> Result<(), CqelsError> {
        let (tx, _rx) = broadcast::channel(self.broadcast_capacity);

        let mut streams = self.streams.lock().await;
        streams.insert(
            name.to_string(),
            StreamState {
                sender: tx,
                _handle: None,
            },
        );
        drop(streams);

        let mut pending = self.pending.lock().await;
        pending.push((name.to_string(), stream));

        if self.running.load(Ordering::Acquire) {
            drop(pending);
            self.activate_pending().await;
        }

        Ok(())
    }

    async fn register_query(
        &self,
        query: Box<dyn ContinuousQuery<Result = StreamElement>>,
    ) -> Result<Pin<Box<dyn Stream<Item = StreamElement> + Send>>, CqelsError> {
        let inputs = self.build_query_inputs().await;
        Ok(query.execute(inputs))
    }

    async fn start(&self) -> Result<(), CqelsError> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        self.activate_pending().await;
        Ok(())
    }

    async fn unregister_stream(&self, name: &str) -> Result<(), CqelsError> {
        let mut streams = self.streams.lock().await;
        if let Some(state) = streams.remove(name) {
            if let Some(handle) = state._handle {
                handle.abort();
            }
        }

        // Also remove from pending if not yet activated
        let mut pending = self.pending.lock().await;
        pending.retain(|(n, _)| n != name);

        Ok(())
    }

    async fn stop(&self) -> Result<(), CqelsError> {
        if self
            .running
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        // Abort all forwarding task handles before clearing
        let mut streams = self.streams.lock().await;
        for (_name, state) in streams.drain() {
            if let Some(handle) = state._handle {
                handle.abort();
            }
        }

        let mut pending = self.pending.lock().await;
        pending.clear();

        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
}

/// Bridge a broadcast receiver into a Stream.
pub fn receiver_to_stream(
    rx: broadcast::Receiver<StreamElement>,
) -> Pin<Box<dyn Stream<Item = StreamElement> + Send>> {
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
    let stream =
        futures::StreamExt::filter_map(stream, |result| futures::future::ready(result.ok()));
    Box::pin(stream)
}

/// Helper: Create an mpsc-based stream pair for feeding data into the engine.
pub fn create_stream_pair(
    buffer: usize,
) -> (
    mpsc::Sender<StreamElement>,
    Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
) {
    let (tx, rx) = mpsc::channel(buffer);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    (tx, Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_core::stream::{RdfStreamElement, StreamElement};
    use cqels_model::statement::Statement;
    use cqels_model::term::{IriTerm, LiteralTerm, Term};
    use futures::StreamExt;

    fn make_rdf_element(subject: &str, predicate: &str, value: &str, ts: i64) -> StreamElement {
        StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new(subject)),
                IriTerm::new(predicate),
                Term::Literal(LiteralTerm::new(value)),
            ),
            ts,
        ))
    }

    #[tokio::test]
    async fn test_engine_lifecycle() {
        let engine = ReactiveStreamEngine::new();
        assert!(!engine.is_running());

        engine.start().await.unwrap();
        assert!(engine.is_running());

        engine.start().await.unwrap();
        assert!(engine.is_running());

        engine.stop().await.unwrap();
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_register_and_receive() {
        let engine = ReactiveStreamEngine::new();

        let (tx, stream) = create_stream_pair(32);

        engine
            .register_stream("http://example.org/sensor", stream)
            .await
            .unwrap();

        engine.start().await.unwrap();

        let rx = engine
            .get_stream_receiver("http://example.org/sensor")
            .await
            .unwrap();
        let mut recv_stream = receiver_to_stream(rx);

        let elem = make_rdf_element("http://s", "http://p", "hello", 100);
        tx.send(elem).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            recv_stream.next(),
        )
        .await;

        assert!(received.is_ok());
        assert!(received.unwrap().is_some());

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_stop_aborts_handles() {
        let engine = ReactiveStreamEngine::new();

        let (tx, stream) = create_stream_pair(32);
        engine.register_stream("test", stream).await.unwrap();
        engine.start().await.unwrap();

        // Send an element to ensure the forwarding task is running
        let elem = make_rdf_element("http://s", "http://p", "val", 1);
        let _ = tx.send(elem).await;

        // Stop the engine — handles should be aborted
        engine.stop().await.unwrap();
        assert!(!engine.is_running());

        // After stop, streams map should be empty
        let streams = engine.streams.lock().await;
        assert!(streams.is_empty());
    }

    #[tokio::test]
    async fn test_unregister_stream() {
        let engine = ReactiveStreamEngine::new();

        let (_tx1, stream1) = create_stream_pair(32);
        let (_tx2, stream2) = create_stream_pair(32);

        engine.register_stream("stream1", stream1).await.unwrap();
        engine.register_stream("stream2", stream2).await.unwrap();
        engine.start().await.unwrap();

        // Both streams should be registered
        assert!(engine.get_stream_receiver("stream1").await.is_some());
        assert!(engine.get_stream_receiver("stream2").await.is_some());

        // Unregister stream1
        engine.unregister_stream("stream1").await.unwrap();

        // stream1 should be gone, stream2 remains
        assert!(engine.get_stream_receiver("stream1").await.is_none());
        assert!(engine.get_stream_receiver("stream2").await.is_some());

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let engine = ReactiveStreamEngine::new();

        let (tx, stream) = create_stream_pair(32);
        engine.register_stream("test", stream).await.unwrap();
        engine.start().await.unwrap();

        let rx1 = engine.get_stream_receiver("test").await.unwrap();
        let rx2 = engine.get_stream_receiver("test").await.unwrap();

        let mut s1 = receiver_to_stream(rx1);
        let mut s2 = receiver_to_stream(rx2);

        let elem = make_rdf_element("http://s", "http://p", "val", 1);
        tx.send(elem).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let r1 = tokio::time::timeout(std::time::Duration::from_millis(200), s1.next()).await;
        let r2 = tokio::time::timeout(std::time::Duration::from_millis(200), s2.next()).await;

        assert!(r1.is_ok() && r1.unwrap().is_some());
        assert!(r2.is_ok() && r2.unwrap().is_some());

        engine.stop().await.unwrap();
    }
}
