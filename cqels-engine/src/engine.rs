use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::stream::Stream;
use tokio::sync::{broadcast, mpsc, watch, Mutex};

use cqels_core::query::{ContinuousQuery, QueryInputs};
use cqels_core::stream::StreamElement;
use cqels_model::{BindingSet, CqelsError};

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

    /// Cancels and removes a previously registered query.
    ///
    /// The query's output stream will terminate on its next item yield.
    async fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError>;

    /// Starts the engine, activating all registered streams.
    async fn start(&self) -> Result<(), CqelsError>;

    /// Stops the engine, completing all streams.
    async fn stop(&self) -> Result<(), CqelsError>;

    /// Registers and executes a continuous query producing [`BindingSet`] results.
    ///
    /// This is the primary method for compiled CQELS-QL and Cypher-QL queries.
    /// The default implementation returns [`CqelsError::UnsupportedOperation`].
    async fn register_binding_query(
        &self,
        query: Box<dyn ContinuousQuery<Result = BindingSet>>,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        let _ = query;
        Err(CqelsError::UnsupportedOperation {
            operation: "register_binding_query".to_string(),
        })
    }

    /// Returns whether the engine is currently running.
    fn is_running(&self) -> bool;
}

/// Shared stream state — a broadcast sender and the forwarding task handle.
struct StreamState {
    sender: broadcast::Sender<StreamElement>,
    _handle: Option<tokio::task::JoinHandle<()>>,
}

/// Tracked state for a registered continuous query.
struct QueryState {
    /// Sending `true` on this channel cancels the query's output stream.
    cancel_tx: watch::Sender<bool>,
}

/// Stream wrapper that removes a query from the registry when the inner stream
/// ends naturally or the stream is dropped.
struct CleanupStream<S> {
    inner: S,
    queries: Arc<StdMutex<HashMap<String, QueryState>>>,
    query_id: String,
    cleaned: bool,
}

impl<S> CleanupStream<S> {
    fn do_cleanup(&mut self) {
        if !self.cleaned {
            self.cleaned = true;
            if let Ok(mut guard) = self.queries.lock() {
                guard.remove(&self.query_id);
            }
        }
    }
}

impl<S: Stream + Unpin> Stream for CleanupStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            this.do_cleanup();
        }
        result
    }
}

impl<S> Drop for CleanupStream<S> {
    fn drop(&mut self) {
        self.do_cleanup();
    }
}

/// Reactive stream engine implementation using tokio channels.
///
/// Maps to Java's `ReactiveStreamEngine`.
///
/// Each registered stream is bridged to a `broadcast::Sender`, enabling multiple
/// subscribers (queries) to receive all events. Queries are tracked in a registry
/// and can be individually cancelled via [`unregister_query`](StreamEngine::unregister_query).
type PendingStream = (String, Pin<Box<dyn Stream<Item = StreamElement> + Send>>);

pub struct ReactiveStreamEngine {
    streams: Arc<Mutex<HashMap<String, StreamState>>>,
    pending: Arc<Mutex<Vec<PendingStream>>>,
    running: AtomicBool,
    broadcast_capacity: usize,
    /// Registry of active queries, keyed by query_id.
    ///
    /// Uses `std::sync::Mutex` (not tokio) so that `CleanupStream::Drop`
    /// can synchronously remove entries without requiring an async runtime.
    queries: Arc<StdMutex<HashMap<String, QueryState>>>,
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
            queries: Arc::new(StdMutex::new(HashMap::new())),
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

    /// Registers and executes a continuous query that produces BindingSets.
    ///
    /// This bridges the type gap between compiled queries (which produce
    /// `BindingSet`) and the engine's stream infrastructure. The query is
    /// tracked in the registry and can be cancelled via [`Self::unregister_query`].
    pub async fn register_binding_query(
        &self,
        query: Box<dyn ContinuousQuery<Result = BindingSet>>,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        let query_id = query.query_id().to_string();
        let inputs = self.build_query_inputs().await;
        let raw_stream = query.execute(inputs);
        self.wrap_with_cancellation(query_id, raw_stream).await
    }

    /// Wraps a query output stream with cancellation and registers it.
    async fn wrap_with_cancellation<T: Send + 'static>(
        &self,
        query_id: String,
        raw_stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Result<Pin<Box<dyn Stream<Item = T> + Send>>, CqelsError> {
        use futures::StreamExt;

        let (cancel_tx, cancel_rx) = watch::channel(false);

        let mut queries = self.queries.lock().unwrap();
        if queries.contains_key(&query_id) {
            return Err(CqelsError::Stream {
                message: format!("query '{}' is already registered", query_id),
            });
        }
        let cleanup_id = query_id.clone();
        queries.insert(query_id, QueryState { cancel_tx });
        drop(queries);

        let wrapped = raw_stream.take_while(move |_| {
            let cancelled = *cancel_rx.borrow();
            futures::future::ready(!cancelled)
        });

        // Wrap with cleanup to remove from registry when stream ends
        let wrapped = CleanupStream {
            inner: wrapped,
            queries: Arc::clone(&self.queries),
            query_id: cleanup_id,
            cleaned: false,
        };

        Ok(Box::pin(wrapped))
    }

    /// Cancels and removes a previously registered query.
    ///
    /// The query's output stream will terminate on its next item yield.
    pub async fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        let mut queries = self.queries.lock().unwrap();
        match queries.remove(query_id) {
            Some(state) => {
                let _ = state.cancel_tx.send(true);
                Ok(())
            }
            None => Err(CqelsError::Stream {
                message: format!("query '{}' not found", query_id),
            }),
        }
    }

    /// Returns the IDs of all currently registered queries.
    pub async fn registered_query_ids(&self) -> Vec<String> {
        let queries = self.queries.lock().unwrap();
        queries.keys().cloned().collect()
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
                let stream_name = name.clone();
                let handle = tokio::spawn(async move {
                    while let Some(element) = input_stream.next().await {
                        if tx.send(element).is_err() {
                            tracing::warn!(
                                stream = %stream_name,
                                "broadcast send failed: all receivers dropped, stopping forwarding"
                            );
                            break;
                        }
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
        tracing::info!(stream_name = %name, "registering stream");
        let (tx, _rx) = broadcast::channel(self.broadcast_capacity);

        let mut streams = self.streams.lock().await;
        if streams.contains_key(name) {
            return Err(CqelsError::Stream {
                message: format!("stream '{}' is already registered", name),
            });
        }
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
        let query_id = query.query_id().to_string();
        tracing::info!(query_id = %query_id, "registering query");
        let inputs = self.build_query_inputs().await;
        let raw_stream = query.execute(inputs);
        self.wrap_with_cancellation(query_id, raw_stream).await
    }

    async fn register_binding_query(
        &self,
        query: Box<dyn ContinuousQuery<Result = BindingSet>>,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        // Delegate to the inherent method
        ReactiveStreamEngine::register_binding_query(self, query).await
    }

    async fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        // Delegate to the inherent method
        ReactiveStreamEngine::unregister_query(self, query_id).await
    }

    async fn start(&self) -> Result<(), CqelsError> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::debug!("engine already running, start is a no-op");
            return Ok(());
        }

        tracing::info!("engine started");
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

        // Cancel all registered queries
        let query_count;
        {
            let mut queries = self.queries.lock().unwrap();
            query_count = queries.len();
            for (_id, state) in queries.drain() {
                let _ = state.cancel_tx.send(true);
            }
        }

        // Abort all forwarding task handles before clearing
        let mut streams = self.streams.lock().await;
        let stream_count = streams.len();
        for (_name, state) in streams.drain() {
            if let Some(handle) = state._handle {
                handle.abort();
            }
        }

        let mut pending = self.pending.lock().await;
        pending.clear();

        tracing::info!(
            queries_cancelled = query_count,
            streams_stopped = stream_count,
            "engine stopped"
        );
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

        let received =
            tokio::time::timeout(std::time::Duration::from_millis(500), recv_stream.next()).await;

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
    async fn test_duplicate_stream_name_rejected() {
        let engine = ReactiveStreamEngine::new();

        let (_tx1, stream1) = create_stream_pair(32);
        let (_tx2, stream2) = create_stream_pair(32);

        engine.register_stream("dup", stream1).await.unwrap();
        let result = engine.register_stream("dup", stream2).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CqelsError::Stream { message } => {
                assert!(message.contains("already registered"));
            }
            other => panic!("expected Stream error, got: {other:?}"),
        }
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

    // -- Mock query for query lifecycle tests --

    use cqels_core::query::QueryType;
    use cqels_core::stream::StreamRecord;

    struct MockContinuousQuery {
        id: String,
    }

    impl MockContinuousQuery {
        fn new(id: &str) -> Self {
            Self { id: id.to_string() }
        }
    }

    #[async_trait]
    impl ContinuousQuery for MockContinuousQuery {
        type Result = StreamElement;

        fn query_id(&self) -> &str {
            &self.id
        }

        fn query_string(&self) -> &str {
            "MOCK QUERY"
        }

        fn query_type(&self) -> QueryType {
            QueryType::Custom
        }

        fn execute(
            &self,
            _inputs: QueryInputs,
        ) -> Pin<Box<dyn Stream<Item = StreamElement> + Send>> {
            Box::pin(futures::stream::unfold(0i64, |ts| async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let elem = StreamElement::Record(StreamRecord::new("mock", ts));
                Some((elem, ts + 1))
            }))
        }
    }

    #[tokio::test]
    async fn test_query_registration_and_unregistration() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        let query = MockContinuousQuery::new("test-query-1");
        let _result_stream = engine.register_query(Box::new(query)).await.unwrap();

        let ids = engine.registered_query_ids().await;
        assert_eq!(ids, vec!["test-query-1"]);

        engine.unregister_query("test-query-1").await.unwrap();

        let ids = engine.registered_query_ids().await;
        assert!(ids.is_empty());

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_query_cancellation_terminates_stream() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        let query = MockContinuousQuery::new("cancel-test");
        let mut result_stream = engine.register_query(Box::new(query)).await.unwrap();

        // Should get at least one item
        let first =
            tokio::time::timeout(std::time::Duration::from_millis(500), result_stream.next()).await;
        assert!(first.is_ok());
        assert!(first.unwrap().is_some());

        // Cancel the query
        engine.unregister_query("cancel-test").await.unwrap();

        // Stream should terminate (return None) on next item
        let after_cancel =
            tokio::time::timeout(std::time::Duration::from_millis(500), result_stream.next()).await;
        assert!(after_cancel.is_ok());
        assert!(after_cancel.unwrap().is_none());

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_query_id_rejected() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        let q1 = MockContinuousQuery::new("dup-id");
        let q2 = MockContinuousQuery::new("dup-id");

        let _s1 = engine.register_query(Box::new(q1)).await.unwrap();
        let result = engine.register_query(Box::new(q2)).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        match err {
            CqelsError::Stream { message } => {
                assert!(message.contains("already registered"));
            }
            other => panic!("expected Stream error, got: {other:?}"),
        }

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_unregister_nonexistent_query() {
        let engine = ReactiveStreamEngine::new();
        let result = engine.unregister_query("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CqelsError::Stream { message } => {
                assert!(message.contains("not found"));
            }
            other => panic!("expected Stream error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_stop_cancels_all_queries() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        let q1 = MockContinuousQuery::new("q1");
        let q2 = MockContinuousQuery::new("q2");
        let mut s1 = engine.register_query(Box::new(q1)).await.unwrap();
        let mut s2 = engine.register_query(Box::new(q2)).await.unwrap();

        let ids = engine.registered_query_ids().await;
        assert_eq!(ids.len(), 2);

        engine.stop().await.unwrap();

        // Both streams should terminate
        let r1 = tokio::time::timeout(std::time::Duration::from_millis(500), s1.next()).await;
        let r2 = tokio::time::timeout(std::time::Duration::from_millis(500), s2.next()).await;

        assert!(r1.is_ok() && r1.unwrap().is_none());
        assert!(r2.is_ok() && r2.unwrap().is_none());

        let ids = engine.registered_query_ids().await;
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_register_unregister() {
        let engine = Arc::new(ReactiveStreamEngine::new());
        let (tx, stream) = create_stream_pair(32);
        engine.register_stream("s1", stream).await.unwrap();
        engine.start().await.unwrap();

        // Spawn a producer
        let producer = tokio::spawn(async move {
            for i in 0..20 {
                let elem = make_rdf_element("http://s", "http://p", &format!("v{i}"), i);
                if tx.send(elem).await.is_err() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        // Register and unregister queries concurrently
        let engine2 = Arc::clone(&engine);
        let register_task = tokio::spawn(async move {
            for i in 0..5 {
                let id = format!("concurrent-q{i}");
                let query = MockContinuousQuery::new(&id);
                let _stream = engine2.register_query(Box::new(query)).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                engine2.unregister_query(&id).await.unwrap();
            }
        });

        register_task.await.unwrap();
        producer.abort();
        engine.stop().await.unwrap();

        let ids = engine.registered_query_ids().await;
        assert!(ids.is_empty());
    }

    // -- Mock binding query for register_binding_query tests --

    struct MockBindingQuery {
        id: String,
    }

    impl MockBindingQuery {
        fn new(id: &str) -> Self {
            Self { id: id.to_string() }
        }
    }

    #[async_trait]
    impl ContinuousQuery for MockBindingQuery {
        type Result = BindingSet;

        fn query_id(&self) -> &str {
            &self.id
        }

        fn query_string(&self) -> &str {
            "MOCK BINDING QUERY"
        }

        fn query_type(&self) -> QueryType {
            QueryType::Custom
        }

        fn execute(&self, _inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = BindingSet> + Send>> {
            Box::pin(futures::stream::unfold(0i64, |ts| async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Some((BindingSet::new(ts), ts + 1))
            }))
        }
    }

    #[tokio::test]
    async fn test_trait_register_binding_query() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        // Call through the StreamEngine trait
        let engine_trait: &dyn StreamEngine = &engine;
        let query = MockBindingQuery::new("binding-q1");
        let mut result_stream = engine_trait
            .register_binding_query(Box::new(query))
            .await
            .unwrap();

        // Should receive at least one BindingSet
        let first =
            tokio::time::timeout(std::time::Duration::from_millis(500), result_stream.next()).await;
        assert!(first.is_ok());
        let binding = first.unwrap().unwrap();
        assert_eq!(binding.timestamp(), 0);

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_completed_stream_removes_query_from_registry() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        // Create a finite query that produces exactly 3 items
        struct FiniteQuery {
            id: String,
        }

        #[async_trait]
        impl ContinuousQuery for FiniteQuery {
            type Result = StreamElement;

            fn query_id(&self) -> &str {
                &self.id
            }
            fn query_string(&self) -> &str {
                "FINITE"
            }
            fn query_type(&self) -> QueryType {
                QueryType::Custom
            }
            fn execute(
                &self,
                _inputs: QueryInputs,
            ) -> Pin<Box<dyn Stream<Item = StreamElement> + Send>> {
                Box::pin(futures::stream::iter(vec![
                    StreamElement::Record(StreamRecord::new("x", 1)),
                    StreamElement::Record(StreamRecord::new("x", 2)),
                    StreamElement::Record(StreamRecord::new("x", 3)),
                ]))
            }
        }

        let query = FiniteQuery {
            id: "finite-q".to_string(),
        };
        let mut result_stream = engine.register_query(Box::new(query)).await.unwrap();

        // Query should be registered
        let ids = engine.registered_query_ids().await;
        assert!(ids.contains(&"finite-q".to_string()));

        // Consume all items
        while result_stream.next().await.is_some() {}

        // Give the spawned cleanup task a moment to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Query should be removed from registry
        let ids = engine.registered_query_ids().await;
        assert!(
            !ids.contains(&"finite-q".to_string()),
            "completed query should be removed from registry"
        );

        engine.stop().await.unwrap();
    }

    #[test]
    fn test_drop_outside_runtime_does_not_panic() {
        // Create a runtime, register a query, then drop the runtime
        // before dropping the stream. This must not panic.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream = rt.block_on(async {
            let engine = ReactiveStreamEngine::new();
            engine.start().await.unwrap();
            let query = MockContinuousQuery::new("outside-rt");
            engine.register_query(Box::new(query)).await.unwrap()
        });
        // Drop the runtime first
        drop(rt);
        // Drop the stream outside any runtime — must not panic
        drop(stream);
    }

    #[tokio::test]
    async fn test_dropped_stream_removes_query_from_registry() {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();

        let query = MockContinuousQuery::new("drop-query");
        let result_stream = engine.register_query(Box::new(query)).await.unwrap();

        // Query should be registered
        let ids = engine.registered_query_ids().await;
        assert!(ids.contains(&"drop-query".to_string()));

        // Drop the stream without consuming it
        drop(result_stream);

        // Give the spawned cleanup task a moment to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Query should be removed from registry
        let ids = engine.registered_query_ids().await;
        assert!(
            !ids.contains(&"drop-query".to_string()),
            "dropped stream should remove query from registry"
        );

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_trait_register_binding_query_default() {
        // A minimal StreamEngine impl that uses the default register_binding_query
        struct MinimalEngine;

        #[async_trait]
        impl StreamEngine for MinimalEngine {
            async fn register_stream(
                &self,
                _name: &str,
                _stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
            ) -> Result<(), CqelsError> {
                Ok(())
            }
            async fn unregister_stream(&self, _name: &str) -> Result<(), CqelsError> {
                Ok(())
            }
            async fn register_query(
                &self,
                _query: Box<dyn ContinuousQuery<Result = StreamElement>>,
            ) -> Result<Pin<Box<dyn Stream<Item = StreamElement> + Send>>, CqelsError> {
                Err(CqelsError::UnsupportedOperation {
                    operation: "register_query".to_string(),
                })
            }
            async fn unregister_query(&self, _query_id: &str) -> Result<(), CqelsError> {
                Ok(())
            }
            async fn start(&self) -> Result<(), CqelsError> {
                Ok(())
            }
            async fn stop(&self) -> Result<(), CqelsError> {
                Ok(())
            }
            fn is_running(&self) -> bool {
                false
            }
        }

        let engine = MinimalEngine;
        let query = MockBindingQuery::new("default-test");
        let result = engine.register_binding_query(Box::new(query)).await;

        match result {
            Err(CqelsError::UnsupportedOperation { operation }) => {
                assert_eq!(operation, "register_binding_query");
            }
            Err(other) => panic!("expected UnsupportedOperation, got: {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
