//! Simplified engine API via [`CqelsEngine`] facade.
//!
//! Provides a builder-based API for creating a CQELS engine with named
//! data streams, query registration with listener callbacks, and
//! optional reasoning integration.

use std::collections::HashMap;
use std::sync::Mutex;

use futures::StreamExt;

use cqels_model::{BindingSet, CqelsError, Statement};
use cqels_reasoning::ReasoningConfig;

use crate::checkpoint_manager::CheckpointManager;
use crate::data_stream::DataStream;
use crate::engine::{create_stream_pair, StreamEngine};
use crate::listener::QueryResultListener;
use crate::persistence::{EnginePersistenceConfig, PersistenceCoordinator};
use crate::runtime::CqelsRuntime;

/// A simplified engine facade wrapping [`CqelsRuntime`] with push-based
/// [`DataStream`] APIs and listener-based query result delivery.
///
/// # Examples
///
/// ```no_run
/// use cqels_engine::facade::CqelsEngine;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), cqels_model::CqelsError> {
/// let engine = CqelsEngine::builder()
///     .id("my-engine")
///     .broadcast_capacity(1024)
///     .build()?;
///
/// let stream = engine.create_stream("sensors").await?;
/// engine.start().await?;
///
/// stream.push_f64("http://sensor/1", "http://ex/temp", 25.5).await?;
///
/// engine.stop().await?;
/// # Ok(())
/// # }
/// ```
pub struct CqelsEngine {
    id: String,
    runtime: CqelsRuntime,
    stream_senders: Mutex<HashMap<String, DataStream>>,
    persistence: Option<PersistenceCoordinator>,
}

impl CqelsEngine {
    /// Returns a builder for constructing a `CqelsEngine`.
    pub fn builder() -> CqelsEngineBuilder {
        CqelsEngineBuilder::default()
    }

    /// Returns the engine identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns a reference to the engine's RDF store.
    ///
    /// The store backs `STATIC { ... }` and `GRAPH <iri> { ... }` patterns
    /// in registered queries. Use the returned
    /// [`cqels_core::store::RdfStore`] trait methods to load static data:
    ///
    /// ```ignore
    /// engine.store().load_statements(&triples)?;
    /// engine.store().load_named_graph("http://ex/g", &triples)?;
    /// ```
    ///
    /// Load before [`register_cqelsql_query`](Self::register_cqelsql_query)
    /// (or any other registration method) so the compiled query's
    /// static-pattern resolution sees the data. The store is shared via
    /// `Arc` — clones reference the same underlying graph, so loading
    /// later is safe but the query's *first* execution captures whatever
    /// is present at that point.
    pub fn store(&self) -> &std::sync::Arc<dyn cqels_core::store::RdfStore> {
        self.runtime.store()
    }

    /// Creates a named data stream and registers it with the engine.
    ///
    /// Returns a [`DataStream`] that can be used to push elements.
    pub async fn create_stream(&self, name: &str) -> Result<DataStream, CqelsError> {
        let (tx, stream) = create_stream_pair(4096);
        self.runtime.register_stream(name, stream).await?;

        let data_stream = DataStream::new(name.to_string(), tx);
        // Keep a reference by cloning the sender
        let probe = DataStream::new(name.to_string(), data_stream.tx.clone());
        self.stream_senders
            .lock()
            .expect("stream sender registry mutex poisoned")
            .insert(name.to_string(), probe);

        Ok(data_stream)
    }

    /// Returns a clone of a previously created data stream handle.
    pub fn get_stream(&self, name: &str) -> Option<DataStream> {
        self.stream_senders
            .lock()
            .expect("stream sender registry mutex poisoned")
            .get(name)
            .cloned()
    }

    /// Closes a previously-created data stream by dropping the
    /// engine's internal sender clone, then awaits the forwarding
    /// task so any buffered events on the mpsc finish flowing
    /// through to subscribers. The caller is expected to have
    /// dropped their own `DataStream` handles first; with every
    /// `Sender` dropped, the mpsc closes naturally, the forwarding
    /// task's loop exits, and its broadcast `Sender` clone drops —
    /// at which point all subscribers see end-of-stream and any
    /// event-driven operators (in particular
    /// `TumblingTimeWindowStream`) flush their final open batch.
    ///
    /// This is the "soft close" variant: never `abort()`s the
    /// forwarding task. Use it when correctness of in-flight events
    /// matters (parity tests, replay drains). For abrupt shutdown
    /// use [`Self::stop`].
    pub async fn close_stream(&self, name: &str) -> Result<(), CqelsError> {
        // 1. Drop the engine's mpsc Sender clone. Combined with the
        //    caller having dropped theirs, the mpsc starts closing.
        self.stream_senders
            .lock()
            .expect("stream sender registry mutex poisoned")
            .remove(name);
        // 2. Take the forwarding task's JoinHandle out of the engine
        //    so we can await its natural completion. Removing the
        //    StreamState also drops the engine's `broadcast::Sender`,
        //    but the forwarding task still holds a clone — the
        //    channel stays open until that clone drops too.
        let handle = self.runtime.engine().take_forwarding_handle(name).await;
        // 3. Wait for the forwarding task to drain the mpsc and
        //    naturally exit. Aborts on the *engine side* would race
        //    with the events still in the mpsc buffer.
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    /// Registers a CQELS-QL query and delivers results to a listener.
    ///
    /// Returns the assigned query ID.
    pub async fn register_cqelsql_query<L>(
        &self,
        query: &str,
        listener: L,
    ) -> Result<String, CqelsError>
    where
        L: QueryResultListener<BindingSet> + 'static,
    {
        let reg = self.runtime.register_cqelsql_query(query).await?;
        let query_id = reg.query_id.clone();

        tokio::spawn(async move {
            let mut stream = reg.stream;
            while let Some(result) = stream.next().await {
                listener.on_result(result);
            }
            listener.on_complete();
        });

        Ok(query_id)
    }

    /// Registers a Cypher-QL query and delivers results to a listener.
    ///
    /// Returns the assigned query ID.
    pub async fn register_cypherql_query<L>(
        &self,
        query: &str,
        listener: L,
    ) -> Result<String, CqelsError>
    where
        L: QueryResultListener<BindingSet> + 'static,
    {
        let reg = self.runtime.register_cypherql_query(query).await?;
        let query_id = reg.query_id.clone();

        tokio::spawn(async move {
            let mut stream = reg.stream;
            while let Some(result) = stream.next().await {
                listener.on_result(result);
            }
            listener.on_complete();
        });

        Ok(query_id)
    }

    /// Registers a CONSTRUCT query and delivers results to a listener.
    ///
    /// Returns the assigned query ID.
    pub async fn register_construct_query<L>(
        &self,
        query: &str,
        listener: L,
    ) -> Result<String, CqelsError>
    where
        L: QueryResultListener<Statement> + 'static,
    {
        let reg = self.runtime.register_construct_query(query).await?;
        let query_id = reg.query_id.clone();

        tokio::spawn(async move {
            let mut stream = reg.stream;
            while let Some(result) = stream.next().await {
                listener.on_result(result);
            }
            listener.on_complete();
        });

        Ok(query_id)
    }

    /// Registers an ASK query and delivers results to a listener.
    ///
    /// Returns the assigned query ID.
    pub async fn register_ask_query<L>(
        &self,
        query: &str,
        listener: L,
    ) -> Result<String, CqelsError>
    where
        L: QueryResultListener<bool> + 'static,
    {
        let reg = self.runtime.register_ask_query(query).await?;
        let query_id = reg.query_id.clone();

        tokio::spawn(async move {
            let mut stream = reg.stream;
            while let Some(result) = stream.next().await {
                listener.on_result(result);
            }
            listener.on_complete();
        });

        Ok(query_id)
    }

    /// Registers a DESCRIBE query and delivers results to a listener.
    ///
    /// Returns the assigned query ID.
    pub async fn register_describe_query<L>(
        &self,
        query: &str,
        listener: L,
    ) -> Result<String, CqelsError>
    where
        L: QueryResultListener<Statement> + 'static,
    {
        let reg = self.runtime.register_describe_query(query).await?;
        let query_id = reg.query_id.clone();

        tokio::spawn(async move {
            let mut stream = reg.stream;
            while let Some(result) = stream.next().await {
                listener.on_result(result);
            }
            listener.on_complete();
        });

        Ok(query_id)
    }

    /// Cancels and removes a previously registered query by its ID.
    pub async fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        self.runtime.unregister_query(query_id).await
    }

    /// Returns the IDs of all currently registered queries.
    pub async fn registered_query_ids(&self) -> Vec<String> {
        self.runtime.registered_query_ids().await
    }

    /// Returns the names of all currently registered input streams.
    pub async fn registered_stream_names(&self) -> Vec<String> {
        self.runtime.registered_stream_names().await
    }

    /// Loads RDF statements into the default graph of the store.
    pub fn load_statements(&self, statements: &[Statement]) -> Result<(), CqelsError> {
        self.runtime.load_statements(statements)
    }

    /// Loads RDF statements into a named graph.
    pub fn load_named_graph(
        &self,
        graph_uri: &str,
        statements: &[Statement],
    ) -> Result<(), CqelsError> {
        self.runtime.load_named_graph(graph_uri, statements)
    }

    /// Starts the engine, activating all registered streams.
    pub async fn start(&self) -> Result<(), CqelsError> {
        self.runtime.start().await
    }

    /// Stops the engine.
    pub async fn stop(&self) -> Result<(), CqelsError> {
        self.runtime.stop().await
    }

    /// Returns whether the engine is currently running.
    pub fn is_running(&self) -> bool {
        self.runtime.engine().is_running()
    }

    /// Returns the active reasoning profile, if reasoning is enabled.
    pub fn reasoning_profile(&self) -> Option<cqels_reasoning::ReasoningProfile> {
        self.runtime.reasoning_profile()
    }

    /// Returns a reference to the underlying runtime.
    pub fn runtime(&self) -> &CqelsRuntime {
        &self.runtime
    }

    /// Returns a reference to the persistence coordinator, if configured.
    pub fn persistence(&self) -> Option<&PersistenceCoordinator> {
        self.persistence.as_ref()
    }
}

/// Builder for [`CqelsEngine`].
pub struct CqelsEngineBuilder {
    id: Option<String>,
    broadcast_capacity: usize,
    reasoning_config: Option<ReasoningConfig>,
    persistence_config: Option<EnginePersistenceConfig>,
}

impl Default for CqelsEngineBuilder {
    fn default() -> Self {
        Self {
            id: None,
            broadcast_capacity: 4096,
            reasoning_config: None,
            persistence_config: None,
        }
    }
}

impl CqelsEngineBuilder {
    fn default_storage_providers(
    ) -> HashMap<String, Box<dyn cqels_storage_spi::StorageBackendProvider>> {
        let mut providers: HashMap<String, Box<dyn cqels_storage_spi::StorageBackendProvider>> =
            HashMap::new();
        providers.insert(
            "file".to_string(),
            Box::new(cqels_storage_spi::FileBackedStorageProvider),
        );
        providers
    }

    /// Sets the engine identifier.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Sets the broadcast channel capacity for stream fan-out.
    pub fn broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
    }

    /// Enables reasoning with the given configuration.
    pub fn reasoning_config(mut self, config: ReasoningConfig) -> Self {
        self.reasoning_config = Some(config);
        self
    }

    /// Sets the persistence configuration.
    pub fn persistence_config(mut self, config: EnginePersistenceConfig) -> Self {
        self.persistence_config = Some(config);
        self
    }

    /// Builds the engine.
    pub fn build(self) -> Result<CqelsEngine, CqelsError> {
        let store = cqels_core::store::create_rdf_store()?;
        let mut runtime = CqelsRuntime::with_config(store, self.broadcast_capacity);

        if let Some(config) = self.reasoning_config {
            runtime.enable_reasoning(config);
        }

        let id = self
            .id
            .unwrap_or_else(|| format!("cqels-engine-{}", uuid_counter()));

        // Wire persistence if configured and enabled
        let persistence = if let Some(ref config) = self.persistence_config {
            if config.enabled {
                let providers = Self::default_storage_providers();

                match PersistenceCoordinator::new(&providers, config) {
                    Ok(engine_coord) => {
                        let engine_coord = std::sync::Arc::new(engine_coord);
                        runtime.engine_mut().set_persistence(engine_coord.clone());

                        let checkpoint_mgr = std::sync::Arc::new(CheckpointManager::new(
                            engine_coord,
                            config.checkpoint_policy.clone(),
                        ));
                        runtime.engine_mut().set_checkpoint_manager(checkpoint_mgr);

                        // Create a separate coordinator for runtime recovery and facade accessor
                        match PersistenceCoordinator::new(&providers, config) {
                            Ok(runtime_coord) => {
                                runtime.set_persistence_coordinator(runtime_coord);
                                PersistenceCoordinator::new(&providers, config).ok()
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "failed to create runtime persistence coordinator: {e}"
                                );
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to create persistence coordinator: {e}");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(CqelsEngine {
            id,
            runtime,
            stream_senders: Mutex::new(HashMap::new()),
            persistence,
        })
    }
}

fn uuid_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_builder_defaults() {
        let engine = CqelsEngine::builder().build().unwrap();
        assert!(!engine.is_running());
        assert!(engine.id().starts_with("cqels-engine-"));
    }

    #[tokio::test]
    async fn test_engine_builder_custom_id() {
        let engine = CqelsEngine::builder().id("test-engine").build().unwrap();
        assert_eq!(engine.id(), "test-engine");
    }

    #[tokio::test]
    async fn test_engine_lifecycle() {
        let engine = CqelsEngine::builder().build().unwrap();
        assert!(!engine.is_running());

        engine.start().await.unwrap();
        assert!(engine.is_running());

        engine.stop().await.unwrap();
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_create_stream() {
        let engine = CqelsEngine::builder().build().unwrap();
        let stream = engine.create_stream("test-stream").await.unwrap();
        assert_eq!(stream.name(), "test-stream");
        assert!(engine.get_stream("test-stream").is_some());
        assert!(engine.get_stream("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_duplicate_stream_rejected() {
        let engine = CqelsEngine::builder().build().unwrap();
        engine.create_stream("dup").await.unwrap();
        let result = engine.create_stream("dup").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn concurrent_create_stream_allows_only_one_winner() {
        let engine = std::sync::Arc::new(CqelsEngine::builder().build().unwrap());
        let first = {
            let engine = engine.clone();
            async move { engine.create_stream("race").await }
        };
        let second = {
            let engine = engine.clone();
            async move { engine.create_stream("race").await }
        };

        let (a, b) = tokio::join!(first, second);
        let successes = usize::from(a.is_ok()) + usize::from(b.is_ok());

        assert_eq!(successes, 1);
        assert!(engine.get_stream("race").is_some());
        assert_eq!(engine.registered_stream_names().await, vec!["race"]);
    }

    #[tokio::test]
    async fn test_push_data_through_stream() {
        let engine = CqelsEngine::builder().build().unwrap();
        let stream = engine.create_stream("data").await.unwrap();
        engine.start().await.unwrap();

        // Push should succeed
        stream
            .push_triple("http://s", "http://p", "http://o")
            .await
            .unwrap();

        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_with_reasoning() {
        use cqels_reasoning::ReasoningProfile;

        let config = ReasoningProfile::Rdfs.create_config();
        let engine = CqelsEngine::builder()
            .reasoning_config(config)
            .build()
            .unwrap();
        assert!(engine.runtime().has_reasoning());
    }

    #[tokio::test]
    async fn test_reasoning_profile_introspection() {
        use cqels_reasoning::ReasoningProfile;

        // Without reasoning — no profile
        let engine = CqelsEngine::builder().build().unwrap();
        assert!(engine.reasoning_profile().is_none());

        // With RDFS profile
        let config = ReasoningProfile::Rdfs.create_config();
        let engine = CqelsEngine::builder()
            .reasoning_config(config)
            .build()
            .unwrap();
        assert_eq!(engine.reasoning_profile(), Some(ReasoningProfile::Rdfs));
    }

    #[tokio::test]
    async fn test_persistence_accessor_none_by_default() {
        let engine = CqelsEngine::builder().build().unwrap();
        assert!(engine.persistence().is_none());
    }
}
