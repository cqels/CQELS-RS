//! Simplified engine API via [`CqelsEngine`] facade.
//!
//! Provides a builder-based API for creating a CQELS engine with named
//! data streams, query registration with listener callbacks, and
//! optional reasoning integration.

use std::collections::HashMap;

use futures::StreamExt;

use cqels_model::{BindingSet, CqelsError};
use cqels_reasoning::ReasoningConfig;

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
/// let mut engine = CqelsEngine::builder()
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
    stream_senders: HashMap<String, DataStream>,
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

    /// Creates a named data stream and registers it with the engine.
    ///
    /// Returns a [`DataStream`] that can be used to push elements.
    pub async fn create_stream(&mut self, name: &str) -> Result<DataStream, CqelsError> {
        if self.stream_senders.contains_key(name) {
            return Err(CqelsError::Stream {
                message: format!("stream '{}' already exists", name),
            });
        }

        let (tx, stream) = create_stream_pair(4096);
        self.runtime.register_stream(name, stream).await?;

        let data_stream = DataStream::new(name.to_string(), tx);
        // Keep a reference by cloning the sender
        let probe = DataStream::new(name.to_string(), data_stream.tx.clone());
        self.stream_senders.insert(name.to_string(), probe);

        Ok(data_stream)
    }

    /// Returns a reference to a previously created data stream.
    pub fn get_stream(&self, name: &str) -> Option<&DataStream> {
        self.stream_senders.get(name)
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

        Ok(CqelsEngine {
            id,
            runtime,
            stream_senders: HashMap::new(),
            persistence: None,
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
        let mut engine = CqelsEngine::builder().build().unwrap();
        let stream = engine.create_stream("test-stream").await.unwrap();
        assert_eq!(stream.name(), "test-stream");
        assert!(engine.get_stream("test-stream").is_some());
        assert!(engine.get_stream("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_duplicate_stream_rejected() {
        let mut engine = CqelsEngine::builder().build().unwrap();
        engine.create_stream("dup").await.unwrap();
        let result = engine.create_stream("dup").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_push_data_through_stream() {
        let mut engine = CqelsEngine::builder().build().unwrap();
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
