//! High-level runtime facade combining engine, store, parser, compiler,
//! reasoning, and CEP.
//!
//! [`CqelsRuntime`] provides a single entry point for:
//! - Registering input streams
//! - Loading static RDF data
//! - Submitting SPARQL/Cypher query strings
//! - Enabling RETE-based reasoning on streams
//! - Registering CEP patterns
//! - Receiving result streams of [`BindingSet`] or [`PatternMatch`]

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use cqels_core::compiler::{CqelsQueryCompiler, CypherQueryCompiler};
use cqels_core::parser::{CqelsQlParser, CypherQlParser};
use cqels_core::store::RdfStore;
use cqels_core::stream::{StreamElement, Timestamped};
use cqels_model::{BindingSet, CqelsError, Statement};

use crate::cep::{NfaPatternProcessor, Pattern, PatternMatch};
use crate::engine::{ReactiveStreamEngine, StreamEngine};

/// High-level runtime combining engine, store, parser, compiler, reasoning,
/// and CEP into a unified API.
///
/// Provides a simplified API for common CQELS workflows:
/// 1. Create a runtime
/// 2. Register input streams
/// 3. (Optionally) load static RDF data and/or enable reasoning
/// 4. Submit query strings or CEP patterns and collect results
pub struct CqelsRuntime {
    engine: ReactiveStreamEngine,
    store: Arc<dyn RdfStore>,
    reasoning_operator: Option<cqels_reasoning::ReteStreamOperator>,
}

impl Default for CqelsRuntime {
    fn default() -> Self {
        Self::try_new().expect("failed to create default RDF store")
    }
}

impl CqelsRuntime {
    /// Creates a new runtime with an in-memory oxigraph store.
    ///
    /// Returns an error if the default RDF store cannot be created.
    pub fn try_new() -> Result<Self, CqelsError> {
        let store = cqels_core::store::create_rdf_store()
            .map_err(|e| CqelsError::Evaluation { message: format!("RDF store creation failed: {e}") })?;
        Ok(Self {
            engine: ReactiveStreamEngine::new(),
            store,
            reasoning_operator: None,
        })
    }

    /// Creates a new runtime with an in-memory oxigraph store.
    ///
    /// Panics if the default RDF store cannot be created. Prefer
    /// [`try_new()`](Self::try_new) if you need error handling.
    pub fn new() -> Self {
        Self::try_new().expect("failed to create default RDF store")
    }

    /// Creates a runtime with a custom store and broadcast capacity.
    pub fn with_config(store: Arc<dyn RdfStore>, broadcast_capacity: usize) -> Self {
        Self {
            engine: ReactiveStreamEngine::with_capacity(broadcast_capacity),
            store,
            reasoning_operator: None,
        }
    }

    /// Returns a reference to the underlying engine.
    pub fn engine(&self) -> &ReactiveStreamEngine {
        &self.engine
    }

    /// Returns a reference to the RDF store.
    pub fn store(&self) -> &Arc<dyn RdfStore> {
        &self.store
    }

    /// Enables RETE-based reasoning on all subsequently registered streams.
    ///
    /// When reasoning is enabled, each input stream element is processed
    /// through the RETE network before being fed to queries. Inferred
    /// triples are injected into the stream alongside original data.
    pub fn enable_reasoning(&mut self, config: cqels_reasoning::ReasoningConfig) {
        self.reasoning_operator = Some(cqels_reasoning::ReteStreamOperator::new(config));
    }

    /// Returns whether reasoning is currently enabled.
    pub fn has_reasoning(&self) -> bool {
        self.reasoning_operator.is_some()
    }

    /// Registers a named input stream.
    ///
    /// If reasoning is enabled, the stream is automatically wrapped with
    /// the RETE reasoning operator so inferred triples flow into queries.
    pub async fn register_stream(
        &self,
        name: &str,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) -> Result<(), CqelsError> {
        let stream = if let Some(operator) = &self.reasoning_operator {
            operator.apply(stream)
        } else {
            stream
        };
        self.engine.register_stream(name, stream).await
    }

    /// Starts the engine, activating all registered streams.
    pub async fn start(&self) -> Result<(), CqelsError> {
        self.engine.start().await
    }

    /// Stops the engine, cancelling all registered queries and streams.
    pub async fn stop(&self) -> Result<(), CqelsError> {
        self.engine.stop().await
    }

    /// Cancels and removes a previously registered query by its ID.
    ///
    /// The query's output stream will terminate on its next item yield.
    pub async fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        self.engine.unregister_query(query_id).await
    }

    /// Returns the IDs of all currently registered queries.
    pub async fn registered_query_ids(&self) -> Vec<String> {
        self.engine.registered_query_ids().await
    }

    /// Loads RDF statements into the default graph of the store.
    pub fn load_statements(&self, statements: &[Statement]) -> Result<(), String> {
        self.store.load_statements(statements)
    }

    /// Loads RDF statements into a named graph.
    pub fn load_named_graph(
        &self,
        graph_uri: &str,
        statements: &[Statement],
    ) -> Result<(), String> {
        self.store.load_named_graph(graph_uri, statements)
    }

    /// Parses, compiles, and registers a CqelsQL (SPARQL-style) query.
    ///
    /// The runtime's RDF store is automatically passed to the compiler
    /// for resolving `STATIC { ... }` and `GRAPH <uri> { ... }` patterns.
    ///
    /// Returns a stream of [`BindingSet`] results.
    pub async fn register_cqelsql_query(
        &self,
        query_string: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        let definition = CqelsQlParser::parse(query_string)
            .map_err(|e| CqelsError::Evaluation { message: format!("Parse error: {e}") })?;

        let compiled =
            CqelsQueryCompiler::compile_with_store(query_string, definition, Some(self.store.clone()))
                .map_err(|e| CqelsError::Evaluation { message: format!("Compile error: {e}") })?;

        self.engine
            .register_binding_query(Box::new(compiled))
            .await
    }

    /// Parses, compiles, and registers a CypherQL query.
    ///
    /// The runtime's RDF store is automatically passed to the compiler
    /// for resolving static and named graph patterns.
    ///
    /// Returns a stream of [`BindingSet`] results.
    pub async fn register_cypherql_query(
        &self,
        query_string: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        let definition = CypherQlParser::parse(query_string)
            .map_err(|e| CqelsError::Evaluation { message: format!("Parse error: {e}") })?;

        let compiled =
            CypherQueryCompiler::compile_with_store(query_string, definition, Some(self.store.clone()))
                .map_err(|e| CqelsError::Evaluation { message: format!("Compile error: {e}") })?;

        self.engine
            .register_binding_query(Box::new(compiled))
            .await
    }

    /// Registers a CEP pattern against a named stream.
    ///
    /// Returns a stream of [`PatternMatch`] results. The pattern is compiled
    /// into an NFA and evaluated against the named stream's elements.
    pub async fn register_cep_pattern<T>(
        &self,
        stream_name: &str,
        pattern: Pattern<T>,
    ) -> Result<Pin<Box<dyn Stream<Item = PatternMatch<T>> + Send>>, CqelsError>
    where
        T: Clone + Send + Sync + Timestamped + 'static,
    {
        let rx = self
            .engine
            .get_stream_receiver(stream_name)
            .await
            .ok_or_else(|| CqelsError::Stream {
                message: format!("stream '{}' not registered", stream_name),
            })?;

        // Convert broadcast receiver into a typed stream
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
        let typed_stream: Pin<Box<dyn Stream<Item = T> + Send>> = Box::pin(
            futures::StreamExt::filter_map(stream, |result| {
                futures::future::ready(result.ok().and_then(|elem| {
                    // Try to downcast — this works when T = StreamElement
                    // For other T types, the caller should use process() directly
                    let any: Box<dyn std::any::Any> = Box::new(elem);
                    any.downcast::<T>().ok().map(|t| *t)
                }))
            }),
        );

        let processor = NfaPatternProcessor::new(pattern);
        Ok(processor.process(typed_stream))
    }

    /// Registers a CEP pattern directly on a `StreamElement` stream.
    ///
    /// This is the most common CEP use case — matching patterns over
    /// the engine's RDF stream elements.
    pub async fn register_stream_cep_pattern(
        &self,
        stream_name: &str,
        pattern: Pattern<StreamElement>,
    ) -> Result<Pin<Box<dyn Stream<Item = PatternMatch<StreamElement>> + Send>>, CqelsError> {
        let rx = self
            .engine
            .get_stream_receiver(stream_name)
            .await
            .ok_or_else(|| CqelsError::Stream {
                message: format!("stream '{}' not registered", stream_name),
            })?;

        let stream = crate::engine::receiver_to_stream(rx);
        let processor = NfaPatternProcessor::new(pattern);
        Ok(processor.process(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_creation() {
        let runtime = CqelsRuntime::new();
        assert!(!runtime.engine().is_running());
        assert!(!runtime.has_reasoning());
    }

    #[tokio::test]
    async fn test_runtime_lifecycle() {
        let runtime = CqelsRuntime::new();
        runtime.start().await.unwrap();
        assert!(runtime.engine().is_running());
        runtime.stop().await.unwrap();
        assert!(!runtime.engine().is_running());
    }

    #[tokio::test]
    async fn test_runtime_load_statements() {
        use cqels_model::term::{IriTerm, LiteralTerm, Term};

        let runtime = CqelsRuntime::new();
        let stmts = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/s")),
            IriTerm::new("http://ex.org/p"),
            Term::Literal(LiteralTerm::new("value")),
        )];
        let result = runtime.load_statements(&stmts);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_runtime_enable_reasoning() {
        use cqels_reasoning::{ReasoningConfig, RuleSet};

        let mut runtime = CqelsRuntime::new();
        assert!(!runtime.has_reasoning());

        let config = ReasoningConfig::default_config(RuleSet::new(vec![]));
        runtime.enable_reasoning(config);
        assert!(runtime.has_reasoning());
    }

    #[tokio::test]
    async fn test_runtime_try_new() {
        let result = CqelsRuntime::try_new();
        assert!(result.is_ok());
        let runtime = result.unwrap();
        assert!(!runtime.engine().is_running());
    }

    #[tokio::test]
    async fn test_runtime_default() {
        let runtime = CqelsRuntime::default();
        assert!(!runtime.engine().is_running());
        assert!(!runtime.has_reasoning());
    }

    #[tokio::test]
    async fn test_runtime_with_config() {
        let store = cqels_core::store::create_rdf_store().unwrap();
        let runtime = CqelsRuntime::with_config(store, 64);
        assert!(!runtime.engine().is_running());
    }

    #[tokio::test]
    async fn test_runtime_register_stream() {
        use cqels_core::stream::{RdfStreamElement, StreamElement};
        use cqels_model::term::{IriTerm, LiteralTerm, Term};

        let runtime = CqelsRuntime::new();
        let elem = StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(
                Term::Iri(IriTerm::new("http://ex.org/s")),
                IriTerm::new("http://ex.org/p"),
                Term::Literal(LiteralTerm::new("v")),
            ),
            1000,
        ));
        let stream = Box::pin(futures::stream::iter(vec![elem]));
        let result = runtime.register_stream("test", stream).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_runtime_load_named_graph() {
        use cqels_model::term::{IriTerm, LiteralTerm, Term};

        let runtime = CqelsRuntime::new();
        let stmts = vec![Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/s")),
            IriTerm::new("http://ex.org/p"),
            Term::Literal(LiteralTerm::new("value")),
        )];
        let result = runtime.load_named_graph("http://ex.org/graph1", &stmts);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_runtime_unregister_query() {
        let runtime = CqelsRuntime::new();
        runtime.start().await.unwrap();

        let result = runtime.unregister_query("nonexistent").await;
        assert!(result.is_err());

        runtime.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_runtime_registered_query_ids() {
        let runtime = CqelsRuntime::new();
        let ids = runtime.registered_query_ids().await;
        assert!(ids.is_empty());
    }
}
