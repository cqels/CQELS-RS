//! High-level runtime facade combining engine, store, parser, and compiler.
//!
//! [`CqelsRuntime`] provides a single entry point for:
//! - Registering input streams
//! - Loading static RDF data
//! - Submitting SPARQL/Cypher query strings
//! - Receiving result streams of [`BindingSet`]

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use cqels_core::compiler::{CqelsQueryCompiler, CypherQueryCompiler};
use cqels_core::parser::{CqelsQlParser, CypherQlParser};
use cqels_core::store::RdfStore;
use cqels_core::stream::StreamElement;
use cqels_model::{BindingSet, CqelsError, Statement};

use crate::engine::{ReactiveStreamEngine, StreamEngine};

/// High-level runtime combining engine, store, parser, and compiler.
///
/// Provides a simplified API for common CQELS workflows:
/// 1. Create a runtime
/// 2. Register input streams
/// 3. (Optionally) load static RDF data
/// 4. Submit query strings and collect results
pub struct CqelsRuntime {
    engine: ReactiveStreamEngine,
    store: Arc<dyn RdfStore>,
}

impl Default for CqelsRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl CqelsRuntime {
    /// Creates a new runtime with an in-memory oxigraph store.
    pub fn new() -> Self {
        Self {
            engine: ReactiveStreamEngine::new(),
            store: cqels_core::store::create_rdf_store()
                .expect("failed to create default RDF store"),
        }
    }

    /// Creates a runtime with a custom store and broadcast capacity.
    pub fn with_config(store: Arc<dyn RdfStore>, broadcast_capacity: usize) -> Self {
        Self {
            engine: ReactiveStreamEngine::with_capacity(broadcast_capacity),
            store,
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

    /// Registers a named input stream.
    pub async fn register_stream(
        &self,
        name: &str,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) -> Result<(), CqelsError> {
        self.engine.register_stream(name, stream).await
    }

    /// Starts the engine, activating all registered streams.
    pub async fn start(&self) -> Result<(), CqelsError> {
        self.engine.start().await
    }

    /// Stops the engine.
    pub async fn stop(&self) -> Result<(), CqelsError> {
        self.engine.stop().await
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
    /// Returns a stream of [`BindingSet`] results.
    pub async fn register_cypherql_query(
        &self,
        query_string: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BindingSet> + Send>>, CqelsError> {
        let definition = CypherQlParser::parse(query_string)
            .map_err(|e| CqelsError::Evaluation { message: format!("Parse error: {e}") })?;

        let compiled = CypherQueryCompiler::compile(query_string, definition)
            .map_err(|e| CqelsError::Evaluation { message: format!("Compile error: {e}") })?;

        self.engine
            .register_binding_query(Box::new(compiled))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_creation() {
        let runtime = CqelsRuntime::new();
        assert!(!runtime.engine().is_running());
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
}
