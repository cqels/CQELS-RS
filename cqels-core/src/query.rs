//! Continuous query trait and input stream management.
//!
//! Defines the [`ContinuousQuery`] trait that all query implementations
//! (SPARQL, Cypher, custom) must implement, along with [`QueryInputs`]
//! for managing named input streams and [`QueryType`] for identifying
//! the query language.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::stream::StreamElement;

/// Types of continuous queries supported.
///
/// Maps to Java's `QueryType` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueryType {
    /// SPARQL-based continuous query (RSP-QL/CQELS-QL).
    Sparql,
    /// Cypher-based continuous query (openCypher over streams).
    Cypher,
    /// A user-defined custom query language.
    Custom,
}

impl fmt::Display for QueryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryType::Sparql => write!(f, "SPARQL"),
            QueryType::Cypher => write!(f, "CYPHER"),
            QueryType::Custom => write!(f, "CUSTOM"),
        }
    }
}

/// Container for named input streams for a continuous query.
///
/// Maps to Java's `QueryInputs<T extends StreamElement>`.
///
/// # Examples
///
/// ```
/// use cqels_core::query::QueryInputs;
///
/// let mut inputs = QueryInputs::new();
/// let stream = Box::pin(futures::stream::empty());
/// inputs.add_stream("sensor_data", stream);
/// assert!(inputs.has_stream("sensor_data"));
/// ```
pub struct QueryInputs {
    streams: HashMap<String, Pin<Box<dyn Stream<Item = StreamElement> + Send>>>,
}

impl QueryInputs {
    /// Creates an empty input container.
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    /// Registers a named input stream.
    pub fn add_stream(
        &mut self,
        name: impl Into<String>,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) {
        self.streams.insert(name.into(), stream);
    }

    /// Removes and returns the stream with the given name.
    pub fn take_stream(
        &mut self,
        name: &str,
    ) -> Option<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> {
        self.streams.remove(name)
    }

    /// Returns `true` if a stream with the given name is registered.
    pub fn has_stream(&self, name: &str) -> bool {
        self.streams.contains_key(name)
    }

    /// Returns an iterator over registered stream names.
    pub fn stream_names(&self) -> impl Iterator<Item = &str> {
        self.streams.keys().map(|s| s.as_str())
    }
}

impl Default for QueryInputs {
    fn default() -> Self {
        Self::new()
    }
}

/// A continuous query that can be registered with the engine.
///
/// Maps to Java's `ContinuousQuery<T extends StreamElement, R>` interface.
#[async_trait]
pub trait ContinuousQuery: Send + Sync {
    /// The result type produced by this query.
    type Result: Send + 'static;

    /// Returns the unique query identifier.
    fn query_id(&self) -> &str;

    /// Returns the query string (SPARQL, Cypher, etc.).
    fn query_string(&self) -> &str;

    /// Returns the query type.
    fn query_type(&self) -> QueryType;

    /// Returns a list of `(synthetic_name, source_name)` pairs the
    /// engine should set up before calling [`Self::execute`]. For
    /// each pair, the engine subscribes to the user-registered
    /// stream named `source_name` and exposes a separate
    /// [`QueryInputs`] entry under `synthetic_name` so the query
    /// sees the same data under a different name (typically because
    /// a different window spec applies to the synthetic view).
    ///
    /// Default: empty — no aliasing.
    ///
    /// Used by [`crate::compiler::named_window`]-lowered queries
    /// that contain RSP-QL named windows with distinct specs over
    /// the same source.
    fn input_stream_aliases(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Executes this query on the given input streams.
    fn execute(&self, inputs: QueryInputs) -> Pin<Box<dyn Stream<Item = Self::Result> + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_type_display() {
        assert_eq!(QueryType::Sparql.to_string(), "SPARQL");
        assert_eq!(QueryType::Cypher.to_string(), "CYPHER");
        assert_eq!(QueryType::Custom.to_string(), "CUSTOM");
    }

    #[test]
    fn test_query_inputs() {
        let mut inputs = QueryInputs::new();
        assert!(!inputs.has_stream("test"));

        let stream = Box::pin(futures::stream::empty());
        inputs.add_stream("test", stream);
        assert!(inputs.has_stream("test"));
        assert!(!inputs.has_stream("other"));

        let names: Vec<&str> = inputs.stream_names().collect();
        assert_eq!(names, vec!["test"]);

        let taken = inputs.take_stream("test");
        assert!(taken.is_some());
        assert!(!inputs.has_stream("test"));
    }
}
