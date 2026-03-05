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
    Sparql,
    Cypher,
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
pub struct QueryInputs {
    streams: HashMap<String, Pin<Box<dyn Stream<Item = StreamElement> + Send>>>,
}

impl QueryInputs {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    pub fn add_stream(
        &mut self,
        name: impl Into<String>,
        stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) {
        self.streams.insert(name.into(), stream);
    }

    pub fn take_stream(
        &mut self,
        name: &str,
    ) -> Option<Pin<Box<dyn Stream<Item = StreamElement> + Send>>> {
        self.streams.remove(name)
    }

    pub fn has_stream(&self, name: &str) -> bool {
        self.streams.contains_key(name)
    }

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
