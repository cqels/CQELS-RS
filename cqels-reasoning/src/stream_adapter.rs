//! Async stream adapter for the synchronous RETE network.
//!
//! [`ReteStreamOperator`] wraps a [`ReteNetwork`] behind `Arc<Mutex<>>`
//! and provides a method to transform an input stream of
//! [`RdfStreamElement`] into an output stream that includes both
//! original and inferred elements.

use std::pin::Pin;
use std::sync::Arc;

use futures::stream::Stream;
use futures::StreamExt;
use tokio::sync::Mutex;

use cqels_core::stream::{RdfStreamElement, StreamElement};

use crate::config::ReasoningConfig;
use crate::network::{InferredRdfStreamElement, ReteNetwork};

/// Async stream adapter for the RETE reasoning engine.
///
/// Wraps a synchronous `ReteNetwork` in `Arc<Mutex<>>` so it can be used
/// safely from async contexts. Transforms an input stream by processing
/// each element through the RETE network and emitting both the original
/// element and any inferred elements.
pub struct ReteStreamOperator {
    network: Arc<Mutex<ReteNetwork>>,
}

impl ReteStreamOperator {
    /// Creates a new stream operator from a reasoning configuration.
    pub fn new(config: ReasoningConfig) -> Self {
        Self {
            network: Arc::new(Mutex::new(ReteNetwork::compile(config))),
        }
    }

    /// Creates a stream operator from an existing `ReteNetwork`.
    pub fn from_network(network: ReteNetwork) -> Self {
        Self {
            network: Arc::new(Mutex::new(network)),
        }
    }

    /// Transforms an input stream by applying RETE reasoning.
    ///
    /// For each input `StreamElement::Rdf`, the RETE network is invoked.
    /// The output stream yields the original element followed by any
    /// inferred elements (as `StreamElement::Rdf`).
    ///
    /// Non-RDF elements pass through unchanged.
    pub fn apply(
        &self,
        input: Pin<Box<dyn Stream<Item = StreamElement> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = StreamElement> + Send>> {
        let network = self.network.clone();

        Box::pin(input.then(move |elem| {
            let network = network.clone();
            async move {
                match &elem {
                    StreamElement::Rdf(rdf) => {
                        let mut net = network.lock().await;
                        let inferred = net.process_element(rdf);
                        drop(net);

                        let mut results = vec![elem];
                        for inf in inferred {
                            results.push(StreamElement::Rdf(inf.to_rdf_stream_element()));
                        }
                        results
                    }
                    _ => vec![elem],
                }
            }
        }).flat_map(futures::stream::iter))
    }

    /// Processes a single element and returns inferred elements only.
    pub async fn process(&self, element: &RdfStreamElement) -> Vec<InferredRdfStreamElement> {
        let mut network = self.network.lock().await;
        network.process_element(element)
    }

    /// Clears the network's working memory and inferred facts.
    pub async fn clear(&self) {
        let mut network = self.network.lock().await;
        network.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReasoningConfig;
    use crate::pattern::{PatternTerm, TriplePattern, TripleTemplate};
    use crate::rule::{Rule, RuleSet};
    use cqels_core::stream::RdfStreamElement;
    use cqels_model::term::{IriTerm, Term};
    use cqels_model::Statement;
    use futures::StreamExt;
    use std::time::Duration;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    fn make_element(s: &str, p: &str, o: &str, ts: i64) -> StreamElement {
        StreamElement::Rdf(RdfStreamElement::new(
            Statement::new(iri(s), IriTerm::new(p), iri(o)),
            ts,
        ))
    }

    fn make_test_config() -> ReasoningConfig {
        let rule = Rule::builder()
            .id("type-to-human")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        ReasoningConfig::builder()
            .rule_set(rule_set)
            .default_window(Duration::from_secs(600))
            .build()
    }

    #[tokio::test]
    async fn test_stream_operator_basic() {
        let operator = ReteStreamOperator::new(make_test_config());

        let input = vec![
            make_element("http://ex.org/alice", "http://ex.org/type", "http://ex.org/Person", 1000),
        ];
        let input_stream = Box::pin(futures::stream::iter(input));

        let results: Vec<StreamElement> = operator.apply(input_stream).collect().await;

        // Should have original + 1 inferred
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_stream_operator_no_match() {
        let operator = ReteStreamOperator::new(make_test_config());

        let input = vec![
            make_element("http://ex.org/alice", "http://ex.org/knows", "http://ex.org/bob", 1000),
        ];
        let input_stream = Box::pin(futures::stream::iter(input));

        let results: Vec<StreamElement> = operator.apply(input_stream).collect().await;

        // Only original element, no inferred
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_stream_operator_multiple_elements() {
        let operator = ReteStreamOperator::new(make_test_config());

        let input = vec![
            make_element("http://ex.org/alice", "http://ex.org/type", "http://ex.org/Person", 1000),
            make_element("http://ex.org/bob", "http://ex.org/type", "http://ex.org/Person", 2000),
        ];
        let input_stream = Box::pin(futures::stream::iter(input));

        let results: Vec<StreamElement> = operator.apply(input_stream).collect().await;

        // 2 original + 2 inferred = 4
        assert_eq!(results.len(), 4);
    }

    #[tokio::test]
    async fn test_process_single() {
        let operator = ReteStreamOperator::new(make_test_config());

        let elem = RdfStreamElement::new(
            Statement::new(
                iri("http://ex.org/alice"),
                IriTerm::new("http://ex.org/type"),
                iri("http://ex.org/Person"),
            ),
            1000,
        );

        let inferred = operator.process(&elem).await;
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].statement.predicate.as_str(), "http://ex.org/is");
    }

    #[tokio::test]
    async fn test_clear() {
        let operator = ReteStreamOperator::new(make_test_config());

        let elem = RdfStreamElement::new(
            Statement::new(
                iri("http://ex.org/alice"),
                IriTerm::new("http://ex.org/type"),
                iri("http://ex.org/Person"),
            ),
            1000,
        );

        // Process once
        let _ = operator.process(&elem).await;
        // Clear
        operator.clear().await;
        // Process again — should produce results again (dedup cleared)
        let inferred = operator.process(&elem).await;
        assert_eq!(inferred.len(), 1);
    }
}
