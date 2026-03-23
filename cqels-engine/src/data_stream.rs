//! Push-based data stream API for feeding data into the CQELS engine.
//!
//! [`DataStream`] wraps an `mpsc::Sender<StreamElement>` and provides
//! typed convenience methods for pushing triples with different object types.

use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::{CqelsError, Statement, Term};
use tokio::sync::mpsc;

/// A named push-based data stream for sending elements into the engine.
///
/// Created via [`CqelsEngine::create_stream`](super::facade::CqelsEngine::create_stream).
/// Dropping the `DataStream` (or calling [`complete`](Self::complete)) signals
/// the end of the stream to downstream consumers.
///
/// # Examples
///
/// ```no_run
/// # use cqels_engine::data_stream::DataStream;
/// # async fn example(stream: DataStream) -> Result<(), cqels_model::CqelsError> {
/// stream.push_triple("http://sensor/1", "http://ex/temp", "http://ex/high").await?;
/// stream.push_f64("http://sensor/1", "http://ex/value", 42.0).await?;
/// stream.complete();
/// # Ok(())
/// # }
/// ```
pub struct DataStream {
    name: String,
    pub(crate) tx: mpsc::Sender<StreamElement>,
}

impl DataStream {
    /// Creates a new `DataStream` with the given name and sender.
    pub(crate) fn new(name: String, tx: mpsc::Sender<StreamElement>) -> Self {
        Self { name, tx }
    }

    /// Returns the stream name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Pushes a raw [`StreamElement`] into the stream.
    pub async fn push(&self, element: StreamElement) -> Result<(), CqelsError> {
        self.tx.send(element).await.map_err(|_| CqelsError::Stream {
            message: format!("stream '{}' channel closed", self.name),
        })
    }

    /// Pushes an RDF triple with an IRI object.
    pub async fn push_triple(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Result<(), CqelsError> {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Iri(IriTerm::new(object)),
        );
        let ts = current_timestamp();
        self.push(StreamElement::Rdf(RdfStreamElement::new(stmt, ts)))
            .await
    }

    /// Pushes an RDF triple with a string literal object.
    pub async fn push_literal(
        &self,
        subject: &str,
        predicate: &str,
        literal: &str,
    ) -> Result<(), CqelsError> {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Literal(LiteralTerm::new(literal)),
        );
        let ts = current_timestamp();
        self.push(StreamElement::Rdf(RdfStreamElement::new(stmt, ts)))
            .await
    }

    /// Pushes an RDF triple with an `f64` literal object.
    pub async fn push_f64(
        &self,
        subject: &str,
        predicate: &str,
        value: f64,
    ) -> Result<(), CqelsError> {
        let lit = LiteralTerm::new(value.to_string())
            .with_datatype("http://www.w3.org/2001/XMLSchema#double");
        let stmt = Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Literal(lit),
        );
        let ts = current_timestamp();
        self.push(StreamElement::Rdf(RdfStreamElement::new(stmt, ts)))
            .await
    }

    /// Pushes an RDF triple with an `i64` literal object.
    pub async fn push_i64(
        &self,
        subject: &str,
        predicate: &str,
        value: i64,
    ) -> Result<(), CqelsError> {
        let lit = LiteralTerm::new(value.to_string())
            .with_datatype("http://www.w3.org/2001/XMLSchema#integer");
        let stmt = Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Literal(lit),
        );
        let ts = current_timestamp();
        self.push(StreamElement::Rdf(RdfStreamElement::new(stmt, ts)))
            .await
    }

    /// Pushes an RDF triple with a boolean literal object.
    pub async fn push_bool(
        &self,
        subject: &str,
        predicate: &str,
        value: bool,
    ) -> Result<(), CqelsError> {
        let lit = LiteralTerm::new(value.to_string())
            .with_datatype("http://www.w3.org/2001/XMLSchema#boolean");
        let stmt = Statement::new(
            Term::Iri(IriTerm::new(subject)),
            IriTerm::new(predicate),
            Term::Literal(lit),
        );
        let ts = current_timestamp();
        self.push(StreamElement::Rdf(RdfStreamElement::new(stmt, ts)))
            .await
    }

    /// Signals that no more elements will be sent. Equivalent to dropping the stream.
    pub fn complete(self) {
        drop(self);
    }
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_data_stream_push_triple() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        ds.push_triple("http://s", "http://p", "http://o")
            .await
            .unwrap();

        let elem = rx.recv().await.unwrap();
        assert!(elem.is_rdf());
        let stmt = elem.as_statement().unwrap();
        assert_eq!(stmt.predicate.as_str(), "http://p");
    }

    #[tokio::test]
    async fn test_data_stream_push_literal() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        ds.push_literal("http://s", "http://p", "hello")
            .await
            .unwrap();

        let elem = rx.recv().await.unwrap();
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => assert_eq!(lit.value(), "hello"),
            _ => panic!("expected literal"),
        }
    }

    #[tokio::test]
    async fn test_data_stream_push_f64() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        ds.push_f64("http://s", "http://p", 42.5).await.unwrap();

        let elem = rx.recv().await.unwrap();
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => {
                assert_eq!(lit.value(), "42.5");
                assert_eq!(
                    lit.datatype(),
                    Some("http://www.w3.org/2001/XMLSchema#double")
                );
            }
            _ => panic!("expected literal"),
        }
    }

    #[tokio::test]
    async fn test_data_stream_push_i64() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        ds.push_i64("http://s", "http://p", 99).await.unwrap();

        let elem = rx.recv().await.unwrap();
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => {
                assert_eq!(lit.value(), "99");
                assert_eq!(
                    lit.datatype(),
                    Some("http://www.w3.org/2001/XMLSchema#integer")
                );
            }
            _ => panic!("expected literal"),
        }
    }

    #[tokio::test]
    async fn test_data_stream_push_bool() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        ds.push_bool("http://s", "http://p", true).await.unwrap();

        let elem = rx.recv().await.unwrap();
        let stmt = elem.as_statement().unwrap();
        match &stmt.object {
            Term::Literal(lit) => {
                assert_eq!(lit.value(), "true");
                assert_eq!(
                    lit.datatype(),
                    Some("http://www.w3.org/2001/XMLSchema#boolean")
                );
            }
            _ => panic!("expected literal"),
        }
    }

    #[tokio::test]
    async fn test_data_stream_complete() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);
        ds.complete();
        // After complete, recv should return None
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_data_stream_name() {
        let (tx, _rx) = mpsc::channel::<StreamElement>(16);
        let ds = DataStream::new("my-stream".to_string(), tx);
        assert_eq!(ds.name(), "my-stream");
    }

    #[tokio::test]
    async fn test_push_after_receiver_dropped() {
        let (tx, rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);
        drop(rx);
        let result = ds.push_triple("http://s", "http://p", "http://o").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_push_raw_stream_element() {
        use cqels_core::stream::{RdfStreamElement, StreamElement};
        use cqels_model::term::{IriTerm, LiteralTerm};
        use cqels_model::{Statement, Term};

        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("raw".to_string(), tx);

        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://s")),
            IriTerm::new("http://p"),
            Term::Literal(LiteralTerm::new("val")),
        );
        let elem = StreamElement::Rdf(RdfStreamElement::new(stmt, 42));
        ds.push(elem).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.timestamp(), 42);
    }

    #[tokio::test]
    async fn test_complete_closes_channel() {
        let (tx, mut rx) = mpsc::channel(16);
        let ds = DataStream::new("test".to_string(), tx);

        // Push one element, then complete
        ds.push_triple("http://s", "http://p", "http://o")
            .await
            .unwrap();
        ds.complete();

        // Should still get the buffered element
        assert!(rx.recv().await.is_some());
        // Then None after channel closes
        assert!(rx.recv().await.is_none());
    }
}
