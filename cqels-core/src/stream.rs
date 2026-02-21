use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use cqels_model::Statement;

/// Trait for anything that has a timestamp.
pub trait Timestamped {
    /// Returns the timestamp in milliseconds since epoch.
    fn timestamp(&self) -> i64;
}

/// Core stream element — the atom of stream processing.
///
/// Maps to Java's `StreamElement` interface. Can contain either an RDF statement
/// or a generic payload.
#[derive(Clone, Debug)]
pub enum StreamElement {
    /// An RDF stream element carrying an RDF statement.
    Rdf(RdfStreamElement),
    /// A generic stream record carrying an arbitrary payload.
    Record(StreamRecord),
}

impl StreamElement {
    /// Returns the timestamp of this stream element.
    pub fn timestamp(&self) -> i64 {
        match self {
            StreamElement::Rdf(rdf) => rdf.timestamp,
            StreamElement::Record(rec) => rec.timestamp,
        }
    }

    /// Returns `true` if this element contains an RDF statement.
    pub fn is_rdf(&self) -> bool {
        matches!(self, StreamElement::Rdf(_))
    }

    /// Returns the RDF statement if this is an RDF element, `None` otherwise.
    pub fn as_statement(&self) -> Option<&Statement> {
        match self {
            StreamElement::Rdf(rdf) => Some(&rdf.statement),
            StreamElement::Record(_) => None,
        }
    }

    /// Returns the RDF element if this is an RDF element.
    pub fn as_rdf(&self) -> Option<&RdfStreamElement> {
        match self {
            StreamElement::Rdf(rdf) => Some(rdf),
            _ => None,
        }
    }
}

impl Timestamped for StreamElement {
    fn timestamp(&self) -> i64 {
        self.timestamp()
    }
}

impl fmt::Display for StreamElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamElement::Rdf(rdf) => write!(f, "{rdf}"),
            StreamElement::Record(rec) => write!(f, "{rec}"),
        }
    }
}

/// An RDF stream element carrying an RDF statement with a timestamp.
///
/// Maps to Java's `RDFStreamElement` class.
#[derive(Clone, Debug)]
pub struct RdfStreamElement {
    pub statement: Statement,
    pub timestamp: i64,
}

impl RdfStreamElement {
    /// Creates a new RDF stream element with the given statement and timestamp.
    pub fn new(statement: Statement, timestamp: i64) -> Self {
        Self {
            statement,
            timestamp,
        }
    }

    /// Creates a new RDF stream element with the current system time.
    pub fn now(statement: Statement) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            statement,
            timestamp,
        }
    }
}

impl Timestamped for RdfStreamElement {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

impl fmt::Display for RdfStreamElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RdfStreamElement[ts={}, stmt={}]",
            self.timestamp, self.statement
        )
    }
}

impl From<RdfStreamElement> for StreamElement {
    fn from(rdf: RdfStreamElement) -> Self {
        StreamElement::Rdf(rdf)
    }
}

/// A generic stream record carrying a typed value with a timestamp.
///
/// Maps to Java's `StreamRecord<T>`.
#[derive(Clone, Debug)]
pub struct StreamRecord {
    pub value: String,
    pub timestamp: i64,
}

impl StreamRecord {
    /// Creates a new stream record.
    pub fn new(value: impl Into<String>, timestamp: i64) -> Self {
        Self {
            value: value.into(),
            timestamp,
        }
    }
}

impl Timestamped for StreamRecord {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

impl fmt::Display for StreamRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamRecord[ts={}, value={}]",
            self.timestamp, self.value
        )
    }
}

impl From<StreamRecord> for StreamElement {
    fn from(rec: StreamRecord) -> Self {
        StreamElement::Record(rec)
    }
}

/// A stream event — either a data record or a watermark.
///
/// Maps to Java's `StreamEvent<T>` interface.
/// Watermarks signal event-time progression and trigger window operations.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent<T: Clone> {
    /// A data record with a value and timestamp.
    Record {
        value: T,
        timestamp: i64,
    },
    /// A watermark indicating that all events up to this timestamp have been received.
    Watermark {
        timestamp: i64,
    },
}

impl<T: Clone> StreamEvent<T> {
    /// Creates a new record event.
    pub fn record(value: T, timestamp: i64) -> Self {
        StreamEvent::Record { value, timestamp }
    }

    /// Creates a new watermark event.
    pub fn watermark(timestamp: i64) -> Self {
        StreamEvent::Watermark { timestamp }
    }

    /// Returns the timestamp of this event.
    pub fn timestamp(&self) -> i64 {
        match self {
            StreamEvent::Record { timestamp, .. } => *timestamp,
            StreamEvent::Watermark { timestamp } => *timestamp,
        }
    }

    /// Returns `true` if this is a watermark.
    pub fn is_watermark(&self) -> bool {
        matches!(self, StreamEvent::Watermark { .. })
    }

    /// Returns `true` if this is a record.
    pub fn is_record(&self) -> bool {
        matches!(self, StreamEvent::Record { .. })
    }

    /// Returns the value if this is a record, `None` if it's a watermark.
    pub fn value(&self) -> Option<&T> {
        match self {
            StreamEvent::Record { value, .. } => Some(value),
            StreamEvent::Watermark { .. } => None,
        }
    }

    /// Maps the value of this event using the provided function.
    pub fn map<U: Clone, F: FnOnce(&T) -> U>(&self, f: F) -> StreamEvent<U> {
        match self {
            StreamEvent::Record { value, timestamp } => StreamEvent::Record {
                value: f(value),
                timestamp: *timestamp,
            },
            StreamEvent::Watermark { timestamp } => StreamEvent::Watermark {
                timestamp: *timestamp,
            },
        }
    }
}

impl<T: Clone> Timestamped for StreamEvent<T> {
    fn timestamp(&self) -> i64 {
        self.timestamp()
    }
}

impl<T: Clone + fmt::Display> fmt::Display for StreamEvent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamEvent::Record { value, timestamp } => {
                write!(f, "Record[ts={timestamp}, value={value}]")
            }
            StreamEvent::Watermark { timestamp } => {
                write!(f, "Watermark[ts={timestamp}]")
            }
        }
    }
}

/// A value with an associated timestamp, used in window operations.
#[derive(Clone, Debug, PartialEq)]
pub struct TimestampedValue<T> {
    pub value: T,
    pub timestamp: i64,
}

impl<T> TimestampedValue<T> {
    pub fn new(value: T, timestamp: i64) -> Self {
        Self { value, timestamp }
    }
}

impl<T> Timestamped for TimestampedValue<T> {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::IriTerm;
    use cqels_model::term::LiteralTerm;
    use cqels_model::Term;

    fn sample_statement() -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new("http://example.org/sensor1")),
            IriTerm::new("http://example.org/temperature"),
            Term::Literal(LiteralTerm::new("25.5")),
        )
    }

    #[test]
    fn test_rdf_stream_element() {
        let stmt = sample_statement();
        let elem = RdfStreamElement::new(stmt.clone(), 1000);
        assert_eq!(elem.timestamp, 1000);
        assert_eq!(elem.statement, stmt);
    }

    #[test]
    fn test_rdf_stream_element_now() {
        let stmt = sample_statement();
        let elem = RdfStreamElement::now(stmt);
        assert!(elem.timestamp > 0);
    }

    #[test]
    fn test_stream_element_rdf() {
        let stmt = sample_statement();
        let elem = StreamElement::Rdf(RdfStreamElement::new(stmt.clone(), 500));
        assert!(elem.is_rdf());
        assert_eq!(elem.timestamp(), 500);
        assert_eq!(elem.as_statement(), Some(&stmt));
    }

    #[test]
    fn test_stream_element_record() {
        let elem = StreamElement::Record(StreamRecord::new("test-value", 100));
        assert!(!elem.is_rdf());
        assert_eq!(elem.timestamp(), 100);
        assert!(elem.as_statement().is_none());
    }

    #[test]
    fn test_stream_element_from_rdf() {
        let rdf = RdfStreamElement::new(sample_statement(), 42);
        let elem: StreamElement = rdf.into();
        assert!(elem.is_rdf());
        assert_eq!(elem.timestamp(), 42);
    }

    #[test]
    fn test_stream_event_record() {
        let event: StreamEvent<i32> = StreamEvent::record(42, 1000);
        assert!(event.is_record());
        assert!(!event.is_watermark());
        assert_eq!(event.timestamp(), 1000);
        assert_eq!(event.value(), Some(&42));
    }

    #[test]
    fn test_stream_event_watermark() {
        let event: StreamEvent<i32> = StreamEvent::watermark(5000);
        assert!(event.is_watermark());
        assert!(!event.is_record());
        assert_eq!(event.timestamp(), 5000);
        assert_eq!(event.value(), None);
    }

    #[test]
    fn test_stream_event_map() {
        let event: StreamEvent<i32> = StreamEvent::record(10, 100);
        let mapped = event.map(|v| v * 2);
        assert_eq!(mapped, StreamEvent::record(20, 100));

        let wm: StreamEvent<i32> = StreamEvent::watermark(200);
        let mapped_wm: StreamEvent<i32> = wm.map(|v| v * 2);
        assert_eq!(mapped_wm, StreamEvent::watermark(200));
    }

    #[test]
    fn test_stream_event_equality() {
        let e1: StreamEvent<String> = StreamEvent::record("hello".to_string(), 100);
        let e2: StreamEvent<String> = StreamEvent::record("hello".to_string(), 100);
        let e3: StreamEvent<String> = StreamEvent::record("world".to_string(), 100);
        assert_eq!(e1, e2);
        assert_ne!(e1, e3);
    }

    #[test]
    fn test_timestamped_value() {
        let tv = TimestampedValue::new("data", 999);
        assert_eq!(tv.value, "data");
        assert_eq!(tv.timestamp, 999);
        assert_eq!(Timestamped::timestamp(&tv), 999);
    }

    #[test]
    fn test_stream_element_display() {
        let elem = StreamElement::Rdf(RdfStreamElement::new(sample_statement(), 100));
        let display = format!("{elem}");
        assert!(display.contains("RdfStreamElement"));
        assert!(display.contains("100"));
    }

    #[test]
    fn test_stream_record_display() {
        let rec = StreamRecord::new("sensor-data", 500);
        let display = format!("{rec}");
        assert!(display.contains("StreamRecord"));
        assert!(display.contains("sensor-data"));
    }
}
