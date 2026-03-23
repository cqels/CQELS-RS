//! Stream envelope for event journaling.

use serde::{Deserialize, Serialize};

/// An envelope wrapping a stream event for persistent journaling.
///
/// Each envelope carries an offset (assigned by the journal), the
/// originating stream name, a timestamp, a payload type tag, and
/// the raw payload bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamEnvelope {
    /// Monotonically increasing offset assigned by the journal.
    pub offset: u64,
    /// Name of the originating stream.
    pub stream_name: String,
    /// Event timestamp (milliseconds since epoch).
    pub timestamp: i64,
    /// Payload type discriminator (e.g., `"rdf"`, `"record"`).
    pub payload_type: String,
    /// Raw payload bytes.
    pub payload: Vec<u8>,
}

impl StreamEnvelope {
    /// Creates a new envelope (offset is typically set by the journal).
    pub fn new(
        offset: u64,
        stream_name: impl Into<String>,
        timestamp: i64,
        payload_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            offset,
            stream_name: stream_name.into(),
            timestamp,
            payload_type: payload_type.into(),
            payload,
        }
    }
}
