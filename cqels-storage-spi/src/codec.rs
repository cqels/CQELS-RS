//! Binary codec trait and registry for stream element serialization.

use std::collections::HashMap;

use crate::error::StorageError;

/// Trait for encoding/decoding stream payloads to/from bytes.
pub trait BinaryCodec: Send + Sync {
    /// The payload type tag this codec handles (e.g., `"rdf"`).
    fn payload_type(&self) -> &str;

    /// Encodes a value into bytes.
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError>;

    /// Decodes bytes back into the original representation.
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError>;
}

/// Registry of binary codecs keyed by payload type.
#[derive(Default)]
pub struct BinaryCodecRegistry {
    codecs: HashMap<String, Box<dyn BinaryCodec>>,
}

impl BinaryCodecRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a codec.
    pub fn register(&mut self, codec: Box<dyn BinaryCodec>) {
        self.codecs.insert(codec.payload_type().to_string(), codec);
    }

    /// Looks up a codec by payload type.
    pub fn get(&self, payload_type: &str) -> Option<&dyn BinaryCodec> {
        self.codecs.get(payload_type).map(|c| c.as_ref())
    }
}

/// A pass-through codec that performs no transformation (identity codec).
pub struct IdentityCodec {
    payload_type: String,
}

impl IdentityCodec {
    /// Creates an identity codec for the given payload type.
    pub fn new(payload_type: impl Into<String>) -> Self {
        Self {
            payload_type: payload_type.into(),
        }
    }
}

impl BinaryCodec for IdentityCodec {
    fn payload_type(&self) -> &str {
        &self.payload_type
    }

    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        Ok(data.to_vec())
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_codec() {
        let codec = IdentityCodec::new("rdf");
        assert_eq!(codec.payload_type(), "rdf");

        let data = b"hello world";
        let encoded = codec.encode(data).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_registry() {
        let mut registry = BinaryCodecRegistry::new();
        registry.register(Box::new(IdentityCodec::new("rdf")));
        registry.register(Box::new(IdentityCodec::new("record")));

        assert!(registry.get("rdf").is_some());
        assert!(registry.get("record").is_some());
        assert!(registry.get("unknown").is_none());
    }
}
