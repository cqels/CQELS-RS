//! RDF codec for serializing/deserializing stream elements to bytes.

use cqels_core::stream::StreamElement;

use crate::codec::BinaryCodec;
use crate::error::StorageError;

/// A codec for RDF stream elements using JSON serialization.
pub struct RdfCodec;

impl BinaryCodec for RdfCodec {
    fn payload_type(&self) -> &str {
        "rdf"
    }

    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        Ok(data.to_vec())
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        Ok(data.to_vec())
    }
}

/// Serializes a `StreamElement` to bytes via JSON.
pub fn serialize_stream_element(element: &StreamElement) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(element).map_err(|e| StorageError::Codec {
        message: format!("failed to serialize StreamElement: {e}"),
    })
}

/// Deserializes a `StreamElement` from bytes.
pub fn deserialize_stream_element(data: &[u8]) -> Result<StreamElement, StorageError> {
    serde_json::from_slice(data).map_err(|e| StorageError::Codec {
        message: format!("failed to deserialize StreamElement: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_core::stream::{RdfStreamElement, StreamRecord};
    use cqels_model::statement::Statement;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::Term;

    #[test]
    fn test_rdf_codec_payload_type() {
        let codec = RdfCodec;
        assert_eq!(codec.payload_type(), "rdf");
    }

    #[test]
    fn test_roundtrip_rdf_element() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/sensor1")),
            IriTerm::new("http://example.org/temperature"),
            Term::Literal(LiteralTerm::new("25.5")),
        );
        let element = StreamElement::Rdf(RdfStreamElement::new(stmt, 1000));

        let bytes = serialize_stream_element(&element).unwrap();
        let deserialized = deserialize_stream_element(&bytes).unwrap();

        assert_eq!(element.timestamp(), deserialized.timestamp());
        assert!(deserialized.is_rdf());
        assert_eq!(
            element.as_statement().unwrap(),
            deserialized.as_statement().unwrap()
        );
    }

    #[test]
    fn test_roundtrip_record_element() {
        let element = StreamElement::Record(StreamRecord::new("test-payload", 2000));

        let bytes = serialize_stream_element(&element).unwrap();
        let deserialized = deserialize_stream_element(&bytes).unwrap();

        assert_eq!(element.timestamp(), deserialized.timestamp());
        assert!(!deserialized.is_rdf());
    }

    #[test]
    fn test_corrupted_data_returns_codec_error() {
        let result = deserialize_stream_element(b"not valid json");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, StorageError::Codec { .. }),
            "expected Codec error, got: {err:?}"
        );
    }
}
