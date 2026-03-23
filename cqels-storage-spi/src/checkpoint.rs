//! Checkpoint manifest and snapshot types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Metadata for a checkpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointManifest {
    /// Unique checkpoint identifier.
    pub id: u64,
    /// Timestamp when this checkpoint was created (millis since epoch).
    pub created_at: i64,
    /// Journal offset up to which this checkpoint covers.
    pub event_offset: u64,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, String>,
}

impl CheckpointManifest {
    /// Creates a new manifest.
    pub fn new(id: u64, created_at: i64, event_offset: u64) -> Self {
        Self {
            id,
            created_at,
            event_offset,
            metadata: HashMap::new(),
        }
    }

    /// Adds a metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// A complete checkpoint snapshot: manifest plus operator state blobs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointSnapshot {
    /// The checkpoint manifest.
    pub manifest: CheckpointManifest,
    /// Operator state blobs keyed by operator ID.
    pub operator_state_blobs: HashMap<String, Vec<u8>>,
}

impl CheckpointSnapshot {
    /// Creates a new snapshot.
    pub fn new(
        manifest: CheckpointManifest,
        operator_state_blobs: HashMap<String, Vec<u8>>,
    ) -> Self {
        Self {
            manifest,
            operator_state_blobs,
        }
    }
}
