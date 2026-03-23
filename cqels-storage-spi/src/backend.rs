//! Core storage backend traits.
//!
//! Defines the [`PersistentBackend`], [`EventJournal`], and
//! [`CheckpointStore`] traits that storage providers must implement.

use async_trait::async_trait;

use crate::checkpoint::{CheckpointManifest, CheckpointSnapshot};
use crate::envelope::StreamEnvelope;
use crate::error::StorageError;

/// A persistent storage backend providing event journaling and checkpointing.
#[async_trait]
pub trait PersistentBackend: Send + Sync {
    /// Returns the event journal.
    fn event_journal(&self) -> &dyn EventJournal;

    /// Returns the checkpoint store.
    fn checkpoint_store(&self) -> &dyn CheckpointStore;

    /// Closes the backend, releasing any held resources.
    async fn close(&self) -> Result<(), StorageError>;
}

/// Append-only event journal for stream envelopes.
#[async_trait]
pub trait EventJournal: Send + Sync {
    /// Appends an event and returns its assigned offset.
    async fn append(&self, event: StreamEnvelope) -> Result<u64, StorageError>;

    /// Reads events starting after the given offset (exclusive).
    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError>;

    /// Truncates (deletes) events with offsets ≤ the given value.
    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError>;
}

/// Persistent store for checkpoint snapshots.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Writes a checkpoint snapshot.
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError>;

    /// Returns the latest checkpoint, if any.
    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError>;

    /// Deletes checkpoints older than the given checkpoint ID.
    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError>;
}
