//! File-backed reference implementation of the storage SPI.
//!
//! Stores events and checkpoints as files in a base directory.
//! Intended for testing and development; not optimized for production use.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{CheckpointStore, EventJournal, PersistentBackend};
use crate::checkpoint::{CheckpointManifest, CheckpointSnapshot};
use crate::config::BackendConfig;
use crate::envelope::StreamEnvelope;
use crate::error::StorageError;
use crate::provider::StorageBackendProvider;

/// File-backed persistent backend.
///
/// Stores events in an append-only file and checkpoints as individual files.
/// Thread-safe via interior mutability.
pub struct FileBackedPersistentBackend {
    journal: FileBackedEventJournal,
    checkpoint_store: FileBackedCheckpointStore,
}

impl FileBackedPersistentBackend {
    /// Creates a new file-backed backend rooted at `base_dir`.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let base = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base)?;

        Ok(Self {
            journal: FileBackedEventJournal::new(base.clone())?,
            checkpoint_store: FileBackedCheckpointStore::new(base)?,
        })
    }
}

#[async_trait]
impl PersistentBackend for FileBackedPersistentBackend {
    fn event_journal(&self) -> &dyn EventJournal {
        &self.journal
    }

    fn checkpoint_store(&self) -> &dyn CheckpointStore {
        &self.checkpoint_store
    }

    async fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

// ── FileBackedEventJournal ───────────────────────────────────────────

struct FileBackedEventJournal {
    events: Mutex<Vec<StreamEnvelope>>,
    next_offset: AtomicU64,
    #[allow(dead_code)]
    base_dir: PathBuf,
}

impl FileBackedEventJournal {
    fn new(base_dir: PathBuf) -> Result<Self, StorageError> {
        let journal_dir = base_dir.join("journal");
        std::fs::create_dir_all(&journal_dir)?;
        Ok(Self {
            events: Mutex::new(Vec::new()),
            next_offset: AtomicU64::new(1),
            base_dir: journal_dir,
        })
    }
}

#[async_trait]
impl EventJournal for FileBackedEventJournal {
    async fn append(&self, mut event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
        event.offset = offset;
        let mut events = self.events.lock().await;
        events.push(event);
        Ok(offset)
    }

    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError> {
        let events = self.events.lock().await;
        Ok(events
            .iter()
            .filter(|e| e.offset > offset_exclusive)
            .cloned()
            .collect())
    }

    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError> {
        let mut events = self.events.lock().await;
        events.retain(|e| e.offset > offset_inclusive);
        Ok(())
    }
}

// ── FileBackedCheckpointStore ────────────────────────────────────────

struct FileBackedCheckpointStore {
    checkpoints: Mutex<Vec<CheckpointSnapshot>>,
    #[allow(dead_code)]
    base_dir: PathBuf,
}

impl FileBackedCheckpointStore {
    fn new(base_dir: PathBuf) -> Result<Self, StorageError> {
        let cp_dir = base_dir.join("checkpoints");
        std::fs::create_dir_all(&cp_dir)?;
        Ok(Self {
            checkpoints: Mutex::new(Vec::new()),
            base_dir: cp_dir,
        })
    }
}

#[async_trait]
impl CheckpointStore for FileBackedCheckpointStore {
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        let snapshot = CheckpointSnapshot::new(manifest, operator_state_blobs);
        let mut checkpoints = self.checkpoints.lock().await;
        checkpoints.push(snapshot);
        Ok(())
    }

    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError> {
        let checkpoints = self.checkpoints.lock().await;
        Ok(checkpoints.last().cloned())
    }

    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError> {
        let mut checkpoints = self.checkpoints.lock().await;
        checkpoints.retain(|cp| cp.manifest.id >= checkpoint_id);
        Ok(())
    }
}

// ── FileBackedStorageProvider ────────────────────────────────────────

/// Provider that creates [`FileBackedPersistentBackend`] instances.
pub struct FileBackedStorageProvider;

impl StorageBackendProvider for FileBackedStorageProvider {
    fn backend_id(&self) -> &str {
        "file"
    }

    fn create(&self, config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError> {
        let path = config
            .properties
            .get("path")
            .ok_or_else(|| StorageError::Other {
                message: "missing 'path' property for file backend".to_string(),
            })?;
        let backend = FileBackedPersistentBackend::new(path)?;
        Ok(Box::new(backend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_journal_append_and_read() {
        let dir = tempdir();
        let backend = FileBackedPersistentBackend::new(&dir).unwrap();
        let journal = backend.event_journal();

        let env1 = StreamEnvelope::new(0, "stream1", 100, "rdf", vec![1, 2, 3]);
        let env2 = StreamEnvelope::new(0, "stream1", 200, "rdf", vec![4, 5, 6]);

        let off1 = journal.append(env1).await.unwrap();
        let off2 = journal.append(env2).await.unwrap();
        assert!(off2 > off1);

        let events = journal.read_from(0).await.unwrap();
        assert_eq!(events.len(), 2);

        let events_after = journal.read_from(off1).await.unwrap();
        assert_eq!(events_after.len(), 1);
        assert_eq!(events_after[0].offset, off2);
    }

    #[tokio::test]
    async fn test_journal_truncate() {
        let dir = tempdir();
        let backend = FileBackedPersistentBackend::new(&dir).unwrap();
        let journal = backend.event_journal();

        let off1 = journal
            .append(StreamEnvelope::new(0, "s", 1, "rdf", vec![]))
            .await
            .unwrap();
        let _off2 = journal
            .append(StreamEnvelope::new(0, "s", 2, "rdf", vec![]))
            .await
            .unwrap();

        journal.truncate_before(off1).await.unwrap();

        let remaining = journal.read_from(0).await.unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_checkpoint_store() {
        let dir = tempdir();
        let backend = FileBackedPersistentBackend::new(&dir).unwrap();
        let store = backend.checkpoint_store();

        assert!(store.latest().await.unwrap().is_none());

        let manifest1 = CheckpointManifest::new(1, 1000, 10);
        store
            .write_checkpoint(manifest1, HashMap::new())
            .await
            .unwrap();

        let manifest2 = CheckpointManifest::new(2, 2000, 20);
        let mut blobs = HashMap::new();
        blobs.insert("op1".to_string(), vec![1, 2, 3]);
        store.write_checkpoint(manifest2, blobs).await.unwrap();

        let latest = store.latest().await.unwrap().unwrap();
        assert_eq!(latest.manifest.id, 2);
        assert_eq!(latest.manifest.event_offset, 20);
        assert!(latest.operator_state_blobs.contains_key("op1"));
    }

    #[tokio::test]
    async fn test_checkpoint_delete_older() {
        let dir = tempdir();
        let backend = FileBackedPersistentBackend::new(&dir).unwrap();
        let store = backend.checkpoint_store();

        for i in 1..=5 {
            let manifest = CheckpointManifest::new(i, i as i64 * 1000, i * 10);
            store
                .write_checkpoint(manifest, HashMap::new())
                .await
                .unwrap();
        }

        store.delete_older_than(3).await.unwrap();

        // Only checkpoints 3, 4, 5 should remain; latest is 5
        let latest = store.latest().await.unwrap().unwrap();
        assert_eq!(latest.manifest.id, 5);
    }

    #[tokio::test]
    async fn test_close_is_noop() {
        let dir = tempdir();
        let backend = FileBackedPersistentBackend::new(&dir).unwrap();
        backend.close().await.unwrap();
    }

    #[test]
    fn test_provider() {
        let dir = tempdir();
        let provider = FileBackedStorageProvider;
        assert_eq!(provider.backend_id(), "file");

        let config = BackendConfig::builder()
            .property("path", dir.to_str().unwrap())
            .build();
        let backend = provider.create(config);
        assert!(backend.is_ok());
    }

    #[test]
    fn test_provider_missing_path() {
        let provider = FileBackedStorageProvider;
        let config = BackendConfig::new();
        let result = provider.create(config);
        assert!(result.is_err());
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cqels-test-{}", rand_id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rand_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        base.wrapping_add(CTR.fetch_add(1, Ordering::Relaxed))
    }
}
