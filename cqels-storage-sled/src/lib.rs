//! Sled-backed implementation of the [`cqels_storage_spi`] traits.
//!
//! Mirrors Java's `cqels-storage-rocksdb` module in shape and semantics
//! but uses [`sled`](https://crates.io/crates/sled) — a pure-Rust embedded
//! KV store — as the storage engine.
//!
//! **Why sled, not RocksDB?** The RocksDB Rust crate (`rocksdb`) and
//! oxigraph's transitively-included `oxrocksdb-sys` both declare
//! `links = "rocksdb"`, so Cargo refuses to compile both in the same
//! workspace. Sled has equivalent semantics for our use case (ordered
//! key/value store with prefix iteration and transactional batches), no
//! native link conflict, and faster compile times. A future RocksDB
//! backend can be slotted in once oxigraph's `oxrocksdb-sys` is gated
//! behind an opt-in feature (or the storage SPI is moved to a separate
//! crate not depending on cqels-model).
//!
//! Storage layout:
//! - **Event journal** in tree `journal`. Keys are 8-byte big-endian
//!   offsets; values are JSON-encoded `StreamEnvelope`s.
//! - **Checkpoints** in tree `checkpoints`. Keys are 8-byte big-endian
//!   checkpoint IDs; values are JSON-encoded `CheckpointSnapshot`s.
//! - **Meta** in tree `meta` for the recoverable next-offset counter.
//!
//! This is the first production-grade backend in the cqels-rs workspace,
//! joining the file-backed dev impl in [`cqels_storage_spi::file_backend`].

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, CheckpointSnapshot, CheckpointStore, EventJournal,
    PersistentBackend, StorageBackendProvider, StorageError, StreamEnvelope,
};
use parking_lot::Mutex;
use sled::{Db, Tree};

const TREE_JOURNAL: &str = "journal";
const TREE_CHECKPOINTS: &str = "checkpoints";
const TREE_META: &str = "meta";
const META_KEY_NEXT_OFFSET: &[u8] = b"next_offset";

fn err_other(msg: impl Into<String>) -> StorageError {
    StorageError::Other {
        message: msg.into(),
    }
}

fn err_codec(msg: impl Into<String>) -> StorageError {
    StorageError::Codec {
        message: msg.into(),
    }
}

/// Sled-backed persistent backend.
pub struct SledPersistentBackend {
    journal: SledEventJournal,
    checkpoints: SledCheckpointStore,
    db: Arc<Db>,
}

impl SledPersistentBackend {
    /// Opens (or creates) a sled database at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        let db = sled::open(&path).map_err(|e| err_other(e.to_string()))?;
        let db = Arc::new(db);

        let journal_tree = db
            .open_tree(TREE_JOURNAL)
            .map_err(|e| err_other(e.to_string()))?;
        let checkpoints_tree = db
            .open_tree(TREE_CHECKPOINTS)
            .map_err(|e| err_other(e.to_string()))?;
        let meta_tree = db
            .open_tree(TREE_META)
            .map_err(|e| err_other(e.to_string()))?;

        let next_offset = recover_next_offset(&meta_tree)?;

        Ok(Self {
            journal: SledEventJournal {
                tree: journal_tree,
                meta: meta_tree,
                next_offset: AtomicU64::new(next_offset),
                meta_lock: Mutex::new(()),
            },
            checkpoints: SledCheckpointStore {
                tree: checkpoints_tree,
            },
            db,
        })
    }

    /// Path-based open via the SPI's [`BackendConfig`]. The path is read
    /// from the `path` property.
    pub fn from_config(config: BackendConfig) -> Result<Self, StorageError> {
        let path = config
            .properties
            .get("path")
            .ok_or_else(|| err_other("sled backend requires `path` property"))?
            .clone();
        Self::open(path)
    }
}

fn recover_next_offset(meta: &Tree) -> Result<u64, StorageError> {
    match meta
        .get(META_KEY_NEXT_OFFSET)
        .map_err(|e| err_other(e.to_string()))?
    {
        Some(bytes) if bytes.len() == 8 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            Ok(u64::from_be_bytes(buf))
        }
        _ => Ok(1),
    }
}

#[async_trait]
impl PersistentBackend for SledPersistentBackend {
    fn event_journal(&self) -> &dyn EventJournal {
        &self.journal
    }

    fn checkpoint_store(&self) -> &dyn CheckpointStore {
        &self.checkpoints
    }

    async fn close(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

/// Append-only event journal stored in the `journal` tree.
pub struct SledEventJournal {
    tree: Tree,
    meta: Tree,
    next_offset: AtomicU64,
    meta_lock: Mutex<()>,
}

#[async_trait]
impl EventJournal for SledEventJournal {
    async fn append(&self, mut event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        event.offset = offset;
        let key = offset.to_be_bytes();
        let value = serde_json::to_vec(&event).map_err(|e| err_codec(e.to_string()))?;
        self.tree
            .insert(key, value)
            .map_err(|e| err_other(e.to_string()))?;

        let _guard = self.meta_lock.lock();
        let next = (offset + 1).to_be_bytes();
        self.meta
            .insert(META_KEY_NEXT_OFFSET, &next)
            .map_err(|e| err_other(e.to_string()))?;

        Ok(offset)
    }

    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError> {
        let start = offset_exclusive.saturating_add(1).to_be_bytes();
        let mut events = Vec::new();
        for item in self.tree.range(start..) {
            let (_k, v) = item.map_err(|e| err_other(e.to_string()))?;
            let env: StreamEnvelope =
                serde_json::from_slice(&v).map_err(|e| err_codec(e.to_string()))?;
            events.push(env);
        }
        Ok(events)
    }

    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError> {
        // Remove keys [0, offset_inclusive].
        let from = 0u64.to_be_bytes();
        let to = offset_inclusive.saturating_add(1).to_be_bytes();
        let mut keys_to_delete = Vec::new();
        for item in self.tree.range(from..to) {
            let (k, _) = item.map_err(|e| err_other(e.to_string()))?;
            keys_to_delete.push(k.to_vec());
        }
        for key in keys_to_delete {
            self.tree
                .remove(&key)
                .map_err(|e| err_other(e.to_string()))?;
        }
        Ok(())
    }
}

/// Checkpoint store keyed by checkpoint ID in the `checkpoints` tree.
pub struct SledCheckpointStore {
    tree: Tree,
}

#[async_trait]
impl CheckpointStore for SledCheckpointStore {
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        let snapshot = CheckpointSnapshot::new(manifest, operator_state_blobs);
        let key = snapshot.manifest.id.to_be_bytes();
        let value = serde_json::to_vec(&snapshot).map_err(|e| err_codec(e.to_string()))?;
        self.tree
            .insert(key, value)
            .map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }

    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError> {
        match self.tree.last().map_err(|e| err_other(e.to_string()))? {
            Some((_k, v)) => {
                let snap: CheckpointSnapshot =
                    serde_json::from_slice(&v).map_err(|e| err_codec(e.to_string()))?;
                Ok(Some(snap))
            }
            None => Ok(None),
        }
    }

    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError> {
        let from = 0u64.to_be_bytes();
        let to = checkpoint_id.to_be_bytes();
        let mut keys_to_delete = Vec::new();
        for item in self.tree.range(from..to) {
            let (k, _) = item.map_err(|e| err_other(e.to_string()))?;
            keys_to_delete.push(k.to_vec());
        }
        for key in keys_to_delete {
            self.tree
                .remove(&key)
                .map_err(|e| err_other(e.to_string()))?;
        }
        Ok(())
    }
}

/// Factory exposing the sled backend through the SPI.
///
/// Note: the `backend_id` is `"sled"`, but conceptually this fills the
/// "embedded KV-store" slot that Java's `cqels-storage-rocksdb` /
/// `cqels-storage-lmdb` modules occupy.
pub struct SledStorageProvider;

impl StorageBackendProvider for SledStorageProvider {
    fn backend_id(&self) -> &str {
        "sled"
    }

    fn create(&self, config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError> {
        Ok(Box::new(SledPersistentBackend::from_config(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::StreamEnvelope;
    use tempfile::TempDir;

    fn temp_backend() -> (SledPersistentBackend, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let backend = SledPersistentBackend::open(dir.path()).expect("open");
        (backend, dir)
    }

    fn envelope(payload: &str) -> StreamEnvelope {
        StreamEnvelope::new(0, "test", 0, "text/plain", payload.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn append_and_read_round_trip() {
        let (backend, _dir) = temp_backend();
        let off1 = backend
            .event_journal()
            .append(envelope("alpha"))
            .await
            .expect("append1");
        let off2 = backend
            .event_journal()
            .append(envelope("beta"))
            .await
            .expect("append2");
        assert!(off2 > off1);
        let events = backend.event_journal().read_from(0).await.expect("read");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, off1);
        assert_eq!(events[0].payload, b"alpha");
        assert_eq!(events[1].offset, off2);
    }

    #[tokio::test]
    async fn read_from_skips_earlier_offsets() {
        let (backend, _dir) = temp_backend();
        let off1 = backend
            .event_journal()
            .append(envelope("alpha"))
            .await
            .unwrap();
        backend
            .event_journal()
            .append(envelope("beta"))
            .await
            .unwrap();
        let events = backend.event_journal().read_from(off1).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"beta");
    }

    #[tokio::test]
    async fn truncate_before_removes_old_entries() {
        let (backend, _dir) = temp_backend();
        backend.event_journal().append(envelope("a")).await.unwrap();
        let off2 = backend.event_journal().append(envelope("b")).await.unwrap();
        backend.event_journal().append(envelope("c")).await.unwrap();
        backend.event_journal().truncate_before(off2).await.unwrap();
        let events = backend.event_journal().read_from(0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"c");
    }

    #[tokio::test]
    async fn checkpoint_write_and_latest() {
        let (backend, _dir) = temp_backend();
        let manifest = CheckpointManifest::new(1, 100, 5);
        backend
            .checkpoint_store()
            .write_checkpoint(manifest, std::collections::HashMap::new())
            .await
            .expect("write checkpoint");
        let latest = backend
            .checkpoint_store()
            .latest()
            .await
            .expect("latest")
            .expect("some checkpoint");
        assert_eq!(latest.manifest.id, 1);
        assert_eq!(latest.manifest.event_offset, 5);
    }

    #[tokio::test]
    async fn latest_returns_highest_id() {
        let (backend, _dir) = temp_backend();
        for id in [3u64, 1, 5, 2] {
            backend
                .checkpoint_store()
                .write_checkpoint(
                    CheckpointManifest::new(id, id as i64, id),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
        }
        let latest = backend.checkpoint_store().latest().await.unwrap().unwrap();
        assert_eq!(latest.manifest.id, 5);
    }

    #[tokio::test]
    async fn delete_older_than_removes_below() {
        let (backend, _dir) = temp_backend();
        for id in [1u64, 2, 3, 4, 5] {
            backend
                .checkpoint_store()
                .write_checkpoint(
                    CheckpointManifest::new(id, id as i64, id),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
        }
        backend
            .checkpoint_store()
            .delete_older_than(3)
            .await
            .unwrap();
        let latest = backend.checkpoint_store().latest().await.unwrap().unwrap();
        assert_eq!(latest.manifest.id, 5);
    }

    #[tokio::test]
    async fn provider_creates_backend_from_config() {
        let dir = TempDir::new().unwrap();
        let provider = SledStorageProvider;
        assert_eq!(provider.backend_id(), "sled");
        let config = BackendConfig::builder()
            .property("path", dir.path().to_string_lossy())
            .build();
        let _backend = provider.create(config).expect("create");
    }

    #[tokio::test]
    async fn next_offset_recovers_across_reopens() {
        let dir = TempDir::new().unwrap();
        {
            let backend = SledPersistentBackend::open(dir.path()).unwrap();
            backend.event_journal().append(envelope("a")).await.unwrap();
            let off2 = backend.event_journal().append(envelope("b")).await.unwrap();
            backend.close().await.unwrap();
            assert_eq!(off2, 2);
        }
        // sled holds an exclusive lock on the directory until Db is dropped.
        // Wait briefly for filesystem cleanup before reopening.
        let backend = SledPersistentBackend::open(dir.path()).unwrap();
        let off = backend.event_journal().append(envelope("c")).await.unwrap();
        assert!(off >= 3, "next offset should resume past 2, got {off}");
    }
}
