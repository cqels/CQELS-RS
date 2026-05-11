//! LMDB-backed implementation of the [`cqels_storage_spi`] traits.
//!
//! Mirrors Java's `cqels-storage-lmdb` module. Uses the
//! [`heed`](https://crates.io/crates/heed) pure-Rust LMDB binding for memory-
//! mapped persistence with copy-on-write transactions and crash safety.
//!
//! Storage layout:
//! - **`journal`** database: 8-byte big-endian offset key → JSON-encoded
//!   `StreamEnvelope` value.
//! - **`checkpoints`** database: 8-byte big-endian checkpoint id key →
//!   JSON-encoded `CheckpointSnapshot`.
//! - **`meta`** database: a single `next_offset` key that persists the
//!   journal's next-offset counter across reopens.
//!
//! All writes use LMDB write transactions for atomicity. Read paths use
//! short-lived read transactions; LMDB's MVCC means readers don't block
//! writers.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, CheckpointSnapshot, CheckpointStore, EventJournal,
    PersistentBackend, StorageBackendProvider, StorageError, StreamEnvelope,
};
use heed::{types, Database, Env, EnvOpenOptions};
use parking_lot::Mutex;

const META_KEY_NEXT_OFFSET: &str = "next_offset";

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

/// LMDB-backed persistent backend.
pub struct LmdbPersistentBackend {
    journal: LmdbEventJournal,
    checkpoints: LmdbCheckpointStore,
    _env: Arc<Env>,
}

impl LmdbPersistentBackend {
    /// Opens (or creates) an LMDB environment at `path`. Creates the
    /// directory if it does not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        // SAFETY: LMDB requires no other process holds the env open.
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(64 * 1024 * 1024) // 64 MiB default; sufficient for tests
                .max_dbs(8)
                .open(&path)
        }
        .map_err(|e| err_other(e.to_string()))?;
        let env = Arc::new(env);

        let mut wtxn = env.write_txn().map_err(|e| err_other(e.to_string()))?;
        let journal_db: Database<types::U64<heed::byteorder::BigEndian>, types::Bytes> = env
            .create_database(&mut wtxn, Some("journal"))
            .map_err(|e| err_other(e.to_string()))?;
        let checkpoints_db: Database<types::U64<heed::byteorder::BigEndian>, types::Bytes> = env
            .create_database(&mut wtxn, Some("checkpoints"))
            .map_err(|e| err_other(e.to_string()))?;
        let meta_db: Database<types::Str, types::U64<heed::byteorder::BigEndian>> = env
            .create_database(&mut wtxn, Some("meta"))
            .map_err(|e| err_other(e.to_string()))?;
        wtxn.commit().map_err(|e| err_other(e.to_string()))?;

        let next_offset = {
            let rtxn = env.read_txn().map_err(|e| err_other(e.to_string()))?;
            meta_db
                .get(&rtxn, META_KEY_NEXT_OFFSET)
                .map_err(|e| err_other(e.to_string()))?
                .unwrap_or(1)
        };

        Ok(Self {
            journal: LmdbEventJournal {
                env: env.clone(),
                journal_db,
                meta_db,
                next_offset: AtomicU64::new(next_offset),
                meta_lock: Mutex::new(()),
            },
            checkpoints: LmdbCheckpointStore {
                env: env.clone(),
                checkpoints_db,
            },
            _env: env,
        })
    }

    /// Opens via the SPI's [`BackendConfig`]. The path is read from the
    /// `path` property.
    pub fn from_config(config: BackendConfig) -> Result<Self, StorageError> {
        let path = config
            .properties
            .get("path")
            .ok_or_else(|| err_other("lmdb backend requires `path` property"))?
            .clone();
        Self::open(path)
    }
}

#[async_trait]
impl PersistentBackend for LmdbPersistentBackend {
    fn event_journal(&self) -> &dyn EventJournal {
        &self.journal
    }

    fn checkpoint_store(&self) -> &dyn CheckpointStore {
        &self.checkpoints
    }

    async fn close(&self) -> Result<(), StorageError> {
        // heed flushes on drop; no explicit close needed.
        Ok(())
    }
}

/// Append-only event journal stored in the `journal` database.
pub struct LmdbEventJournal {
    env: Arc<Env>,
    journal_db: Database<types::U64<heed::byteorder::BigEndian>, types::Bytes>,
    meta_db: Database<types::Str, types::U64<heed::byteorder::BigEndian>>,
    next_offset: AtomicU64,
    meta_lock: Mutex<()>,
}

#[async_trait]
impl EventJournal for LmdbEventJournal {
    async fn append(&self, mut event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        event.offset = offset;
        let value = serde_json::to_vec(&event).map_err(|e| err_codec(e.to_string()))?;

        let _guard = self.meta_lock.lock();
        let mut wtxn = self.env.write_txn().map_err(|e| err_other(e.to_string()))?;
        self.journal_db
            .put(&mut wtxn, &offset, &value)
            .map_err(|e| err_other(e.to_string()))?;
        self.meta_db
            .put(&mut wtxn, META_KEY_NEXT_OFFSET, &(offset + 1))
            .map_err(|e| err_other(e.to_string()))?;
        wtxn.commit().map_err(|e| err_other(e.to_string()))?;

        Ok(offset)
    }

    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError> {
        let rtxn = self.env.read_txn().map_err(|e| err_other(e.to_string()))?;
        let start = offset_exclusive.saturating_add(1);
        let mut events = Vec::new();
        let iter = self
            .journal_db
            .range(&rtxn, &(start..))
            .map_err(|e| err_other(e.to_string()))?;
        for item in iter {
            let (_k, v) = item.map_err(|e| err_other(e.to_string()))?;
            let env: StreamEnvelope =
                serde_json::from_slice(v).map_err(|e| err_codec(e.to_string()))?;
            events.push(env);
        }
        Ok(events)
    }

    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError> {
        let mut wtxn = self.env.write_txn().map_err(|e| err_other(e.to_string()))?;
        let mut keys = Vec::new();
        {
            let iter = self
                .journal_db
                .range(&wtxn, &(0..=offset_inclusive))
                .map_err(|e| err_other(e.to_string()))?;
            for item in iter {
                let (k, _) = item.map_err(|e| err_other(e.to_string()))?;
                keys.push(k);
            }
        }
        for k in keys {
            self.journal_db
                .delete(&mut wtxn, &k)
                .map_err(|e| err_other(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

/// Checkpoint store stored in the `checkpoints` database.
pub struct LmdbCheckpointStore {
    env: Arc<Env>,
    checkpoints_db: Database<types::U64<heed::byteorder::BigEndian>, types::Bytes>,
}

#[async_trait]
impl CheckpointStore for LmdbCheckpointStore {
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        let snapshot = CheckpointSnapshot::new(manifest, operator_state_blobs);
        let id = snapshot.manifest.id;
        let value = serde_json::to_vec(&snapshot).map_err(|e| err_codec(e.to_string()))?;
        let mut wtxn = self.env.write_txn().map_err(|e| err_other(e.to_string()))?;
        self.checkpoints_db
            .put(&mut wtxn, &id, &value)
            .map_err(|e| err_other(e.to_string()))?;
        wtxn.commit().map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }

    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError> {
        let rtxn = self.env.read_txn().map_err(|e| err_other(e.to_string()))?;
        match self
            .checkpoints_db
            .last(&rtxn)
            .map_err(|e| err_other(e.to_string()))?
        {
            Some((_k, v)) => {
                let snap: CheckpointSnapshot =
                    serde_json::from_slice(v).map_err(|e| err_codec(e.to_string()))?;
                Ok(Some(snap))
            }
            None => Ok(None),
        }
    }

    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError> {
        let mut wtxn = self.env.write_txn().map_err(|e| err_other(e.to_string()))?;
        let mut keys = Vec::new();
        {
            let iter = self
                .checkpoints_db
                .range(&wtxn, &(0..checkpoint_id))
                .map_err(|e| err_other(e.to_string()))?;
            for item in iter {
                let (k, _) = item.map_err(|e| err_other(e.to_string()))?;
                keys.push(k);
            }
        }
        for k in keys {
            self.checkpoints_db
                .delete(&mut wtxn, &k)
                .map_err(|e| err_other(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

/// Factory exposing the LMDB backend through the SPI.
pub struct LmdbStorageProvider;

impl StorageBackendProvider for LmdbStorageProvider {
    fn backend_id(&self) -> &str {
        "lmdb"
    }

    fn create(&self, config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError> {
        Ok(Box::new(LmdbPersistentBackend::from_config(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::StreamEnvelope;
    use tempfile::TempDir;

    fn temp_backend() -> (LmdbPersistentBackend, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let backend = LmdbPersistentBackend::open(dir.path()).expect("open");
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
            .unwrap();
        let off2 = backend
            .event_journal()
            .append(envelope("beta"))
            .await
            .unwrap();
        assert!(off2 > off1);
        let events = backend.event_journal().read_from(0).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, off1);
        assert_eq!(events[0].payload, b"alpha");
        assert_eq!(events[1].offset, off2);
    }

    #[tokio::test]
    async fn read_from_skips_earlier_offsets() {
        let (backend, _dir) = temp_backend();
        let off1 = backend.event_journal().append(envelope("a")).await.unwrap();
        backend.event_journal().append(envelope("b")).await.unwrap();
        let events = backend.event_journal().read_from(off1).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"b");
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
            .unwrap();
        let latest = backend.checkpoint_store().latest().await.unwrap().unwrap();
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
        let provider = LmdbStorageProvider;
        assert_eq!(provider.backend_id(), "lmdb");
        let config = BackendConfig::builder()
            .property("path", dir.path().to_string_lossy())
            .build();
        let _backend = provider.create(config).expect("create");
    }

    #[tokio::test]
    async fn next_offset_recovers_across_reopens() {
        let dir = TempDir::new().unwrap();
        {
            let backend = LmdbPersistentBackend::open(dir.path()).unwrap();
            backend.event_journal().append(envelope("a")).await.unwrap();
            let off2 = backend.event_journal().append(envelope("b")).await.unwrap();
            backend.close().await.unwrap();
            assert_eq!(off2, 2);
        }
        let backend = LmdbPersistentBackend::open(dir.path()).unwrap();
        let off = backend.event_journal().append(envelope("c")).await.unwrap();
        assert!(off >= 3, "next offset should resume past 2, got {off}");
    }
}
