//! RocksDB-backed implementation of the [`cqels_storage_spi`] traits.
//!
//! Mirrors Java's `cqels-storage-rocksdb` module in shape and semantics.
//! Closes the long-standing gap noted in `JAVA_PARITY_PLAN.md` ("no
//! RocksDB because of the `links = \"rocksdb\"` clash with oxigraph's
//! `oxrocksdb-sys`"): we now disable oxigraph's `rocksdb` default
//! feature at the workspace level — we only use `Store::new()`
//! (in-memory), never `Store::open(path)` — which frees the `links`
//! slot for this crate's `rocksdb` dependency.
//!
//! Storage layout (column families):
//! - **`journal`** — event journal. Keys are 8-byte big-endian
//!   offsets; values are JSON-encoded `StreamEnvelope`s.
//! - **`checkpoints`** — keys are 8-byte big-endian checkpoint IDs;
//!   values are JSON-encoded `CheckpointSnapshot`s.
//! - **`meta`** — single key (`b"next_offset"`) tracking the
//!   recoverable next-offset counter so the journal monotonically
//!   advances across reopens.
//!
//! All operations are issued through column-family handles. RocksDB
//! ordered-key iteration makes range and "latest" queries cheap.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, CheckpointSnapshot, CheckpointStore, EventJournal,
    PersistentBackend, StorageBackendProvider, StorageError, StreamEnvelope,
};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, IteratorMode, Options, DB};

const CF_JOURNAL: &str = "journal";
const CF_CHECKPOINTS: &str = "checkpoints";
const CF_META: &str = "meta";
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

/// RocksDB-backed persistent backend.
pub struct RocksDbPersistentBackend {
    db: Arc<DB>,
    next_offset: AtomicU64,
    meta_lock: Mutex<()>,
}

impl RocksDbPersistentBackend {
    /// Opens (or creates) a RocksDB database at `path`. Column families
    /// `journal`, `checkpoints`, and `meta` are created on first open.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = [CF_JOURNAL, CF_CHECKPOINTS, CF_META];
        let db = DB::open_cf(&opts, &path, cfs).map_err(|e| err_other(e.to_string()))?;
        let db = Arc::new(db);

        let next_offset = recover_next_offset(&db)?;

        Ok(Self {
            db,
            next_offset: AtomicU64::new(next_offset),
            meta_lock: Mutex::new(()),
        })
    }

    /// Path-based open via the SPI's [`BackendConfig`]. The path is
    /// read from the `path` property.
    pub fn from_config(config: BackendConfig) -> Result<Self, StorageError> {
        let path = config
            .properties
            .get("path")
            .ok_or_else(|| err_other("rocksdb backend requires `path` property"))?
            .clone();
        Self::open(path)
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily, StorageError> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| err_other(format!("missing column family `{name}`")))
    }
}

fn recover_next_offset(db: &DB) -> Result<u64, StorageError> {
    let cf = db
        .cf_handle(CF_META)
        .ok_or_else(|| err_other("missing column family `meta`"))?;
    match db
        .get_cf(cf, META_KEY_NEXT_OFFSET)
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
impl PersistentBackend for RocksDbPersistentBackend {
    fn event_journal(&self) -> &dyn EventJournal {
        self
    }

    fn checkpoint_store(&self) -> &dyn CheckpointStore {
        self
    }

    async fn close(&self) -> Result<(), StorageError> {
        // RocksDB doesn't expose a `flush()` that's universally
        // available on all CFs at once; we issue a default-CF flush
        // which forces a WAL sync.
        self.db.flush().map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl EventJournal for RocksDbPersistentBackend {
    async fn append(&self, mut event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        event.offset = offset;
        let key = offset.to_be_bytes();
        let value = serde_json::to_vec(&event).map_err(|e| err_codec(e.to_string()))?;

        let journal_cf = self.cf(CF_JOURNAL)?;
        self.db
            .put_cf(journal_cf, key, value)
            .map_err(|e| err_other(e.to_string()))?;

        let _guard = self.meta_lock.lock();
        let next = (offset + 1).to_be_bytes();
        let meta_cf = self.cf(CF_META)?;
        self.db
            .put_cf(meta_cf, META_KEY_NEXT_OFFSET, next)
            .map_err(|e| err_other(e.to_string()))?;

        Ok(offset)
    }

    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError> {
        let start_key = offset_exclusive.saturating_add(1).to_be_bytes();
        let journal_cf = self.cf(CF_JOURNAL)?;
        let mode = IteratorMode::From(&start_key, rocksdb::Direction::Forward);
        let iter = self.db.iterator_cf(journal_cf, mode);

        let mut events = Vec::new();
        for item in iter {
            let (_k, v) = item.map_err(|e| err_other(e.to_string()))?;
            let env: StreamEnvelope =
                serde_json::from_slice(&v).map_err(|e| err_codec(e.to_string()))?;
            events.push(env);
        }
        Ok(events)
    }

    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError> {
        // RocksDB exposes `delete_range_cf(start, end)` — a half-open
        // range. We want to remove keys in `[0, offset_inclusive]`,
        // i.e. `[0, offset_inclusive + 1)`.
        let journal_cf = self.cf(CF_JOURNAL)?;
        let from = 0u64.to_be_bytes();
        let to = offset_inclusive.saturating_add(1).to_be_bytes();
        self.db
            .delete_range_cf(journal_cf, from, to)
            .map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for RocksDbPersistentBackend {
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        let snapshot = CheckpointSnapshot::new(manifest, operator_state_blobs);
        let key = snapshot.manifest.id.to_be_bytes();
        let value = serde_json::to_vec(&snapshot).map_err(|e| err_codec(e.to_string()))?;
        let cp_cf = self.cf(CF_CHECKPOINTS)?;
        self.db
            .put_cf(cp_cf, key, value)
            .map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }

    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError> {
        let cp_cf = self.cf(CF_CHECKPOINTS)?;
        let mut iter = self.db.iterator_cf(cp_cf, IteratorMode::End);
        match iter.next() {
            Some(Ok((_k, v))) => {
                let snap: CheckpointSnapshot =
                    serde_json::from_slice(&v).map_err(|e| err_codec(e.to_string()))?;
                Ok(Some(snap))
            }
            Some(Err(e)) => Err(err_other(e.to_string())),
            None => Ok(None),
        }
    }

    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError> {
        // Delete `[0, checkpoint_id)`. `delete_range_cf` is half-open
        // exactly like the sled implementation's manual range delete.
        let cp_cf = self.cf(CF_CHECKPOINTS)?;
        let from = 0u64.to_be_bytes();
        let to = checkpoint_id.to_be_bytes();
        self.db
            .delete_range_cf(cp_cf, from, to)
            .map_err(|e| err_other(e.to_string()))?;
        Ok(())
    }
}

/// Factory exposing the RocksDB backend through the SPI.
///
/// Selectable as `backend_id = "rocksdb"`. Drop this provider into a
/// `BinaryCodecRegistry`-style provider map alongside the file, sled,
/// and LMDB providers; the engine picks one based on its persistence
/// configuration.
pub struct RocksDbStorageProvider;

impl StorageBackendProvider for RocksDbStorageProvider {
    fn backend_id(&self) -> &str {
        "rocksdb"
    }

    fn create(&self, config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError> {
        Ok(Box::new(RocksDbPersistentBackend::from_config(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::StreamEnvelope;
    use tempfile::TempDir;

    fn temp_backend() -> (RocksDbPersistentBackend, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let backend = RocksDbPersistentBackend::open(dir.path()).expect("open");
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
        let provider = RocksDbStorageProvider;
        assert_eq!(provider.backend_id(), "rocksdb");
        let config = BackendConfig::builder()
            .property("path", dir.path().to_string_lossy())
            .build();
        let _backend = provider.create(config).expect("create");
    }

    #[tokio::test]
    async fn next_offset_recovers_across_reopens() {
        let dir = TempDir::new().unwrap();
        {
            let backend = RocksDbPersistentBackend::open(dir.path()).unwrap();
            backend.event_journal().append(envelope("a")).await.unwrap();
            let off2 = backend.event_journal().append(envelope("b")).await.unwrap();
            backend.close().await.unwrap();
            assert_eq!(off2, 2);
        }
        // RocksDB releases the directory lock when the DB handle drops.
        let backend = RocksDbPersistentBackend::open(dir.path()).unwrap();
        let off = backend.event_journal().append(envelope("c")).await.unwrap();
        assert!(off >= 3, "next offset should resume past 2, got {off}");
    }
}
