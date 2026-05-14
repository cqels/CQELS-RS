//! Apache IoTDB-backed persistent storage adapter for CQELS.
//!
//! Mirrors Java's `cqels-storage-iotdb` module. IoTDB is a time-series
//! database — keys are monotonically increasing `i64` timestamps and
//! rows live on per-device "time-series" paths like
//! `root.<group>.<device>.<measurement>`. This crate maps the
//! [`cqels_storage_spi`] traits (`EventJournal`, `CheckpointStore`)
//! onto two such time-series, using the offset / checkpoint-id as the
//! IoTDB `time` field and the row payload as a binary measurement.
//!
//! ## Three executor options
//!
//! IoTDB's wire protocol is Thrift-based with a wide surface
//! (`Session` open/close, storage-group creation, schemas, batched
//! inserts, range deletes, query plans). To keep the dep graph
//! light by default while still shipping a real client, the crate
//! is split:
//!
//! - **[`IotDbExecutor`]** — a narrow async trait (`put`, `read_from`,
//!   `latest`, `delete_range`, `close`) that captures *exactly* what
//!   the SPI needs. Downstream code wires this to a real IoTDB client
//!   in ~50 lines, or implements it against whatever Thrift / native
//!   client they prefer.
//! - **[`InMemoryIotDbExecutor`]** — a fully-functional reference
//!   implementation backed by per-device `BTreeMap<u64, Vec<u8>>`,
//!   used as the default backend, in tests, and as a development
//!   sandbox. **Always available — no feature flag needed.**
//! - **`ThriftIotDbExecutor`** — opt-in real IoTDB session client
//!   built on top of `iotdb-client-rs`. Enable with the `thrift`
//!   Cargo feature. The module is gated on that feature; see its
//!   inline docs for live-test gating via `IOTDB_HOST`.
//! - **[`IotDbPersistentBackend`]** — implements
//!   `cqels_storage_spi::PersistentBackend` on top of any
//!   `Arc<dyn IotDbExecutor>`.
//!
//! ## Storage layout
//!
//! - **Journal**: device path = `journal_device` (default
//!   `"root.cqels.journal"`); each `StreamEnvelope` is JSON-encoded
//!   into the row payload at `time = offset`.
//! - **Checkpoints**: device path = `checkpoint_device` (default
//!   `"root.cqels.checkpoints"`); each `CheckpointSnapshot` is
//!   JSON-encoded at `time = checkpoint_id`.
//! - **Meta**: a single row in the journal device at `time = 0`
//!   carrying the next-offset counter for resumability across reopens.
//!   (Offset 0 is reserved; real offsets start at 1.)
//!
//! ## Wiring a real IoTDB client
//!
//! Provide an `IotDbExecutor` impl wrapping a `Session`-style client:
//!
//! ```ignore
//! struct ThriftExecutor { session: Mutex<RpcSession> }
//!
//! #[async_trait::async_trait]
//! impl IotDbExecutor for ThriftExecutor {
//!     async fn put(&self, device: &str, key: u64, data: Vec<u8>) -> Result<(), IotDbError> {
//!         let mut s = self.session.lock();
//!         s.insert_record(device, vec!["payload"], vec![Value::Binary(data)], key as i64, None)
//!             .map_err(|e| IotDbError::Backend(e.to_string()))
//!     }
//!     // ... other 4 methods similarly
//! }
//! ```
//!
//! Then construct an `IotDbPersistentBackend` against your executor
//! and register it as a `StorageBackendProvider`.

pub mod executor;
pub mod in_memory;
#[cfg(feature = "thrift")]
pub mod thrift_executor;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, CheckpointSnapshot, CheckpointStore, EventJournal,
    PersistentBackend, StorageBackendProvider, StorageError, StreamEnvelope,
};
use tokio::sync::Mutex;

pub use executor::{IotDbError, IotDbExecutor};
pub use in_memory::InMemoryIotDbExecutor;
#[cfg(feature = "thrift")]
pub use thrift_executor::{ThriftConnectConfig, ThriftIotDbExecutor};

/// Default device path for the event journal.
pub const DEFAULT_JOURNAL_DEVICE: &str = "root.cqels.journal";
/// Default device path for checkpoint snapshots.
pub const DEFAULT_CHECKPOINT_DEVICE: &str = "root.cqels.checkpoints";
/// Reserved time-row carrying the next-offset counter in the journal
/// device. Real envelope offsets start at 1.
const META_KEY_NEXT_OFFSET: u64 = 0;
const FIRST_OFFSET: u64 = 1;

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

fn map_iotdb_err(e: IotDbError) -> StorageError {
    err_other(e.to_string())
}

/// Persistent backend mapping the storage SPI onto an [`IotDbExecutor`].
///
/// Cheap to clone — internally an `Arc` over the executor + counters.
pub struct IotDbPersistentBackend {
    inner: Arc<Inner>,
}

struct Inner {
    executor: Arc<dyn IotDbExecutor>,
    journal_device: String,
    checkpoint_device: String,
    next_offset: AtomicU64,
    meta_lock: Mutex<()>,
}

impl IotDbPersistentBackend {
    /// Constructs a backend on top of the supplied executor with the
    /// default journal / checkpoint device paths.
    pub fn with_executor(executor: Arc<dyn IotDbExecutor>) -> Result<Self, StorageError> {
        Self::with_paths(
            executor,
            DEFAULT_JOURNAL_DEVICE.to_string(),
            DEFAULT_CHECKPOINT_DEVICE.to_string(),
        )
    }

    /// Constructs a backend with custom device paths for the journal
    /// and checkpoint time-series.
    pub fn with_paths(
        executor: Arc<dyn IotDbExecutor>,
        journal_device: String,
        checkpoint_device: String,
    ) -> Result<Self, StorageError> {
        let next_offset = recover_next_offset(executor.as_ref(), &journal_device)?;
        Ok(Self {
            inner: Arc::new(Inner {
                executor,
                journal_device,
                checkpoint_device,
                next_offset: AtomicU64::new(next_offset),
                meta_lock: Mutex::new(()),
            }),
        })
    }

    /// Convenience constructor backed by the in-memory reference
    /// executor. Used by [`IotDbStorageProvider`] when no real client
    /// is wired.
    pub fn in_memory() -> Result<Self, StorageError> {
        Self::with_executor(Arc::new(InMemoryIotDbExecutor::new()))
    }
}

fn recover_next_offset(executor: &dyn IotDbExecutor, device: &str) -> Result<u64, StorageError> {
    // Run the executor synchronously on a temporary current-thread
    // runtime; the recovery is a one-shot read at construction time so
    // a dedicated runtime isn't worth it.
    let result = pollster_block_on(async { executor.latest(device).await.map_err(map_iotdb_err) })?;
    match result {
        Some((META_KEY_NEXT_OFFSET, bytes)) if bytes.len() == 8 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            Ok(u64::from_be_bytes(buf))
        }
        Some((key, _)) if key == META_KEY_NEXT_OFFSET => Ok(FIRST_OFFSET),
        Some((highest, _)) => {
            // No meta row written yet (legacy data): resume past the
            // highest real offset.
            Ok(highest.saturating_add(1).max(FIRST_OFFSET))
        }
        None => Ok(FIRST_OFFSET),
    }
}

/// Tiny synchronous block-on for the constructor — avoids an
/// `Arc<tokio::Runtime>` field just for one-shot recovery.
fn pollster_block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;

    struct ThreadWaker(thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: StdArc<Self>) {
            self.0.unpark();
        }
    }

    let waker: Waker = StdArc::new(ThreadWaker(thread::current())).into();
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => thread::park(),
        }
    }
}

#[async_trait]
impl PersistentBackend for IotDbPersistentBackend {
    fn event_journal(&self) -> &dyn EventJournal {
        self
    }

    fn checkpoint_store(&self) -> &dyn CheckpointStore {
        self
    }

    async fn close(&self) -> Result<(), StorageError> {
        self.inner.executor.close().await.map_err(map_iotdb_err)
    }
}

#[async_trait]
impl EventJournal for IotDbPersistentBackend {
    async fn append(&self, mut event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.inner.next_offset.fetch_add(1, Ordering::SeqCst);
        event.offset = offset;
        let payload = serde_json::to_vec(&event).map_err(|e| err_codec(e.to_string()))?;

        self.inner
            .executor
            .put(&self.inner.journal_device, offset, payload)
            .await
            .map_err(map_iotdb_err)?;

        // The meta-row write is serialized so concurrent appends can't
        // leave the persisted "next offset" lagging behind the true
        // monotonic counter. A `tokio::Mutex` (not parking_lot) is
        // necessary because the guard is held across an `.await`.
        let guard = self.inner.meta_lock.lock().await;
        let next = (offset + 1).to_be_bytes().to_vec();
        self.inner
            .executor
            .put(&self.inner.journal_device, META_KEY_NEXT_OFFSET, next)
            .await
            .map_err(map_iotdb_err)?;
        drop(guard);

        Ok(offset)
    }

    async fn read_from(&self, offset_exclusive: u64) -> Result<Vec<StreamEnvelope>, StorageError> {
        // The SPI returns envelopes with `offset > offset_exclusive`,
        // but the executor takes a *closed* lower bound. Bump by one
        // (saturating to guard against u64 overflow) and floor at
        // FIRST_OFFSET so the meta row at key 0 is always skipped.
        let start_inclusive = offset_exclusive.saturating_add(1).max(FIRST_OFFSET);
        let rows = self
            .inner
            .executor
            .read_from(&self.inner.journal_device, start_inclusive)
            .await
            .map_err(map_iotdb_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, bytes) in rows {
            if key == META_KEY_NEXT_OFFSET {
                // Defensive: an executor that's lenient about its
                // lower bound shouldn't be able to leak the meta row.
                continue;
            }
            let env: StreamEnvelope =
                serde_json::from_slice(&bytes).map_err(|e| err_codec(e.to_string()))?;
            out.push(env);
        }
        Ok(out)
    }

    async fn truncate_before(&self, offset_inclusive: u64) -> Result<(), StorageError> {
        // Delete envelope rows in `[1, offset_inclusive + 1)`. Keep
        // the meta row at key 0 untouched.
        if offset_inclusive < FIRST_OFFSET {
            return Ok(());
        }
        let end_exclusive = offset_inclusive.saturating_add(1);
        self.inner
            .executor
            .delete_range(&self.inner.journal_device, FIRST_OFFSET, end_exclusive)
            .await
            .map_err(map_iotdb_err)?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for IotDbPersistentBackend {
    async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        let snapshot = CheckpointSnapshot::new(manifest, operator_state_blobs);
        let key = snapshot.manifest.id;
        let payload = serde_json::to_vec(&snapshot).map_err(|e| err_codec(e.to_string()))?;
        self.inner
            .executor
            .put(&self.inner.checkpoint_device, key, payload)
            .await
            .map_err(map_iotdb_err)?;
        Ok(())
    }

    async fn latest(&self) -> Result<Option<CheckpointSnapshot>, StorageError> {
        match self
            .inner
            .executor
            .latest(&self.inner.checkpoint_device)
            .await
            .map_err(map_iotdb_err)?
        {
            Some((_key, bytes)) => {
                let snap: CheckpointSnapshot =
                    serde_json::from_slice(&bytes).map_err(|e| err_codec(e.to_string()))?;
                Ok(Some(snap))
            }
            None => Ok(None),
        }
    }

    async fn delete_older_than(&self, checkpoint_id: u64) -> Result<(), StorageError> {
        self.inner
            .executor
            .delete_range(&self.inner.checkpoint_device, 0, checkpoint_id)
            .await
            .map_err(map_iotdb_err)?;
        Ok(())
    }
}

/// Factory exposing the IoTDB backend through the SPI.
///
/// With no custom configuration this provider wires the in-memory
/// reference executor — useful for tests and dev. Production
/// deployments construct an `IotDbPersistentBackend` directly with a
/// real-IoTDB-backed [`IotDbExecutor`] impl and register the resulting
/// `Box<dyn PersistentBackend>` themselves; the provider trait is too
/// narrow to accept a custom executor through `BackendConfig`.
pub struct IotDbStorageProvider;

impl StorageBackendProvider for IotDbStorageProvider {
    fn backend_id(&self) -> &str {
        "iotdb"
    }

    fn create(&self, _config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError> {
        Ok(Box::new(IotDbPersistentBackend::in_memory()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::StreamEnvelope;

    fn fresh() -> IotDbPersistentBackend {
        IotDbPersistentBackend::in_memory().expect("in_memory")
    }

    fn envelope(payload: &str) -> StreamEnvelope {
        StreamEnvelope::new(0, "test", 0, "text/plain", payload.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn append_and_read_round_trip() {
        let backend = fresh();
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
        let backend = fresh();
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
        let backend = fresh();
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
        let backend = fresh();
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
        let backend = fresh();
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
        let backend = fresh();
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
    async fn provider_creates_in_memory_backend_by_default() {
        let provider = IotDbStorageProvider;
        assert_eq!(provider.backend_id(), "iotdb");
        let config = BackendConfig::new();
        let _backend = provider.create(config).expect("create");
    }

    #[tokio::test]
    async fn next_offset_recovers_via_meta_row() {
        // Build a backend, append two events, capture next_offset.
        let executor = Arc::new(InMemoryIotDbExecutor::new()) as Arc<dyn IotDbExecutor>;
        {
            let backend = IotDbPersistentBackend::with_executor(executor.clone()).unwrap();
            backend.event_journal().append(envelope("a")).await.unwrap();
            let off2 = backend.event_journal().append(envelope("b")).await.unwrap();
            assert_eq!(off2, 2);
        }
        // Reconstruct over the SAME executor — meta row should be
        // picked up and the next offset resumes.
        let backend = IotDbPersistentBackend::with_executor(executor).unwrap();
        let off = backend.event_journal().append(envelope("c")).await.unwrap();
        assert!(off >= 3, "next offset should resume past 2, got {off}");
    }
}
