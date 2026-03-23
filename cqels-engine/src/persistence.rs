//! Persistence coordinator for engine state checkpointing and recovery.
//!
//! Manages primary and mirror backends, checkpoint policies, and recovery
//! from persistent storage.

use std::collections::HashMap;

use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, CheckpointSnapshot, PersistentBackend,
    StorageBackendProvider, StorageError, StreamEnvelope,
};

/// Configuration for engine persistence.
#[derive(Clone, Debug, Default)]
pub struct EnginePersistenceConfig {
    /// Whether persistence is enabled.
    pub enabled: bool,
    /// ID of the primary backend.
    pub primary_backend_id: String,
    /// IDs of mirror backends.
    pub mirror_backend_ids: Vec<String>,
    /// Backend configurations keyed by backend ID.
    pub backends: HashMap<String, BackendConfig>,
    /// Checkpoint policy.
    pub checkpoint_policy: CheckpointPolicy,
    /// Recovery policy.
    pub recovery_policy: RecoveryPolicy,
}

/// Policy controlling when checkpoints are created and retained.
#[derive(Clone, Debug)]
pub struct CheckpointPolicy {
    /// Minimum interval between checkpoints in milliseconds.
    pub interval_ms: u64,
    /// Create a checkpoint every N events (0 = disabled).
    pub every_events: u64,
    /// Number of recent checkpoints to retain.
    pub retain_last: usize,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            interval_ms: 60_000,
            every_events: 0,
            retain_last: 3,
        }
    }
}

/// Policy controlling recovery behavior on engine start.
#[derive(Clone, Debug)]
pub struct RecoveryPolicy {
    /// Whether to restore the latest checkpoint on startup.
    pub restore_latest_checkpoint: bool,
    /// Whether to replay events from the checkpoint offset.
    pub replay_from_checkpoint_offset: bool,
    /// Whether to fail if the primary backend is unavailable.
    pub fail_if_primary_unavailable: bool,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            restore_latest_checkpoint: true,
            replay_from_checkpoint_offset: true,
            fail_if_primary_unavailable: true,
        }
    }
}

/// Snapshot recovered from persistent storage.
#[derive(Clone, Debug)]
pub struct RecoverySnapshot {
    /// The checkpoint that was restored, if any.
    pub checkpoint: Option<CheckpointSnapshot>,
    /// Events to replay after the checkpoint.
    pub events: Vec<StreamEnvelope>,
}

/// Coordinates persistence across primary and mirror backends.
///
/// Supports primary+mirror fanout with configurable failure semantics.
pub struct PersistenceCoordinator {
    primary: Box<dyn PersistentBackend>,
    mirrors: Vec<MirrorBackend>,
}

struct MirrorBackend {
    backend: Box<dyn PersistentBackend>,
    strict: bool,
}

impl PersistenceCoordinator {
    /// Creates a coordinator from providers and configuration.
    pub fn new(
        providers: &HashMap<String, Box<dyn StorageBackendProvider>>,
        config: &EnginePersistenceConfig,
    ) -> Result<Self, StorageError> {
        let primary_provider =
            providers
                .get(&config.primary_backend_id)
                .ok_or(StorageError::NotFound {
                    message: format!(
                        "primary backend provider '{}' not found",
                        config.primary_backend_id
                    ),
                })?;

        let primary_config = config
            .backends
            .get(&config.primary_backend_id)
            .cloned()
            .unwrap_or_default();

        let primary = primary_provider.create(primary_config)?;

        let mut mirrors = Vec::new();
        for mirror_id in &config.mirror_backend_ids {
            let provider = providers.get(mirror_id).ok_or(StorageError::NotFound {
                message: format!("mirror backend provider '{}' not found", mirror_id),
            })?;
            let mirror_config = config.backends.get(mirror_id).cloned().unwrap_or_default();
            let backend = provider.create(mirror_config.clone())?;
            mirrors.push(MirrorBackend {
                backend,
                strict: mirror_config.strict_mirroring,
            });
        }

        Ok(Self { primary, mirrors })
    }

    /// Appends an event to the primary journal and mirrors.
    pub async fn append_event(&self, event: StreamEnvelope) -> Result<u64, StorageError> {
        let offset = self.primary.event_journal().append(event.clone()).await?;

        for mirror in &self.mirrors {
            let result = mirror.backend.event_journal().append(event.clone()).await;
            if let Err(e) = result {
                if mirror.strict {
                    return Err(e);
                }
                tracing::warn!("mirror journal append failed (lenient): {e}");
            }
        }

        Ok(offset)
    }

    /// Writes a checkpoint to the primary store and mirrors.
    pub async fn write_checkpoint(
        &self,
        manifest: CheckpointManifest,
        operator_state_blobs: HashMap<String, Vec<u8>>,
    ) -> Result<(), StorageError> {
        self.primary
            .checkpoint_store()
            .write_checkpoint(manifest.clone(), operator_state_blobs.clone())
            .await?;

        for mirror in &self.mirrors {
            let result = mirror
                .backend
                .checkpoint_store()
                .write_checkpoint(manifest.clone(), operator_state_blobs.clone())
                .await;
            if let Err(e) = result {
                if mirror.strict {
                    return Err(e);
                }
                tracing::warn!("mirror checkpoint write failed (lenient): {e}");
            }
        }

        Ok(())
    }

    /// Recovers state from the primary backend.
    pub async fn recover(&self) -> Result<RecoverySnapshot, StorageError> {
        let checkpoint = self.primary.checkpoint_store().latest().await?;

        let replay_offset = checkpoint
            .as_ref()
            .map(|cp| cp.manifest.event_offset)
            .unwrap_or(0);

        let events = self
            .primary
            .event_journal()
            .read_from(replay_offset)
            .await?;

        Ok(RecoverySnapshot { checkpoint, events })
    }

    /// Closes all backends.
    pub async fn close(&self) -> Result<(), StorageError> {
        self.primary.close().await?;
        for mirror in &self.mirrors {
            let _ = mirror.backend.close().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::{FileBackedStorageProvider, StorageBackendProvider};

    fn temp_config(
        dir: &str,
    ) -> (
        HashMap<String, Box<dyn StorageBackendProvider>>,
        EnginePersistenceConfig,
    ) {
        let mut providers: HashMap<String, Box<dyn StorageBackendProvider>> = HashMap::new();
        providers.insert("file".to_string(), Box::new(FileBackedStorageProvider));

        let mut backends = HashMap::new();
        backends.insert(
            "file".to_string(),
            BackendConfig::builder().property("path", dir).build(),
        );

        let config = EnginePersistenceConfig {
            enabled: true,
            primary_backend_id: "file".to_string(),
            mirror_backend_ids: vec![],
            backends,
            checkpoint_policy: CheckpointPolicy::default(),
            recovery_policy: RecoveryPolicy::default(),
        };

        (providers, config)
    }

    fn temp_dir() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/cqels-persist-test-{ts}-{id}")
    }

    #[tokio::test]
    async fn test_coordinator_append_and_recover() {
        let dir = temp_dir();
        let (providers, config) = temp_config(&dir);
        let coord = PersistenceCoordinator::new(&providers, &config).unwrap();

        let env = StreamEnvelope::new(0, "stream1", 100, "rdf", vec![1, 2, 3]);
        let offset = coord.append_event(env).await.unwrap();
        assert!(offset > 0);

        let snapshot = coord.recover().await.unwrap();
        assert!(snapshot.checkpoint.is_none());
        assert_eq!(snapshot.events.len(), 1);

        coord.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_checkpoint_and_recover() {
        let dir = temp_dir();
        let (providers, config) = temp_config(&dir);
        let coord = PersistenceCoordinator::new(&providers, &config).unwrap();

        // Append 3 events
        for i in 1..=3 {
            let env = StreamEnvelope::new(0, "s", i * 100, "rdf", vec![i as u8]);
            coord.append_event(env).await.unwrap();
        }

        // Checkpoint at offset 2
        let manifest = CheckpointManifest::new(1, 1000, 2);
        coord
            .write_checkpoint(manifest, HashMap::new())
            .await
            .unwrap();

        let snapshot = coord.recover().await.unwrap();
        assert!(snapshot.checkpoint.is_some());
        assert_eq!(snapshot.checkpoint.as_ref().unwrap().manifest.id, 1);
        // Events after offset 2 should be returned
        assert_eq!(snapshot.events.len(), 1);

        coord.close().await.unwrap();
    }

    #[test]
    fn test_default_policies() {
        let cp = CheckpointPolicy::default();
        assert_eq!(cp.interval_ms, 60_000);
        assert_eq!(cp.retain_last, 3);

        let rp = RecoveryPolicy::default();
        assert!(rp.restore_latest_checkpoint);
        assert!(rp.fail_if_primary_unavailable);
    }
}
