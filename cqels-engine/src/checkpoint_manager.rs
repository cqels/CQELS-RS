//! Checkpoint manager for automatic checkpointing based on event count or time.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqels_storage_spi::CheckpointManifest;

use crate::persistence::{CheckpointPolicy, PersistenceCoordinator};

/// Manages automatic checkpoint creation based on count and time thresholds.
pub struct CheckpointManager {
    coordinator: Arc<PersistenceCoordinator>,
    policy: CheckpointPolicy,
    event_count: AtomicU64,
    last_checkpoint_time: AtomicI64,
    checkpoint_id: AtomicU64,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager.
    pub fn new(coordinator: Arc<PersistenceCoordinator>, policy: CheckpointPolicy) -> Self {
        let now = current_time_ms();
        Self {
            coordinator,
            policy,
            event_count: AtomicU64::new(0),
            last_checkpoint_time: AtomicI64::new(now),
            checkpoint_id: AtomicU64::new(0),
        }
    }

    /// Called after each event is journaled. Triggers checkpoint if thresholds are met.
    pub async fn on_event(&self, offset: u64) {
        let count = self.event_count.fetch_add(1, Ordering::Relaxed) + 1;
        if self.should_checkpoint(count) {
            self.trigger_checkpoint(offset).await;
        }
    }

    /// Checks if a checkpoint should be triggered.
    fn should_checkpoint(&self, count: u64) -> bool {
        // Count-based threshold
        if self.policy.every_events > 0 && count % self.policy.every_events == 0 {
            return true;
        }
        // Time-based threshold
        let now = current_time_ms();
        let last = self.last_checkpoint_time.load(Ordering::Relaxed);
        if self.policy.interval_ms > 0 && (now - last) >= self.policy.interval_ms as i64 {
            return true;
        }
        false
    }

    /// Triggers a checkpoint write and prunes old checkpoints.
    pub async fn trigger_checkpoint(&self, offset: u64) {
        let now = current_time_ms();
        self.last_checkpoint_time.store(now, Ordering::Relaxed);
        self.event_count.store(0, Ordering::Relaxed);

        let id = self.checkpoint_id.fetch_add(1, Ordering::Relaxed) + 1;
        let manifest = CheckpointManifest::new(id, now, offset);

        if let Err(e) = self
            .coordinator
            .write_checkpoint(manifest, std::collections::HashMap::new())
            .await
        {
            tracing::warn!("checkpoint write failed: {e}");
        }
    }

    /// Returns the current event count since last checkpoint.
    pub fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::Relaxed)
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_storage_spi::{BackendConfig, FileBackedStorageProvider};
    use std::collections::HashMap;

    fn temp_dir() -> String {
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/cqels-checkpoint-test-{ts}-{id}")
    }

    fn make_coordinator(dir: &str) -> Arc<PersistenceCoordinator> {
        let mut providers: HashMap<String, Box<dyn cqels_storage_spi::StorageBackendProvider>> =
            HashMap::new();
        providers.insert("file".to_string(), Box::new(FileBackedStorageProvider));

        let mut backends = HashMap::new();
        backends.insert(
            "file".to_string(),
            BackendConfig::builder().property("path", dir).build(),
        );

        let config = crate::persistence::EnginePersistenceConfig {
            enabled: true,
            primary_backend_id: "file".to_string(),
            mirror_backend_ids: vec![],
            backends,
            checkpoint_policy: CheckpointPolicy::default(),
            recovery_policy: crate::persistence::RecoveryPolicy::default(),
        };

        Arc::new(PersistenceCoordinator::new(&providers, &config).unwrap())
    }

    #[tokio::test]
    async fn test_checkpoint_after_n_events() {
        let dir = temp_dir();
        let coord = make_coordinator(&dir);

        let policy = CheckpointPolicy {
            every_events: 3,
            interval_ms: 0,
            retain_last: 3,
        };
        let mgr = CheckpointManager::new(coord.clone(), policy);

        // First 2 events: no checkpoint
        mgr.on_event(1).await;
        mgr.on_event(2).await;
        assert_eq!(mgr.event_count(), 2);

        // 3rd event triggers checkpoint — count resets to 0
        mgr.on_event(3).await;
        assert_eq!(mgr.event_count(), 0);

        // Verify checkpoint was written
        let snapshot = coord.recover().await.unwrap();
        assert!(snapshot.checkpoint.is_some());
    }

    #[tokio::test]
    async fn test_event_count_tracking() {
        let dir = temp_dir();
        let coord = make_coordinator(&dir);

        let policy = CheckpointPolicy {
            every_events: 0, // disabled
            interval_ms: 0,  // disabled
            retain_last: 3,
        };
        let mgr = CheckpointManager::new(coord, policy);

        mgr.on_event(1).await;
        mgr.on_event(2).await;
        assert_eq!(mgr.event_count(), 2);
    }
}
