//! End-to-end persistence integration tests.
//!
//! Tests the full round-trip: journal → checkpoint → recover.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cqels_engine::facade::CqelsEngine;
use cqels_engine::persistence::{CheckpointPolicy, EnginePersistenceConfig, RecoveryPolicy};

fn temp_dir() -> String {
    static CTR: AtomicU64 = AtomicU64::new(0);
    let id = CTR.fetch_add(1, Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/cqels-e2e-persist-{ts}-{id}")
}

fn make_persistence_config(dir: &str) -> EnginePersistenceConfig {
    let mut backends = HashMap::new();
    backends.insert(
        "file".to_string(),
        cqels_storage_spi::BackendConfig::builder()
            .property("path", dir)
            .build(),
    );

    EnginePersistenceConfig {
        enabled: true,
        primary_backend_id: "file".to_string(),
        mirror_backend_ids: vec![],
        backends,
        checkpoint_policy: CheckpointPolicy {
            every_events: 5,
            interval_ms: 0,
            retain_last: 3,
        },
        recovery_policy: RecoveryPolicy::default(),
    }
}

#[tokio::test]
async fn test_persistence_wired_in_builder() {
    let dir = temp_dir();
    let config = make_persistence_config(&dir);

    let engine = CqelsEngine::builder()
        .id("persist-test")
        .persistence_config(config)
        .build()
        .unwrap();

    assert!(engine.persistence().is_some());
}

#[tokio::test]
async fn test_persistence_push_events_and_recover() {
    let dir = temp_dir();
    let config = make_persistence_config(&dir);

    // Phase 1: Create engine, push events
    {
        let mut engine = CqelsEngine::builder()
            .id("e2e-persist-1")
            .persistence_config(config.clone())
            .build()
            .unwrap();

        let stream = engine.create_stream("sensors").await.unwrap();
        engine.start().await.unwrap();

        // Push several events
        for i in 0..10 {
            stream
                .push_f64(
                    &format!("http://sensor/{i}"),
                    "http://ex/temp",
                    20.0 + i as f64,
                )
                .await
                .unwrap();
        }

        // Give forwarding task time to process
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        engine.stop().await.unwrap();
    }

    // Phase 2: Create a new engine with same path — verify persistence is accessible
    {
        let engine = CqelsEngine::builder()
            .id("e2e-persist-2")
            .persistence_config(config)
            .build()
            .unwrap();

        assert!(engine.persistence().is_some());

        // The runtime auto-recovery will run on start
        engine.start().await.unwrap();

        // If we got here without error, auto-recovery succeeded
        engine.stop().await.unwrap();
    }
}

#[tokio::test]
async fn test_persistence_disabled_config() {
    let dir = temp_dir();
    let mut config = make_persistence_config(&dir);
    config.enabled = false;

    let engine = CqelsEngine::builder()
        .id("disabled-persist")
        .persistence_config(config)
        .build()
        .unwrap();

    assert!(engine.persistence().is_none());
}

#[tokio::test]
async fn test_engine_start_with_empty_persistence() {
    let dir = temp_dir();
    let config = make_persistence_config(&dir);

    let engine = CqelsEngine::builder()
        .id("empty-persist")
        .persistence_config(config)
        .build()
        .unwrap();

    // Start with empty persistence — should succeed without error
    engine.start().await.unwrap();
    engine.stop().await.unwrap();
}
