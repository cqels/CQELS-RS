//! Integration tests for the cqels-storage-spi crate.

use std::collections::HashMap;

use cqels_storage_spi::{
    BackendConfig, CheckpointManifest, FileBackedStorageProvider, PersistentBackend,
    StorageBackendProvider, StreamEnvelope,
};

fn temp_dir() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let id = CTR.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/cqels-spi-integ-{ts}-{id}")
}

fn create_backend() -> Box<dyn PersistentBackend> {
    let provider = FileBackedStorageProvider;
    let config = BackendConfig::builder()
        .property("path", temp_dir())
        .build();
    provider.create(config).expect("create backend")
}

#[tokio::test]
async fn provider_create_and_close() {
    let backend = create_backend();
    backend.close().await.expect("close");
}

#[tokio::test]
async fn journal_append_and_read() {
    let backend = create_backend();
    let journal = backend.event_journal();

    let env1 = StreamEnvelope::new(0, "stream1", 100, "rdf", vec![1, 2, 3]);
    let offset1 = journal.append(env1).await.unwrap();
    assert!(offset1 > 0);

    let env2 = StreamEnvelope::new(0, "stream1", 200, "rdf", vec![4, 5]);
    let offset2 = journal.append(env2).await.unwrap();
    assert!(offset2 > offset1);

    // Read all from offset 0
    let events = journal.read_from(0).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].stream_name, "stream1");
    assert_eq!(events[0].timestamp, 100);
    assert_eq!(events[1].timestamp, 200);

    // Read from offset1 should skip first
    let events_from = journal.read_from(offset1).await.unwrap();
    assert_eq!(events_from.len(), 1);
    assert_eq!(events_from[0].timestamp, 200);

    backend.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_write_and_read() {
    let backend = create_backend();
    let store = backend.checkpoint_store();

    // Initially no checkpoint
    let latest = store.latest().await.unwrap();
    assert!(latest.is_none());

    // Write a checkpoint
    let manifest = CheckpointManifest::new(1, 5000, 10);
    let mut blobs = HashMap::new();
    blobs.insert("op1".to_string(), vec![42u8; 100]);
    store
        .write_checkpoint(manifest.clone(), blobs)
        .await
        .unwrap();

    // Read it back
    let latest = store.latest().await.unwrap();
    assert!(latest.is_some());
    let snapshot = latest.unwrap();
    assert_eq!(snapshot.manifest.id, 1);
    assert_eq!(snapshot.manifest.created_at, 5000);
    assert_eq!(snapshot.manifest.event_offset, 10);
    assert!(snapshot.operator_state_blobs.contains_key("op1"));
    assert_eq!(snapshot.operator_state_blobs["op1"].len(), 100);

    backend.close().await.unwrap();
}

#[tokio::test]
async fn journal_then_checkpoint_then_read() {
    let backend = create_backend();
    let journal = backend.event_journal();
    let store = backend.checkpoint_store();

    // Append 3 events
    for i in 1..=3u8 {
        let env = StreamEnvelope::new(0, "s", (i as i64) * 100, "rdf", vec![i]);
        journal.append(env).await.unwrap();
    }

    // Checkpoint at offset 2
    let manifest = CheckpointManifest::new(1, 1000, 2);
    store
        .write_checkpoint(manifest, HashMap::new())
        .await
        .unwrap();

    // Recover: should get checkpoint + events after offset
    let snapshot = store.latest().await.unwrap().unwrap();
    assert_eq!(snapshot.manifest.event_offset, 2);

    let events = journal.read_from(2).await.unwrap();
    // Only the 3rd event (at offset 3) should be returned
    assert_eq!(events.len(), 1);

    backend.close().await.unwrap();
}
