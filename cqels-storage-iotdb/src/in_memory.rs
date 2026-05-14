//! In-memory reference implementation of [`IotDbExecutor`].
//!
//! Backed by a per-device `BTreeMap<u64, Vec<u8>>` so that range
//! reads, latest, and range deletes are all `O(log n + k)` like a
//! real time-series engine. Useful as:
//!
//! - The default backend exposed by [`crate::IotDbStorageProvider`]
//!   (so the crate "just works" without a running IoTDB cluster).
//! - A fixture for unit tests, including downstream code building on
//!   top of `IotDbPersistentBackend`.
//! - A development sandbox for trying out time-series mappings before
//!   wiring a real Thrift client.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::executor::{IotDbError, IotDbExecutor};

/// Per-device sorted KV map. Cheap to clone (each clone gets its own
/// data) but generally used behind an `Arc`.
#[derive(Default)]
pub struct InMemoryIotDbExecutor {
    devices: Mutex<HashMap<String, BTreeMap<u64, Vec<u8>>>>,
}

impl InMemoryIotDbExecutor {
    /// Constructs an empty executor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current row count for `device`.
    pub fn len(&self, device: &str) -> usize {
        self.devices
            .lock()
            .get(device)
            .map(BTreeMap::len)
            .unwrap_or(0)
    }

    /// Returns the list of devices that have ever been written to.
    pub fn device_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.devices.lock().keys().cloned().collect();
        names.sort();
        names
    }
}

#[async_trait]
impl IotDbExecutor for InMemoryIotDbExecutor {
    async fn put(&self, device: &str, key: u64, data: Vec<u8>) -> Result<(), IotDbError> {
        self.devices
            .lock()
            .entry(device.to_string())
            .or_default()
            .insert(key, data);
        Ok(())
    }

    async fn read_from(
        &self,
        device: &str,
        start_inclusive: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IotDbError> {
        let guard = self.devices.lock();
        let rows: Vec<(u64, Vec<u8>)> = guard
            .get(device)
            .map(|tree| {
                tree.range(start_inclusive..)
                    .map(|(k, v)| (*k, v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(rows)
    }

    async fn latest(&self, device: &str) -> Result<Option<(u64, Vec<u8>)>, IotDbError> {
        Ok(self
            .devices
            .lock()
            .get(device)
            .and_then(|tree| tree.iter().next_back().map(|(k, v)| (*k, v.clone()))))
    }

    async fn delete_range(
        &self,
        device: &str,
        start_inclusive: u64,
        end_exclusive: u64,
    ) -> Result<(), IotDbError> {
        if start_inclusive >= end_exclusive {
            return Ok(());
        }
        let mut guard = self.devices.lock();
        if let Some(tree) = guard.get_mut(device) {
            // Collect-then-remove avoids holding a borrow over mutation.
            let keys: Vec<u64> = tree
                .range(start_inclusive..end_exclusive)
                .map(|(k, _)| *k)
                .collect();
            for k in keys {
                tree.remove(&k);
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), IotDbError> {
        // Nothing to release.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_and_read_in_order() {
        let exec = InMemoryIotDbExecutor::new();
        exec.put("d", 1, b"a".to_vec()).await.unwrap();
        exec.put("d", 3, b"c".to_vec()).await.unwrap();
        exec.put("d", 2, b"b".to_vec()).await.unwrap();
        let rows = exec.read_from("d", 0).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[2].0, 3);
    }

    #[tokio::test]
    async fn read_from_respects_lower_bound() {
        let exec = InMemoryIotDbExecutor::new();
        for k in [1u64, 2, 3, 4, 5] {
            exec.put("d", k, vec![k as u8]).await.unwrap();
        }
        let rows = exec.read_from("d", 3).await.unwrap();
        let keys: Vec<u64> = rows.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn latest_returns_highest_key() {
        let exec = InMemoryIotDbExecutor::new();
        exec.put("d", 7, b"hi".to_vec()).await.unwrap();
        exec.put("d", 3, b"lo".to_vec()).await.unwrap();
        let latest = exec.latest("d").await.unwrap().unwrap();
        assert_eq!(latest.0, 7);
        assert_eq!(latest.1, b"hi");
    }

    #[tokio::test]
    async fn delete_range_is_half_open() {
        let exec = InMemoryIotDbExecutor::new();
        for k in [1u64, 2, 3, 4, 5] {
            exec.put("d", k, vec![k as u8]).await.unwrap();
        }
        exec.delete_range("d", 2, 4).await.unwrap();
        let keys: Vec<u64> = exec
            .read_from("d", 0)
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![1, 4, 5]);
    }

    #[tokio::test]
    async fn put_overwrites_existing_key() {
        let exec = InMemoryIotDbExecutor::new();
        exec.put("d", 1, b"first".to_vec()).await.unwrap();
        exec.put("d", 1, b"second".to_vec()).await.unwrap();
        assert_eq!(exec.len("d"), 1);
        let rows = exec.read_from("d", 0).await.unwrap();
        assert_eq!(rows[0].1, b"second");
    }

    #[tokio::test]
    async fn unknown_device_yields_empty_results() {
        let exec = InMemoryIotDbExecutor::new();
        assert!(exec.read_from("nope", 0).await.unwrap().is_empty());
        assert!(exec.latest("nope").await.unwrap().is_none());
        // delete on missing device is a no-op (success).
        exec.delete_range("nope", 0, 100).await.unwrap();
    }
}
