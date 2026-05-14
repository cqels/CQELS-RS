//! The narrow executor trait that captures exactly the IoTDB-shaped
//! operations the storage SPI needs.
//!
//! Implementing this trait against `iotdb-client-rs::Session`,
//! `iotdb-client`, or any custom Thrift client wires the rest of the
//! crate to a real IoTDB cluster. See the crate-level docs for an
//! example sketch.

use async_trait::async_trait;
use thiserror::Error;

/// Errors surfaced by an [`IotDbExecutor`] implementation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IotDbError {
    /// Underlying client / network / session failure.
    #[error("iotdb backend error: {0}")]
    Backend(String),

    /// The operation was not supported by this executor.
    #[error("iotdb operation not supported: {0}")]
    Unsupported(String),
}

/// A narrow async surface mapping the storage SPI onto an IoTDB-shaped
/// time-series database.
///
/// Each method addresses a single `device` (an IoTDB path like
/// `root.cqels.journal`), with `u64` keys (mapped to IoTDB `time`)
/// and `Vec<u8>` payloads (a single binary measurement column).
///
/// Implementations must be `Send + Sync`. Internal mutation should be
/// guarded by the impl (e.g. a `Mutex<Session>` for the Thrift client).
#[async_trait]
pub trait IotDbExecutor: Send + Sync {
    /// Inserts a row at time `key` with payload `data` into `device`.
    /// Overwrites any existing row at the same key.
    async fn put(&self, device: &str, key: u64, data: Vec<u8>) -> Result<(), IotDbError>;

    /// Returns rows in `device` with `time >= start_inclusive`, in
    /// ascending key order.
    async fn read_from(
        &self,
        device: &str,
        start_inclusive: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IotDbError>;

    /// Returns the row with the highest key in `device`, if any.
    async fn latest(&self, device: &str) -> Result<Option<(u64, Vec<u8>)>, IotDbError>;

    /// Deletes rows in `device` whose key falls in `[start, end)`.
    async fn delete_range(
        &self,
        device: &str,
        start_inclusive: u64,
        end_exclusive: u64,
    ) -> Result<(), IotDbError>;

    /// Closes the underlying resources (Thrift session, etc.).
    async fn close(&self) -> Result<(), IotDbError>;
}
