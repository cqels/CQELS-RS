//! Error types for CDSP ingestion.

use thiserror::Error;

/// Errors produced by CDSP signal parsing and mapping.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CdspError {
    /// A VSS signal path was malformed (empty, leading/trailing dot, etc.).
    #[error("invalid VSS signal path: {0}")]
    InvalidSignalPath(String),

    /// A value could not be parsed into a [`crate::VssValue`] variant.
    #[error("invalid VSS value: {0}")]
    InvalidValue(String),

    /// The underlying signal source has been closed.
    #[error("VSS signal source closed")]
    SourceClosed,

    /// A JSON deserialization error wrapping `serde_json::Error`.
    #[error("VSS JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}
