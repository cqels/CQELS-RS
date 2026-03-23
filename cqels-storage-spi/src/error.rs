//! Error types for the storage SPI.

/// Errors that can occur in storage backend operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// I/O error from the underlying storage.
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization error.
    #[error("codec error: {message}")]
    Codec { message: String },

    /// Requested resource was not found.
    #[error("not found: {message}")]
    NotFound { message: String },

    /// The backend is unavailable (e.g., connection lost).
    #[error("backend unavailable: {message}")]
    Unavailable { message: String },

    /// Generic backend error.
    #[error("storage error: {message}")]
    Other { message: String },
}
