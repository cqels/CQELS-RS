//! Storage backend provider (factory pattern).

use crate::backend::PersistentBackend;
use crate::config::BackendConfig;
use crate::error::StorageError;

/// Factory for creating [`PersistentBackend`] instances.
///
/// Each provider has a unique `backend_id` (e.g., `"file"`, `"lmdb"`, `"rocksdb"`)
/// and creates backend instances from a [`BackendConfig`].
pub trait StorageBackendProvider: Send + Sync {
    /// Unique identifier for this backend type.
    fn backend_id(&self) -> &str;

    /// Creates a new backend instance with the given configuration.
    fn create(&self, config: BackendConfig) -> Result<Box<dyn PersistentBackend>, StorageError>;
}
