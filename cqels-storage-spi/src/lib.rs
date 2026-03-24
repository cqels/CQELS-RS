//! Pluggable persistent storage Service Provider Interface (SPI) for CQELS.
//!
//! This crate defines backend-agnostic traits for event journaling,
//! checkpointing, and recovery. A file-backed reference implementation
//! is included for testing and development.
//!
//! # Architecture
//!
//! - [`PersistentBackend`] — top-level backend providing journal + checkpoint store
//! - [`EventJournal`] — append-only event log with offset-based reads
//! - [`CheckpointStore`] — snapshot persistence for operator state
//! - [`StorageBackendProvider`] — factory for creating backend instances
//! - [`FileBackedPersistentBackend`] — reference implementation
//!
//! # Examples
//!
//! ```no_run
//! use cqels_storage_spi::{BackendConfig, FileBackedStorageProvider, StorageBackendProvider};
//!
//! let provider = FileBackedStorageProvider;
//! let config = BackendConfig::builder()
//!     .property("path", "/tmp/cqels-data")
//!     .build();
//! let backend = provider.create(config).expect("create backend");
//! ```

pub mod backend;
pub mod checkpoint;
pub mod codec;
pub mod config;
pub mod envelope;
pub mod error;
pub mod file_backend;
pub mod provider;
pub mod rdf_codec;

pub use backend::{CheckpointStore, EventJournal, PersistentBackend};
pub use checkpoint::{CheckpointManifest, CheckpointSnapshot};
pub use codec::{BinaryCodec, BinaryCodecRegistry, IdentityCodec};
pub use config::{BackendConfig, BackendConfigBuilder};
pub use envelope::StreamEnvelope;
pub use error::StorageError;
pub use file_backend::{FileBackedPersistentBackend, FileBackedStorageProvider};
pub use provider::StorageBackendProvider;
pub use rdf_codec::{deserialize_stream_element, serialize_stream_element, RdfCodec};
