//! COVESA Data Service Platform (CDSP) signal-ingestion adapter for CQELS.
//!
//! This crate is the Rust counterpart of Java's `cqels-cdsp`. It
//! provides the **shape and adapter layer** that lets a CQELS engine
//! consume vehicle telemetry described by the COVESA Vehicle Signal
//! Specification (VSS): a tree-structured catalog of dotted signal
//! paths like `Vehicle.Speed`, `Vehicle.Powertrain.FuelSystem.Level`,
//! and so on, each carrying a typed value plus a millisecond timestamp.
//!
//! The crate intentionally does **not** ship a network client to a
//! specific VSS broker (e.g. KUKSA.val, Eclipse Sommr, COVESA Cloud &
//! Connected Vehicle Systems server). Wiring a concrete transport
//! (HTTP/WebSocket/gRPC) is left to downstream code, which only needs
//! to implement [`VssSignalSource`] and feed [`VssSignal`] values into
//! a channel.
//!
//! ## Pieces
//!
//! - [`VssSignal`] / [`VssValue`] — the canonical signal envelope.
//! - [`VssSignalMapper`] — maps a [`VssSignal`] to a
//!   [`cqels_core::stream::RdfStreamElement`] using a configurable IRI
//!   base for vehicle subjects and a flexible
//!   `signal-path → predicate-IRI` strategy.
//! - [`VssSignalSource`] — an async-trait source of signals; a
//!   reference [`ChannelVssSignalSource`] backed by a
//!   `tokio::sync::mpsc` channel is provided for tests and in-process
//!   adapters.
//! - [`CdspError`] — typed errors for parsing/mapping failures.
//!
//! ## Quickstart
//!
//! ```no_run
//! use cqels_cdsp::{
//!     ChannelVssSignalSource, VssSignal, VssSignalMapper, VssSignalSource, VssValue,
//! };
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let (source, sender) = ChannelVssSignalSource::with_capacity(64);
//! let mapper = VssSignalMapper::new("urn:example:vehicle/abc123");
//!
//! sender
//!     .send(VssSignal::new("Vehicle.Speed", VssValue::from(72.0), 1_700_000_000_000))
//!     .await
//!     .ok();
//!
//! let signal = source.next().await.unwrap();
//! let rdf_element = mapper.to_rdf_stream_element(&signal);
//! assert_eq!(rdf_element.timestamp, 1_700_000_000_000);
//! # Ok(()) }
//! ```

pub mod error;
#[cfg(feature = "kuksa")]
pub mod kuksa_source;
pub mod mapper;
pub mod signal;
pub mod source;

pub use error::CdspError;
#[cfg(feature = "kuksa")]
pub use kuksa_source::{KuksaConnectConfig, KuksaVssSignalSource};
pub use mapper::{PredicateStrategy, VssSignalMapper};
pub use signal::{VssSignal, VssValue};
pub use source::{ChannelVssSignalSource, VssSignalSource};
