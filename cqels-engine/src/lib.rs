//! Stream engine and Complex Event Processing (CEP) for CQELS.
//!
//! This crate provides the runtime engine that orchestrates continuous query
//! evaluation over RDF streams, along with a full NFA-based CEP pattern
//! matching subsystem. It is a Rust port of the engine layer from
//! [CQELS 2.0](https://cqels.github.io/).
//!
//! # Modules
//!
//! - [`engine`] -- The [`StreamEngine`] trait and its tokio-based implementation
//!   [`ReactiveStreamEngine`], which manages stream registration, query
//!   execution, and lifecycle control.
//! - [`cep`] -- Complex Event Processing via NFA (Non-deterministic Finite
//!   Automaton) pattern matching. Supports strict, relaxed, and
//!   non-deterministic contiguity; quantifiers (one, one-or-more, times);
//!   negation; context conditions; and time-window constraints.
//!
//! # Key Types
//!
//! - [`Pattern`] -- A fluent builder for defining CEP event patterns as a
//!   linked list of named states with conditions and transitions.
//! - [`NfaPatternProcessor`] -- Compiles a [`Pattern`] into an NFA and
//!   processes an event stream, emitting [`PatternMatch`] results.
//! - [`PatternMatch`] -- A completed match containing the sequence of matched
//!   events with timestamps and optional named-event access.
//! - [`StreamEngine`] -- Async trait for registering streams, submitting
//!   continuous queries, and controlling engine lifecycle.
//! - [`ReactiveStreamEngine`] -- Concrete engine backed by tokio broadcast
//!   channels.
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//! use cqels_engine::{Pattern, Contiguity};
//!
//! // Detect event A followed (with relaxed contiguity) by event B within 5 s
//! let pattern = Pattern::<(String, i64)>::begin("start")
//!     .where_cond(|e| e.0 == "A")
//!     .followed_by("end")
//!     .where_cond(|e| e.0 == "B")
//!     .within(Duration::from_secs(5));
//! ```

pub mod cep;
pub mod engine;
pub mod runtime;

pub use cep::{Contiguity, NfaPatternProcessor, Pattern, PatternMatch, Quantifier};
pub use engine::{create_stream_pair, receiver_to_stream, ReactiveStreamEngine, StreamEngine};
pub use runtime::{CqelsRuntime, QueryRegistration};
