//! Core stream processing components for the CQELS engine.
//!
//! This crate implements the heart of CQELS -- the continuous query evaluation
//! pipeline. It is a Rust port of the core processing layer from
//! [CQELS 2.0](https://cqels.github.io/) (Continuous Query Evaluation over
//! Linked Streams).
//!
//! # Modules
//!
//! - [`stream`] -- Stream element types ([`StreamElement`](stream::StreamElement),
//!   [`RdfStreamElement`](stream::RdfStreamElement)) and the [`Timestamped`](stream::Timestamped)
//!   trait that all streamable items must implement.
//! - [`window`] -- Window operators for partitioning unbounded streams into
//!   finite batches: [`TumblingWindow`](window::TumblingWindow),
//!   [`SlidingWindow`](window::SlidingWindow), [`SessionWindow`](window::SessionWindow),
//!   and [`TumblingCountWindow`](window::TumblingCountWindow).
//! - [`query`] -- Continuous query representations and input bindings.
//! - [`parser`] -- CQELS-QL query parser that translates textual queries into
//!   executable query plans.
//! - [`operator`] -- Stream operators including aggregation, filtering, joins,
//!   ranking, and the SWAG (Sliding-Window AGgregation) algorithm for efficient
//!   incremental aggregation.
//!
//! # Architecture
//!
//! The processing pipeline follows a dataflow model:
//!
//! 1. **Ingest** -- Raw events arrive as [`StreamElement`](stream::StreamElement) values.
//! 2. **Window** -- The [`Window`](window::Window) trait partitions the stream into
//!    [`WindowedBatch`](window::WindowedBatch) segments.
//! 3. **Evaluate** -- Operators (join, filter, aggregate) process each batch.
//! 4. **Emit** -- Results are pushed downstream as new stream elements.
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//! use cqels_core::window::{TumblingWindow, WindowType};
//!
//! // Create a 5-second tumbling window
//! let window = TumblingWindow::new(Duration::from_secs(5));
//! ```

pub mod stream;
pub mod window;
pub mod query;
pub mod operator;
pub mod parser;
