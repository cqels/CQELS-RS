//! Answer Set Programming (ASP) solver integration for CQELS.
//!
//! This crate provides an ASP solver interface for the CQELS streaming engine,
//! enabling logic-programming-based reasoning over RDF data streams.
//!
//! # Architecture
//!
//! - [`AspSolver`] -- Trait for ASP solver backends.
//! - [`ClingoSubprocessSolver`] -- Solver that shells out to the Clingo
//!   executable, parsing its JSON output.
//! - [`AspTermCodec`] -- Bidirectional encoding between RDF terms and ASP terms.
//! - [`AspFactMapper`] -- Converts RDF statements into ASP facts.
//! - [`IncrementalAspSession`] -- Stateful session for incremental solving
//!   with delta updates.
//! - [`AspContinuousQuery`] -- Bridges ASP solving into the CQELS continuous
//!   query pipeline.
//!
//! # Examples
//!
//! ```no_run
//! use cqels_asp::{ClingoSubprocessSolver, AspSolver};
//!
//! # async fn example() -> Result<(), cqels_asp::AspError> {
//! let solver = ClingoSubprocessSolver::new();
//! let results = solver.solve("a :- not b. b :- not a.", 0).await?;
//! for answer_set in &results {
//!     for atom in &answer_set.atoms {
//!         println!("{atom}");
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod codec;
pub mod config;
pub mod error;
pub mod mapper;
pub mod query;
pub mod session;
pub mod solver;
pub mod subprocess;

// Re-exports for convenience.
pub use codec::AspTermCodec;
pub use config::{AspStreamSolveConfig, AspStreamSolveConfigBuilder};
pub use error::AspError;
pub use mapper::{AspFactDelta, AspFactMapper};
pub use query::{AspContinuousQuery, AspSolveResult};
pub use session::IncrementalAspSession;
pub use solver::{AnswerSet, AspSolver, Atom};
pub use subprocess::ClingoSubprocessSolver;
