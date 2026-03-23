//! SHACL validation with ASP-based repair search for CQELS.
//!
//! This crate provides a SHACL (Shapes Constraint Language) validation
//! pipeline that compiles shape constraints into Answer Set Programs and
//! uses an ASP solver to detect violations and search for minimal repairs.
//!
//! # Architecture
//!
//! 1. **Parse** -- [`ShaclShapeParser`] builds a [`ShaclShapeGraph`] from
//!    RDF statements.
//! 2. **Compile** -- [`ShaclAspProgramCompiler`] translates shapes into ASP
//!    validation and repair programs.
//! 3. **Validate** -- [`ShaclValidationEngine`] orchestrates the solver,
//!    returning [`ShaclValidationResult`] with violations and optional
//!    repair candidates.
//! 4. **Stream** -- [`ShaclContinuousQuery`] (stub) integrates into the
//!    CQELS continuous query pipeline.
//!
//! # Configuration
//!
//! Use [`ShaclStreamSolveConfig`] to control repair search, model limits,
//! and solver timeouts.

pub mod candidate;
pub mod compiler;
pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod parser;
pub mod query;
pub mod result;

// Re-exports for convenience.
pub use candidate::{ShaclCandidateGenerator, ShaclCandidateSet};
pub use compiler::ShaclAspProgramCompiler;
pub use config::{ShaclStreamSolveConfig, ShaclStreamSolveConfigBuilder};
pub use engine::ShaclValidationEngine;
pub use error::ShaclError;
pub use model::{NodeKind, NodeShape, PropertyShape, ShaclShapeGraph};
pub use parser::ShaclShapeParser;
pub use query::ShaclContinuousQuery;
pub use result::{
    ShaclRepairCandidate, ShaclRepairEdit, ShaclRepairOperation, ShaclValidationResult,
    ShaclValidationStatus, ShaclViolation,
};
