//! Query compilers that translate parsed ASTs into executable query pipelines.
//!
//! This module bridges the gap between parsed query definitions and operator execution:
//!
//! - [`pipeline`] — Shared pipeline utilities (pattern matching, filtering, projection)
//! - [`compiled`] — Compiled query types implementing `ContinuousQuery`
//! - [`cqelsql`] — CqelsQL (SPARQL-style) query compiler
//! - [`cypherql`] — CypherQL (Cypher-style) query compiler

pub mod compiled;
pub mod cqelsql;
pub mod cypherql;
pub mod pipeline;

pub use compiled::{CompiledCqelsQuery, CompiledCypherQuery};
pub use cqelsql::CqelsQueryCompiler;
pub use cypherql::CypherQueryCompiler;
