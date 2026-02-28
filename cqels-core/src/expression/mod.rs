//! Expression evaluation for CQELS continuous queries.
//!
//! This module bridges parsed query expressions and operator execution by
//! providing:
//!
//! - [`ast`] — Expression tree types (`Expression`, `BinaryOp`, `UnaryOp`)
//! - [`functions`] — SPARQL 1.1 built-in function implementations
//! - [`evaluator`] — Expression evaluation against `BindingSet` bindings
//! - [`parser`] — Parsing expression strings into `Expression` trees

pub mod ast;
mod cqelsql_parser;
mod cypherql_parser;
pub mod evaluator;
pub mod functions;
pub mod parser;

pub use ast::{AggregateExprFunction, BinaryOp, Expression, UnaryOp};
pub use evaluator::ExpressionEvaluator;
pub use parser::ExpressionParser;
