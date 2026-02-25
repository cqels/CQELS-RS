//! Query language parsers for CQELS.
//!
//! Provides parsers for two continuous query languages:
//! - **CqelsQL**: A SPARQL-based continuous query language with streaming extensions
//! - **CypherQL**: A Cypher-based continuous query language with streaming extensions
//!
//! Both parsers produce AST representations that can be compiled into executable
//! operator graphs by the engine runtime.

pub mod ast;
pub mod cqelsql;
pub mod cypherql;

pub use ast::*;
pub use cqelsql::CqelsQlParser;
pub use cypherql::CypherQlParser;

use thiserror::Error;

use cqels_model::error::{ParseErrorDetail, ParseErrorKind};
use cqels_model::CqelsError;

/// Errors that can occur during query parsing.
///
/// This type is a thin wrapper around [`ParseErrorDetail`] that preserves
/// backward-compatible variant names (`Syntax`, `Semantic`, `Unsupported`)
/// while internally using structured error data. It can be converted into
/// [`CqelsError`] via the `From` impl for seamless error propagation.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Grammar/syntax error in the query string.
    #[error("syntax error: {0}")]
    Syntax(String),

    /// Semantic error (e.g., undefined prefix, invalid window spec).
    #[error("semantic error: {0}")]
    Semantic(String),

    /// Unsupported feature in the query.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl ParseError {
    /// Returns the [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        match self {
            Self::Syntax(_) => ParseErrorKind::Syntax,
            Self::Semantic(_) => ParseErrorKind::Semantic,
            Self::Unsupported(_) => ParseErrorKind::Unsupported,
        }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        match self {
            Self::Syntax(m) | Self::Semantic(m) | Self::Unsupported(m) => m,
        }
    }
}

/// Converts a [`ParseError`] into a [`ParseErrorDetail`].
impl From<ParseError> for ParseErrorDetail {
    fn from(err: ParseError) -> Self {
        ParseErrorDetail::new(err.kind(), err.message())
    }
}

/// Converts a [`ParseError`] directly into a [`CqelsError::Parse`].
impl From<ParseError> for CqelsError {
    fn from(err: ParseError) -> Self {
        CqelsError::Parse(ParseErrorDetail::from(err))
    }
}

/// Result type for parsing operations.
pub type ParseResult<T> = Result<T, ParseError>;
