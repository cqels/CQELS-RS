//! Error types for the CQELS engine.
//!
//! Provides [`CqelsError`] — the unified error enum used across all crates —
//! and [`ParseErrorDetail`] for structured parse/syntax error reporting with
//! optional source-location information.

use std::fmt;

use thiserror::Error;

/// Unified error type for the CQELS engine.
///
/// Each variant carries structured context about the error rather than a
/// bare `String`, making it easier to inspect errors programmatically
/// and produce high-quality diagnostics.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CqelsError {
    /// A query syntax or semantic error originating from the parser.
    ///
    /// This variant wraps [`ParseErrorDetail`] and is automatically
    /// constructed via `From<ParseErrorDetail>`.
    #[error("parse error: {0}")]
    Parse(#[from] ParseErrorDetail),

    /// An error during expression evaluation (e.g., type mismatch).
    #[error("evaluation error: {message}")]
    Evaluation {
        /// Human-readable description of the evaluation error.
        message: String,
    },

    /// An error in the streaming subsystem.
    #[error("stream error: {message}")]
    Stream {
        /// Human-readable description of the stream error.
        message: String,
    },

    /// An error in the windowing subsystem.
    #[error("window error: {message}")]
    Window {
        /// Human-readable description of the window error.
        message: String,
    },

    /// An error in a join operator.
    #[error("join error: {message}")]
    Join {
        /// Human-readable description of the join error.
        message: String,
    },

    /// An error in the RETE reasoning engine.
    #[error("reasoning error: {message}")]
    Reasoning {
        /// Human-readable description of the reasoning error.
        message: String,
    },

    /// The requested operation is not supported.
    #[error("unsupported operation: {operation}")]
    UnsupportedOperation {
        /// Name or description of the unsupported operation.
        operation: String,
    },

    /// An RDF term failed validation (e.g., invalid IRI, bad blank-node id).
    #[error("invalid term: {detail}")]
    InvalidTerm {
        /// What went wrong with the term.
        detail: String,
    },

    /// A required variable binding was not found.
    #[error("binding not found: variable `{variable}`")]
    BindingNotFound {
        /// The variable name that was looked up.
        variable: String,
    },

    /// An I/O error propagated from the standard library.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Convenience alias for `Result<T, CqelsError>`.
pub type CqelsResult<T> = Result<T, CqelsError>;

// ---------------------------------------------------------------------------
// Parse error detail — structured parse/syntax errors
// ---------------------------------------------------------------------------

/// Describes the kind of parse error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// Grammar/syntax error (malformed input).
    Syntax,
    /// Semantic error (e.g., undefined prefix, duplicate definition).
    Semantic,
    /// A language feature that is not yet supported.
    Unsupported,
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax => write!(f, "syntax error"),
            Self::Semantic => write!(f, "semantic error"),
            Self::Unsupported => write!(f, "unsupported"),
        }
    }
}

/// Structured parse error with optional source-location information.
///
/// Compared to the previous bare-`String` representation this preserves the
/// error *kind* and, where available, the line/column of the offending token.
#[derive(Debug, Error)]
#[error("{kind}: {message}")]
pub struct ParseErrorDetail {
    /// Classification of the parse error.
    pub kind: ParseErrorKind,
    /// Human-readable description.
    pub message: String,
    /// 1-based line number in the query string, if known.
    pub line: Option<usize>,
    /// 1-based column number in the query string, if known.
    pub column: Option<usize>,
}

impl ParseErrorDetail {
    /// Creates a new parse error with the given kind and message.
    pub fn new(kind: ParseErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            line: None,
            column: None,
        }
    }

    /// Attaches optional source location to this error.
    pub fn with_location(mut self, line: usize, column: usize) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }

    /// Shorthand for a syntax error.
    pub fn syntax(message: impl Into<String>) -> Self {
        Self::new(ParseErrorKind::Syntax, message)
    }

    /// Shorthand for a semantic error.
    pub fn semantic(message: impl Into<String>) -> Self {
        Self::new(ParseErrorKind::Semantic, message)
    }

    /// Shorthand for an unsupported-feature error.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ParseErrorKind::Unsupported, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cqels_error_display_parse() {
        let detail = ParseErrorDetail::syntax("unexpected token");
        let err = CqelsError::Parse(detail);
        let msg = err.to_string();
        assert!(msg.contains("parse error"));
        assert!(msg.contains("unexpected token"));
    }

    #[test]
    fn test_cqels_error_display_evaluation() {
        let err = CqelsError::Evaluation {
            message: "type mismatch".into(),
        };
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn test_cqels_error_display_stream() {
        let err = CqelsError::Stream {
            message: "channel closed".into(),
        };
        assert!(err.to_string().contains("channel closed"));
    }

    #[test]
    fn test_cqels_error_display_window() {
        let err = CqelsError::Window {
            message: "invalid window size".into(),
        };
        assert!(err.to_string().contains("invalid window size"));
    }

    #[test]
    fn test_cqels_error_display_join() {
        let err = CqelsError::Join {
            message: "buffer overflow".into(),
        };
        assert!(err.to_string().contains("buffer overflow"));
    }

    #[test]
    fn test_cqels_error_display_reasoning() {
        let err = CqelsError::Reasoning {
            message: "cycle detected".into(),
        };
        assert!(err.to_string().contains("cycle detected"));
    }

    #[test]
    fn test_cqels_error_display_unsupported() {
        let err = CqelsError::UnsupportedOperation {
            operation: "GROUP BY".into(),
        };
        assert!(err.to_string().contains("GROUP BY"));
    }

    #[test]
    fn test_cqels_error_display_invalid_term() {
        let err = CqelsError::InvalidTerm {
            detail: "bad IRI".into(),
        };
        assert!(err.to_string().contains("bad IRI"));
    }

    #[test]
    fn test_cqels_error_display_binding_not_found() {
        let err = CqelsError::BindingNotFound {
            variable: "x".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("x"));
        assert!(msg.contains("binding not found"));
    }

    #[test]
    fn test_cqels_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = CqelsError::from(io_err);
        assert!(matches!(err, CqelsError::Io(_)));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn test_cqels_error_from_parse_detail() {
        let detail = ParseErrorDetail::semantic("undefined prefix");
        let err = CqelsError::from(detail);
        assert!(matches!(err, CqelsError::Parse(_)));
    }

    #[test]
    fn test_parse_error_kind_display() {
        assert_eq!(ParseErrorKind::Syntax.to_string(), "syntax error");
        assert_eq!(ParseErrorKind::Semantic.to_string(), "semantic error");
        assert_eq!(ParseErrorKind::Unsupported.to_string(), "unsupported");
    }

    #[test]
    fn test_parse_error_detail_syntax() {
        let err = ParseErrorDetail::syntax("missing semicolon");
        assert_eq!(err.kind, ParseErrorKind::Syntax);
        assert_eq!(err.message, "missing semicolon");
        assert!(err.line.is_none());
        assert!(err.column.is_none());
    }

    #[test]
    fn test_parse_error_detail_semantic() {
        let err = ParseErrorDetail::semantic("undefined prefix 'foo'");
        assert_eq!(err.kind, ParseErrorKind::Semantic);
    }

    #[test]
    fn test_parse_error_detail_unsupported() {
        let err = ParseErrorDetail::unsupported("LATERAL JOIN");
        assert_eq!(err.kind, ParseErrorKind::Unsupported);
    }

    #[test]
    fn test_parse_error_detail_with_location() {
        let err = ParseErrorDetail::syntax("bad token").with_location(10, 5);
        assert_eq!(err.line, Some(10));
        assert_eq!(err.column, Some(5));
    }

    #[test]
    fn test_parse_error_detail_display() {
        let err = ParseErrorDetail::syntax("unexpected EOF");
        let msg = err.to_string();
        assert!(msg.contains("syntax error"));
        assert!(msg.contains("unexpected EOF"));
    }

    #[test]
    fn test_parse_error_detail_new() {
        let err = ParseErrorDetail::new(ParseErrorKind::Syntax, "test message");
        assert_eq!(err.kind, ParseErrorKind::Syntax);
        assert_eq!(err.message, "test message");
    }

    #[test]
    fn test_cqels_error_debug() {
        let err = CqelsError::Evaluation {
            message: "test".into(),
        };
        let debug = format!("{:?}", err);
        assert!(debug.contains("Evaluation"));
    }

    #[test]
    fn test_parse_error_kind_eq() {
        assert_eq!(ParseErrorKind::Syntax, ParseErrorKind::Syntax);
        assert_ne!(ParseErrorKind::Syntax, ParseErrorKind::Semantic);
    }

    #[test]
    fn test_parse_error_kind_clone_copy() {
        let kind = ParseErrorKind::Unsupported;
        let cloned = kind;
        assert_eq!(kind, cloned);
    }
}
