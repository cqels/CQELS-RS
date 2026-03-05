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
