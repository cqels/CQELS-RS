//! Error types for the ASP solver integration.
//!
//! Provides [`AspError`] -- the error enum for all ASP-related operations
//! including solver invocation, output parsing, codec encoding/decoding,
//! and session management.

use thiserror::Error;

/// Error type for ASP solver operations.
///
/// Covers failures during solver invocation, output parsing,
/// RDF/ASP term codec operations, and incremental session management.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AspError {
    /// The ASP solver executable was not found on the system.
    #[error("ASP solver not found")]
    SolverNotFound,

    /// The ASP solver process exited with an unexpected error.
    #[error("ASP solver failed (exit code {exit_code}): {message}")]
    SolverFailed {
        /// Human-readable description of the failure.
        message: String,
        /// The process exit code.
        exit_code: i32,
    },

    /// Failed to parse the solver output.
    #[error("ASP parse error: {message}")]
    ParseError {
        /// Description of the parse failure.
        message: String,
    },

    /// An error during RDF/ASP term encoding or decoding.
    #[error("ASP codec error: {message}")]
    CodecError {
        /// Description of the codec failure.
        message: String,
    },

    /// An error in the incremental solving session.
    #[error("ASP session error: {message}")]
    SessionError {
        /// Description of the session failure.
        message: String,
    },

    /// An I/O error propagated from the standard library.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_solver_not_found_display() {
        let err = AspError::SolverNotFound;
        assert_eq!(err.to_string(), "ASP solver not found");
    }

    #[test]
    fn test_solver_failed_display() {
        let err = AspError::SolverFailed {
            message: "segfault".into(),
            exit_code: 139,
        };
        let msg = err.to_string();
        assert!(msg.contains("139"));
        assert!(msg.contains("segfault"));
    }

    #[test]
    fn test_io_error_transparent() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err = AspError::from(io_err);
        assert!(matches!(err, AspError::Io(_)));
        assert!(err.to_string().contains("missing"));
    }
}
