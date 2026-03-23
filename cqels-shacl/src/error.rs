//! Error types for SHACL validation operations.

use thiserror::Error;

/// Errors that can occur during SHACL validation and repair operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ShaclError {
    /// A SHACL shape definition could not be parsed.
    #[error("SHACL parse error: {message}")]
    ParseError {
        /// Description of the parse failure.
        message: String,
    },

    /// A SHACL shape could not be compiled to an ASP program.
    #[error("SHACL compilation error: {message}")]
    CompilationError {
        /// Description of the compilation failure.
        message: String,
    },

    /// A validation operation failed.
    #[error("SHACL validation error: {message}")]
    ValidationError {
        /// Description of the validation failure.
        message: String,
    },

    /// An error propagated from the ASP solver.
    #[error(transparent)]
    SolverError(#[from] cqels_asp::AspError),

    /// An I/O error propagated from the standard library.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_display() {
        let err = ShaclError::ParseError {
            message: "unexpected token".into(),
        };
        assert_eq!(err.to_string(), "SHACL parse error: unexpected token");
    }

    #[test]
    fn test_compilation_error_display() {
        let err = ShaclError::CompilationError {
            message: "unsupported constraint".into(),
        };
        assert!(err.to_string().contains("unsupported constraint"));
    }

    #[test]
    fn test_validation_error_display() {
        let err = ShaclError::ValidationError {
            message: "timeout".into(),
        };
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_solver_error_from() {
        let asp_err = cqels_asp::AspError::SolverNotFound;
        let err = ShaclError::from(asp_err);
        assert!(matches!(err, ShaclError::SolverError(_)));
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = ShaclError::from(io_err);
        assert!(matches!(err, ShaclError::Io(_)));
        assert!(err.to_string().contains("file missing"));
    }
}
