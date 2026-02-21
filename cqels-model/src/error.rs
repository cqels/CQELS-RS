use thiserror::Error;

/// Unified error type for the CQELS engine.
#[derive(Debug, Error)]
pub enum CqelsError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("evaluation error: {0}")]
    Evaluation(String),

    #[error("stream error: {0}")]
    Stream(String),

    #[error("window error: {0}")]
    Window(String),

    #[error("join error: {0}")]
    Join(String),

    #[error("reasoning error: {0}")]
    Reasoning(String),

    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("invalid term: {0}")]
    InvalidTerm(String),

    #[error("binding not found: {0}")]
    BindingNotFound(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type CqelsResult<T> = Result<T, CqelsError>;
