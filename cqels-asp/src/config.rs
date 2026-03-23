//! Configuration for ASP stream-solving sessions.
//!
//! [`AspStreamSolveConfig`] holds parameters such as model limits, timeout,
//! and a base logic program. Use the [`AspStreamSolveConfigBuilder`] for
//! ergonomic construction.

/// Configuration for an ASP stream-solving session.
///
/// # Examples
///
/// ```
/// use cqels_asp::AspStreamSolveConfig;
///
/// let config = AspStreamSolveConfig::builder()
///     .max_models(5)
///     .timeout_ms(10_000)
///     .base_program("a :- not b.")
///     .build();
///
/// assert_eq!(config.max_models, 5);
/// assert_eq!(config.timeout_ms, 10_000);
/// ```
#[derive(Clone, Debug)]
pub struct AspStreamSolveConfig {
    /// Maximum number of answer sets to enumerate per solve call.
    pub max_models: usize,
    /// Timeout in milliseconds for each solve invocation.
    pub timeout_ms: u64,
    /// The base (static) logic program prepended to every solve call.
    pub base_program: String,
}

impl AspStreamSolveConfig {
    /// Returns a new builder with default values.
    pub fn builder() -> AspStreamSolveConfigBuilder {
        AspStreamSolveConfigBuilder::default()
    }
}

impl Default for AspStreamSolveConfig {
    fn default() -> Self {
        Self {
            max_models: 1,
            timeout_ms: 30_000,
            base_program: String::new(),
        }
    }
}

/// Builder for [`AspStreamSolveConfig`].
///
/// Defaults: `max_models=1`, `timeout_ms=30000`, `base_program=""`.
#[derive(Clone, Debug, Default)]
pub struct AspStreamSolveConfigBuilder {
    max_models: Option<usize>,
    timeout_ms: Option<u64>,
    base_program: Option<String>,
}

impl AspStreamSolveConfigBuilder {
    /// Sets the maximum number of answer sets.
    pub fn max_models(mut self, max_models: usize) -> Self {
        self.max_models = Some(max_models);
        self
    }

    /// Sets the timeout in milliseconds.
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Sets the base logic program.
    pub fn base_program(mut self, program: impl Into<String>) -> Self {
        self.base_program = Some(program.into());
        self
    }

    /// Builds the configuration, using defaults for any unset fields.
    pub fn build(self) -> AspStreamSolveConfig {
        let defaults = AspStreamSolveConfig::default();
        AspStreamSolveConfig {
            max_models: self.max_models.unwrap_or(defaults.max_models),
            timeout_ms: self.timeout_ms.unwrap_or(defaults.timeout_ms),
            base_program: self.base_program.unwrap_or(defaults.base_program),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AspStreamSolveConfig::default();
        assert_eq!(config.max_models, 1);
        assert_eq!(config.timeout_ms, 30_000);
        assert!(config.base_program.is_empty());
    }

    #[test]
    fn test_builder_defaults() {
        let config = AspStreamSolveConfig::builder().build();
        assert_eq!(config.max_models, 1);
        assert_eq!(config.timeout_ms, 30_000);
        assert!(config.base_program.is_empty());
    }

    #[test]
    fn test_builder_custom() {
        let config = AspStreamSolveConfig::builder()
            .max_models(10)
            .timeout_ms(5_000)
            .base_program("edge(a,b).")
            .build();

        assert_eq!(config.max_models, 10);
        assert_eq!(config.timeout_ms, 5_000);
        assert_eq!(config.base_program, "edge(a,b).");
    }

    #[test]
    fn test_builder_partial() {
        let config = AspStreamSolveConfig::builder().max_models(3).build();

        assert_eq!(config.max_models, 3);
        assert_eq!(config.timeout_ms, 30_000); // default
        assert!(config.base_program.is_empty()); // default
    }
}
