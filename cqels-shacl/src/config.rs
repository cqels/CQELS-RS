//! Configuration for the SHACL stream-solve pipeline.
//!
//! [`ShaclStreamSolveConfig`] controls validation behaviour including
//! whether repair search is enabled, model limits, and solver timeouts.
//! Use [`ShaclStreamSolveConfigBuilder`] for ergonomic construction.

/// Configuration for the SHACL validation and repair pipeline.
#[derive(Clone, Debug)]
pub struct ShaclStreamSolveConfig {
    /// Whether to search for repair candidates when violations are found.
    pub enable_repair_search: bool,
    /// Maximum number of answer-set models to enumerate during repair search.
    pub max_candidate_models: usize,
    /// Number of top repair candidates to return.
    pub top_repair_candidates: usize,
    /// Timeout for the ASP solver in milliseconds.
    pub solver_timeout_ms: u64,
}

impl Default for ShaclStreamSolveConfig {
    fn default() -> Self {
        Self {
            enable_repair_search: false,
            max_candidate_models: 5,
            top_repair_candidates: 3,
            solver_timeout_ms: 30000,
        }
    }
}

impl ShaclStreamSolveConfig {
    /// Returns a builder for constructing a configuration.
    pub fn builder() -> ShaclStreamSolveConfigBuilder {
        ShaclStreamSolveConfigBuilder::default()
    }
}

/// Builder for [`ShaclStreamSolveConfig`].
#[derive(Clone, Debug, Default)]
pub struct ShaclStreamSolveConfigBuilder {
    enable_repair_search: Option<bool>,
    max_candidate_models: Option<usize>,
    top_repair_candidates: Option<usize>,
    solver_timeout_ms: Option<u64>,
}

impl ShaclStreamSolveConfigBuilder {
    /// Sets whether repair search is enabled.
    pub fn enable_repair_search(mut self, enable: bool) -> Self {
        self.enable_repair_search = Some(enable);
        self
    }

    /// Sets the maximum number of candidate models.
    pub fn max_candidate_models(mut self, max: usize) -> Self {
        self.max_candidate_models = Some(max);
        self
    }

    /// Sets the number of top repair candidates to return.
    pub fn top_repair_candidates(mut self, top: usize) -> Self {
        self.top_repair_candidates = Some(top);
        self
    }

    /// Sets the solver timeout in milliseconds.
    pub fn solver_timeout_ms(mut self, ms: u64) -> Self {
        self.solver_timeout_ms = Some(ms);
        self
    }

    /// Builds the configuration, using defaults for any unset fields.
    pub fn build(self) -> ShaclStreamSolveConfig {
        let defaults = ShaclStreamSolveConfig::default();
        ShaclStreamSolveConfig {
            enable_repair_search: self
                .enable_repair_search
                .unwrap_or(defaults.enable_repair_search),
            max_candidate_models: self
                .max_candidate_models
                .unwrap_or(defaults.max_candidate_models),
            top_repair_candidates: self
                .top_repair_candidates
                .unwrap_or(defaults.top_repair_candidates),
            solver_timeout_ms: self.solver_timeout_ms.unwrap_or(defaults.solver_timeout_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = ShaclStreamSolveConfig::default();
        assert!(!cfg.enable_repair_search);
        assert_eq!(cfg.max_candidate_models, 5);
        assert_eq!(cfg.top_repair_candidates, 3);
        assert_eq!(cfg.solver_timeout_ms, 30000);
    }

    #[test]
    fn test_builder_all_fields() {
        let cfg = ShaclStreamSolveConfig::builder()
            .enable_repair_search(true)
            .max_candidate_models(10)
            .top_repair_candidates(5)
            .solver_timeout_ms(60000)
            .build();
        assert!(cfg.enable_repair_search);
        assert_eq!(cfg.max_candidate_models, 10);
        assert_eq!(cfg.top_repair_candidates, 5);
        assert_eq!(cfg.solver_timeout_ms, 60000);
    }

    #[test]
    fn test_builder_partial() {
        let cfg = ShaclStreamSolveConfig::builder()
            .enable_repair_search(true)
            .build();
        assert!(cfg.enable_repair_search);
        // Other fields should have defaults.
        assert_eq!(cfg.max_candidate_models, 5);
        assert_eq!(cfg.top_repair_candidates, 3);
        assert_eq!(cfg.solver_timeout_ms, 30000);
    }

    #[test]
    fn test_builder_from_config() {
        let cfg = ShaclStreamSolveConfig::builder()
            .solver_timeout_ms(1000)
            .build();
        assert_eq!(cfg.solver_timeout_ms, 1000);
        assert!(!cfg.enable_repair_search);
    }

    #[test]
    fn test_config_clone() {
        let cfg = ShaclStreamSolveConfig::builder()
            .max_candidate_models(7)
            .build();
        let cloned = cfg.clone();
        assert_eq!(cloned.max_candidate_models, 7);
    }
}
