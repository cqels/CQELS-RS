use std::fmt;
use std::time::Duration;

use crate::conflict::ConflictResolution;
use crate::rule::RuleSet;

/// Configuration for the reasoning engine.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use cqels_reasoning::{ReasoningConfig, RuleSet, ConflictResolution};
///
/// let config = ReasoningConfig::builder()
///     .rule_set(RuleSet::new(vec![]))
///     .default_window(Duration::from_secs(60))
///     .conflict_resolution(ConflictResolution::All)
///     .enable_recursive_inference(true)
///     .max_recursion_depth(5)
///     .build();
///
/// assert_eq!(config.default_window(), Duration::from_secs(60));
/// assert!(config.enable_recursive_inference());
/// ```
pub struct ReasoningConfig {
    pub(crate) rule_set: RuleSet,
    pub(crate) default_window: Duration,
    pub(crate) enable_recursive_inference: bool,
    pub(crate) max_recursion_depth: usize,
    pub(crate) conflict_resolution: ConflictResolution,
    pub(crate) emit_input_triples: bool,
    pub(crate) track_provenance: bool,
}

impl ReasoningConfig {
    pub fn rule_set(&self) -> &RuleSet {
        &self.rule_set
    }

    pub fn default_window(&self) -> Duration {
        self.default_window
    }

    pub fn enable_recursive_inference(&self) -> bool {
        self.enable_recursive_inference
    }

    pub fn max_recursion_depth(&self) -> usize {
        self.max_recursion_depth
    }

    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.conflict_resolution
    }

    pub fn emit_input_triples(&self) -> bool {
        self.emit_input_triples
    }

    /// Returns whether provenance tracking is enabled.
    ///
    /// When enabled, provenance data is only available via the low-level
    /// `ReteStreamOperator::process()` API, not through `CqelsRuntime`.
    pub fn track_provenance(&self) -> bool {
        self.track_provenance
    }

    pub fn default_config(rule_set: RuleSet) -> Self {
        Self::builder().rule_set(rule_set).build()
    }

    pub fn builder() -> ReasoningConfigBuilder {
        ReasoningConfigBuilder::default()
    }
}

impl fmt::Debug for ReasoningConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReasoningConfig")
            .field("rules", &self.rule_set.size())
            .field("window", &self.default_window)
            .field("recursive", &self.enable_recursive_inference)
            .field("max_depth", &self.max_recursion_depth)
            .field("conflict", &self.conflict_resolution)
            .field("emit_input", &self.emit_input_triples)
            .field("provenance", &self.track_provenance)
            .finish()
    }
}

/// Builder for `ReasoningConfig`.
pub struct ReasoningConfigBuilder {
    rule_set: Option<RuleSet>,
    default_window: Duration,
    enable_recursive_inference: bool,
    max_recursion_depth: usize,
    conflict_resolution: ConflictResolution,
    emit_input_triples: bool,
    track_provenance: bool,
}

impl Default for ReasoningConfigBuilder {
    fn default() -> Self {
        Self {
            rule_set: None,
            default_window: Duration::from_secs(300), // 5 minutes
            enable_recursive_inference: false,
            max_recursion_depth: 10,
            conflict_resolution: ConflictResolution::Priority,
            emit_input_triples: true,
            track_provenance: false,
        }
    }
}

impl ReasoningConfigBuilder {
    pub fn rule_set(mut self, rule_set: RuleSet) -> Self {
        self.rule_set = Some(rule_set);
        self
    }

    pub fn default_window(mut self, window: Duration) -> Self {
        self.default_window = window;
        self
    }

    pub fn enable_recursive_inference(mut self, enable: bool) -> Self {
        self.enable_recursive_inference = enable;
        self
    }

    /// Sets the maximum recursion depth for recursive inference.
    ///
    /// The depth must be at least 1. Values less than 1 are silently clamped
    /// to 1 to prevent misconfiguration.
    pub fn max_recursion_depth(mut self, depth: usize) -> Self {
        self.max_recursion_depth = depth.max(1);
        self
    }

    pub fn conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    pub fn emit_input_triples(mut self, emit: bool) -> Self {
        self.emit_input_triples = emit;
        self
    }

    /// Enables provenance tracking on inferred elements.
    ///
    /// When enabled, `InferredRdfStreamElement::derived_from` is populated
    /// with the premises that triggered each rule. **Note:** provenance data
    /// is only available via the low-level `ReteStreamOperator::process()`
    /// API. The high-level `ReteStreamOperator::apply()` and `CqelsRuntime`
    /// APIs convert inferred elements to plain `RdfStreamElement`, discarding
    /// provenance metadata.
    pub fn track_provenance(mut self, track: bool) -> Self {
        self.track_provenance = track;
        self
    }

    /// Builds the `ReasoningConfig`, returning an error if required fields are missing.
    pub fn try_build(self) -> Result<ReasoningConfig, cqels_model::CqelsError> {
        let rule_set = self
            .rule_set
            .ok_or_else(|| cqels_model::CqelsError::Evaluation {
                message: "rule_set is required".to_string(),
            })?;
        Ok(ReasoningConfig {
            rule_set,
            default_window: self.default_window,
            enable_recursive_inference: self.enable_recursive_inference,
            max_recursion_depth: self.max_recursion_depth,
            conflict_resolution: self.conflict_resolution,
            emit_input_triples: self.emit_input_triples,
            track_provenance: self.track_provenance,
        })
    }

    /// Builds the `ReasoningConfig`, panicking if required fields are missing.
    ///
    /// Prefer [`try_build()`](Self::try_build) for fallible construction.
    pub fn build(self) -> ReasoningConfig {
        self.try_build().expect("rule_set is required")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{PatternTerm, TriplePattern, TripleTemplate};
    use crate::rule::{Rule, RuleSet};

    fn make_rule_set() -> RuleSet {
        let rule = Rule::builder()
            .id("test-rule")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .build();
        RuleSet::new(vec![rule])
    }

    #[test]
    fn test_builder_defaults() {
        let config = ReasoningConfig::builder().rule_set(make_rule_set()).build();
        assert_eq!(config.default_window(), Duration::from_secs(300));
        assert!(!config.enable_recursive_inference());
        assert_eq!(config.max_recursion_depth(), 10);
        assert!(config.emit_input_triples());
        assert!(!config.track_provenance());
    }

    #[test]
    fn test_try_build_missing_rule_set() {
        let result = ReasoningConfig::builder().try_build();
        assert!(result.is_err());
    }

    #[test]
    fn test_try_build_success() {
        let result = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_max_recursion_depth_clamped_to_one() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .max_recursion_depth(0)
            .build();
        assert_eq!(config.max_recursion_depth(), 1);
    }

    #[test]
    fn test_max_recursion_depth_normal() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .max_recursion_depth(5)
            .build();
        assert_eq!(config.max_recursion_depth(), 5);
    }

    #[test]
    fn test_builder_custom_window() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .default_window(Duration::from_secs(60))
            .build();
        assert_eq!(config.default_window(), Duration::from_secs(60));
    }

    #[test]
    fn test_builder_recursive_inference() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .enable_recursive_inference(true)
            .build();
        assert!(config.enable_recursive_inference());
    }

    #[test]
    fn test_builder_conflict_resolution() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .conflict_resolution(ConflictResolution::All)
            .build();
        assert_eq!(config.conflict_resolution(), ConflictResolution::All);
    }

    #[test]
    fn test_builder_emit_input_triples() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .emit_input_triples(false)
            .build();
        assert!(!config.emit_input_triples());
    }

    #[test]
    fn test_builder_track_provenance() {
        let config = ReasoningConfig::builder()
            .rule_set(make_rule_set())
            .track_provenance(true)
            .build();
        assert!(config.track_provenance());
    }

    #[test]
    fn test_default_config() {
        let config = ReasoningConfig::default_config(make_rule_set());
        assert_eq!(config.default_window(), Duration::from_secs(300));
        assert_eq!(config.max_recursion_depth(), 10);
    }

    #[test]
    fn test_debug_format() {
        let config = ReasoningConfig::builder().rule_set(make_rule_set()).build();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ReasoningConfig"));
        assert!(debug.contains("rules"));
        assert!(debug.contains("window"));
    }

    #[test]
    fn test_rule_set_accessor() {
        let rs = make_rule_set();
        let size = rs.size();
        let config = ReasoningConfig::builder().rule_set(rs).build();
        assert_eq!(config.rule_set().size(), size);
    }
}
