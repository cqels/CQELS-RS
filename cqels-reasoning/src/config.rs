use std::fmt;
use std::time::Duration;

use crate::conflict::ConflictResolution;
use crate::rule::RuleSet;

/// Configuration for the reasoning engine.
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

    pub fn max_recursion_depth(mut self, depth: usize) -> Self {
        assert!(depth >= 1, "Max recursion depth must be positive");
        self.max_recursion_depth = depth;
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

    pub fn track_provenance(mut self, track: bool) -> Self {
        self.track_provenance = track;
        self
    }

    pub fn build(self) -> ReasoningConfig {
        ReasoningConfig {
            rule_set: self.rule_set.expect("rule_set is required"),
            default_window: self.default_window,
            enable_recursive_inference: self.enable_recursive_inference,
            max_recursion_depth: self.max_recursion_depth,
            conflict_resolution: self.conflict_resolution,
            emit_input_triples: self.emit_input_triples,
            track_provenance: self.track_provenance,
        }
    }
}
