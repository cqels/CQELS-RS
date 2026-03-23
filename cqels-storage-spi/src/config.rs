//! Backend configuration.

use std::collections::HashMap;

/// Configuration for a storage backend.
///
/// Carries an arbitrary set of string properties and a flag indicating
/// whether mirror backends should use strict or lenient failure mode.
#[derive(Clone, Debug, Default)]
pub struct BackendConfig {
    /// Key-value properties passed to the backend provider.
    pub properties: HashMap<String, String>,
    /// When `true`, mirror write failures are propagated as errors.
    /// When `false`, mirror failures are logged but ignored.
    pub strict_mirroring: bool,
}

impl BackendConfig {
    /// Creates an empty configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a builder for fluent construction.
    pub fn builder() -> BackendConfigBuilder {
        BackendConfigBuilder::default()
    }
}

/// Builder for [`BackendConfig`].
#[derive(Default)]
pub struct BackendConfigBuilder {
    properties: HashMap<String, String>,
    strict_mirroring: bool,
}

impl BackendConfigBuilder {
    /// Adds a property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets the strict mirroring flag.
    pub fn strict_mirroring(mut self, strict: bool) -> Self {
        self.strict_mirroring = strict;
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> BackendConfig {
        BackendConfig {
            properties: self.properties,
            strict_mirroring: self.strict_mirroring,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let config = BackendConfig::builder()
            .property("path", "/tmp/data")
            .strict_mirroring(true)
            .build();
        assert_eq!(config.properties.get("path").unwrap(), "/tmp/data");
        assert!(config.strict_mirroring);
    }
}
