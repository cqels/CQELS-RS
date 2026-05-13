//! Core VSS signal envelope.
//!
//! Mirrors the COVESA Vehicle Signal Specification (VSS) "datapoint"
//! shape: a dotted path like `Vehicle.Powertrain.FuelSystem.Level`, a
//! typed value, and a millisecond timestamp.

use serde::{Deserialize, Serialize};

use crate::error::CdspError;

/// A single VSS data point at one instant in time.
///
/// VSS paths are dot-separated identifiers rooted at `Vehicle.*`. The
/// crate does not validate the path against a specific VSS release —
/// any non-empty, dot-separated identifier sequence is accepted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VssSignal {
    /// Dotted VSS path, e.g. `"Vehicle.Speed"`.
    pub path: String,
    /// Typed value, see [`VssValue`].
    pub value: VssValue,
    /// Source timestamp in milliseconds since UNIX epoch.
    pub timestamp: i64,
}

impl VssSignal {
    /// Constructs a new signal.
    pub fn new(path: impl Into<String>, value: VssValue, timestamp: i64) -> Self {
        Self {
            path: path.into(),
            value,
            timestamp,
        }
    }

    /// Validates that the signal path is non-empty and contains no
    /// empty dot-separated segments. Returns the validated signal on
    /// success.
    pub fn validated(self) -> Result<Self, CdspError> {
        if self.path.is_empty() {
            return Err(CdspError::InvalidSignalPath("(empty)".into()));
        }
        if self.path.starts_with('.') || self.path.ends_with('.') {
            return Err(CdspError::InvalidSignalPath(self.path));
        }
        if self.path.split('.').any(str::is_empty) {
            return Err(CdspError::InvalidSignalPath(self.path));
        }
        Ok(self)
    }

    /// Returns the deepest path component (e.g. `"Speed"` for
    /// `"Vehicle.Speed"`), or the whole path if it has no dots.
    pub fn leaf(&self) -> &str {
        self.path.rsplit('.').next().unwrap_or(self.path.as_str())
    }
}

/// Typed value carried by a [`VssSignal`].
///
/// Covers the small set of VSS primitive types: boolean, integer,
/// double, and string. Compound/struct values are flattened by VSS
/// itself into multiple paths, so this enum stays narrow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "lowercase")]
#[non_exhaustive]
pub enum VssValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl VssValue {
    /// Renders the value as a plain string suitable for an RDF literal.
    pub fn as_literal_str(&self) -> String {
        match self {
            VssValue::Bool(b) => b.to_string(),
            VssValue::Int(i) => i.to_string(),
            VssValue::Float(f) => f.to_string(),
            VssValue::String(s) => s.clone(),
        }
    }
}

impl From<bool> for VssValue {
    fn from(b: bool) -> Self {
        VssValue::Bool(b)
    }
}

impl From<i64> for VssValue {
    fn from(i: i64) -> Self {
        VssValue::Int(i)
    }
}

impl From<f64> for VssValue {
    fn from(f: f64) -> Self {
        VssValue::Float(f)
    }
}

impl From<String> for VssValue {
    fn from(s: String) -> Self {
        VssValue::String(s)
    }
}

impl From<&str> for VssValue {
    fn from(s: &str) -> Self {
        VssValue::String(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validated_accepts_typical_paths() {
        let s = VssSignal::new("Vehicle.Speed", VssValue::from(72.0), 0);
        assert!(s.validated().is_ok());
    }

    #[test]
    fn validated_rejects_empty_path() {
        let s = VssSignal::new("", VssValue::from(0_i64), 0);
        assert!(matches!(
            s.validated(),
            Err(CdspError::InvalidSignalPath(_))
        ));
    }

    #[test]
    fn validated_rejects_leading_dot() {
        let s = VssSignal::new(".Vehicle.Speed", VssValue::from(0_i64), 0);
        assert!(s.validated().is_err());
    }

    #[test]
    fn validated_rejects_empty_segment() {
        let s = VssSignal::new("Vehicle..Speed", VssValue::from(0_i64), 0);
        assert!(s.validated().is_err());
    }

    #[test]
    fn leaf_returns_last_segment() {
        assert_eq!(
            VssSignal::new(
                "Vehicle.Powertrain.FuelSystem.Level",
                VssValue::from(0_i64),
                0
            )
            .leaf(),
            "Level"
        );
        assert_eq!(
            VssSignal::new("Speed", VssValue::from(0_i64), 0).leaf(),
            "Speed"
        );
    }

    #[test]
    fn as_literal_str_covers_all_variants() {
        assert_eq!(VssValue::Bool(true).as_literal_str(), "true");
        assert_eq!(VssValue::Int(-7).as_literal_str(), "-7");
        assert_eq!(VssValue::Float(0.5).as_literal_str(), "0.5");
        assert_eq!(VssValue::String("hi".into()).as_literal_str(), "hi");
    }

    #[test]
    fn value_serde_roundtrip() {
        let v = VssValue::Float(72.5);
        let json = serde_json::to_string(&v).unwrap();
        let back: VssValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
