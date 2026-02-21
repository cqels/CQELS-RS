use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::value::Value;
use crate::CqelsError;

/// A set of variable bindings with an associated timestamp.
///
/// Maps to the result row concept in SPARQL query evaluation.
/// Each binding maps a variable name to a `Value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BindingSet {
    bindings: HashMap<String, Value>,
    timestamp: i64,
}

impl BindingSet {
    /// Creates a new empty binding set with the given timestamp.
    pub fn new(timestamp: i64) -> Self {
        Self {
            bindings: HashMap::new(),
            timestamp,
        }
    }

    /// Creates a binding set from an existing map.
    pub fn from_map(bindings: HashMap<String, Value>, timestamp: i64) -> Self {
        Self {
            bindings,
            timestamp,
        }
    }

    /// Inserts a binding for the given variable.
    pub fn insert(&mut self, variable: impl Into<String>, value: Value) {
        self.bindings.insert(variable.into(), value);
    }

    /// Gets the value for the given variable, if bound.
    pub fn get(&self, variable: &str) -> Option<&Value> {
        self.bindings.get(variable)
    }

    /// Gets the value for the given variable, returning an error if not bound.
    pub fn get_required(&self, variable: &str) -> Result<&Value, CqelsError> {
        self.bindings
            .get(variable)
            .ok_or_else(|| CqelsError::BindingNotFound(variable.to_string()))
    }

    /// Returns `true` if the given variable is bound.
    pub fn contains(&self, variable: &str) -> bool {
        self.bindings.contains_key(variable)
    }

    /// Returns the number of bindings.
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Returns `true` if no bindings exist.
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Returns the timestamp associated with this binding set.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Sets the timestamp.
    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    /// Returns an iterator over variable names.
    pub fn variables(&self) -> impl Iterator<Item = &str> {
        self.bindings.keys().map(|s| s.as_str())
    }

    /// Returns an iterator over (variable, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.bindings.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns the underlying map.
    pub fn as_map(&self) -> &HashMap<String, Value> {
        &self.bindings
    }

    /// Consumes this binding set and returns the underlying map.
    pub fn into_map(self) -> HashMap<String, Value> {
        self.bindings
    }

    /// Merges another binding set into this one.
    /// If a variable is already bound, it is overwritten.
    pub fn merge(&mut self, other: &BindingSet) {
        for (k, v) in &other.bindings {
            self.bindings.insert(k.clone(), v.clone());
        }
    }

    /// Returns `true` if this binding set is compatible with another
    /// (i.e., shared variables have equal values).
    pub fn is_compatible(&self, other: &BindingSet) -> bool {
        for (k, v) in &self.bindings {
            if let Some(other_v) = other.bindings.get(k) {
                if v != other_v {
                    return false;
                }
            }
        }
        true
    }

    /// Joins this binding set with another, producing a merged set
    /// if compatible, or `None` if incompatible.
    pub fn join(&self, other: &BindingSet) -> Option<BindingSet> {
        if !self.is_compatible(other) {
            return None;
        }
        let mut result = self.clone();
        result.merge(other);
        result.timestamp = self.timestamp.max(other.timestamp);
        Some(result)
    }
}

impl fmt::Display for BindingSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BindingSet[ts={}", self.timestamp)?;
        for (k, v) in &self.bindings {
            write!(f, ", ?{k}={v}")?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binding_set_creation() {
        let bs = BindingSet::new(1000);
        assert!(bs.is_empty());
        assert_eq!(bs.len(), 0);
        assert_eq!(bs.timestamp(), 1000);
    }

    #[test]
    fn test_binding_set_insert_get() {
        let mut bs = BindingSet::new(1000);
        bs.insert("x", Value::Integer(42));
        bs.insert("name", Value::String("Alice".into()));

        assert_eq!(bs.len(), 2);
        assert!(bs.contains("x"));
        assert!(!bs.contains("y"));
        assert_eq!(bs.get("x"), Some(&Value::Integer(42)));
        assert_eq!(bs.get("y"), None);
    }

    #[test]
    fn test_binding_set_get_required() {
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(1));

        assert!(bs.get_required("x").is_ok());
        assert!(bs.get_required("missing").is_err());
    }

    #[test]
    fn test_binding_set_merge() {
        let mut bs1 = BindingSet::new(100);
        bs1.insert("x", Value::Integer(1));

        let mut bs2 = BindingSet::new(200);
        bs2.insert("y", Value::Integer(2));
        bs2.insert("x", Value::Integer(99)); // will overwrite

        bs1.merge(&bs2);
        assert_eq!(bs1.get("x"), Some(&Value::Integer(99)));
        assert_eq!(bs1.get("y"), Some(&Value::Integer(2)));
    }

    #[test]
    fn test_binding_set_compatibility() {
        let mut bs1 = BindingSet::new(0);
        bs1.insert("x", Value::Integer(1));
        bs1.insert("y", Value::Integer(2));

        let mut bs2 = BindingSet::new(0);
        bs2.insert("x", Value::Integer(1));
        bs2.insert("z", Value::Integer(3));

        assert!(bs1.is_compatible(&bs2));

        let mut bs3 = BindingSet::new(0);
        bs3.insert("x", Value::Integer(999));

        assert!(!bs1.is_compatible(&bs3));
    }

    #[test]
    fn test_binding_set_join() {
        let mut bs1 = BindingSet::new(100);
        bs1.insert("x", Value::Integer(1));
        bs1.insert("y", Value::Integer(2));

        let mut bs2 = BindingSet::new(200);
        bs2.insert("x", Value::Integer(1));
        bs2.insert("z", Value::Integer(3));

        let joined = bs1.join(&bs2).unwrap();
        assert_eq!(joined.get("x"), Some(&Value::Integer(1)));
        assert_eq!(joined.get("y"), Some(&Value::Integer(2)));
        assert_eq!(joined.get("z"), Some(&Value::Integer(3)));
        assert_eq!(joined.timestamp(), 200);
    }

    #[test]
    fn test_binding_set_join_incompatible() {
        let mut bs1 = BindingSet::new(0);
        bs1.insert("x", Value::Integer(1));

        let mut bs2 = BindingSet::new(0);
        bs2.insert("x", Value::Integer(2));

        assert!(bs1.join(&bs2).is_none());
    }

    #[test]
    fn test_binding_set_from_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Integer(10));
        map.insert("b".to_string(), Value::String("test".into()));

        let bs = BindingSet::from_map(map, 500);
        assert_eq!(bs.len(), 2);
        assert_eq!(bs.timestamp(), 500);
    }

    #[test]
    fn test_binding_set_display() {
        let mut bs = BindingSet::new(42);
        bs.insert("x", Value::Integer(1));
        let display = bs.to_string();
        assert!(display.contains("ts=42"));
        assert!(display.contains("?x=1"));
    }

    #[test]
    fn test_binding_set_variables() {
        let mut bs = BindingSet::new(0);
        bs.insert("a", Value::Null);
        bs.insert("b", Value::Null);

        let vars: Vec<&str> = bs.variables().collect();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&"a"));
        assert!(vars.contains(&"b"));
    }
}
