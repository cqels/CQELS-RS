//! Memory store abstraction backing the `store_memory` / `recall_memory`
//! / `forget_memory` MCP tools.
//!
//! Each "memory" is a `MemoryFact` keyed by `(namespace, id)`. Namespaces
//! give LLM agents lightweight isolation between scratchpads, contexts,
//! or sessions; `id` is the unique identifier within a namespace.
//!
//! Two implementations are shipped:
//!
//! - [`InMemoryMemoryStore`] — simple `Mutex<HashMap>`-backed store for
//!   tests and ephemeral use.
//! - [`SledMemoryStore`] — durable, on-disk store via [`sled`].
//!
//! Both implement the same [`MemoryStore`] trait, so the MCP tool
//! handlers don't care which is plugged in.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// A single memory entry — the LLM-visible payload plus metadata.
///
/// `content` is intentionally a raw string so callers can pick whatever
/// serialization they want (JSON, Turtle, plain text, etc.) without the
/// store needing to understand it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryFact {
    pub namespace: String,
    pub id: String,
    pub content: String,
    pub created_at_ms: i64,
}

impl MemoryFact {
    pub fn new(
        namespace: impl Into<String>,
        id: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
            content: content.into(),
            created_at_ms: now_ms(),
        }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Errors returned by [`MemoryStore`] implementations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MemoryError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Codec(String),
}

/// Backend-agnostic memory store.
pub trait MemoryStore: Send + Sync {
    /// Inserts or replaces the fact at `(namespace, id)`.
    fn store(&self, fact: MemoryFact) -> Result<(), MemoryError>;

    /// Returns all facts in `namespace` whose `content` contains
    /// `query_substring`. Pass an empty `query_substring` to return
    /// everything in the namespace.
    fn recall(
        &self,
        namespace: &str,
        query_substring: &str,
    ) -> Result<Vec<MemoryFact>, MemoryError>;

    /// Removes the fact at `(namespace, id)` if present. Returns `true`
    /// when a fact was actually removed.
    fn forget(&self, namespace: &str, id: &str) -> Result<bool, MemoryError>;

    /// Number of facts in `namespace`.
    fn len(&self, namespace: &str) -> Result<usize, MemoryError>;
}

// ─── In-memory implementation ────────────────────────────────────────

#[derive(Default)]
pub struct InMemoryMemoryStore {
    inner: RwLock<HashMap<String, HashMap<String, MemoryFact>>>,
}

impl InMemoryMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenient `Arc` wrapper for embedding in a tool handler.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl MemoryStore for InMemoryMemoryStore {
    fn store(&self, fact: MemoryFact) -> Result<(), MemoryError> {
        let mut map = self.inner.write();
        map.entry(fact.namespace.clone())
            .or_default()
            .insert(fact.id.clone(), fact);
        Ok(())
    }

    fn recall(
        &self,
        namespace: &str,
        query_substring: &str,
    ) -> Result<Vec<MemoryFact>, MemoryError> {
        let map = self.inner.read();
        let bucket = match map.get(namespace) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        let mut out: Vec<MemoryFact> = bucket
            .values()
            .filter(|f| query_substring.is_empty() || f.content.contains(query_substring))
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn forget(&self, namespace: &str, id: &str) -> Result<bool, MemoryError> {
        let mut map = self.inner.write();
        if let Some(bucket) = map.get_mut(namespace) {
            return Ok(bucket.remove(id).is_some());
        }
        Ok(false)
    }

    fn len(&self, namespace: &str) -> Result<usize, MemoryError> {
        Ok(self
            .inner
            .read()
            .get(namespace)
            .map(|b| b.len())
            .unwrap_or(0))
    }
}

// ─── sled-backed implementation ──────────────────────────────────────

/// Durable on-disk memory store backed by [`sled`]. Keys are
/// `<namespace>\x00<id>`; values are JSON-encoded [`MemoryFact`]s.
pub struct SledMemoryStore {
    tree: sled::Tree,
}

impl SledMemoryStore {
    /// Opens a sled database at `path` and a dedicated `memory` tree.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryError> {
        let db = sled::open(path).map_err(|e| MemoryError::Storage(e.to_string()))?;
        let tree = db
            .open_tree("memory")
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        Ok(Self { tree })
    }
}

fn key_bytes(namespace: &str, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(namespace.len() + id.len() + 1);
    k.extend_from_slice(namespace.as_bytes());
    k.push(0u8);
    k.extend_from_slice(id.as_bytes());
    k
}

fn namespace_prefix(namespace: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(namespace.len() + 1);
    k.extend_from_slice(namespace.as_bytes());
    k.push(0u8);
    k
}

impl MemoryStore for SledMemoryStore {
    fn store(&self, fact: MemoryFact) -> Result<(), MemoryError> {
        let key = key_bytes(&fact.namespace, &fact.id);
        let value = serde_json::to_vec(&fact).map_err(|e| MemoryError::Codec(e.to_string()))?;
        self.tree
            .insert(key, value)
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        Ok(())
    }

    fn recall(
        &self,
        namespace: &str,
        query_substring: &str,
    ) -> Result<Vec<MemoryFact>, MemoryError> {
        let prefix = namespace_prefix(namespace);
        let mut out = Vec::new();
        for entry in self.tree.scan_prefix(&prefix) {
            let (_k, v) = entry.map_err(|e| MemoryError::Storage(e.to_string()))?;
            let fact: MemoryFact =
                serde_json::from_slice(&v).map_err(|e| MemoryError::Codec(e.to_string()))?;
            if query_substring.is_empty() || fact.content.contains(query_substring) {
                out.push(fact);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn forget(&self, namespace: &str, id: &str) -> Result<bool, MemoryError> {
        let key = key_bytes(namespace, id);
        let removed = self
            .tree
            .remove(key)
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        Ok(removed.is_some())
    }

    fn len(&self, namespace: &str) -> Result<usize, MemoryError> {
        let prefix = namespace_prefix(namespace);
        let mut count = 0usize;
        for entry in self.tree.scan_prefix(&prefix) {
            entry.map_err(|e| MemoryError::Storage(e.to_string()))?;
            count += 1;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn smoke<S: MemoryStore>(store: &S) {
        store
            .store(MemoryFact::new("default", "k1", "alpha bravo"))
            .unwrap();
        store
            .store(MemoryFact::new("default", "k2", "bravo charlie"))
            .unwrap();
        store
            .store(MemoryFact::new("other", "k1", "delta"))
            .unwrap();

        assert_eq!(store.len("default").unwrap(), 2);
        assert_eq!(store.len("other").unwrap(), 1);
        assert_eq!(store.len("missing").unwrap(), 0);

        let all = store.recall("default", "").unwrap();
        assert_eq!(all.len(), 2);

        let bravo = store.recall("default", "bravo").unwrap();
        assert_eq!(bravo.len(), 2);

        let charlie = store.recall("default", "charlie").unwrap();
        assert_eq!(charlie.len(), 1);
        assert_eq!(charlie[0].id, "k2");

        assert!(store.forget("default", "k1").unwrap());
        assert!(
            !store.forget("default", "k1").unwrap(),
            "second forget is a no-op"
        );
        assert_eq!(store.len("default").unwrap(), 1);

        // store overwrites
        store
            .store(MemoryFact::new("default", "k2", "rewritten"))
            .unwrap();
        let after = store.recall("default", "rewritten").unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].content, "rewritten");
    }

    #[test]
    fn in_memory_store_passes_smoke_test() {
        let store = InMemoryMemoryStore::new();
        smoke(&store);
    }

    #[test]
    fn sled_store_passes_smoke_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SledMemoryStore::open(dir.path()).expect("open sled");
        smoke(&store);
    }

    #[test]
    fn sled_store_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = SledMemoryStore::open(dir.path()).unwrap();
            store
                .store(MemoryFact::new("session", "x", "persisted"))
                .unwrap();
        }
        let store2 = SledMemoryStore::open(dir.path()).unwrap();
        let facts = store2.recall("session", "").unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].content, "persisted");
    }
}
