use std::collections::{BTreeMap, HashMap, HashSet};

use cqels_model::{Statement, Term};

use crate::pattern::TriplePattern;

/// Single-dimension index mapping a Term value to the set of statements
/// containing that value in a particular position (subject, predicate, or object).
#[derive(Debug, Default)]
pub struct FactIndex {
    index: HashMap<Term, HashSet<Statement>>,
}

impl FactIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, key: Term, stmt: Statement) {
        self.index.entry(key).or_default().insert(stmt);
    }

    pub fn remove(&mut self, key: &Term, stmt: &Statement) {
        if let Some(stmts) = self.index.get_mut(key) {
            stmts.remove(stmt);
            if stmts.is_empty() {
                self.index.remove(key);
            }
        }
    }

    pub fn get(&self, key: &Term) -> Option<&HashSet<Statement>> {
        self.index.get(key)
    }

    pub fn contains_key(&self, key: &Term) -> bool {
        self.index.get(key).map(|s| !s.is_empty()).unwrap_or(false)
    }

    pub fn size(&self) -> usize {
        self.index.values().map(|s| s.len()).sum()
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }
}

/// Working memory stores facts with time-based indexing and subject/predicate/object indexes.
///
/// Supports efficient windowed eviction and pattern-based retrieval.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::WorkingMemory;
/// use cqels_model::{Term, IriTerm, Statement};
///
/// let mut wm = WorkingMemory::new();
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://ex.org/alice")),
///     IriTerm::new("http://ex.org/type"),
///     Term::Iri(IriTerm::new("http://ex.org/Person")),
/// );
/// wm.add_fact(stmt.clone(), 1000);
/// assert_eq!(wm.size(), 1);
/// assert!(wm.contains(&stmt));
///
/// let evicted = wm.evict_before(2000);
/// assert_eq!(evicted.len(), 1);
/// assert!(wm.is_empty());
/// ```
#[derive(Debug)]
pub struct WorkingMemory {
    /// timestamp → set of statements
    time_index: BTreeMap<i64, HashSet<Statement>>,
    subject_index: FactIndex,
    predicate_index: FactIndex,
    object_index: FactIndex,
}

impl WorkingMemory {
    pub fn new() -> Self {
        Self {
            time_index: BTreeMap::new(),
            subject_index: FactIndex::new(),
            predicate_index: FactIndex::new(),
            object_index: FactIndex::new(),
        }
    }

    /// Adds a fact to working memory at the given timestamp.
    ///
    /// Optimized: pre-extracts index keys before cloning the full statement,
    /// saving one redundant key clone per index insertion.
    pub fn add_fact(&mut self, stmt: Statement, timestamp: i64) {
        let subj_key = stmt.subject.clone();
        let pred_key = Term::Iri(stmt.predicate.clone());
        let obj_key = stmt.object.clone();

        self.time_index
            .entry(timestamp)
            .or_default()
            .insert(stmt.clone());
        self.subject_index.add(subj_key, stmt.clone());
        self.predicate_index.add(pred_key, stmt.clone());
        self.object_index.add(obj_key, stmt);
    }

    /// Evicts all facts with timestamp strictly before the watermark.
    /// Returns the evicted statements.
    pub fn evict_before(&mut self, watermark: i64) -> Vec<Statement> {
        let mut evicted = Vec::new();

        // Collect keys to remove (timestamps < watermark)
        let keys_to_remove: Vec<i64> = self
            .time_index
            .range(..watermark)
            .map(|(&k, _)| k)
            .collect();

        for key in keys_to_remove {
            if let Some(stmts) = self.time_index.remove(&key) {
                for stmt in stmts {
                    self.remove_from_indexes(&stmt);
                    evicted.push(stmt);
                }
            }
        }

        evicted
    }

    /// Removes a specific fact from all indexes.
    pub fn remove_fact(&mut self, stmt: &Statement) {
        self.remove_from_indexes(stmt);
        // Also remove from time index
        let keys_to_check: Vec<i64> = self.time_index.keys().copied().collect();
        for key in keys_to_check {
            if let Some(stmts) = self.time_index.get_mut(&key) {
                stmts.remove(stmt);
                if stmts.is_empty() {
                    self.time_index.remove(&key);
                }
            }
        }
    }

    fn remove_from_indexes(&mut self, stmt: &Statement) {
        self.subject_index.remove(&stmt.subject, stmt);
        self.predicate_index
            .remove(&Term::Iri(stmt.predicate.clone()), stmt);
        self.object_index.remove(&stmt.object, stmt);
    }

    /// Returns facts matching a triple pattern, using the most selective index.
    pub fn get_matching_facts(&self, pattern: &TriplePattern) -> Vec<Statement> {
        // Choose the most selective index based on which positions are constants
        let candidates = if pattern.has_constant_subject() {
            if let crate::pattern::PatternTerm::Constant(ref term) = pattern.subject {
                self.subject_index.get(term).cloned().unwrap_or_default()
            } else {
                self.all_facts_set()
            }
        } else if pattern.has_constant_predicate() {
            if let crate::pattern::PatternTerm::Constant(ref term) = pattern.predicate {
                self.predicate_index.get(term).cloned().unwrap_or_default()
            } else {
                self.all_facts_set()
            }
        } else if pattern.has_constant_object() {
            if let crate::pattern::PatternTerm::Constant(ref term) = pattern.object {
                self.object_index.get(term).cloned().unwrap_or_default()
            } else {
                self.all_facts_set()
            }
        } else {
            self.all_facts_set()
        };

        candidates
            .into_iter()
            .filter(|stmt| pattern.matches(stmt))
            .collect()
    }

    fn all_facts_set(&self) -> HashSet<Statement> {
        self.time_index
            .values()
            .flat_map(|s| s.iter().cloned())
            .collect()
    }

    pub fn contains(&self, stmt: &Statement) -> bool {
        self.subject_index
            .get(&stmt.subject)
            .map(|s| s.contains(stmt))
            .unwrap_or(false)
    }

    pub fn all_facts(&self) -> Vec<Statement> {
        self.time_index
            .values()
            .flat_map(|s| s.iter().cloned())
            .collect()
    }

    pub fn size(&self) -> usize {
        self.time_index.values().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn clear(&mut self) {
        self.time_index.clear();
        self.subject_index.clear();
        self.predicate_index.clear();
        self.object_index.clear();
    }
}

impl Default for WorkingMemory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::PatternTerm;
    use cqels_model::term::IriTerm;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    fn stmt(s: &str, p: &str, o: &str) -> Statement {
        Statement::new(iri(s), IriTerm::new(p), iri(o))
    }

    #[test]
    fn test_fact_index_add_get() {
        let mut index = FactIndex::new();
        let s = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        index.add(iri("http://ex.org/a"), s.clone());

        let results = index.get(&iri("http://ex.org/a")).unwrap();
        assert!(results.contains(&s));
        assert_eq!(index.size(), 1);
    }

    #[test]
    fn test_fact_index_remove() {
        let mut index = FactIndex::new();
        let s = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        index.add(iri("http://ex.org/a"), s.clone());
        index.remove(&iri("http://ex.org/a"), &s);
        assert!(!index.contains_key(&iri("http://ex.org/a")));
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_working_memory_add_and_retrieve() {
        let mut wm = WorkingMemory::new();
        let s1 = stmt(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
        );
        let s2 = stmt(
            "http://ex.org/bob",
            "http://ex.org/type",
            "http://ex.org/Person",
        );

        wm.add_fact(s1.clone(), 1000);
        wm.add_fact(s2.clone(), 2000);

        assert_eq!(wm.size(), 2);
        assert!(wm.contains(&s1));
        assert!(wm.contains(&s2));
    }

    #[test]
    fn test_working_memory_evict() {
        let mut wm = WorkingMemory::new();
        let s1 = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        let s2 = stmt("http://ex.org/c", "http://ex.org/p", "http://ex.org/d");
        let s3 = stmt("http://ex.org/e", "http://ex.org/p", "http://ex.org/f");

        wm.add_fact(s1.clone(), 1000);
        wm.add_fact(s2.clone(), 2000);
        wm.add_fact(s3.clone(), 3000);

        let evicted = wm.evict_before(2500);
        assert_eq!(evicted.len(), 2);
        assert!(evicted.contains(&s1));
        assert!(evicted.contains(&s2));
        assert_eq!(wm.size(), 1);
        assert!(wm.contains(&s3));
    }

    #[test]
    fn test_working_memory_pattern_query() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt(
                "http://ex.org/alice",
                "http://ex.org/type",
                "http://ex.org/Person",
            ),
            1000,
        );
        wm.add_fact(
            stmt(
                "http://ex.org/bob",
                "http://ex.org/type",
                "http://ex.org/Person",
            ),
            2000,
        );
        wm.add_fact(
            stmt(
                "http://ex.org/alice",
                "http://ex.org/knows",
                "http://ex.org/bob",
            ),
            3000,
        );

        // Query: ?s rdf:type ex:Person
        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let results = wm.get_matching_facts(&pattern);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_working_memory_clear() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b"),
            1000,
        );
        assert!(!wm.is_empty());
        wm.clear();
        assert!(wm.is_empty());
    }

    #[test]
    fn test_fact_index_clear() {
        let mut index = FactIndex::new();
        index.add(
            iri("http://ex.org/a"),
            stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b"),
        );
        assert_eq!(index.size(), 1);
        index.clear();
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_fact_index_contains_key() {
        let mut index = FactIndex::new();
        let key = iri("http://ex.org/a");
        assert!(!index.contains_key(&key));

        index.add(
            key.clone(),
            stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b"),
        );
        assert!(index.contains_key(&key));
    }

    #[test]
    fn test_fact_index_multiple_stmts_same_key() {
        let mut index = FactIndex::new();
        let key = iri("http://ex.org/a");
        let s1 = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        let s2 = stmt("http://ex.org/a", "http://ex.org/q", "http://ex.org/c");
        index.add(key.clone(), s1);
        index.add(key.clone(), s2);
        assert_eq!(index.size(), 2);
        assert_eq!(index.get(&key).unwrap().len(), 2);
    }

    #[test]
    fn test_fact_index_get_missing() {
        let index = FactIndex::new();
        assert!(index.get(&iri("http://ex.org/missing")).is_none());
    }

    #[test]
    fn test_working_memory_remove_fact() {
        let mut wm = WorkingMemory::new();
        let s1 = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        let s2 = stmt("http://ex.org/c", "http://ex.org/p", "http://ex.org/d");
        wm.add_fact(s1.clone(), 1000);
        wm.add_fact(s2.clone(), 2000);
        assert_eq!(wm.size(), 2);

        wm.remove_fact(&s1);
        assert_eq!(wm.size(), 1);
        assert!(!wm.contains(&s1));
        assert!(wm.contains(&s2));
    }

    #[test]
    fn test_working_memory_all_facts() {
        let mut wm = WorkingMemory::new();
        let s1 = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        let s2 = stmt("http://ex.org/c", "http://ex.org/p", "http://ex.org/d");
        wm.add_fact(s1.clone(), 1000);
        wm.add_fact(s2.clone(), 2000);

        let all = wm.all_facts();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&s1));
        assert!(all.contains(&s2));
    }

    #[test]
    fn test_working_memory_default() {
        let wm = WorkingMemory::default();
        assert!(wm.is_empty());
        assert_eq!(wm.size(), 0);
    }

    #[test]
    fn test_working_memory_pattern_all_variables() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b"),
            1000,
        );
        wm.add_fact(
            stmt("http://ex.org/c", "http://ex.org/q", "http://ex.org/d"),
            2000,
        );

        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let results = wm.get_matching_facts(&pattern);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_working_memory_pattern_constant_object() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt(
                "http://ex.org/alice",
                "http://ex.org/type",
                "http://ex.org/Person",
            ),
            1000,
        );
        wm.add_fact(
            stmt(
                "http://ex.org/bob",
                "http://ex.org/type",
                "http://ex.org/Animal",
            ),
            2000,
        );

        let pattern = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::var("p"),
            PatternTerm::constant(iri("http://ex.org/Person")),
        );
        let results = wm.get_matching_facts(&pattern);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_working_memory_pattern_constant_subject() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt("http://ex.org/alice", "http://ex.org/p1", "http://ex.org/b"),
            1000,
        );
        wm.add_fact(
            stmt("http://ex.org/alice", "http://ex.org/p2", "http://ex.org/c"),
            2000,
        );
        wm.add_fact(
            stmt("http://ex.org/bob", "http://ex.org/p1", "http://ex.org/d"),
            3000,
        );

        let pattern = TriplePattern::new(
            PatternTerm::constant(iri("http://ex.org/alice")),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        let results = wm.get_matching_facts(&pattern);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_working_memory_evict_empty() {
        let mut wm = WorkingMemory::new();
        let evicted = wm.evict_before(5000);
        assert!(evicted.is_empty());
    }

    #[test]
    fn test_working_memory_evict_none_expired() {
        let mut wm = WorkingMemory::new();
        wm.add_fact(
            stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b"),
            5000,
        );
        let evicted = wm.evict_before(1000);
        assert!(evicted.is_empty());
        assert_eq!(wm.size(), 1);
    }

    #[test]
    fn test_working_memory_evict_indexes_consistent() {
        let mut wm = WorkingMemory::new();
        let s1 = stmt("http://ex.org/a", "http://ex.org/p", "http://ex.org/b");
        wm.add_fact(s1.clone(), 1000);
        wm.evict_before(2000);

        // After eviction, the fact should not be found via any index
        assert!(!wm.contains(&s1));
        assert_eq!(wm.size(), 0);

        // Pattern query should also return empty
        let pattern = TriplePattern::new(
            PatternTerm::constant(iri("http://ex.org/a")),
            PatternTerm::var("p"),
            PatternTerm::var("o"),
        );
        assert!(wm.get_matching_facts(&pattern).is_empty());
    }
}
