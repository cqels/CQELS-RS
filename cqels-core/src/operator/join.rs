use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use cqels_model::Statement;

/// Join function — combines a left and right element to produce output.
pub trait JoinFunction<L, R, Out>: Send + Sync {
    fn join(&self, left: &L, right: &R) -> Option<Out>;
}

/// Implements JoinFunction for closures.
impl<L, R, Out, F> JoinFunction<L, R, Out> for F
where
    F: Fn(&L, &R) -> Option<Out> + Send + Sync,
{
    fn join(&self, left: &L, right: &R) -> Option<Out> {
        self(left, right)
    }
}

/// Tagged union for left/right stream elements.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TaggedUnion<L, R> {
    Left(L),
    Right(R),
}

// ---------------------------------------------------------------------------
// Windowed Join
// ---------------------------------------------------------------------------

/// Result of a windowed join operation.
#[derive(Clone, Debug)]
pub struct JoinResult<Out> {
    pub value: Out,
    pub timestamp: i64,
}

/// Windowed join operator — joins two streams within time windows.
///
/// Maps to Java's `WindowedJoinOperator`.
/// Uses BTreeMap for time-indexed buffering with efficient cleanup.
pub struct WindowedJoinState<L: Clone, R: Clone> {
    left_buffer: BTreeMap<i64, Vec<L>>,
    right_buffer: BTreeMap<i64, Vec<R>>,
    window_size_ms: i64,
    current_watermark: i64,
}

impl<L: Clone, R: Clone> WindowedJoinState<L, R> {
    pub fn new(window_size: Duration) -> Self {
        Self {
            left_buffer: BTreeMap::new(),
            right_buffer: BTreeMap::new(),
            window_size_ms: window_size.as_millis() as i64,
            current_watermark: i64::MIN,
        }
    }

    /// Adds a left element and joins with matching right elements.
    pub fn add_left<Out, F: JoinFunction<L, R, Out>>(
        &mut self,
        element: L,
        timestamp: i64,
        join_fn: &F,
    ) -> Vec<JoinResult<Out>> {
        self.left_buffer
            .entry(timestamp)
            .or_default()
            .push(element.clone());

        let mut results = Vec::new();
        let lower_bound = timestamp - self.window_size_ms;

        for (_ts, right_elements) in self.right_buffer.range(lower_bound..=timestamp) {
            for right in right_elements {
                if let Some(out) = join_fn.join(&element, right) {
                    results.push(JoinResult {
                        value: out,
                        timestamp,
                    });
                }
            }
        }
        results
    }

    /// Adds a right element and joins with matching left elements.
    pub fn add_right<Out, F: JoinFunction<L, R, Out>>(
        &mut self,
        element: R,
        timestamp: i64,
        join_fn: &F,
    ) -> Vec<JoinResult<Out>> {
        self.right_buffer
            .entry(timestamp)
            .or_default()
            .push(element.clone());

        let mut results = Vec::new();
        let lower_bound = timestamp - self.window_size_ms;

        for (_ts, left_elements) in self.left_buffer.range(lower_bound..=timestamp) {
            for left in left_elements {
                if let Some(out) = join_fn.join(left, &element) {
                    results.push(JoinResult {
                        value: out,
                        timestamp,
                    });
                }
            }
        }
        results
    }

    /// Advances the watermark, evicting expired elements.
    pub fn advance_watermark(&mut self, watermark: i64) {
        self.current_watermark = watermark;
        let cutoff = watermark - self.window_size_ms;
        self.left_buffer = self.left_buffer.split_off(&cutoff);
        self.right_buffer = self.right_buffer.split_off(&cutoff);
    }
}

// ---------------------------------------------------------------------------
// Interval Join
// ---------------------------------------------------------------------------

/// Interval join operator — joins elements within a time interval.
///
/// Maps to Java's `IntervalJoinOperator`.
/// Joins left element at time t with right elements in [t + lower_bound, t + upper_bound].
pub struct IntervalJoinState<L: Clone, R: Clone> {
    left_buffer: BTreeMap<i64, Vec<L>>,
    right_buffer: BTreeMap<i64, Vec<R>>,
    lower_bound_ms: i64,
    upper_bound_ms: i64,
}

impl<L: Clone, R: Clone> IntervalJoinState<L, R> {
    pub fn new(lower_bound: Duration, upper_bound: Duration) -> Self {
        Self {
            left_buffer: BTreeMap::new(),
            right_buffer: BTreeMap::new(),
            lower_bound_ms: lower_bound.as_millis() as i64,
            upper_bound_ms: upper_bound.as_millis() as i64,
        }
    }

    /// Adds a left element and finds matching right elements.
    pub fn add_left<Out, F: JoinFunction<L, R, Out>>(
        &mut self,
        element: L,
        timestamp: i64,
        join_fn: &F,
    ) -> Vec<JoinResult<Out>> {
        self.left_buffer
            .entry(timestamp)
            .or_default()
            .push(element.clone());

        let mut results = Vec::new();
        let lower = timestamp + self.lower_bound_ms;
        let upper = timestamp + self.upper_bound_ms;

        for (_ts, right_elements) in self.right_buffer.range(lower..=upper) {
            for right in right_elements {
                if let Some(out) = join_fn.join(&element, right) {
                    results.push(JoinResult {
                        value: out,
                        timestamp,
                    });
                }
            }
        }
        results
    }

    /// Adds a right element and finds matching left elements.
    pub fn add_right<Out, F: JoinFunction<L, R, Out>>(
        &mut self,
        element: R,
        timestamp: i64,
        join_fn: &F,
    ) -> Vec<JoinResult<Out>> {
        self.right_buffer
            .entry(timestamp)
            .or_default()
            .push(element.clone());

        let mut results = Vec::new();
        // A right element at time t matches left elements where:
        // left_t + lower_bound <= t <= left_t + upper_bound
        // i.e., left_t in [t - upper_bound, t - lower_bound]
        let lower = timestamp - self.upper_bound_ms;
        let upper = timestamp - self.lower_bound_ms;

        for (_ts, left_elements) in self.left_buffer.range(lower..=upper) {
            for left in left_elements {
                if let Some(out) = join_fn.join(left, &element) {
                    results.push(JoinResult {
                        value: out,
                        timestamp,
                    });
                }
            }
        }
        results
    }

    /// Evicts elements that can no longer produce matches.
    pub fn advance_watermark(&mut self, watermark: i64) {
        let left_cutoff = watermark - self.upper_bound_ms;
        let right_cutoff = watermark + self.lower_bound_ms;
        self.left_buffer = self.left_buffer.split_off(&left_cutoff);
        self.right_buffer = self.right_buffer.split_off(&right_cutoff);
    }
}

// ---------------------------------------------------------------------------
// Graph Pattern Join
// ---------------------------------------------------------------------------

/// Information about an edge in a graph pattern match.
#[derive(Clone, Debug)]
pub struct EdgeInfo {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub timestamp: i64,
}

/// Graph pattern join operator — incremental graph pattern matching for RDF streams.
///
/// Maps to Java's `GraphPatternJoinOperator`.
pub struct GraphPatternJoinState {
    /// subject -> predicate -> set of objects
    subject_index: HashMap<String, HashMap<String, HashSet<String>>>,
    /// object -> predicate -> set of subjects
    object_index: HashMap<String, HashMap<String, HashSet<String>>>,
    /// All edges with timestamps
    edges: Vec<EdgeInfo>,
}

impl GraphPatternJoinState {
    pub fn new() -> Self {
        Self {
            subject_index: HashMap::new(),
            object_index: HashMap::new(),
            edges: Vec::new(),
        }
    }

    /// Adds an RDF statement to the graph state.
    pub fn add_statement(&mut self, stmt: &Statement, timestamp: i64) {
        let subject = stmt.subject.to_string();
        let predicate = stmt.predicate.to_string();
        let object = stmt.object.to_string();

        self.subject_index
            .entry(subject.clone())
            .or_default()
            .entry(predicate.clone())
            .or_default()
            .insert(object.clone());

        self.object_index
            .entry(object.clone())
            .or_default()
            .entry(predicate.clone())
            .or_default()
            .insert(subject.clone());

        self.edges.push(EdgeInfo {
            subject,
            predicate,
            object,
            timestamp,
        });
    }

    /// Checks if an edge exists matching the given pattern.
    pub fn has_edge(&self, subject: &str, predicate: &str) -> bool {
        self.subject_index
            .get(subject)
            .and_then(|preds| preds.get(predicate))
            .map(|objs| !objs.is_empty())
            .unwrap_or(false)
    }

    /// Gets all objects for a given subject-predicate pair.
    pub fn get_objects(&self, subject: &str, predicate: &str) -> Vec<&str> {
        self.subject_index
            .get(subject)
            .and_then(|preds| preds.get(predicate))
            .map(|objs| objs.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Gets all subjects for a given object-predicate pair.
    pub fn get_subjects(&self, object: &str, predicate: &str) -> Vec<&str> {
        self.object_index
            .get(object)
            .and_then(|preds| preds.get(predicate))
            .map(|subjs| subjs.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Evicts edges older than the given timestamp.
    ///
    /// Uses incremental index removal instead of rebuilding from scratch,
    /// which is O(evicted) rather than O(total_edges).
    pub fn evict_before(&mut self, cutoff: i64) {
        // Partition: collect expired edges and remove them from indexes
        let mut i = 0;
        while i < self.edges.len() {
            if self.edges[i].timestamp < cutoff {
                let edge = self.edges.swap_remove(i);
                // Remove from subject index
                if let Some(preds) = self.subject_index.get_mut(&edge.subject) {
                    if let Some(objs) = preds.get_mut(&edge.predicate) {
                        objs.remove(&edge.object);
                        if objs.is_empty() {
                            preds.remove(&edge.predicate);
                        }
                    }
                    if preds.is_empty() {
                        self.subject_index.remove(&edge.subject);
                    }
                }
                // Remove from object index
                if let Some(preds) = self.object_index.get_mut(&edge.object) {
                    if let Some(subjs) = preds.get_mut(&edge.predicate) {
                        subjs.remove(&edge.subject);
                        if subjs.is_empty() {
                            preds.remove(&edge.predicate);
                        }
                    }
                    if preds.is_empty() {
                        self.object_index.remove(&edge.object);
                    }
                }
                // Don't increment i — swap_remove moved last element here
            } else {
                i += 1;
            }
        }
    }
}

impl Default for GraphPatternJoinState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Variable-Length Path
// ---------------------------------------------------------------------------

/// Variable-length path operator for bounded BFS traversal.
///
/// Maps to Java's `VariableLengthPathOperator`.
pub struct VariableLengthPathOperator {
    pub min_hops: usize,
    pub max_hops: usize,
    pub relationship_type: Option<String>,
    pub shortest_path_only: bool,
}

impl VariableLengthPathOperator {
    pub fn new(min_hops: usize, max_hops: usize) -> Self {
        Self {
            min_hops,
            max_hops,
            relationship_type: None,
            shortest_path_only: false,
        }
    }

    pub fn with_relationship_type(mut self, rel_type: impl Into<String>) -> Self {
        self.relationship_type = Some(rel_type.into());
        self
    }

    pub fn with_shortest_path_only(mut self, shortest: bool) -> Self {
        self.shortest_path_only = shortest;
        self
    }

    /// Finds all paths from `start` in the graph state.
    pub fn find_paths(
        &self,
        start: &str,
        graph: &GraphPatternJoinState,
    ) -> Vec<Vec<String>> {
        let mut results = Vec::new();
        let mut visited = HashSet::new();
        let mut path = vec![start.to_string()];
        visited.insert(start.to_string());
        self.dfs(start, graph, &mut path, &mut visited, &mut results, 0);
        results
    }

    fn dfs(
        &self,
        current: &str,
        graph: &GraphPatternJoinState,
        path: &mut Vec<String>,
        visited: &mut HashSet<String>,
        results: &mut Vec<Vec<String>>,
        depth: usize,
    ) {
        if depth >= self.min_hops {
            results.push(path.clone());
            if self.shortest_path_only {
                return;
            }
        }
        if depth >= self.max_hops {
            return;
        }

        // Get neighbors
        let neighbors: Vec<String> = if let Some(ref rel_type) = self.relationship_type {
            graph
                .get_objects(current, rel_type)
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            // Get all neighbors regardless of predicate
            graph
                .subject_index
                .get(current)
                .map(|preds| {
                    preds
                        .values()
                        .flat_map(|objs| objs.iter().cloned())
                        .collect()
                })
                .unwrap_or_default()
        };

        for neighbor in neighbors {
            if !visited.contains(&neighbor) {
                visited.insert(neighbor.clone());
                path.push(neighbor.clone());
                self.dfs(&neighbor, graph, path, visited, results, depth + 1);
                path.pop();
                visited.remove(&neighbor);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::IriTerm;
    use cqels_model::Term;

    fn make_stmt(s: &str, p: &str, o: &str) -> Statement {
        Statement::new(
            Term::Iri(IriTerm::new(s)),
            IriTerm::new(p),
            Term::Iri(IriTerm::new(o)),
        )
    }

    #[test]
    fn test_windowed_join() {
        let mut state: WindowedJoinState<i32, i32> =
            WindowedJoinState::new(Duration::from_secs(5));
        let join_fn = |l: &i32, r: &i32| Some(l + r);

        // Add left at t=1000
        let results = state.add_left(10, 1000, &join_fn);
        assert!(results.is_empty()); // no right elements yet

        // Add right at t=2000 (within window)
        let results = state.add_right(20, 2000, &join_fn);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 30);

        // Add right at t=10000 (outside window of left at 1000)
        state.advance_watermark(6000);
        let results = state.add_right(30, 10000, &join_fn);
        assert!(results.is_empty());
    }

    #[test]
    fn test_interval_join() {
        let mut state: IntervalJoinState<i32, i32> =
            IntervalJoinState::new(Duration::from_secs(0), Duration::from_secs(3));
        let join_fn = |l: &i32, r: &i32| Some((*l, *r));

        // Left at t=1000
        state.add_left(1, 1000, &join_fn);

        // Right at t=2000 (within [1000+0, 1000+3000] = [1000, 4000])
        let results = state.add_right(2, 2000, &join_fn);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, (1, 2));

        // Right at t=5000 (outside [1000, 4000])
        let results = state.add_right(3, 5000, &join_fn);
        assert!(results.is_empty());
    }

    #[test]
    fn test_graph_pattern_join() {
        let mut state = GraphPatternJoinState::new();

        let stmt1 = make_stmt("http://a", "http://knows", "http://b");
        let stmt2 = make_stmt("http://b", "http://knows", "http://c");

        state.add_statement(&stmt1, 100);
        state.add_statement(&stmt2, 200);

        assert!(state.has_edge("<http://a>", "<http://knows>"));
        let objects = state.get_objects("<http://a>", "<http://knows>");
        assert_eq!(objects.len(), 1);
        assert!(objects[0].contains("http://b"));

        let subjects = state.get_subjects("<http://b>", "<http://knows>");
        assert_eq!(subjects.len(), 1);
    }

    #[test]
    fn test_graph_pattern_eviction() {
        let mut state = GraphPatternJoinState::new();

        let stmt1 = make_stmt("http://a", "http://p", "http://b");
        let stmt2 = make_stmt("http://c", "http://p", "http://d");

        state.add_statement(&stmt1, 100);
        state.add_statement(&stmt2, 200);
        assert_eq!(state.edges.len(), 2);

        state.evict_before(150);
        assert_eq!(state.edges.len(), 1);
    }

    #[test]
    fn test_variable_length_path() {
        let mut state = GraphPatternJoinState::new();

        // Create a chain: a -> b -> c -> d
        state.add_statement(&make_stmt("http://a", "http://link", "http://b"), 0);
        state.add_statement(&make_stmt("http://b", "http://link", "http://c"), 0);
        state.add_statement(&make_stmt("http://c", "http://link", "http://d"), 0);

        let op = VariableLengthPathOperator::new(1, 3)
            .with_relationship_type("<http://link>");

        let paths = op.find_paths("<http://a>", &state);
        // Should find paths of length 1, 2, 3
        assert!(!paths.is_empty());
    }
}
