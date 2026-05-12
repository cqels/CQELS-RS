//! Join-graph topology + factorized planner — layer 3 of the F-IVM
//! port.
//!
//! Ports the **structural** half of cqels-java's multi-way join
//! planner from `org.cqels.operator.aggregate.swag.join`:
//!
//! - [`JoinGraph`] — declarative join topology (relations = nodes,
//!   join predicates = edges) with neighbor / degree / connectivity
//!   / acyclicity helpers, hypergraph + primal-graph construction,
//!   GYO acyclicity, and variable-order derivation.
//! - [`Hypergraph`] / [`Hyperedge`] — hypergraph representation of
//!   the join.
//! - [`FactorizedViewPlan`] / [`ViewLayer`] — prefix/suffix view
//!   layers built from a [`crate::operator::swag_join::VariableOrder`].
//!
//! What's intentionally **not** in this layer
//! - **No key/payload extractor closures on `Relation`.** The Java
//!   side stores them directly on the graph; that's a runtime
//!   concern that belongs to Layer 4's executor
//!   (`MultiWayJoinState`). Layer 3 keeps the graph purely
//!   structural — relation name, variable set, edges with windows.
//! - **No execution.** `MultiWayJoinState`, the aggregate-spec
//!   hierarchy, and `WindowedJoinAggregateOperator` are Layer 4.
//!
//! # Example
//!
//! ```
//! use cqels_core::operator::swag_join_graph::JoinGraph;
//!
//! // Triangle join over User-Post-Like: 3 relations, 3 edges.
//! let mut graph = JoinGraph::new();
//! graph.add_relation("User", &["u"]);
//! graph.add_relation("Post", &["u", "p"]);
//! graph.add_relation("Like", &["u", "p"]);
//! graph.add_edge("User", "Post", -3600, 3600);
//! graph.add_edge("Post", "Like", -3600, 3600);
//! graph.add_edge("User", "Like", -3600, 3600);
//!
//! assert_eq!(graph.relation_names().len(), 3);
//! assert!(graph.is_connected());
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::operator::swag_join::{VariableOrder, VariableOrderMethod};

// ─── Relation ───────────────────────────────────────────────────────

/// A relation participating in the join — name + the set of join
/// variables it exposes. Mirrors `JoinGraph.Relation` from Java with
/// the runtime closures stripped (those live with the executor).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    name: String,
    variables: BTreeSet<String>,
}

impl Relation {
    fn new(name: String, variables: BTreeSet<String>) -> Self {
        Self { name, variables }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sorted join variables exposed by this relation.
    pub fn variables(&self) -> &BTreeSet<String> {
        &self.variables
    }
}

// ─── Edge ───────────────────────────────────────────────────────────

/// A join predicate between two relations, paired with the temporal
/// window `[window_lower, window_upper]` (in milliseconds, relative
/// to the querying relation's event time). Mirrors `JoinGraph.Edge`.
///
/// The graph is undirected: edges between A and B are symmetric in
/// `relation_a`/`relation_b`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    pub relation_a: String,
    pub relation_b: String,
    pub window_lower: i64,
    pub window_upper: i64,
}

impl Edge {
    pub fn new(
        relation_a: impl Into<String>,
        relation_b: impl Into<String>,
        window_lower: i64,
        window_upper: i64,
    ) -> Self {
        let a = relation_a.into();
        let b = relation_b.into();
        assert_ne!(a, b, "self-joins not supported");
        assert!(
            window_lower <= window_upper,
            "window_lower must be <= window_upper"
        );
        Self {
            relation_a: a,
            relation_b: b,
            window_lower,
            window_upper,
        }
    }

    /// Returns the relation on the other side of the edge from
    /// `relation`, or `None` if `relation` is not incident to this
    /// edge.
    pub fn other(&self, relation: &str) -> Option<&str> {
        if self.relation_a == relation {
            Some(&self.relation_b)
        } else if self.relation_b == relation {
            Some(&self.relation_a)
        } else {
            None
        }
    }

    /// Window bounds for the "other side" given a query timestamp.
    ///
    /// If `querying_relation == relation_a`, the bounds are
    /// `[ts + lower, ts + upper]`. If it's `relation_b`, the bounds
    /// are reversed: `[ts - upper, ts - lower]`. Returns `None` if
    /// `querying_relation` is not incident.
    pub fn window_bounds(&self, timestamp: i64, querying_relation: &str) -> Option<(i64, i64)> {
        if querying_relation == self.relation_a {
            Some((timestamp + self.window_lower, timestamp + self.window_upper))
        } else if querying_relation == self.relation_b {
            Some((timestamp - self.window_upper, timestamp - self.window_lower))
        } else {
            None
        }
    }
}

// ─── JoinGraph ──────────────────────────────────────────────────────

/// Declarative join topology with hypergraph representation. Mirrors
/// Java's `JoinGraph` (minus the runtime extractor closures —
/// those live with the executor at Layer 4).
///
/// Use [`new`](Self::new), [`add_relation`](Self::add_relation), and
/// [`add_edge`](Self::add_edge) to declare the join; query topology
/// with [`is_connected`](Self::is_connected) / [`is_acyclic`](Self::is_acyclic)
/// (DFS-based) / [`is_acyclic_gyo`](Self::is_acyclic_gyo) (hypergraph
/// GYO reduction); plan evaluation with
/// [`build_variable_order`](Self::build_variable_order) followed by
/// [`FactorizedViewPlan::from_graph`].
#[derive(Clone, Debug, Default)]
pub struct JoinGraph {
    relations: BTreeMap<String, Relation>,
    edges: Vec<Edge>,
    adjacency: BTreeMap<String, BTreeSet<String>>,
}

impl JoinGraph {
    /// Empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a relation with the given name and join variables.
    ///
    /// # Panics
    ///
    /// - Duplicate relation name.
    /// - Empty variable list.
    /// - Blank variable name.
    pub fn add_relation(&mut self, name: &str, variables: &[&str]) -> &mut Self {
        assert!(
            !self.relations.contains_key(name),
            "relation already exists: {name}"
        );
        assert!(
            !variables.is_empty(),
            "relation variables must not be empty: {name}"
        );
        let mut vars = BTreeSet::new();
        for v in variables {
            assert!(
                !v.trim().is_empty(),
                "relation variable must not be blank: {name}"
            );
            vars.insert((*v).to_string());
        }
        self.relations
            .insert(name.to_string(), Relation::new(name.to_string(), vars));
        self.adjacency.insert(name.to_string(), BTreeSet::new());
        self
    }

    /// Adds an undirected join edge between two relations with the
    /// given temporal window.
    ///
    /// # Panics
    ///
    /// - Either relation missing.
    /// - Self-edge (`relation_a == relation_b`).
    /// - Duplicate edge.
    /// - `window_lower > window_upper`.
    pub fn add_edge(
        &mut self,
        relation_a: &str,
        relation_b: &str,
        window_lower: i64,
        window_upper: i64,
    ) -> &mut Self {
        assert!(
            self.relations.contains_key(relation_a),
            "relation missing: {relation_a}"
        );
        assert!(
            self.relations.contains_key(relation_b),
            "relation missing: {relation_b}"
        );
        assert_ne!(relation_a, relation_b, "self-edge not supported");
        for e in &self.edges {
            let dup = (e.relation_a == relation_a && e.relation_b == relation_b)
                || (e.relation_a == relation_b && e.relation_b == relation_a);
            assert!(!dup, "edge already exists: {relation_a} - {relation_b}");
        }
        self.edges.push(Edge::new(
            relation_a,
            relation_b,
            window_lower,
            window_upper,
        ));
        self.adjacency
            .get_mut(relation_a)
            .expect("relation present")
            .insert(relation_b.to_string());
        self.adjacency
            .get_mut(relation_b)
            .expect("relation present")
            .insert(relation_a.to_string());
        self
    }

    /// All relation names, sorted lexicographically.
    pub fn relation_names(&self) -> Vec<&str> {
        self.relations.keys().map(String::as_str).collect()
    }

    /// Lookup a relation by name.
    pub fn relation(&self, name: &str) -> Option<&Relation> {
        self.relations.get(name)
    }

    /// All edges in declaration order.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Neighbors of `relation` in the join graph.
    pub fn neighbors(&self, relation: &str) -> Vec<&str> {
        self.adjacency
            .get(relation)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Returns the edge between `a` and `b` if any.
    pub fn edge(&self, a: &str, b: &str) -> Option<&Edge> {
        self.edges.iter().find(|e| {
            (e.relation_a == a && e.relation_b == b) || (e.relation_a == b && e.relation_b == a)
        })
    }

    /// Degree of a relation (number of incident edges).
    pub fn degree(&self, relation: &str) -> usize {
        self.adjacency.get(relation).map(BTreeSet::len).unwrap_or(0)
    }

    /// Maximum degree across all relations. `0` for an empty graph.
    pub fn max_degree(&self) -> usize {
        self.adjacency
            .values()
            .map(BTreeSet::len)
            .max()
            .unwrap_or(0)
    }

    /// Edges incident to `relation`, in declaration order.
    pub fn incident_edges(&self, relation: &str) -> Vec<&Edge> {
        self.edges
            .iter()
            .filter(|e| e.relation_a == relation || e.relation_b == relation)
            .collect()
    }

    /// BFS-based connectivity check. Empty graph is vacuously
    /// connected.
    pub fn is_connected(&self) -> bool {
        if self.relations.is_empty() {
            return true;
        }
        let start = self
            .relations
            .keys()
            .next()
            .expect("non-empty relations")
            .clone();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(start.clone());
        visited.insert(start);
        while let Some(node) = queue.pop_front() {
            if let Some(neighbors) = self.adjacency.get(&node) {
                for n in neighbors {
                    if !visited.contains(n) {
                        visited.insert(n.clone());
                        queue.push_back(n.clone());
                    }
                }
            }
        }
        visited.len() == self.relations.len()
    }

    /// DFS-based undirected-cycle detection. Empty graph is acyclic.
    /// This treats the join *graph* (relation-level) as undirected;
    /// for hypergraph-level GYO acyclicity see
    /// [`is_acyclic_gyo`](Self::is_acyclic_gyo).
    pub fn is_acyclic(&self) -> bool {
        if self.relations.is_empty() {
            return true;
        }
        let mut visited: HashSet<String> = HashSet::new();
        let mut rec_stack: HashSet<String> = HashSet::new();
        for relation in self.relations.keys() {
            if !visited.contains(relation)
                && self.has_cycle_dfs(relation, None, &mut visited, &mut rec_stack)
            {
                return false;
            }
        }
        true
    }

    fn has_cycle_dfs(
        &self,
        current: &str,
        parent: Option<&str>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(current.to_string());
        rec_stack.insert(current.to_string());
        if let Some(neighbors) = self.adjacency.get(current) {
            for n in neighbors {
                if Some(n.as_str()) == parent {
                    continue;
                }
                if rec_stack.contains(n) {
                    return true;
                }
                if !visited.contains(n) && self.has_cycle_dfs(n, Some(current), visited, rec_stack)
                {
                    return true;
                }
            }
        }
        rec_stack.remove(current);
        false
    }

    /// Builds the hypergraph representation: vertices = join
    /// variables across all relations, hyperedges = each relation's
    /// variable set.
    pub fn build_hypergraph(&self) -> Hypergraph {
        let mut vertices: BTreeSet<String> = BTreeSet::new();
        let mut hyperedges: Vec<Hyperedge> = Vec::new();
        for relation in self.relations.values() {
            for v in &relation.variables {
                vertices.insert(v.clone());
            }
            hyperedges.push(Hyperedge {
                relation_name: relation.name.clone(),
                variables: relation.variables.clone(),
            });
        }
        let primal_graph = build_primal_graph(&vertices, &hyperedges);
        Hypergraph {
            vertices,
            hyperedges,
            primal_graph,
        }
    }

    /// Tests join-hypergraph acyclicity via the GYO algorithm.
    pub fn is_acyclic_gyo(&self) -> bool {
        build_join_tree(&self.build_hypergraph()).acyclic
    }

    /// Derives a variable order suitable for multi-way join
    /// evaluation. Acyclic hypergraphs use a DFS over the GYO join
    /// tree; cyclic ones fall back to a greedy min-fill elimination
    /// order on the primal graph.
    pub fn build_variable_order(&self) -> VariableOrder {
        let hypergraph = self.build_hypergraph();
        let join_tree_result = build_join_tree(&hypergraph);
        if join_tree_result.acyclic {
            let prefix = derive_prefix_order(&hypergraph, &join_tree_result.join_tree);
            let mut elimination = prefix.clone();
            elimination.reverse();
            let tree = into_hashmap_of_hashsets(&join_tree_result.join_tree);
            VariableOrder::new(
                prefix,
                elimination,
                true,
                tree,
                VariableOrderMethod::GyoJoinTree,
            )
        } else {
            let elimination = min_fill_elimination_order(&hypergraph.primal_graph);
            let mut prefix = elimination.clone();
            prefix.reverse();
            VariableOrder::new(
                prefix,
                elimination,
                false,
                HashMap::new(),
                VariableOrderMethod::MinFill,
            )
        }
    }
}

// ─── Hypergraph types ───────────────────────────────────────────────

/// Hypergraph view of a [`JoinGraph`] — vertices are join variables,
/// hyperedges are each relation's variable set, and `primal_graph`
/// connects co-occurring variables.
#[derive(Clone, Debug)]
pub struct Hypergraph {
    pub vertices: BTreeSet<String>,
    pub hyperedges: Vec<Hyperedge>,
    pub primal_graph: BTreeMap<String, BTreeSet<String>>,
}

/// A single hyperedge — one relation plus its variable set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hyperedge {
    pub relation_name: String,
    pub variables: BTreeSet<String>,
}

fn build_primal_graph(
    vertices: &BTreeSet<String>,
    hyperedges: &[Hyperedge],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for v in vertices {
        graph.insert(v.clone(), BTreeSet::new());
    }
    for edge in hyperedges {
        let vars: Vec<&String> = edge.variables.iter().collect();
        for i in 0..vars.len() {
            for j in (i + 1)..vars.len() {
                let u = vars[i].clone();
                let v = vars[j].clone();
                graph.entry(u.clone()).or_default().insert(v.clone());
                graph.entry(v).or_default().insert(u);
            }
        }
    }
    graph
}

// ─── GYO acyclicity + join-tree ─────────────────────────────────────

struct JoinTreeResult {
    acyclic: bool,
    join_tree: BTreeMap<String, BTreeSet<String>>,
}

fn build_join_tree(hypergraph: &Hypergraph) -> JoinTreeResult {
    // Working copy of relation -> variables.
    let mut edge_vars: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut join_tree: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for h in &hypergraph.hyperedges {
        edge_vars.insert(h.relation_name.clone(), h.variables.clone());
        join_tree.insert(h.relation_name.clone(), BTreeSet::new());
    }

    let mut changed = true;
    let mut iterations: usize = 0;
    let max_iterations = (edge_vars.len() * edge_vars.len() * 4).max(1);

    while changed && !edge_vars.is_empty() && iterations < max_iterations {
        changed = false;
        iterations += 1;

        // Sort for deterministic ear-removal order.
        let edge_names: Vec<String> = edge_vars.keys().cloned().collect();

        // Phase 1: remove "ears" — hyperedges contained in another.
        let mut removed_ear = false;
        for edge_name in &edge_names {
            let vars = edge_vars.get(edge_name).expect("present").clone();
            let parent = find_containing_edge(edge_name, &vars, &edge_vars, &edge_names);
            match parent {
                Some(p) => {
                    join_tree
                        .get_mut(edge_name)
                        .expect("present")
                        .insert(p.clone());
                    join_tree
                        .get_mut(&p)
                        .expect("present")
                        .insert(edge_name.clone());
                    edge_vars.remove(edge_name);
                    changed = true;
                    removed_ear = true;
                    break;
                }
                None if edge_vars.len() == 1 && vars.is_empty() => {
                    edge_vars.remove(edge_name);
                    changed = true;
                    removed_ear = true;
                    break;
                }
                _ => {}
            }
        }
        if removed_ear {
            continue;
        }

        // Phase 2: remove a "singleton" variable (appears in only one edge).
        if let Some(singleton) = find_singleton_variable(&edge_vars) {
            for vars in edge_vars.values_mut() {
                vars.remove(&singleton);
            }
            changed = true;
        }
    }

    JoinTreeResult {
        acyclic: edge_vars.is_empty(),
        join_tree,
    }
}

fn find_containing_edge(
    candidate: &str,
    candidate_vars: &BTreeSet<String>,
    edge_vars: &BTreeMap<String, BTreeSet<String>>,
    edge_names: &[String],
) -> Option<String> {
    for other in edge_names {
        if other == candidate {
            continue;
        }
        if let Some(other_vars) = edge_vars.get(other) {
            if candidate_vars.is_subset(other_vars) {
                return Some(other.clone());
            }
        }
    }
    None
}

fn find_singleton_variable(edge_vars: &BTreeMap<String, BTreeSet<String>>) -> Option<String> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for vars in edge_vars.values() {
        for v in vars {
            *counts.entry(v.clone()).or_insert(0) += 1;
        }
    }
    // Lexicographically smallest variable with count == 1.
    counts
        .into_iter()
        .filter(|(_, c)| *c == 1)
        .map(|(v, _)| v)
        .next()
}

fn derive_prefix_order(
    hypergraph: &Hypergraph,
    join_tree: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    if hypergraph.vertices.is_empty() {
        return Vec::new();
    }
    let edge_by_relation: BTreeMap<&String, &Hyperedge> = hypergraph
        .hyperedges
        .iter()
        .map(|h| (&h.relation_name, h))
        .collect();
    let Some(root) = choose_join_tree_root(&edge_by_relation) else {
        return Vec::new();
    };
    let mut visited: HashSet<String> = HashSet::new();
    let mut order: Vec<String> = Vec::new();
    dfs_join_tree(
        &root,
        None,
        join_tree,
        &edge_by_relation,
        &mut visited,
        &mut order,
    );

    // Append any unreached variables to keep determinism on
    // disconnected graphs.
    if order.len() < hypergraph.vertices.len() {
        let remaining: Vec<String> = hypergraph
            .vertices
            .iter()
            .filter(|v| !order.contains(v))
            .cloned()
            .collect();
        order.extend(remaining);
    }
    order
}

fn choose_join_tree_root(edge_by_relation: &BTreeMap<&String, &Hyperedge>) -> Option<String> {
    // Choose the hyperedge with the largest variable set, breaking
    // ties lexicographically by relation name.
    let mut best: Option<(usize, String)> = None;
    for (name, edge) in edge_by_relation {
        let size = edge.variables.len();
        match &best {
            None => best = Some((size, (*name).clone())),
            Some((bsize, bname)) => {
                if size > *bsize || (size == *bsize && name.as_str() < bname.as_str()) {
                    best = Some((size, (*name).clone()));
                }
            }
        }
    }
    best.map(|(_, n)| n)
}

fn dfs_join_tree(
    relation: &str,
    parent: Option<&str>,
    join_tree: &BTreeMap<String, BTreeSet<String>>,
    edge_by_relation: &BTreeMap<&String, &Hyperedge>,
    visited: &mut HashSet<String>,
    order: &mut Vec<String>,
) {
    if visited.contains(relation) {
        return;
    }
    visited.insert(relation.to_string());

    if let Some(edge) = edge_by_relation.get(&relation.to_string()) {
        for v in &edge.variables {
            if !order.contains(v) {
                order.push(v.clone());
            }
        }
    }

    let neighbors: Vec<String> = join_tree
        .get(relation)
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();
    for n in neighbors {
        if Some(n.as_str()) != parent {
            dfs_join_tree(
                &n,
                Some(relation),
                join_tree,
                edge_by_relation,
                visited,
                order,
            );
        }
    }
}

fn min_fill_elimination_order(primal: &BTreeMap<String, BTreeSet<String>>) -> Vec<String> {
    // Working copy.
    let mut graph: BTreeMap<String, BTreeSet<String>> =
        primal.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut order: Vec<String> = Vec::new();

    while !graph.is_empty() {
        // Choose vertex minimizing (fill, degree, name).
        let mut best: Option<(usize, usize, String)> = None;
        for vertex in graph.keys() {
            let neighbors = graph.get(vertex).expect("present");
            let fill = compute_fill_in(neighbors, &graph);
            let degree = neighbors.len();
            match &best {
                None => best = Some((fill, degree, vertex.clone())),
                Some((bf, bd, bn)) => {
                    if fill < *bf
                        || (fill == *bf && degree < *bd)
                        || (fill == *bf && degree == *bd && vertex < bn)
                    {
                        best = Some((fill, degree, vertex.clone()));
                    }
                }
            }
        }
        let Some((_, _, victim)) = best else {
            break;
        };

        // Connect remaining neighbors of `victim` to each other,
        // then drop `victim`.
        let neighbors: Vec<String> = graph
            .get(&victim)
            .expect("present")
            .iter()
            .cloned()
            .collect();
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let u = neighbors[i].clone();
                let v = neighbors[j].clone();
                graph.entry(u.clone()).or_default().insert(v.clone());
                graph.entry(v).or_default().insert(u);
            }
        }
        for n in &neighbors {
            if let Some(set) = graph.get_mut(n) {
                set.remove(&victim);
            }
        }
        graph.remove(&victim);
        order.push(victim);
    }

    order
}

fn compute_fill_in(
    neighbors: &BTreeSet<String>,
    graph: &BTreeMap<String, BTreeSet<String>>,
) -> usize {
    if neighbors.len() < 2 {
        return 0;
    }
    let list: Vec<&String> = neighbors.iter().collect();
    let mut missing: usize = 0;
    for i in 0..list.len() {
        let u = list[i];
        let u_neighbors = graph.get(u);
        for v in list.iter().skip(i + 1) {
            let v_str: &String = v;
            if u_neighbors.is_none_or(|s| !s.contains(v_str)) {
                missing += 1;
            }
        }
    }
    missing
}

fn into_hashmap_of_hashsets(
    tree: &BTreeMap<String, BTreeSet<String>>,
) -> HashMap<String, HashSet<String>> {
    tree.iter()
        .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
        .collect()
}

// ─── FactorizedViewPlan ─────────────────────────────────────────────

/// Planning structure for factorized prefix/suffix views over a
/// variable order. Mirrors Java's `FactorizedViewPlan`. The plan
/// is purely descriptive — it lists which relations become *fully
/// bound* at each layer of the prefix and suffix orders, but does
/// not compute anything.
#[derive(Clone, Debug)]
pub struct FactorizedViewPlan {
    variable_order: VariableOrder,
    prefix_layers: Vec<ViewLayer>,
    suffix_layers: Vec<ViewLayer>,
    relation_variables: BTreeMap<String, BTreeSet<String>>,
}

impl FactorizedViewPlan {
    /// Builds a plan from a [`JoinGraph`] and a derived
    /// [`VariableOrder`]. Use
    /// [`JoinGraph::build_variable_order`] to obtain the latter.
    pub fn from_graph(graph: &JoinGraph, variable_order: VariableOrder) -> Self {
        let mut relation_vars: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for name in graph.relation_names() {
            let r = graph.relation(name).expect("present");
            relation_vars.insert(name.to_string(), r.variables().clone());
        }
        let prefix = build_layers(variable_order.prefix_order(), &relation_vars);
        let mut reversed: Vec<String> = variable_order.prefix_order().to_vec();
        reversed.reverse();
        let suffix = build_layers(&reversed, &relation_vars);
        Self {
            variable_order,
            prefix_layers: prefix,
            suffix_layers: suffix,
            relation_variables: relation_vars,
        }
    }

    pub fn variable_order(&self) -> &VariableOrder {
        &self.variable_order
    }

    pub fn prefix_layers(&self) -> &[ViewLayer] {
        &self.prefix_layers
    }

    pub fn suffix_layers(&self) -> &[ViewLayer] {
        &self.suffix_layers
    }

    pub fn relation_variables(&self) -> &BTreeMap<String, BTreeSet<String>> {
        &self.relation_variables
    }
}

/// One layer of a [`FactorizedViewPlan`] — the variables bound so far
/// and the relations that became fully bound at this layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewLayer {
    pub variables: Vec<String>,
    pub relations: BTreeSet<String>,
}

fn build_layers(
    order: &[String],
    relation_vars: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<ViewLayer> {
    let mut layers: Vec<ViewLayer> = Vec::new();
    let mut bound: Vec<String> = Vec::new();
    let mut assigned: BTreeSet<String> = BTreeSet::new();

    for variable in order {
        bound.push(variable.clone());
        let mut layer_relations: BTreeSet<String> = BTreeSet::new();
        for (name, vars) in relation_vars {
            if assigned.contains(name) {
                continue;
            }
            if vars.iter().all(|v| bound.contains(v)) {
                layer_relations.insert(name.clone());
            }
        }
        for name in &layer_relations {
            assigned.insert(name.clone());
        }
        layers.push(ViewLayer {
            variables: bound.clone(),
            relations: layer_relations,
        });
    }

    layers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn star() -> JoinGraph {
        // Star: center R connected to A, B, C.
        let mut g = JoinGraph::new();
        g.add_relation("R", &["x"]);
        g.add_relation("A", &["x", "a"]);
        g.add_relation("B", &["x", "b"]);
        g.add_relation("C", &["x", "c"]);
        g.add_edge("R", "A", -10, 10);
        g.add_edge("R", "B", -10, 10);
        g.add_edge("R", "C", -10, 10);
        g
    }

    fn triangle() -> JoinGraph {
        // Triangle: 3 relations, 3 edges forming a cycle.
        let mut g = JoinGraph::new();
        g.add_relation("R1", &["x", "y"]);
        g.add_relation("R2", &["y", "z"]);
        g.add_relation("R3", &["x", "z"]);
        g.add_edge("R1", "R2", -10, 10);
        g.add_edge("R2", "R3", -10, 10);
        g.add_edge("R1", "R3", -10, 10);
        g
    }

    // ─── Topology basics ──────────────────────────────────────────

    #[test]
    fn relation_names_sorted_lexicographically() {
        let g = star();
        assert_eq!(g.relation_names(), vec!["A", "B", "C", "R"]);
    }

    #[test]
    fn degree_max_degree_and_neighbors() {
        let g = star();
        assert_eq!(g.degree("R"), 3);
        assert_eq!(g.degree("A"), 1);
        assert_eq!(g.max_degree(), 3);
        let mut neighbors = g.neighbors("R");
        neighbors.sort();
        assert_eq!(neighbors, vec!["A", "B", "C"]);
    }

    #[test]
    fn incident_edges_filters_correctly() {
        let g = star();
        let inc_r = g.incident_edges("R");
        assert_eq!(inc_r.len(), 3);
        let inc_a = g.incident_edges("A");
        assert_eq!(inc_a.len(), 1);
        assert_eq!(inc_a[0].relation_a, "R");
        assert_eq!(inc_a[0].relation_b, "A");
    }

    #[test]
    fn is_connected_star_yes_disjoint_no() {
        let g = star();
        assert!(g.is_connected());
        let mut h = JoinGraph::new();
        h.add_relation("L", &["x"]);
        h.add_relation("R", &["y"]);
        // No edge → disconnected.
        assert!(!h.is_connected());
    }

    #[test]
    fn is_acyclic_star_yes_triangle_no_graph_level() {
        assert!(star().is_acyclic());
        // Triangle has a cycle at the relation-graph level.
        assert!(!triangle().is_acyclic());
    }

    // ─── Edge windows ──────────────────────────────────────────────

    #[test]
    fn edge_window_bounds_swap_per_query_side() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", -100, 200);
        let e = g.edge("A", "B").expect("edge");
        // From A's side: [ts - 100, ts + 200].
        assert_eq!(e.window_bounds(1000, "A"), Some((900, 1200)));
        // From B's side: [ts - 200, ts + 100].
        assert_eq!(e.window_bounds(1000, "B"), Some((800, 1100)));
        // Not incident.
        assert!(e.window_bounds(1000, "C").is_none());
    }

    #[test]
    #[should_panic(expected = "self-edge not supported")]
    fn rejects_self_edge() {
        let mut g = JoinGraph::new();
        g.add_relation("R", &["x"]);
        g.add_edge("R", "R", -10, 10);
    }

    #[test]
    #[should_panic(expected = "edge already exists")]
    fn rejects_duplicate_edge() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["y"]);
        g.add_edge("A", "B", -10, 10);
        g.add_edge("B", "A", -20, 20);
    }

    // ─── Hypergraph + GYO ──────────────────────────────────────────

    #[test]
    fn hypergraph_collects_vertices_and_hyperedges() {
        let g = star();
        let h = g.build_hypergraph();
        assert_eq!(h.vertices.len(), 4, "vars are {{x, a, b, c}}");
        assert_eq!(h.hyperedges.len(), 4);
        // x should be connected to a, b, c in the primal graph
        // (because (x, a), (x, b), (x, c) co-occur in A, B, C).
        let x_neighbors = h.primal_graph.get("x").expect("x present");
        assert!(x_neighbors.contains("a"));
        assert!(x_neighbors.contains("b"));
        assert!(x_neighbors.contains("c"));
    }

    #[test]
    fn star_is_acyclic_gyo_triangle_is_cyclic_gyo() {
        assert!(star().is_acyclic_gyo());
        assert!(!triangle().is_acyclic_gyo());
    }

    #[test]
    fn binary_join_is_acyclic_gyo() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x", "y"]);
        g.add_relation("B", &["y", "z"]);
        g.add_edge("A", "B", -10, 10);
        assert!(g.is_acyclic_gyo());
    }

    // ─── Variable order ───────────────────────────────────────────

    #[test]
    fn acyclic_query_uses_gyo_join_tree_method() {
        let g = star();
        let vo = g.build_variable_order();
        assert!(vo.is_acyclic());
        assert_eq!(vo.method(), VariableOrderMethod::GyoJoinTree);
        assert_eq!(vo.prefix_order().len(), 4);
        assert_eq!(vo.elimination_order().len(), 4);
        // Elimination is the reverse of prefix.
        let mut rev = vo.prefix_order().to_vec();
        rev.reverse();
        assert_eq!(rev, vo.elimination_order());
    }

    #[test]
    fn cyclic_query_falls_back_to_min_fill() {
        let g = triangle();
        let vo = g.build_variable_order();
        assert!(!vo.is_acyclic());
        assert_eq!(vo.method(), VariableOrderMethod::MinFill);
        assert_eq!(vo.prefix_order().len(), 3);
        assert!(vo.join_tree().is_empty());
    }

    // ─── FactorizedViewPlan ───────────────────────────────────────

    #[test]
    fn factorized_view_plan_lengths_match_variable_order() {
        let g = star();
        let vo = g.build_variable_order();
        let var_count = vo.prefix_order().len();
        let plan = FactorizedViewPlan::from_graph(&g, vo);
        assert_eq!(plan.prefix_layers().len(), var_count);
        assert_eq!(plan.suffix_layers().len(), var_count);
        // Every layer's `variables` should be monotonically growing.
        let mut prev = 0;
        for layer in plan.prefix_layers() {
            assert!(layer.variables.len() > prev);
            prev = layer.variables.len();
        }
    }

    #[test]
    fn factorized_view_plan_eventually_binds_every_relation() {
        let g = star();
        let vo = g.build_variable_order();
        let plan = FactorizedViewPlan::from_graph(&g, vo);
        let mut bound: BTreeSet<String> = BTreeSet::new();
        for layer in plan.prefix_layers() {
            for r in &layer.relations {
                bound.insert(r.clone());
            }
        }
        assert_eq!(bound.len(), 4, "all 4 relations eventually bind");
    }

    #[test]
    fn factorized_view_plan_relation_variables_match_graph() {
        let g = star();
        let vo = g.build_variable_order();
        let plan = FactorizedViewPlan::from_graph(&g, vo);
        // R has only {x}; A has {x, a}; etc.
        let r_vars = plan.relation_variables().get("R").expect("R present");
        assert!(r_vars.contains("x"));
        assert_eq!(r_vars.len(), 1);
        let a_vars = plan.relation_variables().get("A").expect("A present");
        assert!(a_vars.contains("x"));
        assert!(a_vars.contains("a"));
    }

    // ─── Edge cases ───────────────────────────────────────────────

    #[test]
    fn empty_graph_is_connected_and_acyclic() {
        let g = JoinGraph::new();
        assert!(g.is_connected());
        assert!(g.is_acyclic());
        assert!(g.is_acyclic_gyo());
        assert_eq!(g.max_degree(), 0);
    }

    #[test]
    #[should_panic(expected = "relation already exists")]
    fn duplicate_relation_panics() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("A", &["y"]);
    }

    #[test]
    #[should_panic(expected = "relation variables must not be empty")]
    fn empty_variable_list_panics() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &[]);
    }
}
