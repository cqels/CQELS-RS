use std::collections::HashSet;
use std::fmt;

use cqels_model::Statement;
use cqels_core::stream::RdfStreamElement;

use crate::alpha::AlphaNetwork;
use crate::beta::BetaNetwork;
use crate::config::ReasoningConfig;
use crate::conflict::ConflictResolver;
use crate::memory::WorkingMemory;

/// An inferred RDF stream element with provenance information.
#[derive(Clone, Debug)]
pub struct InferredRdfStreamElement {
    pub statement: Statement,
    pub timestamp: i64,
    pub inferred_by: String,
    pub derived_from: Vec<Statement>,
}

impl InferredRdfStreamElement {
    pub fn new(
        statement: Statement,
        timestamp: i64,
        inferred_by: String,
        derived_from: Vec<Statement>,
    ) -> Self {
        Self {
            statement,
            timestamp,
            inferred_by,
            derived_from,
        }
    }

    /// Returns `true` — this element is always inferred.
    pub fn is_inferred(&self) -> bool {
        true
    }

    /// Converts this into an `RdfStreamElement`.
    pub fn to_rdf_stream_element(&self) -> RdfStreamElement {
        RdfStreamElement::new(self.statement.clone(), self.timestamp)
    }
}

/// The main RETE network that coordinates the reasoning pipeline.
///
/// Processing flow:
/// 1. Evict expired facts based on window
/// 2. Add fact to working memory
/// 3. Propagate through alpha network
/// 4. Propagate matches through beta network
/// 5. Resolve conflicts
/// 6. Fire productions and deduplicate results
pub struct ReteNetwork {
    alpha_network: AlphaNetwork,
    beta_network: BetaNetwork,
    conflict_resolver: ConflictResolver,
    working_memory: WorkingMemory,
    inferred_facts: HashSet<Statement>,
    config: ReasoningConfig,
}

impl ReteNetwork {
    /// Compiles a RETE network from a reasoning configuration.
    pub fn compile(config: ReasoningConfig) -> Self {
        let all_patterns: Vec<_> = config.rule_set().all_patterns().into_iter().collect();
        let alpha_network = AlphaNetwork::compile(all_patterns);
        let beta_network = BetaNetwork::compile(config.rule_set(), &alpha_network);
        let conflict_resolver = ConflictResolver::new(config.conflict_resolution());

        Self {
            alpha_network,
            beta_network,
            conflict_resolver,
            working_memory: WorkingMemory::new(),
            inferred_facts: HashSet::new(),
            config,
        }
    }

    /// Processes a single RDF stream element through the RETE network.
    ///
    /// Returns a list of newly inferred RDF stream elements.
    pub fn process_element(&mut self, element: &RdfStreamElement) -> Vec<InferredRdfStreamElement> {
        let stmt = &element.statement;
        let timestamp = element.timestamp;

        // 1. Evict expired facts based on window
        let window_ms = self.config.default_window().as_millis() as i64;
        if window_ms > 0 {
            let watermark = timestamp - window_ms;
            self.working_memory.evict_before(watermark);
        }

        // 2. Add fact to working memory
        self.working_memory.add_fact(stmt.clone(), timestamp);

        // 3. Propagate through alpha network — collect all matches first
        let alpha_matches = self.alpha_network.evaluate(stmt);

        // 4. Propagate alpha matches through beta network → collect activations
        // Collect match data into owned tuples to avoid borrow conflicts
        let match_data: Vec<_> = alpha_matches
            .iter()
            .map(|m| (m.node.clone(), m.bindings.clone()))
            .collect();

        let mut all_activations = Vec::new();
        for (node, bindings) in match_data {
            let premises = vec![stmt.clone()];
            let activations = self.beta_network.propagate(
                &node,
                bindings,
                premises,
                timestamp,
                self.config.rule_set(),
            );
            all_activations.extend(activations);
        }

        // 5. Resolve conflicts
        let resolved =
            self.conflict_resolver
                .resolve(all_activations, self.config.rule_set());

        // 6. Fire productions, deduplicate, and collect results
        let rules = self.config.rule_set().rules();
        let mut results = Vec::new();
        for activation in &resolved {
            let rule = &rules[activation.rule_index()];
            let inferred_stmts = activation.fire(rule);
            for inferred_stmt in inferred_stmts {
                if !self.inferred_facts.contains(&inferred_stmt)
                    && !self.working_memory.contains(&inferred_stmt)
                {
                    self.inferred_facts.insert(inferred_stmt.clone());
                    results.push(InferredRdfStreamElement::new(
                        inferred_stmt,
                        timestamp,
                        rule.id().to_string(),
                        if self.config.track_provenance() {
                            activation.premises().to_vec()
                        } else {
                            Vec::new()
                        },
                    ));
                }
            }
        }

        // 7. Recursive inference (if enabled)
        if self.config.enable_recursive_inference() && !results.is_empty() {
            let mut depth = 0;
            let mut new_elements: Vec<RdfStreamElement> = results
                .iter()
                .map(|r| RdfStreamElement::new(r.statement.clone(), r.timestamp))
                .collect();

            while !new_elements.is_empty() && depth < self.config.max_recursion_depth() {
                depth += 1;
                let mut next_round = Vec::new();

                for elem in &new_elements {
                    let inferred = self.process_single_no_recurse(elem);
                    for inf in inferred {
                        next_round.push(RdfStreamElement::new(
                            inf.statement.clone(),
                            inf.timestamp,
                        ));
                        results.push(inf);
                    }
                }

                new_elements = next_round;
            }
        }

        results
    }

    /// Processes a single element without recursive inference (to avoid infinite recursion).
    fn process_single_no_recurse(
        &mut self,
        element: &RdfStreamElement,
    ) -> Vec<InferredRdfStreamElement> {
        let stmt = &element.statement;
        let timestamp = element.timestamp;

        self.working_memory.add_fact(stmt.clone(), timestamp);

        let alpha_matches = self.alpha_network.evaluate(stmt);
        let match_data: Vec<_> = alpha_matches
            .iter()
            .map(|m| (m.node.clone(), m.bindings.clone()))
            .collect();

        let mut all_activations = Vec::new();
        for (node, bindings) in match_data {
            let premises = vec![stmt.clone()];
            let activations = self.beta_network.propagate(
                &node,
                bindings,
                premises,
                timestamp,
                self.config.rule_set(),
            );
            all_activations.extend(activations);
        }

        let resolved =
            self.conflict_resolver
                .resolve(all_activations, self.config.rule_set());

        let rules = self.config.rule_set().rules();
        let mut results = Vec::new();
        for activation in &resolved {
            let rule = &rules[activation.rule_index()];
            let inferred_stmts = activation.fire(rule);
            for inferred_stmt in inferred_stmts {
                if !self.inferred_facts.contains(&inferred_stmt)
                    && !self.working_memory.contains(&inferred_stmt)
                {
                    self.inferred_facts.insert(inferred_stmt.clone());
                    results.push(InferredRdfStreamElement::new(
                        inferred_stmt,
                        timestamp,
                        rule.id().to_string(),
                        if self.config.track_provenance() {
                            activation.premises().to_vec()
                        } else {
                            Vec::new()
                        },
                    ));
                }
            }
        }

        results
    }

    pub fn working_memory(&self) -> &WorkingMemory {
        &self.working_memory
    }

    pub fn clear(&mut self) {
        self.working_memory.clear();
        self.beta_network.clear();
        self.inferred_facts.clear();
    }
}

impl fmt::Debug for ReteNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReteNetwork")
            .field("alpha_network", &self.alpha_network)
            .field("beta_network", &self.beta_network)
            .field("working_memory_size", &self.working_memory.size())
            .field("inferred_facts_count", &self.inferred_facts.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReasoningConfig;
    use crate::conflict::ConflictResolution;
    use crate::pattern::{PatternTerm, TriplePattern, TripleTemplate};
    use crate::rule::{Rule, RuleSet};
    use cqels_model::term::IriTerm;
    use cqels_model::Term;
    use std::time::Duration;

    fn iri(s: &str) -> Term {
        Term::Iri(IriTerm::new(s))
    }

    fn make_element(s: &str, p: &str, o: &str, ts: i64) -> RdfStreamElement {
        RdfStreamElement::new(
            Statement::new(iri(s), IriTerm::new(p), iri(o)),
            ts,
        )
    }

    #[test]
    fn test_single_pattern_rule() {
        let rule = Rule::builder()
            .id("type-to-human")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .default_window(Duration::from_secs(600))
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        let results = network.process_element(&elem);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].statement.subject, iri("http://ex.org/alice"));
        assert_eq!(results[0].statement.predicate.as_str(), "http://ex.org/is");
        assert_eq!(results[0].statement.object, iri("http://ex.org/Human"));
    }

    #[test]
    fn test_no_match() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/alice",
            "http://ex.org/knows",
            "http://ex.org/bob",
            1000,
        );
        let results = network.process_element(&elem);
        assert!(results.is_empty());
    }

    #[test]
    fn test_deduplication() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        let results1 = network.process_element(&elem);
        assert_eq!(results1.len(), 1);

        let results2 = network.process_element(&elem);
        assert!(results2.is_empty());
    }

    #[test]
    fn test_two_pattern_join_rule() {
        let rule = Rule::builder()
            .id("knows-inverse")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/knows")),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("o"),
                PatternTerm::constant(iri("http://ex.org/knownBy")),
                PatternTerm::var("s"),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .default_window(Duration::from_secs(600))
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem1 = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        let results1 = network.process_element(&elem1);
        assert!(results1.is_empty());

        let elem2 = make_element(
            "http://ex.org/alice",
            "http://ex.org/knows",
            "http://ex.org/bob",
            2000,
        );
        let results2 = network.process_element(&elem2);
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].statement.subject, iri("http://ex.org/bob"));
        assert_eq!(
            results2[0].statement.predicate.as_str(),
            "http://ex.org/knownBy"
        );
        assert_eq!(results2[0].statement.object, iri("http://ex.org/alice"));
    }

    #[test]
    fn test_windowed_eviction() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/knows")),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("o"),
                PatternTerm::constant(iri("http://ex.org/knownBy")),
                PatternTerm::var("s"),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .default_window(Duration::from_secs(5))
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem1 = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        network.process_element(&elem1);

        let elem2 = make_element(
            "http://ex.org/alice",
            "http://ex.org/knows",
            "http://ex.org/bob",
            7000,
        );
        network.process_element(&elem2);
        assert_eq!(network.working_memory().size(), 1);
    }

    #[test]
    fn test_provenance_tracking() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .track_provenance(true)
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        let results = network.process_element(&elem);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].inferred_by, "r1");
        assert_eq!(results[0].derived_from.len(), 1);
        assert_eq!(
            results[0].derived_from[0].subject,
            iri("http://ex.org/alice")
        );
    }

    #[test]
    fn test_recursive_inference() {
        let rule1 = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("x"),
                PatternTerm::constant(iri("http://ex.org/subClassOf")),
                PatternTerm::var("y"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("x"),
                PatternTerm::constant(iri("http://ex.org/related")),
                PatternTerm::var("y"),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule1]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .enable_recursive_inference(true)
            .max_recursion_depth(5)
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/Cat",
            "http://ex.org/subClassOf",
            "http://ex.org/Animal",
            1000,
        );
        let results = network.process_element(&elem);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].statement.subject, iri("http://ex.org/Cat"));
        assert_eq!(
            results[0].statement.predicate.as_str(),
            "http://ex.org/related"
        );
        assert_eq!(results[0].statement.object, iri("http://ex.org/Animal"));
    }

    #[test]
    fn test_multiple_rules() {
        let rule1 = Rule::builder()
            .id("r1")
            .priority(10)
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::constant(iri("http://ex.org/Human")),
            ))
            .build();

        let rule2 = Rule::builder()
            .id("r2")
            .priority(5)
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::constant(iri("http://ex.org/Person")),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/has")),
                PatternTerm::constant(iri("http://ex.org/Consciousness")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule1, rule2]);
        let config = ReasoningConfig::builder()
            .rule_set(rule_set)
            .conflict_resolution(ConflictResolution::All)
            .build();

        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/alice",
            "http://ex.org/type",
            "http://ex.org/Person",
            1000,
        );
        let results = network.process_element(&elem);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_clear_network() {
        let rule = Rule::builder()
            .id("r1")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/seen")),
                PatternTerm::constant(iri("http://ex.org/true")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element(
            "http://ex.org/a",
            "http://ex.org/p",
            "http://ex.org/b",
            1000,
        );
        network.process_element(&elem);
        assert!(!network.working_memory().is_empty());

        network.clear();
        assert!(network.working_memory().is_empty());
    }

    #[test]
    fn test_inferred_element_conversion() {
        let stmt = Statement::new(
            iri("http://ex.org/a"),
            IriTerm::new("http://ex.org/p"),
            iri("http://ex.org/b"),
        );
        let inferred = InferredRdfStreamElement::new(
            stmt.clone(),
            1000,
            "rule-1".to_string(),
            vec![stmt.clone()],
        );

        assert!(inferred.is_inferred());
        let rdf_elem = inferred.to_rdf_stream_element();
        assert_eq!(rdf_elem.statement, stmt);
        assert_eq!(rdf_elem.timestamp, 1000);
    }

    // ─── NEW TESTS ──────────────────────────────────────────────────────────

    #[test]
    fn test_empty_ruleset() {
        let rule_set = RuleSet::new(vec![]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element("http://s", "http://p", "http://o", 100);
        let results = network.process_element(&elem);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_constant_subject_pattern() {
        // Rule that only fires for a specific subject
        let rule = Rule::builder()
            .id("specific-subject")
            .pattern(TriplePattern::new(
                PatternTerm::constant(iri("http://ex.org/sensor1")),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::constant(iri("http://ex.org/sensor1")),
                PatternTerm::constant(iri("http://ex.org/matched")),
                PatternTerm::constant(iri("http://ex.org/true")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        // Should match
        let elem1 = make_element("http://ex.org/sensor1", "http://ex.org/temp", "42", 100);
        let r1 = network.process_element(&elem1);
        assert_eq!(r1.len(), 1);

        // Should NOT match (different subject)
        let elem2 = make_element("http://ex.org/sensor2", "http://ex.org/temp", "43", 200);
        let r2 = network.process_element(&elem2);
        assert_eq!(r2.len(), 0);
    }

    #[test]
    fn test_multiple_elements_same_rule() {
        let rule = Rule::builder()
            .id("all-match")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/type")),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/is")),
                PatternTerm::var("o"),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem1 = make_element("http://ex.org/a", "http://ex.org/type", "http://ex.org/X", 100);
        let elem2 = make_element("http://ex.org/b", "http://ex.org/type", "http://ex.org/Y", 200);
        let elem3 = make_element("http://ex.org/c", "http://ex.org/type", "http://ex.org/Z", 300);

        let r1 = network.process_element(&elem1);
        let r2 = network.process_element(&elem2);
        let r3 = network.process_element(&elem3);

        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r3.len(), 1);
        // Working memory should contain both input and inferred triples
        assert!(network.working_memory().size() >= 3);
    }

    #[test]
    fn test_inferred_element_provenance() {
        let rule = Rule::builder()
            .id("prov-test")
            .name("provenance-rule")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/seen")),
                PatternTerm::constant(iri("http://ex.org/true")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element("http://ex.org/a", "http://ex.org/p", "http://ex.org/b", 1000);
        let results = network.process_element(&elem);

        assert_eq!(results.len(), 1);
        let inferred = &results[0];
        assert_eq!(inferred.inferred_by, "prov-test");
        assert_eq!(inferred.timestamp, 1000);
        // Verify the inferred triple has the expected predicate
        assert_eq!(inferred.statement.predicate.as_str(), "http://ex.org/seen");
    }

    #[test]
    fn test_bulk_processing() {
        let rule = Rule::builder()
            .id("bulk-test")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/seen")),
                PatternTerm::constant(iri("http://ex.org/true")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let mut total_inferred = 0;
        for i in 0..5 {
            let elem = make_element(
                &format!("http://ex.org/{i}"),
                "http://ex.org/p",
                "http://ex.org/o",
                (i * 100) as i64,
            );
            total_inferred += network.process_element(&elem).len();
        }

        assert!(total_inferred >= 5, "Should infer at least 5 triples, got {total_inferred}");
        assert!(network.working_memory().size() > 0);
    }

    #[test]
    fn test_clear_then_reprocess() {
        let rule = Rule::builder()
            .id("r-clear")
            .pattern(TriplePattern::new(
                PatternTerm::var("s"),
                PatternTerm::var("p"),
                PatternTerm::var("o"),
            ))
            .template(TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/seen")),
                PatternTerm::constant(iri("http://ex.org/true")),
            ))
            .build();

        let rule_set = RuleSet::new(vec![rule]);
        let config = ReasoningConfig::default_config(rule_set);
        let mut network = ReteNetwork::compile(config);

        let elem = make_element("http://ex.org/a", "http://ex.org/p", "http://ex.org/b", 100);
        network.process_element(&elem);
        assert!(!network.working_memory().is_empty());

        network.clear();
        assert!(network.working_memory().is_empty());

        // Processing same element after clear should produce results again
        let results = network.process_element(&elem);
        assert_eq!(results.len(), 1);
    }
}
