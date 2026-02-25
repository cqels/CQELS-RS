use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use cqels_model::{Statement, Term};

use crate::pattern::{TriplePattern, TripleTemplate};

/// A filter predicate that tests variable bindings.
pub type BindingFilter = Box<dyn Fn(&HashMap<String, Term>) -> bool + Send + Sync>;

/// The condition (antecedent/IF part) of a rule.
pub struct RuleCondition {
    patterns: Vec<TriplePattern>,
    filters: Vec<BindingFilter>,
}

impl RuleCondition {
    pub fn new(patterns: Vec<TriplePattern>, filters: Vec<BindingFilter>) -> Self {
        Self { patterns, filters }
    }

    pub fn patterns(&self) -> &[TriplePattern] {
        &self.patterns
    }

    pub fn filters(&self) -> &[BindingFilter] {
        &self.filters
    }

    /// Returns all variable names used across all patterns.
    pub fn variable_names(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        for pattern in &self.patterns {
            for name in pattern.variable_names() {
                names.insert(name);
            }
        }
        names
    }

    /// Tests whether bindings pass all filter predicates.
    pub fn passes_filters(&self, bindings: &HashMap<String, Term>) -> bool {
        self.filters.iter().all(|f| f(bindings))
    }
}

impl fmt::Debug for RuleCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuleCondition")
            .field("patterns", &self.patterns)
            .field("filters_count", &self.filters.len())
            .finish()
    }
}

/// The consequent (THEN part) of a rule.
#[derive(Clone, Debug)]
pub struct RuleConsequent {
    templates: Vec<TripleTemplate>,
}

impl RuleConsequent {
    pub fn new(templates: Vec<TripleTemplate>) -> Self {
        Self { templates }
    }

    pub fn templates(&self) -> &[TripleTemplate] {
        &self.templates
    }

    /// Instantiates all templates with bindings, returning produced statements.
    /// Skips templates that cannot be fully instantiated.
    pub fn instantiate(&self, bindings: &HashMap<String, Term>) -> Vec<Statement> {
        self.templates
            .iter()
            .filter_map(|t| t.instantiate(bindings))
            .collect()
    }
}

/// A reasoning rule with a condition and consequent.
pub struct Rule {
    id: String,
    name: String,
    condition: RuleCondition,
    consequent: RuleConsequent,
    priority: i32,
    time_window: Option<Duration>,
}

impl Rule {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn condition(&self) -> &RuleCondition {
        &self.condition
    }

    pub fn consequent(&self) -> &RuleConsequent {
        &self.consequent
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn time_window(&self) -> Option<Duration> {
        self.time_window
    }

    pub fn builder() -> RuleBuilder {
        RuleBuilder::default()
    }
}

impl fmt::Debug for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rule")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("condition", &self.condition)
            .field("consequent", &self.consequent)
            .finish()
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Rule[id={}, name={}, priority={}]", self.id, self.name, self.priority)
    }
}

impl PartialEq for Rule {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Rule {}

impl std::hash::Hash for Rule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Builder for `Rule`.
#[derive(Default)]
pub struct RuleBuilder {
    id: Option<String>,
    name: Option<String>,
    patterns: Vec<TriplePattern>,
    filters: Vec<BindingFilter>,
    templates: Vec<TripleTemplate>,
    priority: i32,
    time_window: Option<Duration>,
}

impl RuleBuilder {
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn pattern(mut self, pattern: TriplePattern) -> Self {
        self.patterns.push(pattern);
        self
    }

    pub fn filter(mut self, filter: BindingFilter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn template(mut self, template: TripleTemplate) -> Self {
        self.templates.push(template);
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn time_window(mut self, window: Duration) -> Self {
        self.time_window = Some(window);
        self
    }

    pub fn build(self) -> Rule {
        let id = self.id.unwrap_or_else(uuid_like_id);
        let name = self.name.unwrap_or_else(|| id.clone());
        Rule {
            id,
            name,
            condition: RuleCondition::new(self.patterns, self.filters),
            consequent: RuleConsequent::new(self.templates),
            priority: self.priority,
            time_window: self.time_window,
        }
    }
}

fn uuid_like_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("rule-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// A collection of rules, sorted by priority (highest first).
pub struct RuleSet {
    rules: Vec<Rule>,
}

impl RuleSet {
    pub fn new(mut rules: Vec<Rule>) -> Self {
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Self { rules }
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    pub fn size(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn get_by_id(&self, id: &str) -> Option<&Rule> {
        self.rules.iter().find(|r| r.id == id)
    }

    /// Returns all unique triple patterns across all rules.
    pub fn all_patterns(&self) -> HashSet<TriplePattern> {
        let mut patterns = HashSet::new();
        for rule in &self.rules {
            for pattern in rule.condition.patterns() {
                patterns.insert(pattern.clone());
            }
        }
        patterns
    }

    pub fn builder() -> RuleSetBuilder {
        RuleSetBuilder::default()
    }
}

impl fmt::Display for RuleSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RuleSet[{} rules]", self.rules.len())
    }
}

impl fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuleSet")
            .field("rules_count", &self.rules.len())
            .finish()
    }
}

/// Builder for `RuleSet`.
#[derive(Default)]
pub struct RuleSetBuilder {
    rules: Vec<Rule>,
}

impl RuleSetBuilder {
    pub fn add_rule(mut self, rule: Rule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn build(self) -> RuleSet {
        RuleSet::new(self.rules)
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

    #[test]
    fn test_rule_builder() {
        let rule = Rule::builder()
            .id("r1")
            .name("type-inference")
            .priority(10)
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

        assert_eq!(rule.id(), "r1");
        assert_eq!(rule.name(), "type-inference");
        assert_eq!(rule.priority(), 10);
        assert_eq!(rule.condition().patterns().len(), 1);
        assert_eq!(rule.consequent().templates().len(), 1);
    }

    #[test]
    fn test_rule_condition_filters() {
        let condition = RuleCondition::new(
            vec![],
            vec![Box::new(|bindings: &HashMap<String, Term>| {
                bindings.contains_key("x")
            })],
        );
        let mut bindings = HashMap::new();
        assert!(!condition.passes_filters(&bindings));

        bindings.insert("x".to_string(), iri("http://ex.org/val"));
        assert!(condition.passes_filters(&bindings));
    }

    #[test]
    fn test_rule_consequent_instantiate() {
        let consequent = RuleConsequent::new(vec![
            TripleTemplate::new(
                PatternTerm::var("s"),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::var("o"),
            ),
            TripleTemplate::new(
                PatternTerm::var("missing"),
                PatternTerm::constant(iri("http://ex.org/p")),
                PatternTerm::var("o"),
            ),
        ]);

        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://ex.org/a"));
        bindings.insert("o".to_string(), iri("http://ex.org/b"));

        let stmts = consequent.instantiate(&bindings);
        // Only the first template can be fully instantiated
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].subject, iri("http://ex.org/a"));
    }

    #[test]
    fn test_ruleset_sorted_by_priority() {
        let r1 = Rule::builder().id("r1").priority(5).build();
        let r2 = Rule::builder().id("r2").priority(10).build();
        let r3 = Rule::builder().id("r3").priority(1).build();

        let ruleset = RuleSet::new(vec![r1, r2, r3]);
        assert_eq!(ruleset.rules()[0].id(), "r2"); // highest priority first
        assert_eq!(ruleset.rules()[1].id(), "r1");
        assert_eq!(ruleset.rules()[2].id(), "r3");
    }

    #[test]
    fn test_ruleset_all_patterns() {
        let p1 = TriplePattern::new(
            PatternTerm::var("s"),
            PatternTerm::constant(iri("http://ex.org/type")),
            PatternTerm::var("o"),
        );
        let p2 = TriplePattern::new(
            PatternTerm::var("x"),
            PatternTerm::var("y"),
            PatternTerm::var("z"),
        );

        let r1 = Rule::builder().id("r1").pattern(p1.clone()).build();
        let r2 = Rule::builder().id("r2").pattern(p2.clone()).pattern(p1.clone()).build();

        let ruleset = RuleSet::new(vec![r1, r2]);
        let patterns = ruleset.all_patterns();
        assert_eq!(patterns.len(), 2); // p1 is deduplicated
    }
}
