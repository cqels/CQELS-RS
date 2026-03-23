//! Pre-built reasoning profiles implementing standard W3C entailment regimes.
//!
//! Each [`ReasoningProfile`] provides a curated set of [`Rule`]s suitable for
//! a particular level of RDF/OWL reasoning. Profiles can be composed via
//! [`ReasoningProfile::compose`] to build hybrid rule sets.

use std::collections::HashSet;

use crate::config::ReasoningConfig;
use crate::pattern::{PatternTerm, TriplePattern, TripleTemplate};
use crate::rule::{Rule, RuleSet};
use cqels_model::term::IriTerm;
use cqels_model::Term;

// ── Namespace constants ──────────────────────────────────────────────

const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn iri(ns: &str, local: &str) -> Term {
    Term::Iri(IriTerm::new(format!("{ns}{local}")))
}

fn iri_pt(ns: &str, local: &str) -> PatternTerm {
    PatternTerm::constant(iri(ns, local))
}

fn var(name: &str) -> PatternTerm {
    PatternTerm::var(name)
}

// ── ReasoningProfile ─────────────────────────────────────────────────

/// Pre-built reasoning profiles aligned with W3C entailment regimes.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::ReasoningProfile;
///
/// let profile = ReasoningProfile::Rdfs;
/// assert_eq!(profile.name(), "RDFS");
/// assert_eq!(profile.rules().len(), 6);
///
/// let config = profile.create_config();
/// assert!(config.enable_recursive_inference());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReasoningProfile {
    /// No reasoning rules.
    None,
    /// Core RDFS — 6 rules (rdfs2, rdfs3, rdfs5, rdfs7, rdfs9, rdfs11).
    Rdfs,
    /// Full RDFS — 11 rules (adds rdfs1, rdfs4, rdfs8/10, rdfs12, rdfs13).
    RdfsFull,
    /// OWL-Lite — 16 rules (RDFS + equivalentClass/Property, transitive, symmetric, inverse, sameAs).
    OwlLite,
    /// OWL-QL — 14 rules (RDFS + equivalentClass/Property, inverseOf, someValuesFrom, disjointWith).
    OwlQl,
    /// OWL 2 EL — 15 rules (RDFS-Full + equivalentClass/Property).
    Owl2El,
    /// OWL 2 RL — 23 rules (RDFS-Full + OWL-Lite + reflexive, hasValue).
    Owl2Rl,
}

impl ReasoningProfile {
    /// Human-readable name.
    pub fn name(&self) -> &str {
        match self {
            Self::None => "None",
            Self::Rdfs => "RDFS",
            Self::RdfsFull => "RDFS-Full",
            Self::OwlLite => "OWL-Lite",
            Self::OwlQl => "OWL-QL",
            Self::Owl2El => "OWL2-EL",
            Self::Owl2Rl => "OWL2-RL",
        }
    }

    /// Short description of what this profile covers.
    pub fn description(&self) -> &str {
        match self {
            Self::None => "No reasoning rules",
            Self::Rdfs => "Core RDFS entailment (6 rules): subproperty, domain/range, subclass",
            Self::RdfsFull => {
                "Full RDFS entailment (11 rules): core RDFS plus axiomatic triples"
            }
            Self::OwlLite => {
                "OWL-Lite entailment (16 rules): RDFS plus equivalentClass/Property, transitive, symmetric, inverse, sameAs"
            }
            Self::OwlQl => {
                "OWL-QL entailment (14 rules): RDFS plus equivalentClass/Property, inverseOf, someValuesFrom, disjointWith"
            }
            Self::Owl2El => {
                "OWL 2 EL entailment (15 rules): RDFS-Full plus equivalentClass/Property"
            }
            Self::Owl2Rl => {
                "OWL 2 RL entailment (23 rules): RDFS-Full plus full OWL property axioms and hasValue"
            }
        }
    }

    /// Returns the entailment rules for this profile.
    pub fn rules(&self) -> Vec<Rule> {
        match self {
            Self::None => vec![],
            Self::Rdfs => rdfs_rules(),
            Self::RdfsFull => rdfs_full_rules(),
            Self::OwlLite => owl_lite_rules(),
            Self::OwlQl => owl_ql_rules(),
            Self::Owl2El => owl2_el_rules(),
            Self::Owl2Rl => owl2_rl_rules(),
        }
    }

    /// Returns a [`RuleSet`] for this profile.
    pub fn rule_set(&self) -> RuleSet {
        RuleSet::new(self.rules())
    }

    /// Whether this profile requires recursive (fixed-point) inference.
    pub fn requires_recursive_inference(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Creates a [`ReasoningConfig`] with suitable defaults for this profile.
    pub fn create_config(&self) -> ReasoningConfig {
        let max_depth = match self {
            Self::OwlLite | Self::Owl2Rl => 15,
            _ => 10,
        };
        let mut builder = ReasoningConfig::builder()
            .rule_set(self.rule_set())
            .enable_recursive_inference(self.requires_recursive_inference())
            .max_recursion_depth(max_depth);
        builder = builder.profile(*self);
        builder.build()
    }

    /// Composes multiple profiles into a single [`RuleSet`], deduplicating by rule ID.
    pub fn compose(profiles: &[ReasoningProfile]) -> RuleSet {
        let mut seen = HashSet::new();
        let mut rules = Vec::new();
        for profile in profiles {
            for rule in profile.rules() {
                if seen.insert(rule.id().to_string()) {
                    rules.push(rule);
                }
            }
        }
        RuleSet::new(rules)
    }
}

impl std::fmt::Display for ReasoningProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ── Rule factories ───────────────────────────────────────────────────

/// Core RDFS rules (6 rules).
fn rdfs_rules() -> Vec<Rule> {
    vec![
        // rdfs2: ?p rdfs:domain ?c, ?x ?p ?y → ?x rdf:type ?c
        Rule::builder()
            .id("rdfs2")
            .name("RDFS Domain")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDFS, "domain"),
                var("c"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("x"), iri_pt(RDF, "type"), var("c")))
            .build(),
        // rdfs3: ?p rdfs:range ?c, ?x ?p ?y → ?y rdf:type ?c
        Rule::builder()
            .id("rdfs3")
            .name("RDFS Range")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDFS, "range"),
                var("c"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), iri_pt(RDF, "type"), var("c")))
            .build(),
        // rdfs5: ?p rdfs:subPropertyOf ?q, ?q rdfs:subPropertyOf ?r → ?p rdfs:subPropertyOf ?r
        Rule::builder()
            .id("rdfs5")
            .name("SubProperty Transitivity")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                var("q"),
            ))
            .pattern(TriplePattern::new(
                var("q"),
                iri_pt(RDFS, "subPropertyOf"),
                var("r"),
            ))
            .template(TripleTemplate::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                var("r"),
            ))
            .build(),
        // rdfs7: ?p rdfs:subPropertyOf ?superP, ?s ?p ?o → ?s ?superP ?o
        Rule::builder()
            .id("rdfs7")
            .name("SubProperty Propagation")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                var("superP"),
            ))
            .pattern(TriplePattern::new(var("s"), var("p"), var("o")))
            .template(TripleTemplate::new(var("s"), var("superP"), var("o")))
            .build(),
        // rdfs9: ?c rdfs:subClassOf ?d, ?x rdf:type ?c → ?x rdf:type ?d
        Rule::builder()
            .id("rdfs9")
            .name("SubClass Type Propagation")
            .priority(10)
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(RDFS, "subClassOf"),
                var("d"),
            ))
            .pattern(TriplePattern::new(var("x"), iri_pt(RDF, "type"), var("c")))
            .template(TripleTemplate::new(var("x"), iri_pt(RDF, "type"), var("d")))
            .build(),
        // rdfs11: ?c rdfs:subClassOf ?d, ?d rdfs:subClassOf ?e → ?c rdfs:subClassOf ?e
        Rule::builder()
            .id("rdfs11")
            .name("SubClass Transitivity")
            .priority(10)
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(RDFS, "subClassOf"),
                var("d"),
            ))
            .pattern(TriplePattern::new(
                var("d"),
                iri_pt(RDFS, "subClassOf"),
                var("e"),
            ))
            .template(TripleTemplate::new(
                var("c"),
                iri_pt(RDFS, "subClassOf"),
                var("e"),
            ))
            .build(),
    ]
}

/// Additional RDFS-Full rules (beyond core RDFS).
fn rdfs_full_extra_rules() -> Vec<Rule> {
    vec![
        // rdfs1: ?s ?p ?o → ?p rdf:type rdf:Property, ?p rdfs:subPropertyOf ?p
        Rule::builder()
            .id("rdfs1")
            .name("Predicate is Property + self-subProperty")
            .priority(5)
            .pattern(TriplePattern::new(var("s"), var("p"), var("o")))
            .template(TripleTemplate::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(RDF, "Property"),
            ))
            .template(TripleTemplate::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                var("p"),
            ))
            .build(),
        // rdfs4: ?s ?p ?o → ?s rdf:type rdfs:Resource, ?o rdf:type rdfs:Resource
        Rule::builder()
            .id("rdfs4")
            .name("Subject and Object are Resources")
            .priority(5)
            .pattern(TriplePattern::new(var("s"), var("p"), var("o")))
            .template(TripleTemplate::new(
                var("s"),
                iri_pt(RDF, "type"),
                iri_pt(RDFS, "Resource"),
            ))
            .template(TripleTemplate::new(
                var("o"),
                iri_pt(RDF, "type"),
                iri_pt(RDFS, "Resource"),
            ))
            .build(),
        // rdfs8+rdfs10: ?c rdf:type rdfs:Class → ?c rdfs:subClassOf rdfs:Resource, ?c rdfs:subClassOf ?c
        Rule::builder()
            .id("rdfs8-10")
            .name("Class subClassOf Resource + self")
            .priority(5)
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(RDF, "type"),
                iri_pt(RDFS, "Class"),
            ))
            .template(TripleTemplate::new(
                var("c"),
                iri_pt(RDFS, "subClassOf"),
                iri_pt(RDFS, "Resource"),
            ))
            .template(TripleTemplate::new(
                var("c"),
                iri_pt(RDFS, "subClassOf"),
                var("c"),
            ))
            .build(),
        // rdfs12: ?p rdf:type rdfs:ContainerMembershipProperty → ?p rdfs:subPropertyOf rdfs:member
        Rule::builder()
            .id("rdfs12")
            .name("ContainerMembership subPropertyOf member")
            .priority(5)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(RDFS, "ContainerMembershipProperty"),
            ))
            .template(TripleTemplate::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                iri_pt(RDFS, "member"),
            ))
            .build(),
        // rdfs13: ?d rdf:type rdfs:Datatype → ?d rdfs:subClassOf rdfs:Literal
        Rule::builder()
            .id("rdfs13")
            .name("Datatype subClassOf Literal")
            .priority(5)
            .pattern(TriplePattern::new(
                var("d"),
                iri_pt(RDF, "type"),
                iri_pt(RDFS, "Datatype"),
            ))
            .template(TripleTemplate::new(
                var("d"),
                iri_pt(RDFS, "subClassOf"),
                iri_pt(RDFS, "Literal"),
            ))
            .build(),
    ]
}

/// Full RDFS (11 rules).
fn rdfs_full_rules() -> Vec<Rule> {
    let mut rules = rdfs_rules();
    rules.extend(rdfs_full_extra_rules());
    rules
}

/// OWL property rules shared between OWL-Lite and OWL 2 RL.
fn owl_equivalent_class_property_rules(prefix: &str) -> Vec<Rule> {
    vec![
        Rule::builder()
            .id(format!("{prefix}-equivalentClass-fwd"))
            .name("EquivalentClass forward")
            .priority(10)
            .pattern(TriplePattern::new(
                var("a"),
                iri_pt(OWL, "equivalentClass"),
                var("b"),
            ))
            .template(TripleTemplate::new(
                var("a"),
                iri_pt(RDFS, "subClassOf"),
                var("b"),
            ))
            .build(),
        Rule::builder()
            .id(format!("{prefix}-equivalentClass-rev"))
            .name("EquivalentClass reverse")
            .priority(10)
            .pattern(TriplePattern::new(
                var("a"),
                iri_pt(OWL, "equivalentClass"),
                var("b"),
            ))
            .template(TripleTemplate::new(
                var("b"),
                iri_pt(RDFS, "subClassOf"),
                var("a"),
            ))
            .build(),
        Rule::builder()
            .id(format!("{prefix}-equivalentProperty-fwd"))
            .name("EquivalentProperty forward")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "equivalentProperty"),
                var("q"),
            ))
            .template(TripleTemplate::new(
                var("p"),
                iri_pt(RDFS, "subPropertyOf"),
                var("q"),
            ))
            .build(),
        Rule::builder()
            .id(format!("{prefix}-equivalentProperty-rev"))
            .name("EquivalentProperty reverse")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "equivalentProperty"),
                var("q"),
            ))
            .template(TripleTemplate::new(
                var("q"),
                iri_pt(RDFS, "subPropertyOf"),
                var("p"),
            ))
            .build(),
    ]
}

/// OWL-Lite (16 rules).
fn owl_lite_rules() -> Vec<Rule> {
    let mut rules = rdfs_rules();
    rules.extend(owl_equivalent_class_property_rules("owl"));

    // owl-transitiveProperty: ?p rdf:type owl:TransitiveProperty, ?x ?p ?y, ?y ?p ?z → ?x ?p ?z
    rules.push(
        Rule::builder()
            .id("owl-transitiveProperty")
            .name("Transitive Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "TransitiveProperty"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .pattern(TriplePattern::new(var("y"), var("p"), var("z")))
            .template(TripleTemplate::new(var("x"), var("p"), var("z")))
            .build(),
    );

    // owl-symmetricProperty: ?p rdf:type owl:SymmetricProperty, ?x ?p ?y → ?y ?p ?x
    rules.push(
        Rule::builder()
            .id("owl-symmetricProperty")
            .name("Symmetric Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "SymmetricProperty"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), var("p"), var("x")))
            .build(),
    );

    // owl-inverseOf: ?p owl:inverseOf ?q, ?x ?p ?y → ?y ?q ?x
    rules.push(
        Rule::builder()
            .id("owl-inverseOf")
            .name("Inverse Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "inverseOf"),
                var("q"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), var("q"), var("x")))
            .build(),
    );

    // owl-sameAs-propagation: ?x owl:sameAs ?y, ?x ?p ?o → ?y ?p ?o
    rules.push(
        Rule::builder()
            .id("owl-sameAs-propagation")
            .name("SameAs Propagation")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("o")))
            .template(TripleTemplate::new(var("y"), var("p"), var("o")))
            .build(),
    );

    // owl-sameAs-symmetry: ?x owl:sameAs ?y → ?y owl:sameAs ?x
    rules.push(
        Rule::builder()
            .id("owl-sameAs-symmetry")
            .name("SameAs Symmetry")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .template(TripleTemplate::new(
                var("y"),
                iri_pt(OWL, "sameAs"),
                var("x"),
            ))
            .build(),
    );

    // owl-sameAs-transitivity: ?x owl:sameAs ?y, ?y owl:sameAs ?z → ?x owl:sameAs ?z
    rules.push(
        Rule::builder()
            .id("owl-sameAs-transitivity")
            .name("SameAs Transitivity")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .pattern(TriplePattern::new(
                var("y"),
                iri_pt(OWL, "sameAs"),
                var("z"),
            ))
            .template(TripleTemplate::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("z"),
            ))
            .build(),
    );

    rules
}

/// OWL-QL (14 rules).
fn owl_ql_rules() -> Vec<Rule> {
    let mut rules = rdfs_rules();
    rules.extend(owl_equivalent_class_property_rules("owl-ql"));

    // owl-ql-inverseOf: ?p owl:inverseOf ?q, ?x ?p ?y → ?y ?q ?x
    rules.push(
        Rule::builder()
            .id("owl-ql-inverseOf")
            .name("OWL-QL InverseOf")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "inverseOf"),
                var("q"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), var("q"), var("x")))
            .build(),
    );

    // owl-ql-inverseOf-rev: ?p owl:inverseOf ?q, ?y ?q ?x → ?x ?p ?y
    rules.push(
        Rule::builder()
            .id("owl-ql-inverseOf-rev")
            .name("OWL-QL InverseOf Reverse")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "inverseOf"),
                var("q"),
            ))
            .pattern(TriplePattern::new(var("y"), var("q"), var("x")))
            .template(TripleTemplate::new(var("x"), var("p"), var("y")))
            .build(),
    );

    // owl-ql-someValuesFrom: ?c owl:onProperty ?p, ?c owl:someValuesFrom owl:Thing, ?x ?p ?y → ?x rdf:type ?c
    rules.push(
        Rule::builder()
            .id("owl-ql-someValuesFrom")
            .name("OWL-QL SomeValuesFrom")
            .priority(10)
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(OWL, "onProperty"),
                var("p"),
            ))
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(OWL, "someValuesFrom"),
                iri_pt(OWL, "Thing"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("x"), iri_pt(RDF, "type"), var("c")))
            .build(),
    );

    // owl-ql-disjointWith: ?a owl:disjointWith ?b, ?x rdf:type ?a, ?x rdf:type ?b → ?x rdf:type owl:Nothing
    rules.push(
        Rule::builder()
            .id("owl-ql-disjointWith")
            .name("OWL-QL DisjointWith")
            .priority(10)
            .pattern(TriplePattern::new(
                var("a"),
                iri_pt(OWL, "disjointWith"),
                var("b"),
            ))
            .pattern(TriplePattern::new(var("x"), iri_pt(RDF, "type"), var("a")))
            .pattern(TriplePattern::new(var("x"), iri_pt(RDF, "type"), var("b")))
            .template(TripleTemplate::new(
                var("x"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "Nothing"),
            ))
            .build(),
    );

    rules
}

/// OWL 2 EL (15 rules).
fn owl2_el_rules() -> Vec<Rule> {
    let mut rules = rdfs_full_rules();
    rules.extend(owl_equivalent_class_property_rules("owl-el"));
    rules
}

/// OWL 2 RL (23 rules).
fn owl2_rl_rules() -> Vec<Rule> {
    let mut rules = rdfs_full_rules();
    rules.extend(owl_equivalent_class_property_rules("owl-rl"));

    // owl-rl-transitiveProperty
    rules.push(
        Rule::builder()
            .id("owl-rl-transitiveProperty")
            .name("OWL-RL Transitive Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "TransitiveProperty"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .pattern(TriplePattern::new(var("y"), var("p"), var("z")))
            .template(TripleTemplate::new(var("x"), var("p"), var("z")))
            .build(),
    );

    // owl-rl-symmetricProperty
    rules.push(
        Rule::builder()
            .id("owl-rl-symmetricProperty")
            .name("OWL-RL Symmetric Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "SymmetricProperty"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), var("p"), var("x")))
            .build(),
    );

    // owl-rl-inverseOf
    rules.push(
        Rule::builder()
            .id("owl-rl-inverseOf")
            .name("OWL-RL InverseOf")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(OWL, "inverseOf"),
                var("q"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("y")))
            .template(TripleTemplate::new(var("y"), var("q"), var("x")))
            .build(),
    );

    // owl-rl-sameAs-propagation
    rules.push(
        Rule::builder()
            .id("owl-rl-sameAs-propagation")
            .name("OWL-RL SameAs Propagation")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .pattern(TriplePattern::new(var("x"), var("p"), var("o")))
            .template(TripleTemplate::new(var("y"), var("p"), var("o")))
            .build(),
    );

    // owl-rl-sameAs-symmetry
    rules.push(
        Rule::builder()
            .id("owl-rl-sameAs-symmetry")
            .name("OWL-RL SameAs Symmetry")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .template(TripleTemplate::new(
                var("y"),
                iri_pt(OWL, "sameAs"),
                var("x"),
            ))
            .build(),
    );

    // owl-rl-sameAs-transitivity
    rules.push(
        Rule::builder()
            .id("owl-rl-sameAs-transitivity")
            .name("OWL-RL SameAs Transitivity")
            .priority(10)
            .pattern(TriplePattern::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("y"),
            ))
            .pattern(TriplePattern::new(
                var("y"),
                iri_pt(OWL, "sameAs"),
                var("z"),
            ))
            .template(TripleTemplate::new(
                var("x"),
                iri_pt(OWL, "sameAs"),
                var("z"),
            ))
            .build(),
    );

    // owl-rl-reflexiveProperty: ?p rdf:type owl:ReflexiveProperty, ?s ?p ?o → ?s ?p ?s, ?o ?p ?o
    rules.push(
        Rule::builder()
            .id("owl-rl-reflexiveProperty")
            .name("OWL-RL Reflexive Property")
            .priority(10)
            .pattern(TriplePattern::new(
                var("p"),
                iri_pt(RDF, "type"),
                iri_pt(OWL, "ReflexiveProperty"),
            ))
            .pattern(TriplePattern::new(var("s"), var("p"), var("o")))
            .template(TripleTemplate::new(var("s"), var("p"), var("s")))
            .template(TripleTemplate::new(var("o"), var("p"), var("o")))
            .build(),
    );

    // owl-rl-hasValue: ?c owl:onProperty ?p, ?c owl:hasValue ?v, ?x rdf:type ?c → ?x ?p ?v
    rules.push(
        Rule::builder()
            .id("owl-rl-hasValue")
            .name("OWL-RL HasValue")
            .priority(10)
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(OWL, "onProperty"),
                var("p"),
            ))
            .pattern(TriplePattern::new(
                var("c"),
                iri_pt(OWL, "hasValue"),
                var("v"),
            ))
            .pattern(TriplePattern::new(var("x"), iri_pt(RDF, "type"), var("c")))
            .template(TripleTemplate::new(var("x"), var("p"), var("v")))
            .build(),
    );

    rules
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_profile() {
        let p = ReasoningProfile::None;
        assert_eq!(p.name(), "None");
        assert!(p.rules().is_empty());
        assert!(!p.requires_recursive_inference());
    }

    #[test]
    fn test_rdfs_profile_rule_count() {
        assert_eq!(ReasoningProfile::Rdfs.rules().len(), 6);
    }

    #[test]
    fn test_rdfs_full_profile_rule_count() {
        assert_eq!(ReasoningProfile::RdfsFull.rules().len(), 11);
    }

    #[test]
    fn test_owl_lite_profile_rule_count() {
        assert_eq!(ReasoningProfile::OwlLite.rules().len(), 16);
    }

    #[test]
    fn test_owl_ql_profile_rule_count() {
        assert_eq!(ReasoningProfile::OwlQl.rules().len(), 14);
    }

    #[test]
    fn test_owl2_el_profile_rule_count() {
        assert_eq!(ReasoningProfile::Owl2El.rules().len(), 15);
    }

    #[test]
    fn test_owl2_rl_profile_rule_count() {
        assert_eq!(ReasoningProfile::Owl2Rl.rules().len(), 23);
    }

    #[test]
    fn test_profiles_require_recursive_inference() {
        assert!(ReasoningProfile::Rdfs.requires_recursive_inference());
        assert!(ReasoningProfile::Owl2Rl.requires_recursive_inference());
        assert!(!ReasoningProfile::None.requires_recursive_inference());
    }

    #[test]
    fn test_create_config() {
        let config = ReasoningProfile::Rdfs.create_config();
        assert!(config.enable_recursive_inference());
        assert_eq!(config.max_recursion_depth(), 10);
        assert_eq!(config.rule_set().size(), 6);
    }

    #[test]
    fn test_create_config_owl2_rl_depth() {
        let config = ReasoningProfile::Owl2Rl.create_config();
        assert_eq!(config.max_recursion_depth(), 15);
    }

    #[test]
    fn test_compose_deduplicates() {
        let composed =
            ReasoningProfile::compose(&[ReasoningProfile::Rdfs, ReasoningProfile::RdfsFull]);
        // RdfsFull includes all Rdfs rules, so no duplicates
        assert_eq!(composed.size(), 11);
    }

    #[test]
    fn test_compose_empty() {
        let composed = ReasoningProfile::compose(&[]);
        assert!(composed.is_empty());
    }

    #[test]
    fn test_rule_ids_unique_within_profile() {
        for profile in &[
            ReasoningProfile::Rdfs,
            ReasoningProfile::RdfsFull,
            ReasoningProfile::OwlLite,
            ReasoningProfile::OwlQl,
            ReasoningProfile::Owl2El,
            ReasoningProfile::Owl2Rl,
        ] {
            let rules = profile.rules();
            let ids: HashSet<_> = rules.iter().map(|r| r.id()).collect();
            assert_eq!(
                ids.len(),
                rules.len(),
                "duplicate rule IDs in profile {}",
                profile.name()
            );
        }
    }

    #[test]
    fn test_rdfs7_subproperty_propagation() {
        use cqels_model::term::IriTerm;
        use std::collections::HashMap;

        let rules = ReasoningProfile::Rdfs.rules();
        let rdfs7 = rules.iter().find(|r| r.id() == "rdfs7").unwrap();

        // Check patterns
        assert_eq!(rdfs7.condition().patterns().len(), 2);

        // Simulate: :p rdfs:subPropertyOf :superP, :s :p :o → :s :superP :o
        let mut bindings = HashMap::new();
        bindings.insert("s".to_string(), iri("http://example.org/", "s"));
        bindings.insert(
            "p".to_string(),
            Term::Iri(IriTerm::new("http://example.org/p")),
        );
        bindings.insert("o".to_string(), iri("http://example.org/", "o"));
        bindings.insert(
            "superP".to_string(),
            Term::Iri(IriTerm::new("http://example.org/superP")),
        );

        let inferred = rdfs7.consequent().instantiate(&bindings);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].predicate.as_str(), "http://example.org/superP");
    }

    #[test]
    fn test_profile_display() {
        assert_eq!(ReasoningProfile::Rdfs.to_string(), "RDFS");
        assert_eq!(ReasoningProfile::Owl2Rl.to_string(), "OWL2-RL");
    }

    #[test]
    fn test_profile_description_non_empty() {
        for profile in &[
            ReasoningProfile::None,
            ReasoningProfile::Rdfs,
            ReasoningProfile::RdfsFull,
            ReasoningProfile::OwlLite,
            ReasoningProfile::OwlQl,
            ReasoningProfile::Owl2El,
            ReasoningProfile::Owl2Rl,
        ] {
            assert!(
                !profile.description().is_empty(),
                "empty description for {}",
                profile.name()
            );
        }
    }

    #[test]
    fn test_rule_set_from_profile() {
        let rs = ReasoningProfile::Rdfs.rule_set();
        assert_eq!(rs.size(), 6);
        assert!(rs.get_by_id("rdfs2").is_some());
        assert!(rs.get_by_id("rdfs11").is_some());
    }
}
