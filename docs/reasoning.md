# Reasoning

> **Prerequisites**: [Data Model](data-model.md), [Stream Processing](stream-processing.md)
>
> **Next steps**: [Advanced](advanced.md)

The `cqels-reasoning` crate implements a RETE network for forward-chaining
incremental inference over RDF streams. As new triples arrive, the engine
matches them against rules and produces inferred triples in real time.

## Overview

The RETE algorithm is a classic pattern-matching architecture. In CQELS,
it is adapted for streaming data with:

- **Windowed working memory** -- Facts expire after a configurable duration
- **Provenance tracking** -- Each inferred triple records which rule and
  which input facts produced it
- **Incremental processing** -- Only new facts are matched, avoiding
  re-evaluation of the entire knowledge base

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Rule** | An if-then rule: if triple patterns match, produce new triples |
| **TriplePattern** | A pattern with variables and constants that matches triples |
| **TripleTemplate** | A template for producing new triples from variable bindings |
| **Alpha network** | Filters individual triples against single patterns |
| **Beta network** | Joins matches across multiple patterns on shared variables |
| **Working memory** | Indexed storage of currently active facts |
| **Activation** | A fully matched rule ready to fire |

## Defining Rules

### PatternTerm

Each position in a pattern is either a variable or a constant:

```rust
use cqels_reasoning::PatternTerm;
use cqels_model::term::{IriTerm, Term};

let var = PatternTerm::Variable("x".into());
let constant = PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/type")));
```

### TriplePattern

A pattern that matches against incoming RDF triples:

```rust
use cqels_reasoning::TriplePattern;

// Match: ?sensor <http://example.org/temperature> ?temp
let pattern = TriplePattern::new(
    PatternTerm::Variable("sensor".into()),
    PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
    PatternTerm::Variable("temp".into()),
);

// Test against a statement
let bindings = pattern.match_statement(&stmt);
// Some({"sensor": Term::Iri(...), "temp": Term::Literal(...)})
```

### TripleTemplate

A template for producing inferred triples:

```rust
use cqels_reasoning::TripleTemplate;

// Template: ?sensor <hasAlert> <HighTemperature>
let template = TripleTemplate::new(
    PatternTerm::Variable("sensor".into()),
    PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasAlert"))),
    PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/HighTemperature"))),
);

// Instantiate with bindings
let stmt = template.instantiate(&bindings); // Some(Statement)
```

### Building Rules

Use the builder pattern to define rules:

```rust
use cqels_reasoning::Rule;

let rule = Rule::builder()
    .name("high_temperature_alert")
    .priority(1)  // higher priority fires first
    .pattern(TriplePattern::new(
        PatternTerm::Variable("sensor".into()),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
        PatternTerm::Variable("temp".into()),
    ))
    .template(TripleTemplate::new(
        PatternTerm::Variable("sensor".into()),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasAlert"))),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/HighTemperature"))),
    ))
    .build();
```

### Multi-Pattern Rules (Joins)

Rules with multiple patterns perform joins on shared variables:

```rust
// If a sensor has BOTH temperature AND humidity, infer a complete reading
let rule = Rule::builder()
    .name("complete_reading")
    .priority(2)
    .pattern(TriplePattern::new(
        PatternTerm::Variable("sensor".into()),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
        PatternTerm::Variable("temp".into()),
    ))
    .pattern(TriplePattern::new(
        PatternTerm::Variable("sensor".into()),  // shared variable!
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/humidity"))),
        PatternTerm::Variable("hum".into()),
    ))
    .template(TripleTemplate::new(
        PatternTerm::Variable("sensor".into()),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasCompleteReading"))),
        PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/Complete"))),
    ))
    .build();
```

The beta network joins on `?sensor` -- it fires only when the same sensor
has both a temperature and humidity reading in working memory.

### RuleSet

Group rules into a set:

```rust
use cqels_reasoning::RuleSet;

let rule_set = RuleSet::builder()
    .add_rule(alert_rule)
    .add_rule(reading_rule)
    .build();

println!("Rules: {}", rule_set.rules().len());
println!("Total patterns: {}", rule_set.all_patterns().len());
```

## Configuring the Engine

```rust
use std::time::Duration;
use cqels_reasoning::{ReasoningConfig, ConflictResolution};

let config = ReasoningConfig::builder()
    .rule_set(rule_set)
    .default_window(Duration::from_secs(30))       // fact expiration window
    .enable_recursive_inference(false)               // disable recursive rules
    .conflict_resolution(ConflictResolution::Priority) // fire highest priority
    .build();
```

| Option | Default | Description |
|--------|---------|-------------|
| `rule_set` | required | The rules to evaluate |
| `default_window` | 60s | How long facts stay in working memory |
| `enable_recursive_inference` | false | Allow inferred facts to trigger rules |
| `conflict_resolution` | Priority | Strategy when multiple rules match |

### Conflict Resolution Strategies

| Strategy | Behavior |
|----------|----------|
| `Priority` | Fire highest priority rule first, deduplicate |
| `FIFO` | Fire in order received |
| `Lex` | Lexicographic ordering |
| `MEA` | Most recently activated first |

## The RETE Pipeline

```
Input RDF Element
       |
       v
  Working Memory (indexed by subject, predicate, object)
       |
       v
  Alpha Network (single-pattern filtering)
       |
       v
  Beta Network (multi-pattern join on shared variables)
       |
       v
  Conflict Resolution (select which rules fire)
       |
       v
  Fire Productions -> Inferred Statements
```

## Processing Stream Elements

```rust
use cqels_reasoning::ReteNetwork;
use cqels_core::stream::RdfStreamElement;

let mut network = ReteNetwork::compile(config);

for element in &stream_elements {
    let inferred = network.process_element(element);
    for inf in &inferred {
        println!("Inferred: {} (by rule: {})", inf.statement, inf.inferred_by);
    }
}
```

### InferredRdfStreamElement

Each inferred triple carries provenance metadata:

```rust
let inf: &InferredRdfStreamElement = &inferred[0];

inf.statement;     // Statement - the inferred triple
inf.timestamp;     // i64 - timestamp of the inference
inf.inferred_by;   // String - name of the rule that fired
inf.derived_from;  // Vec<Statement> - input facts that triggered the rule
inf.is_inferred(); // true

// Convert to a regular stream element for further processing
let elem = inf.to_rdf_stream_element();
```

### Inspecting Working Memory

```rust
let wm = network.working_memory();
wm.size();                            // number of facts
wm.facts();                           // &[Statement]
wm.lookup_by_subject(&subject_term);  // Vec<&Statement>
wm.lookup_by_predicate(&pred_iri);    // Vec<&Statement>
wm.lookup_by_object(&object_term);    // Vec<&Statement>
```

### Inspecting Inferred Facts

```rust
let all_inferred = network.inferred_facts(); // &HashSet<Statement>
```

## Complete Example

Sensor monitoring with rule-based inference:

```rust
use std::time::Duration;
use cqels_benchmarks::generate_sensor_readings;
use cqels_reasoning::*;
use cqels_model::term::{IriTerm, Term};

fn main() {
    // Define rules
    let alert_rule = Rule::builder()
        .name("high_temperature_alert")
        .priority(1)
        .pattern(TriplePattern::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/temperature"))),
            PatternTerm::Variable("temp".into()),
        ))
        .template(TripleTemplate::new(
            PatternTerm::Variable("sensor".into()),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/hasAlert"))),
            PatternTerm::Constant(Term::Iri(IriTerm::new("http://example.org/HighTemperature"))),
        ))
        .build();

    let rule_set = RuleSet::builder().add_rule(alert_rule).build();
    let config = ReasoningConfig::builder()
        .rule_set(rule_set)
        .default_window(Duration::from_secs(30))
        .build();

    let mut network = ReteNetwork::compile(config);
    let readings = generate_sensor_readings(500);

    let mut alert_count = 0;
    for reading in &readings {
        let inferred = network.process_element(reading);
        alert_count += inferred.len();
    }

    println!("Processed {} readings, inferred {} alerts", readings.len(), alert_count);
    println!("Working memory: {} facts", network.working_memory().size());
}
```

## Advanced Topics

### Recursive Inference

Enable with `.enable_recursive_inference(true)` on `ReasoningConfig`.
Inferred triples are fed back into the network, potentially triggering
further rules. Use with caution -- unbounded recursion is possible.

### Custom Filters

Add binding filters to rule conditions for complex constraints:

```rust
let rule = Rule::builder()
    .name("filtered_rule")
    .pattern(/* ... */)
    .template(/* ... */)
    .build();
```

Filters on `RuleCondition` accept `Box<dyn Fn(&HashMap<String, Term>) -> bool>`,
enabling arbitrary checks on the variable bindings before a rule fires.
