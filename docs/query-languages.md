# Query Languages

> **Prerequisites**: [Data Model](data-model.md), [Stream Processing](stream-processing.md)
>
> **Next steps**: [CEP](cep.md), [Reasoning](reasoning.md), [Advanced](advanced.md)

CQELS supports two query languages, both extended with streaming window
semantics.

## CqelsQL (SPARQL-based)

CqelsQL extends SPARQL with `FROM STREAM` clauses and window specifications.
Use it for triple-pattern queries over RDF streams.

### Syntax Overview

```
[PREFIX prefix: <iri>]*
[REGISTER QUERY name AS]
SELECT [DISTINCT] select_elements
FROM STREAM stream_name [window_spec]
[FROM STATIC 'graph_iri' [DEPTH n] [CACHE duration]]
WHERE {
    triple_patterns
    [FILTER (expression)]
}
[GROUP BY variables]
[HAVING (expression)]
[ORDER BY variable [ASC|DESC]]*
[LIMIT n]
```

### Window Specifications

| Syntax | Type | Description |
|--------|------|-------------|
| `[NOW]` | Instantaneous | Current element only |
| `[RANGE 10s]` | Tumbling | Fixed-duration window |
| `[RANGE 30s SLIDE 10s]` | Sliding | Overlapping windows |
| `[TRIPLES 100]` | Count | Fixed number of triples |

Duration units: `ms` (milliseconds), `s` (seconds), `m` (minutes),
`h` (hours), `d` (days).

### Examples

**Simple select with time window:**

```sparql
PREFIX ex: <http://example.org/>
SELECT ?sensor ?temp
FROM STREAM sensors [RANGE 10s]
WHERE {
    ?sensor ex:temperature ?temp .
}
ORDER BY ?temp DESC
LIMIT 5
```

**Aggregation with GROUP BY:**

```sparql
PREFIX ex: <http://example.org/>
SELECT ?city (AVG(?temp) AS ?avgTemp) (COUNT(?sensor) AS ?cnt)
FROM STREAM readings [RANGE 1m SLIDE 10s]
WHERE {
    ?sensor ex:locatedIn ?city .
    ?sensor ex:temperature ?temp .
}
GROUP BY ?city
HAVING (AVG(?temp) > 30)
ORDER BY ?avgTemp DESC
```

**Multi-stream with static graph:**

```sparql
PREFIX ex: <http://example.org/>
REGISTER QUERY alertMonitor AS
SELECT ?sensor ?label ?value
FROM STREAM sensorData [RANGE 30s]
FROM STATIC 'http://knowledge.org/sensors'
WHERE {
    ?sensor ex:value ?value .
    ?sensor ex:label ?label .
}
```

**Registered query:**

```sparql
REGISTER QUERY myQuery AS
SELECT ?x ?y
FROM STREAM events [NOW]
WHERE { ?x ?p ?y . }
```

### Parser API

```rust
use cqels_core::parser::CqelsQlParser;

let def = CqelsQlParser::parse(query_str)?;

// Inspect the AST
def.name;              // Option<String> - registered query name
def.query_type;        // QueryType (Select, Construct, etc.)
def.prefixes;          // Vec<(prefix, iri)>
def.streams;           // Vec<StreamSource> with name + window
def.select_elements;   // Vec<SelectElement>
def.pattern_groups;    // Vec<PatternGroup> containing triple patterns
def.aggregates;        // Vec<AggregateSpec>
def.group_by_variables; // Vec<String>
def.order_by_conditions; // Vec<OrderByCondition>
def.limit;             // Option<u64>
def.has_order_by();    // bool
def.is_distinct();     // bool
```

## CypherQL (Cypher-based)

CypherQL extends Cypher with `FROM STREAM` clauses and window specifications.
Use it for graph pattern queries with nodes, relationships, and properties.

### Syntax Overview

```
[REGISTER QUERY name AS]
FROM STREAM stream_name [window_spec]
[FROM STATIC 'graph_iri']
MATCH pattern_expression
[WHERE expression]
[GROUP BY expressions]
[HAVING expression]
RETURN return_items
[ORDER BY expression [ASC|DESC]]*
[LIMIT n]
```

### Pattern Expressions

```cypher
-- Node with label
(p:Person)

-- Node with properties
(p:Person {name: "Alice"})

-- Outgoing relationship
(a:Person)-[:FOLLOWS]->(b:Person)

-- Incoming relationship
(a:Person)<-[:FOLLOWS]-(b:Person)

-- Variable-length path
(a:Person)-[:KNOWS*1..3]->(b:Person)

-- Multiple labels/types
(n:Person:Employee)
```

### Source Contexts

CypherQL supports sourcing patterns from streams or static graphs:

```cypher
MATCH
    STREAM events { (e:Event)-[:FROM]->(s:Sensor) },
    STATIC { (s:Sensor)-[:IN]->(l:Location) }
```

### Window Specifications

| Syntax | Type | Description |
|--------|------|-------------|
| `[NOW]` | Instantaneous | Current element only |
| `[RANGE 5m]` | Tumbling | Fixed-duration window |
| `[SLIDE 1m STEP 10s]` | Sliding | Overlapping windows |
| `[TRIPLES 100]` | Count | Fixed number of triples |

### Examples

**Simple pattern match:**

```cypher
FROM STREAM social [NOW]
MATCH (p:Person)
WHERE p.age > 18
RETURN p.name, p.age
ORDER BY p.age DESC
LIMIT 10
```

**Relationship with aggregation:**

```cypher
FROM STREAM social [RANGE 5m]
MATCH (a:Person)-[:FOLLOWS]->(b:Person)
GROUP BY a.city
RETURN a.city, count(b) AS followers
ORDER BY followers DESC
LIMIT 5
```

**Multi-source registered query:**

```cypher
REGISTER QUERY alertMonitor AS
FROM STREAM events [SLIDE 1m STEP 10s]
FROM STATIC 'http://knowledge.org/base'
MATCH
    STREAM events { (e:Event)-[:TRIGGERED_BY]->(s:Sensor) },
    STATIC { (s:Sensor)-[:LOCATED_IN]->(l:Location) }
RETURN e.type, s.id, l.name
```

**Node with properties:**

```cypher
FROM STREAM events [TRIPLES 100]
MATCH (n:Person {name: "Alice"})
RETURN n
```

### Parser API

```rust
use cqels_core::parser::CypherQlParser;

let def = CypherQlParser::parse(query_str)?;

// Inspect the AST
def.name;               // Option<String>
def.streams;            // Vec<StreamSource>
def.static_graphs;      // Vec<String>
def.pattern_groups;     // Vec<PatternGroup> with nodes + relationships
def.where_expression;   // Option<WhereExpression>
def.return_expressions; // Vec<ReturnExpression>
def.group_by_expressions; // Vec<String>
def.order_by_conditions;  // Vec<OrderByCondition>
def.limit;              // Option<u64>

// Pattern inspection
for group in &def.pattern_groups {
    println!("Source: {:?}", group.source); // Stream, Static, Graph, Default
    for pattern in &group.patterns {
        println!("Nodes: {}", pattern.nodes.len());
        println!("Relationships: {}", pattern.relationships.len());
        for rel in &pattern.relationships {
            println!("  Direction: {:?}, Types: {:?}", rel.direction, rel.types);
        }
    }
}
```

## Error Handling

Both parsers return `ParseResult<T>` which is `Result<T, ParseError>`:

```rust
use cqels_core::parser::{CqelsQlParser, ParseError};
use cqels_model::ParseErrorKind;

match CqelsQlParser::parse(bad_query) {
    Ok(def) => { /* use the AST */ }
    Err(ParseError::Syntax(msg)) => {
        println!("Syntax error: {msg}");
    }
    Err(ParseError::Semantic(msg)) => {
        println!("Semantic error: {msg}");
    }
    Err(ParseError::Unsupported(msg)) => {
        println!("Unsupported feature: {msg}");
    }
}
```

`ParseError` converts to `CqelsError::Parse(ParseErrorDetail)` via
`From`, providing structured line/column information.

## Choosing a Language

| Aspect | CqelsQL | CypherQL |
|--------|---------|----------|
| Basis | SPARQL | Cypher |
| Best for | Triple pattern matching | Graph traversal, properties |
| Syntax style | `WHERE { ?s ?p ?o }` | `MATCH (a)-[:REL]->(b)` |
| Aggregation | `SELECT (COUNT(...) AS ?)` | `RETURN count(x) AS name` |
| Static data | `FROM STATIC 'iri'` | `FROM STATIC 'iri'` |
| Variable paths | Not supported | `*min..max` |
| Node properties | Not applicable | `{key: value}` |

Both languages support the same window specifications and can be used
interchangeably for streaming queries. Choose based on your data model
and familiarity.
