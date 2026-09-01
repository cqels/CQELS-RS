# CQELS-QL Specification

**CQELS-RS release:** 2.0.0-alpha.20

CQELS-QL extends SPARQL-style graph patterns with continuous stream sources
and window semantics. This document is the compact distribution reference;
implementation details remain private to the canonical development repository.

## Query shape

```text
[PREFIX prefix: <iri>]*
[REGISTER QUERY name AS]
SELECT [DISTINCT] select_items
FROM STREAM stream_name [window]
[FROM STATIC <graph-iri>]
WHERE { triple_patterns [FILTER(expression)] }
[GROUP BY variables]
[HAVING(expression)]
[ORDER BY variable [ASC|DESC]]*
[LIMIT number]
```

`FILTER NOT EXISTS { ... }` is supported as a correlated anti-join. Its
patterns are evaluated against the complete pre-projection solution, so a
correlation variable does not need to appear in `SELECT`.

## Windows

| Syntax | Meaning |
| --- | --- |
| `[NOW]` | Evaluate each incoming observation immediately. |
| `[RANGE 10s]` | Tumbling event-time window. |
| `[RANGE 30s STEP 10s]` | Overlapping sliding window. |
| `[TRIPLES 100]` | Count-based window. |

`FROM STREAM` may be declared twice for the alpha.20 two-stream interval-join
route. Each `STREAM` block is a natural-join side; multiple triple patterns in
one block are conjoined within that side's declared window. Shared variables
on both sides must agree, and the interval endpoints are inclusive. Per-side
interval retention is bounded by `CQELS_JOIN_INTERVAL_BUFFER_CAP` (default
100000); an exceeded bound fails loudly rather than silently losing rows.

The `sameTerm(A, B)` expression function compares RDF term identity, including
datatype and language tag, without numeric promotion. An unbound argument is
a type error.

Durations support `ms`, `s`, `m`, `h`, and `d`.

## Example

```sparql
PREFIX ex: <http://example.org/>

SELECT ?sensor ?temperature
FROM STREAM sensors [RANGE 10s]
WHERE {
  ?sensor ex:temperature ?temperature .
  FILTER(?temperature > 30)
}
ORDER BY ?temperature DESC
LIMIT 5
```

## CypherQL

The distribution also supports a Cypher-style graph syntax:

```cypher
FROM STREAM social [NOW]
MATCH (person:Person)-[:FOLLOWS]->(friend:Person)
WHERE person.age > 18
RETURN person.name, friend.name
```

For CEP, use the Rust API or the released MCP server to register sequence
patterns with `FILTER(SEQ(...))` semantics.
