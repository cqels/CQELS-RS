# CQELS-QL Specification

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

## Windows

| Syntax | Meaning |
| --- | --- |
| `[NOW]` | Evaluate each incoming observation immediately. |
| `[RANGE 10s]` | Tumbling event-time window. |
| `[RANGE 30s SLIDE 10s]` | Overlapping sliding window. |
| `[TRIPLES 100]` | Count-based window. |

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
