# CQELS-QL reference for the Rust distribution

Release: **2.0.0-alpha.21**. CQELS-QL combines SPARQL-style graph patterns with
continuous stream sources and windows. This compact reference describes the
measured public examples. The complete server-advertised syntax is available
through `resources/read` at `cqels://docs/cqelsql` and `cqels://docs/cep`.
Read [COMPATIBILITY.md](COMPATIBILITY.md): accepted syntax is not proof of correct
execution, and the runtime's broad syntax descriptions include known gaps.

## Query shape and advertised syntax

The following is a compact syntax map, not a claim that every combination
executes correctly in this release. The measured limitations below still apply.

```text
[PREFIX prefix: <iri>]*
[REGISTER QUERY name AS]
SELECT [DISTINCT] select_items
FROM STREAM stream_name [window]
[FROM STREAM second_stream [window]]
[FROM STATIC <graph-iri>]
WHERE { graph_patterns [FILTER(expression)] }
[GROUP BY variables]
[HAVING(expression)]
[ORDER BY expressions]
[LIMIT number]
```

| Window form | Syntax intent |
| --- | --- |
| `[NOW]` | Current observation |
| `[RANGE 10s]` | Time extent; evaluation/closure depends on the route |
| `[RANGE 30s STEP 10s]` | Time extent with a step |
| `[SLIDE 30s STEP 10s]` | Sliding-window form |
| `[TRIPLES 100]` | Count of observations, not individual statements |
| `[FUTURE 10s]` | Directional window; consult the server resource for emission options |
| `[RANGE 10s LATENESS 2s]` | Allowed lateness on supported routes |

Duration units include `ms`, `s`, `m`, `h`, and `d`. The window table follows the
release descriptors. Parser-only checks of the declaration, static-graph,
`sameTerm`, `FILTER NOT EXISTS`, and duration forms are recorded in
[syntax-checks.json](examples/fleet/syntax-checks.json) for the Rust alpha.21
artifact. The fleet static-join fixture uses a plain BGP outside STREAM, rather
than FROM STATIC. Only the concrete linked fleet fixtures have execution
assertions in this public suite. A parser accepting a form does not establish
its retention, emission cadence, or late-event behavior for your query shape.

`FILTER NOT EXISTS { ... }` expresses a correlated anti-join; `sameTerm(A, B)`
expresses RDF term identity rather than numeric equality. Two `FROM STREAM`
clauses describe a two-source join. This compact reference preserves those
language entry points without asserting that all operator combinations or
interval-join retention paths have been verified by the fleet probes. Use the
server syntax resources for detailed constraints and test emitted rows for
these forms before relying on them.

## Sources, patterns, and filters

Create the stream before registration. [low-battery.rq](examples/fleet/low-battery.rq)
uses `FROM STREAM Telemetry [NOW]`, an explicit `STREAM Telemetry { ... }` block,
and `FILTER(?soc < 20)`. Variables `?obs` and `?soc` bind each observation and its
numeric value. Prefixes expand to RDF IRIs; send typed numeric literals through
N-Quads rather than relying on string-to-number coercion.

## Windows and event time

`push_stream_events` carries explicit epoch-millisecond or ISO-instant event
times. An event groups its statements atomically. The tested `[NOW]` filter
matches incoming observations. `[RANGE 30s]` bounds the tested CEP sequence.
The grouped `[RANGE 3s]` aggregate in [aggregation.rq](examples/fleet/aggregation.rq)
returns no Rust rows on the probe input even though it registers successfully.

The server advertises count, sliding, and directional windows as well. Their
complete semantics depend on the query route. This public suite does not verify
all such shapes and does not label every RANGE window as universally tumbling
or rolling. Consult the server syntax resource and test your actual query.

## Joins and aggregation

[static-join.rq](examples/fleet/static-join.rq) combines a stream observation with
stored depot data. The probe seeds data before registering; Java emits a lookup
row but this Rust artifact emits none. [aggregation.rq](examples/fleet/aggregation.rq)
uses three conjoined observation patterns, AVG, MAX, COUNT, and GROUP BY vehicle;
Java emits three running aggregate rows, while this Rust artifact emits none.
Both limitations have executable expectations. Neither probe proves the status
of every possible join or aggregate variant.

## Complex Event Processing

[cep.rq](examples/fleet/cep.rq) uses `FILTER(SEQ(?e1 ; ?e2))` to detect a speed
drop followed by a speed spike. Register with `cep: true`, then send the two
single-triple event observations in order. The result carries `start`, `end`,
and event details. Reversing their order must produce no match, as checked by
the same [cep.rq](examples/fleet/cep.rq) with reversed input.
Quantifiers, negation, and multi-triple event patterns need separate validation;
they are not implied by this two-event example.

## Reasoning

The RDFS demo stores an EV-to-Vehicle subclass edge in `cqels://memory/schema`,
an EV instance in `cqels://memory/longterm`, and calls `reason` with `RDFS_FULL`.
It verifies the inferred Vehicle type. This one-shot materialization example
does not claim support-time retraction or arbitrary Delta/ASP program parity.

## CypherQL and other SPARQL constructs

CypherQL is the server's reduced streaming MATCH/RETURN graph dialect, not full
openCypher. The MCP descriptors and syntax resources advertise additional
SPARQL-style operators. Use `validate_stream_query` for registration diagnostics,
then assert the emitted results for your input. Java-only limitations and
workarounds should not be assumed to apply to Rust without evidence.

## Executable specification

Run `python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp` to check all
linked query files against the release. The driver fails on unexpected rows or
a changed known limitation; reports preserve the distinction between working
scenarios and reproduced differences.
