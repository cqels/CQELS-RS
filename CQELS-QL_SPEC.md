# CQELS-QL reference for the Rust distribution

Release: **2.0.0-alpha.20**. CQELS-QL combines SPARQL-style graph patterns with
continuous stream sources and windows. This compact reference describes the
measured public examples. The complete server-advertised syntax is available
through `resources/read` at `cqels://docs/cqelsql` and `cqels://docs/cep`.
Read [COMPATIBILITY.md](COMPATIBILITY.md): accepted syntax is not proof of correct
execution, and the runtime's broad syntax descriptions include known gaps.

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
[cep-reversed.rq](examples/fleet/cep-reversed.rq) with reversed input.
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
