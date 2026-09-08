# CQELS as an MCP server — Rust

Current release: **2.0.0-alpha.20**. Install with `python3 mcp-server/install.py`
from the repository root. [Platforms and manual installation](../GETTING_STARTED.md).
The Rust executable needs no JDK or Maven launcher.

## Connect over stdio

Configure a local MCP client using an absolute executable path. For clients that
accept an `mcpServers` JSON configuration:

```json
{
  "mcpServers": {
    "cqels": {
      "command": "/absolute/path/to/CQELS-RS/.cqels/bin/cqels-mcp",
      "args": []
    }
  }
}
```

Use the `.exe` path on Windows. The client starts the process and performs MCP
initialization. This server uses one JSON-RPC object per line, without HTTP or
Content-Length framing. Stdout is protocol-only; diagnostics go to stderr. The
negotiated stdio protocol is `2024-11-05`.

## Tools exposed (25)

The table lists schema-required arguments. Optional fields, enums, defaults,
limits, descriptions, and nested argument schemas are in [contract.json](contract.json).
Tools with no schema-required arguments may still need data to do useful work.
Discovery confirms their presence; it does not establish every advertised behavior.

| Tool | Required arguments |
| --- | --- |
| `assemble_context` | `task` |
| `create_stream` | `stream` |
| `explain_decision` | `id` |
| `forget_memory` | None |
| `forget_stream_query` | `queryId` |
| `list_procedures` | None |
| `push_stream_events` | `events`, `stream` |
| `query` | `query` |
| `reason` | None |
| `recall_decisions` | None |
| `recall_episodes` | None |
| `recall_memory` | None |
| `record_event` | `object`, `predicate`, `subject` |
| `register_reasoning` | None |
| `register_rules` | `resultPredicate`, `rules`, `stream` |
| `register_stream_query` | `query` |
| `remove_stream` | `stream` |
| `run_procedure` | `name` |
| `save_procedure` | `body`, `kind`, `name` |
| `set_access_policy` | `role` |
| `solve` | `program` |
| `store_memory` | None |
| `validate` | None |
| `validate_stream_query` | `query` |
| `watch_invariant` | `shapes`, `stream` |

## Resources and prompts

Nine resources: `cqels://engine/status`, `cqels://kg/stats`,
`cqels://kg/namespaces`, `cqels://streams`, `cqels://queries`,
`cqels://reasoning/capabilities`, `cqels://docs/cqelsql`, `cqels://docs/cep`,
`cqels://docs/memory-profile`.

One resource template: `cqels://queries/{queryId}/results`.

Ten prompts: `recent_events_window`, `value_over_window`, `entity_by_type`,
`recall_about`, `store_knowledge`, `extract_facts`, `validate_data`,
`reasoning_workflow`, `memory_routing`, `spatial_recall`.

## Streaming lifecycle

The lossless sequence is **create_stream → register_stream_query →
push_stream_events → recall_memory(queryId)**. Register before pushing because
streams are hot and do not replay earlier observations to a new query.

```json
{"name":"create_stream","arguments":{"stream":"Telemetry"}}
```

Call this through `tools/call`, then register the exact query in
[low-battery.rq](../examples/fleet/low-battery.rq) with `queryId: "low-battery"`.
Each event carries one observation, an explicit `eventTime` in epoch milliseconds
(or an ISO instant), and either a `facts` array or typed `nquads` text.
The [executable demo](../examples/mcp_fleet.py) shows every request and validates
both positive and negative data. Drain `recall_memory` with the query ID;
`forget_stream_query` stops the query. `remove_stream` removes a stream.

`query` reads stored RDF snapshots rather than live stream events. Use
`store_memory` for background facts and `push_stream_events` for observations.
Read [compatibility and limitations](../COMPATIBILITY.md) before choosing a
mixed stream/background query route.

## Streamable HTTP

From the repository root on macOS/Linux:

```bash
CQELS_MCP_TRANSPORT=http \
CQELS_MCP_HTTP_HOST=127.0.0.1 \
CQELS_MCP_HTTP_PORT=3000 \
CQELS_MCP_HTTP_AUTH_TOKEN=demo-token \
.cqels/bin/cqels-mcp
```

Point an HTTP-capable MCP client at `http://127.0.0.1:3000/mcp`, configured with
`Authorization: Bearer demo-token`. Use your own token for deployment. The HTTP
transport uses protocol `2025-03-26`; clients must preserve the returned
`Mcp-Session-Id` and send the protocol version header on subsequent requests.

In another terminal:

```bash
curl --fail http://127.0.0.1:3000/health/ready
```

A ready server responds successfully. Health paths are separate from the MCP
endpoint. Clients should handle MCP JSON and event-stream responses rather than
assuming every response is plain JSON.

| Environment variable | Default / meaning |
| --- | --- |
| `CQELS_MCP_TRANSPORT` | `stdio`; set `http` to listen |
| `CQELS_MCP_HTTP_HOST` | `127.0.0.1` |
| `CQELS_MCP_HTTP_PORT` | `3000` |
| `CQELS_MCP_HTTP_PATH` | `/mcp` |
| `CQELS_MCP_HTTP_AUTH_TOKEN` | Unset; bearer authentication when configured |
| `CQELS_MCP_HTTP_ALLOWED_ORIGINS` | Optional comma-separated allowed origins |
| `CQELS_MCP_HTTP_ALLOWED_HOSTS` | Optional comma-separated allowed hosts |
| `CQELS_MCP_HTTP_HEALTH_PATH` | `/health` |
| `CQELS_MCP_HTTP_HEALTH_STARTED_PATH` | `/health/started` |
| `CQELS_MCP_HTTP_HEALTH_READY_PATH` | `/health/ready` |
| `CQELS_MCP_HTTP_MAX_BODY_BYTES` | `1048576` |

For remote clients use a deployment with HTTPS and the appropriate host/origin
configuration. The local demonstration does not configure a hosted endpoint.

## Persistence

Persistent RDF memory and the durable observation journal are separate. Example:

```bash
CQELS_MCP_RDF_STORE_PATH="$PWD/.cqels/rdf" \
CQELS_MCP_STORAGE_BACKEND=lmdb \
CQELS_MCP_STORAGE_PATH="$PWD/.cqels/operator" \
.cqels/bin/cqels-mcp
```

Use dedicated directories per server and reuse them when restarting that server.
The released binary accepts `lmdb` as its operator storage backend. Other storage
crates mentioned in project literature are not interchangeable launcher backends.
The fleet driver intentionally uses isolated ephemeral processes, so persistence
settings in your shell do not affect its reproducibility.

Inspect `push_stream_events`'s `structuredContent`: `accepted`, `durable`, and
when present `journalId`, `journalPosition`, `mirrorDegraded`, `reason`. An
accepted event with `durable: false` is not a durability guarantee. Use engine
status and readiness resources to inspect the actual configured process.

## Compatibility and evidence

The release-specific public probes cover discovery, the fleet scenarios, and
known differences. Publication CI additionally runs the pinned Java/Rust HTTP
and persistence comparison suites. See [COMPATIBILITY.md](../COMPATIBILITY.md)
for the scope: matching wire schemas does not imply complete Java engine parity.
