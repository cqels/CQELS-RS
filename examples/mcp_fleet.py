#!/usr/bin/env python3
"""Run public fleet demos and release-specific compatibility probes (stdlib only)."""
import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
EX = "https://example.org/fleet/"
SOSA = "http://www.w3.org/ns/sosa/"
FLEET = "https://covesa.global/fleet#"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class Rpc:
    """One isolated, newline-delimited MCP session, with bounded requests."""
    def __init__(self, command, timeout=20):
        self.timeout = timeout
        self.messages = queue.Queue()
        self.contamination = []
        self.stderr = []
        self.sequence = 0
        self.directory = tempfile.TemporaryDirectory(prefix="cqels-fleet-")
        env = {k: v for k, v in os.environ.items() if not k.startswith("CQELS_")}
        try:
            self.process = subprocess.Popen(command, cwd=self.directory.name, env=env,
                                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, text=True, encoding="utf-8")
        except Exception:
            self.directory.cleanup()
            raise
        self.readers = [threading.Thread(target=self._stdout, daemon=True),
                        threading.Thread(target=self._stderr, daemon=True)]
        for reader in self.readers:
            reader.start()

    def _stdout(self):
        for line in self.process.stdout:
            try:
                message = json.loads(line)
                if not isinstance(message, dict):
                    raise ValueError("not an object")
                self.messages.put(message)
            except ValueError:
                self.contamination.append(line.rstrip())

    def _stderr(self):
        for line in self.process.stderr:
            self.stderr.append(line.rstrip())
            self.stderr[:] = self.stderr[-100:]

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def wait_for_java_launcher(self):
        # The pinned Java stdio transport can accept stdin before its output
        # queue is subscribed. Sending initialize then can lose its response.
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if any("CQELS MCP server is running." in line for line in self.stderr):
                return
            if self.process.poll() is not None:
                raise RuntimeError(f"Java exited before launcher readiness: {self.stderr}")
            time.sleep(0.05)
        raise TimeoutError(f"Java launcher did not report readiness: {self.stderr}")

    def request(self, method, params=None):
        self.sequence += 1
        self.send({"jsonrpc": "2.0", "id": self.sequence, "method": method,
                   "params": params or {}})
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                response = self.messages.get(timeout=0.1)
            except queue.Empty:
                if self.process.poll() is not None:
                    raise RuntimeError(f"server exited: {self.stderr[-10:]}")
                continue
            if response.get("id") != self.sequence:  # notifications have no request id
                continue
            if "error" in response:
                raise RuntimeError(f"{method}: {response['error']}")
            return response["result"]
        raise TimeoutError(f"{method}: no response in {self.timeout}s; {self.stderr}")

    def initialize(self):
        result = self.request("initialize", {"protocolVersion": "2024-11-05",
            "capabilities": {}, "clientInfo": {"name": "cqels-fleet", "version": "1"}})
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            status = self.request("resources/read", {"uri": "cqels://engine/status"})
            engine = json.loads(status["contents"][0]["text"])
            if engine.get("running") is True:
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("MCP initialized but the engine never became ready")
        return result

    def tool(self, name, **arguments):
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        if result.get("isError"):
            raise RuntimeError(f"{name}: {result}")
        text = "\n".join(c["text"] for c in result.get("content", []) if c["type"] == "text")
        try:
            return json.loads(text)
        except ValueError:
            return text

    def drain(self, query_id, seconds=1.5):
        # Observe the entire interval, including after the first match, to catch
        # duplicate/unexpected rows. Empty probes are bounded observations.
        deadline = time.monotonic() + seconds
        rows = []
        while time.monotonic() < deadline:
            batch = self.tool("recall_memory", queryId=query_id, limit=1000)
            if not isinstance(batch, list):
                raise RuntimeError(f"unexpected query result: {batch}")
            rows.extend(batch)
            time.sleep(0.05)
        return rows

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        for reader in self.readers:
            reader.join(timeout=2)
        self.process.stdout.close()
        self.process.stderr.close()
        self.directory.cleanup()


def surface(rpc):
    """Full descriptors; ordering is immaterial, schema contents are not."""
    result = {}
    for label, method, key, identity in [
        ("tools", "tools/list", "tools", "name"),
        ("resources", "resources/list", "resources", "uri"),
        ("templates", "resources/templates/list", "resourceTemplates", "uriTemplate"),
        ("prompts", "prompts/list", "prompts", "name")]:
        page = rpc.request(method)
        if page.get("nextCursor"):
            raise RuntimeError("paginated discovery requires updating this release probe")
        result[label] = sorted(page[key], key=lambda item: item[identity])
    return normalize_schema(result)


def normalize_schema(value, key=None):
    if isinstance(value, dict):
        return {k: normalize_schema(v, k) for k, v in value.items()}
    if isinstance(value, list):
        items = [normalize_schema(v) for v in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True)) if key in ("required", "enum") else items
    return value


def event(subject, predicate, obj, timestamp, uri=False):
    return {"eventTime": timestamp, "facts": [{"subject": subject,
        "predicate": predicate, "object": obj, "objectType": "uri" if uri else "literal"}]}


def speed_event(i, timestamp, value):
    subject = f"{EX}obs/{i}"
    triples = [(SOSA + "observedProperty", "<https://covesa.global/vss#Speed>"),
               (SOSA + "hasFeatureOfInterest", f"<{EX}vehicle/EV-7Q2>"),
               (SOSA + "hasSimpleResult", f'"{value}"^^<http://www.w3.org/2001/XMLSchema#double>')]
    return {"eventTime": timestamp,
            "nquads": "\n".join(f"<{subject}> <{p}> {o} ." for p, o in triples)}


def run_scenario(rpc, name):
    fixture = json.loads((HERE / "fleet" / "expectations.json").read_text(encoding="utf-8"))[name]
    if name == "rdfs":
        rpc.tool("store_memory", graph="cqels://memory/schema",
                 turtle=f"<{EX}EV> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <{EX}Vehicle> .")
        rpc.tool("store_memory", turtle=f"<{EX}vehicle/EV-7Q2> a <{EX}EV> .")
        rows = rpc.tool("reason", profile="RDFS_FULL")
        # RDFS_FULL also emits axiomatic triples. Check the demonstrative
        # subclass consequence independently of those unrelated axioms.
        return [{k: row[k] for k in ("subject", "predicate", "object")}
                for row in rows if row.get("subject") == EX + "vehicle/EV-7Q2"
                and row.get("predicate") == TYPE and row.get("object") == EX + "Vehicle"]
    stream = fixture["stream"]
    if name == "static-join":
        rpc.tool("store_memory", turtle=f"<{EX}vehicle/EV-7Q2> <{FLEET}depot> <{EX}depot/north> .")
    rpc.tool("create_stream", stream=stream)
    query = (HERE / "fleet" / f"{name}.rq").read_text(encoding="utf-8")
    rpc.tool("register_stream_query", query=query, queryId=name, cep=name.startswith("cep"))
    if name == "low-battery":
        events = [{"eventTime": i * 1000, "nquads":
                   f'<{EX}obs/{i}> <{SOSA}hasSimpleResult> "{v}"^^<http://www.w3.org/2001/XMLSchema#double> .'}
                  for i, v in enumerate([64.0, 18.5, 41.0, 12.0, 27.5], 1)]
    elif name == "aggregation":
        events = [speed_event(i, t, v) for i, (t, v) in enumerate([(1000, 60), (2000, 80), (5000, 40)], 1)]
    elif name == "static-join":
        events = [event(EX + "obs/1", SOSA + "hasFeatureOfInterest", EX + "vehicle/EV-7Q2", 1000, True)]
    elif name.startswith("cep"):
        kinds = ["SpeedDropEvent", "SpeedSpikeEvent"]
        if name == "cep-reversed":
            kinds.reverse()
        events = [event(EX + f"event/{i}", FLEET + "event", FLEET + kind, i*1000, True)
                  for i, kind in enumerate(kinds, 1)]
    else:
        raise ValueError(name)
    for item in events:
        rpc.tool("push_stream_events", stream=stream, events=[item])
    rows = rpc.drain(name)
    rpc.tool("forget_stream_query", queryId=name)
    if name == "low-battery":
        for row in rows:
            row["soc"] = float(row["soc"])  # Java JSON number vs Rust numeric string
    if name == "aggregation":
        for row in rows:
            for key in ("avgSpeed", "peak", "n"):
                row[key] = float(row[key])
    if name.startswith("cep"):
        # Keep the event identities as well as the timing, to catch matches of
        # the wrong observations (the release encodes events in a string).
        for row in rows:
            if EX + "event/1" not in row.get("events", "") or EX + "event/2" not in row.get("events", ""):
                raise AssertionError(f"unexpected CEP observations: {row}")
        rows = [{k: row[k] for k in ("start", "end")} for row in rows]
    return rows


def canonical_rows(rows):
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    server = parser.add_mutually_exclusive_group(required=True)
    server.add_argument("--server", type=Path, help="extracted Rust cqels-mcp executable")
    server.add_argument("--java-jar", type=Path, help="public Java reference shaded jar")
    parser.add_argument("--scenario", default="all")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    engine = "rust" if args.server else "java"
    command = [str(args.server.resolve())] if args.server else ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", str(args.java_jar.resolve())]
    pin = json.loads((HERE.parent / "RELEASE.json").read_text(encoding="utf-8"))
    expected_surface = json.loads((HERE.parent / "mcp-server" / "contract.json").read_text(encoding="utf-8"))
    fixtures = json.loads((HERE / "fleet" / "expectations.json").read_text(encoding="utf-8"))
    selected = list(fixtures) if args.scenario == "all" else [args.scenario]
    if any(name not in fixtures for name in selected):
        parser.error(f"scenario must be all or one of {', '.join(fixtures)}")
    report = {"engine": engine, "version": pin["version"], "discovery": "not checked", "scenarios": {}}
    try:
        for name in selected:
            rpc = Rpc(command)
            try:
                if engine == "java":
                    rpc.wait_for_java_launcher()
                init = rpc.initialize()
                if init["serverInfo"]["version"] != pin["version"]:
                    raise AssertionError(f"release version mismatch: {init['serverInfo']}")
                if report["discovery"] != "pass":
                    if surface(rpc) != expected_surface:
                        raise AssertionError("MCP descriptor drift: update the public contract and guide together")
                    report["discovery"] = "pass"
                rows = run_scenario(rpc, name)
                expected = fixtures[name][engine]
                if canonical_rows(rows) != canonical_rows(expected):
                    raise AssertionError(f"{name}: expected {expected}, received {rows}")
                status = "known difference" if fixtures[name].get("difference") else "pass"
                report["scenarios"][name] = {"status": status, "rows": rows}
                print(f"{name}: {status.upper()} {json.dumps(rows, sort_keys=True)}", flush=True)
            finally:
                rpc.close()
            if rpc.contamination:
                raise AssertionError(f"non-JSON protocol stdout: {rpc.contamination}")
    except Exception as exc:
        report["error"] = str(exc)
        raise
    finally:
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
