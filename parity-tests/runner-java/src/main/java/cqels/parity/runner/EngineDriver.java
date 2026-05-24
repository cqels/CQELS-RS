package cqels.parity.runner;

import org.cqels.engine.CQELSEngine;
import org.cqels.engine.DataStream;
import org.cqels.engine.QueryResultListener;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Runs a {@link Fixture} through a live cqels-java engine
 * (cqels/claude — the same upstream the cqels-rs port tracks) and
 * collects the bindings the registered query emits. The captured
 * shape mirrors what the Rust runner records: a list of variable
 * → string-value maps, optionally tagged with the binding's
 * timestamp.
 *
 * <p>Lifecycle: every call to {@link #run(Fixture)} provisions a
 * fresh {@link CQELSEngine} (in-memory RDF4J store, auto-shut-down
 * on close), so iterations are isolated.</p>
 *
 * <h2>cqels-java integration verification checklist</h2>
 * The adapter surface is small enough to live in this file. If your
 * cqels-java fork's API diverges from cqels/claude's public façade,
 * the things to retarget are:
 * <ul>
 *   <li>{@link CQELSEngine#builder()} entry point + lifecycle.</li>
 *   <li>{@link CQELSEngine#createStream(String)} returning {@link DataStream}.</li>
 *   <li>{@link CQELSEngine#registerCqelsQuery(String, QueryResultListener)} for CQELS-QL.</li>
 *   <li>{@link DataStream#push(Statement, long)} for event-time-stamped pushes.</li>
 *   <li>{@link #resultToRow(Map)} — turns cqels-java's
 *     {@code Map<String,Object>} result into the variable → string
 *     pairs the fixture format speaks.</li>
 * </ul>
 */
public final class EngineDriver {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    /** One captured binding: variable → string value plus an optional timestamp. */
    public static final class CapturedBinding {
        public final Map<String, String> bindings;
        public final Long timestamp;

        public CapturedBinding(Map<String, String> bindings, Long timestamp) {
            this.bindings = bindings;
            this.timestamp = timestamp;
        }
    }

    /**
     * Runs the fixture once. Returns the bindings the registered
     * query emitted, in the order they arrived.
     */
    public List<CapturedBinding> run(Fixture fixture) throws IOException {
        List<CapturedBinding> captured = new CopyOnWriteArrayList<>();

        try (CQELSEngine engine = CQELSEngine.builder()
                .id("cqels-parity-" + System.nanoTime())
                .withMemoryStore()
                .build()) {

            // Seed static-graph data BEFORE registering the query —
            // cqels-java's compiler captures repository references at
            // registration time, and the query's first evaluation
            // walks the FROM <iri> patterns once. Loading after the
            // first batch arrives risks the static patterns seeing
            // an empty repository.
            if (fixture.staticTrig != null) {
                loadStaticTrig(engine.getRepository(), fixture.staticTrig);
            }

            // Pre-create every stream referenced by the workload's
            // events. cqels-java identifies streams by bare name, so
            // the fixture's `stream` field maps 1:1 — no IRI rewriting
            // is needed (the Rust runner uses the same convention).
            TreeSet<String> streamNames = new TreeSet<>();
            for (Fixture.Event e : fixture.events) {
                streamNames.add(e.stream);
            }
            if (streamNames.isEmpty()) {
                throw new IOException("workload defines no input streams");
            }
            Map<String, DataStream> streams = new LinkedHashMap<>();
            for (String name : streamNames) {
                streams.put(name, engine.createStream(name));
            }

            engine.registerCqelsQuery(fixture.query, new QueryResultListener<Map<String, Object>>() {
                @Override
                public void onResult(Map<String, Object> result) {
                    captured.add(new CapturedBinding(resultToRow(result), null));
                }

                @Override
                public void onError(Throwable error) {
                    System.err.println("query error: " + error);
                }
            });

            engine.start();

            for (Fixture.Event event : fixture.events) {
                DataStream ds = streams.get(event.stream);
                if (ds == null) {
                    throw new IOException("event references undeclared stream `" + event.stream + "`");
                }
                Statement stmt = VF.createStatement(
                        parseTermAsIri(event.s),
                        VF.createIRI(event.p),
                        parseObjectTerm(event.o));
                // DataStream.push honors the event-time timestamp,
                // unlike CQELS-1.x's wall-clock-only model. RANGE
                // windows in fixtures are therefore meaningful.
                ds.push(stmt, event.ts);
            }
            for (DataStream ds : streams.values()) {
                ds.complete();
            }

            // Soft drain: results flow through Reactor backpressure
            // buffers, so wait a bounded amount of time for the last
            // expected binding to land. The fixture metadata can
            // override the default (1s) for slow tumbling-window
            // workloads — same convention as the Rust runner.
            int target = fixture.expected.size();
            int drainMs = fixture.metadata.drainTimeoutMs != null
                    ? fixture.metadata.drainTimeoutMs : 1000;
            long deadline = System.currentTimeMillis() + drainMs;
            while (captured.size() < target && System.currentTimeMillis() < deadline) {
                try { Thread.sleep(10); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            engine.stop();
        }
        return new ArrayList<>(captured);
    }

    /**
     * Parse a fixture's `static.trig` and seed it into the engine's
     * RDF4J Repository. Triples in the default graph get a
     * {@code null} context; triples tagged with a named graph
     * preserve their graph context so {@code FROM <iri>} /
     * {@code GRAPH <iri> { ... }} patterns in the CQELS-QL query
     * resolve against the right partition.
     *
     * <p>Throws {@link IOException} if the TriG can't be parsed —
     * surfaces as the runner's {@code engine error} exit code
     * (2), distinguishable from a binding mismatch (1).
     */
    static void loadStaticTrig(Repository repository, String trig) throws IOException {
        if (repository == null) {
            // Defensive: `withMemoryStore()` should always produce a
            // non-null repository, but engines built via
            // `builder().repository(null)` would land here.
            throw new IOException("static.trig present but engine has no repository configured");
        }
        Model model;
        try {
            model = Rio.parse(
                    new ByteArrayInputStream(trig.getBytes(StandardCharsets.UTF_8)),
                    "",
                    RDFFormat.TRIG);
        } catch (RDFParseException | UnsupportedRDFormatException e) {
            throw new IOException("static.trig parse error: " + e.getMessage(), e);
        }
        try (RepositoryConnection conn = repository.getConnection()) {
            conn.begin();
            for (Statement st : model) {
                Resource ctx = st.getContext();
                if (ctx == null) {
                    conn.add(st);
                } else {
                    conn.add(st, ctx);
                }
            }
            conn.commit();
        }
    }

    /**
     * Parse a fixture term as an RDF4J Value usable in subject or
     * object position. Bracketed {@code <...>} and known IRI
     * schemes become IRIs; everything else becomes a plain literal.
     * Mirrors the Rust runner's {@code parse_term}.
     */
    static Value parseTerm(String term) {
        String trimmed = term.trim();
        if (trimmed.startsWith("<") && trimmed.endsWith(">") && trimmed.length() >= 2) {
            return VF.createIRI(trimmed.substring(1, trimmed.length() - 1));
        }
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")
                || trimmed.startsWith("urn:") || trimmed.startsWith("file://")) {
            return VF.createIRI(trimmed);
        }
        return VF.createLiteral(trimmed);
    }

    /**
     * RDF4J {@link Statement} requires its subject to be a resource
     * (IRI or blank node). We always pass an IRI through
     * {@link #parseTerm(String)} for the subject slot; the object
     * slot accepts any Value. This wrapper documents the symmetry
     * — both fields share the same heuristic.
     */
    static IRI parseTermAsIri(String term) {
        Value v = parseTerm(term);
        if (!(v instanceof IRI iri)) {
            throw new IllegalArgumentException(
                    "subject must be an IRI but got literal `" + term + "`");
        }
        return iri;
    }

    private static Value parseObjectTerm(String term) {
        return parseTerm(term);
    }

    /**
     * Turn cqels-java's {@code Map<String, Object>} result into the
     * flat variable → string-value shape the fixture format speaks.
     * RDF4J {@link Value} stringifies via {@code stringValue()},
     * which yields the IRI string or the literal's lexical form.
     */
    static Map<String, String> resultToRow(Map<String, Object> result) {
        // Sort by variable name so two semantically equivalent
        // bindings with different iteration orders compare equal —
        // mirrors the BTreeMap ordering the Rust runner uses.
        Map<String, String> row = new TreeMap<>();
        for (Map.Entry<String, Object> e : result.entrySet()) {
            String key = Fixture.stripVarPrefix(e.getKey());
            row.put(key, valueToString(e.getValue()));
        }
        return row;
    }

    private static String valueToString(Object value) {
        if (value == null) return "";
        if (value instanceof Value v) {
            // IRI / Literal / BNode all expose `stringValue()` —
            // for literals that's the lexical form (no `"..."@en^^xsd:`).
            return v.stringValue();
        }
        return value.toString();
    }
}
