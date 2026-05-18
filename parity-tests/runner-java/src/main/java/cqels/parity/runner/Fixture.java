package cqels.parity.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads one parity fixture from disk. Mirrors the schema in
 * `parity-tests/README.md` exactly so this implementation and the
 * Rust runner consume the same files unchanged.
 *
 * Layout:
 * <pre>
 *   metadata.toml      → {@link Metadata}
 *   query.cqels        → {@link #query}
 *   streams.jsonl      → list of {@link Event}, one per line
 *   expected.jsonl     → list of {@link ExpectedBinding}, one per line
 * </pre>
 */
public final class Fixture {
    public final Metadata metadata;
    public final String query;
    public final List<Event> events;
    public final List<ExpectedBinding> expected;

    private Fixture(Metadata metadata, String query, List<Event> events,
                    List<ExpectedBinding> expected) {
        this.metadata = metadata;
        this.query = query;
        this.events = events;
        this.expected = expected;
    }

    public static Fixture load(Path dir) throws IOException {
        Metadata metadata = parseMetadata(dir.resolve("metadata.toml"));
        String query = new String(Files.readAllBytes(dir.resolve("query.cqels")));
        List<Event> events = parseEvents(dir.resolve("streams.jsonl"));
        List<ExpectedBinding> expected = parseExpected(dir.resolve("expected.jsonl"));
        return new Fixture(metadata, query, events, expected);
    }

    private static Metadata parseMetadata(Path path) throws IOException {
        TomlParseResult toml = Toml.parse(path);
        if (toml.hasErrors()) {
            throw new IOException("metadata.toml parse errors: " + toml.errors());
        }
        Metadata m = new Metadata();
        m.name = toml.getString("name");
        m.description = toml.getString("description");
        m.groundTruth = toml.getString("ground_truth");
        Long drain = toml.getLong("drain_timeout_ms");
        m.drainTimeoutMs = drain == null ? null : drain.intValue();
        if (m.name == null) {
            throw new IOException(path + ": `name` is required");
        }
        return m;
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private static List<Event> parseEvents(Path path) throws IOException {
        List<Event> events = new ArrayList<>();
        for (String line : Files.readAllLines(path)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            JsonNode n = JSON.readTree(trimmed);
            Event e = new Event();
            e.stream = n.get("stream").asText();
            e.ts = n.get("ts").asLong();
            e.s = n.get("s").asText();
            e.p = n.get("p").asText();
            e.o = n.get("o").asText();
            events.add(e);
        }
        return events;
    }

    private static List<ExpectedBinding> parseExpected(Path path) throws IOException {
        List<ExpectedBinding> expected = new ArrayList<>();
        // `expected.jsonl` is optional — a freshly-created fixture used by
        // `cargo xtask parity diff` (which compares engine outputs against
        // each other rather than against a hand-spec oracle) hasn't had
        // its expected file written yet. Missing → no expected lines.
        if (!Files.exists(path)) {
            return expected;
        }
        for (String line : Files.readAllLines(path)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            JsonNode n = JSON.readTree(trimmed);
            ExpectedBinding b = new ExpectedBinding();
            Iterator<Map.Entry<String, JsonNode>> it = n.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> field = it.next();
                String k = field.getKey();
                if ("_ts".equals(k)) {
                    b.timestamp = field.getValue().asLong();
                    continue;
                }
                String stripped = stripVarPrefix(k);
                b.bindings.put(stripped, field.getValue().isTextual()
                        ? field.getValue().asText()
                        : field.getValue().toString());
            }
            expected.add(b);
        }
        return expected;
    }

    static String stripVarPrefix(String name) {
        if (name.startsWith("?") || name.startsWith("$")) {
            return name.substring(1);
        }
        return name;
    }

    public static final class Metadata {
        public String name;
        public String description;
        public String groundTruth;
        public Integer drainTimeoutMs;
    }

    public static final class Event {
        public String stream;
        public long ts;
        public String s;
        public String p;
        public String o;
    }

    /** One expected binding line. Variable names are stripped of the SPARQL `?`/`$` prefix. */
    public static final class ExpectedBinding {
        public final Map<String, String> bindings = new LinkedHashMap<>();
        public Long timestamp;
    }
}
