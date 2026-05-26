package cqels.parity.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * CLI entry point — Java counterpart to {@code parity-tests/runner-rust/src/main.rs}.
 *
 * <p>Loads one (or every) fixture under a directory, runs it through a
 * live cqels-java engine via {@link EngineDriver}, and diffs the
 * captured bindings against {@code expected.jsonl}. Exit codes mirror
 * the Rust runner so CI scripts can treat both interchangeably:</p>
 *
 * <ul>
 *   <li><b>0</b> — all bindings match expected, in order</li>
 *   <li><b>1</b> — mismatch (unified diff printed to stderr)</li>
 *   <li><b>2</b> — engine error while running</li>
 *   <li><b>3</b> — fixture failed to load (missing / malformed files)</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <pre>
 *   java -jar target/cqels-parity-runner-java-*-jar-with-dependencies.jar &lt;fixture-dir&gt;
 *   java -jar target/cqels-parity-runner-java-*-jar-with-dependencies.jar --all &lt;fixtures-root&gt;
 * </pre>
 */
public final class Runner {
    private static final int EXIT_OK = 0;
    private static final int EXIT_MISMATCH = 1;
    private static final int EXIT_ENGINE_ERROR = 2;
    private static final int EXIT_LOAD_ERROR = 3;

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) {
        int code;
        if (args.length == 2 && "--dump".equals(args[0])) {
            code = dumpOne(Paths.get(args[1]));
        } else if (args.length == 2 && "--all".equals(args[0])) {
            code = runAll(Paths.get(args[1]));
        } else if (args.length == 1
                && !"--all".equals(args[0])
                && !"--dump".equals(args[0])) {
            code = runOne(Paths.get(args[0]));
        } else {
            System.err.println("usage: cqels-parity-runner-java <fixture-dir>");
            System.err.println("       cqels-parity-runner-java --all <fixtures-root>");
            System.err.println("       cqels-parity-runner-java --dump <fixture-dir>");
            code = EXIT_LOAD_ERROR;
        }
        System.exit(code);
    }

    /**
     * Runs the fixture and emits the captured bindings as JSONL to
     * stdout, one binding per line, with no status messages or
     * framing. Used by `cargo xtask parity diff` / `parity capture`
     * to compare engine outputs against each other (or capture one
     * engine's output as the fixture's `expected.jsonl`) without
     * needing a hand-spec oracle.
     *
     * <p>Timestamps are omitted to match the Rust runner's `--dump`
     * default. The differential workflow targets binding content;
     * if a caller needs `_ts`, expose a `--with-ts` flag rather
     * than changing the default.
     */
    private static int dumpOne(Path dir) {
        Fixture fixture;
        try {
            fixture = Fixture.load(dir);
        } catch (IOException e) {
            System.err.println("[" + dir + "] load error: " + e.getMessage());
            return EXIT_LOAD_ERROR;
        }
        List<EngineDriver.CapturedBinding> captured;
        try {
            captured = new EngineDriver().run(fixture);
        } catch (IOException | RuntimeException e) {
            System.err.println("engine error: " + e);
            return EXIT_ENGINE_ERROR;
        }
        for (EngineDriver.CapturedBinding b : captured) {
            System.out.println(renderLine(b.bindings, null, false));
        }
        return EXIT_OK;
    }

    private static int runOne(Path dir) {
        Fixture fixture;
        try {
            fixture = Fixture.load(dir);
        } catch (IOException e) {
            System.err.println("[" + dir + "] load error: " + e.getMessage());
            return EXIT_LOAD_ERROR;
        }
        System.out.println("running `" + fixture.metadata.name + "` ...");

        List<EngineDriver.CapturedBinding> captured;
        try {
            captured = new EngineDriver().run(fixture);
        } catch (IOException e) {
            System.err.println("  engine error: " + e.getMessage());
            return EXIT_ENGINE_ERROR;
        } catch (RuntimeException e) {
            System.err.println("  engine error: " + e);
            return EXIT_ENGINE_ERROR;
        }

        String diff = compare(captured, fixture.expected);
        if (diff == null) {
            System.out.println("  ok (" + captured.size()
                    + " binding" + (captured.size() == 1 ? "" : "s") + ")");
            return EXIT_OK;
        }
        System.err.println("  FAIL");
        System.err.println(diff);
        return EXIT_MISMATCH;
    }

    private static int runAll(Path root) {
        if (!Files.isDirectory(root)) {
            System.err.println("not a directory: " + root);
            return EXIT_LOAD_ERROR;
        }
        List<Path> dirs = new ArrayList<>();
        try {
            for (Path p : (Iterable<Path>) Files.list(root)::iterator) {
                if (Files.isDirectory(p)) {
                    dirs.add(p);
                }
            }
        } catch (IOException e) {
            System.err.println("read fixtures root " + root + ": " + e.getMessage());
            return EXIT_LOAD_ERROR;
        }
        Collections.sort(dirs);
        if (dirs.isEmpty()) {
            System.err.println("no fixture directories under " + root);
            return EXIT_LOAD_ERROR;
        }
        int worst = EXIT_OK;
        for (Path d : dirs) {
            int code = runOne(d);
            if (code > worst) {
                worst = code;
            }
        }
        return worst;
    }

    /**
     * Returns {@code null} on match, or a unified-diff-ish report on mismatch.
     * Comparison is order-sensitive — same as the Rust runner. Timestamps
     * are only included in the comparison if at least one expected line
     * carries {@code _ts}.
     */
    static String compare(List<EngineDriver.CapturedBinding> actual,
                          List<Fixture.ExpectedBinding> expected) {
        boolean anyTs = false;
        for (Fixture.ExpectedBinding e : expected) {
            if (e.timestamp != null) { anyTs = true; break; }
        }

        List<String> expectedLines = new ArrayList<>(expected.size());
        for (Fixture.ExpectedBinding e : expected) {
            expectedLines.add(renderLine(e.bindings, anyTs ? e.timestamp : null, anyTs));
        }
        List<String> actualLines = new ArrayList<>(actual.size());
        for (EngineDriver.CapturedBinding b : actual) {
            actualLines.add(renderLine(b.bindings, anyTs ? b.timestamp : null, anyTs));
        }
        if (expectedLines.equals(actualLines)) {
            return null;
        }
        StringBuilder out = new StringBuilder("bindings mismatch (- expected, + actual):\n");
        int n = Math.max(expectedLines.size(), actualLines.size());
        for (int i = 0; i < n; i++) {
            String e = i < expectedLines.size() ? expectedLines.get(i) : null;
            String a = i < actualLines.size() ? actualLines.get(i) : null;
            if (e != null && e.equals(a)) {
                out.append("  ").append(e).append('\n');
            } else {
                if (e != null) out.append("- ").append(e).append('\n');
                if (a != null) out.append("+ ").append(a).append('\n');
            }
        }
        return out.toString();
    }

    private static String renderLine(Map<String, String> bindings, Long ts, boolean includeTs) {
        // Sort variable names so two bindings with identical content but
        // different insertion order compare equal — mirrors the BTreeMap
        // ordering the Rust runner uses.
        Map<String, String> sorted = new TreeMap<>(bindings);
        ObjectNode node = JSON.createObjectNode();
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            node.put(e.getKey(), e.getValue());
        }
        if (includeTs) {
            node.put("_ts", ts == null ? 0L : ts);
        }
        return node.toString();
    }
}
