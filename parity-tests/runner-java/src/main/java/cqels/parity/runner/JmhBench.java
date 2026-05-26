package cqels.parity.runner;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH counterpart of {@code cqels-benchmarks::parity_fixtures}.
 *
 * <p>Drives each parity fixture end-to-end through a fresh
 * {@link EngineDriver} once per measured operation. The numbers
 * produced are directly comparable to criterion's
 * {@code throughput=events_per_iter} so a side-by-side Rust ↔ Java
 * plot is a thin post-processing step.</p>
 *
 * <p>The fixtures-root defaults to {@code ../fixtures} (relative to
 * the Maven module), matching the Rust crate's discovery. Override
 * with {@code -p fixturesRoot=…} on the JMH command line:</p>
 *
 * <pre>
 *   java -jar target/parity-bench.jar -p fixturesRoot=/abs/path/parity-tests/fixtures
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JmhBench {
    /**
     * Comma-separated list of fixture names to benchmark. Empty
     * means auto-discover every directory under {@link #fixturesRoot}.
     * Lets a CI run trim the matrix without touching code.
     */
    @Param({""})
    public String fixtures;

    /** Root of the parity fixture corpus. Default assumes module-local layout. */
    @Param({"../fixtures"})
    public String fixturesRoot;

    private List<Fixture> loaded;

    @Setup(Level.Trial)
    public void load() throws IOException {
        Path root = Paths.get(fixturesRoot).toAbsolutePath();
        List<Path> dirs;
        if (fixtures == null || fixtures.isEmpty()) {
            dirs = new ArrayList<>();
            for (Path p : (Iterable<Path>) Files.list(root)::iterator) {
                if (Files.isDirectory(p)) {
                    dirs.add(p);
                }
            }
            Collections.sort(dirs);
        } else {
            dirs = new ArrayList<>();
            for (String name : fixtures.split(",")) {
                String trimmed = name.trim();
                if (!trimmed.isEmpty()) {
                    dirs.add(root.resolve(trimmed));
                }
            }
        }
        loaded = new ArrayList<>(dirs.size());
        for (Path d : dirs) {
            loaded.add(Fixture.load(d));
        }
        if (loaded.isEmpty()) {
            throw new IOException("no fixtures resolved from root=" + root);
        }
    }

    /**
     * Single benchmark over the whole fixture set — JMH reports one
     * average-time number per fixture is overkill at the current
     * fixture count, and aggregating mirrors how the criterion harness
     * presents the parity sweep. Split into per-fixture
     * {@code @Benchmark} methods once a fixture's runtime dominates
     * engine spin-up.
     */
    @Benchmark
    public void allFixtures(Blackhole bh) throws IOException {
        EngineDriver driver = new EngineDriver();
        for (Fixture f : loaded) {
            List<EngineDriver.CapturedBinding> out = driver.run(f);
            // Cheap correctness gate: if the engine stops emitting the
            // expected count for some reason, the benchmark surfaces it
            // rather than silently measuring a broken pipeline.
            if (out.size() != f.expected.size()) {
                throw new IllegalStateException(
                        "fixture " + f.metadata.name
                                + " expected " + f.expected.size()
                                + " bindings but produced " + out.size());
            }
            bh.consume(out);
        }
    }
}
