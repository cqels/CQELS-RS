use std::collections::{BTreeSet, HashMap, VecDeque};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};

const DEFAULT_BASE: &str = "origin/main";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ImpactArea {
    Model,
    Core,
    Engine,
    Reasoning,
    Geo,
    Asp,
    Shacl,
    StorageSpi,
    Docs,
    Benchmarks,
    Workflow,
    Full,
}

fn main() -> Result<()> {
    let mut args: VecDeque<String> = env::args().skip(1).collect();
    let Some(cmd) = args.pop_front() else {
        print_usage();
        bail!("missing xtask command");
    };

    match cmd.as_str() {
        "test" => run_test_subcommand(args),
        "coverage" => run_coverage(),
        "bench-observe" => run_bench_observe(),
        "bench-save-baseline" => run_bench_save_baseline(),
        "parity" => run_parity_sweep(args),
        other => bail!("unknown xtask command: {other}"),
    }
}

fn print_usage() {
    eprintln!(
        "usage:
  cargo xtask test pr [--base <git-ref>]
  cargo xtask test impact [--base <git-ref>]
  cargo xtask test full
  cargo xtask coverage
  cargo xtask bench-observe
  cargo xtask bench-save-baseline
  cargo xtask parity [--rust-only|--java-only]
  cargo xtask parity diff <fixture-dir>
  cargo xtask parity capture --engine <rust|java> <fixture-dir>"
    );
}

fn run_test_subcommand(mut args: VecDeque<String>) -> Result<()> {
    let Some(mode) = args.pop_front() else {
        bail!("missing `cargo xtask test` mode");
    };

    match mode.as_str() {
        "pr" => {
            let base = parse_base_arg(&mut args)?;
            run_pr_suite(&base)
        }
        "impact" => {
            let base = parse_base_arg(&mut args)?;
            run_impact_suite(&base)
        }
        "full" => run_full_suite(),
        other => bail!("unknown `cargo xtask test` mode: {other}"),
    }
}

fn parse_base_arg(args: &mut VecDeque<String>) -> Result<String> {
    let mut base = DEFAULT_BASE.to_string();
    while let Some(arg) = args.pop_front() {
        match arg.as_str() {
            "--base" => {
                let Some(value) = args.pop_front() else {
                    bail!("expected value after --base");
                };
                base = value;
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(base)
}

fn run_pr_suite(base: &str) -> Result<()> {
    run_command("cargo", &["fmt", "--all", "--check"])?;
    run_command(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run_command_env(
        "cargo",
        &["doc", "--workspace", "--no-deps"],
        &[("RUSTDOCFLAGS", "-Dwarnings")],
    )?;

    if has_cargo_subcommand("nextest") {
        run_command(
            "cargo",
            &[
                "nextest",
                "run",
                "--workspace",
                "--lib",
                "--bins",
                "--tests",
            ],
        )?;
    } else {
        run_command(
            "cargo",
            &["test", "--workspace", "--lib", "--bins", "--tests"],
        )?;
    }

    run_impact_suite(base)
}

fn run_full_suite() -> Result<()> {
    run_command("cargo", &["test", "--workspace"])
}

fn run_impact_suite(base: &str) -> Result<()> {
    let changed_files = collect_changed_files(base)?;
    if changed_files.is_empty() {
        println!("No impacted files found for base `{base}`; falling back to full suite.");
        return run_full_suite();
    }

    let areas = classify_impacts(&changed_files);
    if areas.contains(&ImpactArea::Full) {
        println!("Impact detector requested full suite.");
        return run_full_suite();
    }

    let commands = commands_for_areas(&areas);
    if commands.is_empty() {
        println!("No suite mapping for changed files; falling back to full suite.");
        return run_full_suite();
    }

    for command in commands {
        command.run()?;
    }
    Ok(())
}

fn run_coverage() -> Result<()> {
    if !has_cargo_subcommand("llvm-cov") {
        bail!(
            "`cargo llvm-cov` is not installed. Install it with `cargo install cargo-llvm-cov --locked`."
        );
    }

    let out_dir = PathBuf::from("target/xtask/coverage");
    fs::create_dir_all(&out_dir).context("creating coverage output directory")?;

    run_command("cargo", &["llvm-cov", "clean", "--workspace"])?;
    run_command(
        "cargo",
        &[
            "llvm-cov",
            "--workspace",
            "--lcov",
            "--output-path",
            "target/xtask/coverage/lcov.info",
        ],
    )?;

    let summary = capture_command("cargo", &["llvm-cov", "report", "--summary-only"], &[])?;
    fs::write(out_dir.join("summary.txt"), summary).context("writing coverage summary")?;
    Ok(())
}

fn run_bench_observe() -> Result<()> {
    let out_dir = PathBuf::from("target/xtask/bench-observe");
    fs::create_dir_all(&out_dir).context("creating bench-observe output directory")?;

    run_command(
        "cargo",
        &[
            "bench",
            "-p",
            "cqels-benchmarks",
            "--bench",
            "stream_throughput",
        ],
    )?;

    // Collect benchmark results from Criterion estimates.json files
    let results = collect_criterion_results()?;
    let current_path = out_dir.join("results.json");
    let baseline_path = PathBuf::from("bench-baseline.json");

    // Write current results
    let results_json = serde_json_minimal(&results);
    fs::write(&current_path, &results_json).context("writing bench results")?;

    // Compare against baseline if it exists
    let mut summary = String::from("Bench observation completed.\n\n");
    if baseline_path.exists() {
        let baseline_data = fs::read_to_string(&baseline_path).context("reading baseline")?;
        let baseline = parse_bench_results(&baseline_data);
        let regression_threshold = 1.10; // 10% regression threshold

        let mut regressions = Vec::new();
        for (name, current_ns) in &results {
            if let Some(baseline_ns) = baseline.get(name.as_str()) {
                let ratio = *current_ns / baseline_ns;
                if ratio > regression_threshold {
                    regressions.push(format!(
                        "  REGRESSION: {name}: {:.2}x slower ({:.0}ns -> {:.0}ns)",
                        ratio, baseline_ns, current_ns
                    ));
                }
            }
        }

        if regressions.is_empty() {
            summary.push_str("No regressions detected (threshold: 10%).\n");
        } else {
            summary.push_str(&format!(
                "WARNING: {} regression(s) detected:\n{}\n",
                regressions.len(),
                regressions.join("\n")
            ));
        }
    } else {
        summary
            .push_str("No baseline found. Run `cargo xtask bench-save-baseline` to create one.\n");
    }

    summary.push_str("\nArtifacts:\n- target/criterion\n- target/xtask/bench-observe\n");
    fs::write(out_dir.join("summary.txt"), &summary).context("writing bench summary")?;
    print!("{summary}");
    Ok(())
}

fn run_bench_save_baseline() -> Result<()> {
    let results_path = PathBuf::from("target/xtask/bench-observe/results.json");
    let baseline_path = PathBuf::from("bench-baseline.json");

    if !results_path.exists() {
        bail!("No results found. Run `cargo xtask bench-observe` first.");
    }

    fs::copy(&results_path, &baseline_path).context("saving baseline")?;
    println!("Baseline saved to {}", baseline_path.display());
    Ok(())
}

/// Collects benchmark results from Criterion's estimates.json files.
fn collect_criterion_results() -> Result<Vec<(String, f64)>> {
    let criterion_dir = PathBuf::from("target/criterion");
    let mut results = Vec::new();

    if !criterion_dir.exists() {
        return Ok(results);
    }

    // Walk the criterion directory looking for estimates.json files
    collect_estimates_recursive(&criterion_dir, "", &mut results)?;
    results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(results)
}

fn collect_estimates_recursive(
    dir: &PathBuf,
    prefix: &str,
    results: &mut Vec<(String, f64)>,
) -> Result<()> {
    let new_dir = dir.join("new");
    let estimates = new_dir.join("estimates.json");

    if estimates.exists() {
        if let Ok(data) = fs::read_to_string(&estimates) {
            // Extract point_estimate.point_estimate from the JSON
            if let Some(ns) = extract_point_estimate(&data) {
                let name = if prefix.is_empty() {
                    dir.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default()
                } else {
                    prefix.to_string()
                };
                results.push((name, ns));
            }
        }
    }

    // Recurse into subdirectories
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                if name == "new" || name == "base" || name == "change" || name == "report" {
                    continue;
                }
                let child_prefix = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                };
                collect_estimates_recursive(&path, &child_prefix, results)?;
            }
        }
    }
    Ok(())
}

/// Extracts the mean point estimate (nanoseconds) from Criterion estimates.json.
fn extract_point_estimate(json: &str) -> Option<f64> {
    // Simple extraction: find "mean":{"confidence_interval":{...},"point_estimate":NUMBER
    let mean_idx = json.find("\"mean\"")?;
    let after_mean = &json[mean_idx..];
    let pe_idx = after_mean.find("\"point_estimate\"")?;
    let after_pe = &after_mean[pe_idx + "\"point_estimate\"".len()..];
    let colon_idx = after_pe.find(':')?;
    let after_colon = after_pe[colon_idx + 1..].trim_start();
    // Read the number until comma, brace, or bracket
    let end = after_colon
        .find([',', '}', ']'])
        .unwrap_or(after_colon.len());
    after_colon[..end].trim().parse::<f64>().ok()
}

/// Minimal JSON serialization for benchmark results.
fn serde_json_minimal(results: &[(String, f64)]) -> String {
    let entries: Vec<String> = results
        .iter()
        .map(|(name, ns)| format!("  \"{name}\": {ns:.2}"))
        .collect();
    format!("{{\n{}\n}}\n", entries.join(",\n"))
}

/// Parses benchmark results from a JSON file.
fn parse_bench_results(json: &str) -> HashMap<String, f64> {
    let mut results = HashMap::new();
    // Simple line-by-line parsing of our minimal JSON format
    for line in json.lines() {
        let line = line.trim().trim_end_matches(',');
        if let Some(rest) = line.strip_prefix('"') {
            if let Some(name_end) = rest.find('"') {
                let name = &rest[..name_end];
                if let Some(colon_idx) = rest.find(':') {
                    if let Ok(val) = rest[colon_idx + 1..].trim().parse::<f64>() {
                        results.insert(name.to_string(), val);
                    }
                }
            }
        }
    }
    results
}

fn collect_changed_files(base: &str) -> Result<Vec<String>> {
    let mut files = BTreeSet::new();
    let base_range = format!("{base}...HEAD");
    let command_sets = vec![
        vec!["diff".to_string(), "--name-only".to_string(), base_range],
        vec![
            "diff".to_string(),
            "--name-only".to_string(),
            "--cached".to_string(),
        ],
        vec!["diff".to_string(), "--name-only".to_string()],
    ];

    for args in command_sets {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let output = capture_command("git", &arg_refs, &[])?;
        for line in output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            files.insert(line.to_string());
        }
    }
    Ok(files.into_iter().collect())
}

fn classify_impacts(paths: &[String]) -> BTreeSet<ImpactArea> {
    let mut areas = BTreeSet::new();

    for path in paths {
        let area = if path.starts_with("cqels-model/") {
            ImpactArea::Model
        } else if path.starts_with("cqels-core/") {
            ImpactArea::Core
        } else if path.starts_with("cqels-engine/") {
            ImpactArea::Engine
        } else if path.starts_with("cqels-reasoning/") {
            ImpactArea::Reasoning
        } else if path.starts_with("cqels-geo/") {
            ImpactArea::Geo
        } else if path.starts_with("cqels-asp/") {
            ImpactArea::Asp
        } else if path.starts_with("cqels-shacl/") {
            ImpactArea::Shacl
        } else if path.starts_with("cqels-storage-spi/") {
            ImpactArea::StorageSpi
        } else if path.starts_with("cqels-benchmarks/") {
            ImpactArea::Benchmarks
        } else if path.starts_with(".github/") {
            ImpactArea::Workflow
        } else if path.starts_with("docs/") {
            ImpactArea::Docs
        } else {
            ImpactArea::Full
        };
        areas.insert(area);
    }

    areas
}

fn commands_for_areas(areas: &BTreeSet<ImpactArea>) -> Vec<TaskCommand> {
    let mut commands = BTreeSet::new();

    for area in areas {
        match area {
            ImpactArea::Model => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-model",
                    "--lib",
                    "--tests",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-model",
                    "--test",
                    "proptest_model",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "query_language_regressions",
                    "issue_serialization_contract_roundtrip",
                ]));
            }
            ImpactArea::Core => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-core",
                    "--lib",
                    "--tests",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-core",
                    "--test",
                    "proptest_parsers",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-core",
                    "--test",
                    "proptest_windows",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "query_language_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "window_aggregation_regressions",
                ]));
            }
            ImpactArea::Engine => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-engine",
                    "--lib",
                    "--tests",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "runtime_lifecycle_regressions",
                ]));
            }
            ImpactArea::Reasoning => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-reasoning",
                    "--lib",
                    "--tests",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "reasoning_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "runtime_lifecycle_regressions",
                ]));
            }
            ImpactArea::Geo => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-geo",
                    "--lib",
                    "--tests",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "geo_regressions",
                ]));
            }
            ImpactArea::Asp => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-asp",
                    "--lib",
                    "--tests",
                ]));
            }
            ImpactArea::Shacl => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-shacl",
                    "--lib",
                    "--tests",
                ]));
            }
            ImpactArea::StorageSpi => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-storage-spi",
                    "--lib",
                    "--tests",
                ]));
            }
            ImpactArea::Benchmarks => {
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "query_language_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "runtime_lifecycle_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "reasoning_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "geo_regressions",
                ]));
                commands.insert(TaskCommand::cargo_test(&[
                    "-p",
                    "cqels-benchmarks",
                    "--test",
                    "window_aggregation_regressions",
                ]));
            }
            ImpactArea::Docs | ImpactArea::Workflow => {
                commands.insert(TaskCommand::cargo_doc());
            }
            ImpactArea::Full => {}
        }
    }

    commands.into_iter().collect()
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TaskCommand {
    program: &'static str,
    args: Vec<String>,
    env: Vec<(&'static str, &'static str)>,
}

impl TaskCommand {
    fn cargo_test(args: &[&str]) -> Self {
        Self {
            program: "cargo",
            args: std::iter::once("test".to_string())
                .chain(args.iter().map(|arg| (*arg).to_string()))
                .collect(),
            env: Vec::new(),
        }
    }

    fn cargo_doc() -> Self {
        Self {
            program: "cargo",
            args: vec!["doc".into(), "--workspace".into(), "--no-deps".into()],
            env: vec![("RUSTDOCFLAGS", "-Dwarnings")],
        }
    }

    fn run(&self) -> Result<()> {
        run_command_env(self.program, &self.args_as_refs(), &self.env)
    }

    fn args_as_refs(&self) -> Vec<&str> {
        self.args.iter().map(String::as_str).collect()
    }
}

fn has_cargo_subcommand(subcommand: &str) -> bool {
    Command::new("cargo")
        .args([subcommand, "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run_command(program: &str, args: &[&str]) -> Result<()> {
    run_command_env(program, args, &[])
}

fn run_command_env(program: &str, args: &[&str], envs: &[(&str, &str)]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .envs(envs.iter().copied())
        .status()
        .with_context(|| format!("running `{program} {}`", args.join(" ")))?;

    if !status.success() {
        bail!("command failed: `{program} {}`", args.join(" "));
    }
    Ok(())
}

fn capture_command(program: &str, args: &[&str], envs: &[(&str, &str)]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .envs(envs.iter().copied())
        .output()
        .with_context(|| format!("running `{program} {}`", args.join(" ")))?;

    if !output.status.success() {
        bail!(
            "command failed: `{program} {}`\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout).map_err(|err| anyhow!(err))
}

// ─── parity sweep ────────────────────────────────────────────────────
//
// Runs every fixture under `parity-tests/fixtures/` through the Rust
// runner and the Java runner, then prints a side-by-side pass/fail
// table. Designed as the single command an operator runs to answer
// "where do we stand on parity?" without having to remember Maven
// invocations or the runner-jar paths.
//
// Per-runner output is captured into `target/xtask/parity/` so a
// failing row can be drilled into without re-running.
//
// Skips:
// - Rust runner is unavailable: bails. (cargo is always present.)
// - Java runner is unavailable (mvn missing, or cqels/claude not in
//   the local Maven repo): the Java column is reported as `skip` and
//   the command still exits 0 if the Rust side passes. This matches
//   the CI job's behavior — keeps the command useful for contributors
//   who don't have access to cqels/claude.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunStatus {
    Ok,
    Mismatch,
    EngineError,
    LoadError,
    Skipped,
}

impl RunStatus {
    fn from_exit_code(code: i32) -> Self {
        match code {
            0 => RunStatus::Ok,
            1 => RunStatus::Mismatch,
            2 => RunStatus::EngineError,
            3 => RunStatus::LoadError,
            _ => RunStatus::EngineError,
        }
    }

    fn label(self) -> &'static str {
        match self {
            RunStatus::Ok => "ok",
            RunStatus::Mismatch => "FAIL",
            RunStatus::EngineError => "ERR",
            RunStatus::LoadError => "LOAD",
            RunStatus::Skipped => "skip",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Rust,
    Java,
    Both,
}

fn run_parity_sweep(mut args: VecDeque<String>) -> Result<()> {
    // Dispatch on subcommand before falling through to the sweep.
    // The legacy `--rust-only` / `--java-only` flags are NOT
    // subcommands, so we only intercept long-form names here.
    if let Some(first) = args.front() {
        match first.as_str() {
            "diff" => {
                args.pop_front();
                return run_parity_diff(args);
            }
            "capture" => {
                args.pop_front();
                return run_parity_capture(args);
            }
            _ => {}
        }
    }

    let mut side = Side::Both;
    while let Some(arg) = args.pop_front() {
        match arg.as_str() {
            "--rust-only" => side = Side::Rust,
            "--java-only" => side = Side::Java,
            other => bail!("unknown flag for `cargo xtask parity`: {other}"),
        }
    }

    let fixtures = discover_fixtures()?;
    if fixtures.is_empty() {
        bail!("no fixture directories under parity-tests/fixtures/");
    }

    let log_root = PathBuf::from("target/xtask/parity");
    fs::create_dir_all(&log_root).context("creating target/xtask/parity")?;

    let rust_log = log_root.join("parity-rust.log");
    let java_log = log_root.join("parity-java.log");

    let rust_results = if side != Side::Java {
        run_rust_side(&fixtures, &rust_log)?
    } else {
        skipped_results(&fixtures)
    };

    let java_results = if side != Side::Rust {
        run_java_side(&fixtures, &java_log)?
    } else {
        skipped_results(&fixtures)
    };

    print_parity_summary(
        &fixtures,
        &rust_results,
        &java_results,
        &rust_log,
        &java_log,
    );

    // Exit-code contract (#82): FAIL (mismatch) stays informational —
    // until each fixture's expected pass/fail status lives in its
    // `metadata.toml`, distinguishing real regressions from
    // known-failing fixtures requires upstream context the xtask
    // doesn't have. But ERR (runner crash) and LOAD (fixture data
    // didn't load) are never expected: they mean the sweep didn't
    // actually measure parity for that fixture, so automated gating
    // (CI, hooks) must not see exit 0.
    let broken = unexpected_failures(&rust_results, &java_results);
    if !broken.is_empty() {
        bail!(
            "parity sweep had unexpected runner/load failures (ERR/LOAD): {} — \
             see the table above and logs under target/xtask/parity/",
            broken.join(", ")
        );
    }
    Ok(())
}

/// Fixtures whose status on either side is an unexpected failure — the runner
/// crashed (`ERR`) or fixture data failed to load (`LOAD`). `FAIL` (output
/// mismatch) and `skip` are NOT included: mismatches are informational until
/// expected-status metadata exists, and skips are deliberate.
fn unexpected_failures(
    rust_results: &[(String, RunStatus)],
    java_results: &[(String, RunStatus)],
) -> Vec<String> {
    let is_broken = |s: &RunStatus| matches!(s, RunStatus::EngineError | RunStatus::LoadError);
    let mut names: Vec<String> = rust_results
        .iter()
        .chain(java_results.iter())
        .filter(|(_, s)| is_broken(s))
        .map(|(name, _)| name.clone())
        .collect();
    names.sort();
    names.dedup();
    names
}

fn discover_fixtures() -> Result<Vec<String>> {
    let root = PathBuf::from("parity-tests/fixtures");
    let entries = fs::read_dir(&root).with_context(|| format!("reading {}", root.display()))?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

fn skipped_results(fixtures: &[String]) -> Vec<(String, RunStatus)> {
    fixtures
        .iter()
        .map(|f| (f.clone(), RunStatus::Skipped))
        .collect()
}

fn run_rust_side(
    fixtures: &[String],
    log_path: &std::path::Path,
) -> Result<Vec<(String, RunStatus)>> {
    println!("── Rust runner (cqels-rs) ──");
    // Build once in release so per-fixture runs are fast.
    println!("  building runner-rust (release) ...");
    let build_status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "--manifest-path",
            "parity-tests/runner-rust/Cargo.toml",
        ])
        .status()
        .context("invoking cargo build for runner-rust")?;
    if !build_status.success() {
        bail!("runner-rust build failed");
    }

    let runner = PathBuf::from("parity-tests/runner-rust/target/release/cqels-parity-runner");
    if !runner.exists() {
        bail!("expected runner binary at {} after build", runner.display());
    }

    let mut log =
        fs::File::create(log_path).with_context(|| format!("creating {}", log_path.display()))?;
    let mut results = Vec::with_capacity(fixtures.len());
    for fixture in fixtures {
        let fixture_path = format!("parity-tests/fixtures/{fixture}");
        let status = run_one_fixture(&runner, &fixture_path, &mut log, fixture)?;
        println!("  {:<32}  {}", fixture, status.label());
        results.push((fixture.clone(), status));
    }
    Ok(results)
}

fn run_java_side(
    fixtures: &[String],
    log_path: &std::path::Path,
) -> Result<Vec<(String, RunStatus)>> {
    println!("── Java runner (cqels-java / cqels/claude) ──");

    if !command_exists("mvn") {
        println!("  mvn not found on PATH — Java column skipped.");
        return Ok(skipped_results(fixtures));
    }

    println!("  building runner-java (mvn package) ...");
    let build_status = Command::new("mvn")
        .args(["-q", "-B", "-DskipTests", "package"])
        .current_dir("parity-tests/runner-java")
        .status()
        .context("invoking mvn package for runner-java")?;
    if !build_status.success() {
        println!(
            "  mvn package failed (cqels/claude likely not installed locally) — Java column skipped."
        );
        return Ok(skipped_results(fixtures));
    }

    let jar = find_runner_jar()?;
    let mut log =
        fs::File::create(log_path).with_context(|| format!("creating {}", log_path.display()))?;
    let mut results = Vec::with_capacity(fixtures.len());
    for fixture in fixtures {
        let fixture_path = format!("../fixtures/{fixture}");
        let status = run_one_fixture_java(&jar, &fixture_path, &mut log, fixture)?;
        println!("  {:<32}  {}", fixture, status.label());
        results.push((fixture.clone(), status));
    }
    Ok(results)
}

fn find_runner_jar() -> Result<PathBuf> {
    let dir = PathBuf::from("parity-tests/runner-java/target");
    for entry in fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("cqels-parity-runner-java-")
            && name_str.ends_with("-jar-with-dependencies.jar")
        {
            // Return an absolute path so callers that change cwd to
            // `parity-tests/runner-java/` (to keep the fixture path
            // `../fixtures/<name>` working) can still find the jar.
            return entry
                .path()
                .canonicalize()
                .with_context(|| format!("canonicalizing {}", entry.path().display()));
        }
    }
    bail!(
        "could not find runner-java jar-with-dependencies in {}",
        dir.display()
    );
}

fn run_one_fixture(
    runner: &std::path::Path,
    fixture_path: &str,
    log: &mut fs::File,
    fixture: &str,
) -> Result<RunStatus> {
    use std::io::Write;
    let output = Command::new(runner)
        .arg(fixture_path)
        .output()
        .with_context(|| format!("running rust runner against {fixture}"))?;
    writeln!(log, "── {fixture} ──").ok();
    log.write_all(&output.stdout).ok();
    log.write_all(&output.stderr).ok();
    Ok(RunStatus::from_exit_code(
        output.status.code().unwrap_or(-1),
    ))
}

fn run_one_fixture_java(
    jar: &std::path::Path,
    fixture_path: &str,
    log: &mut fs::File,
    fixture: &str,
) -> Result<RunStatus> {
    use std::io::Write;
    // Java runner expects fixture path relative to its CWD. We invoke
    // it from `parity-tests/runner-java/` so the same relative form
    // (`../fixtures/<name>`) works as in the README.
    let output = Command::new("java")
        .args(["-jar", jar.to_str().unwrap()])
        .arg(fixture_path)
        .current_dir("parity-tests/runner-java")
        .output()
        .with_context(|| format!("running java runner against {fixture}"))?;
    writeln!(log, "── {fixture} ──").ok();
    log.write_all(&output.stdout).ok();
    log.write_all(&output.stderr).ok();
    Ok(RunStatus::from_exit_code(
        output.status.code().unwrap_or(-1),
    ))
}

fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn print_parity_summary(
    fixtures: &[String],
    rust: &[(String, RunStatus)],
    java: &[(String, RunStatus)],
    rust_log: &std::path::Path,
    java_log: &std::path::Path,
) {
    println!();
    println!("Parity sweep — cqels-rs vs cqels-java");
    println!();
    let width = fixtures.iter().map(|f| f.len()).max().unwrap_or(20).max(20);
    println!("  {:<width$} | Rust | Java", "Fixture", width = width);
    println!("  {:-<width$} | ---- | ----", "", width = width);
    let mut rust_pass = 0;
    let mut java_pass = 0;
    let mut java_skipped = 0;
    for fixture in fixtures {
        let r = rust.iter().find(|(f, _)| f == fixture).map(|(_, s)| *s);
        let j = java.iter().find(|(f, _)| f == fixture).map(|(_, s)| *s);
        if matches!(r, Some(RunStatus::Ok)) {
            rust_pass += 1;
        }
        match j {
            Some(RunStatus::Ok) => java_pass += 1,
            Some(RunStatus::Skipped) => java_skipped += 1,
            _ => {}
        }
        println!(
            "  {:<width$} | {:<4} | {:<4}",
            fixture,
            r.map(|s| s.label()).unwrap_or("?"),
            j.map(|s| s.label()).unwrap_or("?"),
            width = width
        );
    }
    println!("  {:-<width$} | ---- | ----", "", width = width);
    let total = fixtures.len();
    let rust_skipped = rust
        .iter()
        .filter(|(_, s)| *s == RunStatus::Skipped)
        .count();
    let rust_summary = if rust_skipped == total {
        "skip".to_string()
    } else {
        format!("{rust_pass}/{total}")
    };
    let java_summary = if java_skipped == total {
        "skip".to_string()
    } else {
        format!("{java_pass}/{total}")
    };
    println!(
        "  {:<width$} | {:<4} | {:<4}",
        "TOTAL",
        rust_summary,
        java_summary,
        width = width
    );
    println!();
    println!("  Logs:");
    println!("    Rust:  {}", rust_log.display());
    println!("    Java:  {}", java_log.display());
    println!();
    println!(
        "  Status legend: ok=match, FAIL=binding mismatch, ERR=engine error, LOAD=fixture parse error, skip=runner unavailable"
    );
}

// ─── parity diff / parity capture ────────────────────────────────────
//
// Two follow-on workflows on top of `cargo xtask parity`:
//
// - `diff <fixture-dir>`    — run both engines, compare their captured
//   bindings directly. Bypasses `expected.jsonl` entirely. Lets you
//   answer "do the two engines agree on this query?" without writing
//   a hand-spec oracle.
//
// - `capture --engine <rust|java> <fixture-dir>` — run the chosen
//   engine and write its captured bindings as the fixture's
//   `expected.jsonl`. Flips `ground_truth` in `metadata.toml` to
//   `<engine>-captured`. Lets you take a snapshot you trust and pin it
//   for future regression-gating.
//
// Both rely on a `--dump` mode added to each runner: emit captured
// bindings as JSONL on stdout, nothing else. Status / "ok" / "FAIL"
// messages and any engine warnings still go to stderr, so the runner
// stdout is a clean machine-readable artifact.

fn run_parity_diff(mut args: VecDeque<String>) -> Result<()> {
    let dir = args
        .pop_front()
        .context("missing <fixture-dir> argument for `cargo xtask parity diff`")?;
    if let Some(extra) = args.pop_front() {
        bail!("unexpected extra argument: {extra}");
    }
    let dir_path = PathBuf::from(&dir);
    if !dir_path.is_dir() {
        bail!("not a fixture directory: {}", dir_path.display());
    }

    println!("── building runners ──");
    let rust_runner = ensure_rust_runner_built()?;
    let java_jar = ensure_java_runner_built();

    println!();
    println!("── Rust engine ──");
    let rust_out = invoke_rust_dump(&rust_runner, &dir_path)?;
    print!("{rust_out}");
    if !rust_out.ends_with('\n') {
        println!();
    }

    let java_out = match java_jar {
        Ok(jar) => {
            println!();
            println!("── Java engine ──");
            let out = invoke_java_dump(&jar, &dir_path)?;
            print!("{out}");
            if !out.ends_with('\n') {
                println!();
            }
            Some(out)
        }
        Err(reason) => {
            println!();
            println!("── Java engine ──");
            println!("(skipped: {reason})");
            None
        }
    };

    println!();
    println!("── Verdict ──");
    match java_out {
        Some(java_out) if java_out == rust_out => {
            let n = rust_out.lines().count();
            println!(
                "✅ Both engines produced identical output ({n} binding{})",
                if n == 1 { "" } else { "s" }
            );
            Ok(())
        }
        Some(java_out) => {
            println!("❌ Outputs differ");
            print_line_diff(&rust_out, &java_out);
            bail!("engines disagree on {}", dir_path.display());
        }
        None => {
            println!(
                "⚠ Java runner unavailable — only Rust output captured. \
                Cannot answer the differential question with one engine."
            );
            Ok(())
        }
    }
}

fn run_parity_capture(mut args: VecDeque<String>) -> Result<()> {
    let mut engine: Option<String> = None;
    let mut dir: Option<String> = None;
    while let Some(arg) = args.pop_front() {
        match arg.as_str() {
            "--engine" => {
                engine = Some(
                    args.pop_front()
                        .context("expected `rust` or `java` after `--engine`")?,
                );
            }
            other if !other.starts_with("--") => {
                if dir.is_some() {
                    bail!("unexpected extra argument: {other}");
                }
                dir = Some(other.to_string());
            }
            other => bail!("unknown flag for `cargo xtask parity capture`: {other}"),
        }
    }
    let engine = engine.context("missing required flag `--engine <rust|java>`")?;
    let dir = dir.context("missing <fixture-dir> argument")?;
    let dir_path = PathBuf::from(&dir);
    if !dir_path.is_dir() {
        bail!("not a fixture directory: {}", dir_path.display());
    }

    let dump = match engine.as_str() {
        "rust" => {
            let runner = ensure_rust_runner_built()?;
            invoke_rust_dump(&runner, &dir_path)?
        }
        "java" => {
            let jar = ensure_java_runner_built()
                .map_err(|reason| anyhow!("Java runner unavailable: {reason}"))?;
            invoke_java_dump(&jar, &dir_path)?
        }
        other => bail!("unknown engine: `{other}` (expected `rust` or `java`)"),
    };

    let expected_path = dir_path.join("expected.jsonl");
    fs::write(&expected_path, &dump)
        .with_context(|| format!("writing {}", expected_path.display()))?;

    let metadata_path = dir_path.join("metadata.toml");
    let new_ground_truth = format!("{engine}-captured");
    update_ground_truth(&metadata_path, &new_ground_truth)?;

    let n = dump.lines().count();
    println!(
        "Captured {n} binding{} from the {engine} engine into {}",
        if n == 1 { "" } else { "s" },
        expected_path.display()
    );
    println!(
        "Updated `ground_truth` to `\"{new_ground_truth}\"` in {}",
        metadata_path.display()
    );
    Ok(())
}

/// Builds `parity-tests/runner-rust` in release and returns the path
/// to the produced binary. Cargo is idempotent — re-running when
/// nothing changed is fast.
fn ensure_rust_runner_built() -> Result<PathBuf> {
    println!("  cargo build --release -p cqels-parity-runner ...");
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "--manifest-path",
            "parity-tests/runner-rust/Cargo.toml",
        ])
        .status()
        .context("invoking cargo build for runner-rust")?;
    if !status.success() {
        bail!("runner-rust build failed");
    }
    let path = PathBuf::from("parity-tests/runner-rust/target/release/cqels-parity-runner");
    if !path.exists() {
        bail!("expected runner binary at {}", path.display());
    }
    Ok(path)
}

/// Builds `parity-tests/runner-java` and returns the path to the
/// jar-with-dependencies. On any failure (mvn missing, cqels/claude
/// not installed locally, build error) returns Err with a short
/// human-readable reason so the caller can decide to skip the Java
/// side rather than abort.
fn ensure_java_runner_built() -> std::result::Result<PathBuf, String> {
    if !command_exists("mvn") {
        return Err("mvn not found on PATH".to_string());
    }
    println!("  mvn -q -DskipTests package (runner-java) ...");
    let status = Command::new("mvn")
        .args(["-q", "-B", "-DskipTests", "package"])
        .current_dir("parity-tests/runner-java")
        .status()
        .map_err(|e| format!("invoking mvn: {e}"))?;
    if !status.success() {
        return Err("mvn package failed (cqels/claude likely not installed locally)".to_string());
    }
    find_runner_jar().map_err(|e| format!("{e:#}"))
}

fn invoke_rust_dump(runner: &std::path::Path, fixture_dir: &std::path::Path) -> Result<String> {
    let output = Command::new(runner)
        .args(["--dump", &fixture_dir.to_string_lossy()])
        .output()
        .with_context(|| format!("running Rust runner --dump {}", fixture_dir.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Rust runner exited {} on --dump {}\nstderr:\n{stderr}",
            output.status.code().unwrap_or(-1),
            fixture_dir.display()
        );
    }
    String::from_utf8(output.stdout).context("Rust runner emitted non-UTF-8 dump output")
}

fn invoke_java_dump(jar: &std::path::Path, fixture_dir: &std::path::Path) -> Result<String> {
    // Use an absolute path so the runner sees the fixture regardless
    // of the working directory we hand it. ensure_java_runner_built()
    // already canonicalized the jar path.
    let abs_fixture = fixture_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", fixture_dir.display()))?;
    let output = Command::new("java")
        .args([
            // Match the JVM flags `run_one_fixture_java` uses so the
            // dump invocation is also free of slf4j INFO chatter — the
            // dump path writes captured bindings to stdout but engine
            // warnings still go to stderr, and we don't want to
            // re-introduce floor-level noise that obscures real signal.
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
            "-Dorg.slf4j.simpleLogger.showDateTime=false",
            "-Dorg.slf4j.simpleLogger.showThreadName=false",
            "-Dorg.slf4j.simpleLogger.showShortLogName=true",
            "-jar",
            jar.to_str().unwrap(),
            "--dump",
            abs_fixture.to_str().unwrap(),
        ])
        .output()
        .with_context(|| format!("running Java runner --dump {}", fixture_dir.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Java runner exited {} on --dump {}\nstderr:\n{stderr}",
            output.status.code().unwrap_or(-1),
            fixture_dir.display()
        );
    }
    String::from_utf8(output.stdout).context("Java runner emitted non-UTF-8 dump output")
}

/// Order-sensitive line-by-line diff. Mirrors the output style the
/// per-runner FAIL diffs use, so a user familiar with `cargo xtask
/// parity` output recognises the format here. Order-sensitive is the
/// right call: streaming-binding order is part of the semantics.
fn print_line_diff(rust: &str, java: &str) {
    let rust_lines: Vec<&str> = rust.lines().collect();
    let java_lines: Vec<&str> = java.lines().collect();
    let n = rust_lines.len().max(java_lines.len());
    println!("  (- Rust, + Java)");
    for i in 0..n {
        match (rust_lines.get(i), java_lines.get(i)) {
            (Some(r), Some(j)) if r == j => println!("    {r}"),
            (Some(r), Some(j)) => {
                println!("  - {r}");
                println!("  + {j}");
            }
            (Some(r), None) => println!("  - {r}"),
            (None, Some(j)) => println!("  + {j}"),
            (None, None) => {}
        }
    }
}

/// Replaces the `ground_truth = "..."` line in a TOML file with the
/// supplied value. If the line is missing, appends one. Uses
/// line-oriented string surgery rather than a TOML parser so the
/// surrounding formatting / multi-line strings / comments are
/// preserved untouched.
fn update_ground_truth(path: &std::path::Path, new_value: &str) -> Result<()> {
    let original =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut found = false;
    let mut out = String::with_capacity(original.len() + new_value.len());
    for line in original.lines() {
        // Match `ground_truth = "..."` (optionally indented). The
        // fixtures don't use indented keys, but be tolerant.
        let trimmed = line.trim_start();
        if trimmed.starts_with("ground_truth") && trimmed.contains('=') {
            out.push_str(&format!("ground_truth = \"{new_value}\"\n"));
            found = true;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    if !found {
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&format!("ground_truth = \"{new_value}\"\n"));
    }
    // Preserve trailing-newline behavior: if the original ended without
    // one, our writer adds one anyway — `fs::write` of a string that
    // ends in '\n' is the conventional shape for hand-edited TOML.
    fs::write(path, out).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn results(pairs: &[(&str, RunStatus)]) -> Vec<(String, RunStatus)> {
        pairs.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    #[test]
    fn unexpected_failures_empty_for_ok_and_mismatch_and_skip() {
        // FAIL (mismatch) is informational until expected-status metadata
        // exists; skip is deliberate. Neither may flip the exit code (#82).
        let rust = results(&[
            ("a", RunStatus::Ok),
            ("b", RunStatus::Mismatch),
            ("c", RunStatus::Skipped),
        ]);
        let java = results(&[
            ("a", RunStatus::Mismatch),
            ("b", RunStatus::Ok),
            ("c", RunStatus::Skipped),
        ]);
        assert!(unexpected_failures(&rust, &java).is_empty());
    }

    #[test]
    fn unexpected_failures_flags_engine_error_on_either_side() {
        let rust = results(&[("a", RunStatus::Ok), ("b", RunStatus::EngineError)]);
        let java = results(&[("a", RunStatus::EngineError), ("b", RunStatus::Ok)]);
        assert_eq!(unexpected_failures(&rust, &java), vec!["a", "b"]);
    }

    #[test]
    fn unexpected_failures_flags_load_error_and_dedups_across_sides() {
        // Same fixture broken on both sides reports once.
        let rust = results(&[("a", RunStatus::LoadError), ("b", RunStatus::Ok)]);
        let java = results(&[("a", RunStatus::EngineError), ("b", RunStatus::Mismatch)]);
        assert_eq!(unexpected_failures(&rust, &java), vec!["a"]);
    }
}
