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
  cargo xtask bench-save-baseline"
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
