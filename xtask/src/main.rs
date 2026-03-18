use std::collections::{BTreeSet, VecDeque};
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
  cargo xtask bench-observe"
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

    let summary =
        "Bench observation completed.\n\nArtifacts:\n- target/criterion\n- target/xtask/bench-observe\n"
            .to_string();
    fs::write(out_dir.join("summary.txt"), summary).context("writing bench summary")?;
    Ok(())
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
