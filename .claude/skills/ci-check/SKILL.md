---
name: ci-check
description: Check latest GitHub Actions CI run status and diagnose failures
allowed-tools: Bash
---

# CI Status Check

Check the latest CI run on GitHub Actions and diagnose any failures:

1. **List recent runs**: `gh run list --repo HiveIntel/cqels-rs --limit 3`
2. **View latest run**: `gh run view <run_id> --repo HiveIntel/cqels-rs`
3. For any **failed jobs**, get the logs: `gh run view --job=<job_id> --repo HiveIntel/cqels-rs --log 2>&1 | grep -E "error" | head -20`
4. **Diagnose** and report what failed and why
5. If the fix is straightforward, apply it locally, run `/pre-commit`, then commit and push
