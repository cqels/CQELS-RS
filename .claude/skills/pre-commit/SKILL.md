---
name: pre-commit
description: Run full CI checks locally (fmt, clippy, test, docs) before committing
allowed-tools: Bash
---

# Pre-Commit CI Check

Run the complete CI pipeline locally to catch issues before pushing. Execute these steps sequentially, stopping on first failure:

1. **Format**: `~/.cargo/bin/cargo fmt --all`
2. **Format verify**: `~/.cargo/bin/cargo fmt --all --check` — if this fails, formatting was applied in step 1, re-run step 2
3. **Clippy**: `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
4. **Tests**: `~/.cargo/bin/cargo test --workspace`
5. **Docs**: `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`

If all pass, report success with test count. If any fail, fix the issues and re-run the failing step.
