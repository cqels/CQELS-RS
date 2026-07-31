# Repository Publishing Model

CQELS-RS uses two GitHub repositories with different responsibilities:

- `HiveIntel/cqels-rs` is the canonical development repository.
- `cqels/CQELS-RS` is the public publishing proxy for approved source snapshots and releases.

## Canonical development repository

Development work happens in `HiveIntel/cqels-rs`. Its issue tracker, pull requests, review history, agent configuration, CI orchestration, design notes, and internal benchmark material are the source of truth for the project.

The normal contribution path is therefore:

1. Open or update an issue in `HiveIntel/cqels-rs`.
2. Develop and review the change there.
3. Merge the approved change to the canonical release branch.
4. Publish the resulting source snapshot to `cqels/CQELS-RS`.

## Public publishing proxy

The public repository exists for users who need to inspect the released Rust source, documentation, tags, and GitHub releases. It is generated from the canonical repository by the `Publish Public Proxy` workflow.

The export is intentionally filtered. It does not mirror GitHub metadata, and it does not use `git push --mirror`, so private development history and repository administration remain attached to `HiveIntel/cqels-rs`.

The public repository should not be treated as the source of truth for issues, pull requests, or review decisions. Public users should use the published release and crate links from the public repository; maintainers should use `HiveIntel/cqels-rs` for development coordination.

## Private paths excluded from the export

The publishing workflow excludes these development-only paths:

- `.claude/`
- `CLAUDE.md`
- `.github/`
- `scripts/`
- `docs/JAVA_ALPHA10_COMPARATIVE_ANALYSIS.md`
- `docs/JAVA_PARITY_PLAN.md`
- `docs/TEST_REPORT.md`

User-facing documentation and selected benchmark reports remain publishable unless they are added to this exclusion list deliberately.

## Publishing credentials

The source repository must have a repository or environment secret named `CQELS_PUBLIC_PUBLISH_TOKEN`. It needs permission to push to `cqels/CQELS-RS` and create releases there. The workflow uses the `public-publish` environment so maintainers can add an approval gate before a snapshot is published.

The public proxy receives commits on `main`. Version tags are copied only for source tags matching `v*`, and a corresponding GitHub release is created when one does not already exist.
