# Release Verification

The current release line is `2.0.0-alpha.20`. Release archives are published
from the reviewed private Rust source and mirrored here as distribution
artifacts only.

CQELS-RS release archives are published with a SHA-256 checksum beside each
archive. Verify an archive before extracting it:

```bash
shasum -a 256 -c cqels-mcp-<version>-<target>.tar.gz.sha256
```

The checksum file must be downloaded from the same GitHub release as the
archive, and the filename in the checksum entry must match the downloaded
file. Release notes identify the target triple and compatible CQELS-RS
version.

The private HiveIntel repository contains the build, Java parity, and
validation workflow; this public repository contains only the resulting
distribution metadata and release-facing verification instructions.
