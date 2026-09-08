# Release Verification

The current release line is `2.0.0-alpha.20`. Release archives are published
here as distribution artifacts.

CQELS-RS release archives are published with a SHA-256 checksum beside each
archive. Verify an archive before extracting it:

```bash
shasum -a 256 -c cqels-mcp-<version>-<target>.tar.gz.sha256
```

The checksum file must be downloaded from the same GitHub release as the
archive, and the filename in the checksum entry must match the downloaded
file. Release notes identify the target triple and compatible CQELS-RS
version.

This repository contains distribution metadata and release verification
instructions.
