# Verifying CQELS-RS releases

Current release: **2.0.0-alpha.20**. Each platform archive has an adjacent
SHA-256 checksum file. [RELEASE.json](RELEASE.json) independently pins the expected
archive digests used by the public installer and the separately maintained
release-validation pipeline.

## Recommended installation

```bash
python3 mcp-server/install.py
```

The installer checks that the downloaded checksum identifies the exact archive,
that it agrees with the pinned digest, and that the archive bytes hash to that
digest. Only then does it extract the root executable. It does not extract the
older README bundled in historical archives.

## Manual verification

Download the archive and its `.sha256` file from the same release. Example for
Apple Silicon macOS, run in the download directory:

```bash
shasum -a 256 -c cqels-mcp-2.0.0-alpha.20-aarch64-apple-darwin.tar.gz.sha256
```

Expected: the archive filename followed by `OK`. Also compare the digest to
`RELEASE.json`. Linux can use `sha256sum --check`; on Windows use the Python
installer or compare `Get-FileHash -Algorithm SHA256` with the pinned digest.
Do not extract an archive when a checksum disagrees.

## What the checks establish

A checksum detects differing bytes. Its trust depends on the release metadata
and the pinned copy of this repository you trust. Rust alpha.20 does not publish
a signed manifest or signature bundle. The retained `cosign.pub` file alone is
not evidence that the Rust archives were signed, and this guide makes no such
claim. CQELS4J has its own verification procedure; it does not authenticate Rust
artifacts.

`RELEASE.json` records the source revision associated with the successful Rust
binary build and pins the Java reference jar separately. Public distribution
commits and binary build revisions serve different purposes and need not match.
Release assets must not be silently replaced to repair documentation: update the
public guides or publish a new version when binary contents change.
