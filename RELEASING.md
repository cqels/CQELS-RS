# Updating the public CQELS-RS distribution

Current pinned release: **2.0.0-alpha.21**. This repository documents released
artifacts; moving a version string alone does not create an installable release.

1. Confirm that the target source build succeeded and produced all four platform
   archives and their checksum files. Record its exact source revision and
   artifact digests in `RELEASE.json`. Keep historical provenance separate from
   current-version labels.
2. Install each supported platform archive in a clean consumer environment.
   Run `examples/mcp_fleet.py` against the real executable. Every supported
   scenario must emit the expected rows; known differences must be reported
   explicitly and updated when behavior changes.
3. Download and hash the pinned CQELS4J reference. Run the same fixtures on Java
   and compare discovery schemas, HTTP, and persistence through the release
   validation pipeline. Update `mcp-server/contract.json`, tool tables, and
   compatibility claims together. Record differences instead of claiming broad
   equivalence from a matching version number.
4. Check all current-version labels, public relative links, platform targets,
   archive availability, checksums, and absence of repository-internal links.
   Test negative cases: mismatched digests, missing assets, wrong source
   provenance, and unimported direct edits to the public files must fail.
5. Reconcile manual edits on the public branch with the export template before
   publication. Publication must stop if the live public tree differs from the
   reviewed baseline; it must not overwrite a new edit without review.
6. Prepare a draft release after verifying the complete source artifact set and
   the reviewed public documentation commit. Verify mirrored asset digests and
   provenance, publish the release, and check anonymous downloads before updating
   public main with the new links. A rerun may verify an existing release; it
   must not overwrite different binaries or attach assets to stale provenance.
7. Review the CI reports and documentation PR before publication. Public main is
   fast-forwarded to the reviewed commit only after the downloads are available. Repository
   metadata (description, homepage, topics, issues/wiki/discussions settings) is
   managed separately and is not reset by file publication.
8. Record the published public commit and complete file tree as the next
   reviewed publishing baseline before preparing another export. A later manual
   edit must be imported and reviewed separately, not silently accepted.

Library instructions may be added only after anonymous Cargo dependency
resolution and the native examples work against the published version.
Signing can be documented only after the corresponding manifest and signatures
are actually published and verified. No recurring workflow is needed for a
normal documentation update; run the validation checks on each PR/publication.
