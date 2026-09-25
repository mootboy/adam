# Changelog

All notable changes to adam are documented here.

## [0.1.1] - 2026-09-25

### Added

- GitHub Actions validation for supported Node versions and an ephemeral Neo4j instance.
- Tag-driven GitHub Releases containing the tested package tarball and its SHA-256 checksum.
- A package-consumer smoke test that installs and loads the exact npm tarball without source compilation.
- Automated npm and GitHub Actions dependency update configuration.
- GNU General Public License version 3 licensing.

### Changed

- Upgraded the Neo4j JavaScript driver to 6.2 while retaining live round-trip coverage.
- Declared the supported Node.js range and completed repository package metadata.
- Added generated-runtime drift and package-content checks to the release process.

## [0.1.0] - 2026-09-25

### Added

- Lossless, resumable Pi session replication to Neo4j.
- Local-first session import, restoration, current-leaf preservation, and fork lineage.
- Deterministic repository and file evidence across Git worktrees and non-Git workspaces.
- Structural adaptation of persisted observational-memory entries.
- Bounded `/adam:context` and `adam_file_context` retrieval with provenance.
- Lifecycle timing diagnostics and indexed file-memory projection.
