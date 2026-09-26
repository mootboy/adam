# Changelog

All notable changes to adam are documented here.

## [Unreleased]

## [0.2.1] - 2026-09-26

### Changed

- Release tagging now runs as a least-privilege post-merge CI step after protected-main validation, rather than partially pushing a version commit and tag from `npm version`.

## [0.2.0] - 2026-09-26

### Added

- Checkout-independent `/adam:context --origin <git-origin> <relative-path>` and `adam_file_context({ path, origin })` lookup.
- Versioned, restart-safe rebuilding of derived code-memory state from authoritative local session logs.

### Changed

- Origin-backed repositories and files now use canonical identities shared across users, sessions, machines, checkouts, and worktrees.
- File-memory retrieval remains user-isolated through user-owned session and memory provenance.
- Originless repositories retain isolated user-scoped fallback identities.

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
