# Changelog

All notable changes to adam are documented here.

## [Unreleased]

### Added

- A normative producer-neutral memory protocol v1 contract with content-free source coverage, deterministic replay/conflict rules, stream-qualified provenance, and executable JSONL conformance fixtures for the staged Adam 0.4 implementation.
- Lossless producer-memory sidecar ingestion with UTF-8-byte-bounded streaming validation, restart-safe append checkpoints, byte-for-byte `AdamMemoryStream`/`AdamMemoryRecord` persistence, safe-prefix commits before physical conflicts, continued blocked raw mirroring after semantic conflicts, and non-poisoning acknowledgement of missing or unsafe sidecars.
- Canonical producer-scoped observation and reflection identities with a restart-safe graph-native migration that preserves remote-only memories, tombstones, support links, and file provenance.
- Aggregate source-session memory projection across embedded Pi records and retained sidecar producers, with canonical `main`/`agent:<agentId>` Claude stream citations, producer-local support and tombstones, unresolved-reference repair, repository-less retention, and source/producer retrieval provenance.

- A public durable producer-memory notification spool, shared Pi/Claude serialized notification drains without lease-gating Pi source writes/imports, retained-graph memory repair, bounded latest-run diagnostics and retry error messages, and explicit `/adam:reconcile` / `worker.js --once` repair paths.

## [0.3.3] - 2026-09-28

### Changed

- `/adam:context` and `adam_file_context` now return active observations plus relevant reflections. Observations tombstoned by their producer stay projected with their provenance but are excluded inside the storage query, before ordering and limiting, so they no longer consume result slots; reflections remain retrievable when their supporting observations were dropped.

### Fixed

- The file-memory result bound now applies to the combined observation and reflection result. The previous query placed `ORDER BY` and `LIMIT` after a `UNION ALL`, which Neo4j 5 attaches to the last branch only, so observations were never limited and the tool's omission probe could not detect them.

## [0.3.2] - 2026-09-28

### Fixed

- The Claude reconciliation worker no longer stalls the whole inbox behind a notification whose transcript was deleted or not yet written. Such notifications are acknowledged, removed subagent transcripts are rebuilt from their mirrored entries instead of scanned, and a failing stream no longer blocks the streams queued behind it.

### Added

- The detached worker logs failed reconciliations, retries, and missing-transcript acknowledgements to `${XDG_CONFIG_HOME:-~/.config}/adam/worker.log`.

## [0.3.1] - 2026-09-28

### Fixed

- The bundled Claude plugin MCP server now starts in interactive Claude Code sessions launched with `--plugin-dir` from any workspace. The 0.3.0 configuration used a shell-style `${CLAUDE_PLUGIN_ROOT:-.}` default that the plugin loader does not substitute.

### Added

- A package-boundary regression test that resolves `.mcp.json` exactly as the Claude plugin contract does, and an opt-in `npm run test:claude` probe that starts a real interactive Claude Code session.

## [0.3.0] - 2026-09-27

### Added

- A read-only Claude Code plugin and compiled stdio MCP server exposing the existing bounded `adam_file_context` query.
- A bundled Claude skill for selective, provenance-aware file-memory retrieval.
- Transactional, restart-safe migration from Pi-only graph identities to source-scoped session, entry, observation, and reflection identities.
- Lossless Claude parent/subagent transcript scanning with stream checkpoints, compaction and parallel-request continuity, source-scoped graph persistence, and native Read/Edit/Write file evidence.
- Non-blocking Claude lifecycle hooks, a durable locator-only notification inbox, retained parent/subagent locators, and a single-lease retrying reconciliation worker.

### Changed

- Adam's permanent user identity now lives in the host-neutral XDG config directory and atomically adopts an existing Pi identity.
- Pi and MCP file-context tools share one host-neutral execution and output-bounding adapter.

### Fixed

- Code-memory migration version checks now detect stale `WORKED_ON` projections even when a newer session marker exists.

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
