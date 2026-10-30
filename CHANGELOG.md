# Changelog

All notable changes to adam are documented here.

## [Unreleased]

### Fixed

- Keep Pi session startup and turn completion responsive while reconciliation runs: coalesce unread requests, asynchronously send raw record batches to a data-only processing thread, and commit writes/checkpoints outside it. A single validated snapshot serves replication and projection; explicit import/repair remains awaitable.
- Cancel queued processing during shutdown with bounded cleanup, preserve malformed-session diagnostics across the thread boundary, reject stale source/producer projection plans before replacement, and keep failed rebuilds retryable without advancing their version marker. Automatic notification work wakes the existing detached consumer rather than running foreign scanners on Pi's event loop.

## [0.5.2] - 2026-10-16

### Fixed

- Retain multiple repositories and per-worktree evidence in continuing Pi/Claude sessions. Repository filters follow observation files and reflection supports; only single-context sessions supply scope to unlinked discussion. Reference reads count as contexts and may appear in grant-authorized origin hints. No bulk repair or migration of existing data; correction uses normal reconciliation. Saved results for unreconciled sessions can still report current against unchanged, misattributed data. Dedicated repair and `file://` local selectors are not included.

## [0.5.1] - 2026-10-16

### Added

- Native Pi argument completion for `/adam:derive` and `/adam:derivations`: configured recipe names, flags, evidence-depth and language values, plus saved-read limits. Preserves preceding arguments and omits used/incompatible flags.
- Repository-value completion includes the current checkout and directly authorized memory-session origins through a shared 30-second metadata cache. Static options stay IO-free; repository lookup reads no source content, invokes no model and performs no schema initialization. Actual commands reauthorize independently of cached hints.

## [0.5.0] - 2026-10-15

### Added

- Optional, explicitly refreshed memory derivations guided by user-wide YAML recipes. `/adam:derive` uses the executing Pi model or a configured Pi registry override, with isolated tool-free requests, scoped evidence, conflicts and preserved successful history. No lifecycle event generates automatically.
- Principal-bound derivation storage, explicit directional/non-transitive memory grants, complete run-wide authorization, revocation-sensitive saved reads, atomic token-fenced publication and renewable refresh leases. Missing evidence fails closed; empty successful results are cached.
- `/adam:derivations` and read-only `adam_derivations` in Pi and Claude MCP, with existing stable conclusion IDs, scope, freshness, conflict/correction links and separately requested supporting/conflicting memory evidence. Exact evidence lookup remains available for older saved conclusions under current authorization.
- Lossless continuation pages for oversized selected read output instead of clipped conclusions; ordinary text output stays within the existing per-page budget. No separate viewer or custom rendering.
- Command-selected full/active/observations/reflections evidence depth with distinct saved identities and retained authorization dependencies for omitted supporting text.
- Compact model requests with request-local reference aliases and complete selected content; labelled local OpenAI-family token estimates with conservative margins and UTF-8-byte fallback. No remote counting calls or silent input sampling.
- Immediate derivation acknowledgement and namespaced footer progress, cleared after success, failure or cancellation.

### Fixed

- Preserve stable memory nodes and incoming derivation dependency relationships across whole-session reprojection and restart; genuinely removed evidence still denies saved reads.
- Recognize both Cypher node-uniqueness vocabularies while requiring the exact constraints and ONLINE native-entry index. Explicit schema initialization remains approval-gated.
- CI runs compiled ClojureScript test runners explicitly and fails on assertion/error, incomplete/hanging completion or empty test selections rather than trusting Shadow compiler autorun status. Serial release compilation prevents generated-runtime drift.

### Limits

- Derivations synthesize authorized memory; they are not authoritative statements of universal preferences. Attribution, scope, uncertainty and disagreement remain important.
- Provider output allowances and cancellation are best-effort, not strict spending ceilings. Local tokenizer estimates, especially newer Codex aliases, are not server-certified. Invalid/truncated generation preserves prior successful results but may still incur usage.
- Truthful saved freshness and evidence inspection can scan the full authorized corpus. Every continuation reauthorizes and repeats that read cost; changed output requires restarting continuation. Whole-history browsing beyond the requested bounded run selection is not added.
- The installed Pi peer tooling retains one high-severity indirect `brace-expansion` audit finding in its published shrinkwrap. Local patch-only/audit-fix attempts do not update it; [issue 031](docs/issues/031-pi-peer-shrinkwrap-brace-expansion-audit.md) tracks upstream remediation. Adam does not silently override the host's dependency tree.
- Distribution remains GitHub-release/Git installation, not npm publication. The independent Claude memory producer and native producer-log reader remain separate follow-up work.

## [0.4.1] - 2026-10-09

### Fixed

- Pi lifecycle callbacks await serialized synchronization without exposing internal ClojureScript maps as event results, preventing `Invalid boundary entries: drafts is not iterable` at `turn_end` and preserving drafts from preceding extensions.

## [0.4.0] - 2026-10-09

### Scope

- Memory infrastructure release: producer-scoped identities, aggregate projection, retained memory streams and reconciliation. Protocol-v1 sidecars/spool are optional experimental input, not an integration obligation for producers.
- Standalone Claude memory generation and Adam's concrete native-format discovery/reader are separate, unimplemented follow-up work. Producers must generate, persist and recall independently; this release makes no automatic native-ingestion claim.

### Added

- A normative producer-neutral memory protocol v1 contract with content-free source coverage, deterministic replay/conflict rules, stream-qualified provenance, and executable JSONL conformance fixtures for the staged Adam 0.4 implementation.
- Lossless producer-memory sidecar ingestion with UTF-8-byte-bounded streaming validation, restart-safe append checkpoints, byte-for-byte `AdamMemoryStream`/`AdamMemoryRecord` persistence, safe-prefix commits before physical conflicts, continued blocked raw mirroring after semantic conflicts, and non-poisoning acknowledgement of missing or unsafe sidecars.
- Canonical producer-scoped observation and reflection identities with a restart-safe graph-native migration that preserves remote-only memories, tombstones, support links, and file provenance.
- Aggregate source-session memory projection across embedded Pi records and retained sidecar producers, with canonical `main`/`agent:<agentId>` Claude stream citations, producer-local support and tombstones, unresolved-reference repair, repository-less retention, and source/producer retrieval provenance.

- A public durable producer-memory notification spool, shared Pi/Claude serialized notification drains without lease-gating Pi source writes/imports, retained-graph memory repair, bounded latest-run diagnostics and retry error messages, and explicit `/adam:reconcile` / `worker.js --once` repair paths.

- Restart-stable local-mtime expiry for positively confirmed never-mirrored memory sources, with a configurable 10-minute default wait, content-free expiry diagnostics, and no expiry of backend/projection failures or mutation of retained data.
- Durable coalesced parking of expired memory locators and ownership-checked recovery through `/adam:reconcile` or `worker.js --once`, without fresh producer activity or ordinary-drain archive discovery.

### Fixed

- Live-test identity isolation and teardown scope: isolate canonical and legacy Pi roots, preseed fixture identity, delete only registered session-owned records, and use unique canonical-repository fixtures. Live suites require explicit disposable-database acknowledgement to prevent accidental shared/production execution.

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
