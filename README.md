# adam

[![CI](https://github.com/mootboy/adam/actions/workflows/ci.yml/badge.svg)](https://github.com/mootboy/adam/actions/workflows/ci.yml)
[![Release](https://github.com/mootboy/adam/actions/workflows/release.yml/badge.svg)](https://github.com/mootboy/adam/actions/workflows/release.yml)

**A Distributed Agent Memory** — durable session replication and provenance-bearing memory retrieval for coding agents.

Adam provides a full [Pi](https://pi.dev) extension and a [Claude Code](https://docs.anthropic.com/en/docs/claude-code) plugin. Pi support includes automatic, lossless Neo4j session replication, resumable checkpoints, complete entry trees, selected-leaf preservation, canonical fork lineage, historical import, local-first restoration, deterministic repository/file evidence, structural adaptation of persisted `pi-observational-memory` entries, and bounded file-context retrieval. Claude Code automatically mirrors hook-located parent and subagent transcripts and native file evidence through a durable, non-blocking reconciliation worker, and can explicitly query existing memories through a read-only stdio MCP server.

## Design documents

- [`docs/architecture.md`](docs/architecture.md) — product boundary, internal layers, consistency, adapters, and query architecture.
- [`docs/session-replica-contract.md`](docs/session-replica-contract.md) — authority, identity, graph schema, commands, restoration, privacy, and failure semantics.
- [`docs/claude-transcript-contract.md`](docs/claude-transcript-contract.md) — observed Claude transcript authority, continuity, file evidence, and subagent semantics.
- [`docs/claude-memory-producer-contract.md`](docs/claude-memory-producer-contract.md) — characterized non-blocking lifecycle, model isolation, replay, cost, and consent requirements for the planned reference producer.
- [`docs/memory-protocol-contract.md`](docs/memory-protocol-contract.md) — normative producer-neutral sidecar protocol, event schemas, replay rules, and conformance fixtures for Adam 0.4.
- [`docs/memory-notification-contract.md`](docs/memory-notification-contract.md) — unreleased 0.4 public durable notification spool and shared reconciliation/repair behavior.
- [`docs/plan-0.3.3.md`](docs/plan-0.3.3.md) — active file-memory retrieval.
- [`docs/plan-0.4.0.md`](docs/plan-0.4.0.md) — active milestone plan for provider-neutral memory production.
- [`docs/acceptance-matrix.md`](docs/acceptance-matrix.md) — deterministic and live acceptance cases for incremental delivery.
- [`docs/issues/`](docs/issues/) — repository-local issue tracker and issue template.

## Development

Requirements:

- Node.js 22.19 or newer
- Java for ClojureScript compilation

```bash
npm install --ignore-scripts
npm test
```

`npm test` compiles the Pi extension, MCP server, Claude hook, and reconciliation worker to committed `dist/` JavaScript, runs deterministic tests, packs the npm tarball, installs it into a temporary consumer, and exercises the source and packaged JavaScript boundaries. `npm run ci` additionally verifies that the committed generated runtime matches the source build.

The opt-in live suite uses an isolated Neo4j database:

```bash
ADAM_TEST_NEO4J_URI=bolt://127.0.0.1:7687 \
ADAM_TEST_NEO4J_USERNAME=neo4j \
ADAM_TEST_NEO4J_PASSWORD='your-password' \
npm run test:neo4j
```

The opt-in Claude Code probes require a logged-in `claude` on `PATH`. They start an interactive packaged-plugin session and a separate content-free producer-characterization session whose detached worker makes a bounded model call. The latter incurs provider usage and currently validates Linux process-detachment behavior:

```bash
ADAM_TEST_CLAUDE=1 npm run test:claude
```

For incremental development:

```bash
npm run watch
```

## Install in Pi

Install a stable GitHub release tag:

```bash
pi install git:github.com/mootboy/adam@v0.3.0
```

The repository is public and releases are distributed through GitHub. For local development, build and load the checkout directly:

```bash
npm run build
pi install /absolute/path/to/adam
```

Configure Neo4j before restarting Pi:

```bash
export ADAM_NEO4J_URI=bolt://127.0.0.1:7687
export ADAM_NEO4J_USERNAME=neo4j
export ADAM_NEO4J_PASSWORD='your-password'
# export ADAM_NEO4J_DATABASE=neo4j
```

Then run:

```text
/adam:status
/adam:import
/adam:import --all
/adam:resume
/adam:context src/adam/knowledge/query.cljs
/adam:context --origin git@github.com:mootboy/adam.git src/adam/knowledge/query.cljs
```

Agents can call `adam_file_context({ path, origin? })` for explicit, provenance-bearing retrieval of memories linked to a known repository file. Local lookup accepts repository-relative, workspace-relative, or absolute paths. Supplying a Git origin enables checkout-independent lookup with a normalized repository-relative path. The command returns up to 20 memories; the agent tool returns up to 10 and applies fixed item, line, and byte bounds. Both render source-session, stream, producer, and revision provenance and return active observations plus relevant reflections: observations tombstoned by their producer stay in the graph with their provenance but are filtered out before the bound is applied, and a reflection remains retrievable even when the observations it supersedes were dropped. Neither performs semantic or global search.

Origin-backed repository and file identities are canonical across users, sessions, machines, checkouts, and worktrees. Retrieval remains isolated to memories reachable through the requesting user's sessions. Originless repositories retain isolated user-scoped fallback identities.

## Use from Claude Code

Install an Adam release with its dependencies, export the same `ADAM_NEO4J_*` variables, and load the installed directory as a plugin from any trusted workspace. The Pi checkout is such a directory:

```bash
claude --plugin-dir "$HOME/.pi/agent/git/github.com/mootboy/adam"
```

No `--mcp-config` or manual `CLAUDE_PLUGIN_ROOT` is needed; Claude substitutes `${CLAUDE_PLUGIN_ROOT}` in the bundled `.mcp.json` and registers the server as `plugin:adam:adam`.

The plugin starts the packaged stdio MCP server and exposes `adam_file_context` with the same origin lookup, user isolation, provenance rendering, and output bounds as Pi. Its bundled skill recommends explicit retrieval for a known file and does not inject memories automatically. Results include source, producer, stream-qualified entries, and revision context:

```text
[observation aaaaaaaaaaaa] Keep subagent evidence stream-qualified.
  Source: claude-code/session-123; producer: org.example.claude-memory; entries: agent:a:sa-read
  Observed: agent:a · main @ 1a2b3c4
```

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks atomically enqueue bounded locator-only notifications under `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/` and return without waiting for Neo4j. A detached, single-lease worker scans the authoritative parent and explicitly located subagent streams, resumes their checkpoints, and rebuilds native `Read`/`Edit`/`Write` file evidence. Successful work acknowledges notifications; outages and interrupted work retain them for exponential-backoff retry or a later hook wake-up, without blocking other streams, and the worker's diagnostics append to `${XDG_CONFIG_HOME:-~/.config}/adam/worker.log`. Adam itself generates no observations or reflections.

On the unreleased 0.4 development line, independent producers can enqueue canonical locator-only notifications under `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/`. The shared worker mirrors sidecars and rebuilds aggregate memory from retained source records and file evidence, even when local transcripts are gone. Pi lifecycle also drains this queue after source mirroring. Explicit repair is `/adam:reconcile` or `node /path/to/adam/worker.js --once`; one-pass CLI repair exits nonzero for busy or retryable work. Confirmed never-mirrored sources expire after a 10-minute default queued-age window (`ADAM_MEMORY_SOURCE_WAIT_MS`, 1000–86400000 ms); backend/projection failures do not expire. Expiry durably parks one locator per source session/producer before removing active work, leaving sidecars and retained data intact. After source import/mirroring, explicit repair recovers parked locators without a fresh producer notification or append. Ordinary workers/lifecycle drains never reactivate the archive. The reference Claude memory producer remains a separate, not-yet-implemented package.

Adam stores the permanent user UUID under `${XDG_CONFIG_HOME:-~/.config}/adam/config.json`. On first use it atomically adopts an existing Pi-scoped UUID from `${PI_CODING_AGENT_DIR:-~/.pi/agent}/adam/config.json`. Conflicting UUIDs fail visibly rather than silently splitting identity.

Import requires interactive confirmation. Resume lists the exact-cwd union of local and remote sessions, always prefers existing local JSONL, and only materializes complete, validated remote-only sessions without overwriting files.

With valid configuration, adam lazily initializes the graph and reconciles persistent sessions at startup and persisted lifecycle boundaries. Before normal synchronization, graph-native transactional migrations source-scope existing Pi session identities and producer-scope observation/reflection identities without requiring local JSONL; each validated version marker advances only with its complete successful transaction. A separate versioned, restart-safe code-memory rebuild reindexes local authoritative session logs when the derived schema changes and advances its marker only after successful completion. Derived memory projection aggregates embedded Pi records with every retained mirrored producer stream in one source-session transaction, preserving producer isolation and stream-qualified provenance. `/adam:status` reports migration/rebuild state and the latest lifecycle event's queue, initialization, replica synchronization, repository discovery, evidence extraction, Neo4j projection, and total durations. These diagnostics are in-memory, retain only the latest run, and contain no session content. Missing configuration disables replication without preventing Pi from loading. Pi executes the committed JavaScript in `dist/`; Java is only required when rebuilding the ClojureScript source.

## Releases

A release version and its changelog entry are part of the regular implementation PR. After committing the implementation, prepare its version without creating a local commit or tag:

```bash
npm version patch --no-git-tag-version   # or minor / major
```

Commit the resulting `package.json` and `package-lock.json` changes with the dated `CHANGELOG.md` entry on the same branch. Pull-request CI validates the source, committed runtime, package, and live Neo4j behavior as usual.

After the PR is merged, the protected-main CI run repeats those checks. If the package version does not yet have a tag, a least-privilege post-merge job creates that tag at the exact tested merge commit and invokes the release workflow. An already-tagged version is a successful no-op. The release workflow validates the tag and package versions again, smoke-tests the exact tarball, and publishes a GitHub Release containing the tarball and its SHA-256 checksum. Public npm publication remains disabled; GitHub is the distribution channel for now.

## License

adam is licensed under the [GNU General Public License, version 3](LICENSE) (`GPL-3.0-only`).
