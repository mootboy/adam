# adam

[![CI](https://github.com/mootboy/adam/actions/workflows/ci.yml/badge.svg)](https://github.com/mootboy/adam/actions/workflows/ci.yml)
[![Release](https://github.com/mootboy/adam/actions/workflows/release.yml/badge.svg)](https://github.com/mootboy/adam/actions/workflows/release.yml)

**A Distributed Agent Memory** — a standalone memory and session-graph extension for [Pi](https://pi.dev).

adam currently provides automatic, lossless Neo4j session replication with resumable checkpoints, complete entry trees, selected-leaf preservation, canonical fork lineage, historical import, local-first session restoration, deterministic repository/file evidence across Git worktrees, structural adaptation of persisted `pi-observational-memory` entries, and bounded file-context retrieval for humans and agents.

## Design documents

- [`docs/architecture.md`](docs/architecture.md) — product boundary, internal layers, consistency, adapters, and query architecture.
- [`docs/session-replica-contract.md`](docs/session-replica-contract.md) — authority, identity, graph schema, commands, restoration, privacy, and failure semantics.
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

`npm test` compiles ClojureScript to `dist/adam.js`, runs deterministic tests, packs the npm tarball, installs it into a temporary consumer, and exercises both the source and packaged JavaScript extension boundaries. `npm run ci` additionally verifies that the committed `dist/adam.js` matches the source build.

The opt-in live suite uses an isolated Neo4j database:

```bash
ADAM_TEST_NEO4J_URI=bolt://127.0.0.1:7687 \
ADAM_TEST_NEO4J_USERNAME=neo4j \
ADAM_TEST_NEO4J_PASSWORD='your-password' \
npm run test:neo4j
```

For incremental development:

```bash
npm run watch
```

## Install in Pi

Install a stable GitHub release tag:

```bash
pi install git:github.com/mootboy/adam@v0.1.1
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

Agents can call `adam_file_context({ path, origin? })` for explicit, provenance-bearing retrieval of memories linked to a known repository file. Local lookup accepts repository-relative, workspace-relative, or absolute paths. Supplying a Git origin enables checkout-independent lookup with a normalized repository-relative path. The command returns up to 20 memories; the agent tool returns up to 10 and applies fixed item, line, and byte bounds. Neither performs semantic or global search.

Origin-backed repository and file identities are canonical across users, sessions, machines, checkouts, and worktrees. Retrieval remains isolated to memories reachable through the requesting user's sessions. Originless repositories retain isolated user-scoped fallback identities.

Import requires interactive confirmation. Resume lists the exact-cwd union of local and remote sessions, always prefers existing local JSONL, and only materializes complete, validated remote-only sessions without overwriting files.

With valid configuration, adam lazily initializes the graph and reconciles persistent sessions at startup and persisted lifecycle boundaries. A versioned, restart-safe code-memory rebuild reindexes local authoritative session logs when the derived schema changes and advances its marker only after successful completion. `/adam:status` reports rebuild state and the latest lifecycle event's queue, initialization, replica synchronization, repository discovery, evidence extraction, Neo4j projection, and total durations. These diagnostics are in-memory, retain only the latest run, and contain no session content. Missing configuration disables replication without preventing Pi from loading. Pi executes the committed JavaScript in `dist/`; Java is only required when rebuilding the ClojureScript source.

## Releases

Cut a release with npm, which runs the full deterministic check, commits the version bump, tags it, and pushes:

```bash
npm version patch   # or minor / major
```

A `v*` tag must exactly match the versions in `package.json` and `package-lock.json`. The release workflow reruns deterministic and live Neo4j validation, verifies committed generated output, smoke-tests the exact package tarball, and publishes a GitHub Release containing that tarball and its SHA-256 checksum. Public npm publication remains disabled; GitHub is the distribution channel for now.

## License

adam is licensed under the [GNU General Public License, version 3](LICENSE) (`GPL-3.0-only`).
