# adam

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

`npm test` compiles ClojureScript to `dist/adam.js` and exercises the packaged JavaScript extension boundary.

For incremental development:

```bash
npm run watch
```

## Load in Pi

Build first, then load the local package:

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
```

Agents can call `adam_file_context({ path })` for explicit, provenance-bearing retrieval of memories linked to a known repository file. The command returns up to 20 memories; the agent tool returns up to 10 and applies fixed item, line, and byte bounds. Both accept repository-relative, workspace-relative, or absolute paths and do not perform semantic or global search.

Import requires interactive confirmation. Resume lists the exact-cwd union of local and remote sessions, always prefers existing local JSONL, and only materializes complete, validated remote-only sessions without overwriting files.

With valid configuration, adam lazily initializes the graph and reconciles persistent sessions at startup and persisted lifecycle boundaries. Missing configuration disables replication without preventing Pi from loading. Pi executes the committed JavaScript in `dist/`; Java is only required when rebuilding the ClojureScript source.
