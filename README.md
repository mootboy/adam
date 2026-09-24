# adam

**Aloi Distributed Agent Memory** — a standalone memory and session-graph extension for [Pi](https://pi.dev).

The ClojureScript runtime tracer bullet is complete: compiled code registers `/adam:status` through the packaged JavaScript boundary. Session replication and memory retrieval will follow incrementally under the documented contracts.

## Design documents

- [`docs/architecture.md`](docs/architecture.md) — product boundary, internal layers, consistency, adapters, and query architecture.
- [`docs/session-replica-contract.md`](docs/session-replica-contract.md) — authority, identity, graph schema, commands, restoration, privacy, and failure semantics.
- [`docs/acceptance-matrix.md`](docs/acceptance-matrix.md) — deterministic and live acceptance cases for incremental delivery.

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

Restart Pi and run:

```text
/adam:status
```

Pi executes the committed JavaScript in `dist/`; Java is only required when rebuilding the ClojureScript source.
