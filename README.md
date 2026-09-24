# adam

**Aloi Distributed Agent Memory** — a standalone memory and session-graph extension for [Pi](https://pi.dev).

The repository is in its initial bootstrap phase. The current ClojureScript tracer bullet registers `/adam:status`; session replication and memory retrieval will follow incrementally.

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
