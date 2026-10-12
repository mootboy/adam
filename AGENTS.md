# Project agent guidance

- `docs/architecture.md`, `docs/session-replica-contract.md`, and `docs/acceptance-matrix.md` define the approved boundaries and behavior.
- The implementation language is ClojureScript; `extension.js` is only the minimal Pi module boundary.
- Run `npm test` after changes. It performs a release build before exercising the packaged JavaScript interface.
- Commit `dist/adam.js`. Pi must be able to install the Git package without compiling ClojureScript or requiring Java at runtime.
- Keep normal tests independent of Neo4j. Live integration tests will remain opt-in.
- adam is a fresh start. Do not add compatibility checks or negative tests for legacy graph labels, identifiers, commands, or public names unless explicitly requested.

## Development principles
1. Reads up front, writes at the end. Keep the processing code pure, only read state at the beginning of a process and write it at the end.
2. Maintain locality of behaviour, prefer keeping related functions close to the code using it. Avoid creating new files/modules unless they are needed for shared functionality.
3. Focus testing on interfaces first, use parametrised tests. Use property based testing when applicable. Only introduce unit tests for functions that are hard to test through the interfaces.
4. Prefer clojure specs to types, hand-written validation or schema libraries.
