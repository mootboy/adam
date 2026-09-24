# Project agent guidance

- The implementation language is ClojureScript; `extension.js` is only the minimal Pi module boundary.
- Run `npm test` after changes. It performs a release build before exercising the packaged JavaScript interface.
- Commit `dist/adam.js`. Pi must be able to install the Git package without compiling ClojureScript or requiring Java at runtime.
- Keep normal tests independent of Neo4j. Live integration tests will remain opt-in.
