# 034 — Complete configured derivation names

- Status: done
- Created: 2026-10-16
- Follow-up: issues 032/033, PR #36

## Outcome

Both `/adam:derive` and `/adam:derivations` suggest configured recipe names at the first argument position, preserving whitespace and Pi's full-prefix replacement contract. Empty arguments offer names alongside flags; once a name or option has been entered, no further name hints appear.

Reuse the bounded no-follow YAML reader. Keep only validated name strings in a separate 30-second cache shared by both commands, lazily loaded on name completion. Static options and repository completion must not load configuration. No Neo4j or model work for name suggestions; no prompt guidance retained in the cache. Missing/invalid/unreadable configuration drops expired name hints without exposing errors or blocking static flags. Shutdown clears names with other completion state.

Actual commands continue reloading configuration and authorizing independently. Historical saved names absent from current YAML remain manually supplied; this feature does not query saved runs.

## Acceptance

- [x] Test-first registered-command regression reproduces missing names, then verifies prefix/whitespace, shared TTL, unsafe-name filtering, failure/close and no repository work.
- [x] Compiled extension reads disposable YAML and completes names for both commands without Neo4j.
- [x] Clean normal CI/runtime drift: 264 ClojureScript tests/1,705 assertions; 41 Node tests (35 pass/six expected opt-in skips). Disposable port-7688 Neo4j: 16 tests/251 assertions plus four packaged cases; existing repository completion remains green.

No version/release transition, production graph access or model calls.
