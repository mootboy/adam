# Release and cut over Adam 0.3.0

- Status: in-progress
- Created: 2026-09-27
- Owner: unassigned

## Problem

Claude transcript reconciliation is implemented on main, but Pi remains pinned to v0.2.1 and Claude cannot install a canonical tagged 0.3 artifact. Final long-running, cross-host operational validation must exercise the exact released package rather than a development checkout.

## Outcome

Publish v0.3.0 through the protected-main release pipeline, install the exact release in Pi and Claude Code, and verify durable source-scoped reconciliation and retrieval using real long-running host sessions.

## Scope

### Included

- Version and changelog metadata for v0.3.0.
- Reproducible committed extension, MCP, hook, and worker runtimes.
- Exact-tarball package validation and hosted Node/Neo4j gates.
- Post-merge tag and GitHub Release publication.
- Pi upgrade from v0.2.1 to the v0.3.0 Git source.
- Claude plugin installation from the v0.3.0 release.
- Reconciliation of the long-running Claude session `6fbc25a9-21c4-46d2-8b56-d681134038e0`.
- Cross-host file identity, restart persistence, outage repair, and explicit retrieval smoke tests.

### Excluded

- Claude transcript restoration.
- Adam-produced observations or reflections.
- Automatic prompt insertion.
- Direct conversational linkage between Claude and Pi sessions.
- Global or semantic search.

## Acceptance criteria

- [x] Package and lockfile versions are 0.3.0.
- [x] CHANGELOG contains a dated 0.3.0 release section.
- [x] Clean deterministic, package, generated-runtime, and Neo4j validation passes.
- [ ] Release PR passes protected hosted checks and is merged by the user.
- [ ] Post-merge CI creates v0.3.0 and publishes the tarball plus checksum.
- [ ] Pi resolves Adam from `git:github.com/mootboy/adam@v0.3.0`.
- [ ] Claude Code loads the released plugin, MCP server, and lifecycle hooks.
- [ ] The long-running Claude transcript reconciles losslessly and a second pass writes only its suffix or reports unchanged.
- [ ] Native file evidence converges with canonical Pi file identity while Bash/tmux prose remains excluded.
- [ ] Restart persistence and real outage-to-repair behavior are re-established against the released artifact.
- [ ] Explicit file-context retrieval succeeds from both Pi and Claude.

## Validation

Release preparation passed a clean four-target build, 100 deterministic ClojureScript tests with 380 assertions, nine normal Node boundary/package/release tests, committed-runtime drift validation, Claude plugin validation, package dry-run, and the ephemeral Neo4j 5.26 suite with four ClojureScript tests/56 assertions plus the compiled outage-to-repair worker test. Post-release installation and operational evidence will be recorded in a follow-up change before this issue is marked done.

## Notes

The released 0.3.0 plugin did not start its MCP server in interactive Claude Code sessions; [`007`](007-load-plugin-mcp-server-from-released-checkout.md) fixes that in 0.3.1, so the Claude cutover criteria above are validated against 0.3.1.

The release is intentionally required before final cutover validation so both hosts exercise one immutable, checksummed artifact. The existing post-merge pipeline tags only the exact protected-main commit after Node and Neo4j gates pass.
