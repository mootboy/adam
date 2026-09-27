# Release and cut over Adam 0.3.0

- Status: done
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
- [x] Release PR passes protected hosted checks and is merged by the user.
- [x] Post-merge CI creates v0.3.0 and publishes the tarball plus checksum.
- [x] Pi resolves Adam from `git:github.com/mootboy/adam@v0.3.0` (upgraded to `@v0.3.1` for the Claude cutover).
- [x] Claude Code loads the released plugin, MCP server, and lifecycle hooks.
- [x] The long-running Claude transcript reconciles losslessly and a second pass writes only its suffix or reports unchanged.
- [x] Native file evidence converges with canonical Pi file identity while Bash/tmux prose remains excluded.
- [x] Restart persistence and real outage-to-repair behavior are re-established against the released artifact.
- [x] Explicit file-context retrieval succeeds from both Pi and Claude.

## Validation

Release preparation passed a clean four-target build, 100 deterministic ClojureScript tests with 380 assertions, nine normal Node boundary/package/release tests, committed-runtime drift validation, Claude plugin validation, package dry-run, and the ephemeral Neo4j 5.26 suite with four ClojureScript tests/56 assertions plus the compiled outage-to-repair worker test. Post-release installation and operational evidence, recorded on 2026-09-28 against the released artifacts:

- PR #11 merged; protected-main CI tagged `v0.3.0` and published `adam v0.3.0` with its tarball and checksum. The Claude plugin defect found during cutover ([`007`](007-load-plugin-mcp-server-from-released-checkout.md)) shipped as `v0.3.1` the same way, so the Claude-side criteria below were validated against `v0.3.1`.
- Pi: `pi install git:github.com/mootboy/adam@v0.3.1` resolves to the checkout at `~/.pi/agent/git/github.com/mootboy/adam` (tag `v0.3.1`, 100 installed dependencies); Pi lists `mootboy/adam:extension.js` among its extensions.
- Claude Code 2.1.283 started interactively from `/home/linus/git/aloi` with `--plugin-dir` on that checkout loads the inline plugin, one skill, the four lifecycle hooks, and connects `plugin:adam:adam` (adam 0.3.1) over stdio in 236 ms.
- Long-running transcript `6fbc25a9…` (5,255,101 bytes): the reconciliation inbox was found stalled with 109 notifications behind a deleted transcript ([`008`](008-worker-stalls-on-missing-transcripts.md)). With that fix the backlog drained in 4 s; the stream checkpoint reached the exact file size with 2,828 mirrored entries. A second pass triggered through the installed hook acknowledged its notification with no graph change and no log output.
- Cross-host convergence: a headless Claude session in the adam checkout used the native `Read` tool on `.mcp.json`; after reconciliation the canonical `AdamCodeFile` for `.mcp.json` carried two Claude entry `TOUCHES` edges alongside six Pi observations `ABOUT` the same node. Bash-driven edits in the same sessions produced no evidence.
- Restart persistence: the 14 mirrored Claude streams resumed from their byte checkpoints across worker restarts, including the forced replacement of two stale lease holders. Outage-to-repair was re-run through the compiled live suite against an ephemeral Neo4j 5.26 rather than by stopping the shared instance.
- Retrieval: `/adam:context .mcp.json` in Pi returned 11 provenance-bearing results at `main @ ff9bc68`; headless Claude from `/home/linus/git/aloi` called `plugin:adam:adam` `adam_file_context` with origin `git@github.com:mootboy/adam.git` and path `.mcp.json` and received 10 memories, first id `70f6600ddded`, in 122 ms.

## Notes

The released 0.3.0 plugin did not start its MCP server in interactive Claude Code sessions; [`007`](007-load-plugin-mcp-server-from-released-checkout.md) fixes that in 0.3.1, so the Claude cutover criteria above are validated against 0.3.1.

The release is intentionally required before final cutover validation so both hosts exercise one immutable, checksummed artifact. The existing post-merge pipeline tags only the exact protected-main commit after Node and Neo4j gates pass.
