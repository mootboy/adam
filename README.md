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
- [`docs/memory-notification-contract.md`](docs/memory-notification-contract.md) — optional experimental protocol-v1 spool and shared reconciliation/repair behavior.
- [`docs/plan-0.3.3.md`](docs/plan-0.3.3.md) — active file-memory retrieval.
- [`docs/plan-0.4.0.md`](docs/plan-0.4.0.md) — 0.4.0 infrastructure release scope and separate standalone-producer/native-reader follow-up.
- [`docs/plan-0.5.0.md`](docs/plan-0.5.0.md) — planned configurable memory derivations, explicit Pi generation, multi-user grants and provenance-bearing result history.
- [`docs/derivation-contract.md`](docs/derivation-contract.md) — implemented derivation contracts/core, graph authorization/history, explicit Pi generation and read-only Pi/Claude saved-result browsing.
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

The opt-in live suite requires a **fresh disposable Neo4j instance**, never a shared/development/production database. Explicit acknowledgement is required before any live-test connection; it is a safety gate, not proof that the supplied endpoint is disposable. Test subprocesses use isolated canonical/Pi identity roots and teardown uses registered fixture sessions. Use a separate container/port (for example 7688, not the production 7687):

```bash
ADAM_TEST_NEO4J_DISPOSABLE=1 \
ADAM_TEST_NEO4J_URI=bolt://127.0.0.1:7688 \
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

Normal/live gates compile then execute the ClojureScript Node runner explicitly; compiler autorun alone does not propagate test failures. Watch retains development autorun through the **build-local** `{:autorun true}` override (ignored by the ESM build), not a nested `:builds` map. The test-only runner rejects zero tests/incomplete completion and has a whole-run watchdog: 180 seconds normally, 600 seconds live, configurable with `ADAM_TEST_RUN_TIMEOUT_MS` (10–3,600,000 ms). Intentional failure probes are isolated from the normal/live suites. To invoke Node boundary tests directly, first run `npm run test:cljs` to compile their runner fixtures.

## Install in Pi

Install a stable GitHub release tag:

```bash
pi install git:github.com/mootboy/adam@v0.4.1
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

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks atomically enqueue bounded locator-only notifications under `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/` and return without waiting for Neo4j. A detached, single-lease worker scans the authoritative parent and explicitly located subagent streams, resumes their checkpoints, and rebuilds native `Read`/`Edit`/`Write` file evidence. Successful work acknowledges notifications; outages and interrupted work retain them for exponential-backoff retry or a later hook wake-up, without blocking other streams, and the worker's diagnostics append to `${XDG_CONFIG_HOME:-~/.config}/adam/worker.log`. Adam itself generates no observations or reflections. The illustrative Claude-produced memory above requires separately supplied memory input; this plugin bundles neither a memory generator nor automatic discovery of a standalone producer's native logs.

The revised [0.4 plan](docs/plan-0.4.0.md) keeps memory producers fully standalone: they own native persistence and recall, with no Adam format, inbox, export or worker knowledge. Adam will own native log discovery/readers/adapters and scheduling, as it does for Pi observational-memory entries. That concrete native reader and the reference producer are not yet implemented and are separate follow-up work, not 0.4.0 release dependencies. Their planned visibility is at the next Adam reconciliation, not immediately after every background append; no polling or daemon is promised.

0.4.0 releases memory infrastructure: producer-scoped identities, aggregate projection, lossless retained protocol streams and explicit repair. As an **optional experimental input**, existing protocol-v1 infrastructure accepts canonical locator-only notifications under `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/`. The shared worker mirrors sidecars and rebuilds aggregate memory from retained source records and file evidence, even when local transcripts are gone. Pi lifecycle also drains this queue after source mirroring. Explicit repair is `/adam:reconcile` or `node /path/to/adam/worker.js --once`; one-pass CLI repair exits nonzero for busy or retryable work. Confirmed never-mirrored sources expire after a 10-minute default queued-age window (`ADAM_MEMORY_SOURCE_WAIT_MS`, 1000–86400000 ms); backend/projection failures do not expire. Expiry durably parks one locator per source session/producer before removing active work, leaving sidecars and retained data intact. After source import/mirroring, explicit repair recovers parked locators without a fresh producer notification or append. Ordinary workers/lifecycle drains never reactivate the archive. The reference Claude memory producer remains a separate, not-yet-implemented package.

Adam stores the permanent user UUID under `${XDG_CONFIG_HOME:-~/.config}/adam/config.json`. On first use it atomically adopts an existing Pi-scoped UUID from `${PI_CODING_AGENT_DIR:-~/.pi/agent}/adam/config.json`. Conflicting UUIDs fail visibly rather than silently splitting identity.

Import requires interactive confirmation. Resume lists the exact-cwd union of local and remote sessions, always prefers existing local JSONL, and only materializes complete, validated remote-only sessions without overwriting files.

With valid configuration, adam lazily initializes the graph and reconciles persistent sessions at startup and persisted lifecycle boundaries. Before normal synchronization, graph-native transactional migrations source-scope existing Pi session identities and producer-scope observation/reflection identities without requiring local JSONL; each validated version marker advances only with its complete successful transaction. A separate versioned, restart-safe code-memory rebuild reindexes local authoritative session logs when the derived schema changes and advances its marker only after successful completion. Derived memory projection aggregates embedded Pi records with every retained mirrored producer stream in one source-session transaction, preserving producer isolation and stream-qualified provenance. `/adam:status` reports migration/rebuild state and the latest lifecycle event's queue, initialization, replica synchronization, repository discovery, evidence extraction, Neo4j projection, and total durations. These diagnostics are in-memory, retain only the latest run, and contain no session content. Missing configuration disables replication without preventing Pi from loading. Pi executes the committed JavaScript in `dist/`; Java is only required when rebuilding the ClojureScript source.

## Unreleased 0.5 Pi generation tracer

Create user-wide `$XDG_CONFIG_HOME/adam/derivations.yaml` (default `~/.config/adam/derivations.yaml`):

```yaml
derivations:
  preferences:
    version: 1
    guidance: Identify attributed architectural and style preferences; preserve scope, uncertainty and disagreements.
```

`/adam:derive preferences` refreshes one recipe. `/adam:derive` processes configured recipes sequentially, continuing after failures. Either form accepts at most one `--repository <git-origin>`, `--session <source-scoped-session-urn>` or `--language <language>` filter. Configuration and explicit refresh authorize the model call; no lifecycle event generates automatically. These commands are not in the published 0.4.1 artifact.

The first use checks graph schema. If constraints or the online native-entry index are missing, **interactive approval is required** before creating/checking three derivation uniqueness constraints and the native-entry index. This can build an index over all mirrored entries; source content is unchanged. Back up production and explicitly approve its first use—stage-3 tests do not authorize it. Existing ready schema needs no DDL. Initialization is cached only after success; `/adam:status` shows its outcome.

The separate request uses Pi registry/authentication, defaults to the session model, declares no tools, disables SDK retries and prompt caching, and excludes the current conversation. Optional YAML `model: {provider: ..., id: ...}` uses Pi's registry. Current conservative sizing supports stock Anthropic Messages and OpenAI/Azure Completions/Responses APIs. Complete system/data UTF-8 byte count is an intentionally loose input-token bound (often 3–4× English token counts), plus 1024 framing tokens and the output reservation. Oversized inputs fail before model work without sampling; narrow scope or select a larger supported model. This is not real-provider capacity certification.

Output defaults to 8192 tokens and model-call timeout to five minutes; YAML `limits` may change them. Reasoning-capable models request minimal effort; budgeted Anthropic models require at least 2048 output tokens and reserve their 1024 thinking budget inside that allowance. Responses must be raw JSON; fences, tool calls, truncation or failures preserve prior results. Reasoning may still consume the allowance and cause a billed-but-rejected truncated response.

Pi awaits the command handler, so the invoking window may be occupied for input loading and generation. Cancellation is **shutdown-only**, plus the configured model-call timeout; Esc/turn-abort signals do not control this separate command. Abort requests do not promise instant billing cessation. Graph cleanup/shutdown may wait for driver timeouts; failed release falls back to lease expiry. Same-bucket refreshes report busy; different buckets are independent. Renewal failure cancels conservatively rather than retrying model work. `reused` describes the processed snapshot, not current freshness.

Read saved results without generating or repairing anything:

```text
/adam:derivations
/adam:derivations preferences --history --limit 10
/adam:derivations preferences --repository git@github.com:example/project.git
adam_derivations({"name":"preferences","history":true,"limit":10})
```

`adam_derivations` is read-only in both Pi and Claude MCP when Neo4j is configured. Optional name, one repository/session/language selector, history and limit (1–20, default 10) share the command contract. Omitting a selector lists all matching filter buckets. Results show scoped conclusions, prominent conflict/correction links, model, current configuration/evidence freshness and authorized source/producer/support locators. Free-form fields are JSON-quoted, not instructions. Output is capped at 200 lines/12,000 UTF-8 bytes with explicit omissions. Missing/invalid configuration are distinct; unavailable/revoked evidence withholds conclusions, and successful empty outcomes remain inspectable. Historical freshness is compared against current evidence/configuration, not against its own saved inputs.

**Read cost:** truthful freshness currently rechecks the complete authorized memory corpus and streams retained native source activity, even for a small result page or filter. This can read hundreds of MB in a long transaction; it is not a cheap lookup or fixed-heap guarantee. Empty or entirely denied pages skip source loading. Avoid casual/repeated agent calls. One saved read per process is permitted; the command and tool share this gate, so `busy` means another read is active, not an authorization error. Pi/MCP tools propagate request cancellation; Pi shutdown and MCP stdin shutdown cancel and await read cleanup. The Pi command has shutdown cancellation rather than an unrelated agent-turn signal. Cleanup may wait for driver timeouts. Server `db.transaction.timeout` may stop a long read (`read-timeout`); selector filtering does not currently reduce full-corpus verification. Reads use a separate lazy pool so they cannot trigger replica migration/initialization or DDL. Reauthorization after streaming respects Neo4j read-committed isolation; this is not an atomic cross-query snapshot.

Validation uses fake providers and disposable Neo4j only; real-provider quality/cost probes require separate approval.

## Releases

A release version and its changelog entry are part of the regular implementation PR. After committing the implementation, prepare its version without creating a local commit or tag:

```bash
npm version patch --no-git-tag-version   # or minor / major
```

Commit the resulting `package.json` and `package-lock.json` changes with the dated `CHANGELOG.md` entry on the same branch. Pull-request CI validates the source, committed runtime, package, and live Neo4j behavior as usual.

After the PR is merged, the protected-main CI run repeats those checks. If the package version does not yet have a tag, a least-privilege post-merge job creates that tag at the exact tested merge commit and invokes the release workflow. An already-tagged version is a successful no-op. The release workflow validates the tag and package versions again, smoke-tests the exact tarball, and publishes a GitHub Release containing the tarball and its SHA-256 checksum. Public npm publication remains disabled; GitHub is the distribution channel for now.

## License

adam is licensed under the [GNU General Public License, version 3](LICENSE) (`GPL-3.0-only`).
