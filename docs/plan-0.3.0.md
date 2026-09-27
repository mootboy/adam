# Adam 0.3.0 plan: Claude Code support

Status: Stages 1–3 implemented

Adam 0.3.0 introduces Claude Code as a second host while keeping Adam outside observation production and preserving explicit, auditable memory retrieval. This plan is intentionally contract-first; transcript behavior must be characterized before ingestion semantics are finalized.

## Target architecture

```text
                         ┌────────────────────────────┐
                         │ Host-neutral Adam core     │
                         │                            │
                         │ • permanent user identity  │
                         │ • Neo4j storage            │
                         │ • canonical code identity  │
                         │ • file-memory query        │
                         │ • bounded rendering        │
                         └──────────────┬─────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
             ┌──────▼──────┐                         ┌──────▼──────────┐
             │ Pi adapter  │                         │ Claude adapter  │
             │             │                         │                 │
             │ Extension   │                         │ MCP server      │
             │ Pi JSONL    │                         │ Claude JSONL    │
             │ Lifecycle   │                         │ Plugin hooks    │
             └─────────────┘                         └─────────────────┘
```

## Stage 1: read-only Claude support

Implemented on the 0.3 development branch: Claude Code can query existing Adam memories without introducing transcript ingestion.

```text
Claude Code
    │
    │ stdio MCP
    ▼
adam MCP server
    │
    ├── adam_file_context
    └── adam_status (optional)
             │
             ▼
       existing Neo4j graph
```

The MCP server reuses the existing repository resolution, origin lookup, user isolation, result limits, and provenance rendering.

Package layout:

```text
.claude-plugin/plugin.json
.mcp.json
skills/adam-memory/SKILL.md
mcp.js
dist/adam-mcp.js
src/adam/mcp.cljs
```

The skill recommends explicit file-context retrieval and discourages indiscriminate querying or automatic context insertion.

### Host-neutral identity

Before exposing queries to Claude, establish canonical permanent user identity outside either host. The legacy location is:

```text
~/.pi/agent/adam/config.json
```

The canonical location is `${XDG_CONFIG_HOME:-~/.config}/adam/config.json`. Adoption rules:

1. Use the host-neutral identity if present.
2. Otherwise atomically adopt the existing Pi UUID.
3. If both identities exist with the same UUID, continue.
4. If they contain different UUIDs, fail visibly rather than silently splitting identity.
5. Pi and Claude thereafter resolve the same Adam user.

## Stage 2: Claude transcript ingestion

Introduce a host-neutral session-source protocol:

```clojure
(defprotocol SessionSource
  (scan-session [source locator options]))
```

Conceptually it emits:

```clojure
{:source-kind       :claude-code
 :source-session-id "..."
 :source-locator    "..."
 :cwd               "..."
 :entries           [...]
 :current-leaf-id   "..."
 :scan-metadata     {...}}
```

Pi's existing scanner remains behind a Pi implementation. Claude receives a separate scanner because its transcript format is materially different.

### Approved Claude transcript authority contract

The observed and approved authority, continuity, file-evidence, compaction, and subagent rules are defined in [`claude-transcript-contract.md`](claude-transcript-contract.md). In summary:

- The transcript identified by a hook's `transcript_path` is authoritative for that Claude stream.
- Adam never writes or repairs Claude transcripts and does not discover them by scanning undocumented storage directories.
- Every complete physical JSONL record is preserved exactly, including unknown and UUID-less records.
- UUID-bearing records preserve `uuid` and `parentUuid`; compact-boundary continuity additionally follows `logicalParentUuid`.
- Parallel assistant fragments are grouped by active `requestId`, with results resolved by tool-use ID and source assistant UUID.
- Only Claude `Read`, `Edit`, and `Write` calls provide file evidence from `input.file_path`.
- Bash, MCP calls, summaries, handbacks, tool output, and prose never provide file evidence.
- Explicitly located subagent streams remain child streams of the owning Claude session.

Canonical session identity becomes source-scoped:

```text
urn:adam:session:<user>:pi:<session-id>
urn:adam:session:<user>:claude-code:<session-id>
```

Repository and file identities remain shared, so equivalent Pi and Claude activity converges on the same `AdamCodeFile`.

### Transcript characterization evidence

Deliberate Claude Code 2.1.283 experiments established native Read/Edit/Write structure, byte-prefix-preserving resume, byte-prefix-preserving manual compaction, `last-prompt` leaf markers, compact-boundary logical continuity, parallel request fragments, and independently rooted subagent sidechain streams under the same session identity. Raw transcripts remain local and were not committed.

Permanent sanitized fixtures, malformed-tail cases, duplicate-tool cases, and concurrent-write defenses are deferred to scanner implementation, when the exact parser invariants are known.

## Stage 3: non-blocking reconciliation

Do not rely on the MCP process itself as a permanent daemon. Claude owns its lifecycle and may stop it whenever no MCP call is active.

Use a durable notification inbox and a serialized worker instead:

```text
Claude hook
    │
    │ tiny atomic notification
    ▼
~/.config/adam/inbox/
    │
    ▼
single serialized Adam worker
    │
    ├── scan transcript
    ├── mirror suffix
    ├── project file evidence
    └── retry after outage
```

Hooks:

- `SessionStart`: enqueue startup or resume reconciliation.
- `Stop`: enqueue completed-turn reconciliation.
- `SessionEnd`: enqueue final reconciliation.
- `PostToolUse`: optional wake-up signal only.

A hook must:

1. Parse bounded JSON from standard input.
2. Validate `session_id`, `transcript_path`, and `cwd`.
3. Atomically enqueue a small notification.
4. Optionally wake a single worker.
5. Return immediately without waiting for Neo4j.

Notifications contain locators and structural metadata, not transcript content. The worker owns serialization, retries, and deduplication. A Neo4j outage therefore cannot hold Claude in a working state.

## Memory-production boundary

Claude ingestion initially creates:

- sessions;
- entries;
- tree relationships;
- file evidence; and
- revision provenance.

It does **not** create observations or reflections.

Consequently:

- Claude can immediately retrieve memories previously produced in Pi.
- Claude file activity can converge on the same canonical files.
- New Claude conversations do not become file-linked memories until a separate compatible memory producer exists.

Adam remains a memory transport and index, not an observation generator.

## Graph migration

Version 0.3.0 implements a versioned, transactionally atomic migration for source-scoped session identity.

It updates:

- `AdamSession.id`;
- `AdamEntry.id` and `sessionId`;
- observation and reflection IDs containing session identity;
- canonical fork references;
- derived session relationships; and
- any stored session-ID properties.

Raw JSONL and raw entry payloads remain unchanged. Remote-only sessions survive because migration updates the existing graph in place rather than deleting it and rebuilding only from local files. The user marker advances in the same transaction after all dependent identities; effective-version checks also detect stale dependent nodes hidden by a newer marker.

## Deferred

The following remain outside 0.3.0:

- restoring Claude transcripts from Neo4j;
- changing Claude's resume behavior;
- automatic memory insertion into prompts;
- global or semantic search;
- shell-command path inference;
- Adam-generated observations; and
- treating undocumented Claude storage paths as stable discovery APIs.

## Recommended implementation sequence

1. Define the host-neutral identity and read-only MCP contract.
2. Define the Claude plugin and package contract.
3. Characterize Claude transcripts and approve their authority contract.
4. Implement provider-neutral session identity and its migration.
5. Implement the Claude scanner and file-evidence adapter.
6. Implement the durable notification queue and worker.
7. Validate end-to-end Pi/Claude convergence, restart persistence, and outage repair.

## Stage 1 evidence

The compiled stdio server initializes and advertises the bounded, read-only `adam_file_context` contract. A live Claude Code session loaded the packaged plugin, connected to `plugin:adam:adam`, queried `README.md` by Git origin from an unrelated cwd, and returned an existing Pi-produced memory ID.

## Stage 2 characterization evidence

Controlled sessions proved native file-tool extraction, append-only resume and compaction in the tested lifecycle, compact-boundary continuity, active parallel tool grouping, and subagent sidechain ownership. The restart-safe source-scoped identity migration, lossless stream scanner, stream-checkpointed Neo4j storage, context reconstruction, and native file-evidence adapter are implemented.

## Stage 3 reconciliation evidence

Claude Code 2.1.283 characterization confirmed that `SubagentStop` supplies the parent `transcript_path`, explicit `agent_transcript_path`, and `agent_id`. Packaged hooks now atomically enqueue bounded locator-only notifications and return independently of Neo4j. A single-lease worker retains parent/subagent locators, coalesces repeated notifications, retries failures with backoff, synchronizes every known stream, and projects native file evidence. Automated live validation covers an enqueue during simulated configuration outage followed by successful worker repair into source-scoped Neo4j session, stream, entry, and `TOUCHES` state.
