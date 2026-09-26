# Adam 0.3.0 plan: Claude Code support

Status: Stage 1 implemented; Stages 2–3 remain design draft

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

### Proposed Claude transcript authority contract

- The transcript identified by a hook's `transcript_path` is authoritative for that Claude session.
- Adam never writes or repairs Claude transcripts.
- Every physical JSONL record is preserved exactly.
- Unknown record types are retained.
- UUID-bearing records preserve `uuid` and `parentUuid`.
- Assistant `tool_use.id` values are structurally linked to user `tool_result.tool_use_id` values.
- Only Claude `Read`, `Edit`, and `Write` calls provide file evidence.
- File paths come only from `tool_input.file_path`.
- Bash commands and prose never provide file evidence.

Canonical session identity becomes source-scoped:

```text
urn:adam:session:<user>:pi:<session-id>
urn:adam:session:<user>:claude-code:<session-id>
```

Repository and file identities remain shared, so equivalent Pi and Claude activity converges on the same `AdamCodeFile`.

### Transcript characterization required before approval

Capture fixtures and define behavior for:

- records without `uuid`;
- resumed sessions;
- transcript rewriting versus append-only behavior;
- compaction;
- `last-prompt` and selected-leaf semantics;
- `isSidechain`;
- subagent transcript ownership;
- duplicate or repeated tool records;
- concurrent reads while Claude is appending; and
- malformed or incomplete final lines.

These rules must be established through deliberate Claude Code experiments rather than inferred from one transcript.

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

Version 0.3.0 needs a versioned migration for source-scoped session identity.

It must update, transactionally or restart-safely:

- `AdamSession.id`;
- `AdamEntry.id` and `sessionId`;
- observation and reflection IDs containing session identity;
- canonical fork references;
- derived session relationships; and
- any stored session-ID properties.

Raw JSONL and raw entry payloads remain unchanged. Remote-only sessions must survive the migration; this cannot be implemented as deleting the graph and rebuilding only from local files.

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
3. Capture transcript characterization fixtures.
4. Implement provider-neutral session identity and its migration.
5. Implement the Claude scanner and file-evidence adapter.
6. Implement the durable notification queue and worker.
7. Validate end-to-end Pi/Claude convergence, restart persistence, and outage repair.

## Stage 1 evidence

The compiled stdio server initializes and advertises the bounded, read-only `adam_file_context` contract. A live Claude Code session loaded the packaged plugin, connected to `plugin:adam:adam`, queried `README.md` by Git origin from an unrelated cwd, and returned an existing Pi-produced memory ID. Stage 2 remains blocked on the transcript characterization experiments above.
