# adam architecture

Status: approved design; implementation is incremental.

`adam` provides host-neutral durable memory infrastructure with a full Pi adapter and a read-only Claude Code adapter. In Pi it runs alongside memory producers such as `pi-observational-memory`; it does not execute or import their runtime. Claude Code currently queries memories already indexed through Pi and does not yet contribute transcript data.

## Authority and boundaries

Pi session JSONL is authoritative. adam reads persisted sessions but never modifies or overwrites an existing session file. Neo4j is an eventually consistent replica and a rebuildable derived index.

```text
Pi
├── session JSONL (authoritative)
├── pi-observational-memory
│   └── appends typed memory entries
└── adam
    ├── lossless session replica
    ├── memory-source adapters
    ├── repository/file projection
    └── explicit query surfaces
```

The persisted log is the integration protocol. Separate extensions need no shared process state, package dependency, or direct event channel.

### Memory producers own

- deciding what should become memory;
- recording, consolidating, and dropping memory;
- compaction behavior;
- their persisted custom-entry schemas;
- deep recall by producer-specific memory ID.

### adam owns

- complete session, entry-tree, current-leaf, and fork-lineage replication;
- user and identity ownership plus canonical repository/file identity, worktree context, and revision provenance;
- deterministic file-tool evidence;
- adapters that interpret supported persisted memory formats;
- Neo4j persistence and reconstruction reads;
- import, resume, status, and explicit file-context retrieval.

## Internal layers

```text
host-neutral core
  ├── identity and Neo4j configuration
  ├── session replica and knowledge projection
  └── file-memory query and bounded tool adapter
       ├── Pi extension boundary
       │    ├── lifecycle, commands, and session scanner
       │    └── adam_file_context
       └── Claude Code plugin boundary
            └── stdio MCP adam_file_context
```

The session replica and derived knowledge projection are separate failure domains. A projection or adapter failure must never invalidate a successfully mirrored session.

## ClojureScript boundary

Application behavior is implemented in ClojureScript and compiled ahead of time to ESM JavaScript. `extension.js` is the minimal Pi factory shim and `mcp.js` is the executable stdio MCP boundary. The committed `dist/adam.js` and `dist/adam-mcp.js` artifacts let Pi and Claude Code run without Java or a ClojureScript compiler.

Pi, MCP, and Node objects remain JavaScript values at their boundaries. Lifecycle handlers and tools return native promises for asynchronous work. Storage and domain logic expose narrow ClojureScript protocols rather than depend directly on a host API.

## Eventual consistency

Pi extensions run handlers in registration order, but adam must not depend on ordering relative to separately installed memory producers. A scan can occur before another extension appends memory entries for the same turn.

adam therefore:

- reconciles a persisted active session at startup;
- reconciles after persisted turns and relevant session mutations;
- resumes from idempotent checkpoints;
- catches entries missed by an earlier event on the next reconciliation;
- offers explicit historical import/rebuild;
- never promises visibility after every individual JSONL append.

## Memory-source adapters

A memory adapter consumes validated stored entries and emits a provider-neutral projection. Conceptually:

```clojure
(defprotocol MemorySourceAdapter
  (extract-memories [adapter stored-entries]))
```

The first adapter supports persisted `pi-observational-memory` entries. It recognizes recorded observations, recorded reflections, and dropped-observation tombstones and emits:

- stable producer and memory IDs;
- content and memory kind;
- recording entry ID;
- source entry IDs;
- reflection-to-observation support IDs;
- dropped state;
- adapter schema/extractor version.

Unknown custom entries are ignored. Malformed or unsupported producer entries produce an isolated projection diagnostic; their raw session entries remain mirrored losslessly.

The first adapter is implemented structurally in `src/adam/sources/pi_observational_memory/` without importing the producer package. It preserves first-valid-record semantics, dropped-observation tombstones, source-entry provenance, and reflection support links. The resulting `AdamObservation` and `AdamReflection` nodes are rebuilt transactionally with the file-evidence projection; adapter diagnostics expose only producer, entry ID, and reason.

## Repository and file evidence

Only explicit Pi tool calls named `read`, `edit`, or `write` with a non-empty `path` provide file evidence. Matching tool-result entries inherit evidence through `toolCallId`.

Paths are canonicalized through the containing root reported by `git worktree list --porcelain -z`. Main-checkout and linked-worktree spellings of the same repository-relative path identify one file while evidence retains the actual worktree commit, branch, and dirty state.

For repositories with a normalizable Git origin, repository identity is the credential-free normalized origin and is independent of adam user, session, checkout, machine, and worktree. File identity is the canonical repository identity plus normalized repository-relative path. Users own sessions and memories, not these shared code-identity nodes. Queries preserve user isolation by traversing from the requested user through owned sessions and their memories before reaching a canonical file.

Originless repositories use a user-scoped local fallback derived from their resolved root. They cannot converge across machines or users and cannot be queried by origin.

When a session cwd is a non-Git workspace, the selected branch is searched in session order for the first explicit file-tool path that resolves to a repository. Shell commands, prose, and path-looking memory text never establish evidence.

The projection follows parent links from the recorded current leaf. Abandoned branches remain in the lossless replica but do not contribute current code-memory associations.

This file-evidence layer is implemented in `src/adam/knowledge/evidence.cljs`, `repository.cljs`, `index.cljs`, and `store.cljs`, with lifecycle composition in `src/adam/replica/register.cljs` and Neo4j persistence in `src/adam/replica/neo4j.cljs`. It runs only after successful lossless mirroring, is rebuildable, and reports a healthy waiting state when neither the session cwd nor selected explicit evidence resolves to Git.

## Query architecture

One shared service owns repository discovery, path validation, bounded lookup, content compaction, provenance rendering, and typed errors.

- `/adam:context <path>` is the local-path human-facing command.
- `/adam:context --origin <git-origin> <repository-relative-path>` is its checkout-independent form.
- `adam_file_context({ path, origin? })` is the smaller model-facing adapter.

Both are explicit and read-only. Local mode retains Git/worktree discovery and repository-bounded path validation. Origin mode normalizes the supplied Git origin and queries canonical repository identity directly without requiring a local checkout; it rejects empty, absolute, or traversing paths before storage access. The shared service in `src/adam/knowledge/query.cljs` owns validation, Neo4j lookup, content compaction, provenance rendering, and typed errors. The host-neutral adapter in `src/adam/knowledge/tool.cljs` probes 11 rows for the tool's 10-result bound and applies independent 200-line and 12,000-byte output caps with explicit omission notices. Pi registration in `surfaces.cljs` and the Claude stdio server in `mcp.cljs` adapt that same result. Backend failures remain invocation-local so a later call can retry. adam does not inject retrieved memory automatically and does not provide semantic or global search.

## Failure isolation

- Missing configuration disables Neo4j behavior without preventing Pi startup.
- Neo4j outages leave local Pi work unaffected and are retried later.
- Malformed JSONL is reported and never repaired automatically.
- Immutable payload conflicts stop writes for the affected session only.
- Repository discovery or file-evidence indexing failures are reported separately and never mark successful lossless replication unhealthy.
- Memory-adapter and file-projection failures do not mark the session mirror invalid.
- Command and tool failures are bounded to their invocation.

## Public surface

Commands:

- `/adam:status`
- `/adam:import`
- `/adam:resume`
- `/adam:context <path>`

Agent tool in Pi and Claude Code:

- `adam_file_context({ path, origin? })`

The Claude Code plugin launches a read-only stdio MCP server from `mcp.js`. It shares query semantics and identity with Pi but does not ingest Claude transcripts in this stage.

Environment:

- `ADAM_NEO4J_URI`
- `ADAM_NEO4J_USERNAME`
- `ADAM_NEO4J_PASSWORD`
- optional `ADAM_NEO4J_DATABASE`, default `neo4j`

Non-secret global state lives at `${XDG_CONFIG_HOME:-~/.config}/adam/config.json`. When canonical state is absent, Adam atomically adopts the existing UUID from `${PI_CODING_AGENT_DIR:-~/.pi/agent}/adam/config.json`; differing dual identities are a visible error. Credentials remain environment-only.
