# adam architecture

Status: approved design; implementation is incremental.

`adam` is a standalone Pi extension for durable session replication and provenance-bearing memory retrieval. It runs alongside memory producers such as `pi-observational-memory`; it does not execute or import their runtime.

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
- user, identity, repository, worktree, file, and revision identity;
- deterministic file-tool evidence;
- adapters that interpret supported persisted memory formats;
- Neo4j persistence and reconstruction reads;
- import, resume, status, and explicit file-context retrieval.

## Internal layers

```text
extension boundary
  └── lifecycle and command registration
       ├── session scanner
       ├── synchronization service
       │    └── session replica store
       ├── repository/file projector
       │    ├── Git resolver
       │    └── memory-source adapters
       └── file-memory query service
            ├── /adam:context
            └── adam_file_context
```

The session replica and derived knowledge projection are separate failure domains. A projection or adapter failure must never invalidate a successfully mirrored session.

## ClojureScript boundary

Application behavior is implemented in ClojureScript and compiled ahead of time to ESM JavaScript. `extension.js` is a minimal Pi factory shim. The committed `dist/adam.js` is the runtime artifact, so Pi does not require Java or a ClojureScript compiler.

Pi and Node objects remain JavaScript values at the boundary. Lifecycle handlers and tools return native promises for asynchronous work. Storage and domain logic should expose narrow ClojureScript protocols rather than depend directly on Pi contexts.

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

When a session cwd is a non-Git workspace, the selected branch is searched in session order for the first explicit file-tool path that resolves to a repository. Shell commands, prose, and path-looking memory text never establish evidence.

The projection follows parent links from the recorded current leaf. Abandoned branches remain in the lossless replica but do not contribute current code-memory associations.

This file-evidence layer is implemented in `src/adam/knowledge/evidence.cljs`, `repository.cljs`, `index.cljs`, and `store.cljs`, with lifecycle composition in `src/adam/replica/register.cljs` and Neo4j persistence in `src/adam/replica/neo4j.cljs`. It runs only after successful lossless mirroring, is rebuildable, and reports a healthy waiting state when neither the session cwd nor selected explicit evidence resolves to Git.

## Query architecture

One shared service owns repository discovery, path validation, bounded lookup, content compaction, provenance rendering, and typed errors.

- `/adam:context <path>` is the human-facing command.
- `adam_file_context({ path })` is the smaller model-facing adapter.

Both are explicit and read-only. adam does not inject retrieved memory automatically and does not provide semantic or global search in the first release.

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

Agent tool:

- `adam_file_context({ path })`

Environment:

- `ADAM_NEO4J_URI`
- `ADAM_NEO4J_USERNAME`
- `ADAM_NEO4J_PASSWORD`
- optional `ADAM_NEO4J_DATABASE`, default `neo4j`

Non-secret global state lives at `join(getAgentDir(), "adam/config.json")`. Credentials remain environment-only.
