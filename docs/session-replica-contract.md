# adam session-replica contract

Status: approved design; not all behavior is implemented.

## Goals

The first release provides an extension-only, non-destructive Neo4j replica of persistent Pi session JSONL. It must:

- keep local JSONL authoritative whenever it exists;
- preserve complete headers and entry JSON, including unknown fields and inline base64 payloads;
- preserve JSONL ordering, the complete entry tree, selected leaf, and canonical fork lineage;
- synchronize active sessions eventually and import historical sessions explicitly;
- restore a missing local session through an explicit adam command;
- tolerate backend and projection failures without breaking Pi or memory producers.

Ephemeral sessions without a session file are skipped.

## Local authority and conflicts

adam never modifies or overwrites an existing session JSONL. Import and mirroring only read local files. Resume prefers an existing local file unchanged.

Entries are immutable. If an existing entry identity has a different JSON payload hash, adam preserves the remote payload, marks that session conflicted, stops automatic writes for it, and records structural diagnostics only. A conflicted remote-only session cannot be reconstructed; a conflicted session with a local file may resume locally after a warning.

Local deletion is not propagated. Explicit deletion tooling is deferred.

## Configuration and transport

Required:

- `ADAM_NEO4J_URI`
- `ADAM_NEO4J_USERNAME`
- `ADAM_NEO4J_PASSWORD`

Optional:

- `ADAM_NEO4J_DATABASE`, default `neo4j`

Missing or incomplete configuration disables replication and retrieval while Pi continues normally.

Unencrypted Neo4j schemes are accepted only for loopback hosts. Non-loopback endpoints require encrypted transport. Credentials must never be written to config files, session logs, diagnostics, or graph properties.

## Identity

A generated UUID is adam's stable local principal across hosts. Canonical non-secret state lives at `${XDG_CONFIG_HOME:-~/.config}/adam/config.json`. If it is absent, Adam atomically adopts an existing Pi-scoped UUID from `${PI_CODING_AGENT_DIR:-~/.pi/agent}/adam/config.json`. Matching dual states are accepted; differing UUIDs fail visibly rather than silently splitting identity. The initial external identity source is the effective Git email for a session cwd. Absence of a Git email is allowed.

Git email normalization trims whitespace and lowercases the full address for hashing while retaining observed spelling as a display value. Distinct addresses are never automatically merged.

User-owned identifiers use a private, user-scoped namespace. Code identity is canonical when a normalized origin exists:

```text
urn:adam:user:<userUuid>
urn:adam:identity:git-email:<sha256(normalizedEmail)>
urn:adam:session:<userUuid>:pi:<piSessionId>
urn:adam:session:<userUuid>:claude-code:<claudeSessionId>
urn:adam:stream:<userUuid>:claude-code:<claudeSessionId>:<streamId>
urn:adam:entry:<userUuid>:<sourceKind>:<sourceSessionId>:<entryId>
urn:adam:repository:git:<sha256(normalizedOrigin)>
urn:adam:repository:local:<userUuid>:<sha256(resolvedRoot)>
urn:adam:file:<sha256(repositoryId + NUL + normalizedRelativePath)>
urn:adam:observation:<userUuid>:<sourceKind>:<sourceSessionId>:<producerId>:<memoryId>
urn:adam:reflection:<userUuid>:<sourceKind>:<sourceSessionId>:<producerId>:<memoryId>
```

Observation and reflection segments use canonical percent encoding. Producer identity is immutable and prevents equal producer-local memory IDs in one source session from colliding.

Session and entry IDs are user- and source-scoped so copied files or equal host-local IDs cannot create accidental cross-user or cross-host ownership and lineage. Before normal synchronization, the versioned 0.3 migration transactionally rewrites earlier unqualified Pi session, entry, observation, reflection, parent, and denormalized session identities while preserving raw payloads, relationships, checkpoints, and remote-only sessions. The 0.4 graph-native memory migration then rewrites only observation and reflection IDs from persisted source and producer metadata. A producerless legacy `pi` memory receives the historical `pi-observational-memory` producer; missing producer provenance for any other source is rejected. Its per-user marker advances last in the same write transaction; exact effective-identity checks force repair when a stale memory contradicts an advanced marker. Sessions, entries, tombstones, support links, file provenance, and remote-only memories remain in place. Repositories with equivalent credential-free SSH or HTTPS origins converge across users, sessions, machines, checkouts, and registered worktrees. Originless repositories retain isolated user-scoped local identities and cannot be queried by origin.

## JSONL validation

A valid persisted session has:

- exactly one header as the first non-empty line;
- valid JSON on every non-empty line;
- a non-empty Pi session ID in the header;
- unique, non-empty entry IDs;
- parent references that resolve within the file or are null roots;
- deterministic ordinals, byte offsets, byte lengths, and SHA-256 hashes.

The Pi scanner streams lines and preserves each complete original JSON line. New or unknown entry types are retained rather than rejected. Malformed sessions are reported with file and line context and are never repaired automatically.

The Claude scanner separately preserves every complete parent or explicitly located subagent record, including unknown and UUID-less records. Its canonical transcript stream IDs are exactly `main` for the parent and `agent:<agentId>` for each subagent; memory-producer checkpoints and citations use those same IDs. UUID-less records receive deterministic stream-local identities. A non-newline invalid tail is deferred as a concurrent partial write; malformed newline-terminated records are rejected. Stream checkpoints independently detect shrinkage and committed-prefix changes. Parent and `logicalParentUuid` continuity, latest `last-prompt.leafUuid`, active `requestId` fragments, tool-use/result IDs, and `sourceToolAssistantUUID` determine current context without relying on timestamp or physical order alone.

## Incremental synchronization

Each session checkpoint contains:

- committed ordinal;
- committed byte offset;
- committed-prefix hash.

Normal synchronization seeks to the committed offset and uploads only appended entries. File shrinkage, prefix mismatch, or checkpoint regression marks a conflict.

Transactions are bounded by encoded payload bytes, initially targeting approximately 4 MiB. Checkpoints advance only after a batch commits. Completion metadata includes entry count, log hash, log bytes, and largest entry bytes. Interrupted work is resumable and idempotent, and an incomplete prefix is never exposed as restorable.

## Claude lifecycle reconciliation

Claude `SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks accept bounded host JSON and atomically persist only validated session, cwd, parent-transcript, child-transcript, agent, event, and timestamp locators under `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/`. Hooks do not read transcripts or contact Neo4j and return after waking a detached worker.

One filesystem lease serializes workers. Repeated notifications for the same session/stream locator coalesce; notifications are removed only after lossless stream synchronization and derived file-evidence handling succeed. Parent and explicitly supplied subagent locators are retained in an owner-only per-session manifest for complete subsequent rebuilds. Backend or source failures retain notifications and retry with exponential backoff. A dead worker PID or old incomplete lease can be recovered, while a concurrent live worker prevents duplicate processing.

## Producer memory sidecars

The normative 0.4 file boundary is [`memory-protocol-contract.md`](memory-protocol-contract.md). One producer/source session owns one append-only sidecar under XDG data storage. It uses four event kinds: recorded observations, recorded reflections, dropped-observation tombstones, and content-free source coverage.

`source.covered` commits a successful no-memory result without inventing a memory. Producer restart recovery folds source coverage from the sidecar, so a crash after durable append does not repay indefinitely for a range the model already declined. Event replay compares SHA-256 hashes of RFC 8785 canonical JSON; an equal repeated event is idempotent and a changed payload under the same event ID conflicts.

Each event carries a complete stream-qualified source checkpoint. It is producer provenance and producer replay authority only. Adam validates it monotonically against that sidecar's last accepted checkpoint; a disappearing stream, decreasing offset, or changed hash at an equal offset is a non-conflicting `checkpoint-regression` diagnostic and the event is skipped. Adam must not compare it with independently observed Pi or Claude synchronization checkpoints. Source citations may remain unresolved until later host-source reconciliation.

The physical stream uses owner-only, non-symlink, single-writer UTF-8 JSONL with a 1 MiB record bound. The implemented sidecar scanner retains complete malformed and unsupported records with diagnostics, defers a non-LF tail, and detects concurrent changes. Checkpointed synchronization stores raw records in byte-bounded batches. Sidecar shrinkage, committed-prefix mutation, invalid UTF-8, or oversize records physically conflict only that memory stream after any preceding safe records commit. Immutable event reuse is a semantic conflict: later complete raw suffixes still mirror as `blocked`, but semantic advancement stops. A missing or unsafe sidecar is terminal for the current notification, which is acknowledged and logged without marking the stream conflicted; a later notification can ingest a repaired source. Concurrent change and a not-yet-mirrored source session are transient and retryable. POSIX owner-mode enforcement currently limits sidecar ingestion to Linux and macOS.

## Fork lineage

Pi's header `parentSession` path is retained as provenance but is not canonical identity. adam reads the referenced parent header with a bounded reader and derives the parent session URN from its Pi session ID.

```text
(child:AdamSession)-[:FORKED_FROM]->(parent:AdamSession)
```

Child-first and parent-first synchronization must converge on one edge. Missing, unreadable, malformed, cross-user, or self-referential parents do not fail child mirroring. Later reconciliation may establish a previously unresolved edge.

## Neo4j graph

Uniqueness constraints currently cover `id` on:

- `AdamUser`
- `AdamIdentity`
- `AdamSession`
- `AdamEntry`
- `AdamTranscriptStream`
- `AdamRepository`
- `AdamCodeFile`
- `AdamObservation`
- `AdamReflection`

The 0.4 lossless sidecar implementation adds unique `AdamMemoryStream` and `AdamMemoryRecord` nodes. A source session owns producer streams through `HAS_MEMORY_STREAM`; each stream owns its physical records through `HAS_RECORD`. Streams distinguish `physical` from `semantic` conflict class so only physical conflicts stop suffix writes. Record payloads, ordinals, byte offsets, hashes, semantic status, and bounded diagnostics feed a deterministic aggregate projection across embedded Pi memory and every retained producer prefix.

A composite range index on `AdamEntry(sessionId, entryId)` supports bounded per-session evidence and memory projection lookups without scanning the complete replicated entry graph.

### Lossless replica nodes

```text
AdamUser
  id, createdAt

AdamIdentity
  id, kind, value, normalizedValue, displayValue, firstSeenAt, lastSeenAt

AdamSession
  id, sourceKind, sourceSessionId, piSessionId, headerJson, cwd, version, createdAt
  parentSession, parentPiSessionId, parentSessionId
  name, currentLeafId, sourceFile
  completeThroughOrdinal, completeThroughByteOffset, committedPrefixHash
  entryCount, logHash, logBytes, largestEntryBytes
  conflicted, lastMirroredAt, writerVersion

AdamTranscriptStream
  id, sessionId, streamId, agentId, transcriptPath
  completeThroughOrdinal, completeThroughByteOffset, committedPrefixHash
  entryCount, logHash, logBytes, sourceBytes, incompleteTailBytes
  conflicted, lastMirroredAt

AdamEntry
  id, sessionId, sourceKind, sourceSessionId, entryId, recordUuid, streamId, agentId
  type, role, parentId, logicalParentId, cwd, requestId, timestamp, ordinal
  rawJson, payloadHash, payloadBytes
```

Relationships:

```text
(AdamUser)-[:HAS_IDENTITY]->(AdamIdentity)
(AdamUser)-[:OWNS]->(AdamSession)
(AdamSession)-[:FORKED_FROM]->(AdamSession)
(AdamSession)-[:HAS_STREAM]->(AdamTranscriptStream)
(AdamTranscriptStream)-[:HAS_ENTRY]->(AdamEntry)
(AdamSession)-[:HAS_ENTRY]->(AdamEntry)
(AdamEntry)-[:PARENT]->(AdamEntry)
(AdamEntry)-[:LOGICAL_PARENT]->(AdamEntry)
(AdamSession)-[:CURRENT_LEAF]->(AdamEntry)
```

### Derived knowledge nodes

```text
AdamRepository
  id, normalizedOrigin

AdamCodeFile
  id, repositoryId, relativePath

AdamObservation
  id, sourceKind, sourceSessionId, producer, memoryId, content, timestamp, relevance
  tokenCount, recordingEntryId, sourceEntryIds, dropped, extractorVersion

AdamReflection
  id, sourceKind, sourceSessionId, producer, memoryId, content, tokenCount
  recordingEntryId, supportingObservationIds, extractorVersion
```

Relationships:

```text
(AdamSession)-[:WORKED_ON]->(AdamRepository)
(AdamRepository)-[:CONTAINS]->(AdamCodeFile)
(AdamSession)-[:HAS_MEMORY]->(AdamObservation|AdamReflection)
(AdamObservation)-[:SOURCED_FROM]->(AdamEntry)
(AdamReflection)-[:SUPPORTED_BY]->(AdamObservation)
(AdamEntry)-[:TOUCHES]->(AdamCodeFile)
(AdamObservation)-[:ABOUT]->(AdamCodeFile)
```

`WORKED_ON` and `TOUCHES` retain checkout-specific root, commit, optional branch, and dirty state rather than placing mutable checkout state on canonical repository/file nodes. `TOUCHES` also records evidence basis and extractor version.

Queries enforce user isolation through `(AdamUser)-[:OWNS]->(AdamSession)-[:HAS_MEMORY]->(...)-[:ABOUT]->(AdamCodeFile)`, never through ownership of shared repository/file nodes.

Derived knowledge may be deleted and rebuilt without affecting the lossless replica. A session's file evidence and producer-scoped memory are rebuilt as one aggregate snapshot; when a source transcript or memory sidecar no longer exists, projection reads its retained mirrored raw entries or accepted memory-record prefix so prior evidence and memory survive the rebuild. Repository-less sessions retain memory nodes and unresolved citation diagnostics while omitting file links; later source/repository reconciliation rebuilds those links.

The initial aggregate implementation rereads and reparses every retained record from every producer stream for that source session on each reconciliation. Step 6 worker sizing must assume work linear in retained per-session memory records. A later optimization may cache each stream's folded semantic state by committed stream checkpoint, but that cache remains derived and must not change aggregate results. The 0.2 code-memory schema/extractor version performs a restart-safe rebuild that replaces user-scoped repository/file identities, removes obsolete derived relationships and orphaned code nodes, and can resume idempotently after interruption.

## Commands

### `/adam:import`

Imports persisted sessions whose header cwd exactly equals the current cwd.

### `/adam:import --all`

Imports every session discoverable from Pi's configured session storage.

Both require confirmation, stream progress, remain idempotent and resumable, continue past per-file failures, and report unchanged, imported, malformed, conflicted, and failed counts.

### `/adam:resume`

Shows the deduplicated union of exact-cwd local and Neo4j sessions with `local+neo`, `local`, `neo`, or `conflict` status.

Local files are always preferred and opened unchanged. A complete, validated, remote-only session is materialized through a flushed temporary file and atomic rename into Pi's normal session directory. Existing targets are never overwritten. After switching, adam restores the selected leaf without summarization; an invalid leaf warns and falls back to Pi's default.

Cross-machine path mapping is deferred.

### `/adam:status`

Reports configuration, connection state, adam user UUID, masked Git email, active persisted session, checkpoint/pending work, conflicts, mirror state, knowledge-projection state, and the last dependency error. Repeated outage warnings are deduplicated and recovery is announced once.

### `/adam:context <path>`

Returns active observations plus relevant reflections for the file, up to 20. A dropped observation (`dropped = true`) stays in the graph with its `HAS_MEMORY`, `ABOUT`, and `SOURCED_FROM` provenance but is excluded inside the storage query before results are ordered and limited; an absent `dropped` property counts as active. Reflections are not excluded because their supporting observations were dropped.

Returns a bounded, provenance-bearing list of file-linked memories. Local mode accepts repository-relative, workspace-relative, or absolute paths and rejects paths outside the resolved repository.

### `/adam:context --origin <git-origin> <repository-relative-path>`

Normalizes the explicit origin and queries canonical repository/file identity without a local checkout. Origin mode rejects malformed origins and empty, absolute, or traversing paths before Neo4j access.

## Agent tool

`adam_file_context({ path, origin? })` uses the same query service as `/adam:context`, with a smaller model-facing result bound and explicit item, line, and byte truncation. Omitting `origin` selects existing local-path behavior; supplying it requires a normalized repository-relative path and does not require a checkout. Empty results are successful. Backend, origin, and path failures remain distinguishable. The tool is registered only when Neo4j configuration enables the subsystem.

Pi and the Claude Code stdio MCP adapter expose the same host-neutral execution contract. The Claude plugin starts the compiled `mcp.js` boundary, registers the tool as read-only, non-destructive, and idempotent, and uses the permanent canonical Adam user. Backend failures are returned as tool-local errors and the Neo4j driver closes when the MCP process ends.

Pi prompt guidance and the bundled Claude skill recommend retrieval for a known file when prior decisions may materially affect work, while discouraging calls for every file or semantic/global search. Claude transcript ingestion and automatic prompt insertion are not part of this read-only stage.

## Logging and privacy

Diagnostics may contain structural metadata, counts, durations, reason classes, resource IDs, offsets, and hashes. They must not contain raw entry JSON, message or memory text, base64 payloads, credentials, or full Git email addresses.

## Non-goals

The first release does not provide:

- replacement of Pi's built-in persistence or `/resume`;
- ephemeral-session replication;
- a second storage adapter;
- cross-machine cwd mapping;
- blob storage;
- automatic local-deletion propagation;
- JSONL repair;
- semantic or global memory search;
- automatic prompt injection;
- shell-command path inference;
- symbol or line-range identity;
- model-assisted association;
- user federation or automatic identity merging;
- migration or compatibility with APIOM graph state or public names.
