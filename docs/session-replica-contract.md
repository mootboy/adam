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

A generated UUID is adam's stable local principal. The initial external identity source is the effective Git email for a session cwd. Absence of a Git email is allowed.

Git email normalization trims whitespace and lowercases the full address for hashing while retaining observed spelling as a display value. Distinct addresses are never automatically merged.

User-owned identifiers use a private, user-scoped namespace. Code identity is canonical when a normalized origin exists:

```text
urn:adam:user:<userUuid>
urn:adam:identity:git-email:<sha256(normalizedEmail)>
urn:adam:session:<userUuid>:<piSessionId>
urn:adam:entry:<userUuid>:<piSessionId>:<entryId>
urn:adam:repository:git:<sha256(normalizedOrigin)>
urn:adam:repository:local:<userUuid>:<sha256(resolvedRoot)>
urn:adam:file:<sha256(repositoryId + NUL + normalizedRelativePath)>
urn:adam:observation:<userUuid>:<piSessionId>:<memoryId>
urn:adam:reflection:<userUuid>:<piSessionId>:<memoryId>
```

Session and entry IDs are user-scoped so copied files cannot create accidental cross-user ownership or lineage. Repositories with equivalent credential-free SSH or HTTPS origins converge across users, sessions, machines, checkouts, and registered worktrees. Originless repositories retain isolated user-scoped local identities and cannot be queried by origin.

## JSONL validation

A valid persisted session has:

- exactly one header as the first non-empty line;
- valid JSON on every non-empty line;
- a non-empty Pi session ID in the header;
- unique, non-empty entry IDs;
- parent references that resolve within the file or are null roots;
- deterministic ordinals, byte offsets, byte lengths, and SHA-256 hashes.

The scanner streams lines and preserves each complete original JSON line. New or unknown entry types are retained rather than rejected. Malformed sessions are reported with file and line context and are never repaired automatically.

## Incremental synchronization

Each session checkpoint contains:

- committed ordinal;
- committed byte offset;
- committed-prefix hash.

Normal synchronization seeks to the committed offset and uploads only appended entries. File shrinkage, prefix mismatch, or checkpoint regression marks a conflict.

Transactions are bounded by encoded payload bytes, initially targeting approximately 4 MiB. Checkpoints advance only after a batch commits. Completion metadata includes entry count, log hash, log bytes, and largest entry bytes. Interrupted work is resumable and idempotent, and an incomplete prefix is never exposed as restorable.

## Fork lineage

Pi's header `parentSession` path is retained as provenance but is not canonical identity. adam reads the referenced parent header with a bounded reader and derives the parent session URN from its Pi session ID.

```text
(child:AdamSession)-[:FORKED_FROM]->(parent:AdamSession)
```

Child-first and parent-first synchronization must converge on one edge. Missing, unreadable, malformed, cross-user, or self-referential parents do not fail child mirroring. Later reconciliation may establish a previously unresolved edge.

## Neo4j graph

Uniqueness constraints cover `id` on:

- `AdamUser`
- `AdamIdentity`
- `AdamSession`
- `AdamEntry`
- `AdamRepository`
- `AdamCodeFile`
- `AdamObservation`
- `AdamReflection`

A composite range index on `AdamEntry(sessionId, entryId)` supports bounded per-session evidence and memory projection lookups without scanning the complete replicated entry graph.

### Lossless replica nodes

```text
AdamUser
  id, createdAt

AdamIdentity
  id, kind, value, normalizedValue, displayValue, firstSeenAt, lastSeenAt

AdamSession
  id, piSessionId, headerJson, cwd, version, createdAt
  parentSession, parentPiSessionId, parentSessionId
  name, currentLeafId, sourceFile
  completeThroughOrdinal, completeThroughByteOffset, committedPrefixHash
  entryCount, logHash, logBytes, largestEntryBytes
  conflicted, lastMirroredAt, writerVersion

AdamEntry
  id, entryId, type, role, parentId, timestamp, ordinal
  rawJson, payloadHash, payloadBytes
```

Relationships:

```text
(AdamUser)-[:HAS_IDENTITY]->(AdamIdentity)
(AdamUser)-[:OWNS]->(AdamSession)
(AdamSession)-[:FORKED_FROM]->(AdamSession)
(AdamSession)-[:HAS_ENTRY]->(AdamEntry)
(AdamEntry)-[:PARENT]->(AdamEntry)
(AdamSession)-[:CURRENT_LEAF]->(AdamEntry)
```

### Derived knowledge nodes

```text
AdamRepository
  id, normalizedOrigin

AdamCodeFile
  id, repositoryId, relativePath

AdamObservation
  id, producer, memoryId, content, timestamp, relevance
  tokenCount, recordingEntryId, sourceEntryIds, dropped, extractorVersion

AdamReflection
  id, producer, memoryId, content, tokenCount
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

Derived knowledge may be deleted and rebuilt without affecting the lossless replica. The 0.2 code-memory schema/extractor version performs a restart-safe rebuild that replaces user-scoped repository/file identities, removes obsolete derived relationships and orphaned code nodes, and can resume idempotently after interruption.

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

Returns a bounded, provenance-bearing list of file-linked memories. Local mode accepts repository-relative, workspace-relative, or absolute paths and rejects paths outside the resolved repository.

### `/adam:context --origin <git-origin> <repository-relative-path>`

Normalizes the explicit origin and queries canonical repository/file identity without a local checkout. Origin mode rejects malformed origins and empty, absolute, or traversing paths before Neo4j access.

## Agent tool

`adam_file_context({ path, origin? })` uses the same query service as `/adam:context`, with a smaller model-facing result bound and explicit item, line, and byte truncation. Omitting `origin` selects existing local-path behavior; supplying it requires a normalized repository-relative path and does not require a checkout. Empty results are successful. Backend, origin, and path failures remain distinguishable. The tool is registered only when Neo4j configuration enables the subsystem.

Its Pi prompt guidance recommends retrieval for a known file when prior decisions may materially affect work, while discouraging calls for every file or semantic/global search.

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
