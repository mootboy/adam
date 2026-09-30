# Adam 0.4.0 plan: provider-neutral memory production

Status: draft for review

Adam 0.3 made Claude Code a second ingestion host: Adam mirrors its parent and subagent transcripts, indexes native file evidence, and exposes the same explicit file-memory query used by Pi. Claude conversations still produce no observations or reflections. Adam should not become the component that decides what is memorable; instead, 0.4.0 defines a durable protocol through which independent producers can publish memories that Adam losslessly replicates, validates, links to source entries, and projects onto canonical files.

This plan keeps three boundaries explicit:

- hosts own authoritative sessions and transcript streams;
- memory producers own generation, consolidation, reflection, and dropping;
- Adam owns lossless replication, identity, provenance, deterministic file association, and bounded retrieval.

## Required invariants

1. Adam never calls a model to create an observation or reflection.
2. Producers do not import Adam or write Neo4j. Their only integration is the documented file protocol and durable notification spool.
3. Host lifecycle hooks remain fast. Model calls and graph writes happen only in detached, retryable workers.
4. Source sessions, transcript streams, and producer memory streams remain independently authoritative. Failure in one never invalidates another.
5. Every accepted memory event is mirrored byte-for-byte before it affects derived memory projection.
6. Rebuilding one producer cannot delete another producer's memories.
7. A producer retry after a crash cannot silently create a second logical result for the same committed source checkpoint.
8. Memories are associated with files only through cited source entries that already carry deterministic `TOUCHES` evidence. Memory prose never establishes a file association.
9. Adam retrieval remains explicit. This work adds no automatic prompt insertion, semantic search, or global search.

## Target architecture

```text
  Pi session JSONL              Claude transcript streams        producer memory sidecars
  host authority                host authority                   producer authority
          │                              │                                │
          │ lossless sync                │ lossless sync                  │ lossless sync
          ▼                              ▼                                ▼
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │ Adam                                                                        │
  │ sessions · entries · transcript streams · memory streams · memory records   │
  │ file evidence · observations · reflections · tombstones                     │
  │                                                                              │
  │ aggregate projection per source session:                                    │
  │ memory → cited entry → TOUCHES → canonical file                              │
  └──────────────────────────────────────────────────────────────────────────────┘
                     ▲                                  ▲
                     │ /adam:context                    │ adam_file_context
                     │                                  │
                    Pi                              Claude Code
```

Pi continues to receive `pi-observational-memory` events inside authoritative session JSONL. Adam normalizes those custom entries into the same internal protocol model used for sidecars. Claude and future hosts use sidecars because third-party producers must not modify host transcripts.

## Stage 1: memory protocol v1

Write `docs/memory-protocol-contract.md` before implementation. It is the normative contract; this plan fixes the architectural decisions it must encode.

### Canonical producer identity

A producer declares an immutable `producerId`, distinct from its display name and package version. It must match:

```text
^[a-z0-9](?:[a-z0-9._-]{0,127})$
```

Examples are `pi-observational-memory` and `org.example.claude-memory`. The ID is case-sensitive only in the sense that uppercase is invalid. A package rename or version upgrade does not change it. `producerVersion` is provenance, not identity.

Adam never uses an unvalidated display name as a path or URN segment. Every record's producer and source fields must match the sidecar location and the source session being reconciled; a record cannot redirect itself to another user, session, or stream.

### Authoritative sidecar location

Memory content is durable user data, not configuration. Sidecars live under XDG data storage:

```text
${XDG_DATA_HOME:-~/.local/share}/adam/memories/v1/
  <sourceKind>/<sha256(sourceSessionId)>/<producerId>.jsonl
```

`sourceKind` and `producerId` are validated protocol identifiers. Hashing the source session ID prevents path traversal and platform-specific path ambiguity; the unhashed ID remains in every record and is verified when scanning.

Directories are owner-only (`0700`) and files are owner-only (`0600`). Writers must reject symlinks and non-regular files. Adam state such as checkpoints, notifications, leases, and diagnostics remains separate from authoritative content and belongs under `${XDG_STATE_HOME:-~/.local/state}/adam/` for this new protocol. Existing 0.3 state paths are not migrated as part of this work.

There is one logical sidecar per producer and source session. Producer upgrades append to the same stream while recording the version that emitted each event.

### Physical JSONL rules

A sidecar is an append-only UTF-8 JSONL stream. Each complete physical record is retained exactly, including supported, unsupported, and malformed records.

The protocol contract must specify:

- a maximum record size;
- one exclusive writer lease per producer/session stream;
- one append operation per encoded record using append mode;
- file and containing-directory synchronization after durable creation/append;
- deferred handling of an incomplete trailing record;
- rejection of malformed completed JSON;
- checkpoint validation by committed byte offset and prefix hash;
- conflict on committed-prefix mutation, shrinkage, or changed immutable event payload;
- bounded diagnostics that contain identifiers and reasons, not memory content.

A writer must not assume that concurrent append calls to a regular file provide a portable record boundary. Concurrent hook invocations enqueue producer work; one producer worker serializes actual sidecar writes.

### Event envelope

Every line has a stable event identity and explicit source provenance. A representative observation event is:

```json
{
  "protocolVersion": 1,
  "eventId": "01JQ7M6N0FK3AH1X7N6DK0D8YZ",
  "kind": "observations.recorded",
  "producer": {
    "id": "org.example.claude-memory",
    "version": "0.1.0"
  },
  "source": {
    "kind": "claude-code",
    "sessionId": "6fbc25a9-21c4-46d2-8b56-d681134038e0"
  },
  "sourceCheckpoint": {
    "streams": [
      {
        "streamId": "main",
        "committedBytes": 1013706,
        "prefixSha256": "…",
        "selectedLeafEntryId": "d0b1…"
      }
    ]
  },
  "recordedAt": "2026-09-28T09:12:44.120Z",
  "observations": [
    {
      "id": "70f6600ddded",
      "content": "…",
      "timestamp": "2026-09-28T09:12:44.120Z",
      "relevance": "high",
      "sourceEntries": [
        {"streamId": "main", "entryId": "d0b1…"}
      ],
      "tokenCount": 123
    }
  ]
}
```

Supported event kinds are:

- `observations.recorded`;
- `reflections.recorded`;
- `observations.dropped`.

Observation and reflection item validation initially follows Adam's existing `pi-observational-memory` structural rules. Sidecar entry citations are qualified by stream because Claude sessions can contain parent and subagent streams. Pi normalization supplies its single implicit stream.

A reflection's `supportingObservationIds` and a drop event's `observationIds` refer only to memories from the same producer and source session. Cross-producer support, dropping, consolidation, and deduplication are not protocol v1 behavior.

`sourceCheckpoint` records the physical source prefixes and selected context used by the producer. It is provenance supplied by the producer, not an Adam synchronization checkpoint: Adam validates its shape and cited stream identities but never compares its offsets or hashes with Adam's independently observed transcript checkpoints. The producer and Adam can inspect the same source at different moments, so such a comparison would create false conflicts. A single `coversUpToEntryId` is deliberately insufficient for Claude because physical append order, selected tree continuity, parallel request fragments, compaction ancestry, and subagent streams are distinct concerns.

### Replay and crash consistency

`eventId` is immutable within a producer/session stream. Repeating an event ID with byte-equivalent semantic payload is idempotent; reusing it with different payload is a stream conflict.

The sidecar itself is the producer's commit log. After restart, a producer derives completed source checkpoints from its valid sidecar records before consulting any disposable worker state. Therefore:

- a crash before append may repeat model generation but has committed no result;
- a crash after durable append observes the committed event on restart and does not generate a second logical result;
- a separately stored producer checkpoint can optimize scanning but cannot advance authority beyond the sidecar.

The reference producer must prove this crash window with deterministic tests.

### Memory semantics

Within one producer/session stream:

- the first valid immutable definition of a memory ID wins;
- an incompatible later definition is diagnosed as a conflict rather than silently replacing content;
- tombstones are durable and may arrive before the corresponding observation;
- unresolved entry citations and support links are retained and retried after later source reconciliation;
- malformed or unsupported records remain mirrored but do not enter derived projection;
- Adam never deletes or rewrites producer output.

## Stage 2: lossless Adam ingestion

### Graph model

Add a lossless graph representation before extending derived projection:

```text
(AdamSession)-[:HAS_MEMORY_STREAM]->(AdamMemoryStream)
(AdamMemoryStream)-[:HAS_RECORD]->(AdamMemoryRecord)
```

`AdamMemoryStream` is uniquely identified by Adam user, source kind, source session ID, and producer ID. It stores checkpoint, prefix hash, byte counts, completion/conflict state, and producer/source metadata.

`AdamMemoryRecord` has a deterministic stream-scoped identity and retains:

- exact `rawJson`;
- ordinal and byte boundaries;
- protocol version, event ID, and event kind when structurally readable;
- source locator and producer provenance;
- immutable-payload hash.

Unknown and malformed records are still raw records. This lossless replica allows rebuilds and graph-native migrations even when a local sidecar is temporarily absent. Restoring a sidecar to the filesystem remains deferred; retained graph records are a replica, not a replacement authority.

### Scanner and synchronization

Reuse the proven transcript framing and append-checkpoint concepts through a shared low-level JSONL scanner, without coupling memory semantics to Claude transcript schemas. Each memory stream synchronizes independently in byte-bounded batches. A conflict in one memory stream does not block source-session replication, file evidence, or other producers.

If a previously known sidecar is absent, Adam preserves its mirrored raw records and last successful derived contribution. If it reappears, synchronization resumes against the prior checkpoint and normal shrink/prefix-conflict rules apply.

### One validator, multiple envelopes

Create one protocol-v1 validator and normalizer. Inputs are:

- sidecar event envelopes;
- a thin Pi adapter that supplies known producer/source metadata and converts supported `om.*` custom entries into the same normalized event form.

Adam does not claim that legacy Pi entries literally contain every sidecar envelope field. Values absent from the producer payload, such as the known Pi producer identity or recording locator, are supplied by the trusted structural adapter and remain distinguishable as adapter-derived provenance. Because Pi custom entries have no protocol event ID, the adapter deterministically synthesizes `eventId` from the source-scoped Pi custom-entry identity. Re-reading or rebuilding the same session therefore produces the same event ID and participates in the same replay/conflict rules as a sidecar event.

### Aggregate session projection

Replace the current single-adapter assumption with one deterministic aggregate snapshot per source session. Before writing derived memory state, Adam assembles:

1. normalized embedded memory events, such as Pi custom entries;
2. every successfully mirrored sidecar producer stream;
3. retained mirrored records for a known stream whose source file is currently absent;
4. the session's current deterministic file evidence.

The knowledge-store transaction then replaces the complete session projection once. It must never run a per-producer delete-and-recreate operation. A failed or conflicted producer stream retains its last committed prefix and previous valid contribution while healthy producers continue to reconcile.

Projection creates or rebuilds:

- `AdamObservation` and `AdamReflection` nodes;
- `SOURCED_FROM` links to entries in the same owned source session;
- `ABOUT` links derived only through cited entries' `TOUCHES` relationships;
- producer-local `SUPPORTED_BY` links;
- dropped state and unresolved-reference diagnostics.

Unresolved citations do not discard the memory record. A later aggregate rebuild creates the missing provenance and file links after the cited source entry is mirrored.

### Producer-scoped memory identity migration

0.4.0 adopts one identity scheme; legacy Pi identities are not grandfathered:

```text
urn:adam:observation:<user>:<sourceKind>:<sourceSessionId>:<producerId>:<memoryId>
urn:adam:reflection:<user>:<sourceKind>:<sourceSessionId>:<producerId>:<memoryId>
```

Canonical encoding applies only to the new observation and reflection URNs shown above. Existing user, session, transcript-stream, and entry URNs do not change in 0.4.0; expanding this into another source-identity migration is explicitly out of scope.

Before normal memory writes, a graph-native transaction migrates existing Pi observations and reflections using their persisted producer property. It updates identities, stored IDs, support relationships, tombstones, and dependent provenance without requiring local session files. The per-user migration marker advances only at the end of a completely successful transaction. Stale dependent identities force retry even if a marker claims completion. Live coverage must include remote-only memories, dropped observations, support links, file provenance, rollback, and idempotency.

### Retrieval and status

`/adam:context` and `adam_file_context` retain their existing bounds and active-memory semantics. Rendering adds concise source and producer provenance without exposing local sidecar paths. Result ordering and item/line/byte truncation remain deterministic.

`/adam:status` reports bounded latest-run information for memory synchronization: stream counts, records appended, conflicts, unresolved citations, migration state, and timing. It does not retain memory content or unbounded history.

## Stage 3: durable discovery and reconciliation

### Notification spool

Directory discovery alone is insufficient: Adam's final lifecycle hook may run before a producer commits its sidecar, leaving no later host event to trigger a scan. Protocol v1 therefore includes a durable, locator-only memory notification spool at `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/`.

After a sidecar append, the producer atomically writes an owner-only notification containing only:

- notification version and ID;
- source kind and source session ID;
- producer ID;
- sidecar locator hash or canonical relative locator;
- queued timestamp.

It contains no memory text, transcript content, credentials, or model output. Notifications use create/fsync/rename/directory-fsync durability and remain until Adam acknowledges successful or terminally classified processing.

Writing the notification is the producer's only Adam-facing action and does not import or invoke Adam. Any later Adam entry point may drain it: Pi startup/lifecycle, the Claude reconciliation worker, or an explicit reconciliation command. A best-effort wake mechanism may reduce latency, but correctness depends on the durable spool, not process ordering. Reversed final-hook ordering must converge after the next Adam startup or explicit reconciliation without another producer append.

This spool is a public file protocol, not reuse of Claude's private hook payload. Adam therefore drains two deliberately separate queues:

- `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/` retains the implemented 0.3 Claude lifecycle notifications that locate authoritative parent and subagent transcripts;
- `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/` contains protocol-v1 producer notifications that locate authoritative memory sidecars.

They have different schemas, authorities, coalescing keys, and acknowledgement conditions even if one worker lease eventually drains both. A transcript notification is acknowledged after host-source synchronization and evidence projection; a memory notification is acknowledged only after the named sidecar prefix is mirrored and included in an aggregate memory projection. The existing Claude inbox is not exposed as a producer API, and migrating its location is deferred.

### Worker behavior

One Adam lease serializes memory-stream graph writes. The worker:

1. validates and coalesces notifications by source session and producer;
2. mirrors the relevant sidecars independently;
3. loads retained records for absent known streams;
4. assembles one aggregate session projection;
5. acknowledges only notifications covered by successful processing;
6. continues healthy groups when another stream fails;
7. retries transient failures with capped backoff.

A malformed supported record is a bounded adapter diagnostic and does not poison source-session replication. Prefix mutation or immutable-event conflict marks only that memory stream conflicted and preserves its last committed projection.

Both Pi and Claude composition roots use this host-neutral memory reconciliation service. If v1 sidecars are intentionally implemented only for Claude, the contract must say so; the preferred 0.4.0 outcome is that any known source session can own a protocol sidecar.

## Stage 4: reference Claude producer

The reference producer is a separate package and repository. It has no Neo4j access and no Adam runtime dependency.

### Non-blocking lifecycle

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks only validate bounded locators, enqueue producer work durably, wake a detached producer worker, and return. They never call a model or wait for sidecar reconciliation.

One producer worker:

- maintains explicit parent and subagent locators;
- scans append-only source prefixes and reconstructs selected context according to Adam's documented Claude transcript contract;
- resolves per-record cwd but leaves file association to Adam;
- calls a configured model backend outside Claude's interactive lifecycle;
- writes protocol events under one producer/session writer lease;
- derives its committed source progress from its own sidecar;
- writes an Adam memory notification only after the sidecar append is durable;
- retries outages without duplicating committed logical work.

Native `Read`, `Edit`, and `Write` activity may be cited. Bash commands, MCP calls, compact-summary prose, subagent handback prose, and path-looking text do not become file evidence merely because the producer mentions them.

### Producer characterization before implementation

The initial characterization is recorded in [`claude-memory-producer-contract.md`](claude-memory-producer-contract.md). It approves fast locator-only hooks, a detached serialized worker, a repeatable explicitly enabled safe-mode Claude CLI tracer bullet, sidecar-derived replay, bounded structured output, source-progress scheduling, and explicit background-processing consent. Step 2 must still decide how no-memory coverage is committed and must default to an explicit-credential adapter unless inherited-login background use is confirmed to comply with the applicable subscription terms.

The characterization covered:

- credentials and model-provider configuration;
- whether generation can run detached after Claude exits;
- transcript-delta and compaction behavior visible to the producer;
- parent/subagent processing and checkpoint ownership;
- token and monetary cost controls;
- retry, cancellation, and duplicate-generation behavior;
- data sent to the model and the user's consent/privacy boundary;
- observation and reflection scheduling.

A periodic reflection job operates only on that producer's observations. Cross-producer consolidation remains deferred.

### Tracer bullet

The end-to-end proof is:

1. Claude performs a native file operation in a parent or subagent stream.
2. The producer hook returns promptly while its worker records an observation citing the exact source entry.
3. The sidecar and durable notification survive process restart.
4. Adam mirrors the source and memory streams, then projects one aggregate session snapshot.
5. Claude's `adam_file_context` returns the Claude-produced memory with source, stream, revision, and producer provenance.
6. Pi's `/adam:context` returns the same memory through the same canonical file identity.
7. Reversing Adam/producer hook completion order still converges after a later Adam wake.
8. A producer crash immediately after sidecar append does not create a duplicate logical event on retry.

## Deferred

- Semantic or global search.
- Automatic retrieval or prompt insertion.
- Model-assisted file association.
- Cross-producer deduplication, support links, consolidation, or dropping.
- Restoring host transcripts or producer sidecars from Neo4j.
- Treating undocumented host storage layouts as discovery APIs.
- A continuously running Adam daemon; protocol-v1 correctness uses durable notifications and later reconciliation.
- Migration of Adam's existing 0.3 config/inbox/log paths to XDG data/state directories.

## Delivery sequence

The scope is one 0.4.0 milestone, not one implementation PR. Track each numbered increment with a repository issue and deliver it through one or more small, independently reviewable PRs. Intermediate PRs land on `main` without changing the released package version. Only the final release-preparation PR bumps the package and plugin versions to 0.4.0, finalizes the changelog, rebuilds all committed runtimes, and activates the protected-main tag/release pipeline.

1. **Complete:** characterize the reference Claude producer and record the non-blocking generation/privacy contract in [`claude-memory-producer-contract.md`](claude-memory-producer-contract.md).
2. Commit `docs/memory-protocol-contract.md` with fixtures for valid events, replay, malformed complete records, incomplete tails, unresolved citations, tombstones, duplicate IDs, prefix changes, and source/producer mismatch.
3. Implement lossless memory-stream scanning, raw-record storage, checkpoints, and live round-trip tests.
4. Implement the graph-native producer-scoped memory-identity migration without changing session, stream, or entry identities.
5. Implement aggregate multi-producer projection and retrieval provenance.
6. Implement the distinct durable memory-notification spool and host-neutral reconciliation service.
7. Build the reference producer and complete the cross-host tracer bullet.
8. Prepare and merge the sole 0.4.0 release PR through the protected-main flow, then cut over Pi and Claude installations to the same tagged artifact.

## 0.4.0 acceptance

- An independent producer writes protocol-v1 events without importing Adam or accessing Neo4j.
- Every complete sidecar record, including unknown and malformed records, is mirrored byte-for-byte with restart-safe append checkpoints.
- Multiple producers for one session survive aggregate rebuilds without deleting or overwriting one another.
- A missing or conflicted producer stream preserves its last committed raw replica and derived contribution while healthy streams continue.
- Pi and Claude memories about the same canonical file are returned together from both hosts with user-isolated source, stream, revision, and producer provenance.
- Records citing not-yet-mirrored entries converge after later source reconciliation.
- Reversed final-hook ordering converges through the durable notification spool after restart or explicit reconciliation.
- Producer retries across the append/checkpoint crash window do not duplicate logical events.
- Existing Pi memories migrate transactionally to producer-scoped identities, including remote-only graph state, tombstones, support links, and file provenance.
- Malformed records, model outages, Neo4j outages, and one stream's conflict do not block host operation or unrelated reconciliation.
- Claude lifecycle hooks remain bounded and do not wait for model generation or Neo4j.
- The reference Claude producer completes the tracer bullet while Adam itself still creates no observation or reflection.
