# Adam 0.4.0 plan: standalone producers and memory-source adapters

Status: active milestone plan

Adam 0.3 made Claude Code a second ingestion host: Adam mirrors its parent and subagent transcripts, indexes native file evidence, and exposes the same explicit file-memory query used by Pi. Claude conversations still produce no observations or reflections. Adam must not decide what is memorable or require memory producers to know that Adam exists. As with Pi observational memory, a standalone Claude producer owns its persisted format; Adam owns a read-only adapter, discovery, lossless replication, normalization, provenance, and retrieval.

**Dependency correction (2026-10-02):** this supersedes the earlier requirement that the reference producer write Adam protocol-v1 files, enqueue Adam notifications, or optionally wake Adam's worker. No-import integration still reverses the dependency if a producer must know Adam's paths or process commands. Completed protocol/storage/spool work remains existing Adam infrastructure, not a requirement imposed on producers. The native-format reader/discovery path described below is planned, not implemented.

This plan keeps three boundaries explicit:

- hosts own authoritative sessions and transcript streams;
- memory producers own generation, consolidation, reflection, and dropping;
- Adam owns lossless replication, identity, provenance, deterministic file association, and bounded retrieval.

## Required invariants

1. Adam never calls a model to create an observation or reflection.
2. Producers operate fully without Adam: no Adam imports, identity/configuration, native-format requirement, inbox writes, worker discovery/spawning, or Adam-specific export obligation. Adam depends on supported producer persistence, never the reverse.
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

Pi continues to receive `pi-observational-memory` events inside authoritative session JSONL. Adam structurally adapts those entries without requiring producer cooperation. The Claude producer owns a separate memory JSONL because no supported custom-entry append API has been established for Claude transcripts. Adam reads that producer's native format through its own adapter. A separate file is a host-storage decision, not an Adam dependency.

## Stage 1: memory protocol v1

The implemented [`memory-protocol-contract.md`](memory-protocol-contract.md) and fixtures define the existing protocol-v1 ingestion format. The rules below describe that format, not the mandatory native format or directory layout of the standalone reference producer. Producer-specific adapters may normalize native events into Adam's domain model without exporting or rewriting authoritative source files. Native-format scanning/storage must preserve original records, not falsely describe reserialized normalized events as a byte-for-byte replica. Its implementation remains a separate increment.

### Canonical producer identity

For protocol-v1 input, a producer identity is represented by immutable `producerId`, distinct from its display name and package version. For native input, Adam's adapter supplies a stable producer identity from the producer's documented identity/version; the producer need not adopt Adam's identifier grammar. It must match:

```text
^[a-z0-9](?:[a-z0-9._-]{0,127})$
```

Examples are `pi-observational-memory` and `org.example.claude-memory`. The ID is case-sensitive only in the sense that uppercase is invalid. A package rename or version upgrade does not change it. `producerVersion` is provenance, not identity.

Adam never uses an unvalidated display name as a path or URN segment. Every record's producer and source fields must match the sidecar location and the source session being reconciled; a record cannot redirect itself to another user, session, or stream.

### Authoritative sidecar location

Memory content is durable user data, not configuration. Existing protocol-v1 sidecars live under XDG data storage (a native producer owns its own location; Adam must not require this path):

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
- lossless retention and diagnostic rejection of malformed completed JSON;
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
- `observations.dropped`;
- `source.covered`, a content-free successful no-memory result that commits only `sourceCheckpoint`.

Observation and reflection item validation initially follows Adam's existing `pi-observational-memory` structural rules. Sidecar entry citations are qualified by stream because Claude sessions can contain parent and subagent streams. Pi normalization supplies its single implicit stream.

A reflection's `supportingObservationIds` and a drop event's `observationIds` refer only to memories from the same producer and source session. Cross-producer support, dropping, consolidation, and deduplication are not protocol v1 behavior.

`sourceCheckpoint` records the physical source prefixes and selected context used by the producer. It is provenance supplied by the producer, not an Adam synchronization checkpoint: Adam validates its shape, cited stream identities, and monotonicity against that producer sidecar's last accepted checkpoint, but never compares its offsets or hashes with Adam's independently observed transcript checkpoints. A producer-internal regression skips that event with `checkpoint-regression` rather than conflicting the stream. The producer and Adam can inspect the same source at different moments, so cross-authority comparison would create false conflicts. A single `coversUpToEntryId` is deliberately insufficient for Claude because physical append order, selected tree continuity, parallel request fragments, compaction ancestry, and subagent streams are distinct concerns.

### Replay and crash consistency

`eventId` is immutable within a producer/session stream. Repeating an event ID with byte-equivalent semantic payload is idempotent; reusing it with different payload is a stream conflict.

The sidecar itself is the producer's commit log. After restart, a producer derives completed source checkpoints from its valid sidecar records before consulting any disposable worker state. Therefore:

- a crash before append may repeat model generation but has committed no result;
- a crash after durable append observes the committed event on restart and does not generate a second logical result;
- a separately stored producer checkpoint can optimize scanning but cannot advance authority beyond the sidecar.

A valid no-memory model result is durably committed as `source.covered`; model failures and cancellations are not coverage. The reference producer must prove this crash window with deterministic tests.

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

The implemented aggregate layer replaces the previous single-adapter assumption with one deterministic snapshot per source session. Before writing derived memory state, Adam assembles:

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

Before normal memory writes, an implemented graph-native transaction migrates existing Pi observations and reflections using their persisted producer property, defaulting a producerless legacy `pi` memory to `pi-observational-memory` while rejecting that ambiguity for other sources. It updates only memory identities and required source/producer metadata; support relationships, tombstones, dependent provenance, sessions, and entries remain in place without requiring local session files. The per-user migration marker advances only at the end of a completely successful transaction. Exact effective-identity checks force retry even if a marker claims completion. Live coverage includes remote-only memories, dropped observations, support links, file provenance, rollback, and idempotency.

### Retrieval and status

`/adam:context` and `adam_file_context` retain their existing bounds and active-memory semantics. Rendering adds concise source and producer provenance without exposing local sidecar paths. Result ordering and item/line/byte truncation remain deterministic.

`/adam:status` reports bounded latest-run information for memory synchronization: stream counts, records appended, conflicts, unresolved citations, migration state, and timing. It does not retain memory content or unbounded history.

## Stage 3: Adam-owned discovery and reconciliation

### Required reader-first path (not yet implemented)

Adam registers a native memory-source adapter with an Adam-configured root or the producer's documented storage layout. At Adam startup, appropriate host lifecycle boundaries, and explicit `/adam:reconcile` / `worker.js --once`, Adam discovers and rereads supported memory logs for known owned source sessions. A producer does not register itself, export Adam files, or notify Adam. Discovery is bounded and restricted to configured/documented producer roots; it must not infer paths from prose or scan undocumented Claude transcript directories.

Adam owns native-format version detection, stable producer/event identity mapping, stream-qualified citation normalization, lossless raw replication, and append checkpoints. Native coverage fields remain producer provenance, not fictitious literal protocol-v1 fields. Unknown records remain raw; unsupported schemas produce isolated diagnostics rather than guessed semantics. Native memory and host transcripts remain separate authorities.

After reading source/session work, Adam reads memory suffixes and creates one aggregate session projection from all retained producers. Missing local logs retain mirrored accepted state. A never-mirrored source is deferred in Adam-owned state until source ownership exists; discovery must revisit its existing log after late import without another producer append or notification.

Background producer completion may occur after Adam's final hook. The minimum guarantee is visibility at the **next Adam startup/lifecycle reconciliation or explicit repair**, which rediscovers committed files even across restart. No prompt visibility after every append is promised. If prompt-independent low-latency ingestion is required, Adam can add its own bounded polling/watch scheduling; frequency, worker lifetime, shutdown behavior, and resource limits must be specified and tested in an Adam increment, not shifted onto the producer. A producer-specific wake command or notification is never necessary for correctness.

### Existing notification spool (implemented infrastructure, not a producer obligation)

The previous design implemented a durable, locator-only memory notification spool at `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/` to record work missed by final-hook ordering. Its mechanics below remain implemented, but requiring producers to populate it is superseded. Adam-owned discovery/adapters may use such a queue internally; an external spool writer is not required for native ingestion.

Existing spool entries contain only:

- notification version and ID;
- source kind and source session ID;
- producer ID;
- sidecar locator hash or canonical relative locator;
- queued timestamp.

It contains no memory text, transcript content, credentials, or model output. Notifications use create/fsync/rename/directory-fsync durability and remain until Adam acknowledges successful or terminally classified processing.

The spool can still be drained by Pi startup/lifecycle, the Claude reconciliation worker, or explicit repair. Existing spool acceptance remains useful infrastructure evidence, but is not proof of producer independence. Native adapter acceptance must demonstrate rediscovery and convergence with **no producer-written Adam notification and no producer-started Adam worker**.

This spool is a public file protocol, not reuse of Claude's private hook payload. Adam therefore drains two deliberately separate queues:

- `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/` retains the implemented 0.3 Claude lifecycle notifications that locate authoritative parent and subagent transcripts;
- `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/` contains protocol-v1 producer notifications that locate authoritative memory sidecars.

They have different schemas, authorities, coalescing keys, and acknowledgement conditions even if one worker lease eventually drains both. A transcript notification is acknowledged after host-source synchronization and evidence projection; a memory notification is normally acknowledged only after the named sidecar prefix is mirrored and included in an aggregate memory projection. Missing, unsafe, or non-regular sidecars are notification-terminal exceptions: Adam logs and acknowledges that notification without marking the stream conflicted, preserving prior mirrored state and allowing a later notification to ingest a repaired source. The existing Claude inbox is not exposed as a producer API, and migrating its location is deferred.

### Worker behavior

One Adam lease serializes memory-stream graph writes. The worker:

1. validates and coalesces notifications by source session and producer;
2. for each source session, drains pending transcript notifications and ensures the source-scoped `AdamSession` exists before its memory notifications; a missing source session remains transient within the configured 10-minute default wait window, then positively confirmed absence durably parks/coalesces those locators before active acknowledgement as `source-never-mirrored`;
3. mirrors the relevant sidecars independently;
4. loads retained records for absent known streams;
5. assembles one aggregate session projection;
6. acknowledges notifications covered by successful processing, plus notification-terminal missing or unsafe sources after logging them;
7. continues healthy groups when another stream fails;
8. retries transient failures with capped backoff.

A malformed supported record is a bounded adapter diagnostic and does not poison source-session replication. Prefix mutation physically stops only that memory stream. Immutable-event conflict stops semantic advancement while later raw suffix records continue mirroring as blocked; either case preserves the last committed projection. Missing or unsafe source files never create a permanent graph conflict, so repaired sidecars remain ingestible on a later notification.

Both Pi and Claude composition roots use the implemented host-neutral memory reconciliation service. Any owned source session can own a protocol sidecar. The existing XDG-config worker lease serializes both notification queues, not Pi's own mirroring/evidence projection or historical imports. Pi source writes proceed even when the drain lease is busy; the worker also releases it during retry backoff. `/adam:reconcile` and `worker.js --once` provide explicit repair without another producer append. Memory-only aggregate repair uses retained graph source records and `TOUCHES` evidence rather than requiring local files. Never-mirrored-source expiry uses a restart-stable local-file-mtime age window (`ADAM_MEMORY_SOURCE_WAIT_MS`); backend/projection failures are never expired by this policy. Expired locators are durably coalesced under private `memory-inbox/expired/`. Only explicit repair checks source ownership and requeues them, so late source mirroring needs no producer append/notification while ordinary drains and detached idle/retry checks never enumerate the archive. See [`memory-notification-contract.md`](memory-notification-contract.md).

## Stage 4: standalone reference Claude producer

The reference producer is a separate package and repository. It must generate, persist, list, and recall its memories with Adam entirely absent. It owns its native versioned JSONL format, documented data location, model configuration, progress/replay, and user-facing memory IDs. It has no Neo4j access and no knowledge of Adam's package, identity, protocol paths, notification spool, worker executable, or installation. Adam alone owns the adapter that consumes it.

### Non-blocking lifecycle

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks only validate bounded locators, enqueue producer work durably, wake a detached producer worker, and return. They never call a model or wait for sidecar reconciliation.

One producer worker:

- maintains explicit parent and subagent locators;
- scans explicitly located source prefixes and reconstructs selected Claude context, including compaction, parallel tools, and subagents; Adam's characterization is evidence, not a producer runtime dependency;
- records native source citations and per-record context, without relying on Adam to retain or recall them;
- calls a configured model backend outside Claude's interactive lifecycle;
- writes native memory/coverage events under its own writer lease;
- derives committed source progress from its own memory log;
- exposes independent memory listing/recall;
- retries outages without duplicating committed logical work.

There is no post-commit Adam notification or worker wake step. Only the producer's own worker is started by its hooks.

Native `Read`, `Edit`, and `Write` activity may be cited. Bash commands, MCP calls, compact-summary prose, subagent handback prose, and path-looking text do not become file evidence merely because the producer mentions them.

### Producer characterization before implementation

The initial characterization is recorded in [`claude-memory-producer-contract.md`](claude-memory-producer-contract.md). Fast hooks, detached serialized execution, bounded structured output, source-progress scheduling, durable no-memory coverage, and explicit background-processing consent remain applicable. The producer defaults to an explicit-credential adapter unless inherited-login background use is confirmed to comply with applicable subscription terms. Its native schema/path and standalone listing/recall surface must be specified in its own repository before implementation; Adam's adapter follows that contract, not vice versa.

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
3. With Adam absent (no package, environment, config, spool, or worker), the producer saves, lists, recalls, and restarts from its native log without duplicate generation.
4. After independently enabling Adam, its adapter discovers and losslessly mirrors the existing native log and source streams, then projects one aggregate session snapshot. No export or new producer append is required.
5. Claude's `adam_file_context` returns the Claude-produced memory with source, stream, revision, and producer provenance.
6. Pi's `/adam:context` returns the same memory through the same canonical file identity.
7. Reverse hook completion order: the producer commits after Adam stops; the next Adam startup/lifecycle or explicit repair discovers that commit without a notification or producer-started Adam process.
8. A producer crash immediately after native-log append does not create a duplicate logical event on retry.
9. Adam/Neo4j absence or outage does not change producer generation, persistence, listing, recall, or hook latency.

## Deferred

- Semantic or global search.
- Automatic retrieval or prompt insertion.
- Model-assisted file association.
- Cross-producer deduplication, support links, consolidation, or dropping.
- Restoring host transcripts or producer sidecars from Neo4j.
- Treating undocumented host storage layouts as discovery APIs.
- A continuously running Adam daemon or default polling cadence; the initial native-reader guarantee is next Adam reconciliation, with any low-latency polling owned separately by Adam.
- Migration of Adam's existing 0.3 config/inbox/log paths to XDG data/state directories.

## Delivery sequence

The scope is one 0.4.0 milestone, not one implementation PR. Track each numbered increment with a repository issue and deliver it through one or more small, independently reviewable PRs. Intermediate PRs land on `main` without changing the released package version. Only the final release-preparation PR bumps the package and plugin versions to 0.4.0, finalizes the changelog, rebuilds all committed runtimes, and activates the protected-main tag/release pipeline.

1. **Complete:** characterize the reference Claude producer and record the non-blocking generation/privacy contract in [`claude-memory-producer-contract.md`](claude-memory-producer-contract.md).
2. **Complete:** commit [`memory-protocol-contract.md`](memory-protocol-contract.md) with fixtures for valid events, replay and JCS edge canonicalization, malformed complete records, incomplete tails, source-checkpoint regressions, unresolved citations, tombstones, duplicate IDs, prefix changes, and source/producer mismatch.
3. **Complete:** implement lossless memory-stream scanning, raw-record storage, checkpoints, and live round-trip tests.
4. **Complete:** implement the graph-native producer-scoped memory-identity migration without changing session, stream, or entry identities.
5. **Complete:** implement aggregate multi-producer projection and retrieval provenance.
6. **Complete infrastructure; producer obligation superseded:** distinct memory spool, shared reconciliation, bounded absent-source expiry, and parked repair. Runtime remains unchanged by this plan correction.
7. Deliver small reviewable increments: (a) specify/build the standalone producer's native format, generation, replay and independent listing/recall; (b) implement Adam-owned native log discovery, lossless reader and structural adapter; (c) prove standalone and cross-host acceptance without any producer awareness of Adam. Low-latency polling, if desired, is a separate Adam-owned scheduling decision.
8. Prepare and merge the sole 0.4.0 release PR through the protected-main flow, then cut over Pi and Claude installations to the same tagged artifact.

## 0.4.0 acceptance

- The producer generates, persists, lists and recalls native memories with Adam absent, and restarts safely with no Adam configuration, protocol, notifications, or worker invocation.
- Adam's adapter reads the existing native log without modifying it, requiring an export, or requiring a fresh producer event.
- Every complete sidecar record, including unknown and malformed records, is mirrored byte-for-byte with restart-safe append checkpoints.
- Multiple producers for one session survive aggregate rebuilds without deleting or overwriting one another.
- A missing or conflicted producer stream preserves its last committed raw replica and derived contribution while healthy streams continue.
- Pi and Claude memories about the same canonical file are returned together from both hosts with user-isolated source, stream, revision, and producer provenance.
- Records citing not-yet-mirrored entries converge after later source reconciliation.
- Reversed final-hook ordering converges through Adam-owned discovery at the next reconciliation, without a producer-written notification or producer-started Adam worker.
- Producer retries across the append/checkpoint crash window do not duplicate logical events.
- Existing Pi memories migrate transactionally to producer-scoped identities, including remote-only graph state, tombstones, support links, and file provenance.
- Malformed records, model outages, Neo4j outages, and one stream's conflict do not block host operation or unrelated reconciliation.
- Claude lifecycle hooks remain bounded and do not wait for model generation or Neo4j.
- The reference Claude producer completes the tracer bullet while Adam itself still creates no observation or reflection.
