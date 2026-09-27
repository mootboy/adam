# Adam 0.4.0 plan: provider-neutral memory production

Status: draft for review

Adam 0.3 made Claude Code a second host: its transcripts are mirrored, its native file evidence converges on canonical files, and it can retrieve memories that Pi produced. What Claude cannot do is produce memories. Adam stays a transport and index, so the missing piece is a contract that lets any producer, on any host, hand adam observations and reflections that adam can link to sessions, entries, and files with the same provenance it gives `pi-observational-memory` today.

## Why now

- Every `adam_file_context` call from Claude returns only Pi-era memories; Claude conversations leave entries and file evidence behind but no memory. Issue 006's cutover evidence shows the gap directly: `.mcp.json` carries Claude `TOUCHES` edges and only Pi observations.
- The Pi adapter already proves the shape adam needs: recorded observations, recorded reflections, dropped tombstones, source-entry provenance, and support links, all parsed structurally from persisted records without importing the producer.
- The 0.3 worker, inbox, byte checkpoints, and source-scoped identities give a second producer everything except a place to write and a way to be found.

## Target architecture

```text
  Pi session JSONL                 Claude transcript JSONL          memory sidecar JSONL
  (host-owned, authoritative)      (host-owned, authoritative)      (producer-owned, append-only)
          │                                │                                │
          │ scan + checkpoint              │ scan + checkpoint              │ scan + checkpoint
          ▼                                ▼                                ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │ adam                                                                              │
  │  sessions · entries · streams · file evidence · memory records (protocol v1)      │
  │  identity: user → source kind → source session → producer → memory id            │
  │  projection: (AdamObservation)-[:SOURCED_FROM]->(AdamEntry)-[:TOUCHES]->(AdamCodeFile)│
  └───────────────────────────────────────────────────────────────────────────────────┘
          ▲                                ▲
          │ /adam:context                  │ adam_file_context
       Pi user                          Claude user
```

Producers never call adam and adam never calls producers. A producer persists memory records; adam discovers and adapts them. For Pi the persisted place stays the session JSONL. For Claude, whose transcript is not writable by third parties, the persisted place is a sidecar file that follows the same rules as a transcript.

## Stage 1: memory record protocol v1 (contract first)

Write `docs/memory-protocol-contract.md` before any code, mirroring how 0.3 characterized Claude transcripts before ingesting them.

### Record shape

One JSON object per line, append-only, UTF-8, newline-terminated. Structurally the existing `om.*` payloads, lifted out of Pi's custom-entry envelope and given explicit source and producer fields:

```json
{"version": 1,
 "kind": "observations.recorded",
 "producer": {"name": "claude-observational-memory", "version": "0.1.0"},
 "source": {"kind": "claude-code", "sessionId": "6fbc25a9-…", "streamId": "main"},
 "coversUpToEntryId": "d0b1…",
 "recordedAt": "2026-09-28T09:12:44.120Z",
 "observations": [
   {"id": "70f6600ddded", "content": "…", "timestamp": "…", "relevance": "high",
    "sourceEntryIds": ["…"], "tokenCount": 123}]}
```

`kind` is one of `observations.recorded`, `reflections.recorded`, `observations.dropped`, with the item schemas adam already validates for Pi. `sourceEntryIds` and `coversUpToEntryId` are the host's own entry identifiers (Pi entry ids, Claude `uuid`s), which is what lets adam link a memory to mirrored entries and, through their `TOUCHES` evidence, to canonical files.

### Location and discovery

```text
${XDG_CONFIG_HOME:-~/.config}/adam/memories/<sourceKind>/<sourceSessionId>/<producerName>.jsonl
```

Owner-only, one file per producer per source session, appended atomically per record. Discovery is by convention, not IPC: whenever the worker reconciles a session it also scans that session's sidecar directory. A producer that wants prompt indexing can enqueue an ordinary locator notification through the existing hook boundary; nothing new is required for correctness because the next lifecycle hook reconciles the sidecar anyway.

Pi keeps writing into its session JSONL. The Pi adapter is re-expressed as protocol v1 over custom entries so both paths share one validator and one projection.

### Identity and provenance

Today: `urn:adam:observation:<user>:<sourceKind>:<sourceSessionId>:<memoryId>`. Two producers observing one session may reuse a 12-hex memory id, so 0.4.0 adds the producer:

```text
urn:adam:observation:<user>:<sourceKind>:<sourceSessionId>:<producerName>:<memoryId>
```

This is a versioned, transactional identity migration of the kind 0.3 already performs for sessions, applied to observation and reflection ids, `SUPPORTED_BY` links, and dropped tombstones. Decision to confirm before implementation: migrate (recommended, one scheme) versus grandfather Pi ids (no migration, two schemes forever).

Every projected memory records `producer.name`, `producer.version`, protocol version, adapter version, source kind, recording locator, and the mirrored entries it cites. Rendering in `/adam:context` and `adam_file_context` shows the producer so a reader can weigh a Claude-side summary differently from a Pi-side one.

### Ordering and failure semantics

- A record citing entries adam has not mirrored yet is retained, not dropped; the projection retries on the next reconciliation of that session, exactly like event-order reversal in Pi.
- Malformed records are isolated with bounded diagnostics and never affect session replication.
- Sidecars use the transcript checkpoint model: byte offset, prefix hash, shrink and rewrite detection. A rewritten sidecar is a conflict, not a silent reindex.
- Tombstones (`observations.dropped`) mark memories dropped; adam never deletes producer output.

## Stage 2: adam ingestion

- Sidecar scanner reusing the Claude stream scanner's framing and checkpoints, with its own `AdamMemoryStream` checkpoint node per sidecar.
- Protocol v1 validator and adapter generalized from `sources/pi_observational_memory`; the Pi adapter becomes a thin envelope reader over it.
- Projection through the existing knowledge store: `AdamObservation`/`AdamReflection` nodes, `SOURCED_FROM` to entries, `ABOUT` to files, `SUPPORTED_BY` between reflections and observations, dropped state.
- Identity migration per Stage 1.
- Worker: reconcile sidecars in the same pass as the session, under the same lease, with the 0.3.2 per-stream failure isolation.
- `/adam:status` reports memory-stream counts and the latest sidecar reconciliation.

Deterministic tests cover validation, checkpoints, unresolved citations, tombstones, identity migration, and mixed-producer isolation; the live suite proves outage-to-repair for a sidecar.

## Stage 3: reference Claude producer (separate package)

Adam does not generate observations, so the producer lives outside this repository, in the way `pi-observational-memory` does. The reference producer is a small Claude Code plugin:

- a `Stop` hook that reads the transcript delta since its last checkpoint (the same byte-offset model), asks a model for observations in the protocol's item schema, and appends a protocol v1 record to the sidecar;
- a periodic reflection pass over its own observations;
- no Neo4j access, no adam import, no prompt injection.

Its only coupling to adam is the file format and location in Stage 1. The tracer bullet is the 0.3 side-by-side proof again, on Claude: native `Read` of a file, an observation recorded about it, adam reconciling both, `adam_file_context` returning the Claude-produced memory with producer provenance, and Pi's `/adam:context` seeing the same memory on the same canonical file.

## Deferred

- Semantic or global search, automatic prompt injection, model-assisted file association.
- Cross-producer deduplication or consolidation; each producer owns its own drops.
- Restoring Claude transcripts or sidecars from Neo4j.
- A push notification API for producers beyond the existing locator hook.
- Treating any host's undocumented storage layout as a stable discovery API.

## Recommended sequence

1. Issue: characterize a Claude-side producer (what a `Stop` hook can see, transcript delta cost, model cost per turn), the way issue 002 characterized transcripts.
2. Issue: `docs/memory-protocol-contract.md`, the identity decision, and fixtures for valid, malformed, unresolved-citation, and tombstone records.
3. Issue: sidecar scanner, protocol adapter, projection, migration, worker integration, status.
4. Issue: reference producer repository and the Claude tracer bullet.
5. Release 0.4.0 through the existing protected-main flow; cut over both hosts and record evidence.

## 0.4.0 acceptance

- A protocol v1 sidecar written by an independent producer is discovered, checkpointed, validated, and projected without adam importing the producer.
- Pi and Claude memories about the same canonical file are retrieved together from both hosts, each carrying producer and source provenance.
- Records citing not-yet-mirrored entries converge after later reconciliation; malformed records never affect session replication.
- Observation and reflection identities are producer-scoped under one versioned migration, or the grandfathering decision is recorded instead.
- The reference Claude producer completes the side-by-side tracer bullet, and adam still creates no observation or reflection of its own.
