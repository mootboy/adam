# Adam memory protocol v1

Status: normative contract; ingestion implementation is deferred to later Adam 0.4 increments.

This document defines the file protocol through which an independent memory producer publishes observations, reflections, tombstones, and content-free source coverage for Adam. Producers do not import Adam, modify host transcripts, or write Neo4j. Adam remains transport, provenance, indexing, and retrieval infrastructure; it does not decide what should become memory.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

## Authority and scope

A producer memory sidecar is the authoritative append-only log for that producer's memories about one source session. Host session/transcript files remain separately authoritative for source entries. Adam mirrors both authorities losslessly and joins them only in rebuildable derived projection.

Protocol v1 covers:

- sidecar identity and safe filesystem placement;
- physical JSONL framing and append durability;
- event envelopes and four supported event kinds;
- producer replay and content-free coverage;
- immutable event and memory semantics;
- stream-qualified source citations and checkpoints;
- bounded validation and diagnostics.

It does not define model prompts, provider credentials, notification-spool delivery, Neo4j schema, retrieval rendering, or producer scheduling thresholds. The reference producer contract requires explicit provider credentials by default unless inherited-login background use is confirmed separately.

## Canonical sidecar identity and location

A logical stream is identified by this tuple:

```text
(sourceKind, sourceSessionId, producerId)
```

There is exactly one logical sidecar for that tuple. Its canonical path is:

```text
${XDG_DATA_HOME:-~/.local/share}/adam/memories/v1/
  <sourceKind>/<sha256(UTF-8(sourceSessionId))>/<producerId>.jsonl
```

The hash is lowercase hexadecimal over the exact UTF-8 bytes; no Unicode normalization is applied. The unhashed source session ID remains in every event and MUST match the locator tuple. Hashing is path safety, not identity. When set, `XDG_DATA_HOME` and `XDG_STATE_HOME` MUST be absolute; otherwise the documented home-directory defaults apply.

`sourceKind` and `producerId` use the protocol-identifier grammar:

```text
^[a-z0-9](?:[a-z0-9._-]{0,127})$
```

Known initial source kinds are `pi` and `claude-code`. `producerId` is permanent identity, not a display name or package version. Renaming or upgrading a package MUST NOT change it.

`sourceSessionId` is a non-empty UTF-8 string of at most 512 bytes with no C0 or DEL control characters. It is compared exactly and is never interpreted as a path.

Every complete event's `source.kind`, `source.sessionId`, and `producer.id` MUST equal the locator tuple. A mismatch is retained as a raw record with a bounded `source-mismatch` or `producer-mismatch` diagnostic and has no semantic effect.

## Filesystem safety and writer ownership

Authoritative memory content belongs under XDG data storage. Disposable coordination state belongs under XDG state storage:

```text
${XDG_STATE_HOME:-~/.local/state}/adam/memory-writers/v1/
  <sourceKind>/<sha256(UTF-8(sourceSessionId))>/<producerId>.lock
```

A conforming writer MUST:

- create directories with mode `0700` and sidecars with mode `0600` (or fail closed when the platform cannot enforce an equivalent owner-only policy);
- reject a symlink or non-directory component beneath the selected XDG root and reject a symlink or non-regular sidecar;
- acquire one exclusive writer lease for the locator tuple before appending;
- recover a stale lease without allowing two live writers;
- encode exactly one event plus LF before one append operation;
- reject an encoded record larger than 1 MiB, excluding its terminal LF;
- flush the sidecar after append and fsync the containing directory after durable creation;
- release the lease only after append durability succeeds.

A writer MUST NOT depend on concurrent regular-file appends preserving record boundaries. Hooks enqueue work; one producer worker owns sidecar writes.

## Physical JSONL framing

A sidecar is an append-only UTF-8 JSONL file.

- LF (`0x0a`) commits a physical record.
- The exact bytes before LF are the record payload. A preceding CR, if present, is payload whitespace and is preserved.
- Empty completed lines are malformed records, not separators.
- A trailing sequence without LF is an incomplete active append, even if it happens to parse as JSON. Adam defers it and does not checkpoint or mirror it until LF appears.
- A completed payload MUST be at most 1 MiB and valid UTF-8. Oversize or invalid UTF-8 stops synchronization before that record and marks the stream conflicted.
- Completed valid UTF-8 that is malformed JSON is still mirrored exactly, receives `malformed-json`, and has no semantic effect. Later independently framed records remain eligible.
- JSON values other than an object and objects with duplicate member names are structurally malformed protocol records. They are retained but not interpreted.
- Unknown fields are retained. For supported v1 events they participate in immutable-payload comparison.

Adam's lossless record stores exact decoded `rawJson` excluding LF, plus ordinal and byte boundaries. Raw mirroring precedes any derived projection. A malformed, unsupported, or semantically invalid record never causes Adam to rewrite or delete producer output.

## Common limits and identifiers

Limits are measured after UTF-8 encoding where this contract says bytes and as array counts otherwise.

| Value | Rule |
| --- | --- |
| physical record | at most 1 MiB excluding LF |
| event ID | `^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,255})$` |
| memory ID | exactly 12 lowercase hexadecimal characters |
| producer version | non-empty UTF-8 string, at most 128 bytes |
| stream or entry ID | non-empty UTF-8 string, at most 512 bytes, no C0/DEL controls |
| content | non-empty string, at most 65,536 UTF-8 bytes |
| event item arrays | 1–256 items |
| checkpoint streams | 1–256 unique stream IDs |
| token count | integer from 0 through `9007199254740991` |
| timestamp | RFC 3339 UTC instant ending in `Z`, at most 64 bytes |

Arrays documented as unique MUST not contain duplicate IDs or duplicate `(streamId, entryId)` pairs.

## Event envelope

Every supported line is one JSON object with these required fields:

```json
{
  "protocolVersion": 1,
  "eventId": "01JQ7M6N0FK3AH1X7N6DK0D8YZ",
  "kind": "source.covered",
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
        "prefixSha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "selectedLeafEntryId": "d0b1"
      }
    ]
  },
  "recordedAt": "2026-09-30T12:00:00.000Z"
}
```

Field semantics:

| Field | Meaning |
| --- | --- |
| `protocolVersion` | integer `1` |
| `eventId` | immutable identity within this sidecar |
| `kind` | one of the four v1 kinds below |
| `producer.id` | permanent producer identity matching the path |
| `producer.version` | emitting implementation provenance, not identity |
| `source.kind` / `source.sessionId` | source identity matching the path |
| `sourceCheckpoint` | complete producer-observed source coverage snapshot |
| `recordedAt` | event creation time, never ordering authority |

Physical ordinal is the authoritative event order. Timestamps MUST NOT reorder events.

A record with an unsupported `protocolVersion` or unknown `kind` is mirrored as raw data with an `unsupported-version` or `unsupported-kind` diagnostic. It does not affect memories or committed producer coverage as understood by Adam v1.

The informative machine-readable schema is [`fixtures/memory-protocol-v1/event.schema.json`](fixtures/memory-protocol-v1/event.schema.json). This document is normative when filesystem, byte-level, ordering, or cross-record rules cannot be expressed in JSON Schema.

## Source checkpoints

`sourceCheckpoint.streams` is a full snapshot of all source streams known to the producer at event creation. Entries MUST be unique and sorted by the UTF-8 byte sequence of `streamId`.

Each stream contains:

- `streamId`: the host stream identity (`main` for a single main stream; explicit child IDs for subagents);
- `committedBytes`: source bytes the producer treated as complete;
- `prefixSha256`: SHA-256 of exactly those source bytes;
- `selectedLeafEntryId`: the selected source leaf used for context, or `null` when the host has none.

A producer folding accepted events carries this full snapshot forward. Once present, a stream MUST NOT disappear from later snapshots. `committedBytes` MUST NOT decrease. Equal byte offsets MUST retain the same prefix hash. A selected leaf MAY change as host context changes.

`sourceCheckpoint` has two roles only:

1. provenance describing what the producer considered; and
2. replay authority for that producer when folded from its own sidecar.

It is never Adam's host-transcript checkpoint. Adam validates shape, monotonicity, and cited stream identity but MUST NOT compare producer offsets or hashes with Adam's independently observed transcript checkpoints. Source and memory synchronization can legitimately observe different moments.

Entry citations may remain unresolved until Adam mirrors the corresponding source record. Unresolved citations do not invalidate an event or advance Adam's host checkpoint.

## Event kinds

### `observations.recorded`

Requires `observations`, an array of 1–256 objects:

```json
{
  "id": "70f6600ddded",
  "content": "The memory protocol is producer-neutral.",
  "timestamp": "2026-09-30T12:00:00.000Z",
  "relevance": "high",
  "sourceEntries": [
    { "streamId": "main", "entryId": "d0b1" }
  ],
  "tokenCount": 7
}
```

Observation IDs MUST be unique within the event. `relevance` is `low`, `medium`, `high`, or `critical`. `sourceEntries` contains 1–256 unique stream-qualified citations in the same source session, and every cited `streamId` MUST appear in the event's source checkpoint. Observation content may contain newlines.

The event MUST NOT also contain `reflections` or `observationIds`.

### `reflections.recorded`

Requires `reflections`, an array of 1–256 objects:

```json
{
  "id": "bd7229882294",
  "content": "Producers own generation while Adam owns transport and indexing.",
  "supportingObservationIds": ["70f6600ddded"],
  "tokenCount": 10
}
```

Reflection IDs MUST be unique within the event. Reflection content is one non-empty line. `supportingObservationIds` contains 1–256 unique observation IDs from the same producer and source session. References may be unresolved when recorded.

The event MUST NOT also contain `observations` or `observationIds`.

### `observations.dropped`

Requires `observationIds`, an array of 1–256 unique observation IDs from the same producer and source session. A tombstone may precede the observation definition and remains effective if that definition later arrives.

The event MUST NOT also contain `observations` or `reflections`.

### `source.covered`

`source.covered` commits a valid no-memory outcome. It carries only the common envelope and `sourceCheckpoint`; it MUST NOT contain `observations`, `reflections`, or `observationIds`.

This event exists so a producer that intentionally emits no memory can durably advance its own source coverage. After restart, folding the sidecar prevents paying repeatedly for the same declined range. It creates no observation, reflection, tombstone, or retrieval result.

A producer MUST append `source.covered` only after successful processing of the represented source snapshot. Model/provider failures and cancelled generation MUST NOT be recorded as coverage.

## Canonical payload and event replay

The immutable event payload is the entire parsed JSON object, including unknown fields. Its canonical byte representation is RFC 8785 JSON Canonicalization Scheme (JCS), encoded as UTF-8. The immutable payload hash is SHA-256 of those bytes.

Within one sidecar:

- the first structurally valid occurrence of an `eventId` establishes its immutable payload;
- a later occurrence with the same canonical payload hash is an `idempotent-replay`; its raw physical record is mirrored, but semantic effects apply once;
- a later occurrence with a different canonical payload hash is an `immutable-event-conflict`; its raw record is mirrored, the stream is marked conflicted, and no later semantic state is accepted automatically;
- key order and insignificant JSON whitespace do not change canonical payload identity.

Producers SHOULD avoid appending duplicate events, but consumers MUST implement these replay rules.

## Memory fold semantics

Semantic folding uses accepted events in physical ordinal order.

For each producer/source session:

- the first valid definition of an observation ID wins;
- the first valid definition of a reflection ID wins;
- a byte-equivalent canonical redefinition is an idempotent duplicate;
- an incompatible redefinition produces `immutable-memory-conflict`; the first definition remains active and unrelated valid items in the later event may still apply;
- tombstones form a durable set and apply regardless of whether they precede or follow an observation definition;
- a tombstoned observation remains retained with provenance but is inactive for observation retrieval;
- reflections remain valid when supporting observations are tombstoned;
- support IDs and tombstones cannot cross producer or source-session scope;
- cross-producer deduplication, support, dropping, and consolidation do not exist in v1.

An unresolved source entry or supporting observation produces `unresolved-reference`, not a conflict. Adam retains the normalized memory and retries relationship construction after later source or producer reconciliation.

## Sidecar checkpoints and prefix conflicts

Adam's memory-sidecar synchronization checkpoint is separate from every event's `sourceCheckpoint`. It contains:

- complete physical-record ordinal;
- committed sidecar byte offset;
- SHA-256 of the committed sidecar prefix.

Before suffix synchronization, Adam MUST verify that the file is at least the committed size and that the committed prefix hash is unchanged.

- a shorter file is `checkpoint-out-of-range`;
- a same-length or longer file with a changed committed prefix is `committed-prefix-mismatch`;
- either condition marks only that memory stream conflicted and stops automatic suffix writes;
- an incomplete tail does not enter the checkpoint;
- a malformed completed record does enter the lossless checkpoint after its raw record and diagnostic commit.

Synchronization batches are bounded by raw encoded bytes. A valid single record larger than the target batch but within the 1 MiB protocol maximum is committed alone.

## Crash consistency

The sidecar is the producer's commit log. Disposable producer checkpoints may optimize scanning but MUST NOT claim progress beyond the latest accepted sidecar event.

| Crash point | Required recovery |
| --- | --- |
| before generation completes | retry may regenerate; no coverage committed |
| after generation but before append | retry may regenerate; no event committed |
| during append before LF/fsync | incomplete tail is deferred; writer recovery must finish or seal it as malformed before another event |
| after durable append before producer acknowledgement | fold the event and do not create a second logical result |
| after durable append before Adam notification | rewrite the idempotent locator-only notification; do not rewrite the event |
| after `source.covered` append | fold its checkpoint and do not pay again for that covered range |

Only the producer writer may recover its incomplete tail. If it has durable private state containing the exact intended remaining bytes, it may append those bytes and LF. Otherwise it MUST append LF to seal the partial bytes as one malformed completed record before appending a freshly generated event. It MUST NOT truncate or overwrite the sidecar. Adam only defers the tail and never performs producer recovery.

A producer can generate a fresh event ID after an uncommitted crash. Once an event is durable, its identity and payload never change.

## Validation classes and failure isolation

Protocol processing distinguishes:

- **physical conflict**: invalid UTF-8, oversize record, file shrinkage, or committed-prefix mutation; stop this stream;
- **stream identity conflict**: locator mismatch or changed payload for an existing event ID; retain raw data and stop semantic advancement for this stream;
- **record diagnostic**: malformed JSON, unsupported version/kind, malformed supported shape, duplicate memory definition, or unresolved reference; retain raw data and continue when framing remains safe;
- **incomplete append**: defer bytes after the final LF without a diagnostic until completed.

One memory stream's failure MUST NOT invalidate its source session, file evidence, another producer stream, or host operation. A missing sidecar preserves the last mirrored prefix and derived contribution; local deletion is not propagated.

Diagnostics may contain locator identity, event or memory IDs, ordinals, offsets, hashes, and reason classes. They MUST NOT contain memory text, source transcript content, credentials, or raw JSON.

## Pi normalization

Existing `pi-observational-memory` custom session entries remain authoritative inside Pi session JSONL and are not rewritten into sidecars. Adam's trusted structural adapter normalizes supported `om.observations.recorded`, `om.reflections.recorded`, and `om.observations.dropped` entries into this event model.

The adapter supplies:

- source kind `pi` and the source-scoped Pi session ID;
- producer ID `pi-observational-memory`;
- one implicit stream for Pi entry citations;
- adapter-derived recording provenance;
- a deterministic synthetic `eventId` derived from the source-scoped custom-entry identity.

A legacy Pi entry does not literally gain a sidecar `sourceCheckpoint`. Its existing coverage fields remain adapter provenance. This distinction must remain visible in normalized diagnostics and migration logic.

## Fixtures and conformance

Normative examples live under [`fixtures/memory-protocol-v1/`](fixtures/memory-protocol-v1/). [`manifest.json`](fixtures/memory-protocol-v1/manifest.json) names the canonical locator and expected outcome for:

- all four valid event kinds;
- canonical idempotent replay and conflicting event reuse;
- malformed completed JSON and an incomplete tail;
- unresolved entry/support citations;
- a tombstone before definition;
- conflicting memory definitions;
- committed-prefix mutation and shrinkage;
- source and producer mismatch.

`npm test` executes deterministic fixture checks without Neo4j, model access, or host transcripts. Later scanner and storage increments MUST consume the same fixtures rather than replace them with implementation-specific examples.

## Privacy and non-goals

Sidecars contain user memory and source identifiers and MUST be treated as private user data. They MUST NOT contain provider credentials. Their location MUST NOT be exposed in normal retrieval output.

Protocol v1 does not provide:

- automatic prompt insertion or retrieval;
- semantic/global search;
- cross-producer memory operations;
- file association from memory prose;
- model invocation or credential policy;
- host transcript or sidecar restoration;
- sidecar deletion propagation;
- multiple writers for one producer/session stream;
- compatibility aliases for legacy APIOM names or graph state.
