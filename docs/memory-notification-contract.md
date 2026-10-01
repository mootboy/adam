# Producer memory notifications v1

Status: implementation in progress (Adam 0.4 delivery step 6). The spool and storage-neutral drain are implemented; host composition, serialized worker integration, explicit repair, and live end-to-end acceptance remain pending. This is not yet a shipped producer API.

## Authority and location

Notifications are durable discovery hints, not memory authority. The producer sidecar remains authoritative under `docs/memory-protocol-contract.md`. Notification loss cannot invalidate an already committed sidecar; producers can enqueue a fresh notification for the same locator.

The public queue is `${XDG_STATE_HOME:-~/.local/state}/adam/memory-inbox/`. It is distinct from the private Claude lifecycle queue under XDG config. XDG roots must be absolute. Every Adam directory beneath the state root is owner-only (`0700`) and cannot be a symlink. Notifications are owner-only regular files (`0600`), at most 4096 UTF-8 bytes. The initial filesystem contract is POSIX Linux/macOS.

## Envelope

The filename is `<id>.json`. The exact JSON object has no additional fields:

```json
{
  "version": 1,
  "id": "a3949e52-42c5-4d09-a2ca-4b6ae1c9dd27",
  "sourceKind": "claude-code",
  "sourceSessionId": "session-123",
  "producerId": "org.example.memory",
  "sidecarLocator": "claude-code/b9c84322f82434cb46e239d20daf1f3714eeb5077f87fb0f0cd4bd336bc01b54/org.example.memory.jsonl",
  "queuedAt": "2026-10-01T12:00:00.000Z"
}
```

`id` is a lowercase UUID and must match the filename. `queuedAt` is a valid UTC ISO timestamp including milliseconds. Source and producer identifiers obey memory protocol v1 bounds, measured in UTF-8 bytes.

`sidecarLocator` is recomputed, not trusted as an arbitrary path: `<sourceKind>/<sha256(exact UTF-8 sourceSessionId)>/<producerId>.jsonl`. It resolves only beneath `${XDG_DATA_HOME:-~/.local/share}/adam/memories/v1/`. Neither absolute paths nor alternate relative paths are accepted. Implementations always compute the hash from the exact source session ID bytes.

No transcript locators, memory text, model output, credentials, or additional metadata are permitted. A producer atomically writes the envelope with a fresh UUID after its sidecar append is durable. Enqueue uses exclusive temporary creation, file fsync, rename, and containing-directory fsync. The notification stays present until acknowledgement durably unlinks it.

## Drain semantics

One serialized Adam worker owns processing. The storage-neutral drain receives callbacks to:

1. establish the owned source session and reconcile pending transcript work;
2. synchronize each independently coalesced producer sidecar;
3. rebuild one aggregate projection for the source session from all retained mirrored streams.

Callbacks execute serially; synchronous errors and promise rejections share the retry path. Notifications arriving during a drain are not included in that drain's acknowledgement snapshot. Duplicate locators coalesce per source session and producer, while acknowledgement removes each original notification individually.

Successful raw synchronization, including a diagnosed physical or semantic conflict with retained safe prefix, still requires successful aggregate projection before acknowledgement. Transient sidecar failures leave only that producer's notifications pending; healthy producers in the same session may still project and acknowledge. Source-session failure leaves all that session's notifications pending without blocking other sessions. Projection failure retains every successfully synchronized notification in the group for a restart-safe idempotent retry.

Missing or unsafe sidecars use the step-3 notification-terminal disposition: log a content-free reason and acknowledge without reading the source or changing graph state. No projection is required for that rejected notification alone. A later fresh notification can ingest a repaired sidecar.

Malformed/unsafe notifications are diagnosed independently of healthy notifications. Regular rejected entries and symlinks can be unlinked without following them; unsafe directory entries are not recursively removed and do not block healthy work. Diagnostics contain classification and source identity only, never raw envelope contents or error strings that may contain credentials.

## Pending acceptance

The production worker must hold an exclusive recoverable lease, retry pending transient work with capped backoff, and recheck both queues after lease release to avoid a wake/shutdown race. Pi and Claude entry points must use the same reconciliation service; hooks must never wait for Neo4j. The explicit repair path must process notifications even when no further producer append or host turn occurs. Reversed hook completion, worker restart, outage repair, missing source ownership, and multi-producer failure isolation require deterministic and live coverage before this contract is marked implemented.
