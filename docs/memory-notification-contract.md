# Producer memory notifications v1

Status: implemented on the unreleased Adam 0.4 development line (delivery step 6). The public spool, shared serialized worker, Pi/Claude composition, explicit repair paths, and deterministic/live acceptance are implemented. The separate reference producer and 0.4 release remain later increments.

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

Successful raw synchronization, including a diagnosed physical or semantic conflict with retained safe prefix, still requires successful aggregate projection before acknowledgement. Transient sidecar failures leave only that producer's notifications pending; healthy producers in the same session may still project and acknowledge. Source-session failure leaves that session's notifications pending without blocking other sessions, except for positively confirmed absence past the source-wait window below. Projection failure retains every successfully synchronized notification in the group for a restart-safe idempotent retry.

Missing or unsafe sidecars use the step-3 notification-terminal disposition: log a content-free reason and acknowledge without reading the source or changing graph state. No projection is required for that rejected notification alone. A later fresh notification can ingest a repaired sidecar.

Malformed/unsafe notifications are diagnosed independently of healthy notifications. Regular rejected entries and symlinks can be unlinked without following them; unsafe directory entries are not recursively removed and do not block healthy work. Diagnostics contain classification and source identity only, never raw envelope contents or error strings that may contain credentials.

## Never-mirrored source expiry

`ADAM_MEMORY_SOURCE_WAIT_MS` configures the per-notification missing-source wait window: default **600000 ms (10 minutes)**, accepting only decimal integers from **1000 through 86400000 ms**. Invalid values fail closed without acknowledgement; the standalone worker exits at startup before connecting or retrying, while Pi isolates the configuration error to its notification drain.

Expiry applies only when the ownership query **successfully confirms** that the source is not mirrored (`missing-source-session`), before any sidecar scan. At age **greater than or equal to** the window, Adam logs `source-never-mirrored` and acknowledges only the expired notifications. No source, memory stream, raw record, or projection is created, conflicted, or deleted. A fresh notification after source mirroring/import works normally. Once source ownership is confirmed, even old notifications remain eligible for normal ingestion. Currently an expired locator is unlinked, not parked: if the producer has ended and never notifies again, later source mirroring cannot recover its orphaned sidecar via `/adam:reconcile`. Durable parked-locator recovery is proposed in [issue 016](issues/016-recover-expired-memory-notifications.md), not yet implemented.

Age uses wall-clock milliseconds and each private notification file's **mtime**, on the same host clock as Adam. Use `queuedAt` only if finite local mtime is unavailable; a producer clock ahead or behind must not shorten or extend the wait for a fresh local notification. Neither worker restart nor a newer duplicate changes an older file's mtime, and fresh coalesced notifications retain their own window. No mutable retry counter or public wire change is required. Negative ages after a local clock rollback clamp to zero.

Backend/initialization failures, sidecar synchronization failures, and projection failures never trigger this absent-source policy. They retain normal transient retry behavior. Expiry diagnostics are emitted once per source/producer group, with notification count and age, never sidecar content. No additional mutable retry state or public wire fields are introduced.

## Production entry points and serialization

The detached Claude worker and Pi's notification drains share the existing recoverable `${XDG_CONFIG_HOME:-~/.config}/adam/worker.lock` lease. Pi active-session mirroring, file-evidence projection, and historical imports run independently of that lease; a busy notification worker must not skip those source writes. This deliberately retains the 0.3 lease location rather than creating a competing lock or migrating existing state. The new public memory inbox remains under XDG state. The worker releases the lease before capped-backoff sleeps, allowing Pi to establish a missing source session, and rechecks both queues after lease release to avoid a wake/shutdown race. Only the busy notification drains are deferred and reported by `/adam:status`; local JSONL remains authoritative. Replication failures retain their separate `Last error`/connectivity status rather than being mislabeled as notification deferrals.

Pi startup/lifecycle drains both queues after mirroring the active source. Claude's detached worker drains its pending transcript work before producer-memory work. Hooks still only enqueue and wake; they never wait for Neo4j. Producers need no Adam runtime dependency or wake call: correctness depends on a later entry point reading the durable spool.

Explicit repair is `/adam:reconcile` in Pi, or `node /path/to/adam/worker.js --once` with the normal `ADAM_NEO4J_*` environment. A notification does not authorize transcript discovery: if its source session has never been mirrored, first resume/import that source (or deliver its host lifecycle notification); until then memory attachment remains retryable inside its source-wait window. The CLI exits nonzero if the lease is busy or transient work remains; successful/terminal groups have already progressed. It does not sleep/retry indefinitely in one-pass mode. Normal detached mode retries transient work with capped exponential backoff. The delay is capped; confirmed missing sources additionally use the age-based expiry policy above. Other transient failures retain retries rather than being expired by an overall attempt limit.

Aggregate memory-only repair uses an ownership-checked Neo4j write transaction: it reads retained source entries and producer records, structurally adapts selected Pi embedded entries, uses existing `TOUCHES` evidence, and replaces only session memory. It never clears file evidence or requires local transcript/repository files, so remote-only sources and deleted subagent transcripts retain provenance. Only Pi repair fetches source entry `rawJson` for embedded memory adaptation; Claude repair fetches entry/stream identifiers and file evidence without transferring raw transcript payloads. A subsequent source/evidence reconciliation repairs newly resolvable citations.

`/adam:status` retains only the latest memory-drain counts/timing: acknowledged and pending notifications, failures, expired source notifications, appended records, synchronized streams, conflicts, unresolved citations, and duration. Detached retry diagnostics contain classifications, source/producer identity, and an Adam error message capped at 512 characters, not sidecar content. No unbounded timing history is collected.

Deterministic acceptance covers source-expiry boundary/default/configuration validation, restart/coalescing mtime preservation, ahead/behind producer-clock isolation, queuedAt fallback, detached exit, backend/projection non-expiry, and coalescing, ownership-before-attachment, projection-before-acknowledgement, failure isolation, busy/recoverable leases, lease-independent Pi mirroring/evidence/import, replication/drain error separation, capped backoff, lease release during backoff, post-release queue rechecks, and retained Pi branch adaptation. Live packaged-worker/Pi validation proves absent-source expiry without retained-prefix mutation, fresh-notification repair after late source mirroring, missing-source retry, transcript-before-memory repair, subagent citation/file links, post-final-hook sidecar append, restart with deleted source transcripts, and combined Pi embedded/sidecar retrieval.
