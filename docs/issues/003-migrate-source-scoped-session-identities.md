# Migrate session identities to source-scoped URNs

- Status: done
- Created: 2026-09-27
- Owner: unassigned

## Problem

Pi-only session and dependent graph identities did not identify their host source, so equivalent local session IDs from Pi and Claude Code could collide once Claude ingestion begins.

## Outcome

Adam uses source-scoped Pi session, entry, observation, and reflection identities. Before normal synchronization it transactionally migrates each user's existing Pi graph, including remote-only sessions, and records a validated schema marker.

## Scope

### Included

- Source-scoped Pi resource constructors and newly written graph properties.
- Atomic Neo4j migration of sessions, entries, memories, and stored parent references.
- Preservation of graph relationships, raw payloads, checkpoints, and remote-only sessions.
- Restart-safe marker advancement and stale-marker detection across dependent nodes.
- Lifecycle ordering and status reporting.

### Excluded

- Claude transcript scanning.
- Claude session graph writes.
- Hook inbox and worker.

## Acceptance criteria

- [x] New Pi identities include the `pi` source kind.
- [x] Migration occurs before normal synchronization.
- [x] All dependent node IDs and denormalized session references migrate atomically.
- [x] Raw entry payloads and graph relationships remain unchanged.
- [x] Remote-only sessions migrate without requiring local JSONL.
- [x] Interrupted transactions retain the previous version and retry.
- [x] A stale marker cannot hide stale session, entry, observation, or reflection identity.
- [x] Repeated migration is a no-op.

## Validation

- Deterministic ClojureScript suite: 75 tests, 283 assertions.
- Ephemeral Neo4j 5.26 suite: 3 tests, 43 assertions.
- Live migration coverage seeds a remote-only legacy Pi graph and verifies source-scoped sessions, entries, memories, fork/current-leaf/file-memory relationships, byte-identical raw JSON, stale-marker detection, and idempotency.

## Notes

The migration is one Neo4j write transaction per Adam user. Its version marker is written last within the same transaction, so failure rolls back all identity changes and leaves the migration retryable.
