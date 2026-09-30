# Migrate memories to producer-scoped identities

- Status: done
- Created: 2026-09-30
- Owner: unassigned

## Problem

Adam observation and reflection URNs identify the user, source, session, and memory ID but omit the producer. Different producers can therefore collide when they use the same memory ID in one source session, and protocol-v1 sidecar memories cannot safely join the existing derived graph.

## Outcome

Adam uses canonical producer-scoped observation and reflection identities. Before normal memory writes, a graph-native restart-safe migration rewrites existing Pi memories and validates a per-user marker without requiring local session files.

## Scope

### Included

- Producer-scoped observation and reflection URN constructors.
- Producer/source metadata on newly projected Pi memories.
- Transactional Neo4j migration of existing Pi observations and reflections.
- Preservation of sessions, entries, raw payloads, tombstones, file provenance, and support relationships.
- Stale-marker detection across dependent memory identities and producer metadata.
- Lifecycle ordering and bounded migration status.
- Deterministic and live Neo4j migration coverage, including remote-only memories.

### Excluded

- Session, transcript-stream, entry, repository, or file identity changes.
- Aggregate multi-producer projection.
- Memory notification discovery and reconciliation.
- Reference Claude memory producer.
- 0.4.0 release changes.

## Acceptance criteria

- [x] New observation and reflection URNs include immutable producer identity.
- [x] Existing Pi memories migrate using persisted producer metadata without local JSONL.
- [x] Only memory IDs and required producer/source metadata change; sessions and entries remain unchanged.
- [x] Dropped state, `ABOUT`, `SOURCED_FROM`, `SUPPORTED_BY`, and `HAS_MEMORY` relationships remain intact.
- [x] Migration runs before normal memory projection.
- [x] The per-user marker advances only after one completely successful transaction.
- [x] A stale marker cannot hide stale memory IDs or producer metadata.
- [x] Interrupted migration retains the prior marker and is retryable.
- [x] Repeated migration is an idempotent no-op.
- [x] Normal tests remain independent of Neo4j and live coverage is opt-in.

## Validation

Implemented on `feat/producer-scoped-memory-identity`.

- Deterministic ClojureScript suite: 124 tests with 506 assertions.
- Ephemeral Neo4j 5.26 suite: 7 tests with 114 assertions plus the worker outage-repair test.
- Live migration coverage seeds a stale current marker and remote-only Pi memories, rejects an invalid transaction without advancing identities or the marker, repairs and migrates, verifies dropped state plus `ABOUT`, `SOURCED_FROM`, `SUPPORTED_BY`, and `HAS_MEMORY` relationships, and confirms idempotency.
- Release build, compiled boundaries, exact-tarball test, package dry-run, committed-dist drift, and diff checks are required before proposal.

## Notes

This is Adam 0.4 delivery step 4 in [`../plan-0.4.0.md`](../plan-0.4.0.md). The migration is deliberately graph-native so remote-only memories and their provenance survive without reconstructing local host logs.
