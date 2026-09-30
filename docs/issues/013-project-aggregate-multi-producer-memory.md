# Project aggregate multi-producer memory

- Status: done
- Created: 2026-09-30
- Owner: agent

## Problem

Memory projection is still coupled to one embedded Pi adapter. Rebuilding file evidence deletes the session's complete derived memory state, so independently mirrored producer sidecars cannot contribute memories safely and one producer can overwrite another.

## Outcome

Adam deterministically folds embedded Pi memory and every mirrored protocol-v1 producer stream into one source-session snapshot, then replaces derived memory state once while preserving producer isolation, stream-qualified citations, unresolved references, and retrieval provenance.

## Scope

### Included

- Provider-neutral normalized memory folding for embedded Pi records and mirrored sidecar records.
- One aggregate observation/reflection snapshot per source-scoped session.
- First-valid definitions, durable tombstones, producer-local support links, and unresolved-reference diagnostics.
- Session-wide Neo4j reads of retained `AdamMemoryRecord` data.
- One transactional replacement of derived memory nodes alongside deterministic file evidence.
- Stream-qualified `SOURCED_FROM` and derived `ABOUT` relationships.
- Source, source-session, stream, and producer provenance in bounded file-context retrieval.
- Deterministic and opt-in live Neo4j coverage.

### Excluded

- Producer-memory notification spool and worker reconciliation.
- Reference Claude memory producer.
- Sidecar discovery or restoration.
- Cross-producer consolidation, support links, dropping, or deduplication.
- Package-version or release changes.

## Acceptance criteria

- [x] Equal producer-local memory IDs from different producers coexist.
- [x] Rebuilding one session writes one aggregate snapshot rather than deleting each producer independently.
- [x] Healthy producer suffixes update without removing retained contributions from missing or conflicted streams.
- [x] Sidecar citations resolve by source stream and entry ID within the owned source session.
- [x] Unresolved citations retain memory nodes and resolve on a later aggregate rebuild.
- [x] Tombstones apply before or after observation definitions.
- [x] Reflection support remains producer-local.
- [x] Pi embedded memory remains available through the same aggregate path.
- [x] File-context rendering identifies source session and producer without exposing sidecar paths.
- [x] Normal tests remain independent of Neo4j and live coverage is opt-in.

## Validation

Implemented on `feat/aggregate-memory-projection`.

- Deterministic ClojureScript suite: 131 tests with 534 assertions.
- Node boundary/package suite: 20 passing tests with 3 expected opt-in skips.
- Ephemeral Neo4j 5.26 suite: 8 tests with 126 assertions plus the worker outage-repair test.
- Live coverage mirrors two producers with the same memory ID, resolves stream-qualified citations and revision provenance, retains a semantically conflicted producer after its sidecar disappears, preserves memory through repository-less projection, and restores file links on later reconciliation.
- Release build, exact-tarball package boundary, committed-dist drift, package dry-run, and diff checks are required before proposal.

## Notes

Depends on memory protocol v1, raw memory-stream ingestion, and producer-scoped memory identity migration. This is Adam 0.4 delivery step 5.
