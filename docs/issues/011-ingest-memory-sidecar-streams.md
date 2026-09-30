# Ingest producer memory sidecar streams losslessly

- Status: done
- Created: 2026-09-30
- Owner: unassigned

## Problem

Memory protocol v1 is frozen, but Adam cannot yet scan or mirror producer sidecars. The next 0.4 increment must prove that every complete physical record—including unknown and malformed records—can be preserved byte-for-byte with restart-safe append checkpoints without coupling ingestion to memory projection.

## Outcome

Given an explicit validated sidecar locator, Adam scans the authoritative JSONL stream, mirrors its complete records into Neo4j as `AdamMemoryStream` and `AdamMemoryRecord`, resumes append-only suffixes idempotently, defers incomplete tails, and isolates stream conflicts from source sessions and other producer streams.

## Scope

### Included

- Bounded synchronous streaming scan of an explicitly supplied sidecar.
- Lossless complete-record preservation, including unsupported and malformed records.
- Protocol-v1 envelope validation, RFC 8785 event replay hashes, source-checkpoint monotonicity diagnostics, and immutable-event conflict detection.
- Separate sidecar prefix checkpoints with byte-bounded append batches.
- `AdamMemoryStream` and `AdamMemoryRecord` Neo4j constraints and transactional writes.
- Deterministic conformance tests consuming the frozen fixture corpus.
- Opt-in live Neo4j round-trip, append-resume, idempotency, partial-tail, and conflict-isolation coverage.

### Excluded

- Producer-scoped observation/reflection identity migration.
- Aggregate memory projection or retrieval changes.
- Durable memory-notification discovery and worker reconciliation.
- Reference Claude memory producer or model invocation.
- Sidecar restoration, deletion propagation, package-version changes, or release preparation.

## Acceptance criteria

- [x] Complete records are preserved byte-for-byte with ordinal, byte-offset, payload hash, prefix hash, and bounded diagnostics.
- [x] A non-LF tail is deferred and does not advance committed stream progress.
- [x] Malformed and unsupported complete records are mirrored without poisoning later safely framed records.
- [x] Exact canonical replay of an event ID is idempotent; changed payload reuse stops semantic advancement while later raw suffix records continue as blocked.
- [x] `checkpoint-regression` skips semantic acceptance without conflicting physical synchronization.
- [x] Sidecar shrinkage and committed-prefix mutation are detected before suffix writes.
- [x] Invalid UTF-8 or oversize records commit the preceding safe prefix before physically conflicting the stream.
- [x] Missing or unsafe files are notification-terminal without poisoning stream state, concurrent file changes are transient, and protocol string bounds use UTF-8 bytes.
- [x] Synchronization resumes from committed checkpoints in byte-bounded batches and repeated unchanged synchronization is idempotent.
- [x] Neo4j round-trip returns the exact mirrored raw records in physical order.
- [x] Normal tests remain independent of Neo4j; live coverage is opt-in.
- [x] Release builds, packaged runtime tests, committed-dist checks, package dry-run, and diff checks pass.

## Validation

Implemented on `feat/memory-stream-ingestion`.

- `npm test`: 120 ClojureScript tests with 485 assertions; 20 Node tests passed and 3 opt-in tests skipped.
- Live Neo4j validation: 7 ClojureScript tests with 102 assertions plus the worker outage-repair test.
- The live memory-stream case proves byte-for-byte raw round-trip, unchanged idempotency, append-only suffix resume, partial-tail deferral, raw blocked-suffix mirroring after semantic conflict, safe-prefix persistence before physical conflict, non-poisoning unsafe/missing source handling, and isolation of a healthy producer stream.
- Release build, package dry-run, generated-runtime drift check, and `git diff --check` passed.

## Notes

This is delivery step 3 in [`../plan-0.4.0.md`](../plan-0.4.0.md). The frozen protocol and fixtures in [`../memory-protocol-contract.md`](../memory-protocol-contract.md) are the executable input contract. This increment creates the lossless storage substrate only; derived memory semantics remain a later rebuildable projection.
