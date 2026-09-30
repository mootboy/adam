# Freeze memory protocol v1

- Type: feature
- Status: done
- Created: 2026-09-30
- Owner: unassigned

## Problem

Adam 0.4 needs a producer-neutral, append-only memory sidecar protocol before lossless scanning or graph storage can be implemented. The milestone plan fixes the architectural boundary but does not yet provide a normative event schema, physical framing rules, replay/conflict semantics, or executable fixtures.

Two producer-characterization questions also remain protocol-visible: how a producer durably commits a valid no-memory outcome, and how stream-qualified source coverage participates in crash recovery without becoming an Adam transcript checkpoint.

## Expected outcome

A normative `docs/memory-protocol-contract.md` defines protocol v1 and a committed fixture corpus exercises its externally observable framing, validation, replay, citation, tombstone, identity, and prefix-conflict behavior. The contract adds a content-free `source.covered` event so a producer can durably advance coverage without inventing memory or paying repeatedly after restart.

This increment is contract-only. It does not implement Adam's production scanner, Neo4j storage, migration, aggregate projection, notification spool, or reference producer.

## Acceptance criteria

- [x] Protocol identifiers, sidecar location, file permissions, writer serialization, record-size bound, UTF-8/JSONL framing, fsync, partial-tail, and non-symlink rules are normative.
- [x] Event envelopes define immutable IDs, producer/source identity, recorded time, stream-qualified source checkpoints, and supported protocol-version behavior.
- [x] `observations.recorded`, `reflections.recorded`, `observations.dropped`, and content-free `source.covered` schemas are complete and bounded.
- [x] Source checkpoints are explicitly provenance and producer replay authority, never Adam transcript synchronization checkpoints.
- [x] Replay defines deterministic canonical payload comparison, idempotent duplicate events, immutable-event conflicts, and append/checkpoint crash recovery.
- [x] First-valid memory definitions, producer/session-local support and tombstones, unresolved citation repair, unknown versions/kinds, and malformed records are specified.
- [x] Fixtures cover valid events, identical replay, conflicting duplicate event IDs, RFC 8785 edge canonicalization, malformed completed JSON, incomplete tails, checkpoint regression, unresolved citations, tombstones before definitions, duplicate memory IDs, prefix mutation/shrinkage, and source/producer mismatch.
- [x] Deterministic fixture-conformance tests run without Neo4j or model access.
- [x] Architecture, session-replica contract, acceptance matrix, plan, README, and issue index link the normative contract consistently.
- [x] `npm test` and diff validation pass.

## Evidence

The contract is published with one informative JSON Schema, a 14-case manifest, and 15 JSONL files covering 27 structurally valid events plus malformed and incomplete framing. Eleven deterministic protocol tests exercise event kinds, canonical replay and its non-ASCII edge vector, framing, checkpoint regression, unresolved references, tombstone order, immutable memory definitions, prefix conflicts, locator identity, and bounded event shape.

Validation:

- `npm test`: 104 ClojureScript tests with 401 assertions; 20 Node tests passed and 3 opt-in tests skipped.
- Focused protocol/package tests: 12 passed.
- JSON Schema draft validation: 27 fixture events accepted.
- `npm run check:dist`, package dry-run, and `git diff --check` passed.

Commit, PR, and hosted-check evidence are recorded by the branch and pull request containing this issue.

## Notes

This is delivery step 2 from [`../plan-0.4.0.md`](../plan-0.4.0.md). Delivery steps 3–7 remain separate reviewable increments, and only the final 0.4.0 release PR changes package versions.

Implementation and test artifacts produced with AI assistance must be reviewed for correctness and security before merge.
