# Scan Claude transcripts and derive native file evidence

- Status: done
- Created: 2026-09-27
- Owner: unassigned

## Problem

Adam can serve existing Pi-produced memories to Claude but cannot yet losslessly interpret Claude's authoritative transcript streams or derive native Read/Edit/Write evidence from them.

## Scope

### Included

- Streaming, read-only scanning of hook-located parent and explicit subagent JSONL streams.
- Exact preservation of complete physical records, including unknown and UUID-less records.
- Source-scoped session and entry models with deterministic stream-local IDs.
- Parent, compact-boundary logical-parent, current-leaf, and parallel-request context reconstruction.
- Deterministic evidence from native Read/Edit/Write `input.file_path` and matched results.
- Permanent sanitized structural fixtures and defensive malformed-tail, duplicate, and concurrent-change coverage.

### Excluded

- Claude hooks, durable inbox, worker, and retries.
- Undocumented transcript-directory discovery.
- Claude transcript restoration.
- Observation or reflection production.

## Acceptance criteria

- [x] Every complete physical JSONL record is preserved byte-for-byte.
- [x] Unknown and UUID-less records receive stable stream-local identities.
- [x] Append checkpoints detect changed prefixes and file shrinkage.
- [x] Current context crosses compact boundaries and includes parallel request fragments/results.
- [x] Abandoned branches do not contribute evidence.
- [x] Explicit subagent streams contribute evidence under the owning session.
- [x] Only native Read/Edit/Write call paths and structurally matched results contribute evidence.
- [x] Per-record cwd and actual worktree revision are retained.
- [x] Normal tests remain independent of Neo4j.

## Completion evidence

- Clean release build completed for both ESM targets.
- 86 deterministic ClojureScript tests passed with 340 assertions.
- Seven Node boundary, package, and release tests passed.
- Four ephemeral Neo4j 5.26 tests passed with 56 assertions, including exact Claude raw-record round trip, independent parent/subagent checkpoints, logical-parent continuity, native file evidence, and idempotent resynchronization.
- Package dry-run and committed-dist drift validation passed.
