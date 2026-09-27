# Characterize Claude Code transcripts for ingestion

- Status: done
- Created: 2026-09-27
- Owner: unassigned

## Problem

Adam's Claude adapter could query existing memory, but transcript authority, resume, compaction, parallel-tool, and subagent behavior had not been observed closely enough to approve ingestion semantics.

## Outcome

A concise, evidence-based Claude transcript contract defines the initial authority, identity, continuity, file-evidence, and subagent rules without committing private transcript fixtures.

## Scope

### Included

- Native Read/Edit/Write call and result structure.
- Controlled resume and manual compaction behavior.
- Current-leaf and compact-boundary continuity.
- Parallel tool-call grouping.
- Subagent ownership, sidechain structure, and handback behavior.
- Deferred-fixture decision.

### Excluded

- Production transcript scanner.
- Source-scoped graph migration.
- Hook inbox and reconciliation worker.
- Permanent transcript fixtures.

## Acceptance criteria

- [x] Native Read/Edit/Write evidence fields are identified.
- [x] Resume behavior is compared against a byte-prefix checkpoint.
- [x] Manual compaction behavior and continuity fields are identified.
- [x] Parallel tool activity is distinguished from abandoned conversation branches.
- [x] Subagent stream ownership and parent linkage are identified.
- [x] The approved contract records observed behavior separately from deferred scanner validation.

## Validation

Claude Code 2.1.283 was exercised through disposable sessions. A controlled resume preserved a 1,004,794-byte prefix exactly and appended 12 valid records. Manual compaction preserved a 1,013,706-byte prefix exactly and appended a compact boundary, summary branch, and metadata. A general-purpose subagent produced an independently rooted sidechain stream under the same Claude session and executed a native Read call.

No raw transcript or private message content was copied into the repository.

## Notes

The resulting contract is [`../claude-transcript-contract.md`](../claude-transcript-contract.md). Permanent sanitized fixtures are intentionally deferred until scanner implementation establishes the exact parser invariants they need to protect.
