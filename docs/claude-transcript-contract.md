# Claude Code transcript ingestion contract

Status: implemented, including durable non-blocking lifecycle reconciliation.

This contract records behavior observed with Claude Code 2.1.283 and defines the initial Adam ingestion boundary. Claude transcripts remain authoritative; Adam is a read-only replica and deterministic file-evidence index.

## Authority and discovery

- A hook-provided `transcript_path` is the authoritative locator for a Claude transcript stream.
- Adam does not discover sessions by scanning undocumented Claude storage directories.
- Adam never writes, repairs, truncates, or replaces a Claude transcript.
- Every complete physical JSONL record is retained exactly, including unknown record types and records without UUIDs.
- The parent transcript and any explicitly located subagent transcript are separate streams owned by one Claude session.

A stream is deferred until a supported hook supplies its locator. `SubagentStop` supplies both `transcript_path` and `agent_transcript_path`; Adam records the explicit child locator without scanning Claude's storage directories. Observed filesystem placement remains characterization evidence, not a discovery API.

## Source identity

Session identity is scoped by host source:

```text
urn:adam:session:<userUuid>:pi:<piSessionId>
urn:adam:session:<userUuid>:claude-code:<claudeSessionId>
```

A Claude parent transcript and its subagent streams share one `AdamSession`. Subagent stream identity is carried separately by `agentId`; it does not create another user session.

UUID-bearing records retain `uuid` and `parentUuid`. UUID-less records receive deterministic stream-local identities during scanning and remain part of the lossless replica even when they do not participate in the UUID tree.

## Observed append behavior

Controlled resume and manual compaction both preserved the complete prior byte prefix and appended complete JSONL records. Adam may therefore use the existing byte-offset and committed-prefix checkpoint model for the observed lifecycle.

A changed committed prefix, file shrinkage, or conflicting immutable record remains a conflict rather than an invitation to overwrite either source.

## Tree and current context

`last-prompt.leafUuid` is the current parent-stream leaf marker. It is metadata rather than a UUID-bearing tree record.

Normal resume appends a new user record whose `parentUuid` is the preceding leaf. Manual compaction appends:

1. a `system` record with `subtype: "compact_boundary"`;
2. `parentUuid: null` and `logicalParentUuid` pointing to pre-compaction history;
3. a child user record marked `isCompactSummary: true` and `isVisibleInTranscriptOnly: true`; and
4. a new current branch rooted at the compact boundary.

Adam follows `logicalParentUuid` across a compact boundary when reconstructing conversation continuity. Compact-summary prose never creates file evidence.

Claude can serialize parallel assistant tool calls and tool results as sibling UUID branches even though they belong to one active model request. Strict leaf-to-root traversal is therefore insufficient. For current-context projection Adam:

- starts from the latest valid `last-prompt.leafUuid`;
- follows `parentUuid` and compact-boundary `logicalParentUuid` continuity;
- includes all assistant fragments sharing an active fragment's `requestId`;
- resolves their results by `tool_use.id`, `tool_result.tool_use_id`, and `sourceToolAssistantUUID`.

This grouping recovers active parallel tool activity without treating every abandoned conversational branch as current.

## File evidence

Only assistant tool calls named exactly `Read`, `Edit`, or `Write` provide Claude file evidence. The path comes only from non-empty `input.file_path`.

Observed inputs are:

```text
Read  {file_path}
Edit  {file_path, old_string, new_string, replace_all}
Write {file_path, content}
```

Claude normalized tested relative paths to absolute paths. Adam still validates and resolves each path against the tool-call record's `cwd`; a transcript can contain several cwd values over its lifetime.

A matching user tool-result record is linked through:

```text
tool_use.id == tool_result.tool_use_id
sourceToolAssistantUUID == tool-call record uuid
```

Bash commands, MCP calls, prompts, responses, compact summaries, subagent handback text, file-history records, and path-looking prose do not create file evidence.

## Subagent streams

Observed subagent transcripts:

- share the parent Claude `sessionId`;
- carry one `agentId` on every record;
- mark every record `isSidechain: true`;
- have an independent UUID root and tree;
- use the same Read/Edit/Write representation as the parent stream.

The parent `Agent` result identifies the launched `agentId`. Completion later appears in the parent as synthetic peer/notification records rather than a second Agent tool result. Those handback records are preserved but do not create evidence.

When a subagent transcript is explicitly located, its native Read/Edit/Write calls contribute evidence to the owning Claude session. Revision and repository resolution use the subagent tool record's cwd.

## Memory boundary

Initial Claude ingestion creates sessions, lossless entries, structural stream/tree relationships, explicit file evidence, and revision provenance. It does not create observations or reflections and does not infer memory from Claude prose.

Claude can therefore retrieve Pi-produced memories associated with files it touches, while its own conversation does not become an Adam memory without a separate compatible producer.

## Lifecycle reconciliation

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks accept at most 64 KiB of JSON input, validate bounded session/cwd/absolute transcript locators, atomically write owner-only notifications under `${XDG_CONFIG_HOME:-~/.config}/adam/inbox/`, and wake a detached worker without waiting for Neo4j. Notifications contain no transcript content, prompts, tool payloads, credentials, or memories.

A single filesystem lease serializes workers. Repeated notifications for the same stream locator coalesce to the latest event, while successful reconciliation acknowledges every covered notification. Parent and subagent locators are retained in an owner-only per-session manifest so later rebuilds remain tree-complete. Failed initialization, scanning, synchronization, repository discovery, or projection leaves notifications durable and retries with bounded exponential backoff. Dead process locks and old incomplete locks are recoverable; a fresh competing worker exits without performing duplicate writes.

## Deferred validation

Permanent sanitized structural fixtures now cover parent and subagent streams, native file tools, parallel request fragments, compaction continuity, unknown and UUID-less records, malformed trailing records, duplicate tool IDs, changed prefixes, shrinkage, and concurrent writes. The scanner defers an incomplete non-newline tail, rejects malformed complete records and immutable identity conflicts, and preserves every accepted raw record byte-for-byte.
