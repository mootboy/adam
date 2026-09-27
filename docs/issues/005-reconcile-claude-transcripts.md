# Reconcile Claude transcripts without blocking Claude

- Status: done
- Created: 2026-09-27
- Owner: unassigned

## Problem

The Claude scanner and Neo4j projection exist, but Claude lifecycle events do not invoke them. Performing scanning and graph writes directly in a hook would make Claude wait on transcript size, Git discovery, and Neo4j availability.

## Outcome

Claude lifecycle hooks atomically enqueue bounded locator-only notifications and return immediately. One serialized worker durably reconciles parent and explicitly located subagent streams, projects native file evidence, and retries interrupted or unavailable work without losing notifications.

## Scope

### Included

- SessionStart, Stop, SubagentStop, and SessionEnd command hooks.
- A 64 KiB input bound and strict locator validation.
- Owner-only atomic inbox and per-session locator manifests under the canonical Adam config home.
- Detached worker wake-up, one live filesystem lease, dead/incomplete lease recovery, and serialized processing.
- Notification coalescing, acknowledge-after-success semantics, and exponential-backoff retry.
- Parent/subagent scanning, checkpoint synchronization, repository discovery, and file-evidence projection.
- Compiled hook/worker boundaries and package artifacts.

### Excluded

- Claude transcript restoration.
- Adam-produced observations or reflections.
- Automatic prompt-context insertion.
- Shell/prose path inference.
- Scanning undocumented Claude storage directories.

## Acceptance criteria

- [x] Hooks persist only bounded locator metadata and do not contact Neo4j.
- [x] Hook execution returns successfully while Neo4j is unavailable.
- [x] Notifications survive worker and backend failure until successful acknowledgment.
- [x] Repeated stream notifications coalesce without losing the latest locator.
- [x] Only one worker processes the inbox at a time.
- [x] Dead and old incomplete worker leases are recoverable.
- [x] Parent and SubagentStop-provided child locators survive notification acknowledgment.
- [x] Reconciliation mirrors every known stream before replacing session file evidence.
- [x] Repository-less reconciliation clears stale derived evidence without affecting lossless entries.
- [x] Compiled package boundaries run without ClojureScript or Java.
- [x] A live outage-to-repair path produces source-scoped Claude graph state and native file evidence.

## Validation

- Clean release builds completed for the Pi extension, MCP server, Claude hook, and reconciliation worker.
- 100 deterministic ClojureScript tests passed with 380 assertions.
- Nine normal Node boundary, package, and release tests passed; the opt-in worker test skipped without live configuration as designed.
- Four ClojureScript Neo4j 5.26 tests passed with 56 assertions.
- The compiled hook/worker outage-to-repair test passed against Neo4j 5.26, including parent plus explicit subagent streams, durable acknowledgment, current leaf, five lossless entries, and four `TOUCHES` relationships.
- A real Claude Code 2.1.283 plugin run automatically queued and reconciled its 29-record transcript into one source-scoped session and stream with the recorded current leaf.
- Claude plugin validation, package dry-run, generated-runtime build, and diff checks passed.

## Notes

The worker is independent of the MCP process because Claude may stop MCP servers when no query is active. `SubagentStop` provides the authoritative parent `transcript_path`, explicit `agent_transcript_path`, and `agent_id`; Adam records those locators without directory discovery.
