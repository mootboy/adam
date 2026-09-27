# Worker stalls on missing transcripts

- Status: done
- Created: 2026-09-28
- Owner: unassigned

## Problem

While validating the 0.3.1 cutover ([`006`](006-release-and-cut-over-0.3.0.md)), the Claude reconciliation inbox held 109 unacknowledged notifications from 21:15 UTC on 2026-09-27 onward, and the lease was owned by a worker from the removed 0.3.0 checkout that had run for 93 minutes using 0.7 s of CPU. A fresh v0.3.1 worker behaved the same: over 45 s it only ever stat'ed the oldest notification's transcript, which no longer existed, six times with growing backoff, and never touched the other 108 notifications.

Fifty-one queued notifications referenced transcripts that were gone: sessions that exited before their first record was written, and subagent transcripts that Claude Code deletes once the subagent finishes. The recorded subagent locators for the long-running session `6fbc25a9…` pointed at six such deleted files, so even its parent reconciliation would have failed on the next scan.

## Diagnosis

- `scanner/scan-stream` stats the transcript unconditionally, so a missing file throws.
- `worker/drain-once!` processed coalesced groups in inbox order and rethrew the first failure, so the retry loop restarted at the same poisoned group every time. One unrecoverable notification blocked every stream queued behind it.
- `worker/run-worker!` retries without an attempt cap (intentional, for Neo4j outages) but logged nothing, and the hook spawned the detached worker with all stdio ignored, so the stall was invisible.

## Outcome

- A notification whose transcript no longer exists resolves as `:missing-transcript` and is acknowledged without touching the graph or recording locators. A later hook for the same stream reconciles it normally once the file exists.
- Recorded subagent locators whose transcripts were removed are not scanned; because a session's evidence is rebuilt as a whole (every `TOUCHES` of the session is deleted and recreated from one projection), the reconciler reads those streams' mirrored raw entries back from the replica and includes them in the projection, so their evidence survives the rebuild. Review of the first draft caught that the scan-only skip would have silently deleted it.
- Each stream's failure is isolated: the rest of the inbox is processed and acknowledged, the failed notifications stay for retry, and the pass still rejects so the existing exponential backoff applies.
- The detached worker's stderr is appended to `${XDG_CONFIG_HOME:-~/.config}/adam/worker.log` (owner-only), and the worker logs each failed reconciliation, each retry, and each missing-transcript acknowledgement.

## Scope

### Included

- Scanner, reconcile, worker, and hook changes above with deterministic tests.
- Draining the live backlog with the released worker as cutover evidence.

### Excluded

- Log rotation; the log receives at most one line per failed attempt or acknowledged notification.
- Treating Claude's subagent directory layout as a stable discovery API.

## Acceptance criteria

- [x] A notification for a missing transcript is acknowledged, logged, and leaves no locator.
- [x] A failing stream does not prevent later streams from being reconciled and acknowledged in the same pass.
- [x] A recorded subagent locator whose transcript was removed does not fail the parent scan, and its previously derived evidence survives the parent's next evidence rebuild (live regression).
- [x] The hook-spawned worker writes its diagnostics to `adam/worker.log`.
- [x] The live backlog drains with the released worker.

## Validation

- Deterministic: 103 ClojureScript tests / 388 assertions including the new missing-transcript, subagent-skip, failure-isolation, and acknowledgement-logging cases; the Node hook test now waits for the detached worker's refusal to appear in `adam/worker.log`.
- Live: the fixed worker, run in the foreground from `/home/linus/git/aloi` against the shared Neo4j, drained the real 111-notification backlog in 4 s, acknowledging 44 notifications for missing transcripts (30 `SubagentStop`, 8 `SessionEnd`, 6 `SessionStart`) with no failures, and advanced `6fbc25a9…` to its full 5,255,101 bytes.
- Release validation is recorded in the 0.3.2 pull request.

## Notes

Evidence: `~/.config/adam/inbox/` listing and `~/.config/adam/worker.lock` at 00:49 local on 2026-09-28; `strace` of a fresh worker started from `/home/linus/git/aloi` showing repeated `statx` ENOENT on `2fd34e2d-….jsonl`.
