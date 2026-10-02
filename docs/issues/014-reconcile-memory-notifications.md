# Reconcile durable producer memory notifications

- Status: done
- Created: 2026-10-01
- Owner: coding agent

## Problem

Explicit sidecar ingestion and aggregate projection exist, but producer appends after the final host hook have no durable public discovery/reconciliation path.

## Outcome

A locator-only public spool and host-neutral serialized reconciliation connect independently committed producer sidecars to aggregate file-memory retrieval without blocking host hooks.

## Scope

### Included

- Versioned bounded notifications under XDG state, canonical sidecar locators under XDG data, owner-only atomic/fsynced enqueue and acknowledgement.
- Coalescing, source-session-before-memory ordering, independent failures, restart/retry safety, and aggregate projection before acknowledgement.
- Pi and Claude worker entry points plus explicit repair invocation.
- Deterministic and opt-in live Neo4j coverage, including reversed completion ordering.

### Excluded

- Reference producer/model generation, automatic prompt injection, release/version changes.
- Migration of existing Claude lifecycle inbox paths; folded-state caching.

## Acceptance criteria

- [x] Notifications contain only bounded validated locators, never memory/transcript content.
- [x] Unsafe spool paths and malformed notifications cannot redirect reads or poison unrelated work.
- [x] Pending transcript work runs before memory attachment; absent owned sessions remain retryable.
- [x] Sidecars synchronize independently and one aggregate session projection precedes acknowledgement.
- [x] Missing/unsafe sidecars acknowledge without graph conflict; transient failures remain queued.
- [x] Restart, outage, reversed hook completion, producer isolation, and concurrent enqueue converge.
- [x] Pi/Claude composition roots share reconciliation and an explicit repair path.

## Validation

- All four release targets build; normal suite passes 148 ClojureScript tests with 592 assertions and 20 Node tests with three expected opt-in skips.
- Live ephemeral Neo4j 5.26 coverage passes eight ClojureScript tests with 127 assertions plus the packaged worker/Pi integration test.
- Deterministic coverage proves private bounded notifications, symlink/malformed-envelope isolation, coalescing, ownership-before-attachment, projection-before-acknowledgement, transient isolation, terminal rejection, projection-failure retention, lease release during backoff, busy-lease Pi mirroring/evidence/import, replication-error versus drain-deferral reporting, bounded retry diagnostics, conditional Pi-only raw entry fetch, busy/capped retry handling, and post-release queue rechecks.
- Packaged live coverage proves notification survival without Neo4j configuration, missing-source retry, transcript-first repair, canonical subagent citation links, append after the final source hook, restart with deleted local source streams, and Pi embedded/sidecar aggregate repair through `/adam:reconcile`.
- Clean `npm run ci`, runtime reproducibility, clean-consumer tarball, package dry-run, and diff checks passed before PR handoff.

## Notes

PR #21 review findings 1/2 are covered by failing-then-passing busy-lease and connected-error register regressions; small logging/raw-transfer follow-ups 4/5 are also addressed. Never-mirrored-source expiry (finding 3) is implemented as the separate follow-up in [issue 015](015-bound-never-mirrored-source-retries.md).

Delivery step 6 in `docs/plan-0.4.0.md`. Aggregate folding currently rereads retained per-session records; worker sizing must account for this linear cost. Notifications and lifecycle inboxes remain distinct public/private protocols.
