# Reconcile durable producer memory notifications

- Status: in-progress
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

- [ ] Notifications contain only bounded validated locators, never memory/transcript content.
- [ ] Unsafe spool paths and malformed notifications cannot redirect reads or poison unrelated work.
- [ ] Pending transcript work runs before memory attachment; absent owned sessions remain retryable.
- [ ] Sidecars synchronize independently and one aggregate session projection precedes acknowledgement.
- [ ] Missing/unsafe sidecars acknowledge without graph conflict; transient failures remain queued.
- [ ] Restart, outage, reversed hook completion, producer isolation, and concurrent enqueue converge.
- [ ] Pi/Claude composition roots share reconciliation and an explicit repair path.

## Validation

Foundation checkpoint: `npm test` passed all four release builds, 139 deterministic ClojureScript tests with 560 assertions, and 20 Node tests with three expected opt-in skips. `npm run check:dist` and `git diff --check` passed.

New test-first coverage proves canonical private notifications, strict bounded envelopes, symlink rejection, malformed-envelope isolation, per-producer coalescing, source-before-memory ordering, one aggregate projection before acknowledgement, transient producer/source isolation, terminal rejection acknowledgement, and projection-failure retention. Production lease/composition wiring and live end-to-end acceptance remain pending.

## Notes

Delivery step 6 in `docs/plan-0.4.0.md`. Aggregate folding currently rereads retained per-session records; worker sizing must account for this linear cost. Notifications and lifecycle inboxes remain distinct public/private protocols.
