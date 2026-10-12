# Persist derivations with fail-closed authorization and fenced refreshes

- Status: in-progress
- Category: enhancement
- Created: 2026-10-12
- Target: 0.5.0 stage 2
- Parent: [022](022-configurable-memory-derivations.md)
- Plan: [0.5.0](../plan-0.5.0.md)
- Contract: [derivation core](../derivation-contract.md)

## Scope

A concrete Neo4j boundary shared by future Pi generation and Pi/MCP saved-result reads. Capture authorized source state before pure processing; persist only a completed validated candidate at the end. Model work never runs inside a retryable transaction. No provider, host command, automatic generation, grant-management UI, release bump or production mutation.

## Acceptance

- [x] Unique derivation/run/conclusion constraints and deterministic user/name/filter identity.
- [x] Atomic token/expiry acquisition, renewal, crash recovery and stale-publisher fencing independent of the notification-worker lease.
- [ ] Authorized complete evidence snapshots, verified source provenance and directional/non-transitive grants.
- [x] Atomic publication of successful runs, conclusions, dependencies and latest pointer; successful empty history and failure preservation.
- [x] Saved latest/history reads reauthorize every recorded dependency, including comparison inheritance; missing ownership/dependencies fail closed.
- [x] Source-memory rebuild repairs stable-ID relationships without reducing permissions; revocation blocks cached/history reads.
- [ ] Synthetic interface tests and explicitly disposable live concurrency/rebuild/restart validation with isolated identities and exact cleanup.

## Progress

The first slice implements explicit unique constraints for the three derived labels and principal-bound token/expiry acquisition, renewal and sanitized failure release. A server-time lease is atomically guarded by a per-derivation write lock, independent of the notification-worker lease. Tokens are allocated before retryable transactions. Lease windows are explicit safe integers from 1 ms to 24 hours; longer work must renew. No lifecycle or host entry point initializes these schemas yet.

Synthetic interface tests cover bucket/principal binding, closed outcomes, invalid requests before I/O, token guards and session closure. Disposable Neo4j 5.26 on port 7688 proves concurrent acquisition yields one winner, separate identities proceed independently, restart can recover expired work, obsolete tokens cannot renew/release newer work, and failure preserves an existing latest-run pointer. Exact fixture cleanup touches only registered derivation/run IDs and the isolated disconnected user. Production was not accessed.

Final foundation validation: clean CI passes all four ESM builds, 205 ClojureScript tests/935 assertions and 30 Node tests (26 pass/four opt-in skips); generated runtime drift, package dry-run and diff checks pass. Disposable live validation passes nine ClojureScript tests/138 assertions plus two packaged worker/cleanup cases. All committed runtime artifacts remain unchanged; no real model or production access occurred.

The second slice adds closed candidate validation, token-fenced atomic publication of runs/conclusions/dependencies and optional scoped links, bounded latest/history reads with live run-wide authorization, and explicit stable-ID relationship repair. Snapshot state is captured before repair writes. Comparison inheritance remains complete; malformed graph metadata and ambiguous ownership fail closed without returning saved text. Source data is never mutated.

Disposable live coverage proves expiry and newer-token publisher fencing, retained supersession history and empty outcomes, revocation of an unreturned input blocking the entire run, rejection of a forged transitive grant, same-ID source-memory rebuild/repair across store restart, and rollback of a last-write uniqueness failure without changing latest. Normal interface tests cover malformed candidates before I/O, permission/missing-source fences, ambiguous ownership, wrong derivation parents, and rejection-only repairs. Schema/host/provider registration remains unchanged.

Second-slice final validation: clean CI passes four ESM builds, 208 ClojureScript tests/983 assertions and 30 Node tests (26 pass/four opt-in skips), plus dist/package/diff gates. Fresh disposable Neo4j validation passes 10 tests/165 assertions and both packaged worker/cleanup cases. Generated artifacts remain unchanged; no real model or production access occurred.

Authorized generation-input snapshot loading, verified original source occasions and full stage-2 acceptance remain unfinished. Repair and history reads are explicitly bounded pages with truncation; large-history paging/scheduling is not claimed.
