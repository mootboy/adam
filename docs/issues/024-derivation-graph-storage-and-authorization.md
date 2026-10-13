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

Review follow-up reproduces the nonempty-diagnostics decoding bug as four deterministic failures and fixes the enum normalization. It also gates graph writes on actual uniqueness constraints, pins canonical UTC timestamps and integer lease durations, requires comparison provenance for comparison roles, and caches authoritative run roles on repairable USED_MEMORY edges. Interface query fragments remain only for fake routing; outcome assertions no longer depend on token-guard query wording.

New live regressions round-trip comparison-unavailable diagnostics and reuse that run as prior context, revoke a comparison-only owner grant (withholding the supersession-bearing run), reject publication from a previously authorized comparison snapshot after revocation, and bound/filter history across buckets. All four memory-lookup query plans use both source-label unique index seeks on disposable Neo4j 5.26.32; no memory full/label scans were observed, so the query text is retained. Stage-3 failure-release/status responsibilities are documented rather than introducing cross-process model cleanup now.

Review follow-up validation: clean CI passes four ESM builds, 210 ClojureScript tests/998 assertions, 30 Node tests (26 pass/four opt-in skips), dist/package/diff gates, and disposable Neo4j with 10 tests/179 assertions plus both packaged worker/cleanup cases. Generated artifacts and release metadata remain unchanged. No real model calls or production DB tests/mutations occurred.

The next working slice adds load-input!: selector validation before IO, one principal-bound managed read transaction, complete own/direct-granted memory loading (including unlinked/dropped records), producer-qualified reflection support declarations, and exact-bucket prior lookup with unavailable text withheld. Synthetic tests reproduced the missing interface before implementation. Disposable live tests prove non-transitive selection and immediate grant-revocation exclusion. Native activity anchors remain intentionally unverified/empty; this is a snapshot foundation, not completion of stage 2.

Snapshot-foundation validation: clean CI passes all four builds, 212 ClojureScript tests/1030 assertions, 26 passing Node tests/four opt-in skips, runtime drift, packaging and diff checks. Disposable port-7688 Neo4j passes 10 tests/184 assertions plus both packaged worker/cleanup cases. Source-row validation is spec-composed; tests include dropped records, producer-qualified reflection supports, malformed host identity and ambiguous ownership. No model calls, production DB tests/mutations, host registration or release changes occurred.

Snapshot review corrections reproduce four configured-refresh wiring failures and two live selection failures (empty repository scope, reflection excluded from language scope). The fixes read normalizedOrigin, pass original selectors into the loader, derive reflection paths through supported observations, and use actual graph support IDs with declaration/producer/session agreement. Owner expansion is anchored on reader/self plus direct incoming read grants. Disposable Neo4j EXPLAIN shows the reader unique-index seek and union, without label/all-node scans. Read-committed semantics, Cypher-authorized DTO grants, whole-load rejection and content-free diagnostic counts are explicit.

Correction validation: clean CI passes 215 ClojureScript tests/1054 assertions, 26 Node tests/four opt-in skips, four builds, committed-runtime drift, packaging and diff gates. Disposable port-7688 Neo4j passes 11 tests/190 assertions plus both packaged worker/cleanup cases. Coverage includes actual WORKED_ON normalizedOrigin selection, reflection language eligibility, graph support IDs/mismatch, Pi producer normalization, all raw selector refresh interfaces, revoked prior withholding and sanitized rejection counts. No production DB tests/mutations or real model calls occurred; generated artifacts and release metadata remain unchanged.

With explicit user approval, a read-only production audit on 2026-10-13 observed 1,869 reflections, all uniquely owned. It found zero support-edge count mismatches, empty declarations, missing/undeclared supports, orphaned reflections or cross-session supports. This is an aggregate read-committed observation, not an authorization-equivalent full-loader audit; no mutation, repair or policy change was authorized/performed. Whole-load rejection remains the contract, including malformed granted-owner inputs.

A further red regression reproduces the misleading count (three malformed rows reported as one). Processing now validates every loaded row and rejects with the exact invalid-row count, without partially returning healthy evidence. Core-interface tests pin exact raw selector forwarding for both named and all-config refresh.

Latest count/selector follow-up validation: clean CI passes 216 ClojureScript tests/1058 assertions, 26 Node tests/four expected skips, all four builds, runtime drift, package and diff gates. Fresh disposable port-7688 Neo4j passes 11 tests/190 assertions plus both packaged worker/cleanup cases. The exact-count regression includes three malformed rows alongside healthy evidence. Production access was restricted to the explicitly approved aggregate read audit; live testing remained disposable-only.

Verified source-occasion anchors are implemented in the working slice. Synthetic interface coverage checks Pi/Claude call-result deduplication, sibling request fragments, distinct same-session user requests, summary exclusion and logical compaction continuity, structured child-task provenance, ambiguous/mismatched tool IDs, missing origins/cycles and byte-identical fork origins. Disposable graph coverage now includes native Pi requests, Claude parent/child activity and fork-copy origin preservation. Original raw content is used only inside the graph boundary; model inputs retain locator provenance, not transcripts. Full source-session reads are correctness-first and caching remains deferred. An additional interface regression reproduced stack overflow on a 15,000-entry Claude continuation. Native ancestry and uniquely matched results now use iterative traversal; the regression passes without weakening the anchor contract. Final local anchor validation passes clean CI (219 ClojureScript tests/1079 assertions, 26 Node tests/four opt-in skips), all four builds, runtime drift, exact-tarball/package and diff checks. Fresh disposable port-7688 Neo4j passes 11 tests/193 assertions plus both packaged worker/cleanup cases, including real graph Pi/Claude/subagent/fork anchors. Generated artifacts and release metadata are unchanged. No production access or model calls occurred in anchor work. Review remains pending; host/provider wiring remains outside stage 2. Repair and history reads are explicitly bounded pages with truncation; large-history paging/scheduling is not claimed.
