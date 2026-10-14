# Wire explicit Pi derivation generation

- Status: done
- Category: enhancement
- Created: 2026-10-14
- Target: 0.5.0 stage 3
- Parent: [022](022-configurable-memory-derivations.md)
- Plan: [0.5.0](../plan-0.5.0.md)
- Prerequisite: [024](024-derivation-graph-storage-and-authorization.md)

## Scope

Explicit named/all-config Pi refresh using the deterministic core and principal-bound store. Reuse Pi's model registry and request-time authentication; exclude the session conversation/tools. No automatic generation, Claude refresh, saved-read surfaces (stage 4), release changes, production access or real model calls.

## Acceptance

- [x] Configuration/path and command selectors validated before graph/model work.
- [x] Session model or configured override resolved through Pi; credentials stay private to its registry.
- [x] Complete isolated request receives conservative supported-API sizing and explicit framing/output reservations; excess input fails before the one model call. No real-provider certification is claimed.
- [x] Approved lazy schema initialization, atomic renewable lease acquisition, publication fencing, failure/shutdown cleanup and preservation of prior latest.
- [x] Named refresh and sequential all-config refresh have independent sanitized outcomes, including cache reuse and empty generation.
- [x] Synthetic registry/executor, configuration and packaged-Pi tests plus disposable graph integration; real-provider probes require separate approval.

## Completion evidence

- Initial adapter foundation: 85a9160. Subsequent reviewed implementation covers provider options/system separation, conservative sizing, the shared dispatcher, lease controller, authorized transactional reuse, schema approval/readiness, explicit command and shutdown ownership.
- Clean normal validation: four ESM release builds, 237 ClojureScript tests/1,264 assertions; 31 Node tests (26 passing, five opt-in skips).
- Disposable Neo4j on port 7688: 13 tests/209 assertions plus packaged worker, cleanup and Pi derivation acceptance. The packaged Pi probe uses only a fake provider and verifies unlinked input, explicit generation, cache/comment reuse, truncation preservation, lease clearing and exact cleanup.
- Reviewer performed read-only per-slice reviews and final re-review with no blockers. Production schema initialization requires a separate explicit UI approval; no production access, real model calls, version bump or release occurred.
- Cancellation is shutdown-only plus the model-call timeout. Driver cleanup may delay shutdown; failed release falls back to lease expiry. A slow index population may require a later approved retry; no generation proceeds without online-index readiness.
- Saved-result rendering and Pi/Claude read adapters remain stage 4; quality/cost/capacity probes and release remain later acceptance work.
