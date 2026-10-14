# Derivation acceptance and release gates

- Status: in-progress
- Created: 2026-10-14
- Target: 0.5.0 stage 5
- Parent: [022](022-configurable-memory-derivations.md)
- Plan: [0.5.0](../plan-0.5.0.md)
- Prerequisite: [026](026-saved-derivation-read-surfaces.md)

## Outcome

Map the approved acceptance criteria to reproducible evidence, close isolated test gaps and identify the separately approved provider/production/release gates. No version bump until final release preparation.

## Included

- Make normal/live ClojureScript failures propagate to npm/CI rather than relying on compiler autorun status.
- Compiled-host restart/read/reuse and disposable graph conflict/history acceptance where current coverage is incomplete.
- Acceptance evidence with clear separation between structural fake-provider correctness and actual model interpretation/capacity.
- Reviewer feedback after each slice.

## Excluded without separate approval

Production access/mutation, real model/provider calls, installed-runtime changes, version bump, tag, merge and release.

## Acceptance

- [x] Intentional ClojureScript failure/error exits npm nonzero; later shell steps do not execute.
- [x] Compiled Pi/MCP restart retains authorized result and unchanged reuse without new generation.
- [x] Disposable graph proves within/across-run contradiction links and scoped supersession survive history reads.
- [x] Approved criteria are linked to deterministic/live evidence; semantic/provider and operational gaps remain explicit.
- [x] Clean normal CI, acknowledged disposable port-7688 validation, exact resource cleanup and reviewer acceptance.

## Isolated validation baseline

First clean **gated** `npm run ci`: 246 ClojureScript tests/1,333 assertions, **0 failures, 0 errors**; 39 Node tests (34 pass, five opt-in skips); exact-tarball and committed-dist gates pass. Acknowledged disposable port-7688 `npm run test:neo4j`: 14 tests/232 assertions, **0 failures, 0 errors**, plus three packaged worker/cleanup/Pi-MCP cases. Package dry-run and diff checks pass. No production access, real provider calls, version bump, tag, installation or release change.

This issue remains in progress for separately approved semantic/provider and operational acceptance and final release readiness; isolated checks do not complete those gates.

## Diagnosis: test exit status

Shadow 3.5.5's node-test autorun waits for Node and stores its exit code in build state, but the compile command does not propagate that test code. The stock Node runner itself exits 1 on failed assertions. Therefore compile and execute are separate CI steps; watch-only autorun remains available through a build-local `{:autorun true}` override, not nested `:builds`. Historical CI did not enforce ClojureScript test failure status, including stages 1–4. Read-only inspection of the last main run (38531623459) confirmed both Node jobs reported 246 tests/1,332 assertions and zero failures/errors; the live job reported 14 tests/228 assertions and zero failures/errors. Prior manually inspected passing summaries are not disproven.

The configured test-only runner captures discovery after selected namespaces compile and fails when no tests are registered/selected. It rejects natural process exit without completion and bounds active-handle hangs. The whole-run budget is 180 seconds normally, 600 seconds for live, overridable through strict ADAM_TEST_RUN_TIMEOUT_MS (10..3,600,000). Timeouts report elapsed/budget values, not test contents. Fixtures prove pass/assertion/captured-error/uncaught-rejection/incomplete/active-hang/empty-selection outcomes; a separately compiled zero-registered fixture exits 1. Negative probes use no Adam runtime or database.

## Restart defect and correction

Disposable packaged Pi acceptance reproduced unchanged refresh returning generated after restart. Session-wide reprojection previously deleted/recreated all memory nodes, removing saved derivation dependencies. Projection now preserves stable retained nodes/incoming dependency edges, replaces only producer-owned outgoing provenance, and prunes genuinely absent IDs. Changed contents remain subject to fingerprints/current grants; removed evidence still denies reads. This is a complete session snapshot boundary, not an incremental producer update API. Acceptance proves restart reuse without a model call or new run and selected-branch removal withholding saved content.

Live history acceptance now covers within-run and cross-run CONTRADICTS plus scoped SUPERSEDES through actual publication/authorized rendering. This validates relationships and authorization, not a real model's semantic judgment.
