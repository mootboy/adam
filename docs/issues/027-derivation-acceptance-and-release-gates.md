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

## Approved default-provider probe: blocked at preflight

User approved the executing session default for up to three synthetic calls with a 2,048-token output allowance and 60-second timeout. Pi 1.1.0 resolves that default to `openai-codex/gpt-6.1-sol`, API `openai-codex-responses`, with configured authentication. An isolated temporary ESM build of the actual derivation adapter plus the real ModelRegistry reproduces `invalid-budget` during sizing, with **zero model dispatches** and no graph access: this API is absent from the conservative sizing allowlist.

Inspected Pi 1.1.0 Codex request construction also does not forward `maxTokens` into a server output-token cap. Merely allowlisting the API would not certify the approved output bound. No alternative model was substituted, no paid probe was made, and no production graph/installation was changed. Codex compatibility/resource-control policy or a different approved model remains a gate before semantic/provider certification. Temporary probe code is not a runtime or packaged change.

## Pi-owned compatibility and approved three-call probe

The user subsequently approved removing Adam's API-name gate and treating provider resource controls as non-universal. Pi still resolves/authenticates/dispatches; Adam keeps resolved-model checks, complete-input measurement/reservations, stop/block/output validation, authorization and publication fencing. Accepted generated text is capped at 1 MiB UTF-8 before parsing; thinking is discarded. This is not a stream-heap or cost cap.

Exactly three real `openai-codex/gpt-6.1-sol` calls ran through Pi 1.1.0 ModelRegistry and the compiled Adam adapter/core on **synthetic inputs only**, without graph access. Each had a 60-second timeout; the 2,048-token SDK argument was **not transmitted as a Codex server output cap**, including reasoning. Authentication was subscription OAuth; usage consumes plan limits, not an asserted per-token charge. No alternate model, installed-runtime change or production mutation.

| Case | SDK input/output tokens | Result |
| --- | --- | --- |
| Scoped Alice correction plus unendorsed assistant choice/repeated summary | 926 / 120 | Valid explicit repository-Alpha conclusion, supporting the correction only, explicitly not universal |
| Alice/Bob disagreement | 805 / 257 | Normal provider stop; rejected by Adam as invalid-output |
| Assistant choice without endorsement plus duplicate summary | 775 / 12 | Valid empty conclusions/links |

Total elapsed: 18,090 ms; all three provider stop reasons were stop. Generated text sizes were 553 / 1,159 / 29 bytes. First/third conservative input measurements were 4,068 / 3,469 against SDK input usage 926 / 775. This is a bounded compatibility/quality sample, not general capacity certification.

The failed case's raw synthetic reply was not retained; **its exact validation failure cannot be inferred**. No fourth call was made. Review found model-visible rules omitted existing validator constraints. Follow-up exposes exact eligible IDs/scopes, verified occurrence keys, category/inference and compatible-link/prior-supersession rules without relaxing them. Fixed content-free validation-reason codes now accompany invalid-output outcomes, including parse/wire/non-text/oversize/incomplete failures. Source-derived rule identifiers remain in the user/data message, never elevated into system instructions. Internal policy version 2 makes older results outdated. Cross-user disagreement must use separately attributed scopes/conflicting-memory-ids, not a cross-subject CONTRADICTS link. Parameterized synthetic interface coverage verifies accepted and rejected shapes; real-model confirmation of this follow-up requires renewed approval.

### Follow-up validation

After Pi-owned compatibility and output-contract follow-ups: clean gated CI passes 250 ClojureScript tests/1,515 assertions and 39 Node tests (34 pass, five opt-in skips), with generated-runtime/exact-tarball checks. Disposable port-7688 validation passes 14 tests/232 assertions plus three packaged cases. The packaged truncation expectation now explicitly verifies the content-free incomplete-output reason. Final read-only reviews find no blockers. No additional model calls or production graph access occurred.

## First-use Neo4j schema detection correction

User-authorized read-only operational diagnosis found Neo4j 2026.05.0 reporting node ID uniqueness as NODE_PROPERTY_UNIQUENESS. The three derivation constraints had been successfully created and all required indexes were ONLINE, but Adam's UNIQUENESS-only readiness/write guard rejected them. Detection now accepts both node-uniqueness names, still requiring all three labels and exact id properties; relationship uniqueness/existence or incomplete constraints do not satisfy the guard. Parameterized readiness and lease-interface regression reproduced two failures before the correction. Diagnosis performed no DDL, memory reads, reconciliation or model calls. The native entryId index also reports RANGE / ONLINE as required. Final clean CI passes 251 ClojureScript tests/1,533 assertions and 39 Node tests (34 pass/five opt-in skips), with package/runtime drift checks. Full disposable live suites pass on both Neo4j 5.26 and 2026.05.0: each 14 tests/232 assertions plus three packaged cases, isolated on port 7688 without production mounts. Final read-only review finds no blockers. Source and rebuilt dist/adam.js are committed together. Local runtime changes require restarting the loaded extension; successful real generation remains an operational check, not inferred from schema metadata.

## Restart defect and correction

Disposable packaged Pi acceptance reproduced unchanged refresh returning generated after restart. Session-wide reprojection previously deleted/recreated all memory nodes, removing saved derivation dependencies. Projection now preserves stable retained nodes/incoming dependency edges, replaces only producer-owned outgoing provenance, and prunes genuinely absent IDs. Changed contents remain subject to fingerprints/current grants; removed evidence still denies reads. This is a complete session snapshot boundary, not an incremental producer update API. Acceptance proves restart reuse without a model call or new run and selected-branch removal withholding saved content.

Live history acceptance now covers within-run and cross-run CONTRADICTS plus scoped SUPERSEDES through actual publication/authorized rendering. This validates relationships and authorization, not a real model's semantic judgment.

## 0.5.0 release preparation (2026-10-15)

The user accepted launch after the output-limit correction. Candidate restarts and successful repository-scoped observations/reflections generation/reads were user-confirmed. The separately approved policy-v2 follow-up probe is documented only in [PR #35's evidence comment](https://github.com/mootboy/adam/pull/35#issuecomment-6166816603), not replicated here. This supersedes the older pending-reprobe/first-use notes above, without claiming general provider or production-performance certification.

Issue 030 replaces clipped read text with lossless continuation and separately requested memory evidence. Existing conclusion IDs locate older authorized runs without a history-window search. Versions, plugin metadata, changelog and installation docs are prepared for 0.5.0. Final validation and user review/merge remain required; merging the version bump starts the protected-main tagging/release pipeline. No assistant merge, tag, installation, production action or real model call is part of release preparation.

Final local release checks pass: `npm ci`, cache-cleared release build and clean `npm run ci`, 261 ClojureScript tests/1,648 assertions, 39 Node tests (34 passing/five opt-in skips), exact-tarball/runtime drift, disposable port-7688 Neo4j 15 tests/248 assertions plus three packaged cases, package dry-run, Claude plugin validation and diff checks. Claude validation emits a non-blocking marketplace-install README advisory; the documented plugin-dir installation remains supported. `npm audit` still reports the upstream peer-shrinkwrap finding tracked in issue 031; targeted patch-only/audit-fix attempts made no dependency changes. These known limitations remain visible for user release review. Hosted CI and publication follow the pushed PR and user-controlled merge.
