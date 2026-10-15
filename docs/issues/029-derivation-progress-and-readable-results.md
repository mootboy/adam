# 029 — Derivation progress and saved-result presentation

Status: done

## Request and decision

Explicit derivation commands were silent until completion. Saved reads also looked like raw diagnostic output rather than a thematic summary.

Commit `0f9f195` tried summary-first formatting, opt-in evidence, a custom Markdown screen and a tool renderer. The user found the separate UI jarring and requested a targeted rollback. Those presentation changes and the new direct TUI dependency are removed; the prior text output and read interface are restored. The local Pi session can be prompted to present saved output as a normal response. No new prompt or automatic model call is installed by this rollback.

## Retained scope

- Immediate generation acknowledgement and namespaced footer progress for idle waiting, model/authentication, evidence loading, generation, reuse and publication.
- Clear status on success/failure/cancellation without taking over Pi's main-agent working indicator.
- Preserve existing generation, authorization, storage and read behavior. No schema, version or release change.

## Validation

Retained interface regressions check synchronous acknowledgement before idle/evidence/model work, generation progress and footer cleanup after success and validation failure. Previous UI-specific tests are removed with the reverted UI. Rollback validation passed: clean CI and committed-runtime drift gates, 259 ClojureScript tests/1,607 assertions and 39 Node tests (34 passing/five opt-in skips). Compared with the pre-UI commit, runtime source/test changes are limited to generation progress and its regressions. Dependency manifests and saved-read implementation/tests exactly match the pre-UI state. No production access or real model calls occurred.
