# Saved derivation read surfaces

- Status: done
- Category: enhancement
- Created: 2026-10-14
- Target: 0.5.0 stage 4
- Parent: [022](022-configurable-memory-derivations.md)
- Plan: [0.5.0](../plan-0.5.0.md)
- Prerequisite: [025](025-pi-derivation-generation.md)

## Scope

One shared read-only service/rendering boundary for `/adam:derivations` and `adam_derivations` in Pi and Claude MCP. Authorized bounded latest/history pages, truthful freshness, prominent scoped conflicts/corrections, inspectable source/producer/support provenance and explicit item/line/byte truncation. No model calls, auto-generation, DDL, grants, production access, release/version changes or automatic relationship repair.

## Acceptance

- [x] Name/single selector/history/limit are validated before storage/configuration IO; no caller-selected principal.
- [x] Current grants and complete run dependencies gate all text/supports/history; unavailable inputs never become merely outdated results.
- [x] Configuration/evidence freshness is recomputed; model-only changes do not stale reads; missing configuration/evidence is reported honestly.
- [x] Results show scope/filter/provider/model, supports/conflicts and authorized producer/source provenance, with bounded output and explicit omissions.
- [x] Equivalent inert Pi and MCP adapters read only; no schema initialization, model work, source mutation or repair.
- [x] Synthetic parameterized/property interface tests, compiled host boundaries and disposable resource-scoped Neo4j revocation/history/rebuild acceptance.

## Completion evidence

Clean `npm run ci`: four ESM builds, 246 ClojureScript tests/1,332 assertions, 31 Node tests (26 pass, five explicit opt-in skips), exact-tarball and committed-runtime drift gates. Disposable acknowledged Neo4j on port 7688: 14 tests/228 assertions plus three packaged worker/Pi/MCP cases. Interface/property/adversarial tests cover argument preflight, missing/invalid YAML, quoted content, bounds, busy/cancellation/first-close wind-down and inert shutdown. Live tests cover nonempty/empty current results, history, changed evidence/configuration, before/during-stream revocation, missing relationships and separate repair; compiled Pi/MCP parity makes no generation calls while reading. Final read-only review found no blockers. No production access, real model call, version bump or release.

## Validation boundary

Use fake providers and disposable acknowledged Neo4j on port 7688 only. No production access or real model calls. Request reviewer feedback after each slice; user controls the history-preserving merge.
