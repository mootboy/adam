# 032 — Native completion for derivation command options

- Status: done
- Created: 2026-10-15

## Outcome

Use Pi's existing `getArgumentCompletions` integration for `/adam:derive` and `/adam:derivations`, not a new UI.

## Acceptance

- [x] Complete applicable flags and known depth/language values; saved reads additionally complete limits 1–20.
- [x] Preserve preceding arguments/whitespace; replace only the current token through Pi's full-argument replacement contract.
- [x] Suppress used flags, mutually exclusive selectors, unsupported/repeated flags and invalid known values.
- [x] Free-form names/locators/tokens remain manually supplied. Tab performs no configuration reads, Neo4j work, source scans or model calls; registration remains inert.
- [x] Parameterized registered-command tests and compiled-extension acceptance through Pi's actual autocomplete provider/applyCompletion.
- [x] Final clean CI and generated-runtime drift checks: 262 ClojureScript tests/1,682 assertions and 39 Node tests (34 pass/five expected opt-in skips), including Pi-native completion application against compiled Adam. No local live database was required.

No schema, generation, authorization, dependency, version or release change. No production access or real model calls are needed for validation.
