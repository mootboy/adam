# Wire explicit Pi derivation generation

- Status: in-progress
- Category: enhancement
- Created: 2026-10-14
- Target: 0.5.0 stage 3
- Parent: [022](022-configurable-memory-derivations.md)
- Plan: [0.5.0](../plan-0.5.0.md)
- Prerequisite: [024](024-derivation-graph-storage-and-authorization.md)

## Scope

Explicit named/all-config Pi refresh using the existing deterministic core and principal-bound store. Reuse Pi's model registry and request-time authentication, default to the executing session model, and exclude the session conversation/tools from the separate request. No automatic generation, Claude refresh, saved-read surfaces (stage 4), release changes, production access or real model calls in normal validation.

## Acceptance

- [ ] Configuration/path and command selectors validated before graph/model work.
- [ ] Session model or configured override resolved through Pi; credentials remain private to its registry.
- [ ] Complete isolated request receives supported-model sizing and verified provider headroom; oversized inputs fail before the single model call.
- [ ] Schema initialization, atomic lease acquisition/renewal, publication fencing, failure/cancellation cleanup and preservation of prior latest.
- [ ] Named refresh and sequential all-config refresh have independent sanitized outcomes, including cache reuse and empty generation.
- [ ] Synthetic registry/executor and packaged-Pi tests plus disposable graph integration; real-provider probes require separate approval.

## Progress

Development starts on feat/derivation-host-provider from merged stage 2. Initial seam is the Pi registry adapter, tested without importing the Pi runtime or invoking a model. Provider-aware measurement/headroom, refresh controller and registration remain unfinished; no production schema is initialized.

Initial adapter validation: clean `npm run ci` passes all four builds, 224 ClojureScript tests/1,174 assertions, Node/package and generated-runtime drift checks. Fake-registry interface cases exercise session/override selection, credential-free capabilities, tool-free isolated context, output/abort forwarding, one dispatch, invalid preflight and sanitized provider failures. These tests are not real-provider capacity or authentication certification. No production access or real model calls occurred.
