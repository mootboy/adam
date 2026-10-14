# 028: Derivation depth and input sizing

Status: in progress

## Scope

Three separately committed increments, in order:

1. Command-level evidence depth, preserved authorization, separate saved-result identity/history, equivalent Pi/MCP reads.
2. Compact model-facing representation preserving selected content and validated support/provenance mappings.
3. Improved token measurement, distinguishing verified counts from tokenizer estimates and conservative fallback.

Pause before classification for further design discussion. No classification/embedding pipeline, release bump, production mutation or real provider/counting call is authorized by this work.

## Acceptance

- Depth values full (default), active, observations, reflections; independent of the one existing selector.
- Hidden reflection supports remain complete authorized dependencies; revocation or missing evidence denies saved reads/reuse.
- Prompt-visible conflicts/supports do not refer to omitted evidence; shallow conclusions disclose evidence depth.
- Independent cache/comparison/lease/history buckets per depth; default full identity preserved.
- Read commands/tools optionally filter depth and label every available result.
- Complete selected content survives compaction, with original IDs restored before output validation/publication.
- Token counts include system and data messages; unsupported mappings are not described as exact.
- Synthetic normal tests and explicitly disposable graph/package validation only.

## Progress

Depth implementation passes synthetic refresh/CLI/read regressions and disposable graph publication/history tests, including hidden-support authorization, separate depth buckets and default full identity. Normal validation passes 254 ClojureScript tests/1,573 assertions and 39 Node tests (34 pass, five opt-in skips); disposable port-7688 validation passes 15 graph tests/242 assertions plus three packaged cases. Cache-cleared normal validation regenerated all four packaged runtimes; final drift checks use those clean artifacts. Review verifies depth fencing at publication, fail-closed hidden dependencies, visible-only support/conflict eligibility and strict Pi/MCP inputs. Context measurements motivated these changes but are not tokenizer certification. Classification remains deliberately unimplemented.
