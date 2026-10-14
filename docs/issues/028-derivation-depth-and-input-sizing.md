# 028: Derivation depth and input sizing

Status: done

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

Depth implementation passes synthetic refresh/CLI/read regressions and disposable graph publication/history tests, including hidden-support authorization, separate depth buckets and default full identity. Normal validation passes 254 ClojureScript tests/1,573 assertions and 39 Node tests (34 pass, five opt-in skips); disposable port-7688 validation passes 15 graph tests/242 assertions plus three packaged cases. Cache-cleared normal validation regenerated all four packaged runtimes; final drift checks use those clean artifacts. Review verifies depth fencing at publication, fail-closed hidden dependencies, visible-only support/conflict eligibility and strict Pi/MCP inputs. Context measurements motivated these changes but are not tokenizer certification. Depth is committed as 563890d. Compaction now preserves source text, timestamps and dependency semantics while using transient collision-safe IDs and deduplicated repository/occasion context. Interface regressions cover literal short-ID-looking content, restored model references, historical supersession and prior-ID reuse rejection. Compaction is committed as 87395af. Token measurement now lazily uses packaged cl100k/o200k ranks for recognized OpenAI models and an explicitly unverified o200k family estimate for newer Codex aliases, with a 20% policy margin plus 32 message-framing tokens and the existing output/1024-token reservations. Unknown providers retain byte sizing without dispatch gating. Methods are closed, persisted, quoted on reads and retained in safe budget diagnostics. Tests exercise literal special-token-shaped text, no dispatch during counting, fallback/encoding families, metadata JSON round trips and packaged ESM execution. Sol's exact tokenizer/server accounting is not certified. No remote count/model call or production graph mutation occurred. Classification remains deliberately unimplemented; stop here for discussion.

Final validation: cache-cleared normal suite (259 ClojureScript tests/1,602 assertions, 39 Node tests with 34 passing and five opt-in skips), generated-runtime/package/drift/diff gates, and disposable port-7688 Neo4j (15 tests/242 assertions plus three packaged cases). The sole npm high advisory remains pre-existing transitive brace-expansion, not js-tiktoken. Independent review and user-managed merge remain external handoff steps; no release/version transition is included.
