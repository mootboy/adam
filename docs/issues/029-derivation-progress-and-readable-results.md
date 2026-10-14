# 029 — Derivation progress and readable saved results

Status: done

## Request

Explicit derivation commands were silent until completion, and saved reads looked like raw diagnostic output rather than a thematic summary.

## Scope

- Immediate generation acknowledgement and namespaced footer progress for idle waiting, model/authentication, evidence loading, generation, reuse and publication. Clear status on every completion/failure/cancellation path; do not take over Pi's main-agent working indicator.
- Shared summary-first Markdown for Pi/MCP reads: thematic prose, explicit/inferred labels, scope, evidence counts and prominent conflicts/history relations.
- Optional `--evidence` / `evidence: true` appends full authorized provenance after all summaries. Presentation flags never enter graph selectors, identities or fingerprints.
- Interactive `/adam:derivations` uses a temporary scrollable Markdown screen, with arrows/PageUp/PageDown/Home/End and Esc/q/Ctrl-C close. Noninteractive contexts retain text notifications; agent tools return text rather than opening screens.
- No session entry/message append, prompt injection, generation change, automatic model call, graph mutation, or authorization relaxation. Model prose is escaped and blockquoted to prevent forged metadata/headings or active Markdown links.

## Validation

Interface regressions cover summary/detail ordering, presentation-flag stripping before storage, spoof-resistant prose, footer completion cleanup and the real TUI component at narrow/wide widths with Unicode and keyboard closure. Packaged disposable Pi acceptance exercises the compiled Markdown viewer with fake model execution. Final validation passed: cache-cleared release builds, 261 ClojureScript tests with 1,632 assertions, 39 Node tests (34 passing/five opt-in skips), exact-tarball packaging and disposable port-7688 Neo4j (15 tests/242 assertions plus three packaged cases). Packaged Pi exercises the real compiled viewer and MCP accepts opt-in evidence; no real model calls or production mutations were performed. User visual acceptance and independent review remain pending. No version or release change.
