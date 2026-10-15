# 030 — Complete derivation reads and explicit evidence inspection

Status: done

## Problem

Saved reads interleaved full provenance with conclusions, then clipped the combined output at the file-context envelope limit. Agents could not see every saved conclusion or trailing conflict links. Raising the ceiling would only postpone this failure.

## Contract

- Default ordinary-text reads show saved conclusion IDs/text, explicit/inferred basis, scope, evidence/conflict counts and scoped contradiction/supersession links. No custom UI, automatic generation or regeneration.
- Detailed supporting/conflicting memory text and producer/owner/source locators are a separate explicit action in the existing surface: `/adam:derivations [name] --evidence <conclusion-ID>` or `adam_derivations({evidence: "<conclusion-ID>"})`. Optional matching name/depth/filter selectors still apply. Exact ID lookup finds the owning authorized run even when it is no longer latest; it is not limited to the latest/history window. Evidence is memory text, not raw host transcripts.
- All selected output is available through lossless continuation pages, each within 200 lines/12,000 UTF-8 bytes. `details.next-cursor`/MCP `structuredContent.next-cursor` is null at completion. Follow non-null cursors with unchanged arguments plus `cursor`; command equivalent is `--cursor <token>`. Unicode is never split inside a surrogate pair. Segmentation may occur inside long quoted prose and must be reassembled before presentation.
- A cursor identifies rendered output and offset, not authorization. Every continuation reloads and reauthorizes; changed output rejects the cursor and requires restarting without it. No cached private response or new process/worker is introduced.
- `truncated?` now means additional saved runs were outside the requested bounded result selection (default 10, maximum 20), not a clipped body. Continuations finish the selected output; they do not advance beyond that run selection. Unbounded history browsing remains separate work.
- Freshness checking retains its existing full-corpus cost on every page. Do not poll or parallelize. Complete conclusions normally fit one page now that provenance is separate.

## Acceptance

- [x] Long Unicode conclusions reconstruct exactly across pages without an output-truncated notice.
- [x] Default output excludes evidence text/locators; explicit evidence reconstructs memory text and provenance.
- [x] CLI/tool parity and strict MCP schema, including nullable continuation and evidence ID.
- [x] Invalid arguments fail before IO; stale cursors and revoked access cannot return prior content.
- [x] Disposable live lookup finds older conclusions with limit 1, rechecks run-wide grants after streaming, and withholds raw host transcript content.
- [x] Clean CI/runtime drift, 261 ClojureScript tests/1,648 assertions, 39 Node tests (34 passing/five opt-in skips); disposable port-7688 Neo4j 15 tests/248 assertions plus three packaged worker/cleanup/Pi-MCP cases. Packaged Pi/MCP verify summary/evidence parity without additional fake generations.

Citation ordering is canonicalized so unchanged graph collection order does not invalidate evidence cursors. JSON quoting also escapes terminal C1 and directional controls in memory text. No production access, schema/migration change or real model call occurred during implementation validation.
