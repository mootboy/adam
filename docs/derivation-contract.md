# Derivation core contract v1

This freezes the storage-neutral stage-1 interfaces for the [0.5.0 plan](plan-0.5.0.md). The implementation is in `src/adam/derivation/`; no command/tool, production model adapter, graph schema or background generation is registered by this increment.

## Development structure

`config.cljs` owns YAML parsing and its local Clojure specs. `core.cljs` keeps selector, evidence and output specs beside the refresh/read behavior that uses them; there are no separate selector/evidence modules or generic validation framework. YAML is a syntax parser, not the domain validator. Specs are normative, and failed checks never attach `explain-data` containing source values.

Each named refresh reads its source snapshot once through the boundary callback. All-config refresh deliberately uses a per-name process boundary: read the first snapshot, finish its model call/outcome, then read the next snapshot. It does not preload every recipe's evidence, and never rereads mutable source state within a named refresh. Selection/role accumulation, fingerprints, sizing and output processing are immutable transformations. Run identity/time are captured before execution; model execution and timeout cleanup are explicit effects. The returned candidate is published only by the later storage stage. Publication's transactional permission/freshness checks are a separate process, not permission to reread mutable state within derivation processing.

## Configuration

`adam.derivation.config/parse-config` accepts YAML 1.2 and returns named compiled recipes in deterministic name order. `config-path` resolves an explicitly supplied absolute configuration root to `adam/derivations.yaml`; host file loading and missing-file disablement are wired in the Pi increment, not here.

- Top-level field: `derivations`, with 1–64 names matching `[a-z][a-z0-9_-]{0,63}`.
- Required recipe fields: positive safe-integer `version`, nonblank `guidance` (at most 64 KiB UTF-8).
- Optional `model`: exactly `provider` and `id`, resolved through Pi's registry/authentication later. Absence means executing-session model. The supplied resolved model must match an explicit override.
- Optional `limits`: positive safe integers `max_input_tokens`, `max_output_tokens`, `timeout_ms`. Output defaults to 8192 and is clamped to the model's supported maximum; timeout defaults to 300000 and cannot exceed Node's native timer maximum, 2147483647 ms.
- Optional `categories`: named entries containing only nonblank `guidance` (at most 8 KiB each); at most 64 categories. When present/nonempty, output categories must be among them.
- Optional `evidence`: `require_supporting_memory_ids` may only be true; `inferred_min_distinct_occasions` is positive and defaults to 2.
- Optional `scope.distinguish`: a nonempty unique subset of `personal`, `repository`, `group`, `session`; default is all four.
- Optional `conflicts.policy`: only `retain_and_highlight` is supported.

A configuration is at most 256 KiB UTF-8. Unknown fields, duplicate keys, multiple YAML documents, aliases, unsupported tags and invalid types/limits are rejected with sanitized errors. There are no inline credential fields, executable YAML tags, repo-local overrides or silently ignored options. These are configuration/parser safety bounds, not a memory-corpus sampling limit.

## Requests and selectors

`core/refresh-configured!` accepts a request map with optional `name` and at most one of `repository`, `session`, `language`. The eventual host command tokenizer must preserve argument values; this core accepts structured requests, not an ad-hoc shell parser.

- Git origins normalize through Adam's existing credential-stripping remote normalization. SSH/HTTPS equivalents select the same bucket. Public selectors require Git remote URL/SCP syntax; ambiguous bare shorthand such as `foo/bar` (or canonical host/path) is rejected before loading evidence. Canonical host/path is accepted only in trusted memory rows. Repository filter matching normalizes both the selector and the source session's `WORKED_ON` origins supplied in the snapshot (raw remotes or canonical host/path), not inferred applicability. Serialized memory origins are credential-stripped canonical values.
- Session requires the full `urn:adam:session:<user>:<pi|claude-code>:<session>` identity. Ownership/access is checked separately, never implied by this string.
- No filter normalizes to `{type: none}` for refresh.
- Language names are case-insensitive; aliases normalize to canonical names. File extensions are matched case-insensitively in the final basename of verified linked repository-relative paths, never memory prose. Extensionless names such as `go`, `c` or `rs`, and dotted parent directories, do not count as extensions. Reflections receive linked paths through supporting observations in the later graph adapter.

Initial language mapping:

| Language | Extensions | Aliases |
|---|---|---|
| python | py, pyi | py |
| javascript | js, jsx, mjs, cjs | js |
| typescript | ts, tsx, mts, cts | ts |
| clojure | clj, cljc | clj |
| clojurescript | cljs, cljc | cljs |
| edn | edn | — |
| java | java | — |
| kotlin | kt, kts | — |
| rust | rs | — |
| go | go | — |
| ruby | rb | — |
| c | c, h | — |
| cpp | cpp, cc, cxx, hpp, h | c++ |
| shell | sh, bash, zsh | sh |
| sql | sql | — |
| html | html, htm | — |
| css | css | — |
| json | json | — |
| yaml | yaml, yml | yml |
| markdown | md, markdown | md |

Shared extensions match both languages. Unknown languages fail rather than silently creating an empty scope. Selecting a language excludes unlinked candidate memories explicitly; no filter retains eligible unlinked memories. Historical supporting observations can accompany selected reflections as provenance, not independent candidates.

`core/normalize-read` accepts the same optional selector/name fields plus boolean `history` (default false) and integer `limit` (default 10, maximum 20). No read filter means list matching buckets of all filter types, **not** just the unfiltered bucket. There is no caller-supplied identity/provider/generation field. The read tool's fixed output envelope will be at most 200 lines or 12,000 UTF-8 bytes, with explicit omission/truncation metadata; actual paging/rendering is stage 4. Schema normalization already rejects unknown and combined selectors.

## Trusted normalized evidence seam

The future graph/host adapter supplies the configured `reader-id`, direct grants and unique normalized memory rows. It must obtain ownership and source facts from the graph/authoritative entries, not from caller arguments or claims inside memory text. Source IDs are canonical stable graph identities in production; synthetic tests use opaque fixture IDs.

Memory rows contain `id`, `owner-id`, `session-id`, `kind` (`observation`/`reflection`), `producer`, `content`, optional `dropped?`, source entries/contexts, repository origins/IDs and linked paths. Reflections additionally have canonical `supporting-memory-ids`. Source content remains nonblank opaque text, including terminal control characters; JSON serialization escapes them rather than rejecting or altering the memory. Its bound is complete-request context sizing, not the identifier/guidance text spec. This core does not query Neo4j, discover users or infer ownership from prose.

Direct grants contain `owner-id`, `reader-id`, `permission: "read"`; grants do not reverse or transitively expand. Authorization precedes selector matching and serialization. Dropped observations are not candidates; a selected reflection expands its observation supports. Missing/unauthorized support fails closed rather than silently weakening the evidence.

### Source occasions

`source-occasions` contain verified `{session-id, stream-id, entry-id}` anchors for the original activity, not the memory recording entry or a generated summary. A fork/import may retain an original-session anchor different from its recording session; adapters must verify that origin, not relabel a copied prefix as fresh activity. Host adapters must resolve shared user-request/activity anchors so tool-call/results and repeated summaries of one activity share an occasion. Subagent handbacks and compaction summaries are not fresh user confirmations; adapters must carry the original activity anchor where resolvable.

The pure core unions `(source-kind, original host session ID, stream ID, entry ID)` anchors of supporting observations, expanding reflections. The canonical session's ownership namespace is excluded from independence counting so copying the same physical host event across users does not manufacture repetition; authorization still uses full canonical memory/owner identities. Distinct occasions in one session strengthen an inference; repeating the same anchor does not. Missing/unresolved anchors contribute zero, never fabricated independence. Host-specific anchor resolution is a required graph/host integration task; no Pi/Claude raw transcript traversal is silently implemented in this stage.

### Comparison and completeness

One optional prior-run snapshot supplies `id`, `input-memory-ids`, `conclusions`. All original dependencies must resolve and be authorized before comparison text is serialized. Nonempty conclusions cannot claim zero dependencies. Prior conclusions must satisfy the closed conclusion spec, with known string basis/scope enums normalized to keywords. Every supporting or conflicting memory ID must occur in the prior run's declared `input-memory-ids`; incomplete declarations withhold comparison rather than silently reducing inherited permissions. The same completeness spec protects cache reuse and saved-result freshness. If comparison is unavailable (including malformed comparison-only source rows), omit its text, report `comparison-unavailable`, and permit a fresh source-only derivation; do not expose private content or block remaining authorized evidence indefinitely.

Roles are explicit: `evidence`, `historical-support`, `comparison-dependency`. The dependency union covers all three, but inherited comparison dependencies do not become new source support or independent occasions. Comparison payload is minimized to known conclusion fields; unrelated prior-run metadata is never forwarded. Recorded IDs/hashes/roles permit later rebuild-safe relationship repair and revocation checks.

## Separate request and sizing

`core/prepare-refresh` takes a compiled recipe, normalized filter, trusted evidence context and a resolved model with `provider`, `id`, `context-window`, `max-output-tokens`. An injected synchronous `measure-input(payload, model)` receives the **complete serialized payload**, including guidance, schema, source content/provenance and authorized comparison conclusions, plus credential-free resolved model capabilities; it must return a nonnegative integer count/bound for that model. No stored producer token count or current Pi conversation substitutes for request sizing.

The stage-1 arithmetic reserves the effective output allowance plus 1024 provider-envelope headroom tokens. The input cap is the smaller of remaining context and an optional configured input cap. Model-aware tokenization/conservative measurement and verification of sufficient provider headroom are the real Pi adapter's responsibility in stage 3; this fake-executor stage does not claim to certify token counts for any hosted model. No real tokenizer/provider is installed here.

Oversized input raises `input-too-large` with measured/available counts and actionable scope/budget guidance before execution. Sequential configured refresh retains those validated numeric counts in the named failure result, but never arbitrary exception data or provider messages. Invalid measurement fails. There is no truncation, sampling, batching or automatic retry. Effective limits and measured input are recorded separately from the requested recipe.

## Output and fake-executor seam

`core/output-schema` describes the provider's wire format, not a schema-library validator. Local Clojure specs enforce closed output objects, classifications and context-dependent source/scope/link constraints. `decode-response` parses JSON with exactly `conclusions` and `links` and takes the completion reason separately from trusted executor metadata. Model JSON cannot claim its own successful finish reason.

A conclusion has:

```json
{
  "id": "local-output-id",
  "content": "A scoped, evidence-bearing conclusion",
  "category": "architecture",
  "basis": "explicit",
  "scope-type": "personal",
  "scope-subject-ids": ["<canonical-subject-id>"],
  "supporting-memory-ids": ["<selected-source-memory-id>"],
  "conflicting-memory-ids": []
}
```

`basis` is `explicit`/`inferred`; subjects match the configured scope. Personal/session/repository have one supplied subject; groups have at least two distinct supplied users. Comparison-only subjects cannot manufacture new source-supported scope. Conclusion content is bounded to 64 KiB UTF-8; original source memory content is governed by aggregate request/context sizing, not a protocol-v1 per-item cap. Support is nonempty, unique and selected; conflict citations can refer to source evidence/historical support, never inaccessible or invented records. Inferred conclusions must meet the verified occasion threshold. These structural rules do not prove semantic truth or whether an explicit request actually corrects another; evaluated interpretation remains required.

Links have `relation` (`contradicts`/`supersedes`), `from` and `to`. The origin is a new local conclusion. Contradiction can target another new conclusion or an authorized prior one; supersession targets a prior conclusion. Subjects and scope must match, self-links and unknown targets are invalid. Interpretation must distinguish a genuine correction from mere recency.

An injected executor receives model metadata, payload, output limit, timeout and AbortSignal, with no tools or session history. It returns normalized `{finish-reason: stop, conclusions, links}` (keyword values internally). Non-stop/truncated, malformed, unsupported or ungrounded output is rejected. A complete empty result is valid.

`refresh!` makes at most one executor call, requests abort on timeout and yields a completed candidate run only after validation. It does not publish anything or implement cross-process locking; graph publication/leases are stage 2. `refresh-configured!` loads and processes selected recipes sequentially, continuing after independent failures and reporting sanitized reason codes, never raw provider errors. Unknown named configurations fail before loading/execution.

## Identity, snapshots and freshness

Version-1 fingerprint encoding is recursively key-sorted JSON; sets/unordered evidence lists are sorted, memory rows sort by ID. SHA-256 is applied to UTF-8 bytes. This is an internal derivation snapshot encoding, not a producer memory-protocol envelope or an imposed JCS requirement.

- Derivation ID: `urn:adam:derivation:<SHA256([reader-id, name, normalized-filter])>`.
- Run ID: `urn:adam:derivation-run:<fresh-UUID>`.
- Conclusion ID: `urn:adam:derived-observation:<SHA256([run-id, local-output-id])>`; local link targets are rewritten accordingly.

Input fingerprints include the selected source state and scope, not the newly generated comparison result. Per-dependency fingerprints/roles cover comparison inheritance for access/freshness. Configuration fingerprints cover the compiled recipe and internal policy version (currently 1); the resolved model key participates separately in refresh reuse. Bumping the policy version makes prior results outdated without changing stable derivation identity. Runs retain requested configuration/effective-limit snapshots, generating provider/model, dependency IDs/hashes/roles and comparison-run provenance, not copies of original memory content.

A cache is reused only if its derivation identity and source/configuration/model match, dependency completeness/authorization holds and dependency fingerprints match. Invalid requesting identities and malformed saved dependency declarations are `unavailable`, not merely outdated. Successful empty results are reusable; input ordering does not create a new call. `freshness` is evaluated in the original derivation identity context: changed source/configuration is `outdated`, missing/unauthorized dependencies are `unavailable`, and the current session model alone does not make a saved run stale. Cross-reader query visibility and graph-owner traversal are implemented and tested in stage 2, not inferred from this pure helper.

Latest publication is a graph concern: a completed run remains a candidate even if its inputs changed mid-call; the later publisher records latest-but-outdated rather than retrying. No core callback writes sources, producer records, graph nodes, locks or Pi boundary drafts.

## Validation boundary

Normal tests exercise the configuration and refresh/read interfaces using synthetic rows and fake executor/measurement callbacks. Parameterized cases cover evidence, sharing, comparison and selectors end to end; seeded `test.check` properties cover YAML round trips, origin equivalence and input-order invariance. `test.check` is a compiler/test dependency, not a runtime requirement. This stage requires no live Neo4j, real model, inherited credentials or production mutations. Source-occasion extraction, provider certification, production commands and MCP registration remain later work. The concrete stage-2 storage boundary below is now available for direct tests; it is not registered or initialized by either host.

## Concrete graph boundary (stage 2, in progress)

`adam.derivation.store/with-driver` captures a host-resolved principal and database. No operation accepts a caller-supplied reader. `ensure-schema!` explicitly creates the three derived-label uniqueness constraints; no host initializes them in this slice. Every write checks those actual constraints through `SHOW CONSTRAINTS` before data operations and fails with `schema-unavailable` if any are missing. Stage 3 must initialize schema before acquiring a refresh; successful prior initialization is not cached as permission to write after constraints disappear.

`acquire-refresh!`, `renew-refresh!` and `release-refresh!` use a per-derivation server-time lease, not the notification-worker lease. Lease windows are explicit safe integers from 1 ms through 24 hours, with renewal for longer work. Token allocation precedes transaction retries. Lease durations are transmitted as Neo4j integers so stored deadlines remain integers. Busy, unavailable and fenced outcomes are content-free.

`publish-refresh!` takes a named selector, live token and complete validated core candidate. Closed specs validate run identity, configuration/model snapshots, fingerprints, role completeness and conclusions before opening a session. Creation times use canonical UTC with exactly three fractional digits (`Date.toISOString`), making chronological string ordering unambiguous; invalid calendar dates are rejected. Comparison-dependency roles require a non-null comparison-run ID. Saved diagnostic enum values normalize on decoding, including `comparison-unavailable`. Transactional ownership/grant checks cover **every** declared input, including unreturned evidence and inherited comparison dependencies. The original compared run must belong to this derivation, have complete inherited dependency roles and remain authorized. Comparison-only inputs cannot become new support/conflict evidence. Links require compatible authorized endpoints.

Publication creates the run, conclusions, source dependencies and optional contradiction/supersession relationships and replaces `LATEST_RUN` in one token-fenced transaction. Empty successful runs retain history. Constraint or permission failures roll back all publication writes; failed attempts create no completed run and the caller must release its token or allow expiry. Stage-3 controllers must release their own token in failure/cancellation cleanup, including publication rejection, and preserve prior results when release itself fails. A crash may leave the last attempt recorded as `refreshing`; status must consult the deadline rather than claiming expired work is still active. No model work occurs inside transactions, and original memories/session state are never changed. The candidate's processed fingerprints are retained even if source content changed; `available` below is **not** a current-freshness claim.

`read-runs!` accepts the frozen read request and returns `{items, truncated?}`. Reads default to latest across matching filter buckets; explicit history is bounded to 10 rows by default, at most 20, with a one-row omission probe. Each available item contains name, normalized filter and the saved run. Denied items expose only run ID and `unauthorized` or `unresolved-evidence`, never conclusion/configuration text. Every read resolves current one-hop read grants, unique source-session ownership and all recorded inputs; recorded IDs, actual source relationships and conclusion properties must remain complete. Revocation applies to latest and history without regeneration.

`repair-dependencies!` explicitly rebuilds `USED_MEMORY`, `SUPPORTED_BY` and `CONFLICTING_EVIDENCE` from stored stable IDs for the bounded matching history page, returning per-run repair/denial outcomes and `truncated?`. It captures all candidate/dependency state before writes, denies unresolved/unauthorized records without mutation, and does not generate or alter source records. Recreated source nodes with the same IDs remain unreadable until the repair restores complete links. Large-history pagination and host scheduling remain later integration work; truncation must not be represented as whole-history repair.

`runJson` is the authoritative immutable dependency-role record. `USED_MEMORY.roles` stores the sorted role names for graph-level provenance and is reconstructed during repair; missing or altered cached edge roles never reduce the recorded authorization requirements. Memory ID lookups rely on source-foundation `AdamObservation` and `AdamReflection` uniqueness indexes. Disposable Neo4j 5.26 EXPLAIN of the four affected complete queries (including seven memory lookup occurrences) uses both label-specific unique index seeks, not memory label/all-node scans; no speculative query rewrite is required.

`load-input!` captures a principal-bound, read-only generation snapshot in one managed read transaction. It validates a named selector before opening a session, loads all own/directly granted memories without a LIMIT or file requirement, includes dropped support records, and preserves source/recording, session repository and linked-file metadata. Reflection support IDs are producer/source qualified from their owning session. Ambiguous ownership or inconsistent host identity denies the snapshot. Recipe filtering and complete-request sizing remain pure core responsibilities. The previous latest lookup is restricted to the exact normalized derivation identity, not the latest arbitrary filter bucket; unavailable prior text is withheld. There is no model work or graph write in this boundary.

Native source-activity verification is still unfinished: snapshot `source-occasions` are deliberately empty, not synthesized from citation strings. The boundary is not wired into hosts and does not yet satisfy full stage-2 acceptance. Stage-4 provenance must read authoritative dependency roles from runJson, never infer them from informational edge roles. If schema is lost, failure release cannot write either; recovery requires restored constraints or lease expiry. The schema check currently targets the supported Neo4j 5.x constraint vocabulary; a future Cypher-25/2025.x upgrade must validate its renamed node-uniqueness type before cutover.

Configuration loading and verified original source occasions remain unfinished stage-2 work. Provider execution and host registration remain stages 3–4.
