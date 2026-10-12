# Adam 0.5.0 — Configurable memory derivations

- Status: in progress; stage-1 contracts and deterministic core implemented, host/graph wiring deferred
- Tracking issue: [`022`](issues/022-configurable-memory-derivations.md)
- Target: 0.5.0

This feature was originally proposed as an intermediary 0.4.2 release. Its agreed scope now includes model execution, multi-user authorization, persistent derivation history and a new Neo4j model, so it is planned as 0.5.0 instead. This document changes no installed runtime, package version or production schema.

## Problem

File-context retrieval cannot directly answer questions such as “What architectural or coding-style preferences have we established?” Relevant evidence spans sessions and producers, may have no file association, and may express the same preference in different words. Keyword search is not the desired outcome.

The user wants configurable, model-generated meta-observations grounded in existing observations and reflections, with YAML prompt guidance defining what to extract and how to classify it.

## Agreed outcome and execution boundary

An **Adam-owned internal subsystem** synthesizes the requesting user's own memories and directly grant-authorized memories into bounded, inspectable conclusions. Preferences are the first concrete use case, not a requirement for a general-purpose workflow engine.

Configuration presence opts into the feature. **Only explicit Pi refresh commands generate results.** Refresh reuses Pi's configured provider/model registry and authentication; by default it uses the model of the session executing the command. A configured model override resolves through that same registry. Credentials are never copied into derivation YAML, graph state or diagnostics. No separate mandatory consent toggle is required: configuration and explicit refresh must clearly identify the provider receiving memory content.

Pi and Claude MCP can read saved results without model calls. Claude generation/refresh is deferred until the Claude producer; there is no fallback model or unattended host-login policy to invent for Claude in this increment.

Outputs live in a separate derived namespace. They do not replace producer memories, rewrite source logs, silently become instructions, or affect normal session replication and file-context behavior. Generation is a separate tool-free model request, not continuation of the Pi conversation: it receives guidance/schema, selected authorized memories and provenance, and authorized prior conclusions for comparison. It receives neither the current conversation nor Pi's tools.

## Configuration and refresh scope

Use one user-wide file:

```text
$XDG_CONFIG_HOME/adam/derivations.yaml
# default: ~/.config/adam/derivations.yaml
```

No repository-local overrides initially. YAML contains named derivations, each with a version and guidance; category/evidence settings may refine the concrete derivation.

Illustrative shape, not yet a frozen schema:

```yaml
derivations:
  preferences:
    version: 1
    guidance: |
      Identify architectural and coding-style preferences.
      Distinguish explicit requests from inferred patterns.
      Treat scoped user corrections as strong evidence.
      Preserve uncertainty and highlight conflicts with attribution.
    # Omit model to use the executing Pi session's model.
    # Optional override, resolved through Pi's configured registry/auth:
    # model:
    #   provider: anthropic
    #   id: <configured-model-id>
    limits:
      # Optional lower input cap for cost control:
      # max_input_tokens: 100000
      max_output_tokens: 8192
      timeout_ms: 300000
    categories:
      architecture:
        guidance: Module boundaries, dependencies, abstraction and testing.
      style:
        guidance: Naming, formatting, idioms and organization.
    evidence:
      require_supporting_memory_ids: true
      inferred_min_distinct_occasions: 2
    scope:
      distinguish: [personal, repository, group, session]
    conflicts:
      policy: retain_and_highlight
```

Without a filter, refresh considers all authorized eligible memories. Each refresh accepts **at most one filter type and one value**:

- Repository: canonical normalized Git origin; association is evidence provenance, not an assertion that a conclusion applies to that repository.
- Session: canonical source-scoped session identity, avoiding equal host-local IDs across users/hosts.
- Language: language of linked files, not language inferred from prose. Selecting it explicitly accepts exclusion of unlinked memories. Without a language filter, unlinked memories remain eligible.

The [stage-1 contract](derivation-contract.md) pins language/extension mapping and normalized selector semantics. Repository matching uses source-session `WORKED_ON` association, not asserted applicability; the graph adapter must supply and verify those associations. No combined filters in the initial increment.

## Public interfaces

Pi refresh commands:

```text
/adam:derive
/adam:derive preferences
/adam:derive preferences --repository <git-origin>
/adam:derive preferences --session <canonical-session-id>
/adam:derive preferences --language <language>
```

`preferences` selects the named YAML configuration. Without a name, run all configured derivations sequentially. An optional single filter applies to each selected derivation. Each derivation makes at most one model call; an unchanged authorized cache needs none. Failure or an oversized input in one derivation does not prevent attempting the others; report outcomes individually.

Read-only surfaces:

```text
/adam:derivations
/adam:derivations preferences
adam_derivations
```

The tool is available in both Pi and Claude MCP. Listing/reading shows saved scope/filter, freshness, generating provider/model, conclusions and evidence. Default reads show the latest run per result identity; an explicit history option exposes older versions under the same authorization rules. The [stage-1 contract](derivation-contract.md) pins optional name/single-filter arguments, boolean history and bounded item limits. Rendering/paging and the host `--history` command boundary remain stage 4.

## Model input, budgets and completion

- One tool-free call per uncached named derivation; no automatic retries or multi-batch synthesis.
- Default input capacity follows the resolved model's context window, reserving output and prompt/provider overhead. A configured lower input cap can control cost; users may raise it within the model's actual capacity.
- Default output allowance is 8,192 tokens, respecting the model's supported maximum and configurable within it.
- Default model-call timeout is five minutes, configurable. Cancellation is requested on timeout but cannot guarantee immediate cessation of provider billing.
- Include guidance, schema, memory content, citation/provenance metadata and comparison context in input sizing. Stored producer `tokenCount` values alone are not exact counts for the resolved model or serialized request.
- If the complete selected input exceeds the configured budget or available context, **stop before calling the model** and ask the user to narrow scope or raise the cap. Do not sample, silently omit evidence, or make extra calls. Language-filter exclusion is an explicit scope choice, not sampling.
- Invalid or truncated output is incomplete: preserve the previous successful result. A validated empty result is successful and cached.

### Read-only sizing evidence

Production aggregates observed during this design discussion contained approximately 303k recorded content tokens in active observations/reflections, and 526k when expanding their dropped historical observation supports. Repository-scoped Adam evidence was approximately 149k including historical supports; Python-linked evidence approximately 74k and ClojureScript-linked evidence approximately 61k. These are stored token-count sums, exclude request/comparison/output overhead, and came from separate transactions while the graph was changing. They motivate model-aware preflight rather than an arbitrary 32k default; they are not a guaranteed future corpus size or exact model-token measurement.

## Classification, evidence and conflicts

- Explicit user requests or corrections can establish a preference without repetition, within their stated scope. A local correction is not automatically a universal rule.
- An unendorsed assistant implementation choice is not a user preference.
- Genuine repetition on distinct source occasions strengthens evidence, including within one session. Multiple summaries of the same occasion, or reflections repeating the same supporting observation, do not manufacture independent evidence. Sessions provide provenance, not the unit of independence.
- The default inferred threshold is two distinct occasions, configurable. Stage 1 unions verified stream-qualified activity anchors and expands reflection support without double-counting summaries; the later host/graph adapter must resolve those anchors from native source provenance, not assume every tool entry is a new occasion.
- Keep explicit versus inferred basis, category and subject/scope. Personal, repository, group and session assertions remain distinct; a shared repository or read grant never establishes collective agreement.
- Preserve uncertainty and distinguish user requests, assistant assertions and tool-verified facts. Code validates schema, authorization, citation membership, limits and checkable thresholds; valid citations alone do not prove a model's interpretation is true.
- Active observations and reflections are eligible. Dropped observations can be expanded as historical support for retained reflections, not independently promoted as current preferences.
- Empty/insufficient-evidence outcomes are valid and cached.
- **Highlight conflicts prominently**, with competing conclusions, attribution, scope and supporting/conflicting evidence. Different users disagreeing remains disagreement, not a collective preference.
- A clear user correction can supersede an earlier derived conclusion within compatible subject/scope, retaining history. Recency alone does not resolve ambiguity.
- Old conclusions can be comparison-only context in the same call, but never evidence for new conclusions. New support must cite original producer observations/reflections supplied as evidence. Check cache reuse first to avoid self-triggering endless regeneration.
- Memory and comparison text are untrusted data, never execution instructions or permission to obtain tools/credentials.

## User read grants

Directed from evidence owner to permitted reader:

```text
(owner:AdamUser)-[:GRANTS_MEMORY_ACCESS {
  permission: "read"
}]->(reader:AdamUser)
```

There is **no purpose property**. This is an all-memories read grant from that owner, not a derivation-only grant. No grant-management UI, repository-specific grants, federation or transitive access is included.

- Own memories require no self-grant; other owners require a direct grant. Alice granting Bob does not give Alice access to Bob or give Bob's readers access to Alice.
- The principal is Adam's configured identity, not an arbitrary caller-supplied user ID.
- Check authorization before selection limits, before model transmission, and on each result/history read. Recheck relevant permissions before publishing/exposing generated output.
- A result inherits restrictions from **every owner's content supplied to its run**, not only the citations the model chooses to return. Context and conflicting evidence count too.
- Comparison context also carries restrictions: retain the original memory dependencies behind older compared conclusions. Inherited dependencies affect authorization, not independent-evidence strength. They must not be mistaken for newly supplied source evidence.
- Revocation blocks subsequent reads of affected cached/history text and citations without requiring regeneration. It does not delete authoritative memory; a fresh explicit derivation may use remaining authorized inputs.
- Missing dependency/ownership resolution fails closed, never reduces the required permissions.

This is application-level authorization, not protection against unrestricted database credentials. Manual graph administration can manage grants for acceptance; this issue creates no production grants. Existing file-context read behavior is unchanged; new derivation paths implement the above authorization rules.

## Proposed Neo4j model

This is the agreed design proposal, not installed schema. Producer `AdamObservation`/`AdamReflection`, their identities and their existing relationships remain unchanged.

### Stable derivation identity

```text
(AdamUser)-[:OWNS_DERIVATION]->(AdamDerivation)
```

One deterministic identity per **requesting user + configuration name + normalized filter type/value**. An unfiltered result and a Python-filtered result are separate buckets. Configuration version and model do not change this identity. YAML remains configuration authority.

Proposed properties:

```text
AdamDerivation
  id, name, filterType, filterValue
  lastRefreshAttemptAt, lastRefreshOutcome, lastRefreshError
  refreshToken, refreshLeaseUntil                 # temporary operational fields
```

Filter types: `none`, `repository`, `session`, `language`. No filter has no value. Errors/status are bounded and sanitized, not raw prompt/provider-response logs.

### Completed generation runs and history

```text
(Derivation)-[:HAS_RUN]->(AdamDerivationRun)
(Derivation)-[:LATEST_RUN]->(AdamDerivationRun)
(Run)-[:USED_MEMORY]->(AdamObservation|AdamReflection)
```

Each successful generation gets a fresh run identity. Reused cache creates none; failed attempts create no completed run. A successful empty run can be latest.

Proposed properties:

```text
AdamDerivationRun
  id, createdAt
  configurationVersion, configurationFingerprint
  configurationSnapshotJson
  inputFingerprint, inputMemoryIds
  provider, modelId, inputMemoryCount
```

`configurationSnapshotJson` is a serialized validated configuration snapshot (guidance, evidence rules and effective limits), not a Neo4j map property or a copy of credentials. Effective provider/model are recorded separately. [Contract v1](derivation-contract.md) pins the deterministic snapshot/hash encoding; graph integration must preserve and repair its dependency relationships.

`USED_MEMORY` and recorded canonical `inputMemoryIds` cover all source-memory dependencies, including the dependencies inherited from comparison context, not just returned citations. Direct/historical evidence and inherited comparison dependencies must remain distinguishable in dependency bookkeeping; only genuine source occasions contribute to evidence strength. The compared run/conclusion identities must also be recorded for reproducibility.

There is **no `REQUIRES_ACCESS_TO` relationship**. Resolve required owners through:

```text
(Run)-[:USED_MEMORY]->(Memory)
(Session)-[:HAS_MEMORY]->(Memory)
(Owner:AdamUser)-[:OWNS]->(Session)
```

Recorded input IDs detect incomplete dependency traversal. Existing producer-memory projection deletes/recreates nodes, so implementation must preserve or reconstruct derivation evidence relationships from stable IDs and deny reads while dependencies are incomplete. Missing links must never imply fewer permission requirements.

### Conclusions

```text
(Run)-[:HAS_CONCLUSION]->(AdamDerivedObservation)
(Conclusion)-[:SUPPORTED_BY]->(AdamObservation|AdamReflection)
(Conclusion)-[:CONFLICTING_EVIDENCE]->(AdamObservation|AdamReflection)
(NewConclusion)-[:CONTRADICTS]->(OtherConclusion)
(NewConclusion)-[:SUPERSEDES]->(OldConclusion)
```

Proposed properties:

```text
AdamDerivedObservation
  id, content, category, basis
  scopeType, scopeSubjectIds
  supportingMemoryIds, conflictingMemoryIds
```

Conclusion identity is scoped to its run. Equal wording across runs remains distinct historical evidence. Canonical subject IDs identify the relevant user(s), repository or session; exact group/scope representation still needs validation rules. Every conclusion has nonempty validated support from that run's source evidence. Access derives from the entire run, not only the conclusion's citations.

Optional `CONTRADICTS` can connect conclusions within a run or across history, with compatible subject/scope and evidence. It does not inherently mean supersession. `SUPERSEDES` represents a clear scoped correction, not merely a newer run. Check authorization for both endpoints before exposing either relationship. Prior source/conclusion records are retained.

Stored supporting/conflicting IDs allow relationship restoration after memory rebuilds without duplicating memory content. Unresolved dependencies produce evidence-unavailable status, never fabricated support or permissive reads.

### Latest, freshness and atomic publication

- Only complete, validated generation is published. In one write transaction save the run/conclusions/dependencies and move `LATEST_RUN`; retain prior runs and mark them superseded by the latest-run relationship/state.
- **Latest and current are different.** If evidence/configuration changed during generation, the new completed run is still latest but marked outdated using the snapshot it processed. No automatic retry.
- Current evidence/configuration changes invalidate freshness. Reads may show authorized outdated results with a clear label, never as current.
- Switching the Pi session's model alone does **not** mark an existing result stale. The next explicit refresh resolves the model and includes it in cache comparison. Changing configured guidance/settings still affects configuration freshness.
- Reuse requires matching selected evidence/effective configuration/resolved provider/model and current authorization/dependency completeness. Successful empty results are reusable.
- Comparison history must not enter cache identity in a way that each new result invalidates itself. Record comparison provenance separately from the reuse decision; permission checks still include all inherited dependencies.
- Failed, cancelled, invalid, truncated or timed-out attempts preserve the previous result with truthful status. No attempt claims new input was processed when it was not.

### Constraints and refresh lease

All three new labels have unique `id` constraints. URN normalization/hash encoding, indexes and bounded dependency representation must be pinned before implementation.

Only one active refresh per stable derivation identity. Acquire a temporary token/expiry atomically; another request reports already-refreshing. Different derivation/filter identities are independent. The lock is **not** Adam's notification-worker lease and must not block normal mirrors/imports.

Model work happens outside Neo4j transactions, avoiding transaction retries duplicating calls. The lease covers the configured timeout plus publication time, with renewal as needed. Publication verifies the live token; expiry/crash recovery cannot let an old token overwrite newer work. Failures release their own lease and preserve prior results. Reads can continue to use the previous completed result under current permissions.

## Acceptance criteria

Behavior reviewed and agreed one by one with the user. Checkboxes remain implementation evidence, not markers of design approval.

- [ ] User-wide named YAML configuration is validated before model work. Invalid syntax, unknown options, invalid limits and unsupported model selections fail visibly; no repository-local override, arbitrary YAML execution or credential leakage.
- [ ] Pi refresh uses configured Pi providers/authentication and defaults to the executing session model. Each run records resolved provider/model and validated configuration. Lifecycle/read paths never generate; Claude refresh is absent.
- [ ] Named refresh and sequential all-config refresh accept at most one repository/session/language filter. Language uses linked files, explicitly excluding unlinked memories only for that selection. Own and directly grant-authorized inputs are filtered before limits.
- [ ] Oversized complete input stops before calling the model with actionable scope/budget guidance. No sampling, batching or automatic retries. Context-aware input sizing reserves output/overhead; output truncation/timeout is incomplete, not a successful replacement.
- [ ] Grant tests prove directional/non-transitive access, no grant-derived consensus, denial without permission, all-input and comparison-dependency restrictions, revocation on subsequent reads and fail-closed unresolved ownership/dependencies.
- [ ] Conclusions distinguish explicit/inferred basis, category and subject/scope, with validated nonempty support. Tests cover scoped requests/corrections, unendorsed assistant choices, insufficient evidence, genuinely repeated occasions and duplicated summaries/reflection support.
- [ ] Conflicts are prominently highlighted with attribution/evidence; clear scoped corrections are distinguished from unresolved disagreement. Tested `CONTRADICTS` links work within/across runs; `SUPERSEDES` does not arise from recency alone.
- [ ] Provenance resolves through producer/source session and underlying observations/reflections where authorized and available. Missing source is reported honestly. Memory rebuilds restore run/conclusion relationships from IDs without relaxing permissions.
- [ ] Result identities separate users/names/filters. Runs/conclusions retain history, configuration snapshots and fingerprints. Default reads show latest; explicit history is authorized and bounded. Empty successful runs are cached.
- [ ] Matching authorized input/configuration/model reuses cache. Changing session model affects only next refresh, not read freshness. Mid-generation evidence/configuration changes produce latest-but-outdated results, not automatic retry or falsely current status.
- [ ] Same-identity concurrent refresh reports busy; independent identities can proceed. Token-fenced atomic publication and recoverable leases prevent stale writers and preserve prior completed results after failures. No model call occurs inside a retryable database transaction.
- [ ] Pi and Claude MCP expose equivalent bounded read-only results and truncation/status metadata. Model/configuration failure does not affect source logs, producer memories, normal mirroring, imports, notification drains or existing file-context behavior.
- [ ] Normal tests use synthetic memories and a fake executor. Real-model probes require explicit opt-in/resource bounds; live database tests use disposable acknowledged endpoints, independent identities and exact resource-scoped cleanup.

## Staged delivery

Deliver this minor-release scope through reviewable implementation increments; exact tickets and boundaries follow the remaining implementation-contract decisions.

1. **Contracts and deterministic core:** implemented under [issue 023](issues/023-derivation-contracts-and-core.md). [Contract v1](derivation-contract.md) freezes YAML/read schemas, selector normalization, dependency roles, source-occasion accounting, request sizing and result fingerprints, with synthetic/fake-executor coverage. No host commands, graph storage or real-model adapter are enabled.
2. **Graph storage and authorization:** add derivation/run/conclusion identities, indexes, grants, history, fenced refresh leases and atomic publication. Prove revocation, comparison inheritance, memory-rebuild repair and fail-closed missing dependencies on a disposable graph.
3. **Pi generation tracer:** wire explicit named refresh through Pi's model registry/authentication, context-aware budgets, timeout/cancellation and the one-call policy; then sequential all-config refresh with independent outcomes.
4. **Reads and interpretation:** implement Pi command/tool and Claude read-only MCP parity, bounded latest/history results, freshness, prominent conflict/correction reporting and provenance traversal.
5. **Acceptance and release:** complete deterministic, packaged-host and disposable graph restart/concurrency acceptance, plus separately approved real-model quality/cost probes. Only the final release-preparation increment updates versions/changelog for 0.5.0.

Keep existing session replication, producer projection and notification-worker behavior independently functional throughout. PRs remain user-merged with history preserved; merging this documentation does not initiate implementation, migrations or publication.

## Remaining implementation-contract details

1. Wire the frozen YAML/selector contracts and optional model override resolution through the Pi SDK; obtain verified graph provenance/occasion anchors for normalized rows.
2. Input serialization/token sizing/headroom for supported models, exact occasion deduplication and support expansion rules. Provider cost guards cannot promise a strict spend ceiling.
3. Read/tool arguments and history selection, bounded pagination/truncation, and precise user/group/scope subject validation.
4. Canonical URN/hash encoding, configuration/input snapshots and comparison dependency roles, freshness checks, rebuild-safe relationship repair and indexes.
5. Atomic lease acquisition/renewal/fencing and cancellation behavior across processes; publication-time authorization/freshness validation and sanitized status handling.
6. Implementation breakdown and release readiness. This document authorizes no runtime/schema migration, production grant, version bump, tag, merge or publication by itself.

## Validation

Stage 1 implements the storage-neutral core with deterministic fake-executor tests; no production generation command, graph schema or MCP derivation tool is registered. The sizing query was read-only and made no model call or graph mutation. Later stages require disposable graph restart/concurrency/rebuild validation and both host read adapters. Real-model quality evaluation remains separate opt-in acceptance.

## Notes

Requested after rejecting keyword search in favor of configurable model-generated understanding. Architectural/style preferences are the first use case; named configurations reuse a concrete subsystem rather than creating a generic workflow framework. The separate Claude producer remains independent and is not required to read the existing Pi-generated evidence.
