# Adam 0.3.3 plan: active file-memory retrieval

Status: implemented

Adam 0.3.3 is a patch release that removes superseded observations from normal file-memory retrieval. Adam already preserves observational-memory drop tombstones and projects `AdamObservation.dropped`; however, the current read query returns those observations, labels them as dropped, and lets them consume the bounded result budget. The default read surface should instead return active memories while retaining dropped records in the graph for provenance and audit.

## Goal

Both explicit file-memory surfaces must omit dropped observations before applying their result limits:

- Pi command: `/adam:context`
- Pi and Claude agent tool: `adam_file_context`

The shared storage query is the enforcement point, so every host receives the same behavior and superseded observations do not occupy one of the 20 command results or 10 displayed tool results.

## Retrieval semantics

### Observations

An `AdamObservation` is active when its `dropped` property is absent, `null`, or `false`. Normal file-memory queries return only active observations.

An observation with `dropped = true` remains stored with its existing `HAS_MEMORY`, `ABOUT`, `SOURCED_FROM`, and provenance relationships. Filtering is read-layer behavior, not deletion, migration, or projection cleanup.

### Reflections

Reflections remain retrievable when they are relevant to the requested file, even when their supporting observations are dropped. Consolidation commonly creates a reflection and then drops the observations it supersedes; filtering a reflection merely because its support is tombstoned would remove the replacement memory as well as its sources.

The reflection traversal therefore remains:

```text
AdamReflection-[:SUPPORTED_BY]->AdamObservation-[:ABOUT]->AdamCodeFile
```

The supporting observation's dropped state does not exclude the reflection.

### Bounds and ordering

Dropped observations must be excluded in the Neo4j query before the combined result is ordered and limited. Filtering only after storage retrieval is incorrect because dropped observations would still consume the bounded query budget and could crowd out active observations or reflections.

Existing deterministic ordering, user-owned session provenance, repository/file identity, origin lookup, content compaction, and output line/byte limits remain unchanged.

## Implementation

Update the observation branch of `FileMemoryQueryStore/query-file-memory!` in `src/adam/replica/neo4j.cljs` to require an active observation, treating a missing property as active. Keep the reflection branch independent of the support observation's dropped state.

The intended predicate is equivalent to:

```cypher
WHERE coalesce(observation.dropped, false) = false
```

The predicate must occur before the `UNION ALL` result is ordered and limited. No filtering is added to rendering, and the existing dropped marker may remain available for diagnostics or future explicit historical surfaces even though default queries no longer return dropped observations.

No graph schema, extractor version, session identity version, code-memory version, or restart-time rebuild is required. Existing projected data already records the tombstone state needed by the query.

## Acceptance criteria

- A file-linked observation with `dropped = true` is absent from `/adam:context` results.
- The same dropped observation is absent from `adam_file_context` in both Pi and Claude.
- An observation with `dropped = false` remains retrievable.
- A legacy observation with no `dropped` property remains retrievable.
- A reflection supported only by dropped observations remains retrievable for the associated file.
- Dropped observations are filtered before the requested result limit and do not consume result slots.
- Tool truncation reflects active query results rather than hidden dropped rows.
- Dropped observation nodes and provenance relationships remain unchanged in Neo4j.
- User isolation through owned session and memory provenance remains unchanged.
- Local-path and checkout-independent origin queries produce the same filtering behavior.

## Validation

### Deterministic

Add query-shape coverage proving that:

- the observation branch filters `dropped = true` while treating an absent property as active;
- the reflection branch does not filter on the supporting observation's dropped state; and
- the shared query service continues forwarding its requested bound to storage.

Retain rendering coverage for dropped observations as a diagnostic capability unless that behavior is deliberately removed in a later contract change.

### Live Neo4j

Create active, legacy, and dropped observations for one canonical file, plus a reflection supported by the dropped observation. Verify that:

1. direct bounded storage retrieval returns active and legacy observations but not the dropped observation;
2. the reflection remains present;
3. enough low-sorting dropped observations cannot reduce the number of active rows returned at a small limit;
4. graph inspection still finds the dropped observation and its provenance relationships; and
5. the compiled `adam_file_context` boundary returns the same active-only result set.

Normal tests remain independent of Neo4j. Live coverage runs through the existing opt-in ephemeral Neo4j validation.

## Documentation

Update the file-memory sections of the README, architecture/replica contracts, and acceptance matrix to state that explicit retrieval returns active observations plus relevant reflections by default while retaining tombstoned observations in the lossless derived graph.

Use “dropped observation” or “tombstoned observation” rather than “deleted memory”: the source memory event and projected provenance remain preserved.

## Release sequence

1. Complete and release v0.3.2 first.
2. Branch v0.3.3 from the protected `main` containing v0.3.2.
3. Add failing deterministic and live retrieval regressions.
4. Implement the storage-query predicate without changing projection data.
5. Update contracts, acceptance evidence, and the changelog.
6. Bump `package.json`, `package-lock.json`, and `.claude-plugin/plugin.json` to `0.3.3`.
7. Rebuild and commit all generated runtimes required by the package drift gate.
8. Run release builds, deterministic tests, Node boundary/package tests, generated-runtime checks, plugin validation, package dry-run, exact-tarball smoke tests, and ephemeral Neo4j validation.
9. Open a two-commit, history-preserving release PR where practical: behavior/tests/docs first, release preparation second.
10. Leave the PR for user-managed non-squash merge. Protected-main CI creates `v0.3.3` only after all post-merge gates pass, and the release workflow publishes the tarball and checksum.

## Implementation note

The live bound regression exposed a latent defect: the query's trailing `ORDER BY … LIMIT $limit` after `UNION ALL` applied only to the reflection branch under Neo4j 5, so observations were never limited. Both branches now run inside a `CALL { … }` subquery and the combined rows are ordered and limited afterwards, which is also what makes the dropped-observation predicate's "before the bound" guarantee meaningful.

## Deferred

Adam 0.3.3 does not add:

- destructive removal of dropped observations;
- filtering of reflections merely because supporting observations were dropped;
- an `includeDropped` or historical-query option;
- semantic deduplication, ranking, or global search;
- a larger result limit;
- automatic prompt/context injection;
- new memory producers or observation-generation behavior; or
- a graph migration or derived-state rebuild.
