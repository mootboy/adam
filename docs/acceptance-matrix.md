# adam acceptance matrix

Status: executable specification for incremental delivery.

Normal tests must be deterministic and independent of Neo4j. Cases marked **live** belong to the opt-in `ADAM_TEST_NEO4J_*` suite.

## Extension and packaging

| Case | Expected result |
| --- | --- |
| Compiled package loads through `extension.js` | Pi factory executes ClojureScript output |
| `/adam:status` is invoked | Configuration, connection, identity, active-session, last-sync, and bounded error state are displayed |
| Java is absent at runtime | Committed ESM output still loads and executes |
| npm package is inspected | Runtime boundary, compiled output, and docs are present; source compilation is not required |

## Configuration and identity

| Case | Expected result |
| --- | --- |
| Required environment values are absent or partial | adam remains loaded; Neo4j functionality is disabled with a reason |
| Loopback URI is unencrypted | Configuration is accepted |
| Non-loopback URI is unencrypted | Configuration is rejected without interrupting Pi |
| Equivalent Git email spellings differ only by case/whitespace | One deterministic hashed identity; latest observed spelling may be displayed |
| No Git email is configured | Mirroring continues under the generated user UUID |
| State is initialized concurrently | One owner-only config wins atomically and remains valid |

## JSONL and session identity

| Case | Expected result |
| --- | --- |
| Header and all entries are valid | Scanner emits lossless raw lines, deterministic metadata, and a summary |
| Empty lines occur | They do not create entries or change entry ordinals |
| A line is malformed JSON | Scan fails with file/line context; no repair is attempted |
| Header is absent, duplicated, or not first | Session is rejected |
| Entry IDs are duplicated | Session is rejected |
| Parent reference is unresolved | Session is rejected |
| Entry type or nested fields are unknown | Complete raw JSON is preserved |
| Entry contains inline base64 data | Bytes round-trip unchanged |

## Checkpoints and synchronization

| Case | Expected result |
| --- | --- |
| No checkpoint exists | Entries stream from the beginning in byte-bounded batches |
| File has only appended lines | Sync resumes at the committed offset and writes only the suffix |
| Completed file is unchanged | Sync is idempotent and reports unchanged |
| Process stops after a committed batch | Retry resumes from that batch's checkpoint |
| File shrinks or prefix hash changes | Session is marked conflicted and writes stop |
| Existing entry ID has different raw JSON | Existing payload is preserved and session is marked conflicted |
| A single entry exceeds the target batch size | It is sent alone rather than split or omitted |
| Neo4j is unavailable | Pi continues, one deduplicated warning is emitted, synchronization remains pending, and a later lifecycle event retries |

## Session graph and restoration

| Case | Expected result |
| --- | --- |
| Branched session is mirrored | Every entry and parent relationship is present |
| Current leaf is not the final JSONL line | `CURRENT_LEAF` identifies the selected branch endpoint |
| Child is mirrored before its parent | Later parent reconciliation creates one `FORKED_FROM` edge |
| Parent is mirrored before its child | Child synchronization creates the same canonical edge |
| Parent path is missing or malformed | Child mirrors without canonical lineage and retains provenance |
| Existing local and remote sessions share identity | Resume offers one `local+neo` choice and opens local unchanged |
| Complete remote-only session matches exact cwd | It materializes to a new validated file and restores its leaf |
| Restoration target already exists | Restoration aborts without overwrite |
| Remote-only session is incomplete, conflicted, or hash-invalid | Restoration is refused |

## Repository and file evidence

| Case | Expected result |
| --- | --- |
| Observation cites `read`, `edit`, or `write` call entry | Observation is linked to the canonical file and source entry |
| Observation cites only the matching tool-result entry | Tool-call resolution still links it to the file |
| Reflection supports a linked observation | Reflection is returned through its support relationship |
| Relative and absolute paths identify the same file | One repository-scoped file identity |
| Main checkout and linked worktree paths identify the same relative file | One file identity; evidence retains each actual revision |
| Session cwd is a non-Git workspace and explicit file evidence enters a repository | First selected-branch resolvable path discovers the repository |
| Workspace has no resolvable file evidence | Replica remains healthy and projection reports a waiting state |
| Path is outside all registered worktree roots | No file evidence is created |
| Path occurs only in prose, memory text, or a shell command | No file evidence is created |
| Same relative path exists in different repositories | Distinct file identities |
| Abandoned branch contains memory | It remains mirrored but is excluded from the current projection |
| Projection runs repeatedly | Results and relationships remain deterministic without duplicates |

## Memory-source adapter

| Case | Expected result |
| --- | --- |
| Supported recorded-observation entry is present | Provider-neutral observation and source IDs are emitted |
| Supported reflection entry is present | Reflection and support IDs are emitted |
| Drop entry names an observation | Observation is projected as dropped without deleting it |
| Unrelated custom entry is present | Adapter ignores it |
| Supported producer entry is malformed | Projection reports an isolated diagnostic; replica remains valid |
| Unknown producer schema version is present | Adapter does not guess or corrupt the projection |

## Query surfaces

| Case | Expected result |
| --- | --- |
| Known repository-relative file has linked memories | Command and tool return IDs, content, session/source provenance, and observed revisions |
| Absolute or workspace-relative path identifies the same file | Query resolves to the same canonical result set |
| File has no linked memories | Successful explicit empty result |
| Path is outside the resolved repository | Typed path failure before store access |
| More memories exist than the tool bound | Extra probe detects omission and truncation is explicit |
| Rendered provenance exceeds byte/line limits | Output is deterministically truncated and marked |
| Neo4j query fails | Only that command/tool invocation fails; later retry can recover |
| Tool is available to a model | Pi guidance explains when to use it and discourages indiscriminate/global use |

## Side-by-side proof

| Case | Expected result |
| --- | --- |
| Observational memory writes after adam's turn handler | A later reconciliation discovers the memory |
| Observational memory is absent | Session replication works; memory query may be empty |
| adam is absent or Neo4j is down | Observational-memory generation continues |
| Both extensions are installed | No command, tool, state, or runtime dependency collision |
| **Live:** memory is recorded from explicit file work | `adam_file_context` returns it with graph-backed provenance |
| **Live:** process restarts after indexing | Session, projection, and query results remain available |
