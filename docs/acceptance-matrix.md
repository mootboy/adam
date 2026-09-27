# adam acceptance matrix

Status: executable specification for incremental delivery.

Normal tests must be deterministic and independent of Neo4j. Cases marked **live** belong to the opt-in `ADAM_TEST_NEO4J_*` suite.

## Extension and packaging

| Case | Expected result |
| --- | --- |
| Compiled package loads through `extension.js` | Pi factory executes ClojureScript output |
| Compiled package starts through `mcp.js` | A stdio MCP client can initialize and list the read-only `adam_file_context` tool |
| Claude loads the packaged plugin | `.mcp.json` starts the packaged MCP executable and the bundled skill is available |
| `/adam:status` is invoked | Configuration, connection, identity, active-session, last-sync, and bounded error state are displayed |
| Java is absent at runtime | Both committed ESM outputs still load and execute |
| npm package is inspected | Pi and MCP runtime boundaries, compiled outputs, plugin metadata, skill, changelog, and docs are present; source compilation is not required |
| Packed tarball is installed in a clean consumer | Extension loads and registers its public surface without ClojureScript source compilation |
| Supported Node matrix runs in CI | Deterministic build, tests, package smoke, and committed `dist` verification pass on Node 22.19 and current Node 24 |
| Ephemeral Neo4j CI job runs | The live integration suite passes without external service credentials |
| Pull-request validation succeeds | No job has permission to create a release tag |
| Protected-main validation sees an already-tagged package version | Tagging is a successful no-op |
| Protected-main validation sees a new package version | After all required jobs pass, the exact tested merge commit receives the matching tag without pushing the branch |
| A superseded protected-main run reaches the tagging job | It exits without tagging and leaves publication to the latest main run |
| Release tag differs from package or lockfile version | Release is rejected before an artifact is created |
| A valid `v*` tag passes all validation | One GitHub Release contains the tested tarball and its SHA-256 checksum |

## Configuration and identity

| Case | Expected result |
| --- | --- |
| Required environment values are absent or partial | adam remains loaded; Neo4j functionality is disabled with a reason |
| Loopback URI is unencrypted | Configuration is accepted |
| Non-loopback URI is unencrypted | Configuration is rejected without interrupting Pi |
| Equivalent Git email spellings differ only by case/whitespace | One deterministic hashed identity; latest observed spelling may be displayed |
| No Git email is configured | Mirroring continues under the generated user UUID |
| State is initialized concurrently | One owner-only config wins atomically and remains valid |
| Canonical host state is absent and Pi-scoped state exists | Existing Pi UUID is atomically adopted under the XDG config home |
| Canonical and Pi-scoped state contain the same UUID | Pi and Claude continue under one principal |
| Canonical and Pi-scoped state contain different UUIDs | Initialization fails visibly before a query or mirror can use split identity |

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

## Source-scoped session identity migration

| Case | Expected result |
| --- | --- |
| Existing Pi-only graph starts under 0.3 | Session, entry, observation, reflection, parent, and denormalized session identities gain the `pi` source kind before normal synchronization |
| Graph contains a remote-only Pi session | It migrates in place without local JSONL |
| Migration succeeds | Raw payloads, checkpoints, current leaf, fork lineage, and file-memory relationships remain unchanged |
| Migration transaction fails | All identity changes and the version marker roll back and retry later |
| Marker is current but a dependent node is stale | Effective version is outdated and migration runs again |
| User graph is already current | Migration is an idempotent no-op |

## Claude transcript ingestion

These cases define the implemented 0.3 scanner, graph-storage, and file-evidence contract. Hook-triggered automatic reconciliation remains pending.

| Case | Expected result |
| --- | --- |
| Hook supplies a Claude `transcript_path` | That stream is read as authoritative without scanning undocumented storage directories |
| Parent transcript contains unknown or UUID-less records | Every complete physical record is preserved losslessly and UUID-less identity is deterministic within the stream |
| Transcript ends with incomplete non-newline JSON | The tail remains uncommitted and is retried after a later append |
| Transcript contains malformed completed JSON, duplicate UUIDs, or unresolved structural references | The stream is rejected without repairing the source |
| Transcript changes during scanning | The scan is rejected and retried later |
| Claude session resumes normally | Existing committed prefix remains unchanged and only the appended suffix is synchronized |
| Transcript contains a manual compact boundary | Continuity follows `logicalParentUuid`; compact-summary prose creates no file evidence |
| Latest `last-prompt` names a valid leaf | Parent-stream current context begins from its `leafUuid` |
| One active model request emits parallel tool fragments | Active fragments sharing `requestId` and their structurally matched results remain in current context |
| Claude calls native `Read`, `Edit`, or `Write` | Non-empty `input.file_path` creates deterministic file evidence using that record's cwd |
| Claude uses Bash, MCP, prose, a compact summary, or handback text containing a path | No file evidence is created |
| Explicitly located subagent stream shares the parent session ID | It remains a checkpointed child stream of that Claude session, retains `agentId`/sidechain provenance, and its native file tools may create evidence |
| Parent and subagent streams are mirrored | One source-scoped `AdamSession` owns both `AdamTranscriptStream` nodes and every lossless entry; compact continuity uses `LOGICAL_PARENT` |
| Subagent completion is copied into parent handback records | Records are preserved but handback prose creates no evidence |

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
| Equivalent SSH, HTTPS, and credential-bearing origins are observed by different users | One canonical repository identity without credentials or user UUID |
| The same origin-relative path is observed across users, sessions, machines, checkouts, and worktrees | One canonical file identity while queries remain user-isolated |
| Repository has no normalizable origin | User-scoped local fallback remains isolated and is unavailable to origin lookup |
| Main checkout and linked worktree paths identify the same relative file | One file identity; evidence retains each actual revision |
| Session cwd is a non-Git workspace and explicit file evidence enters a repository | First selected-branch resolvable path discovers the repository |
| Workspace has no resolvable file evidence | Replica remains healthy and projection reports a waiting state |
| Path is outside all registered worktree roots | No file evidence is created |
| Path occurs only in prose, memory text, or a shell command | No file evidence is created |
| Same relative path exists in different repositories | Distinct file identities |
| Abandoned branch contains memory | It remains mirrored but is excluded from the current projection |
| Projection runs repeatedly | Results and relationships remain deterministic without duplicates |
| Code-memory schema version is old or rebuild was interrupted | Rebuild resumes idempotently, removes obsolete derived identities, and never modifies lossless session entries |
| Replica contains many sessions and entries | Evidence projection uses the composite session/entry lookup index rather than scanning all AdamEntry nodes |

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
| Explicit normalized origin plus relative path is supplied without a checkout | Command and tool query the same canonical file directly |
| Origin lookup is invoked from an unrelated repository | Current cwd does not constrain the result |
| Origin mode receives an absolute path, traversal, empty path, or malformed origin | Typed validation failure before store access |
| Shared canonical file has memories from multiple users | Query returns only memories reachable through the requesting user's sessions |
| File has no linked memories | Successful explicit empty result |
| Path is outside the resolved repository | Typed path failure before store access |
| More memories exist than the tool bound | Extra probe detects omission and truncation is explicit |
| Rendered provenance exceeds byte/line limits | Output is deterministically truncated and marked |
| Neo4j query fails | Only that command/tool invocation fails; later retry can recover |
| Tool is available to a model | Pi guidance and the Claude skill explain when to use it and discourage indiscriminate/global use |
| Claude invokes `adam_file_context` through the plugin MCP server | Existing Pi-produced memory is returned with the same origin lookup, user isolation, bounds, and provenance |
| MCP standard input closes or the host terminates it | Neo4j resources close and the MCP process exits cleanly |
| MCP query fails | The invocation returns a bounded tool error without terminating Claude Code |

## Side-by-side proof

| Case | Expected result |
| --- | --- |
| Observational memory writes after adam's turn handler | A later reconciliation discovers the memory |
| Observational memory is absent | Session replication works; memory query may be empty |
| adam is absent or Neo4j is down | Observational-memory generation continues |
| Both extensions are installed | No command, tool, state, or runtime dependency collision |
| **Live:** memory is recorded from explicit file work | `adam_file_context` returns it with graph-backed provenance |
| **Live:** process restarts after indexing | Session, projection, and query results remain available |
