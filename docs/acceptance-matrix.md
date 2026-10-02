# adam acceptance matrix

Status: executable specification for incremental delivery.

Normal tests must be deterministic and independent of Neo4j. Cases marked **live** belong to the opt-in `ADAM_TEST_NEO4J_*` suite.

## Extension and packaging

| Case | Expected result |
| --- | --- |
| Compiled package loads through `extension.js` | Pi factory executes ClojureScript output |
| Compiled package starts through `mcp.js` | A stdio MCP client can initialize and list the read-only `adam_file_context` tool |
| Claude loads the packaged plugin | `.mcp.json` starts the packaged MCP executable and the bundled skill is available |
| Packaged `.mcp.json` is resolved like a Claude plugin | Only the literal `${CLAUDE_PLUGIN_ROOT}` placeholder is needed; the spawned command completes an MCP initialize from a foreign cwd |
| Interactive Claude Code starts with the installed package as `--plugin-dir` (**opt-in**, `ADAM_TEST_CLAUDE=1`) | `plugin:adam:adam` connects without `--mcp-config` or a manual `CLAUDE_PLUGIN_ROOT` |
| Content-free producer characterization runs (**opt-in**, `ADAM_TEST_CLAUDE=1`, Linux) | Hooks expose bounded locators, return before generation, and launch a detached worker which outlives Claude, clears `CLAUDECODE`, avoids recursive hooks, and validates bounded structured output |
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

## Producer-scoped memory identity migration

| Case | Expected result |
| --- | --- |
| Existing Pi observation or reflection uses a producerless source-scoped URN | Its ID gains canonical `pi-observational-memory` producer scope before normal memory projection |
| A producerless memory belongs to a non-Pi source | Migration rejects the ambiguous provenance transactionally (**live**) |
| Two producers use the same memory ID in one source session | Their observation/reflection URNs remain distinct |
| Graph contains remote-only memories | IDs migrate from persisted source and producer metadata without local JSONL (**live**) |
| Observation is dropped or linked through `ABOUT` and `SOURCED_FROM` | Tombstone and provenance remain unchanged after migration (**live**) |
| Reflection has `SUPPORTED_BY` links | Existing producer-local support relationships remain unchanged (**live**) |
| Marker claims current but a memory ID or producer field is stale | Effective version is outdated and migration runs again (**live**) |
| A memory lacks required migration provenance or the transaction fails | No memory ID or marker advances, and migration remains retryable (**live**) |
| User memory graph is already current | Migration is an idempotent no-op (**live**) |
| Session, transcript-stream, and entry identities are inspected before and after | They remain unchanged |

## Memory protocol v1

These protocol cases run deterministically in the normal suite. Lossless scanning and checkpoint planning are implemented without Neo4j; raw graph round-trip and resume behavior are covered by opt-in live validation. Aggregate memory projection and durable notification reconciliation are implemented on the unreleased 0.4 development line.

| Case | Expected result |
| --- | --- |
| Producer writes each supported v1 event | Observation, reflection, drop, and content-free coverage envelopes satisfy the common bounded schema |
| Model successfully emits no memory | `source.covered` durably advances producer coverage without creating a memory |
| Same event ID repeats with RFC 8785-equivalent payload | Raw records are retained and semantic effect applies once |
| Same event ID repeats with changed canonical payload | The memory stream reports immutable-event conflict and stops semantic advancement |
| JCS edge vector uses non-ASCII, an escaped control, and a non-integer number | Canonical UTF-8 bytes and SHA-256 match the committed RFC 8785 vector |
| Source checkpoint drops a stream, decreases an offset, or changes a hash at equal offset | Raw record receives `checkpoint-regression`; the event is skipped and later records continue against the last accepted checkpoint |
| Event fields individually fit but combined encoding exceeds 1 MiB | Producer splits events or reduces content; the oversized record is rejected |
| Completed line is malformed JSON | Exact raw record is retained with a diagnostic and no semantic effect |
| File ends in a non-LF tail | Tail is deferred and excluded from the memory-stream checkpoint |
| Observation cites an entry not yet mirrored | Memory is retained with unresolved provenance and repaired after source reconciliation |
| Reflection supports an unknown observation | Reflection is retained with unresolved support and repaired after producer reconciliation |
| Tombstone precedes an observation definition | Later observation is retained as dropped |
| Memory ID receives incompatible definitions | First valid definition wins and later definition is diagnosed |
| Sidecar shrinks or committed prefix changes | Only that memory stream conflicts; other source/producer work continues |
| Event source or producer differs from sidecar locator | Raw record is retained, identity mismatch is diagnosed, and it has no semantic effect |
| Protocol fixtures run in the normal suite | Contract conformance needs no Neo4j, model, or host transcript |
| Explicit sidecar is mirrored, rerun unchanged, then appended | Raw records round-trip byte-for-byte, unchanged synchronization writes nothing, and only the suffix is added |
| Semantic event conflict appears after valid records, then more records append | The conflicting record is mirrored; later complete suffix records continue mirroring as `blocked` while semantic advancement remains stopped (**live**) |
| Invalid UTF-8 or an oversized record follows valid records | Valid preceding records commit, the offending record does not, and the stream becomes physically conflicted (**live**) |
| Sidecar path or owner-only permissions are unsafe | The notification is acknowledged and logged without marking the stream conflicted; after repair, a later notification ingests normally; POSIX mode enforcement supports Linux/macOS only |
| Notified sidecar no longer exists | The notification is acknowledged and logged, and the last mirrored prefix and derived contribution remain intact |
| Sidecar changes during scanning | The scan is transiently rejected and may retry without marking a conflict |
| Memory notification arrives before its source session | The missing-session failure remains transient; the worker reconciles transcript notifications first for that session |
| One memory stream conflicts | Its source session, transcript/file evidence, and other producer streams remain available |

## Claude transcript ingestion

These cases define the implemented 0.3 scanner, graph-storage, file-evidence, and lifecycle-reconciliation contract.

| Case | Expected result |
| --- | --- |
| SessionStart, Stop, or SessionEnd supplies a Claude `transcript_path` | A bounded locator-only notification is atomically queued and the hook returns without waiting for Neo4j |
| SubagentStop supplies `transcript_path`, `agent_transcript_path`, and `agent_id` | Parent and explicit child locators are retained under one source-scoped session without directory scanning |
| Several hooks notify the same stream before processing | Notifications coalesce for one reconciliation and all are acknowledged only after success |
| Two workers wake concurrently | One filesystem lease owner processes notifications; the other exits without duplicate writes |
| Worker crashes while holding its lease | A later worker recovers a dead or old incomplete lease and resumes durable notifications |
| Neo4j is unavailable after enqueue | Claude remains unblocked; the notification survives and retries with bounded backoff |
| A queued transcript no longer exists | The notification is acknowledged and logged; no locator or graph write occurs |
| One stream fails while others are queued | Later streams are reconciled and acknowledged in the same pass; only the failed stream's notifications remain |
| A recorded subagent transcript was removed | The parent scan skips the file; the stream's evidence is rebuilt from its mirrored entries and survives while present streams are rebuilt from their transcripts (**live**) |
| The hook spawns the detached worker | Worker diagnostics append to `adam/worker.log` |
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

## Durable producer-memory reconciliation

| Given / when | Required behavior |
| --- | --- |
| Producer commits after the final host hook | Durable locator notification converges at the next Adam entry point without another producer append (**live**) |
| Source session has not been mirrored | Notification remains retryable within its source-wait window; pending transcript work establishes ownership before memory attachment (**live**) |
| Ownership query positively confirms absence at/past the window | Park one locator per source/session/producer durably before active acknowledgement; detached worker exits without reading sidecars or mutating retained state (**live**) |
| Backend or projection fails past the window | No absent-source expiry; notifications remain pending |
| Worker restarts or new duplicate notifications coalesce | Each local file mtime survives; newer notifications do not reset older deadlines |
| Producer clock is ahead or behind Adam's clock | Fresh local notification uses mtime, not producer queuedAt; slow-clock source-hook race remains retryable (**live**) |
| Expired source is later mirrored, with no new producer notification/append | Explicit worker/Pi repair requeues its parked locator and produces memory/file links (**live**) |
| Ordinary lifecycle/worker runs with parked-only work | Archive remains untouched; no background retry, discovery, or ephemeral Pi initialization (**live**) |
| Crash after parking or recovery enqueue | Active/parked overlap remains recoverable; repeated repair/coalescing is idempotent |
| Parked source stays absent or backend fails | Archive is retained; explicit repair reports backend failure while healthy producers progress |
| Archive is unsafe, malformed, or uncommittable | Never follow/overwrite/delete unsafe snapshots; retain active work on parking failure; permit operator repair |
| Source-wait configuration is invalid | Standalone worker fails at startup without connection/retry or acknowledgement; Pi source mirroring remains independent |
| Producer repeats notifications for one locator | Coalesced sidecar synchronization and one aggregate session projection precede acknowledgement |
| One source/producer fails synchronously or asynchronously | Healthy groups progress; only failed work remains pending |
| Projection fails after raw synchronization | Notification survives and a restart retries idempotently |
| Detached worker holds the notification lease | Pi active-session mirroring, evidence projection, and historical imports still complete; only notification drains defer |
| Pi replication fails after connecting | Replication `last-error` and unhealthy connectivity remain distinct from notification deferral |
| Worker waits for retry backoff | Shared lease is released; Pi source writes are independently lease-free |
| Worker closes its lease while new notifications arrive | Both queues are rechecked after release |
| Local transcript/subagent files are gone | Retained raw records and `TOUCHES` evidence still produce memory provenance without deleting evidence (**live**) |
| Pi explicit repair runs with embedded and sidecar memory | Both producer contributions survive graph-native aggregate repair (**live**) |
| A source is missing or unsafe | Terminal notification acknowledgement preserves graph state and future repairability |
| Latest status is inspected | Content-free counts and timing are bounded to the latest drain; no history or content telemetry |

## Aggregate memory projection

| Case | Expected result |
| --- | --- |
| Embedded Pi memory and mirrored sidecar memory belong to one session | One transaction replaces their combined derived snapshot |
| Two producers use the same memory ID | Producer-scoped observation/reflection nodes coexist (**live**) |
| Tombstone precedes or follows an observation definition | Producer-local observation remains projected as dropped |
| Reflection supports an observation with the same producer | `SUPPORTED_BY` is created; another producer's equal memory ID is not linked |
| Citation names a source stream and entry | `SOURCED_FROM` resolves only the matching owned session entry and `ABOUT` follows its `TOUCHES` evidence (**live**) |
| Claude citation names a subagent entry | Canonical `agent:<agentId>` resolves to the mirrored child entry and its file evidence (**live**) |
| Citation target is not mirrored yet | Memory remains projected with an `unresolved-reference` diagnostic and links appear after a later rebuild |
| Producer stream is physically or semantically conflicted | Its accepted retained prefix remains in the aggregate while later invalid/blocked records do not apply |
| Sidecar disappears after successful mirroring | Retained `AdamMemoryRecord` data preserves the producer's derived contribution (**live**) |
| Session has no resolvable repository | Aggregate memory remains projected without file links and can link after later repository reconciliation (**live**) |
| Aggregate projection runs repeatedly | Results and relationships remain deterministic without duplicates |

## Query surfaces

| Case | Expected result |
| --- | --- |
| Known repository-relative file has linked memories | Command and tool return IDs, content, source-session, stream, producer, and observed-revision provenance |
| Absolute or workspace-relative path identifies the same file | Query resolves to the same canonical result set |
| Explicit normalized origin plus relative path is supplied without a checkout | Command and tool query the same canonical file directly |
| Origin lookup is invoked from an unrelated repository | Current cwd does not constrain the result |
| Origin mode receives an absolute path, traversal, empty path, or malformed origin | Typed validation failure before store access |
| Shared canonical file has memories from multiple users | Query returns only memories reachable through the requesting user's sessions |
| File has no linked memories | Successful explicit empty result |
| A linked observation was dropped by its producer | Command and tool omit it; its node and provenance relationships remain in the graph (**live**) |
| A linked observation has no `dropped` property | It is treated as active and returned |
| A reflection is supported only by dropped observations | The reflection is still returned for the file |
| Dropped observations sort ahead of active ones at a small bound | They are excluded before the limit, so active rows still fill the bound and tool truncation reflects active rows only (**live**) |
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
