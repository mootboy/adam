# Adam 0.4.0: memory infrastructure release

Status: release preparation; operational acceptance pending

## Release boundary

0.4.0 releases the implemented memory infrastructure, not a Claude memory generator or an automatic native-log discovery feature. The standalone reference producer and its concrete Adam reader are separate follow-up work, **not release dependencies**. This replaces the former requirement to finish both before publishing 0.4.0.

Adam must never require producers to know it exists. Producers independently generate, persist, list and recall memories in their own formats. Adam owns read-only adapters, discovery and reconciliation. Avoiding imports is insufficient if a producer still needs Adam's paths, protocol, inbox, installation or worker commands.

Pi observational memory already follows this boundary: its native custom entries are embedded in authoritative Pi JSONL and Adam structurally reads them. A standalone Claude producer and Adam's reader for its native format are **not implemented**. Claude transcript replication, native file evidence and explicit retrieval of existing memories remain supported; Claude memory generation is not bundled.

## Included in 0.4.0

- Producer-scoped observation/reflection identities with a graph-native transactional, self-auditing migration. Remote-only memories, tombstones and provenance survive; session, entry, stream, repository and file identities do not change.
- One aggregate memory snapshot per source session, combining embedded Pi memories with accepted records from retained mirrored producer streams. Producer-local IDs, supports and drops cannot erase another producer's contribution.
- Stream-qualified citations and source/producer/revision provenance in bounded file-context retrieval. Active observations and reflections supported by dropped observations retain the established read semantics.
- Lossless checkpointed protocol-v1 memory-stream storage, partial-tail tolerance, isolated diagnostics/conflicts and retained contributions when sidecars or repositories are unavailable.
- Existing protocol-v1 locator-spool reconciliation, lease-independent Pi source mirroring/imports, retained-graph repair, bounded missing-source expiry, durable parking and explicit recovery.
- Live-test identity isolation, exact fixture-session teardown and a disposable-database acknowledgement gate.

### Experimental optional protocol input

[`memory-protocol-contract.md`](memory-protocol-contract.md) defines **one optional, experimental input format**, not a producer standard or a requirement for independent producers. [`memory-notification-contract.md`](memory-notification-contract.md) documents its existing locator spool and repair mechanics. Their implemented contracts remain unchanged; this release does not remove state, rewrite sidecars or introduce a migration for the spool.

The reference producer must not adopt these Adam-specific paths or notifications as its native interface. A later Adam-owned reader may use existing queue machinery internally, but producer cooperation is never required. Existing spool tests prove protocol-input infrastructure, **not** native discovery or producer independence.

Protocol sidecar ingestion enforces owner-only POSIX permissions and currently supports Linux/macOS. Complete raw records remain authoritative producer input; Neo4j is only a lossless replica and derived index. Never claim reserialized normalized events are byte-for-byte copies of native records.

## Keep the safety boundaries

- Hosts own authoritative sessions/transcripts; producers own authoritative memory logs; Adam never modifies those files or generates memories.
- Raw synchronization precedes semantic projection. Physical history/framing conflicts and semantic immutable-event conflicts remain distinct.
- A failed producer or derived projection cannot invalidate successful session replication or block healthy producers.
- File relevance follows cited source entries and explicit native file-tool evidence, never memory prose or shell-path interpretation.
- Migrations advance markers only after complete successful transactions and self-audit stale identities.
- Retrieval remains explicit, user-isolated and bounded; no automatic prompt insertion, global search or semantic search.
- Test cleanup uses fixture identities and registered session-owned resources only. All live validation uses disposable Neo4j, never shared production.

The detailed executable requirements live in [`acceptance-matrix.md`](acceptance-matrix.md), [`architecture.md`](architecture.md) and [`session-replica-contract.md`](session-replica-contract.md), rather than being duplicated here.

## Release gates and cutover

Tracked by [`020`](issues/020-release-0.4.0-memory-infrastructure.md).

1. Package, lockfile, Claude manifest and changelog agree on 0.4.0. All four committed JavaScript runtimes are reproducible and the exact tarball passes consumer validation.
2. Clean deterministic and disposable Neo4j suites pass; hosted Node 22.19/24 and Neo4j checks are green.
3. Before merging the version bump, confirm post-incident ownership/provenance recovery and candidate restart/worker-completion acceptance. Import completion or session counts alone are insufficient. Production operations require separate authorization; a drained backlog alone does not establish why the earlier worker stalled.
4. The user merges the release PR preserving history. Protected-main CI tags/publishes the exact tested merge; never create a tag early. Keep the PR draft while operational gates are pending.
5. After publication, install the same tagged artifact in Pi and Claude and confirm status, explicit retrieval and idempotent reconciliation. Publication/cutover is not recorded as completed before it happens.

No public npm publication, runtime cleanup, generic adapter framework or new scheduler is part of this release.

## Separate follow-up: one independent producer, one reader

The producer owns its repository, native format/data path, event and coverage/replay rules, model/privacy configuration, and independent listing/recall. [`claude-memory-producer-contract.md`](claude-memory-producer-contract.md) preserves characterization and safety requirements, not a 0.4 release gate. Specify that real producer contract before writing Adam's concrete reader.

The smallest Adam integration is:

1. Read the producer's documented native format from bounded configured/documented roots; no producer registration, export or Adam-specific command.
2. Preserve exact native raw records, then adapt identity, memory events and citations into the existing aggregate domain. Reuse checkpoints and projection; do not add a general plugin registry for one reader.
3. Rediscover committed logs at startup, relevant lifecycle reconciliation and explicit repair. Late source import must find an existing log without a fresh append or notification.
4. Producer completion after Adam's final hook becomes visible at the **next Adam reconciliation**, not necessarily immediately. No daemon, watcher or polling until that latency is demonstrated to be inadequate.
5. Keep missing-source bookkeeping only where necessary; do not duplicate notification expiry/archive machinery for files that can simply be rediscovered.

Acceptance for that later work:

- With Adam actually absent, generation, persistence, listing, recall, crash/restart recovery and durable no-memory coverage work.
- Enabling Adam ingests the same unchanged logs without exports, fresh appends, producer notifications or producer-started Adam processes.
- Parent/subagent citations resolve to exact source entries and file evidence; both hosts retrieve the same provenance-bearing memory.
- Reversed completion order, late source import and Neo4j outage converge at the next reconciliation without changing producer behavior.
- Unsupported schemas fail in isolation; native data is never coerced into literal protocol-v1 provenance or modified in place.

This acceptance is deliberately **outside 0.4.0**. No follow-up release version is promised before the producer's contract and reader are demonstrated.
