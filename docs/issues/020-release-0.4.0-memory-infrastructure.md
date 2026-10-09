# Release Adam 0.4.0 memory infrastructure

- Status: blocked
- Created: 2026-10-09
- Owner: unassigned

## Problem

The implemented 0.4 memory infrastructure is still unreleased. The earlier milestone incorrectly couples Adam's release to building a separate Claude memory producer and a native reader. Producer independence means neither package's release should require the other.

The production incident additionally requires post-recovery ownership/provenance and worker-completion evidence before publication. Green isolated tests alone do not establish the shared deployment is healthy.

## Outcome

Publish a precisely scoped 0.4.0 infrastructure artifact through the protected-main pipeline, without claiming automatic standalone-producer ingestion or bundling generation. Preserve existing protocol-input functionality as optional experimental infrastructure rather than removing state or imposing producer obligations.

## Scope

### Included

- Simplified release plan and aligned architecture, acceptance, input-contract and user documentation.
- Package/lockfile/Claude manifest 0.4.0 and dated changelog.
- Clean four-target runtime generation, reproducibility, exact-tarball and disposable live validation.
- User-managed history-preserving release PR, gated on operational acceptance.
- Post-publication Pi/Claude cutover and explicit retrieval/idempotent reconciliation smoke.

### Excluded

- Standalone producer implementation and native schema decisions.
- Native log discovery/reader, generic adapter registry, daemon, polling or watchers.
- Runtime deletion, protocol/spool state migration or graph cleanup.
- Public npm publication.
- Production recovery/draining/importing without separate authorization.

## Acceptance criteria

- [x] Release scope distinguishes implemented infrastructure from experimental optional protocol input and deferred native-producer integration.
- [x] Package, lockfile, manifest and changelog agree on 0.4.0.
- [x] Clean normal CI, reproducible runtimes, exact tarball/package checks and fresh disposable Neo4j validation pass.
- [ ] Hosted Node 22.19/24 and Neo4j checks are green.
- [ ] User confirms Pi recovery: source ownership and orphaned observation provenance/file links restored, not merely session totals.
- [ ] Candidate host restart, explicit retrieval and worker completion/lease release are verified; outstanding/deleted Claude sources are accounted for and unchanged replay is healthy.
- [ ] User approves and merges the release PR preserving history after these gates.
- [ ] Protected-main CI tags/publishes the exact tested commit with tarball and checksum.
- [ ] Pi and Claude run the same published artifact and pass operational smoke.

## Validation

Local release preparation passes:

- Clean `npm run ci`: four ESM targets, 164 ClojureScript tests / 688 assertions, 25 Node passes / four expected opt-in skips, exact-tarball validation and committed-runtime drift check.
- All four release runtimes were rebuilt cleanly and remain byte-identical to committed main; no generated change is necessary for the metadata-only bump.
- Fresh disposable Neo4j 5.26 on `127.0.0.1:7688`, with explicit disposable acknowledgement: eight live ClojureScript tests / 127 assertions plus packaged worker/Pi and scoped-teardown Node live cases pass.
- `claude plugin validate .`, package dry-run and diff checks pass.

Hosted validation is pending. The release PR must remain draft until user-confirmed operational acceptance; this is the blocking dependency. No production recovery commands, model calls, tags or publication performed.

User-reported recovery state before preparation: authorized stream-checkpoint reset/backlog draining restored 19/23 Claude sessions and 4,549/4,579 Claude entries; 92 Pi sessions and 701 orphaned observations still await `/adam:import --all`. These are reported facts, not independently verified acceptance. The remaining Claude sources/entries must be accounted for, not automatically assumed lost or recovered.

## Notes

Keep the PR draft while operational gates are pending: merging a version bump activates post-merge tagging/publication. No direct push/tag release shortcut. The previous stalled installed worker must not be declared fixed solely because a backlog drained.

The separate producer owns its native persistence/recall; Adam later implements one concrete read-only adapter with rediscovery at the next reconciliation. No producer-written Adam notifications or worker commands are required. See [`../plan-0.4.0.md`](../plan-0.4.0.md).
