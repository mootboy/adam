# Issue tracker

This directory is adam's repository-local issue tracker. Use one Markdown file per issue so plans, decisions, and completion evidence remain versioned with the code.

## Issues

- [`001`](001-long-working-pause-after-turns.md) — Long “Working” pause after turns (`done`, bug)
- [`002`](002-characterize-claude-transcripts.md) — Characterize Claude Code transcripts for ingestion (`done`)
- [`003`](003-migrate-source-scoped-session-identities.md) — Migrate session identities to source-scoped URNs (`done`)
- [`004`](004-scan-claude-transcripts.md) — Scan Claude transcripts and derive native file evidence (`done`)
- [`005`](005-reconcile-claude-transcripts.md) — Reconcile Claude transcripts without blocking Claude (`done`)
- [`006`](006-release-and-cut-over-0.3.0.md) — Release and cut over Adam 0.3.0 (`done`)
- [`007`](007-load-plugin-mcp-server-from-released-checkout.md) — Load the plugin MCP server from a released checkout (`done`, bug)
- [`008`](008-worker-stalls-on-missing-transcripts.md) — Worker stalls on missing transcripts (`done`, bug)
- [`009`](009-characterize-claude-memory-producer.md) — Characterize the reference Claude memory producer (`done`)
- [`010`](010-freeze-memory-protocol-v1.md) — Freeze memory protocol v1 (`done`)
- [`011`](011-ingest-memory-sidecar-streams.md) — Ingest producer memory sidecar streams losslessly (`done`)
- [`012`](012-migrate-producer-scoped-memory-identities.md) — Migrate memories to producer-scoped identities (`done`)
- [`013`](013-project-aggregate-multi-producer-memory.md) — Project aggregate multi-producer memory (`done`)
- [`014`](014-reconcile-memory-notifications.md) — Reconcile durable producer memory notifications (`done`)

## Naming

Use `<number>-<short-kebab-case-title>.md`, for example:

```text
001-prove-side-by-side-reconciliation.md
```

Numbers are assigned sequentially and are never reused.

## Workflow

Each issue has one status:

- `proposed` — needs clarification or approval
- `ready` — sufficiently specified to implement
- `in-progress` — actively being implemented
- `blocked` — waiting on a stated dependency
- `done` — acceptance criteria are met and evidence is recorded

Copy [`template.md`](template.md) when creating an issue. Keep the issue focused on one independently verifiable outcome. Link commits, tests, and operational evidence before marking it done.
