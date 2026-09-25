# Issue tracker

This directory is adam's repository-local issue tracker. Use one Markdown file per issue so plans, decisions, and completion evidence remain versioned with the code.

## Issues

- [`001`](001-long-working-pause-after-turns.md) — Long “Working” pause after turns (`proposed`, bug)

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
