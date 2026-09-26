---
name: adam-memory
description: Retrieve provenance-bearing observational memories linked to a specific repository file. Use when prior decisions, rationale, or implementation history for a known file could materially affect the current task.
---

# Adam file memory

Use `adam_file_context` when prior decisions or rationale associated with a specific file could materially affect the work.

- Provide a repository-relative, workspace-relative, or absolute `path` for a local checkout.
- When no checkout is available, provide a normalized repository-relative `path` together with the repository's Git `origin`.
- Treat results as historical evidence with explicit provenance, not as current source-of-truth code.
- Consider the reported commit, branch, and dirty state when assessing staleness.
- Do not call the tool for every file.
- Do not use it as semantic or global search.
- Adam does not automatically insert memories into prompts; retrieval is always explicit.
