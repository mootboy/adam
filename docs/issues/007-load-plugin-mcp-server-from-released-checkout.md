# Load the plugin MCP server from a released checkout

- Status: done
- Created: 2026-09-28
- Owner: unassigned

## Problem

Starting interactive Claude Code 2.1.283 from another workspace with only `claude --plugin-dir "$HOME/.pi/agent/git/github.com/mootboy/adam"` (the Pi checkout of v0.3.0) loads adam's hooks and skill but not its bundled MCP server. Depending on the session, the debug log either shows no `plugin:adam:adam` startup at all or an immediate failure:

```text
MCP server "plugin:adam:adam": Connection failed after 3ms (ENOENT): ENOENT: no such file or directory, posix_spawn 'stdio'
```

The server only connected when the user set `CLAUDE_PLUGIN_ROOT` by hand and passed `--mcp-config "$root/.mcp.json"`, at which point Claude suppressed the plugin server as a duplicate of the manual one.

## Outcome

A plain `claude --plugin-dir <installed adam>` from any trusted cwd starts hooks, the skill, and the namespaced `plugin:adam:adam` server, and a regression test fails if the bundled MCP configuration leaves the documented plugin contract again.

## Diagnosis

0.3.0 shipped `.mcp.json` with `"command": "${CLAUDE_PLUGIN_ROOT:-.}/mcp.js"`. The shell-style default was meant to let the file double as a project `--mcp-config` from the checkout, but it is not part of the Claude Code plugin contract:

- The plugin manifest reference documents only the literal `${CLAUDE_PLUGIN_ROOT}`, `${CLAUDE_PLUGIN_DATA}`, and `${CLAUDE_PROJECT_DIR}` placeholders, substituted per element as plain strings. A `${VAR:-default}` form is only documented for ordinary `${ENV_VAR}` expansion of user MCP configuration.
- In the 2.1.283 binary, plugin MCP entries are first substituted with an exact `${CLAUDE_PLUGIN_ROOT}` match (`String.replace(/\$\{CLAUDE_PLUGIN_ROOT\}/g, pluginPath)`), and only afterwards passed to the generic environment expander, which understands `:-` but reads the parent process environment where `CLAUDE_PLUGIN_ROOT` is not set. `${CLAUDE_PLUGIN_ROOT:-.}` therefore never resolves to the plugin directory during plugin loading.
- Headless `claude -p` and `claude mcp list`/`get` tolerate the leftover placeholder and connect, which is why the defect was not visible in earlier command-line checks. Interactive sessions do not: under the default v2 MCP runtime the server is spawned as the literal `stdio`, and under the v1 runtime (`MCP_SDK_GENERATION=v1`) the server is silently omitted. Both failures disappear with the same configuration change, so the placeholder is the cause; the exact interactive spawn path is Claude Code internals and is not relied on.
- `claude plugin validate` passes for both the broken and the fixed configuration, so it cannot serve as the regression check.

The manifest entry `"mcpServers": "./.mcp.json"` is redundant with Claude's automatic loading of a root `.mcp.json`, but it is documented, harmless, and explicit, so it stays. Claude accepts both the bare server map and the `{ "mcpServers": ... }` wrapper for referenced files.

## Fix

`.mcp.json` now invokes `"${CLAUDE_PLUGIN_ROOT}/mcp.js"` directly as the command, the form that also carried the 0.2.1-era end-to-end proof. The exec form `"command": "node", "args": ["${CLAUDE_PLUGIN_ROOT}/mcp.js"]` also connects on 2.1.283, but adam's session memory records an earlier Claude version treating the argument placeholder literally, so the direct executable is the better-evidenced choice. The server keeps its stdio read-only retrieval, canonical identity, inherited `ADAM_NEO4J_*` environment, and clean stdin shutdown; only how Claude locates the executable changed.

## Scope

### Included

- Plugin MCP configuration that Claude Code resolves without help.
- A deterministic package-boundary regression test and an opt-in interactive Claude probe.
- 0.3.1 release metadata and documentation.

### Excluded

- Changes to hook behavior, packaging layout, or the Pi extension.
- Working around the workspace trust dialog in the opt-in probe; it runs in the trusted checkout with the installed tarball as the plugin root.

## Acceptance criteria

- [x] Interactive Claude Code started from another cwd with `--plugin-dir <installed adam>` connects `plugin:adam:adam` without `--mcp-config` or a manual `CLAUDE_PLUGIN_ROOT`.
- [x] The bundled configuration contains only documented plugin placeholders, and the packaged server, spawned exactly as the plugin contract spawns it, completes an MCP initialize and lists `adam_file_context`.
- [x] Both regression checks fail against the 0.3.0 configuration.
- [x] Package, lockfile, and plugin manifest versions are 0.3.1 with a dated changelog entry.

## Validation

Reproduction on 2026-09-28 with Claude Code 2.1.283 and the Pi checkout at tag `v0.3.0` (593b77a):

| Probe from `/home/linus/git/aloi` | 0.3.0 `.mcp.json` | Fixed `.mcp.json` |
| --- | --- | --- |
| Interactive session, v2 MCP runtime | `posix_spawn 'stdio'` after 3 ms | connected in 232 ms (exec form: 324 ms) |
| Interactive session, `MCP_SDK_GENERATION=v1` | no `plugin:adam:adam` lines | connected in 263 ms |
| Headless `claude -p`, six runs | connected every run | connected |
| `claude mcp get plugin:adam:adam` | connected | connected |
| `claude plugin validate` | passes | passes |

Regression tests:

- `test/package.test.js` installs the packed tarball into a clean consumer, substitutes only `${CLAUDE_PLUGIN_ROOT}`, asserts no `${` remains, and spawns the configured command directly from a foreign cwd through an MCP initialize and `tools/list`. Against the 0.3.0 configuration it fails with `unsupported placeholder left for Claude to expand: ${CLAUDE_PLUGIN_ROOT:-.}/mcp.js`.
- `npm run test:claude` (`ADAM_TEST_CLAUDE=1`) starts a real interactive Claude Code session under a pseudo-terminal with the installed tarball as `--plugin-dir` and waits for `plugin:adam:adam` to connect. It passes with the fix and fails against the 0.3.0 configuration.

Release validation is recorded in the 0.3.1 pull request.

## Notes

- Evidence: Claude debug logs from the failing interactive probes (`~/.cache/claude-cli-nodejs/-home-linus-git-aloi/mcp-logs-plugin-adam-adam/2026-09-27T21-1*.jsonl`) and the successful headless probes from the same checkout.
- Related: [`006`](006-release-and-cut-over-0.3.0.md) keeps its Claude cutover criteria open until the 0.3.1 release is installed.
