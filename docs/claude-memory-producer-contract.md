# Reference Claude memory producer contract

Status: characterized; implementation deferred to a separate package.

This contract records the evidence and decisions that gate Adam 0.4.0's reference Claude memory producer. The producer remains separate from Adam: it decides what becomes memory and writes protocol-v1 sidecars, while Adam losslessly mirrors and indexes those records.

## Characterization evidence

A disposable Claude Code 2.1.285 plugin was exercised on 2026-09-30. The probe retained only event names, field names, boolean credential-presence checks, process liveness, timings, exit state, and fixed test responses. It did not retain transcript content, credential values, or private prompts. A content-free version is retained under `test/fixtures/claude-producer-probe-plugin` and runs with `ADAM_TEST_CLAUDE=1 npm run test:claude` so future Claude Code versions can be re-characterized.

### Hook boundary

A non-interactive parent session emitted:

| Event | Observed input fields relevant to the producer |
| --- | --- |
| `SessionStart` | `session_id`, absolute `transcript_path`, absolute `cwd`, `source` |
| `Stop` | `session_id`, absolute `transcript_path`, absolute `cwd`, `prompt_id`, `permission_mode`, `stop_hook_active`, plus response/background metadata |
| `SessionEnd` | `session_id`, absolute `transcript_path`, absolute `cwd`, `prompt_id`, `reason` |

The established Claude transcript contract separately records that `SubagentStop` provides the parent `transcript_path`, distinct absolute `agent_transcript_path`, and `agent_id`. The producer does not scan undocumented Claude directories.

The hook environment contained `CLAUDECODE` and `CLAUDE_PLUGIN_ROOT`. Neither `ANTHROPIC_API_KEY` nor `CLAUDE_CODE_OAUTH_TOKEN` was exposed. A producer must not expect Claude to pass raw credentials to hooks.

### Detached execution and authentication

The disposable `SessionEnd` hook spawned an unreferenced detached Node worker and returned. Process ancestry identified the hook shell's parent as the Claude process. After a 1.5-second delay, the worker observed that this Claude process no longer existed, then launched a bounded model request. Repeated requests completed successfully in approximately 2.4–4.6 seconds and returned the fixed expected response.

The exploratory worker inherited `CLAUDECODE`, but this did not prevent a nested non-interactive invocation when the child used:

```text
claude --safe-mode --restricted --strict-mcp-config \
  --no-session-persistence --permission-prompts none \
  --model <configured-model> --max-budget-usd <configured-budget> \
  --json-schema <schema> --output-format json -p <bounded-prompt>
```

`--safe-mode` disabled hooks and other customizations while retaining normal Claude authentication. The probe recorded only the parent plugin's three lifecycle events, so the child did not recursively invoke producer hooks. With no API-key environment variables present, the child still authenticated through the user's existing Claude login.

The repeatable probe deliberately removes `CLAUDECODE` from the model child's environment before spawning it. The producer must do the same rather than relying on nested-invocation behavior observed in one Claude Code version.

By contrast, `--bare` failed without an explicit API key and reported that login was required. Safe mode is therefore a viable CLI tracer-bullet adapter, but not yet the default unattended adapter. A seat-based Claude login may have terms distinct from explicit API credentials. Unless the applicable subscription terms are confirmed to permit this background use, the implementation contract must default to an explicit-credential provider adapter and expose inherited-login CLI execution only as a separately acknowledged opt-in fallback.

### Structured output and budget behavior

A safe-mode Haiku request successfully returned JSON matching a supplied schema. The response reported one model iteration, 170 output tokens, approximately 2.2 seconds of model duration, and USD 0.0078702 of usage.

A separate request configured with `--max-budget-usd 0.02` terminated as `error_max_budget_usd` but reported USD 0.030358 of usage. The CLI budget is therefore a useful guard, not a strict pre-request spending cap. The producer must also bound serialized input, validate and cap accepted output, limit calls per drain, and expose actual usage diagnostics.

## Approved producer lifecycle

### Fast hooks

`SessionStart`, `Stop`, `SubagentStop`, and `SessionEnd` hooks may only:

1. read and validate bounded locator-only input;
2. atomically enqueue producer work;
3. best-effort wake one detached worker;
4. return without waiting for transcript scanning, model generation, sidecar writes, or Adam.

A hook failure cannot modify a Claude transcript. Queue records contain locators and scheduling metadata, never transcript or memory content.

### Serialized worker

One recoverable producer lease serializes work. The worker:

1. coalesces notifications by source session and stream;
2. updates an owner-only parent/subagent locator registry;
3. scans only explicitly located streams;
4. reconstructs selected context using Adam's documented Claude transcript semantics;
5. folds the producer's existing sidecar before consulting disposable checkpoint state;
6. generates at most the configured bounded observation/reflection work for the drain;
7. durably appends a protocol event under one sidecar writer lease;
8. writes Adam's locator-only memory notification only after the sidecar append commits;
9. acknowledges producer work only after that commit;
10. retries transient failures with capped exponential backoff.

Detached execution is proven on Linux. Windows command-hook packaging can be implemented, but detached-worker survival requires its own boundary test before being claimed as supported.

## Source progress and replay

The sidecar is the producer's commit log. Each event records producer-supplied, provenance-only source checkpoints for every covered parent or subagent stream. Adam does not compare those values with its independently observed transcript checkpoints.

On startup, the producer folds valid sidecar events to recover committed source coverage. A separate local checkpoint is only a scanning optimization and cannot advance beyond sidecar authority.

Crash rules are:

- before model completion: no event exists; retry may call the model again;
- after model completion but before append: no event exists; retry may regenerate;
- during an incomplete append: protocol framing rules decide recovery before any later append;
- after a complete durable append but before local acknowledgement: folding the sidecar finds the committed event, so retry does not generate a second logical result;
- after sidecar commit but before Adam notification: the producer rewrites the idempotent locator-only notification without rewriting the event.

Protocol v1 makes event identity and immutable-payload comparison deterministic through RFC 8785 canonical JSON and SHA-256.

A model may validly decline to emit memory for a covered source range. Protocol v1 commits that successful no-memory result as a content-free `source.covered` event carrying only the stream-qualified `sourceCheckpoint`. Folding the sidecar therefore avoids paying again after a post-append crash without inventing an observation. Model/provider failures and cancelled generation do not commit coverage.

## Scheduling

Hooks are wake signals, not one-model-call-per-hook triggers.

- `SessionStart` performs repair/reconciliation only.
- `Stop` makes new parent-stream activity eligible for observation.
- `SubagentStop` records the child locator and makes child activity eligible under the parent source session.
- `SessionEnd` performs a final eligibility check and repair pass; it does not force duplicate work for an already covered source checkpoint.
- Notifications coalesce to the newest locator state before generation.

The initial reference policy is source-progress based rather than wall-clock based:

- observations become eligible after approximately 10,000 new estimated source tokens;
- a final `SessionEnd` may flush a smaller non-empty remainder so short sessions can produce memory;
- reflections become eligible after approximately 20,000 newly covered source tokens and run only after committed observations exist;
- tests may lower thresholds to complete the tracer bullet deterministically.

Thresholds are producer configuration, not memory-protocol fields. A model may return no durable memory; the producer advances authoritative coverage with `source.covered` rather than inventing an observation.

## Model and cost controls

Memory production is disabled until the user explicitly enables it. Configuration must include:

- Claude executable or provider adapter;
- model name;
- per-call budget guard;
- maximum serialized source size;
- maximum accepted observations/reflections and content length;
- observation and reflection thresholds;
- retry ceiling/backoff and worker diagnostics.

The characterized CLI adapter uses safe mode, restricted tools, strict MCP configuration, no session persistence, no permission prompts, a fixed JSON schema, no file or shell tools, and an environment with `CLAUDECODE` removed. It records bounded usage totals and failure categories, not prompts or generated memory text, in operational diagnostics. One worker call runs at a time, and one failed model request cannot block unrelated source sessions indefinitely. Explicit-provider credentials remain the default implementation direction unless inherited-login background use is confirmed to comply with the applicable subscription terms.

Cancellation is process-based: terminating the worker may abandon uncommitted generation, which is retried later. Once a complete sidecar event is durable, cancellation cannot retract it; the producer must recover from the sidecar and finish notification.

## Privacy and consent

Enabling the producer is an affirmative choice separate from enabling Adam retrieval. Before the first model call, documentation and configuration must make clear that selected Claude conversation content—including user/assistant text and relevant structured tool activity—will be sent to the configured model provider for background memory generation after the interactive turn may have ended.

The producer:

- never obtains or logs raw Claude credentials;
- never sends unknown operational records merely because they are present in the transcript;
- never treats Bash, compact-summary prose, subagent handback prose, or path-looking text as deterministic file evidence;
- stores authoritative memory text only in the owner-only sidecar;
- keeps queue, checkpoint, lease, and diagnostics files content-free and owner-only;
- provides a documented disable/stop procedure;
- does not insert generated memories into prompts automatically.

Users remain responsible for whether the configured provider may receive repository and conversation content. Corporate or regulated environments may require a direct approved provider adapter rather than inherited Claude login. Successful authentication through an existing Claude seat is technical evidence only, not evidence that unattended background use is permitted by that subscription's terms.

## Consequences for Adam 0.4.0

- The memory protocol contract may assume producer work is asynchronous and replayed from a producer-owned commit log.
- Adam must not require producer model credentials.
- Adam's durable memory notification spool closes the final-hook ordering race; it does not schedule model generation.
- Parent and subagent citations require stream-qualified source references.
- The reference producer can use the Claude CLI for an explicitly enabled tracer bullet without importing Adam; explicit provider credentials remain the default unless applicable subscription terms are confirmed to permit inherited-login background use.
