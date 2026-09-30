#!/usr/bin/env node
import { appendFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const outputDirectory = process.env.ADAM_PRODUCER_PROBE_DIR;
if (!outputDirectory) process.exit(0);

const hostPid = Number(process.argv[2]);
await sleep(1500);
let hostAliveAfterDelay = true;
try {
  process.kill(hostPid, 0);
} catch {
  hostAliveAfterDelay = false;
}

const childEnvironment = { ...process.env };
const inheritedClaudeCode = Object.hasOwn(childEnvironment, "CLAUDECODE");
delete childEnvironment.CLAUDECODE;
const startedAt = Date.now();
const result = spawnSync(process.env.CLAUDE_BIN || "claude", [
  "--safe-mode",
  "--restricted",
  "--strict-mcp-config",
  "--no-session-persistence",
  "--permission-prompts", "none",
  "--model", process.env.ADAM_PRODUCER_PROBE_MODEL || "haiku",
  "--max-budget-usd", process.env.ADAM_PRODUCER_PROBE_BUDGET_USD || "0.05",
  "--json-schema", JSON.stringify({
    type: "object",
    properties: {
      observations: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 1 },
    },
    required: ["observations"],
    additionalProperties: false,
  }),
  "--output-format", "json",
  "-p", "Return one observation containing exactly probe-ok.",
], {
  cwd: outputDirectory,
  env: childEnvironment,
  encoding: "utf8",
  timeout: 120000,
});

let response;
try {
  response = JSON.parse(result.stdout || "");
} catch {}
const structuredOutputValid = response?.structured_output?.observations?.length === 1
  && response.structured_output.observations[0] === "probe-ok";
appendFileSync(join(outputDirectory, "worker.jsonl"), `${JSON.stringify({
  hostPid,
  hostAliveAfterDelay,
  inheritedClaudeCode,
  childClaudeCodePresent: Object.hasOwn(childEnvironment, "CLAUDECODE"),
  inheritedApiKey: Object.hasOwn(process.env, "ANTHROPIC_API_KEY"),
  inheritedOauthToken: Object.hasOwn(process.env, "CLAUDE_CODE_OAUTH_TOKEN"),
  modelExit: result.status,
  modelSignal: result.signal,
  modelError: response?.subtype || (result.error ? result.error.code || "spawn-error" : null),
  structuredOutputValid,
  reportedCostUsd: typeof response?.total_cost_usd === "number" ? response.total_cost_usd : null,
  modelDurationMs: Date.now() - startedAt,
})}\n`, { mode: 0o600 });
