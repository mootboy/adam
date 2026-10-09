import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

const enabled = process.env.ADAM_TEST_CLAUDE === "1";
const supported = process.platform === "linux";
const pluginRoot = path.resolve("test/fixtures/claude-producer-probe-plugin");

function runClaude(outputDirectory) {
  return new Promise((resolve, reject) => {
    const environment = {
      ...isolatedAdamEnvironment(path.join(outputDirectory, "adam-state")),
      ADAM_PRODUCER_PROBE_DIR: outputDirectory,
    };
    delete environment.CLAUDECODE;
    const startedAt = Date.now();
    const child = spawn(process.env.CLAUDE_BIN || "claude", [
      "--plugin-dir", pluginRoot,
      "--restricted",
      "--strict-mcp-config",
      "--no-session-persistence",
      "--permission-prompts", "none",
      "--model", process.env.ADAM_PRODUCER_PROBE_MODEL || "haiku",
      "--max-budget-usd", process.env.ADAM_PRODUCER_PROBE_BUDGET_USD || "0.05",
      "-p", "Reply with exactly PARENT_OK.",
    ], {
      cwd: outputDirectory,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({
      code,
      signal,
      stdout: stdout.trim(),
      stderr: stderr.trim(),
      durationMs: Date.now() - startedAt,
    }));
  });
}

async function waitForJsonLines(file, deadline) {
  while (Date.now() < deadline) {
    const text = await readFile(file, "utf8").catch(() => "");
    const lines = text.trim().split("\n").filter(Boolean);
    if (lines.length > 0) return lines.map((line) => JSON.parse(line));
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out waiting for ${path.basename(file)}`);
}

test("Claude producer hooks detach an isolated content-free model probe", {
  skip: !enabled ? "set ADAM_TEST_CLAUDE=1" : !supported ? "Linux process ancestry is required" : false,
  timeout: 180000,
}, async () => {
  const outputDirectory = await mkdtemp(path.join(tmpdir(), "adam-producer-probe-"));
  try {
    await chmod(path.join(pluginRoot, "hook.mjs"), 0o755);
    await chmod(path.join(pluginRoot, "worker.mjs"), 0o755);
    const parent = await runClaude(outputDirectory);
    assert.deepEqual(
      { code: parent.code, signal: parent.signal, stdout: parent.stdout, stderr: parent.stderr },
      { code: 0, signal: null, stdout: "PARENT_OK", stderr: "" },
    );

    const [worker] = await waitForJsonLines(
      path.join(outputDirectory, "worker.jsonl"),
      Date.now() + 130000,
    );
    const hooks = await waitForJsonLines(path.join(outputDirectory, "hooks.jsonl"), Date.now() + 5000);
    assert.deepEqual(hooks.map(({ event }) => event), ["SessionStart", "Stop", "SessionEnd"]);
    for (const hook of hooks) {
      assert.equal(hook.sessionIdPresent, true);
      assert.equal(hook.transcriptPathAbsolute, true);
      assert.equal(hook.cwdAbsolute, true);
      assert.equal(hook.environmentPresent.CLAUDECODE, true);
      assert.equal(typeof hook.environmentPresent.ANTHROPIC_API_KEY, "boolean");
      assert.equal(typeof hook.environmentPresent.CLAUDE_CODE_OAUTH_TOKEN, "boolean");
      assert.ok(hook.hostParent.pid > 0);
      assert.notEqual(hook.hostParent.name, "unavailable");
    }
    assert.equal(worker.hostAliveAfterDelay, false);
    assert.equal(worker.inheritedClaudeCode, true);
    assert.equal(worker.childClaudeCodePresent, false);
    assert.equal(worker.modelExit, 0);
    assert.equal(worker.modelSignal, null);
    assert.equal(worker.structuredOutputValid, true);
    assert.equal(typeof worker.reportedCostUsd, "number");
    assert.ok(worker.modelDurationMs < 120000);
    assert.ok(parent.durationMs < 120000);

    for (const file of ["hooks.jsonl", "worker.jsonl"]) {
      assert.equal((await stat(path.join(outputDirectory, file))).mode & 0o777, 0o600);
      const content = await readFile(path.join(outputDirectory, file), "utf8");
      assert.equal(content.includes("PARENT_OK"), false);
      assert.equal(content.includes("probe-ok"), false);
    }
  } finally {
    await rm(outputDirectory, { recursive: true, force: true });
  }
});
