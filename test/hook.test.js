import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

const neo4jKeys = [
  "ADAM_NEO4J_URI",
  "ADAM_NEO4J_USERNAME",
  "ADAM_NEO4J_PASSWORD",
  "ADAM_NEO4J_DATABASE",
];

function hookEnvironment(configHome) {
  const environment = isolatedAdamEnvironment(configHome);
  for (const key of neo4jKeys) delete environment[key];
  return environment;
}

function runHook(input, environment) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const child = spawn(process.execPath, ["hook.js"], {
      cwd: process.cwd(),
      env: environment,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal, stderr, elapsed: Date.now() - startedAt }));
    child.stdin.end(input);
  });
}

test("the compiled Claude hook durably queues a locator and returns without Neo4j", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-hook-boundary-"));
  try {
    const result = await runHook(JSON.stringify({
      hook_event_name: "Stop",
      session_id: "claude-session-1",
      transcript_path: "/tmp/session.jsonl",
      cwd: "/tmp/project",
      private_content: "must not persist",
    }), hookEnvironment(root));

    assert.deepEqual({ code: result.code, signal: result.signal, stderr: result.stderr },
      { code: 0, signal: null, stderr: "" });
    assert.ok(result.elapsed < 2000, `hook took ${result.elapsed}ms`);

    const inbox = path.join(root, "adam", "inbox");
    const files = (await readdir(inbox)).filter((name) => name.endsWith(".json"));
    assert.equal(files.length, 1);
    const notification = await readFile(path.join(inbox, files[0]), "utf8");
    assert.equal(notification.includes("must not persist"), false);
    assert.deepEqual(
      Object.keys(JSON.parse(notification)).sort(),
      ["cwd", "event", "id", "queued-at", "session-id", "transcript-path", "version"],
    );

    // The detached worker cannot reach Neo4j here; its refusal must land in the worker log.
    const workerLog = path.join(root, "adam", "worker.log");
    const deadline = Date.now() + 5000;
    let logged = "";
    while (!logged.includes("adam worker unavailable") && Date.now() < deadline) {
      logged = await readFile(workerLog, "utf8").catch(() => "");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.match(logged, /adam worker unavailable: ADAM_NEO4J_URI/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the compiled Claude hook rejects oversized input without queuing it", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-hook-boundary-"));
  try {
    const result = await runHook("x".repeat(64 * 1024 + 1), hookEnvironment(root));
    assert.equal(result.code, 1);
    assert.match(result.stderr, /exceeds 64 KiB/);
    await assert.rejects(readdir(path.join(root, "adam", "inbox")), { code: "ENOENT" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
