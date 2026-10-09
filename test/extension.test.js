import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import adam from "../extension.js";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

test("the compiled ClojureScript extension registers and serves adam:status", async () => {
  const commands = new Map();
  const events = new Map();
  const pi = {
    registerCommand(name, definition) {
      commands.set(name, definition);
    },
    on(name, handler) {
      events.set(name, handler);
    },
  };

  const neo4jEnvironment = [
    "ADAM_NEO4J_URI",
    "ADAM_NEO4J_USERNAME",
    "ADAM_NEO4J_PASSWORD",
    "ADAM_NEO4J_DATABASE",
  ];
  const savedEnvironment = new Map(neo4jEnvironment.map((key) => [key, process.env[key]]));
  try {
    for (const key of neo4jEnvironment) delete process.env[key];
    await adam(pi);
  } finally {
    for (const [key, value] of savedEnvironment) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }

  const status = commands.get("adam:status");
  assert.ok(status, "expected adam:status to be registered");
  assert.equal(status.description, "Show adam status");

  const notifications = [];
  await status.handler("", {
    ui: {
      notify(message, level) {
        notifications.push([message, level]);
      },
    },
  });

  assert.equal(notifications.length, 1);
  assert.equal(notifications[0][1], "info");
  assert.match(notifications[0][0], /adam session replica: (disabled|not connected)/);
  assert.equal(events.size, 0, "disabled configuration must not register lifecycle synchronization");
});

test("compiled lifecycle callbacks expose no Pi event result for ephemeral sessions", () => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-extension-results-"));
  const env = { ...isolatedAdamEnvironment(root),
    ADAM_NEO4J_URI: "bolt://127.0.0.1:1",
    ADAM_NEO4J_USERNAME: "test",
    ADAM_NEO4J_PASSWORD: "test-only-unreachable",
  };
  const program = `
    import assert from "node:assert/strict";
    import adam from ${JSON.stringify(new URL("../extension.js", import.meta.url).href)};
    const events = new Map();
    await adam({ registerCommand() {}, registerTool() {}, on(name, handler) { events.set(name, handler); } });
    const ctx = { cwd: ${JSON.stringify(root)}, sessionManager: { getSessionFile: () => null }, ui: { notify() {} } };
    const names = ["session_start", "turn_end", "session_compact", "session_tree", "session_info_changed", "model_select", "thinking_level_select"];
    for (const name of names) {
      const work = events.get(name)({ type: name }, ctx);
      assert.equal(typeof work?.then, "function", "handler still returns awaitable work");
      assert.equal(await work, undefined, name + " must not leak an internal boundary result");
    }
    await events.get("session_shutdown")({}, ctx);
    console.log(JSON.stringify(names));
  `;
  try {
    const stdout = execFileSync(process.execPath, ["--input-type=module", "-e", program],
      { env, cwd: root, encoding: "utf8", timeout: 15000 });
    assert.equal(JSON.parse(stdout).length, 7);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
