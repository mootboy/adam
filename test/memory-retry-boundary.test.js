import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

test("invalid source expiry policy exits before a worker connection or retry", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-policy-isolated-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const result = spawnSync(process.execPath, ["worker.js", "--once"], {
    encoding: "utf8", timeout: 10000,
    env: { ...isolatedAdamEnvironment(root),
      ADAM_NEO4J_URI: "bolt://127.0.0.1:1", ADAM_NEO4J_USERNAME: "neo4j",
      ADAM_NEO4J_PASSWORD: "unused-test-password", ADAM_MEMORY_SOURCE_WAIT_MS: "0",
    },
  });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /ADAM_MEMORY_SOURCE_WAIT_MS/);
  assert.doesNotMatch(result.stderr, /retrying in/);
});
