import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

test("invalid source expiry policy exits before a worker connection or retry", () => {
  const result = spawnSync(process.execPath, ["worker.js", "--once"], {
    encoding: "utf8", timeout: 10000,
    env: { ...process.env,
      ADAM_NEO4J_URI: "bolt://127.0.0.1:1", ADAM_NEO4J_USERNAME: "neo4j",
      ADAM_NEO4J_PASSWORD: "unused-test-password", ADAM_MEMORY_SOURCE_WAIT_MS: "0",
    },
  });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /ADAM_MEMORY_SOURCE_WAIT_MS/);
  assert.doesNotMatch(result.stderr, /retrying in/);
});
