import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url)));

test("normal and live npm gates execute compiled Node tests explicitly", () => {
  assert.match(pkg.scripts["test:cljs"], /&& node target\/tests\.cjs(?:\s|$)/);
  assert.match(pkg.scripts["test:neo4j"], /&& node target\/neo4j-tests\.cjs &&/);
});

for (const [mode, expected] of [["pass", 0], ["assertion", 1], ["captured-error", 1], ["uncaught-rejection", 1], ["incomplete", 1], ["active-hang", 1], ["empty-selection", 1]]) {
  test(`real ClojureScript ${mode} outcome gates subsequent work`, () => {
    assert.ok(existsSync("target/test-exit-probe.cjs"), "compile the fixture first: npm run test:cljs");
    const root = mkdtempSync(path.join(tmpdir(), "adam-test-exit-"));
    const marker = path.join(root, "later-step");
    // This fixture imports cljs.test only: no Adam initialization or database IO.
    const program = `
      const { spawnSync } = require('node:child_process');
      const { writeFileSync } = require('node:fs');
      const result = spawnSync(process.execPath, ['target/test-exit-probe.cjs',
        ...(process.env.ADAM_TEST_EXIT_PROBE === 'empty-selection' ? ['--test=adam.nonexistent-test-namespace'] : [])], { env: process.env, encoding: 'utf8' });
      process.stdout.write(result.stdout || ''); process.stderr.write(result.stderr || '');
      if (result.error) throw result.error;
      if (result.status !== 0) process.exit(result.status ?? 1);
      writeFileSync(${JSON.stringify(marker)}, 'completed');
    `;
    try {
      const result = spawnSync(process.execPath, ["-e", program], {
        env: { ...process.env, ADAM_TEST_EXIT_PROBE: mode,
          ...(mode === "active-hang" ? { ADAM_TEST_RUN_TIMEOUT_MS: "50" } : {}) }, encoding: "utf8", timeout: 30000,
      });
      assert.equal(result.status, expected, result.stdout + result.stderr);
      assert.equal(existsSync(marker), expected === 0);
      if (["pass", "assertion", "captured-error"].includes(mode)) assert.match(result.stdout, /Ran 2 tests/);
      if (["assertion", "captured-error"].includes(mode)) assert.match(result.stdout, /1 failures, 0 errors|0 failures, 1 errors/);
      if (mode === "incomplete") assert.match(result.stderr, /runner ended without completion/);
      if (mode === "uncaught-rejection") assert.match(result.stderr, /intentional uncaught runner probe/);
      if (mode === "active-hang") assert.match(result.stderr, /completion timeout: elapsed \d+ ms; budget 50 ms/);
      if (mode === "empty-selection") assert.match(result.stderr, /No ClojureScript tests registered or selected/);
    } finally { rmSync(root, { recursive: true, force: true }); }
  });
}
