import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { isolatedAdamEnvironment, seedTestIdentity, assertTestIdentity } from "./support/adam-environment.js";
import { cleanupTestSessions } from "./support/neo4j-cleanup.js";
import { requireDisposableNeo4j } from "../scripts/require-disposable-neo4j.mjs";

test("Adam subprocess fixtures isolate legacy identity and all state roots, including outage hooks", () => {
  const root = path.resolve("/tmp/fixture-only");
  const inherited = { HOME: "/simulated-user", PI_CODING_AGENT_DIR: "/simulated-user/.pi/agent",
    XDG_STATE_HOME: "/simulated-user/state", XDG_DATA_HOME: "/simulated-user/data",
    ADAM_NEO4J_URI: "bolt://production.invalid:7687", ADAM_NEO4J_PASSWORD: "must-not-inherit",
    ADAM_MEMORY_SOURCE_WAIT_MS: "1" };
  const environment = isolatedAdamEnvironment(root, inherited);
  assert.equal(environment.PI_CODING_AGENT_DIR, path.join(root, "pi"));
  assert.equal(environment.XDG_CONFIG_HOME, root);
  assert.equal(environment.XDG_STATE_HOME, path.join(root, "state"));
  assert.equal(environment.XDG_DATA_HOME, path.join(root, "data"));
  assert.equal(environment.ADAM_NEO4J_URI, undefined);
  assert.equal(environment.ADAM_NEO4J_PASSWORD, undefined);
  assert.equal(environment.ADAM_MEMORY_SOURCE_WAIT_MS, "600000");
  assert.equal(inherited.PI_CODING_AGENT_DIR, "/simulated-user/.pi/agent");
});

test("seeded test identity differs from synthetic user legacy state; changed runtime identity fails closed", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-identity-isolation-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const legacy = path.join(root, "simulated-user", ".pi", "agent", "adam");
  const inheritedUuid = randomUUID();
  const payload = JSON.stringify({ version: 1, userUuid: inheritedUuid });
  await mkdir(legacy, { recursive: true, mode: 0o700 });
  await writeFile(path.join(legacy, "config.json"), payload);
  const environment = isolatedAdamEnvironment(path.join(root, "fixture"), {
    PI_CODING_AGENT_DIR: path.dirname(legacy),
  });
  const expected = await seedTestIdentity(environment);
  assert.notEqual(expected, inheritedUuid);
  await assertTestIdentity(environment, expected);
  await writeFile(path.join(environment.XDG_CONFIG_HOME, "adam", "config.json"), payload);
  await assert.rejects(assertTestIdentity(environment, expected), /independently seeded identity/);
  assert.equal(await readFile(path.join(legacy, "config.json"), "utf8"), payload);
});

test("teardown authorization uses exact registered session IDs, not runtime state or all user sessions", async () => {
  const uuid = randomUUID();
  const id = `urn:adam:session:${uuid}:pi:fixture`;
  const calls = [];
  await cleanupTestSessions({ async run(query, parameters) { calls.push({ query, parameters }); } }, uuid, [id]);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].parameters.sessionIds, [id]);
  assert.match(calls[0].query, /WHERE s.id IN \$sessionIds/);
  assert.doesNotMatch(calls[0].query, /ABOUT|TOUCHES|FORKED_FROM/);
  assert.match(calls[1].query, /WHERE NOT \(u\)--\(\) DELETE u/);
  calls.length = 0;
  await assert.rejects(cleanupTestSessions({ async run() { calls.push(1); } }, uuid,
    [`urn:adam:session:${randomUUID()}:pi:unrelated`]));
  assert.deepEqual(calls, []);
});

test("live suite refuses unacknowledged endpoints before connecting", () => {
  assert.throws(() => requireDisposableNeo4j({ ADAM_TEST_NEO4J_URI: "bolt://production.invalid:7687" }), /DISPOSABLE/);
  assert.throws(() => requireDisposableNeo4j({ ADAM_TEST_NEO4J_DISPOSABLE: "1" }), /URI, USERNAME and PASSWORD/);
});
