import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export function isolatedAdamEnvironment(root, inherited = process.env) {
  const environment = { ...inherited,
    XDG_CONFIG_HOME: root,
    XDG_STATE_HOME: path.join(root, "state"),
    XDG_DATA_HOME: path.join(root, "data"),
    PI_CODING_AGENT_DIR: path.join(root, "pi"),
    ADAM_MEMORY_SOURCE_WAIT_MS: "600000",
  };
  for (const key of Object.keys(environment)) {
    if (key.startsWith("ADAM_NEO4J_")) delete environment[key];
  }
  return environment;
}

// Allocate identity independently, before any subprocess can initialize Neo4j.
// Never learn teardown authorization from state created by the worker under test.
export async function seedTestIdentity(environment) {
  const userUuid = randomUUID();
  const payload = `${JSON.stringify({ version: 1, userUuid })}\n`;
  for (const root of [environment.XDG_CONFIG_HOME, environment.PI_CODING_AGENT_DIR]) {
    const directory = path.join(root, "adam");
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await writeFile(path.join(directory, "config.json"), payload, { flag: "wx", mode: 0o600 });
  }
  return userUuid;
}

export async function assertTestIdentity(environment, expectedUuid) {
  for (const root of [environment.XDG_CONFIG_HOME, environment.PI_CODING_AGENT_DIR]) {
    const state = JSON.parse(await readFile(path.join(root, "adam", "config.json"), "utf8"));
    assert.equal(state.userUuid, expectedUuid, "test process must retain its independently seeded identity");
  }
}
