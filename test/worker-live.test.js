import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import neo4j from "neo4j-driver";

function run(command, args, { env, input } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: process.cwd(), env: env ?? process.env, stdio: ["pipe", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal, stderr }));
    child.stdin.end(input);
  });
}

function workerEnvironment(configHome) {
  return {
    ...process.env,
    XDG_CONFIG_HOME: configHome,
    ADAM_NEO4J_URI: process.env.ADAM_TEST_NEO4J_URI,
    ADAM_NEO4J_USERNAME: process.env.ADAM_TEST_NEO4J_USERNAME,
    ADAM_NEO4J_PASSWORD: process.env.ADAM_TEST_NEO4J_PASSWORD,
    ADAM_NEO4J_DATABASE: process.env.ADAM_TEST_NEO4J_DATABASE ?? "neo4j",
  };
}

test("durable Claude notification repairs after an outage and projects native file evidence", async (t) => {
  const uri = process.env.ADAM_TEST_NEO4J_URI;
  const username = process.env.ADAM_TEST_NEO4J_USERNAME;
  const password = process.env.ADAM_TEST_NEO4J_PASSWORD;
  if (!uri || !username || !password) return t.skip("ADAM_TEST_NEO4J_* is not configured");

  const root = await mkdtemp(path.join(tmpdir(), "adam-worker-live-"));
  const configHome = path.join(root, "config");
  const sessionId = `worker-live-${crypto.randomUUID()}`;
  const transcriptPath = path.join(root, "session.jsonl");
  const agentTranscriptPath = path.join(root, "agent-a1.jsonl");
  const cwd = process.cwd();
  const lines = [
    { type: "assistant", uuid: "call-1", parentUuid: null, sessionId, cwd, requestId: "request-1",
      message: { role: "assistant", content: [{ type: "tool_use", id: "tool-1", name: "Read", input: { file_path: "README.md" } }] } },
    { type: "user", uuid: "result-1", parentUuid: "call-1", sessionId, cwd, sourceToolAssistantUUID: "call-1",
      message: { role: "user", content: [{ type: "tool_result", tool_use_id: "tool-1", content: "ok" }] } },
    { type: "last-prompt", leafUuid: "result-1", sessionId, cwd },
  ];
  await writeFile(transcriptPath, `${lines.map(JSON.stringify).join("\n")}\n`, "utf8");
  const agentLines = [
    { type: "assistant", uuid: "agent-call-1", parentUuid: null, sessionId, agentId: "a1",
      isSidechain: true, cwd, requestId: "agent-request-1",
      message: { role: "assistant", content: [{ type: "tool_use", id: "agent-tool-1", name: "Read", input: { file_path: "README.md" } }] } },
    { type: "user", uuid: "agent-result-1", parentUuid: "agent-call-1", sessionId, agentId: "a1",
      isSidechain: true, cwd, sourceToolAssistantUUID: "agent-call-1",
      message: { role: "user", content: [{ type: "tool_result", tool_use_id: "agent-tool-1", content: "ok" }] } },
  ];
  await writeFile(agentTranscriptPath, `${agentLines.map(JSON.stringify).join("\n")}\n`, "utf8");

  const notification = JSON.stringify({
    hook_event_name: "SubagentStop", session_id: sessionId,
    transcript_path: transcriptPath, agent_transcript_path: agentTranscriptPath,
    agent_id: "a1", cwd,
  });
  const unavailableEnvironment = { ...process.env, XDG_CONFIG_HOME: configHome };
  for (const key of ["ADAM_NEO4J_URI", "ADAM_NEO4J_USERNAME", "ADAM_NEO4J_PASSWORD", "ADAM_NEO4J_DATABASE"]) {
    delete unavailableEnvironment[key];
  }

  const driver = neo4j.driver(uri, neo4j.auth.basic(username, password));
  const session = driver.session({ database: process.env.ADAM_TEST_NEO4J_DATABASE ?? "neo4j" });
  try {
    assert.equal((await run(process.execPath, ["hook.js"], { env: unavailableEnvironment, input: notification })).code, 0);
    const inboxDirectory = path.join(configHome, "adam", "inbox");
    assert.equal((await readdir(inboxDirectory)).filter((name) => name.endsWith(".json")).length, 1);

    const workerResult = await run(process.execPath, ["worker.js"], { env: workerEnvironment(configHome) });
    assert.deepEqual(workerResult, { code: 0, signal: null, stderr: "" });
    assert.equal((await readdir(inboxDirectory)).filter((name) => name.endsWith(".json")).length, 0);

    const state = JSON.parse(await readFile(path.join(configHome, "adam", "config.json"), "utf8"));
    const sessionUrn = `urn:adam:session:${state.userUuid}:claude-code:${sessionId}`;
    const result = await session.run(
      `MATCH (s:AdamSession {id: $sessionId})
       OPTIONAL MATCH (s)-[:HAS_STREAM]->(stream:AdamTranscriptStream)
       OPTIONAL MATCH (s)-[:HAS_ENTRY]->(entry:AdamEntry)
       OPTIONAL MATCH (entry)-[touch:TOUCHES]->(file:AdamCodeFile)
       RETURN s.currentLeafId AS leaf,
              count(DISTINCT stream) AS streams,
              count(DISTINCT entry) AS entries,
              count(DISTINCT touch) AS touches,
              collect(DISTINCT file.relativePath) AS files`,
      { sessionId: sessionUrn },
    );
    const record = result.records[0];
    assert.equal(record.get("leaf"), "result-1");
    assert.equal(record.get("streams").toNumber(), 2);
    assert.equal(record.get("entries").toNumber(), 5);
    assert.equal(record.get("touches").toNumber(), 4);
    assert.deepEqual(record.get("files"), ["README.md"]);
  } finally {
    const state = await readFile(path.join(configHome, "adam", "config.json"), "utf8")
      .then(JSON.parse)
      .catch(() => null);
    if (state) {
      const parameters = { userId: `urn:adam:user:${state.userUuid}` };
      await session.run(
        "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession) DETACH DELETE s",
        parameters,
      ).catch(() => {});
      await session.run(
        "MATCH (u:AdamUser {id: $userId}) DETACH DELETE u",
        parameters,
      ).catch(() => {});
    }
    await session.close();
    await driver.close();
    await rm(root, { recursive: true, force: true });
  }
});
