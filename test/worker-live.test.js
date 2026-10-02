import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile, utimes } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
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
    XDG_STATE_HOME: path.join(configHome, "state"),
    XDG_DATA_HOME: path.join(configHome, "data"),
    PI_CODING_AGENT_DIR: path.join(configHome, "pi"),
    ADAM_NEO4J_URI: process.env.ADAM_TEST_NEO4J_URI,
    ADAM_NEO4J_USERNAME: process.env.ADAM_TEST_NEO4J_USERNAME,
    ADAM_NEO4J_PASSWORD: process.env.ADAM_TEST_NEO4J_PASSWORD,
    ADAM_NEO4J_DATABASE: process.env.ADAM_TEST_NEO4J_DATABASE ?? "neo4j",
    ADAM_MEMORY_SOURCE_WAIT_MS: "600000",
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

    const environment = workerEnvironment(configHome);
    const producer = "org.example.worker-memory";
    const relativeLocator = `claude-code/${createHash("sha256").update(sessionId).digest("hex")}/${producer}.jsonl`;
    const sidecar = path.join(environment.XDG_DATA_HOME, "adam", "memories", "v1", relativeLocator);
    const memoryInbox = path.join(environment.XDG_STATE_HOME, "adam", "memory-inbox");
    // Every component below the XDG roots is a real owner-only directory.
    for (const directory of [path.dirname(sidecar), memoryInbox]) {
      await mkdir(directory, { recursive: true, mode: 0o700 });
    }
    const event = {
      protocolVersion: 1, eventId: randomUUID(), kind: "observations.recorded",
      recordedAt: "2026-01-01T00:00:00.000Z", producer: { id: producer, version: "1.0.0" },
      source: { kind: "claude-code", sessionId },
      sourceCheckpoint: { streams: [{ streamId: "agent:a1", committedBytes: 1,
        prefixSha256: "a".repeat(64), selectedLeafEntryId: "agent-call-1" }] },
      observations: [{ id: "aaaaaaaaaaaa", content: "Worker memory from retained subagent evidence",
        timestamp: "2026-01-01T00:00:00.000Z", relevance: "high", tokenCount: 8,
        sourceEntries: [{ streamId: "agent:a1", entryId: "agent-call-1" }] }],
    };
    await writeFile(sidecar, `${JSON.stringify(event)}\n`, { mode: 0o600 });
    const enqueue = async (queuedAt = new Date().toISOString()) => {
      const id = randomUUID();
      await writeFile(path.join(memoryInbox, `${id}.json`), JSON.stringify({
        version: 1, id, sourceKind: "claude-code", sourceSessionId: sessionId,
        producerId: producer, sidecarLocator: relativeLocator, queuedAt,
      }), { mode: 0o600 });
    };
    // Fresh local notification, but the producer clock is 15 minutes behind.
    // It must wait for the source hook rather than expiring on its first drain.
    await enqueue(new Date(Date.now() - 900000).toISOString());
    const lifecycleFile = path.join(inboxDirectory, (await readdir(inboxDirectory)).find((name) => name.endsWith(".json")));
    const lifecycleBytes = await readFile(lifecycleFile);
    await rm(lifecycleFile);
    const awaitingSource = await run(process.execPath, ["worker.js", "--once"], { env: environment });
    assert.equal(awaitingSource.code, 1, "missing source ownership remains retryable");
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 1);
    await writeFile(lifecycleFile, lifecycleBytes, { mode: 0o600 });
    const workerResult = await run(process.execPath, ["worker.js"], { env: environment });
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
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 0);
    const memoryLinks = await session.run(
      `MATCH (s:AdamSession {id: $sessionId})-[:HAS_MEMORY]->(o:AdamObservation {producer: $producer})
       MATCH (o)-[:SOURCED_FROM]->(entry:AdamEntry {streamId: 'agent:a1'})
       MATCH (o)-[:ABOUT]->(file:AdamCodeFile {relativePath: 'README.md'})
       RETURN o.content AS content, entry.entryId AS entry`,
      { sessionId: sessionUrn, producer },
    );
    assert.equal(memoryLinks.records[0].get("entry"), "agent-call-1");
    assert.equal(memoryLinks.records[0].get("content"), event.observations[0].content);
    // Reversed final-hook ordering: append after the last transcript hook, then
    // restart explicitly without any further host event or transcript file.
    await rm(transcriptPath);
    await rm(agentTranscriptPath);
    const secondEvent = { ...event, eventId: randomUUID(), kind: "source.covered" };
    delete secondEvent.observations;
    await writeFile(sidecar, `${JSON.stringify(event)}\n${JSON.stringify(secondEvent)}\n`, { mode: 0o600 });
    await enqueue();
    const repaired = await run(process.execPath, ["worker.js", "--once"], { env: environment });
    assert.deepEqual(repaired, { code: 0, signal: null, stderr: "" });
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 0);
    const retained = await session.run(
      `MATCH (s:AdamSession {id: $sessionId})-[:HAS_MEMORY]->(o:AdamObservation {producer: $producer})
       MATCH (o)-[:ABOUT]->(file:AdamCodeFile {relativePath: 'README.md'})
       MATCH (s)-[:HAS_MEMORY_STREAM]->(:AdamMemoryStream)-[:HAS_RECORD]->(record:AdamMemoryRecord)
       RETURN count(DISTINCT o) AS memories, count(DISTINCT record) AS records`,
      { sessionId: sessionUrn, producer },
    );
    assert.equal(retained.records[0].get("memories").toNumber(), 1);
    assert.equal(retained.records[0].get("records").toNumber(), 2);

    // A positively absent source expires and lets detached mode exit without
    // touching an existing retained source/memory prefix. A later source hook
    // and fresh notification must still ingest normally for that same locator.
    const absentId = `absent-${randomUUID()}`;
    const absentLocator = `claude-code/${createHash("sha256").update(absentId).digest("hex")}/${producer}.jsonl`;
    const absentSidecar = path.join(environment.XDG_DATA_HOME, "adam", "memories", "v1", absentLocator);
    await mkdir(path.dirname(absentSidecar), { recursive: true, mode: 0o700 });
    const lateEvent = { ...event, eventId: randomUUID(), source: { kind: "claude-code", sessionId: absentId },
      sourceCheckpoint: { streams: [{ streamId: "main", committedBytes: 1, prefixSha256: "c".repeat(64), selectedLeafEntryId: "call-1" }] },
      observations: [{ ...event.observations[0], sourceEntries: [{ streamId: "main", entryId: "call-1" }] }] };
    await writeFile(absentSidecar, `${JSON.stringify(lateEvent)}\n`, { mode: 0o600 });
    const enqueueAbsent = async (queuedAt) => {
      const id = randomUUID();
      const notificationPath = path.join(memoryInbox, `${id}.json`);
      await writeFile(notificationPath, JSON.stringify({
        version: 1, id, sourceKind: "claude-code", sourceSessionId: absentId,
        producerId: producer, sidecarLocator: absentLocator, queuedAt,
      }), { mode: 0o600 });
      return notificationPath;
    };
    const prefixSnapshot = async () => {
      const rows = await session.run(
        `MATCH (:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(entry:AdamEntry)
         RETURN entry.id AS id, entry.rawJson AS raw ORDER BY id`, { sessionId: sessionUrn });
      return rows.records.map((row) => [row.get("id"), row.get("raw")]);
    };
    const originalPrefix = await prefixSnapshot();
    const agedFile = await enqueueAbsent(new Date().toISOString());
    const agedTime = new Date(Date.now() - 2000);
    await utimes(agedFile, agedTime, agedTime); // stable local age, not a producer timestamp
    const expiryEnvironment = { ...environment, ADAM_MEMORY_SOURCE_WAIT_MS: "1000" };
    const expired = await run(process.execPath, ["worker.js"], { env: expiryEnvironment });
    assert.equal(expired.code, 0, expired.stderr);
    assert.match(expired.stderr, /source-never-mirrored/);
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 0);
    assert.deepEqual(await prefixSnapshot(), originalPrefix);
    const absentStreams = await session.run(
      "MATCH (stream:AdamMemoryStream {sourceSessionId: $id}) RETURN count(stream) AS count", { id: absentId });
    assert.equal(absentStreams.records[0].get("count").toNumber(), 0, "expiry must not scan or mirror sidecars");
    const lateTranscript = path.join(root, "late-source.jsonl");
    await writeFile(lateTranscript, `${lines.map((line) => JSON.stringify({ ...line, sessionId: absentId })).join("\n")}\n`);
    const lateHook = await run(process.execPath, ["hook.js"], {
      env: { ...expiryEnvironment, ADAM_NEO4J_URI: "" },
      input: JSON.stringify({ hook_event_name: "Stop", session_id: absentId, transcript_path: lateTranscript, cwd }),
    });
    assert.equal(lateHook.code, 0, lateHook.stderr);
    await enqueueAbsent(new Date().toISOString());
    const lateRepair = await run(process.execPath, ["worker.js", "--once"], { env: expiryEnvironment });
    assert.equal(lateRepair.code, 0, lateRepair.stderr);
    const lateMemory = await session.run(
      `MATCH (:AdamUser {id: $userId})-[:OWNS]->(:AdamSession {sourceSessionId: $id})
       -[:HAS_MEMORY]->(o:AdamObservation)-[:ABOUT]->(:AdamCodeFile {relativePath: 'README.md'})
       RETURN o.content AS content`, { userId: `urn:adam:user:${state.userUuid}`, id: absentId });
    assert.equal(lateMemory.records[0].get("content"), lateEvent.observations[0].content);
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 0);

    // The packaged Pi composition root shares the same source-before-memory
    // repair path, including embedded memory from its authoritative ledger.
    const piId = `pi-worker-${randomUUID()}`;
    const piPath = path.join(root, "pi.jsonl");
    const piRecords = [
      { type: "session", id: piId, cwd },
      { type: "message", id: "pi-tool", parentId: null, message: { role: "assistant", content: [
        { type: "toolCall", id: "pi-call", name: "read", arguments: { path: "README.md" } },
      ] } },
      { type: "custom", id: "pi-memory", parentId: "pi-tool", customType: "om.observations.recorded",
        data: { coversUpToId: "pi-tool", observations: [{ id: "cccccccccccc", content: "embedded Pi memory",
          timestamp: event.recordedAt, relevance: "high", tokenCount: 3, sourceEntryIds: ["pi-tool"] }] } },
    ];
    await writeFile(piPath, `${piRecords.map(JSON.stringify).join("\n")}\n`);
    const piLocator = `pi/${createHash("sha256").update(piId).digest("hex")}/${producer}.jsonl`;
    const piSidecar = path.join(environment.XDG_DATA_HOME, "adam", "memories", "v1", piLocator);
    await mkdir(path.dirname(piSidecar), { recursive: true, mode: 0o700 });
    const piEvent = { ...event, eventId: randomUUID(), source: { kind: "pi", sessionId: piId },
      sourceCheckpoint: { streams: [{ streamId: "main", committedBytes: 1, prefixSha256: "b".repeat(64), selectedLeafEntryId: "pi-tool" }] },
      observations: [{ ...event.observations[0], id: "bbbbbbbbbbbb", content: "sidecar Pi memory",
        sourceEntries: [{ streamId: "main", entryId: "pi-tool" }] }] };
    await writeFile(piSidecar, `${JSON.stringify(piEvent)}\n`, { mode: 0o600 });
    const piNotificationId = randomUUID();
    await writeFile(path.join(memoryInbox, `${piNotificationId}.json`), JSON.stringify({
      version: 1, id: piNotificationId, sourceKind: "pi", sourceSessionId: piId,
      producerId: producer, sidecarLocator: piLocator, queuedAt: new Date().toISOString(),
    }), { mode: 0o600 });
    const piProbe = `
      import extension from './extension.js';
      import { execFile } from 'node:child_process';
      import { promisify } from 'node:util';
      const exec = promisify(execFile);
      const commands = new Map(); const events = new Map();
      extension({
        registerCommand(name, command) { commands.set(name, command); },
        registerTool() {}, on(name, handler) { events.set(name, handler); },
        async exec(cmd, args, options) {
          try { return { ...(await exec(cmd, args, options)), code: 0 }; }
          catch (error) { return { stdout: '', stderr: '', code: error.code ?? 1 }; }
        }
      });
      const ctx = { cwd: ${JSON.stringify(cwd)},
        sessionManager: { getSessionFile() { return ${JSON.stringify(piPath)}; }, getLeafId() { return 'pi-memory'; } },
        ui: { notify(message, level) { if (level === 'warning') console.error(message); } } };
      await commands.get('adam:reconcile').handler('', ctx);
      ctx.ui.notify = (message) => console.error(message);
      await commands.get('adam:status').handler('', ctx);
      await events.get('session_shutdown')({}, ctx);
    `;
    const piResult = await run(process.execPath, ["--input-type=module", "-e", piProbe], { env: environment });
    assert.equal(piResult.code, 0, piResult.stderr);
    assert.match(piResult.stderr, /adam session replica: connected/);
    assert.match(piResult.stderr, /Memory notifications: 1 acknowledged, 0 pending/);
    const piMemories = await session.run(
      `MATCH (:AdamUser {id: $userId})-[:OWNS]->(:AdamSession {sourceKind: 'pi', sourceSessionId: $sessionId})
       -[:HAS_MEMORY]->(memory:AdamObservation)-[:ABOUT]->(:AdamCodeFile {relativePath: 'README.md'})
       RETURN memory.content AS content ORDER BY content`,
      { userId: `urn:adam:user:${state.userUuid}`, sessionId: piId },
    );
    assert.deepEqual(piMemories.records.map((row) => row.get("content")), ["embedded Pi memory", "sidecar Pi memory"], piResult.stderr);
    assert.equal((await readdir(memoryInbox)).filter((name) => name.endsWith(".json")).length, 0);
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
