import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import neo4j from "neo4j-driver";
import { isolatedAdamEnvironment, seedTestIdentity, assertTestIdentity } from "./support/adam-environment.js";
import { cleanupTestSessions } from "./support/neo4j-cleanup.js";
import { requireDisposableNeo4j } from "../scripts/require-disposable-neo4j.mjs";

function probe(program, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", program], { env, stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    const timer = setTimeout(() => child.kill("SIGTERM"), 60000);
    child.stderr.setEncoding("utf8"); child.stderr.on("data", chunk => { stderr += chunk; });
    child.once("error", error => { clearTimeout(timer); reject(error); });
    child.once("exit", code => { clearTimeout(timer); resolve({ code, stderr }); });
  });
}

test("packaged Pi generation, restart reuse and Pi/MCP reads preserve saved history", async t => {
  if (!process.env.ADAM_TEST_NEO4J_URI) return t.skip("opt-in disposable Neo4j only");
  requireDisposableNeo4j();
  const root = await mkdtemp(path.join(tmpdir(), "adam-derive-live-"));
  const env = {
    ...isolatedAdamEnvironment(root),
    ADAM_NEO4J_URI: process.env.ADAM_TEST_NEO4J_URI,
    ADAM_NEO4J_USERNAME: process.env.ADAM_TEST_NEO4J_USERNAME,
    ADAM_NEO4J_PASSWORD: process.env.ADAM_TEST_NEO4J_PASSWORD,
    ADAM_NEO4J_DATABASE: process.env.ADAM_TEST_NEO4J_DATABASE ?? "neo4j",
  };
  const uuid = await seedTestIdentity(env);
  const userId = `urn:adam:user:${uuid}`;
  const sessionId = `derive-${randomUUID()}`;
  const sessionUrn = `urn:adam:session:${uuid}:pi:${sessionId}`;
  const derivationId = `urn:adam:derivation:${createHash("sha256").update(JSON.stringify([userId, "preferences", { type: "none" }])).digest("hex")}`;
  const configPath = path.join(env.XDG_CONFIG_HOME, "adam", "derivations.yaml");
  const transcript = path.join(root, "session.jsonl");
  await mkdir(path.dirname(configPath), { recursive: true });
  await writeFile(configPath, "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope and uncertainty.\n", { mode: 0o600 });
  await writeFile(transcript, [
    { type: "session", id: sessionId, cwd: root },
    { type: "message", id: "request", parentId: null, message: { role: "user", content: "Please use small functions." } },
    { type: "custom", id: "memory", parentId: "request", customType: "om.observations.recorded", data: {
      coversUpToId: "request", observations: [{ id: "abcdef123456", content: "The user requests small functions.",
        timestamp: "2026-10-14T00:00:00.000Z", relevance: "high", tokenCount: 8, sourceEntryIds: ["request"] }],
    } },
  ].map(JSON.stringify).join("\n") + "\n");
  const driver = neo4j.driver(env.ADAM_NEO4J_URI, neo4j.auth.basic(env.ADAM_NEO4J_USERNAME, env.ADAM_NEO4J_PASSWORD));
  const session = driver.session({ database: env.ADAM_NEO4J_DATABASE });
  try {
    const program = `
      import assert from 'node:assert/strict';
      import { appendFile } from 'node:fs/promises';
      import extension from './extension.js';
      const commands = new Map(), events = new Map(), tools = new Map(), notices = [];
      const runtime = extension({ registerCommand(n, c) { commands.set(n, c); }, registerTool(tool) { tools.set(tool.name, tool); },
        on(n, f) { events.set(n, f); }, async exec() { return { code: 1, stdout: '', stderr: '' }; } });
      let count = 0, truncated = false;
      const ctx = { cwd: ${JSON.stringify(root)}, hasUI: true, waitForIdle: async () => {},
        messages: ['PRIVATE_CONVERSATION'], tools: ['write'],
        sessionManager: { getSessionFile: () => ${JSON.stringify(transcript)}, getLeafId: () => 'memory' },
        ui: { notify(message) { notices.push(message); }, confirm: async () => true },
        model: { provider: 'synthetic', id: 'fake', api: 'openai-responses', contextWindow: 200000, maxTokens: 8192 },
        modelRegistry: { hasConfiguredAuth() { return true; }, streamSimple(model, context, options) {
          count++; assert.equal(options.maxRetries, 0); assert.equal(options.cacheRetention, 'none');
          assert.equal(context.messages.length, 1); assert.deepEqual(context.tools, []);
          assert.ok(!JSON.stringify(context).includes('PRIVATE_CONVERSATION'));
          const data = JSON.parse(context.messages[0].content);
          assert.equal(data.memories.length, 1); assert.equal(data.memories[0].content, 'The user requests small functions.');
          const output = { conclusions: [{ id: 'preference', content: 'Prefer small functions in this context.', category: 'style', basis: 'explicit',
            'scope-type': 'personal', 'scope-subject-ids': [${JSON.stringify(userId)}],
            'supporting-memory-ids': [data.memories[0].id], 'conflicting-memory-ids': [] }], links: [] };
          return { result: async () => ({ stopReason: truncated ? 'length' : 'stop', content: [{ type: 'text', text: JSON.stringify(output) }] }) };
        } } };
      await events.get('session_start')({}, ctx); assert.equal(count, 0, 'lifecycle must not generate');
      await commands.get('adam:derive').handler('preferences', ctx);
      assert.equal(notices.at(-1), 'preferences: generated');
      await commands.get('adam:derive').handler('preferences', ctx);
      assert.equal(notices.at(-1), 'preferences: reused'); assert.equal(count, 1);
      await commands.get('adam:derivations').handler('preferences', ctx);
      const commandRead = notices.at(-1);
      const toolRead = await tools.get('adam_derivations').execute('read', { name: 'preferences' }, new AbortController().signal, undefined, ctx);
      assert.equal(toolRead.content[0].text, commandRead);
      assert.match(commandRead, /current/); assert.match(commandRead, /Prefer small functions in this context/);
      assert.ok(!commandRead.includes('Please use small functions.')); assert.equal(count, 1);
      truncated = true; await appendFile(${JSON.stringify(configPath)}, ${JSON.stringify('# configuration comment alone does not change fingerprints\n')});
      await commands.get('adam:derive').handler('preferences', ctx); assert.equal(count, 1);
      const fs = await import('node:fs/promises');
      await fs.writeFile(${JSON.stringify(configPath)}, ${JSON.stringify('derivations:\n  preferences:\n    version: 2\n    guidance: Preserve scoped corrections.\n')});
      await commands.get('adam:derive').handler('preferences', ctx);
      assert.equal(notices.at(-1), 'preferences: failed (invalid-output); validation incomplete-output'); assert.equal(count, 2);
      await events.get('session_shutdown')({}, ctx);
      console.error('packaged derivation acceptance passed');
    `;
    const result = await probe(program, env);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stderr, /packaged derivation acceptance passed/);
    await writeFile(configPath, "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope and uncertainty.\n", { mode: 0o600 });
    const restartProgram = program.slice(0, program.indexOf("      await commands.get('adam:derive').handler('preferences', ctx);")) + `
      await commands.get('adam:derive').handler('preferences', ctx);
      assert.equal(notices.at(-1), 'preferences: reused', 'normal restart must preserve unchanged derivation dependencies');
      assert.equal(count, 0, 'restart must not regenerate unchanged saved understanding');
      await commands.get('adam:derivations').handler('preferences', ctx);
      assert.match(notices.at(-1), /current/);
      assert.match(notices.at(-1), /Prefer small functions in this context/);
      await events.get('session_shutdown')({}, ctx);
    `;
    const restartResult = await probe(restartProgram, env);
    assert.equal(restartResult.code, 0, restartResult.stderr);
    await writeFile(configPath, 'derivations:\n  preferences:\n    version: 2\n    guidance: Preserve scoped corrections.\n', { mode: 0o600 });
    const mcpResult = await probe(`
      import assert from 'node:assert/strict';
      import { Client } from '@modelcontextprotocol/sdk/client/index.js';
      import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
      const client = new Client({ name: 'isolated-saved-read', version: '1' });
      const transport = new StdioClientTransport({ command: process.execPath,
        args: [${JSON.stringify(path.resolve('mcp.js'))}], env: process.env });
      try {
        await client.connect(transport);
        const listing = await client.listTools();
        const definition = listing.tools.find(tool => tool.name === 'adam_derivations');
        assert.equal(definition.annotations.readOnlyHint, true); assert.ok(definition.outputSchema);
        const read = await client.callTool({ name: 'adam_derivations', arguments: { name: 'preferences' } });
        assert.ok(!read.isError); assert.match(read.content[0].text, /Prefer small functions in this context/);
        assert.match(read.content[0].text, /outdated/);
        assert.equal(read.structuredContent['result-count'], 1);
        const invalid = await client.callTool({ name: 'adam_derivations', arguments: { name: 'preferences', readerId: 'other' } });
        assert.ok(invalid.isError);
      } finally { await client.close(); }
      console.error('saved Pi/MCP parity acceptance passed');
    `, env);
    assert.equal(mcpResult.code, 0, mcpResult.stderr);
    assert.match(mcpResult.stderr, /saved Pi\/MCP parity acceptance passed/);
    const saved = await session.run(`MATCH (d:AdamDerivation {id: $id})-[:LATEST_RUN]->(r)
      OPTIONAL MATCH (d)-[:HAS_RUN]->(history)
      RETURN d.refreshToken AS token, d.lastRefreshOutcome AS outcome, count(history) AS runs, r.inputMemoryCount AS inputs`, { id: derivationId });
    assert.equal(saved.records.length, 1);
    assert.equal(saved.records[0].get("token"), null);
    assert.equal(saved.records[0].get("outcome"), "reused");
    assert.equal(Number(saved.records[0].get("runs")), 1);
    assert.equal(Number(saved.records[0].get("inputs")), 1);
    const removedEvidenceProgram = program.slice(0, program.indexOf("      await commands.get('adam:derive').handler('preferences', ctx);"))
      .replace("getLeafId: () => 'memory'", "getLeafId: () => 'request'") + `
      await commands.get('adam:derivations').handler('preferences', ctx);
      assert.match(notices.at(-1), /unavailable/);
      assert.ok(!notices.at(-1).includes('Prefer small functions in this context'));
      assert.equal(count, 0);
      await events.get('session_shutdown')({}, ctx);
    `;
    const removedResult = await probe(removedEvidenceProgram, env);
    assert.equal(removedResult.code, 0, removedResult.stderr);
  } finally {
    try {
      await assertTestIdentity(env, uuid);
      await session.run(`MATCH (d:AdamDerivation {id: $id}) OPTIONAL MATCH (d)-[:HAS_RUN]->(r)
        OPTIONAL MATCH (r)-[:HAS_CONCLUSION]->(c) DETACH DELETE c, r, d`, { id: derivationId });
      await cleanupTestSessions(session, uuid, [sessionUrn]);
    } finally {
      await session.close(); await driver.close(); await rm(root, { recursive: true, force: true });
    }
  }
});
