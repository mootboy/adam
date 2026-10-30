import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn, execFileSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { randomUUID, createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import neo4j from 'neo4j-driver';
import { isolatedAdamEnvironment, seedTestIdentity, assertTestIdentity } from './support/adam-environment.js';
import { cleanupTestSessions } from './support/neo4j-cleanup.js';
import { requireDisposableNeo4j } from '../scripts/require-disposable-neo4j.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');

test('packaged Pi stays responsive through long-session reads, processing and committed projection', async t => {
  if (!process.env.ADAM_TEST_NEO4J_URI) return t.skip('opt-in disposable Neo4j only');
  requireDisposableNeo4j();
  const root = await mkdtemp(path.join(tmpdir(), 'adam-background-live-'));
  const env = { ...isolatedAdamEnvironment(root),
    ADAM_NEO4J_URI: process.env.ADAM_TEST_NEO4J_URI,
    ADAM_NEO4J_USERNAME: process.env.ADAM_TEST_NEO4J_USERNAME,
    ADAM_NEO4J_PASSWORD: process.env.ADAM_TEST_NEO4J_PASSWORD,
    ADAM_NEO4J_DATABASE: process.env.ADAM_TEST_NEO4J_DATABASE ?? 'neo4j',
    GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1',
  };
  const uuid = await seedTestIdentity(env);
  const id = randomUUID(), sid = `urn:adam:session:${uuid}:pi:${id}`;
  const origin = `example.invalid/adam-background-${id}`;
  const repo = `urn:adam:repository:git:${hash(origin)}`;
  const file = `urn:adam:file:${hash(repo + '\0src/a.cljs')}`;
  const transcript = path.join(root, 'session.jsonl');
  const driver = neo4j.driver(env.ADAM_NEO4J_URI, neo4j.auth.basic(env.ADAM_NEO4J_USERNAME, env.ADAM_NEO4J_PASSWORD));
  const session = driver.session({ database: env.ADAM_NEO4J_DATABASE });
  try {
    execFileSync('git', ['init', '-q', root], { env });
    execFileSync('git', ['-C', root, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
      '-c', 'commit.gpgsign=false', '-c', 'core.hooksPath=/dev/null', 'commit', '--allow-empty', '-qm', 'fixture'], { env });
    execFileSync('git', ['-C', root, 'remote', 'add', 'origin', `https://${origin}.git`], { env });
    const records = [{ type: 'session', id, cwd: root }];
    for (let n = 0; n < 6000; n++) records.push({ type: 'message', id: `e${n}`, parentId: n ? `e${n-1}` : null,
      payload: 'synthetic payload '.repeat(60),
      message: n % 2 === 0
        ? { role: 'assistant', content: [{ type: 'toolCall', id: `c${n/2}`, name: 'read', arguments: { path: 'src/a.cljs' } }] }
        : { role: 'toolResult', toolCallId: `c${(n-1)/2}`, content: [] } });
    records.push({ type: 'custom', id: 'memory', parentId: 'e5999', customType: 'om.observations.recorded', data: {
      coversUpToId: 'e5999', observations: Array.from({ length: 1000 }, (_, n) => ({
        id: n.toString(16).padStart(12, '0'), content: 'Synthetic sourced finding.',
        timestamp: '2026-10-28T00:00:00Z', relevance: 'high', tokenCount: 5,
        sourceEntryIds: [`e${n*2}`, `e${n*2+1}`],
      })),
    } });
    await writeFile(transcript, records.map(JSON.stringify).join('\n') + '\n');
    const program = `
      import assert from 'node:assert/strict';
      import {execFile} from 'node:child_process';
      import extension from ${JSON.stringify(new URL('../extension.js', import.meta.url).href)};
      const commands = new Map(), events = new Map();
      extension({ registerCommand(n, c) { commands.set(n, c); }, registerTool() {}, on(n, f) { events.set(n, f); },
        exec(command, args, options) { return new Promise(resolve => execFile(command, args, options,
          (error, stdout, stderr) => resolve({ code: error?.code ?? 0, stdout, stderr }))); } });
      const ctx = { cwd: ${JSON.stringify(root)}, sessionManager: { getSessionFile: () => ${JSON.stringify(transcript)}, getLeafId: () => 'memory' },
        ui: { notify() {} }, modelRegistry: { streamSimple() { throw Error('model calls forbidden'); } } };
      let last = performance.now(), maxGap = 0, ticks = 0;
      const heartbeat = setInterval(() => { const now = performance.now(); maxGap = Math.max(maxGap, now-last); last=now; ticks++; }, 5);
      try {
        const before = performance.now();
        assert.equal(events.get('session_start')({}, ctx), undefined);
        const hookMs = performance.now()-before;
        assert.ok(hookMs < 100, 'hook delayed '+hookMs+' ms');
        await commands.get('adam:reconcile').handler('', ctx);
        assert.ok(ticks > 20, 'heartbeat did not run during reconciliation');
        assert.ok(maxGap < 500, 'main thread stalled '+maxGap+' ms');
        console.error(JSON.stringify({hookMs, maxGap, ticks}));
      } finally { clearInterval(heartbeat); await events.get('session_shutdown')({}, ctx); }
    `;
    const result = await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['--input-type=module', '-e', program], { env, stdio: ['ignore', 'ignore', 'pipe'] });
      let stderr = '';
      const timer = setTimeout(() => child.kill('SIGTERM'), 60000);
      child.stderr.setEncoding('utf8'); child.stderr.on('data', chunk => { stderr += chunk; });
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.once('exit', code => { clearTimeout(timer); resolve({ code, stderr }); });
    });
    assert.equal(result.code, 0, result.stderr);
    t.diagnostic(result.stderr.trim());
    const rows = await session.run(`MATCH (s:AdamSession {id:$sid})
      RETURN s.entryCount AS count, s.currentLeafId AS leaf,
        COUNT { (s)-[:HAS_ENTRY]->() } AS entries,
        COUNT { (s)-[:HAS_MEMORY]->(:AdamObservation)-[:ABOUT]->() } AS memories`, { sid });
    assert.equal(Number(rows.records[0].get('count')), 6001);
    assert.equal(Number(rows.records[0].get('entries')), 6001);
    assert.equal(rows.records[0].get('leaf'), 'memory');
    assert.equal(Number(rows.records[0].get('memories')), 1000);
  } finally {
    try {
      await assertTestIdentity(env, uuid);
      await cleanupTestSessions(session, uuid, [sid]);
      // Exact synthetic resources only; never delete a shared real origin/file.
      await session.run(`MATCH (f:AdamCodeFile {id:$file})
        WHERE NOT EXISTS { MATCH ()-[:TOUCHES|ABOUT]->(f) } DETACH DELETE f`, { file });
      await session.run(`MATCH (r:AdamRepository {id:$repo})
        WHERE NOT EXISTS { MATCH ()-[:WORKED_ON]->(r) } DETACH DELETE r`, { repo });
    } finally { await session.close(); await driver.close(); await rm(root, { recursive: true, force: true }); }
  }
});
