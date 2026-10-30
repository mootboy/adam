import assert from 'node:assert/strict';
import test from 'node:test';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

function processor() {
  const worker = new Worker(new URL('../processor.js', import.meta.url), { execArgv: [], env: {} });
  let sequence = 0;
  const pending = new Map();
  worker.on('message', packet => {
    const task = pending.get(packet.id);
    if (!task) return;
    pending.delete(packet.id);
    if (packet.error) task.reject(new Error(packet.error.message));
    else task.resolve(packet.value);
  });
  worker.on('error', error => { for (const task of pending.values()) task.reject(error); pending.clear(); });
  return {
    call(event) {
      return new Promise((resolve, reject) => {
        const id = ++sequence;
        pending.set(id, { resolve, reject });
        worker.postMessage({ id, event });
      });
    },
    close: () => worker.terminate(),
  };
}

const record = value => ({ bytes: Buffer.from(JSON.stringify(value)), 'newline?': true });

test('compiled processor consumes record data and emits bounded replica batches', async () => {
  const service = processor();
  try {
    await service.call({ op: 'start', path: '/nonexistent/input.jsonl' });
    const records = [record({ type: 'session', id: 'synthetic', cwd: '/repo' }),
      record({ type: 'message', id: 'a', parentId: null }),
      record({ type: 'message', id: 'b', parentId: 'a' })];
    await service.call({ op: 'records', records, 'byte-start': 0 });
    const { summary } = await service.call({ op: 'finish', 'current-leaf-id': 'a' });
    assert.equal(summary['entry-count'], 2, 'worker never opens the nonexistent source locator');
    const plan = await service.call({ op: 'plan', 'user-uuid': 'test-user', 'current-leaf-id': 'a', 'batch-bytes': 1 });
    assert.equal(plan['batch-count'], 2);
    assert.equal(plan.session['current-leaf-id'], 'a');
    assert.equal(plan.batches, undefined);
    const batch = await service.call({ op: 'batch', index: 1 });
    assert.equal(batch.entries[0]['entry-id'], 'b', 'abandoned entries are still mirrored');
    await assert.rejects(service.call({ op: 'not-a-command' }), /invalid processing event/);
    await service.call({ op: 'release' });
  } finally { await service.close(); }
});

test('compiled processing of a large record batch leaves the host heartbeat responsive', async t => {
  const service = processor();
  let heartbeat;
  try {
    await service.call({ op: 'start', path: '/nonexistent/large.jsonl' });
    let latest = performance.now();
    let maxGap = 0;
    let ticks = 0;
    heartbeat = setInterval(() => {
      const now = performance.now();
      maxGap = Math.max(maxGap, now - latest);
      latest = now;
      ticks++;
    }, 5);
    await service.call({ op: 'records', records: [record({ type: 'session', id: 'large', cwd: '/repo' })] });
    for (let start = 0; start < 6000; start += 200) {
      const records = Array.from({ length: 200 }, (_, offset) => {
        const n = start + offset;
        return record({ type: 'message', id: `e${n}`, parentId: n ? `e${n-1}` : null,
          payload: 'synthetic evidence '.repeat(60),
          message: n % 2 === 0
            ? { role: 'assistant', content: [{ type: 'toolCall', id: `c${n/2}`, name: 'read', arguments: { path: 'src/a.cljs' } }] }
            : { role: 'toolResult', toolCallId: `c${(n-1)/2}`, content: [] } });
      });
      await service.call({ op: 'records', records });
    }
    const observations = Array.from({ length: 1000 }, (_, n) => ({
      id: n.toString(16).padStart(12, '0'), content: 'Synthetic sourced finding.',
      timestamp: '2026-10-28T00:00:00Z', relevance: 'high', tokenCount: 5,
      sourceEntryIds: [`e${n*2}`, `e${n*2+1}`],
    }));
    await service.call({ op: 'records', records: [record({ type: 'custom', id: 'memory', parentId: 'e5999',
      customType: 'om.observations.recorded', data: { coversUpToId: 'e5999', observations } })] });
    await service.call({ op: 'finish' });
    const prepared = await service.call({ op: 'prepare' });
    assert.equal(prepared.summary['entry-count'], 6001);
    const projection = await service.call({ op: 'projection', 'user-uuid': 'synthetic', streams: [],
      repositories: [{ id: 'synthetic-repository', root: '/repo', worktrees: [{ root: '/repo', commit: 'synthetic' }] }] });
    assert.equal(projection.observations.length, 1000);
    assert.equal(projection['entry-file-evidence'].length, 6000);
    assert.equal(projection.files.length, 1);
    assert.ok(ticks >= 5, `expected heartbeat during processing, got ${ticks}`);
    assert.ok(maxGap < 500, `host stalled ${maxGap.toFixed(1)} ms`);
    t.diagnostic(`6000 file-tool entries + 1000 sourced observations; ${ticks} heartbeat ticks; longest gap ${maxGap.toFixed(1)} ms (500 ms ceiling)`);
  } finally {
    clearInterval(heartbeat);
    await service.close();
  }
});
