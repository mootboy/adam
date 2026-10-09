import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import neo4j from "neo4j-driver";
import { cleanupTestSessions } from "./support/neo4j-cleanup.js";
import { requireDisposableNeo4j } from "../scripts/require-disposable-neo4j.mjs";

test("scoped teardown preserves unrelated sessions, raw records and provenance even under the same user", async (t) => {
  const { ADAM_TEST_NEO4J_URI: uri, ADAM_TEST_NEO4J_USERNAME: username, ADAM_TEST_NEO4J_PASSWORD: password } = process.env;
  if (!uri || !username || !password) return t.skip("ADAM_TEST_NEO4J_* is not configured");
  requireDisposableNeo4j();
  const uuid = randomUUID();
  const otherUuid = randomUUID();
  const prefix = `urn:adam:session:${uuid}:claude-code:`;
  const target = `${prefix}registered`;
  const retained = `${prefix}not-registered`;
  const other = `urn:adam:session:${otherUuid}:pi:unrelated`;
  const ownUser = `urn:adam:user:${uuid}`;
  const otherUser = `urn:adam:user:${otherUuid}`;
  const file = `urn:adam:file:test-cleanup:${randomUUID()}`;
  const targetIds = [target, `${target}:entry`, `${target}:stream`, `${target}:memory`, `${target}:memory-stream`, `${target}:record`];
  const protectedIds = [ownUser, otherUser, retained, `${retained}:entry`, `${retained}:stream`,
    `${retained}:memory`, other, `${other}:entry`, file];
  const allIds = [...targetIds, ...protectedIds];
  const driver = neo4j.driver(uri, neo4j.auth.basic(username, password));
  const session = driver.session({ database: process.env.ADAM_TEST_NEO4J_DATABASE ?? "neo4j" });
  const snapshot = async () => {
    const result = await session.run(
      `MATCH (n) WHERE n.id IN $ids
       OPTIONAL MATCH (n)-[r]->(other) WHERE other.id IN $ids
       RETURN n.id AS id, properties(n) AS properties,
         collect(type(r) + ':' + other.id) AS edges ORDER BY id`, { ids: protectedIds });
    return result.records.map((record) => ({ id: record.get("id"), properties: record.get("properties"), edges: record.get("edges").sort() }));
  };
  try {
    await session.run(
      `CREATE (u:AdamUser {id: $ownUser}), (v:AdamUser {id: $otherUser}), (f:AdamCodeFile {id: $file})
       CREATE (s:AdamSession {id: $target}), (p:AdamSession {id: $retained}), (q:AdamSession {id: $other})
       CREATE (u)-[:OWNS]->(s), (u)-[:OWNS]->(p), (v)-[:OWNS]->(q)
       WITH s, p, q, f
       UNWIND [s,p,q] AS source
       CREATE (e:AdamEntry {id: source.id + ':entry', rawJson: 'must preserve unrelated raw payload'})
       CREATE (source)-[:HAS_ENTRY]->(e), (e)-[:TOUCHES]->(f)
       WITH DISTINCT s, p, f
       UNWIND [s,p] AS source
       MATCH (source)-[:HAS_ENTRY]->(sourceEntry)
       CREATE (stream:AdamTranscriptStream {id: source.id + ':stream'})
       CREATE (memory:AdamObservation {id: source.id + ':memory', content: 'retained memory'})
       CREATE (source)-[:HAS_STREAM]->(stream), (source)-[:HAS_MEMORY]->(memory), (memory)-[:ABOUT]->(f),
         (stream)-[:HAS_ENTRY]->(sourceEntry), (memory)-[:SOURCED_FROM]->(sourceEntry)
       WITH DISTINCT s
       CREATE (m:AdamMemoryStream {id: s.id + ':memory-stream'}), (r:AdamMemoryRecord {id: s.id + ':record', rawJson: 'raw sidecar'})
       CREATE (s)-[:HAS_MEMORY_STREAM]->(m), (m)-[:HAS_RECORD]->(r)`,
      { ownUser, otherUser, file, target, retained, other });
    const before = await snapshot();
    assert.equal(before.length, protectedIds.length);
    await cleanupTestSessions(session, uuid, [target]);
    const removed = await session.run("MATCH (n) WHERE n.id IN $ids RETURN count(n) AS count", { ids: targetIds });
    assert.equal(removed.records[0].get("count").toNumber(), 0);
    assert.deepEqual(await snapshot(), before);
    await cleanupTestSessions(session, uuid, [target]);
    assert.deepEqual(await snapshot(), before, "repeated teardown stays narrow and idempotent");
  } finally {
    try {
      // Every ID here was allocated locally, independently of runtime state.
      await session.run("MATCH (n) WHERE n.id IN $ids DETACH DELETE n", { ids: allIds });
    } finally {
      await session.close();
      await driver.close();
    }
  }
});
