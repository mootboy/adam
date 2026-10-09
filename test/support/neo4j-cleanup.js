import assert from "node:assert/strict";

// Caller records exact canonical session IDs before creating them. Follow only
// session-owned raw/memory children; never ABOUT/TOUCHES or fork/user edges.
export async function cleanupTestSessions(session, userUuid, sessionIds) {
  assert.match(userUuid, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  const prefix = `urn:adam:session:${userUuid}:`;
  assert.ok(sessionIds.every((id) => typeof id === "string" && id.startsWith(prefix)));
  const parameters = { userId: `urn:adam:user:${userUuid}`, sessionIds: [...new Set(sessionIds)] };
  await session.run(
    `MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)
     WHERE s.id IN $sessionIds
     OPTIONAL MATCH (s)-[:HAS_ENTRY|HAS_STREAM|HAS_MEMORY|HAS_MEMORY_STREAM]->(child)
     OPTIONAL MATCH (s)-[:HAS_MEMORY_STREAM]->(:AdamMemoryStream)-[:HAS_RECORD]->(record)
     WITH collect(DISTINCT s) + collect(DISTINCT child) + collect(DISTINCT record) AS nodes
     UNWIND nodes AS n
     WITH DISTINCT n WHERE n IS NOT NULL
     DETACH DELETE n`, parameters);
  // Do not detach-delete a user that still owns unrelated resources.
  await session.run(
    "MATCH (u:AdamUser {id: $userId}) WHERE NOT (u)--() DELETE u", parameters);
}
