import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const fixtures = path.resolve("docs/fixtures/memory-protocol-v1");

async function readJson(relativePath) {
  return JSON.parse(await readFile(path.join(fixtures, relativePath), "utf8"));
}

async function readCompleteLines(relativePath) {
  const bytes = await readFile(path.join(fixtures, relativePath));
  assert.equal(bytes.at(-1), 0x0a, `${relativePath} must end with LF`);
  return bytes.toString("utf8").trimEnd().split("\n");
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort(compareUtf16).map((key) =>
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function payloadHash(event) {
  return createHash("sha256").update(canonicalJson(event)).digest("hex");
}

function compareUtf16(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function checkpointRegresses(previousStreams, currentStreams) {
  const current = new Map(currentStreams.map((stream) => [stream.streamId, stream]));
  return previousStreams.some((prior) => {
    const next = current.get(prior.streamId);
    return !next
      || next.committedBytes < prior.committedBytes
      || (next.committedBytes === prior.committedBytes && next.prefixSha256 !== prior.prefixSha256);
  });
}

const protocolId = /^[a-z0-9](?:[a-z0-9._-]{0,127})$/;
const stableId = /^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,255})$/;
const memoryId = /^[a-f0-9]{12}$/;
const sha256 = /^[a-f0-9]{64}$/;
const relevance = new Set(["low", "medium", "high", "critical"]);

function assertUnique(values, label) {
  assert.equal(new Set(values).size, values.length, `${label} must be unique`);
}

function assertEventShape(event) {
  assert.equal(event.protocolVersion, 1);
  assert.match(event.eventId, stableId);
  assert.match(event.producer.id, protocolId);
  assert.ok(Buffer.byteLength(event.producer.version) > 0 && Buffer.byteLength(event.producer.version) <= 128);
  assert.match(event.source.kind, protocolId);
  assert.ok(Buffer.byteLength(event.source.sessionId) > 0 && Buffer.byteLength(event.source.sessionId) <= 512);
  assert.match(event.recordedAt, /Z$/);
  assert.equal(Number.isNaN(Date.parse(event.recordedAt)), false);

  const streams = event.sourceCheckpoint.streams;
  assert.ok(streams.length >= 1 && streams.length <= 256);
  assertUnique(streams.map(({ streamId }) => streamId), "checkpoint stream IDs");
  assert.deepEqual(streams.map(({ streamId }) => streamId),
    [...streams.map(({ streamId }) => streamId)].sort(compareUtf16));
  for (const stream of streams) {
    assert.ok(Number.isSafeInteger(stream.committedBytes) && stream.committedBytes >= 0);
    assert.match(stream.prefixSha256, sha256);
    assert.ok(stream.selectedLeafEntryId === null || typeof stream.selectedLeafEntryId === "string");
  }

  if (event.kind === "observations.recorded") {
    assert.ok(event.observations.length >= 1 && event.observations.length <= 256);
    assertUnique(event.observations.map(({ id }) => id), "observation IDs");
    assert.equal(event.reflections, undefined);
    assert.equal(event.observationIds, undefined);
    const checkpointStreams = new Set(streams.map(({ streamId }) => streamId));
    for (const observation of event.observations) {
      assert.match(observation.id, memoryId);
      assert.ok(Buffer.byteLength(observation.content) >= 1 && Buffer.byteLength(observation.content) <= 65536);
      assert.match(observation.timestamp, /Z$/);
      assert.ok(relevance.has(observation.relevance));
      assert.ok(Number.isSafeInteger(observation.tokenCount) && observation.tokenCount >= 0);
      assert.ok(observation.sourceEntries.length >= 1 && observation.sourceEntries.length <= 256);
      assertUnique(observation.sourceEntries.map(({ streamId, entryId }) => `${streamId}\0${entryId}`),
        "observation citations");
      for (const citation of observation.sourceEntries) assert.equal(checkpointStreams.has(citation.streamId), true);
    }
  } else if (event.kind === "reflections.recorded") {
    assert.ok(event.reflections.length >= 1 && event.reflections.length <= 256);
    assertUnique(event.reflections.map(({ id }) => id), "reflection IDs");
    assert.equal(event.observations, undefined);
    assert.equal(event.observationIds, undefined);
    for (const reflection of event.reflections) {
      assert.match(reflection.id, memoryId);
      assert.doesNotMatch(reflection.content, /[\r\n]/);
      assertUnique(reflection.supportingObservationIds, "reflection support IDs");
      for (const id of reflection.supportingObservationIds) assert.match(id, memoryId);
      assert.ok(Number.isSafeInteger(reflection.tokenCount) && reflection.tokenCount >= 0);
    }
  } else if (event.kind === "observations.dropped") {
    assert.ok(event.observationIds.length >= 1 && event.observationIds.length <= 256);
    assertUnique(event.observationIds, "dropped observation IDs");
    for (const id of event.observationIds) assert.match(id, memoryId);
    assert.equal(event.observations, undefined);
    assert.equal(event.reflections, undefined);
  } else {
    assert.equal(event.kind, "source.covered");
    assert.equal(event.observations, undefined);
    assert.equal(event.reflections, undefined);
    assert.equal(event.observationIds, undefined);
  }
}

test("memory protocol fixture manifest covers the v1 contract cases", async () => {
  const manifest = await readJson("manifest.json");
  assert.equal(manifest.protocolVersion, 1);
  assert.deepEqual(manifest.cases.map(({ id }) => id), [
    "valid-events",
    "identical-replay",
    "conflicting-event-id",
    "canonical-jcs",
    "malformed-complete-record",
    "incomplete-tail",
    "checkpoint-regression",
    "unresolved-citations",
    "tombstone-before-definition",
    "conflicting-memory-id",
    "prefix-mutation",
    "prefix-shrinkage",
    "source-mismatch",
    "producer-mismatch",
  ]);
  for (const fixtureCase of manifest.cases) {
    if (fixtureCase.expected.completeRecords === undefined) continue;
    const bytes = await readFile(path.join(fixtures, fixtureCase.file));
    assert.equal(bytes.reduce((count, byte) => count + (byte === 0x0a ? 1 : 0), 0),
      fixtureCase.expected.completeRecords, `${fixtureCase.id} complete-record count`);
  }
});

test("valid fixture contains all four protocol-v1 event kinds", async () => {
  const events = (await readCompleteLines("valid-events.jsonl")).map(JSON.parse);
  assert.deepEqual(events.map(({ kind }) => kind), [
    "observations.recorded",
    "reflections.recorded",
    "observations.dropped",
    "source.covered",
  ]);
  for (const event of events) {
    assert.equal(event.protocolVersion, 1);
    assert.equal(event.producer.id, "org.example.claude-memory");
    assert.deepEqual(event.source, { kind: "claude-code", sessionId: "session-123" });
    assert.ok(event.sourceCheckpoint.streams.length > 0);
  }
});

test("repeated event IDs are idempotent only for identical canonical payloads", async () => {
  const manifest = await readJson("manifest.json");
  const expectedHashes = (id) => manifest.cases.find((fixtureCase) => fixtureCase.id === id)
    .expected.canonicalPayloadSha256;

  const repeated = (await readCompleteLines("identical-replay.jsonl")).map(JSON.parse);
  assert.equal(repeated[0].eventId, repeated[1].eventId);
  assert.deepEqual(repeated.map(payloadHash), expectedHashes("identical-replay"));
  assert.equal(payloadHash(repeated[0]), payloadHash(repeated[1]));

  const conflicting = (await readCompleteLines("conflicting-event-id.jsonl")).map(JSON.parse);
  assert.equal(conflicting[0].eventId, conflicting[1].eventId);
  assert.deepEqual(conflicting.map(payloadHash), expectedHashes("conflicting-event-id"));
  assert.notEqual(payloadHash(conflicting[0]), payloadHash(conflicting[1]));
});

test("the JCS edge vector fixes UTF-16 ordering, escaping, Unicode, and number serialization", async () => {
  const manifest = await readJson("manifest.json");
  const expected = manifest.cases.find((fixtureCase) => fixtureCase.id === "canonical-jcs").expected;
  const [event] = (await readCompleteLines("canonical-jcs.jsonl")).map(JSON.parse);

  assert.equal(event.extension.text, "Café\t雪");
  assert.equal(event.extension.amount, 333333333.3333333);
  assert.deepEqual(event.sourceCheckpoint.streams.map(({ streamId }) => streamId),
    ["stream-😀", "stream-�"]);
  assert.equal(canonicalJson(event), expected.canonicalPayload);
  assert.deepEqual([payloadHash(event)], expected.canonicalPayloadSha256);
});

test("completed malformed JSON is rejected while an incomplete tail is deferred", async () => {
  const malformed = await readCompleteLines("malformed-complete-record.jsonl");
  assert.throws(() => JSON.parse(malformed[0]), SyntaxError);

  const incomplete = await readFile(path.join(fixtures, "incomplete-tail.jsonl"));
  assert.notEqual(incomplete.at(-1), 0x0a);
  const lastLf = incomplete.lastIndexOf(0x0a);
  const committed = incomplete.subarray(0, lastLf).toString("utf8");
  const deferred = incomplete.subarray(lastLf + 1).toString("utf8");
  assert.doesNotThrow(() => JSON.parse(committed));
  assert.throws(() => JSON.parse(deferred), SyntaxError);
});

test("checkpoint regressions skip events without conflicting the stream", async () => {
  const manifest = await readJson("manifest.json");
  const expected = manifest.cases.find((fixtureCase) => fixtureCase.id === "checkpoint-regression").expected;
  const events = (await readCompleteLines("checkpoint-regression.jsonl")).map(JSON.parse);
  let acceptedCheckpoint = null;
  let acceptedEvents = 0;
  let diagnosticCount = 0;

  for (const event of events) {
    const streams = event.sourceCheckpoint.streams;
    if (acceptedCheckpoint && checkpointRegresses(acceptedCheckpoint, streams)) {
      diagnosticCount += 1;
      continue;
    }
    acceptedCheckpoint = streams;
    acceptedEvents += 1;
  }

  assert.equal(acceptedEvents, expected.acceptedEvents);
  assert.equal(diagnosticCount, expected.diagnosticCount);
  assert.equal(acceptedCheckpoint, events.at(-1).sourceCheckpoint.streams);
});

test("unresolved entry and support citations remain structurally valid", async () => {
  const events = (await readCompleteLines("unresolved-citations.jsonl")).map(JSON.parse);
  assert.deepEqual(events[0].observations[0].sourceEntries,
    [{ streamId: "agent-a", entryId: "not-mirrored-yet" }]);
  assert.deepEqual(events[1].reflections[0].supportingObservationIds,
    ["999999999999"]);
});

test("tombstones apply before definitions and conflicting memory definitions keep the first", async () => {
  const tombstoneEvents = (await readCompleteLines("tombstone-before-definition.jsonl")).map(JSON.parse);
  const dropped = new Set(tombstoneEvents.flatMap((event) => event.observationIds ?? []));
  const observation = tombstoneEvents.flatMap((event) => event.observations ?? [])[0];
  assert.equal(dropped.has(observation.id), true);

  const duplicateEvents = (await readCompleteLines("conflicting-memory-id.jsonl")).map(JSON.parse);
  const definitions = duplicateEvents.flatMap((event) => event.observations ?? []);
  assert.equal(definitions[0].id, definitions[1].id);
  assert.notEqual(canonicalJson(definitions[0]), canonicalJson(definitions[1]));
  const first = new Map();
  for (const definition of definitions) {
    if (!first.has(definition.id)) first.set(definition.id, definition);
  }
  assert.equal(first.get(definitions[0].id).content, definitions[0].content);
});

test("committed-prefix mutation and shrinkage are distinguishable conflicts", async () => {
  const original = await readFile(path.join(fixtures, "prefix-original.jsonl"));
  const mutated = await readFile(path.join(fixtures, "prefix-mutated.jsonl"));
  const shrunk = await readFile(path.join(fixtures, "prefix-shrunk.jsonl"));
  const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");

  assert.equal(mutated.length, original.length);
  assert.notEqual(hash(mutated), hash(original));
  assert.ok(shrunk.length < original.length);
  assert.notEqual(hash(shrunk), hash(original));
});

test("event source and producer must match the sidecar locator", async () => {
  const expected = (await readJson("manifest.json")).locator;
  const [wrongSource] = (await readCompleteLines("source-mismatch.jsonl")).map(JSON.parse);
  assert.equal(wrongSource.source.kind, expected.sourceKind);
  assert.notEqual(wrongSource.source.sessionId, expected.sourceSessionId);
  assert.equal(wrongSource.producer.id, expected.producerId);

  const [wrongProducer] = (await readCompleteLines("producer-mismatch.jsonl")).map(JSON.parse);
  assert.equal(wrongProducer.source.kind, expected.sourceKind);
  assert.equal(wrongProducer.source.sessionId, expected.sourceSessionId);
  assert.notEqual(wrongProducer.producer.id, expected.producerId);
});

test("all structurally valid fixture records conform to the v1 event shape", async () => {
  const schema = await readJson("event.schema.json");
  assert.equal(schema.$schema, "https://json-schema.org/draft/2020-12/schema");
  assert.equal(schema.oneOf.length, 4);

  const files = [
    "valid-events.jsonl",
    "identical-replay.jsonl",
    "conflicting-event-id.jsonl",
    "canonical-jcs.jsonl",
    "checkpoint-regression.jsonl",
    "unresolved-citations.jsonl",
    "tombstone-before-definition.jsonl",
    "conflicting-memory-id.jsonl",
    "prefix-original.jsonl",
    "prefix-mutated.jsonl",
    "prefix-shrunk.jsonl",
    "source-mismatch.jsonl",
    "producer-mismatch.jsonl",
  ];
  for (const file of files) {
    for (const line of await readCompleteLines(file)) assertEventShape(JSON.parse(line));
  }

  const incomplete = await readFile(path.join(fixtures, "incomplete-tail.jsonl"));
  const committed = incomplete.subarray(0, incomplete.lastIndexOf(0x0a)).toString("utf8");
  assertEventShape(JSON.parse(committed));
});
