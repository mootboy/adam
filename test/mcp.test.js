import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import readline from "node:readline";
import test from "node:test";

const neo4jEnvironment = {
  ADAM_NEO4J_URI: "bolt://localhost:7687",
  ADAM_NEO4J_USERNAME: "neo4j",
  ADAM_NEO4J_PASSWORD: "test-password",
  ADAM_NEO4J_DATABASE: "neo4j",
};

function request(child, message) {
  child.stdin.write(`${JSON.stringify(message)}\n`);
}

function nextResponse(responses, id) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`timed out waiting for MCP response ${id}`)), 5000);
    const inspect = (response) => {
      if (response.id !== id) return;
      clearTimeout(timeout);
      responses.off("response", inspect);
      resolve(response);
    };
    responses.on("response", inspect);
  });
}

test("the compiled MCP server advertises the read-only file-context tool", async () => {
  const child = spawn(process.execPath, ["mcp.js"], {
    cwd: process.cwd(),
    env: { ...process.env, ...neo4jEnvironment },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const { EventEmitter } = await import("node:events");
  const responses = new EventEmitter();
  const errors = [];
  readline.createInterface({ input: child.stdout }).on("line", (line) => {
    responses.emit("response", JSON.parse(line));
  });
  readline.createInterface({ input: child.stderr }).on("line", (line) => errors.push(line));

  try {
    const initialized = nextResponse(responses, 1);
    request(child, {
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "adam-test", version: "1" },
      },
    });
    const initializeResponse = await initialized;
    assert.equal(initializeResponse.result.serverInfo.name, "adam");

    request(child, { jsonrpc: "2.0", method: "notifications/initialized" });
    const listed = nextResponse(responses, 2);
    request(child, { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
    const listResponse = await listed;
    const tool = listResponse.result.tools.find(({ name }) => name === "adam_file_context");
    assert.ok(tool);
    assert.deepEqual(tool.inputSchema.required, ["path"]);
    assert.equal(tool.inputSchema.properties.origin.type, "string");
    assert.equal(tool.annotations.readOnlyHint, true);
    assert.equal(errors.length, 0);
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
});

test("the MCP process drains buffered requests before exiting on closed standard input", async () => {
  const child = spawn(process.execPath, ["mcp.js"], {
    cwd: process.cwd(),
    env: { ...process.env, ...neo4jEnvironment },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const responses = [];
  const errors = [];
  readline.createInterface({ input: child.stdout }).on("line", (line) => {
    responses.push(JSON.parse(line));
  });
  readline.createInterface({ input: child.stderr }).on("line", (line) => errors.push(line));
  const exited = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("MCP process did not exit after stdin closed"));
    }, 5000);
    child.once("exit", (code, signal) => {
      clearTimeout(timeout);
      resolve({ code, signal });
    });
  });

  request(child, {
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "adam-test", version: "1" },
    },
  });
  request(child, { jsonrpc: "2.0", method: "notifications/initialized" });
  request(child, {
    jsonrpc: "2.0",
    id: 2,
    method: "tools/call",
    params: {
      name: "adam_file_context",
      arguments: { origin: "not-a-git-origin", path: "README.md" },
    },
  });
  child.stdin.end();

  assert.deepEqual(await exited, { code: 0, signal: null });
  assert.equal(responses.find(({ id }) => id === 1).result.serverInfo.name, "adam");
  const queryResponse = responses.find(({ id }) => id === 2);
  assert.equal(queryResponse.result.isError, true);
  assert.match(queryResponse.result.content[0].text, /origin must be a valid Git remote/);
  assert.deepEqual(errors, []);
});
