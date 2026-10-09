import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

// Opt-in: starts a real interactive Claude Code session with the installed
// tarball as --plugin-dir and waits for the namespaced plugin MCP server to
// connect. The headless `-p` path and `claude mcp list` do not exercise the
// interactive plugin startup that failed in 0.3.0. The session runs in this
// checkout because a fresh cwd would block on the workspace trust dialog.
const enabled = process.env.ADAM_TEST_CLAUDE === "1";

async function waitForConnection(log, deadline) {
  while (Date.now() < deadline) {
    const text = await readFile(log, "utf8").catch(() => "");
    const line = text.split("\n").find((entry) =>
      /MCP server "plugin:adam:adam": (Successfully connected|Connection failed)/.test(entry));
    if (line) return line;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("interactive Claude never reported the plugin MCP server");
}

test("interactive Claude Code starts the packaged plugin MCP server", { skip: !enabled && "set ADAM_TEST_CLAUDE=1" }, async () => {
  const temporaryDirectory = await mkdtemp(path.join(tmpdir(), "adam-claude-live-"));
  const log = path.join(temporaryDirectory, "debug.log");
  const [{ filename }] = JSON.parse(execFileSync(
    "npm", ["pack", "--json", "--pack-destination", temporaryDirectory], { encoding: "utf8" },
  ));
  const consumerDirectory = path.join(temporaryDirectory, "consumer");
  execFileSync("npm", ["init", "--yes"], { cwd: temporaryDirectory, stdio: "ignore" });
  await mkdir(consumerDirectory);
  await rename(path.join(temporaryDirectory, "package.json"), path.join(consumerDirectory, "package.json"));
  execFileSync(
    "npm",
    ["install", "--ignore-scripts", "--legacy-peer-deps", path.join(temporaryDirectory, filename)],
    { cwd: consumerDirectory, stdio: "ignore" },
  );
  const pluginRoot = path.join(consumerDirectory, "node_modules", "@mootboy", "adam");
  // Valid config only for lazy MCP initialization, with an unreachable endpoint.
  // Hooks inherit no backend config, so detached workers exit rather than retry.
  // Never inherit production credentials or start production reconciliation.
  const mcpPath = path.join(pluginRoot, ".mcp.json");
  const mcpConfig = JSON.parse(await readFile(mcpPath, "utf8"));
  mcpConfig.mcpServers.adam.env = {
    ADAM_NEO4J_URI: "bolt://127.0.0.1:1", ADAM_NEO4J_USERNAME: "neo4j",
    ADAM_NEO4J_PASSWORD: "unused-test-password",
  };
  await writeFile(mcpPath, JSON.stringify(mcpConfig));
  const command = [
    "claude", "--plugin-dir", pluginRoot, "--model", "haiku", "--debug", "--debug-file", log,
  ].map((part) => `'${part}'`).join(" ");
  const child = spawn("script", ["-qfec", command, "/dev/null"], {
    cwd: process.cwd(),
    env: isolatedAdamEnvironment(path.join(temporaryDirectory, "adam-state")),
    stdio: ["pipe", "ignore", "ignore"],
    detached: true,
  });
  try {
    const line = await waitForConnection(log, Date.now() + 90000);
    assert.match(line, /Successfully connected \(transport: stdio\)/);
  } finally {
    process.kill(-child.pid, "SIGKILL");
    await new Promise((resolve) => child.once("exit", resolve));
    await rm(temporaryDirectory, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
  }
});
