#!/usr/bin/env node
import { appendFileSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const outputDirectory = process.env.ADAM_PRODUCER_PROBE_DIR;
if (!outputDirectory) process.exit(0);

let raw = "";
try {
  raw = readFileSync(0, "utf8");
} catch {}
const input = raw ? JSON.parse(raw) : {};

function processInfo(pid) {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    const close = stat.lastIndexOf(")");
    const fields = stat.slice(close + 2).trim().split(/\s+/);
    return {
      pid,
      name: stat.slice(stat.indexOf("(") + 1, close),
      parentPid: Number(fields[1]),
    };
  } catch {
    return { pid, name: "unavailable", parentPid: 0 };
  }
}

const commandParent = processInfo(process.ppid);
const hostParent = processInfo(commandParent.parentPid);
appendFileSync(join(outputDirectory, "hooks.jsonl"), `${JSON.stringify({
  event: input.hook_event_name,
  sessionIdPresent: typeof input.session_id === "string" && input.session_id.length > 0,
  transcriptPathAbsolute: typeof input.transcript_path === "string" && isAbsolute(input.transcript_path),
  cwdAbsolute: typeof input.cwd === "string" && isAbsolute(input.cwd),
  keys: Object.keys(input).sort(),
  environmentPresent: {
    CLAUDECODE: Object.hasOwn(process.env, "CLAUDECODE"),
    CLAUDE_PLUGIN_ROOT: Object.hasOwn(process.env, "CLAUDE_PLUGIN_ROOT"),
    ANTHROPIC_API_KEY: Object.hasOwn(process.env, "ANTHROPIC_API_KEY"),
    CLAUDE_CODE_OAUTH_TOKEN: Object.hasOwn(process.env, "CLAUDE_CODE_OAUTH_TOKEN"),
  },
  commandParent,
  hostParent,
})}\n`, { mode: 0o600 });

if (input.hook_event_name === "SessionEnd") {
  try {
    const child = spawn(
      process.execPath,
      [join(dirname(fileURLToPath(import.meta.url)), "worker.mjs"), String(hostParent.pid)],
      { detached: true, stdio: "ignore", env: process.env },
    );
    child.unref();
  } catch {}
}
