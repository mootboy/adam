#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import { start } from "./dist/adam-hook.js";

const maximumInputBytes = 64 * 1024;

try {
  const chunks = [];
  let totalBytes = 0;
  for await (const chunk of process.stdin) {
    totalBytes += chunk.length;
    if (totalBytes > maximumInputBytes) {
      throw new Error("Claude hook input exceeds 64 KiB");
    }
    chunks.push(chunk);
  }

  const workerPath = fileURLToPath(new URL("./worker.js", import.meta.url));
  start(Buffer.concat(chunks).toString("utf8"), workerPath);
} catch (error) {
  console.error(error?.message ?? String(error));
  process.exitCode = 1;
}
