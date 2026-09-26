#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { start } from "./dist/adam-mcp.js";

const manifest = JSON.parse(await readFile(new URL("./package.json", import.meta.url), "utf8"));

try {
  const runtime = await start(manifest.version);
  const close = runtime.close;
  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    try {
      await close();
    } finally {
      process.exit(0);
    }
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
} catch (error) {
  console.error(error?.message ?? String(error));
  process.exitCode = 1;
}
