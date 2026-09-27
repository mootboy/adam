#!/usr/bin/env node

import { start } from "./dist/adam-worker.js";

try {
  await start();
} catch (error) {
  console.error(error?.message ?? String(error));
  process.exitCode = 1;
}
