import assert from "node:assert/strict";
import test from "node:test";
import adam from "../extension.js";

test("the compiled ClojureScript extension registers and serves adam:status", async () => {
  const commands = new Map();
  const pi = {
    registerCommand(name, definition) {
      commands.set(name, definition);
    },
  };

  await adam(pi);

  const status = commands.get("adam:status");
  assert.ok(status, "expected adam:status to be registered");
  assert.equal(status.description, "Show adam status");

  const notifications = [];
  await status.handler("", {
    ui: {
      notify(message, level) {
        notifications.push([message, level]);
      },
    },
  });

  assert.deepEqual(notifications, [["adam is running", "info"]]);
});
