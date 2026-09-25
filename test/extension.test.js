import assert from "node:assert/strict";
import test from "node:test";
import adam from "../extension.js";

test("the compiled ClojureScript extension registers and serves adam:status", async () => {
  const commands = new Map();
  const events = new Map();
  const pi = {
    registerCommand(name, definition) {
      commands.set(name, definition);
    },
    on(name, handler) {
      events.set(name, handler);
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

  assert.equal(notifications.length, 1);
  assert.equal(notifications[0][1], "info");
  assert.match(notifications[0][0], /adam session replica: (disabled|not connected)/);
  assert.equal(events.size, 0, "disabled configuration must not register lifecycle synchronization");
});
