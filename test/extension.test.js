import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import adam from "../extension.js";
import { isolatedAdamEnvironment } from "./support/adam-environment.js";

test("the compiled ClojureScript extension registers and serves adam:status", async (t) => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-status-config-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
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

  const neo4jEnvironment = [
    "ADAM_NEO4J_URI",
    "ADAM_NEO4J_USERNAME",
    "ADAM_NEO4J_PASSWORD",
    "ADAM_NEO4J_DATABASE",
    "XDG_CONFIG_HOME",
  ];
  const savedEnvironment = new Map(neo4jEnvironment.map((key) => [key, process.env[key]]));
  try {
    for (const key of neo4jEnvironment) delete process.env[key];
    process.env.XDG_CONFIG_HOME = root;
    await adam(pi);
  } finally {
    for (const [key, value] of savedEnvironment) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }

  // Exercise Pi's own editor completion contract, not just callback return values.
  // Resolve its host-provided TUI through the peer; Adam adds no TUI dependency.
  const peerRequire = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
  const { CombinedAutocompleteProvider } = await import(pathToFileURL(peerRequire.resolve("@earendil-works/pi-tui")).href);
  const provider = new CombinedAutocompleteProvider([...commands].map(([name, definition]) => ({ name, ...definition })), process.cwd());
  for (const command of ["adam:derive", "adam:derivations"]) {
    const line = `/${command} preferences --depth ref`;
    const suggestions = await provider.getSuggestions([line], 0, line.length, {});
    assert.equal(suggestions.items.length, 1);
    const applied = provider.applyCompletion([line], 0, line.length, suggestions.items[0], suggestions.prefix);
    assert.equal(applied.lines[0], `/${command} preferences --depth reflections`);
    assert.equal(applied.cursorCol, applied.lines[0].length);
  }

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

test("compiled lifecycle callbacks expose no Pi event result for ephemeral sessions", () => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-extension-results-"));
  const env = { ...isolatedAdamEnvironment(root),
    ADAM_NEO4J_URI: "bolt://127.0.0.1:1",
    ADAM_NEO4J_USERNAME: "test",
    ADAM_NEO4J_PASSWORD: "test-only-unreachable",
  };
  const program = `
    import assert from "node:assert/strict";
    import adam from ${JSON.stringify(new URL("../extension.js", import.meta.url).href)};
    const events = new Map();
    await adam({ registerCommand() {}, registerTool() {}, on(name, handler) { events.set(name, handler); } });
    const ctx = { cwd: ${JSON.stringify(root)}, sessionManager: { getSessionFile: () => null }, ui: { notify() {} } };
    const names = ["session_start", "turn_end", "agent_settled", "session_compact", "session_tree", "session_info_changed", "model_select", "thinking_level_select"];
    for (const name of names) {
      const work = events.get(name)({ type: name }, ctx);
      assert.equal(work, undefined, name + " must enqueue and return immediately without a boundary result");
    }
    await events.get("session_shutdown")({}, ctx);
    console.log(JSON.stringify(names));
  `;
  try {
    const stdout = execFileSync(process.execPath, ["--input-type=module", "-e", program],
      { env, cwd: root, encoding: "utf8", timeout: 15000 });
    assert.equal(JSON.parse(stdout).length, 8);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("compiled connection setup saves privately and enables the next host without environment credentials", () => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-connection-setup-"));
  const peerRequire = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
  const tuiUrl = pathToFileURL(peerRequire.resolve("@earendil-works/pi-tui")).href;
  const program = `
    import assert from "node:assert/strict";
    import fs, { readFileSync, statSync, writeFileSync } from "node:fs";
    import { syncBuiltinESMExports } from "node:module";
    import { spawnSync } from "node:child_process";
    import adam from ${JSON.stringify(new URL("../extension.js", import.meta.url).href)};
    import { getKeybindings } from ${JSON.stringify(tuiUrl)};
    import { parse } from ${JSON.stringify(import.meta.resolve("yaml"))};
    const commands = new Map(), events = new Map(), tools = new Map();
    const pi = { registerCommand(n,c) { commands.set(n,c); }, registerTool(t) { tools.set(t.name,t); }, on(n,h) { events.set(n,h); } };
    await adam(pi);
    assert.equal(events.size, 0);
    const notices = [];
    let action = "Cancel", password = "synthetic-only-secret";
    const ctx = { hasUI: true, mode: "tui", ui: {
      input: async () => "",
      select: async () => action,
      notify: (m) => notices.push(m),
      custom: (factory) => new Promise(resolve => {
        const component = factory({ requestRender() {} }, {}, getKeybindings(), resolve);
        for (const key of [password, "\\r"]) {
          component.handleInput(key);
          for (const width of [1, 20, 100]) {
            const lines = component.render(width);
            assert.ok(lines.every(line => line.length <= width));
            assert.ok(!lines.join("").includes(password));
          }
        }
        component.dispose?.();
      })
    }};
    const filename = process.env.XDG_CONFIG_HOME + "/adam/connection.yaml";
    await commands.get("adam:configure").handler("", ctx);
    assert.throws(() => readFileSync(filename), { code: "ENOENT" });
    action = "Save";
    await commands.get("adam:configure").handler("", ctx);
    const saved = parse(readFileSync(filename, "utf8"));
    assert.equal(saved.password, "synthetic-only-secret");
    assert.equal(statSync(filename).mode & 0o777, 0o600);
    assert.equal(statSync(process.env.XDG_CONFIG_HOME + "/adam").mode & 0o777, 0o700);
    assert.ok(!notices.join("").includes(saved.password));
    assert.ok(notices.some(m => m.includes("Restart")));
    // Exercise actual compiled save/error propagation, not an injected save stub.
    const originalSync = fs.fsyncSync;
    for (const phase of ["file", "directory"]) {
      const before = readFileSync(filename, "utf8");
      password = "synthetic-sync-" + phase;
      fs.fsyncSync = fd => {
        if ((phase === "directory") === fs.fstatSync(fd).isDirectory()) throw new Error(password);
        originalSync(fd);
      };
      syncBuiltinESMExports();
      try { await commands.get("adam:configure").handler("", ctx); }
      finally { fs.fsyncSync = originalSync; syncBuiltinESMExports(); }
      assert.ok(!notices.at(-1).includes(password), "raw fsync errors must remain private");
      if (phase === "file") {
        assert.equal(readFileSync(filename, "utf8"), before);
        assert.match(notices.at(-1), /no settings saved/);
      } else {
        assert.equal(parse(readFileSync(filename, "utf8")).password, password);
        assert.match(notices.at(-1), /replaced.*durability/);
        assert.ok(!notices.at(-1).includes("no settings saved"));
      }
    }
    password = saved.password;
    await commands.get("adam:configure").handler("", ctx);
    assert.match(notices.at(-1), /saved privately/);
    const previous = readFileSync(filename, "utf8");
    action = "Cancel";
    await commands.get("adam:configure").handler("", ctx);
    assert.equal(readFileSync(filename, "utf8"), previous, "cancel must preserve the previous file byte-for-byte");
    assert.equal(events.size, 0, "save must not activate the old runtime");
    writeFileSync(process.env.XDG_CONFIG_HOME + "/adam/derivations.yaml", "invalid: [yaml");
    assert.throws(() => readFileSync(process.env.XDG_CONFIG_HOME + "/adam/config.json"), { code: "ENOENT" });
    await adam(pi);
    await commands.get("adam:status").handler("", ctx);
    assert.ok(events.has("session_start"), "next initialization must load the file: " + notices.at(-1));
    assert.ok(tools.has("adam_file_context"));
    // Worker uses the same saved settings. Invalid policy stops it before DB IO.
    const worker = spawnSync(process.execPath, [${JSON.stringify(new URL("../worker.js", import.meta.url).pathname)}], {
      env: { ...process.env, ADAM_MEMORY_SOURCE_WAIT_MS: "0" }, encoding: "utf8", timeout: 10000
    });
    assert.notEqual(worker.status, 0);
    assert.match(worker.stderr, /ADAM_MEMORY_SOURCE_WAIT_MS/);
    assert.ok(!worker.stderr.includes("adam worker unavailable"));
    assert.ok(!worker.stderr.includes(saved.password));
    process.env.ADAM_NEO4J_URI = "";
    events.clear(); tools.clear();
    await adam(pi);
    assert.equal(events.size, 0, "an explicit empty environment must suppress saved credentials");
    assert.equal(tools.size, 0);
  `;
  try {
    execFileSync(process.execPath, ["--input-type=module", "-e", program],
      { env: isolatedAdamEnvironment(root), cwd: root, encoding: "utf8", timeout: 20000 });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("compiled connection-test cancellation closes a stalled Bolt socket without awaiting RESET", () => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-stalled-probe-"));
  const peerRequire = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
  const tuiUrl = pathToFileURL(peerRequire.resolve("@earendil-works/pi-tui")).href;
  const program = `
    import assert from "node:assert/strict";
    import net from "node:net";
    import { existsSync } from "node:fs";
    import adam from ${JSON.stringify(new URL("../extension.js", import.meta.url).href)};
    import { getKeybindings } from ${JSON.stringify(tuiUrl)};
    const sockets = new Set(), commands = new Map(), notices = [];
    let component, sawRun = false, sawReset = false, resolveClosed;
    const closed = new Promise(resolve => { resolveClosed = resolve; });
    // Minimal synthetic Bolt 5.1 peer: authenticate, then never reply to RUN
    // or RESET. There is no receive-timeout hint and no actual Neo4j database.
    const server = net.createServer(socket => {
      sockets.add(socket);
      socket.on("error", () => {});
      socket.on("close", () => { sockets.delete(socket); resolveClosed(); });
      let incoming = Buffer.alloc(0), handshake = false, chunks = [];
      const success = hello => {
        const agent = Buffer.from("Neo4j/5.1.0");
        const payload = hello
          ? Buffer.concat([Buffer.from([0xb1, 0x70, 0xa1, 0x86]), Buffer.from("server"), Buffer.from([0x80 + agent.length]), agent])
          : Buffer.from([0xb1, 0x70, 0xa0]);
        const size = Buffer.alloc(2); size.writeUInt16BE(payload.length);
        socket.write(Buffer.concat([size, payload, Buffer.alloc(2)]));
      };
      socket.on("data", data => {
        incoming = Buffer.concat([incoming, data]);
        if (!handshake) {
          if (incoming.length < 20) return;
          incoming = incoming.subarray(20); handshake = true;
          socket.write(Buffer.from([0, 0, 1, 5]));
        }
        while (incoming.length >= 2) {
          const size = incoming.readUInt16BE(0);
          if (incoming.length < size + 2) return;
          const chunk = incoming.subarray(2, size + 2); incoming = incoming.subarray(size + 2);
          if (size) { chunks.push(chunk); continue; }
          const message = Buffer.concat(chunks); chunks = [];
          if (message[1] === 1) success(true);
          else if (message[1] === 0x6a) success(false);
          else if (message[1] === 0x10) { sawRun = true; setImmediate(() => component.handleInput("\\u001b")); }
          else if (message[1] === 0x0f) sawReset = true;
        }
      });
    });
    let timer;
    try {
      await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
      await adam({ registerCommand(n, c) { commands.set(n, c); }, registerTool() {}, on() {} });
      const ctx = { hasUI: true, mode: "tui", ui: {
        input: async title => title.startsWith("Neo4j URI") ? "bolt://127.0.0.1:" + server.address().port : "",
        select: async () => "Test connection", notify: m => notices.push(m),
        custom: factory => new Promise(resolve => {
          component = factory({ requestRender() {} }, {}, getKeybindings(), value => { component.dispose?.(); resolve(value); });
          if (component.render(80).join("").includes("Adam password")) {
            component.handleInput("synthetic-only-secret"); component.handleInput("\\r");
          }
        })
      }};
      await Promise.race([
        (async () => { await commands.get("adam:configure").handler("", ctx); await closed; })(),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("probe cancellation or socket shutdown stalled")), 3000); })
      ]);
      assert.ok(sawRun, "cancellation must occur after the query is pending");
      assert.ok(sawReset, "peer must ignore RESET, not merely stall the handshake");
      assert.equal(sockets.size, 0, "driver shutdown must close the active socket");
      assert.ok(notices.some(m => m.includes("cancelled")));
      assert.ok(!notices.join("").includes("synthetic-only-secret"));
      assert.equal(existsSync(process.env.XDG_CONFIG_HOME + "/adam/connection.yaml"), false);
    } finally {
      clearTimeout(timer);
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(resolve));
    }
  `;
  try {
    execFileSync(process.execPath, ["--input-type=module", "-e", program],
      { env: isolatedAdamEnvironment(root), cwd: root, encoding: "utf8", timeout: 10000 });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("compiled derivation commands complete configured names and the checkout without Neo4j", () => {
  const root = mkdtempSync(path.join(tmpdir(), "adam-repository-completion-"));
  try {
    const env = isolatedAdamEnvironment(root);
    const configDirectory = path.join(env.XDG_CONFIG_HOME, "adam");
    mkdirSync(configDirectory, { recursive: true });
    writeFileSync(path.join(configDirectory, "derivations.yaml"),
      "derivations:\n  architectural-preferences:\n    version: 1\n    guidance: Preserve scope.\n", { mode: 0o600 });
    execFileSync("git", ["init", "-q", root]);
    execFileSync("git", ["-C", root, "remote", "add", "origin", "https://alice:FAKE_SECRET@example.com/checkout.git"]);
    const program = `
      import assert from "node:assert/strict";
      import adam from ${JSON.stringify(new URL("../extension.js", import.meta.url).href)};
      const commands = new Map();
      await adam({ registerCommand(name, definition) { commands.set(name, definition); }, on() {} });
      for (const name of ["adam:derive", "adam:derivations"]) {
        const recipes = await commands.get(name).getArgumentCompletions("arch");
        assert.deepEqual(recipes.map(item => item.value), ["architectural-preferences"]);
        const items = await commands.get(name).getArgumentCompletions("preferences --repository ");
        assert.deepEqual(items.map(item => item.value), ["preferences --repository https://example.com/checkout"]);
      }
    `;
    execFileSync(process.execPath, ["--input-type=module", "-e", program],
      { env, cwd: root, encoding: "utf8", timeout: 15000 });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
