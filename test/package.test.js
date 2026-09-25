import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { access, mkdir, mkdtemp, readFile, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";

const neo4jEnvironment = [
  "ADAM_NEO4J_URI",
  "ADAM_NEO4J_USERNAME",
  "ADAM_NEO4J_PASSWORD",
  "ADAM_NEO4J_DATABASE",
];

function cleanEnvironment() {
  const environment = { ...process.env };
  for (const key of neo4jEnvironment) delete environment[key];
  return environment;
}

async function exists(file) {
  try {
    await access(file);
    return true;
  } catch {
    return false;
  }
}

test("the release tarball is complete and runs without source compilation", async () => {
  const temporaryDirectory = await mkdtemp(path.join(tmpdir(), "adam-package-"));

  try {
    let tarball = process.env.ADAM_PACKAGE_TARBALL;
    if (tarball) {
      tarball = path.resolve(tarball);
    } else {
      const packDirectory = path.join(temporaryDirectory, "pack");
      await mkdir(packDirectory);
      const output = execFileSync(
        "npm",
        ["pack", "--json", "--pack-destination", packDirectory],
        { cwd: process.cwd(), encoding: "utf8", env: cleanEnvironment() },
      );
      const [result] = JSON.parse(output);
      tarball = path.join(packDirectory, result.filename);
    }

    const consumerDirectory = path.join(temporaryDirectory, "consumer");
    execFileSync("npm", ["init", "--yes"], {
      cwd: temporaryDirectory,
      stdio: "ignore",
      env: cleanEnvironment(),
    });
    await mkdir(consumerDirectory);
    await rename(path.join(temporaryDirectory, "package.json"), path.join(consumerDirectory, "package.json"));

    execFileSync(
      "npm",
      ["install", "--ignore-scripts", "--legacy-peer-deps", tarball],
      { cwd: consumerDirectory, stdio: "ignore", env: cleanEnvironment() },
    );

    const packageRoot = path.join(consumerDirectory, "node_modules", "@mootboy", "adam");
    for (const relativePath of [
      "package.json",
      "extension.js",
      "dist/adam.js",
      "README.md",
      "CHANGELOG.md",
      "docs/architecture.md",
      "docs/session-replica-contract.md",
      "docs/acceptance-matrix.md",
    ]) {
      assert.equal(await exists(path.join(packageRoot, relativePath)), true, `${relativePath} must be packaged`);
    }
    assert.equal(await exists(path.join(packageRoot, "src")), false, "ClojureScript source is not a runtime dependency");

    const manifest = JSON.parse(await readFile(path.join(packageRoot, "package.json"), "utf8"));
    assert.equal(manifest.name, "@mootboy/adam");
    assert.equal(manifest.private, true);
    assert.equal(manifest.engines.node, ">=22.19.0");
    assert.equal(manifest.repository.url, "git+ssh://git@github.com/mootboy/adam.git");

    const savedEnvironment = new Map(neo4jEnvironment.map((key) => [key, process.env[key]]));
    try {
      for (const key of neo4jEnvironment) delete process.env[key];
      const { default: adam } = await import(pathToFileURL(path.join(packageRoot, "extension.js")));
      const commands = new Map();
      await adam({
        registerCommand(name, definition) {
          commands.set(name, definition);
        },
        on() {
          assert.fail("disabled packaged extension must not register lifecycle handlers");
        },
      });
      assert.ok(commands.has("adam:status"));
    } finally {
      for (const [key, value] of savedEnvironment) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
});
