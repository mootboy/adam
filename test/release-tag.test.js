import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { tagRelease, validateReleaseMetadata } from "../scripts/release-tag.mjs";

function git(cwd, ...args) {
  return execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
}

function metadata(version = "1.2.3") {
  return {
    manifest: { version },
    lockfile: { version, packages: { "": { version } } },
    changelog: `# Changelog\n\n## [Unreleased]\n\n## [${version}] - 2026-09-26\n`,
  };
}

test("release metadata requires matching package, lockfile, and changelog versions", () => {
  assert.deepEqual(validateReleaseMetadata(metadata()), { version: "1.2.3", tag: "v1.2.3" });
  assert.throws(
    () => validateReleaseMetadata({ ...metadata(), lockfile: { version: "1.2.2", packages: { "": { version: "1.2.2" } } } }),
    /package-lock\.json version does not match/,
  );
  assert.throws(
    () => validateReleaseMetadata({ ...metadata(), changelog: "# Changelog\n\n## [Unreleased]\n" }),
    /does not contain a dated \[1\.2\.3\] release heading/,
  );
});

test("post-merge tagging pushes only the version tag and is idempotent", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-release-tag-"));
  const remote = path.join(root, "remote.git");
  const checkout = path.join(root, "checkout");

  try {
    git(root, "init", "--bare", "--initial-branch=main", remote);
    git(root, "init", "--initial-branch=main", checkout);
    git(checkout, "config", "user.name", "Adam Test");
    git(checkout, "config", "user.email", "adam@example.test");
    git(checkout, "remote", "add", "origin", remote);

    const values = metadata();
    await writeFile(path.join(checkout, "package.json"), `${JSON.stringify(values.manifest, null, 2)}\n`);
    await writeFile(path.join(checkout, "package-lock.json"), `${JSON.stringify(values.lockfile, null, 2)}\n`);
    await writeFile(path.join(checkout, "CHANGELOG.md"), values.changelog);
    git(checkout, "add", ".");
    git(checkout, "commit", "-m", "release fixture");
    git(checkout, "push", "-u", "origin", "main");
    const head = git(checkout, "rev-parse", "HEAD");

    const environment = { GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main", GITHUB_SHA: head };
    const first = tagRelease({ cwd: checkout, env: environment, log() {} });
    assert.equal(first.status, "created");
    assert.equal(git(remote, "rev-parse", "refs/heads/main"), head);
    assert.equal(git(remote, "rev-parse", "refs/tags/v1.2.3^{}"), head);

    const second = tagRelease({ cwd: checkout, env: environment, log() {} });
    assert.equal(second.status, "existing");
    assert.equal(git(remote, "rev-parse", "refs/heads/main"), head);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a superseded main run skips tagging", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "adam-release-stale-"));
  const remote = path.join(root, "remote.git");
  const checkout = path.join(root, "checkout");

  try {
    git(root, "init", "--bare", "--initial-branch=main", remote);
    git(root, "init", "--initial-branch=main", checkout);
    git(checkout, "config", "user.name", "Adam Test");
    git(checkout, "config", "user.email", "adam@example.test");
    git(checkout, "remote", "add", "origin", remote);

    const values = metadata();
    await writeFile(path.join(checkout, "package.json"), `${JSON.stringify(values.manifest, null, 2)}\n`);
    await writeFile(path.join(checkout, "package-lock.json"), `${JSON.stringify(values.lockfile, null, 2)}\n`);
    await writeFile(path.join(checkout, "CHANGELOG.md"), values.changelog);
    git(checkout, "add", ".");
    git(checkout, "commit", "-m", "first merge");
    git(checkout, "push", "-u", "origin", "main");
    const testedHead = git(checkout, "rev-parse", "HEAD");

    await writeFile(path.join(checkout, "later.txt"), "later merge\n");
    git(checkout, "add", "later.txt");
    git(checkout, "commit", "-m", "later merge");
    git(checkout, "push", "origin", "main");
    git(checkout, "reset", "--hard", testedHead);

    const result = tagRelease({
      cwd: checkout,
      env: { GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main", GITHUB_SHA: testedHead },
      log() {},
    });
    assert.equal(result.status, "stale");
    assert.equal(git(remote, "tag", "--list", "v1.2.3"), "");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
