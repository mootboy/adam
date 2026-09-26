import { execFileSync } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

function runGit(args, { cwd, allowFailure = false } = {}) {
  try {
    return execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", allowFailure ? "ignore" : "inherit"],
    }).trim();
  } catch (error) {
    if (allowFailure) return null;
    throw error;
  }
}

function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function validateReleaseMetadata({ manifest, lockfile, changelog }) {
  const version = manifest.version;
  if (typeof version !== "string" || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`invalid package version: ${String(version)}`);
  }
  if (lockfile.version !== version || lockfile.packages?.[""]?.version !== version) {
    throw new Error("package-lock.json version does not match package.json");
  }
  const heading = new RegExp(`^## \\[${escapeRegularExpression(version)}\\] - \\d{4}-\\d{2}-\\d{2}$`, "m");
  if (!heading.test(changelog)) {
    throw new Error(`CHANGELOG.md does not contain a dated [${version}] release heading`);
  }
  return { version, tag: `v${version}` };
}

function readMetadata(cwd) {
  return validateReleaseMetadata({
    manifest: JSON.parse(readFileSync(`${cwd}/package.json`, "utf8")),
    lockfile: JSON.parse(readFileSync(`${cwd}/package-lock.json`, "utf8")),
    changelog: readFileSync(`${cwd}/CHANGELOG.md`, "utf8"),
  });
}

function emitOutput(env, values) {
  if (!env.GITHUB_OUTPUT) return;
  appendFileSync(env.GITHUB_OUTPUT, Object.entries(values).map(([key, value]) => `${key}=${value}\n`).join(""));
}

export function tagRelease({ cwd = process.cwd(), env = process.env, log = console.log } = {}) {
  if (env.GITHUB_EVENT_NAME && env.GITHUB_EVENT_NAME !== "push") {
    throw new Error("release tagging is only allowed for a push event");
  }
  if (env.GITHUB_REF && env.GITHUB_REF !== "refs/heads/main") {
    throw new Error(`release tagging is only allowed from refs/heads/main, not ${env.GITHUB_REF}`);
  }
  if (runGit(["status", "--porcelain"], { cwd }) !== "") {
    throw new Error("working tree must be clean before release tagging");
  }

  const { version, tag } = readMetadata(cwd);
  const head = runGit(["rev-parse", "HEAD"], { cwd });
  if (env.GITHUB_SHA && env.GITHUB_SHA !== head) {
    throw new Error(`checked-out HEAD ${head} does not match GITHUB_SHA ${env.GITHUB_SHA}`);
  }

  const remoteMainLine = runGit(["ls-remote", "--heads", "origin", "refs/heads/main"], { cwd });
  const remoteMain = remoteMainLine.split(/\s+/)[0];
  if (!remoteMain) throw new Error("origin/main could not be resolved");
  if (remoteMain !== head) {
    log(`Skipping ${tag}: tested commit ${head} is no longer origin/main (${remoteMain}).`);
    emitOutput(env, { created: "false", tag });
    return { status: "stale", tag, head, remoteMain };
  }

  const remoteTag = runGit(["ls-remote", "--tags", "origin", `refs/tags/${tag}`], { cwd });
  if (remoteTag) {
    runGit(["fetch", "--force", "origin", `refs/tags/${tag}:refs/tags/${tag}`], { cwd });
    const taggedCommit = runGit(["rev-parse", `${tag}^{}`], { cwd });
    const isAncestor = runGit(["merge-base", "--is-ancestor", taggedCommit, head], { cwd, allowFailure: true }) !== null;
    if (!isAncestor) throw new Error(`${tag} exists but does not belong to the current main history`);
    log(`${tag} already exists at ${taggedCommit}; nothing to publish.`);
    emitOutput(env, { created: "false", tag });
    return { status: "existing", tag, head, taggedCommit };
  }

  const localTag = runGit(["rev-parse", "--verify", `${tag}^{}`], { cwd, allowFailure: true });
  if (localTag && localTag !== head) {
    throw new Error(`local ${tag} points to ${localTag}, not ${head}`);
  }
  if (!localTag) {
    runGit([
      "-c", "user.name=github-actions[bot]",
      "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
      "tag", "--annotate", tag, "--message", `adam ${tag}`, head,
    ], { cwd });
  }
  runGit(["push", "origin", `refs/tags/${tag}`], { cwd });
  log(`Published ${tag} at ${head}.`);
  emitOutput(env, { created: "true", tag });
  return { status: "created", tag, head };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  tagRelease();
}
