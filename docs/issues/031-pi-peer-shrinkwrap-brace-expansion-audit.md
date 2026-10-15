# 031 — Upstream Pi peer shrinkwrap retains vulnerable brace-expansion

- Status: proposed
- Created: 2026-10-15
- Severity: high (npm advisory)

## Finding

Release-preparation `npm audit` reports one high-severity indirect vulnerability at `@earendil-works/pi-coding-agent@0.87.1 -> minimatch@10.2.6 -> brace-expansion@5.0.9`. The published Pi package includes an npm-shrinkwrap.json pinning 5.0.9. The registry offers patched 5.0.12, but both targeted lockfile-only update and `npm audit fix --package-lock-only --ignore-scripts` leave that shrinkwrapped dependency unchanged. No dependency changes were made by these attempts.

Advisories include denial of service through quadratic expansion and uncontrolled recursion: GHSA-q2hr-2g5m-vwhr, GHSA-qhr7-859c-m2p7 and GHSA-6j4f-fj2g-mc7p. This is Pi's installed peer-tooling dependency, not a newly introduced direct Adam dependency. Host-provided Pi maintains its own dependency tree; an Adam override would not establish that the actual host is patched.

## Follow-up

- Obtain an upstream Pi release/shrinkwrap containing the patched dependency, refresh Adam's test lock and rerun isolated CI/package validation.
- Verify the actual supported Pi host separately; do not claim its security state from Adam's lockfile.
- Do not silently patch vendored node_modules, force a peer major upgrade, or claim this is resolved by unchanged audit-fix output.

This known audit finding remains open for the 0.5.0 release review. No production access or model call was performed.
