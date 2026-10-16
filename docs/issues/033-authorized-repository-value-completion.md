# 033 — Repository values in derivation completion

- Status: done
- Created: 2026-10-15
- Follow-up: [032](032-derivation-command-completion.md)

## Outcome

After `--repository `, suggest origins from all self-owned/directly read-granted memory sessions plus the current checkout. Use the existing Pi completion menu, not another UI.

## Contract

- One principal-bound, lazy 30-second metadata cache shared by both commands; concurrent requests share pending work. Connected session startup warms it without awaiting it. No recurring timer/polling.
- Only origin strings are queried, using source-session WORKED_ON associations. No source content, native-anchor scan, generation, schema or graph writes. Self/direct read grants only; transitive grants and ambiguous session/memory ownership are excluded.
- Normalize origins to credential-free lookup values and deduplicate; put the current checkout first. Prefer HTTPS, preserving SCP form where HTTPS encoding would change the normalized path. Reject control/bidi/whitespace-unsafe menu values. Track current Pi cwd, with process cwd as initial fallback.
- Static flags/known values remain IO-free. A pending backend gets at most 250 ms grace before checkout fallback; later completion sees finished loading. Graph transactions have a two-second server timeout and three-second cancellation signal. Reload failures drop expired authorized hints; shutdown aborts loading and clears cache.
- Cached origin names can remain visible for up to the cache interval after revocation; they are hints, never authorization. Generation and saved reads perform their existing current authorization checks. No old metadata fallback after a failed expired reload.
- Recipe names, session/conclusion IDs and cursors are not discovered by this feature.

## Validation

- [x] Registered-command regression covers both commands, shared single-flight loading, credential stripping, TTL/failure behavior, cwd changes and close; static flags trigger zero state loading.
- [x] Compiled extension completes a disposable checkout's credential-free origin with Neo4j disabled.
- [x] Disposable Neo4j proves self/direct grant visibility, transitive/empty-session exclusion, revocation and ambiguous ownership denial.
- [x] Final clean CI and committed-runtime drift: 263 ClojureScript tests/1,693 assertions; 41 Node tests (35 pass/six expected opt-in skips). Disposable port-7688 Neo4j: 16 tests/251 assertions plus four packaged cases, including repository completion through the real lazy read pool. Hosted validation follows the PR update.

No version bump or release, production access or real model calls are included.
