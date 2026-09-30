# Memory protocol v1 fixtures

These files are executable examples for [`docs/memory-protocol-contract.md`](../../memory-protocol-contract.md). `manifest.json` supplies the canonical sidecar locator and expected classification for each case. `event.schema.json` is the contract's informative JSON Schema; normative byte, filesystem, replay, and cross-record rules remain in the contract.

All `.jsonl` fixtures are immutable test data:

- complete physical records end in LF;
- `incomplete-tail.jsonl` deliberately does not end in LF;
- malformed and conflicting records remain present so the later lossless scanner can prove raw preservation;
- prefix comparison files model mutation and shrinkage of an already checkpointed stream;
- mismatch fixtures are structurally readable events whose envelope identity disagrees with `manifest.json`'s sidecar locator.

The fixture text is synthetic and contains no transcript or memory content from a real user session.
