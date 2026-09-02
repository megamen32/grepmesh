# Three-state GrepMesh host status

Status: reviewed; rollout pending

Started at 2026-09-02T15:06:54+03:00 (host `date --iso-8601=seconds`).
Estimate: 10 / 25 active minutes.

Wanted result: each host reports `ok`, `partial`, or `failed`, and one broken
root does not stop searches in other selected roots on that host.

Shortest real canary: one host with a broken root and a readable root returns a
readable match with `state: partial`; all-broken roots return `state: failed`.

Smallest YAGNI slice: isolate root errors in the local backend, add a
backward-compatible host-state field, and preserve existing `ok/error` fields.

Discarded now: per-root result objects, retry scheduling, automatic permission
changes, and a new fleet monitoring control plane.

Evidence:
- `cargo test`: 81 passed, 0 failed.
- `bash scripts/test-install-contracts.sh`: passed.
- Terra reviewer: APPROVED after two repair rounds covering async partial jobs,
  legacy peer normalization across every remote response type, and mixed-root
  text/path continuation.
