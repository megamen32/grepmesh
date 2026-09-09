# Acceptance record — minimal working mesh (MVP stop-line)

Plan: `plan-20260910-minimal-working-mesh.md`
Date: 2026-09-10 (MSK)
Result: **PASS** — stop-line canary passed twice in a row, plus honest bounded
degradation and recovery, on the deployed two-node runtime.

## Deployed state (single history)

- Owner source: this repository, commit `1311be26dbc06a2276876a59ffac118e0986367b`
  (`main`), plus the MVP delivery commit that adds this record.
- Runtime binary: `/opt/grepmesh-custom/grepmesh-mcp`,
  sha256 `986859473aa470d1721f8a182b443afd044960a47b9a5dbfbff42729f8873c1b`
  — byte-identical to `cargo build --release` of the tree above (verified on
  both nodes; no `src/` changes in the delivery commit).
- Active topology (both nodes): peers reduced to exactly the healthy pair
  `server-100 ↔ server-88` (config receipts below). Loopback bind
  `127.0.0.1:9419` and peer bind kept separate.
- Deploy receipts (old binary+config backups + manifests):
  - server-100: `/opt/grepmesh-custom/.mvp-receipts/20260909T215405Z`
  - server-88: `/opt/grepmesh-custom/.mvp-receipts/20260909T215505Z`
  - rollback: `deploy/mvp-deploy.sh --rollback <receipt-dir> [--remote roomhacker-server-88]`

## Step 2 acceptance — two healthy peers only

After deploy, on both nodes (checked on each node's own loopback):

- `GET /api/catalog`: `partial=false`, exactly `server-100` and `server-88`,
  both `ok`, no errors from removed machines (`mac-m1`, `mac-mini`,
  `server-44`, `windows-190` absent from fanout).
- MCP `list_locations` on `["server-100","server-88"]`: both `ok`, `partial=false`.

## Step 3 acceptance — plugin loader and API contract

- `grepmesh-search@megamen32-private` upgraded 1.0.0 → 1.1.0 via native
  `codex plugin add`; `codex plugin list` shows `installed, enabled 1.1.0`
  (CLI replaced the 1.0.0 cache — no stale version loadable).
- Fresh Codex sessions (`codex exec`, MCP-only prompt) discovered the plugin
  skill and actually called `grepmesh/search` → `running` →
  `grepmesh/search_status` → terminal, plus `grepmesh/read_text` remote read.
- One live runtime serves all surfaces: `/ui` (307 → `/ui/`), `/api/catalog`,
  `/api/search`, `/api/search/status`, and MCP `tools/call` on the same
  `:9419` listener with consistent status fields
  (`state`, `partial`, `truncated`, `host_status`).

## Step 4 acceptance — real two-node canary (twice) + degradation

Marker: `GREPMESH-MVP-CANARY-20260910-7F3A9C` in
`/home/roomhacker/grepmesh-canary-mvp-20260910.txt` on both nodes
(node-distinguishable content). Both runs were fresh Codex sessions using
only the grepmesh MCP tools (`hosts=["server-100","server-88"]`,
`wait_ms=500`, `max_matches=4`, literal):

- Run 1 (`CANARY1 PASS`): `state=complete`, `partial=false`, `truncated=false`,
  both `host_status ok`, exactly 2 results (file on each host);
  `read_text` on each returned the exact marker and the correct node content.
- Run 2 (`CANARY2 PASS`): identical outcome, no manual repair between runs.
- Degradation: `systemctl stop grepmesh-mcp` on server-88 → search returned
  the preserved server-100 hit with `partial=true` and server-88
  `failed` (`direct transport unavailable; no GPTAdmin fallback is
  configured`) — bounded, explicit, not an empty success.
- Recovery: service restarted → `partial=false`, both hosts `ok`, 2 hits.
- Cleanup: marker files deleted on both nodes; after the indexer debounce
  window the same search returns 0 hits with `partial=false`.

Independent cross-check: `scripts/grepmesh_client.py` (stdlib MCP client with
the same contract, 17/17 stub unit tests in
`scripts/test_grepmesh_client.py`) reproduced search, polling and remote
`read_text` against the live mesh.

## Source tests

- `cargo test --all-targets`: 108 passed, 0 failed (baseline and final run).
- `python3 -m unittest scripts.test_grepmesh_client`: 17 passed, 0 failed.

## Explicitly out of this delivery (per plan)

Wildcard `hosts="*"` restoration, GPTAdmin topology discovery/relay fallback,
third+ nodes, portable GPTAdmin Node, mac-mini/mac-m1/server-44 repair,
public Console/SSO, backups/OCR/STT claims, Finder UI rework.

The old Next adapter `gptadmin/browser-os/app/api/grepmesh/route.ts` was
rewritten as a thin async adapter over the built-in Console API (explicit
MVP host set, never `*`, polls `/api/search/status`, honest
partial/truncated/host_status passthrough, `{data:…}` wrapper kept for its
single consumer) — committed separately in the gptadmin repository.
