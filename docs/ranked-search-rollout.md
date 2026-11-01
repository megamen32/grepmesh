## Owner decision — 2026-11-01: supersedes standalone Ranked

One public search, rank=true by default; rank=false opts out. Ranking only
orders already-authorized matches and must not narrow literal/regex, OCR or
historical deleted-file coverage. The old ranked_search tool is removed, not
aliased. Do not install a second ranked index/service or reintroduce its MCP entry.
The independent source-manifest selector remains a corpus policy utility only.

Acceptance: default ranking before top-k; deterministic opt-out; identical match
sets within the reported candidate/deadline budget; compact/job output preserves
relevance order (compact fields remain unchanged); forged ACL denied; removed tool unavailable. Keep original indexers and
SQLite untouched during the ongoing cloud migration. A source commit is NOT a
claim that the native production runtime has been switched.

Current step: 14 focused Rust checks (including real MCP, compact output and
no-index fallback), 1 verified-metadata ACL regression and 9 bridge/source-policy
checks passed: 24 targeted checks. The compact-output regression was fixed without
adding score fields to compact responses; verbose responses carry scores. Native
production release remains pending; the test binary deliberately has no OCR/STT
features and MUST NOT be deployed as a replacement runtime. The owner explicitly retired the standalone variant; only its isolated
server-100-ranked listener may be disabled. Keep canonical port 9419 and its
original indexers running until the approved migration/release gate. No original SQLite/WAL/SHM removal is authorized by this change.

Additional regression note: the old broad mesh fixture suite is not accepted: four
cases failed and an unauthenticated fake-peer fixture stalled. Its isolated test
unit was stopped; no live service was touched. Do not count this as mesh/ACL
production acceptance. Focused new ACL and API contract checks are separate.

Live retirement receipt (2026-11-01): grepmesh-ranked.service is disabled and
inactive. Its dedicated port 19431 was retired; original port 9419 retained PID
3068 without restart. The standalone index.sqlite (496373760 bytes) is preserved.
This is intentional retirement, NOT an incident to auto-repair or re-enable.
Receipt: ~/.local/state/grepmesh-ranked-retired-20261101.json.
Native production rank=true API is NOT installed yet. Do not deploy the no-feature
test binary or stop original migration indexers to make the API look complete.
The legacy asynchronous/fanout fixture suite is still unaccepted; the isolated
async fixture expects a running job but receives an immediate response. No ACL
policy was loosened to make these older fixtures green.

## 2026-11-01 migration continuation / rollback gate
- **server-44:** independent private 8-scope FTS audit complete:
  849721 documents, 141 shards, 8 read_text, 8 denials, 8 regex.
  The immutable 75-document historical oversize archive has 672 verified chunks;
  durable cloud Reader + oversize integration passed a bounded real read,
  literal, regex and denied-path test. Private receipt:
  ~/.local/state/grepmesh-historical44-oversize-acceptance-20261101.json
  on server-44. safe_for_deletion=false.
- **server-100:** all 443 archived rolling chunks independently verified;
  source checkpoint explicitly remains consistent_snapshot=false,
  safe_for_deletion=false. The private searchable publishing unit
  grepmesh-native100-private-search-resume-20261101 is independently
  running; do not duplicate or restart it on stale progress.
- **Native full-feature release:** locked build from source commit 39db864
  was started in bounded user unit grepmesh-fullfeat-shadow-39db864 with
  Cargo default OCR+local-STT features. This is NOT a production deploy;
  original port 9419 is unchanged.
- **Still mandatory before SSD deletion:** authenticated GPTAdmin filesystem
  ACL acceptance (not synthetic owner-host scope checks), preservation of
  deleted-file historical text, full live update/OCR/regex parity, real
  read-after-reboot, exact rollback and approved production cutover.
  Do not stop source indexers or delete their SQLite/WAL/SHM until all pass.

---

# Ranked search rollout route

This note records the isolated candidate and release path for ranked search.
It does not authorize a production restart or deploy.

## Build the full-feature candidate

The required full-feature build is the native Linux build with Cargo defaults:

```bash
cd /home/roomhacker/agents-projects/grepmesh
cargo build --release --locked
```

`Cargo.toml` defaults include both `local-stt` and `ocr`. Do not use the
current Linux release script as the candidate build: `scripts/build-release.sh`
delegates to `scripts/build-linux-compatible.sh`, which builds with
`--no-default-features --features ocr-cuda` and therefore omits local STT.
The existing `target/` cache is 5.5 GiB (release 1.8 GiB); Cargo registry and
git caches are 735 MiB and 60 MiB. No build was started during route discovery.

At discovery time server-100 had 32 GiB `MemAvailable`, 138 GiB disk free, and
the live service charged 3.9/4 GiB at its configured `MemoryHigh`, with only
86 MiB of its service swap allowance available. Host I/O PSI `full avg10` was
17.11% and load average was about 43. Because the indexer was under real I/O
pressure and near its memory high, the route discovery deferred a local build.
Do not stop or restart the service to make room; measure actual current capacity
before another build.

The approved native full-feature build completed on 2026-10-28 with:

```text
cargo build --release --locked -j2
elapsed=81.40s, maxrss=1,015,468 KiB, exit=0
features=default (local-stt, ocr)
base checkout commit=3239766b097c58a5c628592866d96b9981163e9a (working-tree source hashes are recorded separately)
binary SHA-256=a5e22cb67f48ef3f95acbe7eebd0633875f997fe1aa08c423de23091956d1b87
```

The immutable artifact is
`/home/roomhacker/.local/share/grepmesh-ranked/releases/a5e22cb67f48ef3f95acbe7eebd0633875f997fe1aa08c423de23091956d1b87/grepmesh-mcp`.
Its `build.txt`, `build.time`, and `source-files.sha256` record the command,
resources, feature set, checkout identity, and source file hashes. This artifact
was used for the isolated candidate before the following relevance refinement.

The follow-up refinement preserves exact structured-token anchors and
contiguous identifier suffixes (for example, `mcp_servers` in
`agent_mcp_servers`). Its full-feature build also completed with Cargo defaults:

```text
cargo build --release --locked -j2
elapsed=94.17s, maxrss=994,592 KiB, exit=0
binary SHA-256=165195436014711d76e2217c5ea625554adf2b64551e50ca50ac72342072b339
```

The newer immutable artifact is
`/home/roomhacker/.local/share/grepmesh-ranked/releases/165195436014711d76e2217c5ea625554adf2b64551e50ca50ac72342072b339/grepmesh-mcp`.
The same three evidence files record its build and source hashes. It is ready
for the isolated candidate below. The previous a5e22 artifact remains intact
for reproducing its benchmark results.

## Isolated candidate process

The binary serves MCP over HTTP at `/mcp`; `src/main.rs` exposes no stdio
transport. Start the candidate with a disposable config that has one narrow
read root, a separate index and loopback listener, no peers, no GPTAdmin
topology, and no remote-index source/store. Keep the build features enabled;
the candidate fixture need not submit OCR or STT work.

Example config values (write them under a task-owned temporary directory):

```json
{
  "host_id": "grepmesh-ranked-candidate",
  "bind": "127.0.0.1:19419",
  "root": "/home/roomhacker/agents-projects/grepmesh/docs",
  "roots": {
    "docs": ["/home/roomhacker/agents-projects/grepmesh/docs"]
  },
  "peers": [],
  "index_path": "/tmp/grepmesh-ranked-candidate/index.sqlite3",
  "gptadmin_topology_url": null,
  "gptadmin_token_env": null,
  "peer_auth_token_env": null,
  "remote_index_source": null,
  "remote_index_store": null
}
```

Start only the candidate binary and config:

```bash
mkdir -p /tmp/grepmesh-ranked-candidate
target/release/grepmesh-mcp --config /tmp/grepmesh-ranked-candidate/config.json
```

Probe `http://127.0.0.1:19419/health`, then initialize MCP and exercise the
ranked-search tool against a marker found under `docs/`. The existing package
smoke script demonstrates the MCP initialize and tool-call envelope; it uses
HTTP JSON-RPC and a loopback-only listener.

The isolated loopback candidate serves port 19430 from PID 136300 using
`/tmp/grepmesh-ranked-candidate/config-current.json`, its own database at
`/tmp/grepmesh-ranked-candidate/candidate-fresh.sqlite3`, and the narrow
`/home/roomhacker/ServersAdministartion/docs` root. `/health` returned
`{"ok":true}` after the swap. `search_status` reports `Ready`, 299 indexed
documents, and generation 0. The generation value is a process-local rebuild
counter: on restart with a durable ready database it resets to 0, so it should
not be compared across process lifetimes. `/proc/136300/exe` resolves to the
immutable release path for SHA
`165195436014711d76e2217c5ea625554adf2b64551e50ca50ac72342072b339` and hashes
to the same value. The replaced candidate PID 3747331 used the preserved
a5e22 artifact. An earlier candidate PID 2972663 used SHA-256
`537664bdfec81bfe00357fdc0f690c6485f88c2a9310d70bc3a1ac771390a4bd`; both
were stopped after recording executable identity.

## Authentication and production boundary

Peer MCP bearer authentication is controlled by `peer_auth_token_env` and is
optional. When configured, the HTTP middleware requires the Bearer value for
`/` and `/mcp`; `/health` remains unauthenticated for liveness. The candidate
uses loopback and no peers, so it should not inherit production credentials.
The separate `gptadmin_token_env` authenticates GrepMesh calls to GPTAdmin; it
does not authenticate incoming MCP requests. Remote-index write routes use a
separate configured token. Do not copy any of these secrets into the candidate
config or this document.

## Installed artifact and rollback evidence

At discovery time the live executable was
`/opt/grepmesh-custom/grepmesh-mcp`, SHA-256
`32f9ccef3950ea3f194bc476efe4f8823e34a904f478b1687b2786f0d3cefe0d`.
The checkout was at `730dee7820264bbe6df6c203afe0b0bc15e4f9e0`. The latest
deployment receipt (`20261028T140638Z`) records binary and config hashes but
does not record the source commit, so the installed binary cannot be attributed
to that checkout from the receipt alone. Its previous binary/config copies are
preserved in the receipt directory.

The existing atomic installation seam is `deploy/mvp-deploy.sh`. It snapshots
the old binary and config into `/opt/grepmesh-custom/.mvp-receipts/<timestamp>`,
installs by same-filesystem atomic rename, restarts `grepmesh-mcp`, and checks
health. Its rollback command is:

```bash
deploy/mvp-deploy.sh --rollback /opt/grepmesh-custom/.mvp-receipts/<timestamp>
```

For any ranked-search production release, first agree on the build command and
artifact/source identity, then use the normal deployment owner and acceptance
path. This document is route evidence only; it does not represent a build,
installation, restart, or consumer acceptance.

## Installed local release, 2026-10-29

The additive user service `grepmesh-ranked.service` on loopback port 19431
now runs immutable full-default-feature binary SHA-256
`c74678c61ffd1b421b2200cde097358e3d0876abde71847349c539b8653f634a`.
Its release directory under `~/.local/share/grepmesh-ranked/releases/` contains
the binary, build log, source hash manifest, bridge, and build metadata.
The full locked release build took 10m12s after a host reboot; the two new
read-only annotation and bounded-token regression tests each executed and passed.

The configuration generator is pinned independently at
`~/.local/share/grepmesh-ranked/config-tools/a3a8e007974242ec95f40a5431f6dffdb9d103e8a89dc754830822443c3536ef/ranked-local-config.py`.
The directory name is its SHA-256. Install the checked-in generator at that
path with mode 0555 before installing `deploy/grepmesh-ranked.service`.
It mirrors ancestor-name exclusions and the native default system-directory
exclusions, so excluded paths are not advertised as admitted source files.
This changes configuration admission only; the immutable binary is preserved.

Fresh Codex Luna sessions performed authorized MCP reads under approval policy
`never` with sandbox `read-only`, including external HTTPX source and internal
Airlock source. OpenCode performed the external HTTPX search and source read;
the nested ranked call fulfilled with source-verified results. Compact receipts
are in `benchmarks/ranked-search/native-consumer.json`; full private answers
remain outside versioned output. `installed-acceptance.json` records all mapped
root canaries, actual coverage and partial warnings.

A controlled stop of only the additive service left the original GrepMesh
health endpoint and XERJ available; the additive service was then restored.
The system service on port 9419 was not restarted or replaced. These receipts
prove the local installation, not a fleet-wide production release.
