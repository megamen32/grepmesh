# GrepMesh

![GrepMesh remote search](docs/screenshots/hero.png)

> **Stop making coding agents SSH into every machine just to find a file.**

GrepMesh gives Codex, OpenCode, Hermes, and any MCP client one local MCP
endpoint for search and exact-file reads across machines you configure.

**Recorded benchmark result:** 10.91 ms through GrepMesh versus 286.98 ms
through SSH + `rg` — **26.3× faster**, with 25/25 correct results on both
paths. The harness and required environment variables are in
[the benchmark script](bench/remote-search.js); treat these numbers as a
workload-specific snapshot, not a latency guarantee.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/megamen32/grepmesh/main/install.sh | bash
```

```powershell
irm https://raw.githubusercontent.com/megamen32/grepmesh/main/install.ps1 | iex
```

The release installer downloads ready-to-run `grepmesh-mcp` and `rg` binaries,
creates a local config, installs a user service (systemd, launchd, or a Windows
logon task), and starts GrepMesh at `http://127.0.0.1:9419/mcp`. No Rust,
Cargo, clone, or manual build is required when a matching release asset exists.
The checked-in release workflow targets Linux x86_64, macOS arm64/x86_64, and
Windows x86_64; confirm that a release asset exists for your platform.

For multi-host searches, GrepMesh waits up to 30 seconds by default. Fast
searches return immediately. A still-running search returns ready matches and
an opaque `job_id` (plus opaque `artifact_id` while partial); poll
`search_status` with the job ID and its opaque cursor for incremental batches.
It includes a 30-second poll hint. The configured result limit is stable in
arrival order: the first unique hits admitted are never replaced by a later
peer, so a batch cannot contradict a later final result.

Search jobs are stored as private service artifacts, not filesystem paths
exposed to MCP clients. They expire after the configured TTL; after a service
restart an in-flight job is explicitly reported as `lost`, and an unknown or
expired job tells the client to start the search again.

## Why GrepMesh

- **One search across machines.** Find text and paths, then read the exact file.
- **Built for agents.** MCP instructions explicitly route search through GrepMesh.
- **Small and direct.** About 6–8 MB RAM per node; peers search peers without a central index.

Add more machines by giving each node a routable peer URL and a named search
root. See [multi-machine setup](docs/MESH.md) and [agent connections](docs/CLIENTS.md).

MIT licensed.

## Evidence and limits

See [verification notes](./docs/VERIFICATION.md) for the checked-in installer,
test, release-smoke, and benchmark evidence. GrepMesh searches configured roots
and peers; it is not a central index, a universal filesystem search, or a
guarantee that every remote host is reachable.
