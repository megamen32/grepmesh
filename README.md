# GrepMesh

![GrepMesh is 26x faster for remote search across machines](docs/screenshots/hero.png)

> **Stop making coding agents SSH into every machine just to find a file.**

GrepMesh gives Codex, OpenCode, Hermes, and any MCP client one local endpoint
for fast search and reads across all your machines.

**Measured on a live remote-file benchmark:** 10.91 ms through GrepMesh versus
286.98 ms through SSH + `rg` — **26.3× faster**, with 25/25 correct results on
both paths. [Reproduce the benchmark](bench/remote-search.js).

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/megamen32/grepmesh/main/install.sh | bash
```

```powershell
irm https://raw.githubusercontent.com/megamen32/grepmesh/main/install.ps1 | iex
```

That command downloads ready-to-run `grepmesh-mcp` and `rg` binaries, creates a
local config, installs a user service (systemd, launchd, or a Windows logon
task), and starts GrepMesh at
`http://127.0.0.1:9419/mcp`. No Rust, Cargo, clone, or manual build required.
Binary releases support Linux x86_64, macOS arm64/x86_64, and Windows x86_64.

For multi-host searches, pass a small `wait_ms` budget. Fast searches return
their result immediately. Slower searches return ready matches plus a `job_id`;
poll `search_status` with that ID to receive the completed result and opaque
cursor pages without flooding the model context or relying on a remote file
path.

## Why GrepMesh

- **One search across machines.** Find text and paths, then read the exact file.
- **Built for agents.** MCP instructions explicitly route search through GrepMesh.
- **Small and direct.** About 6–8 MB RAM per node; peers search peers without a central index.

Add more machines by giving each node a routable peer URL and a named search
root. See [multi-machine setup](docs/MESH.md) and [agent connections](docs/CLIENTS.md).

MIT licensed.
