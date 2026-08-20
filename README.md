# GrepMesh

[Docs](docs/CLIENTS.md) · [Multi-machine setup](docs/MESH.md) · [Verification](docs/VERIFICATION.md)

![GrepMesh remote search](docs/screenshots/hero.png)

GrepMesh gives an MCP client one local endpoint for searching configured files and reachable peers, then reading the exact matching file.

## What it does

- Searches text across configured local roots and mesh peers with `search_text`.
- Finds files by path with `find_paths`.
- Reads a selected file with `read_text`.
- Returns ready matches immediately and lets clients poll a longer search with `search_status`.
- Lets each node use narrow, named roots and explicit peer URLs.

## Install

**Linux or macOS** (Linux x86_64; macOS arm64 or x86_64):

```bash
curl -fsSL https://raw.githubusercontent.com/megamen32/grepmesh/main/install.sh | bash
```

**Windows x86_64** (PowerShell):

```powershell
irm https://raw.githubusercontent.com/megamen32/grepmesh/main/install.ps1 | iex
```

The installers download the matching GitHub release asset, install and start
`grepmesh-mcp`, and create a local configuration if one does not already exist.
They require a matching release asset; see the [verification notes](docs/VERIFICATION.md)
for the checked-in installer and release checks.

## Get started in under a minute

1. Run the installer for your platform. It starts the local MCP endpoint at `http://127.0.0.1:9419/mcp`.
2. Add that URL to your MCP client using the exact [OpenCode, Codex, or Hermes configuration](docs/CLIENTS.md).
3. Ask the client to search; GrepMesh searches the configured local root. To search more machines, install it on each node and follow [multi-machine setup](docs/MESH.md).

## Documentation

- [Agent connections](docs/CLIENTS.md) — OpenCode, Codex, and Hermes configuration.
- [Multi-machine setup](docs/MESH.md) — roots, peers, routing, and optional tunnel fallback.
- [Verification notes](docs/VERIFICATION.md) — checked-in test and release evidence, plus limits.

GrepMesh searches only configured roots and reachable peers. It is licensed under [MIT](LICENSE).
