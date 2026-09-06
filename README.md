# GrepMesh

[Philosophy](docs/PHILOSOPHY.md) · [Docs](docs/CLIENTS.md) · [Multi-machine setup](docs/MESH.md) · [Verification](docs/VERIFICATION.md)

![GrepMesh remote search](docs/screenshots/hero.png)

GrepMesh gives an MCP client one local endpoint for searching configured files and reachable peers, then reading the exact matching file.

## What it does

- Uses one `search` tool for file names and indexed content across configured local roots and mesh peers (`search_text` remains a compatibility alias).
- Builds a local persistent FTS index on every node by default; `hosts: "*"` is the logical cross-machine index.
- Uses Firecrawl AnyDoc locally to index Word, PowerPoint, Excel, OpenDocument, RTF, EPUB, CSV, and text-based PDF files as Markdown.
- Finds files by path with `find_paths`.
- Reads a selected file with `read_text`.
- Returns ready matches immediately and lets clients poll a longer search with `search_status`.
- Uses practical local roots and lightweight defaults; additional exclusions and transport controls are opt-in.

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
   Loopback is the default; peer bearer authentication is opt-in via `peer_auth_token_env`. GPTAdmin tunnels do not require a second GrepMesh auth layer.
2. Add that URL to your MCP client using the exact [OpenCode, Codex, or Hermes configuration](docs/CLIENTS.md).
3. Ask the client to search; GrepMesh indexes the configured roots locally and searches them immediately. To search more machines, install it on each node; `hosts: "*"` fans the same query across every reachable node without centralizing the documents.

## Defaults and optional security controls

See [project philosophy](docs/PHILOSOPHY.md).

GrepMesh is optimized for a comfortable local-first default. It does not silently hide files such as SSH configuration, key files, credential files, or other user data from search. If a path is inside a configured root and the GrepMesh process can read it, it is searchable.

The built-in exclusions are operational rather than security policy: dependency/build/cache trees and dynamic pseudo-filesystems such as `/proc`, `/sys`, `/dev`, and `/run` are skipped because indexing them is noisy, expensive, or unstable. Add any organization-specific exclusions explicitly with `exclude_globs`. Peer bearer authentication is also opt-in through `peer_auth_token_env`.

## Optional media transcription

Audio/video indexing is an opt-in local backend. Set `stt.enabled` to `true`; with the default `backend: "auto"` and `model: "auto"`, GrepMesh selects Parakeet-TDT-0.6B-v3 int8 and downloads the sherpa-onnx model package into `~/.cache/grepmesh/models/` on first media transcription. Nothing is downloaded while STT is disabled.

The local flow is: `audio/video -> ffmpeg -> Parakeet/sherpa-onnx -> timestamped transcript -> local FTS -> search`. The model supports English, Russian, and 23 other European languages. Original media and transcripts remain on the node that owns the file. `auto_download: false` can be used when models are provisioned manually.

## Documentation

- [Agent connections](docs/CLIENTS.md) — OpenCode, Codex, and Hermes configuration.
- [Multi-machine setup](docs/MESH.md) — roots, peers, routing, and optional tunnel fallback.
- [Verification notes](docs/VERIFICATION.md) — checked-in test and release evidence, plus limits.

GrepMesh searches only configured roots and reachable peers. It is licensed under [MIT](LICENSE).
