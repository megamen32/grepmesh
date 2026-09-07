# GrepMesh

[Philosophy](docs/PHILOSOPHY.md) · [Docs](docs/CLIENTS.md) · [Multi-machine setup](docs/MESH.md) · [Verification](docs/VERIFICATION.md)

![GrepMesh remote search](docs/screenshots/hero.png)

GrepMesh gives an MCP client one local endpoint for searching configured files and reachable peers, then reading the exact matching file.

## What it does

- Uses one `search` tool for file names and indexed content across configured local roots and mesh peers (`search_text` remains a compatibility alias).
- Builds a local persistent FTS index on every node by default; `hosts: "*"` is the logical cross-machine index.
- Uses Firecrawl AnyDoc locally to index Word, PowerPoint, Excel, OpenDocument, RTF, EPUB, CSV, and text-based PDF files as Markdown.
- Uses native Rust OAR-OCR locally for images and scanned-PDF fallback. JPEG/JPG, PNG, WebP, TIFF/TIF and every image format supported by the Rust `image` decoder are eligible; the default bilingual pipeline uses a PP-OCRv6 tiny detector with a PP-OCRv5 Eastern-Slavic recognizer for Russian + English.
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

## Browse in the local console

Open `http://127.0.0.1:9419/ui/` in a browser on the machine running GrepMesh.
The console is available only on its loopback listener. Its Finder-style list
shows configured hosts and roots, with bookmarks stored in your browser.
The visible path segments navigate between folders; column headings sort by
name, size, modification date, or creation date. Unsupported creation dates
remain empty rather than being substituted with modification dates. Directory
sizes are not recursively calculated.

Host information includes indexing state, configured roots, the current scan
directory when available, and the combined SQLite database/WAL/SHM size. Older
peers may not provide all of these fields. Refreshing status does not rescan
roots or write telemetry.

Search history records the query, completion state, overall duration and each
host's duration and result count. It is stored privately in
`.grepmesh-jobs/telemetry.json` under the configured primary root, including
failed searches, and survives restarts. History is bounded to 500 completed
searches, 30 days, and 2 MiB; query text is capped at 512 characters. Only search
completion writes history. The loopback APIs are `GET /api/host-status` and
`GET /api/telemetry`; browsing continues to use `POST /api/browse`.

## Discover peers through GPTAdmin

Set `gptadmin_topology_url` to your Hub's `/mcp-relay/grepmesh` URL and
`gptadmin_token_env` to the environment variable containing its bearer token
(default `GPTADMIN_GREPMESH_TOKEN`). You may also configure `/mcp-relay/agents`
directly. The topology projection is augmented with that existing agent registry,
including when an older projection is empty; no second registration service is
required. A valid projection remains usable if the registry is unavailable.

Online GrepMesh child MCPs are probed through their existing `/server/.../mcp`
paths using a local-only request (`hosts: "local"`, `hop_count: 1`). The returned
host ID deduplicates aliases and excludes this node. Offline and failed children
are not promoted to healthy. Probes have a three-second per-child deadline and
a ten-second total discovery budget; partial discovery retains configured peers.

Known direct peer routes stay preferred. Discovered `gptadmin_relay_url` routes
provide a fallback on connection failure, or the primary route when no direct
address is known. Hub calls use `gptadmin_token_env`, independently of
`peer_auth_token_env`; cached topology contains URLs, never tokens. Relay paths
are resolved against the configured Hub origin, and Hub requests do not follow
redirects. Registry discovery reuses `topology_ttl_ms` and `topology_cache_path`.

## Defaults and optional security controls

See [project philosophy](docs/PHILOSOPHY.md).

GrepMesh is optimized for a comfortable local-first default. It does not silently hide files such as SSH configuration, key files, credential files, or other user data from search. If a path is inside a configured root and the GrepMesh process can read it, it is searchable.

The built-in exclusions are operational rather than security policy: dependency/build/cache trees and dynamic pseudo-filesystems such as `/proc`, `/sys`, `/dev`, and `/run` are skipped because indexing them is noisy, expensive, or unstable. Add any organization-specific exclusions explicitly with `exclude_globs`. Peer bearer authentication is also opt-in through `peer_auth_token_env`.

## Image OCR

Image OCR is enabled by default because the bilingual model set is small. GrepMesh uses OAR-OCR with automatic model download and caches models under OAR-OCR's normal cache (`~/.oar` unless `OAR_HOME` is set). The default pipeline is `pp-ocrv6_tiny_det.onnx` + `eslav_pp-ocrv5_mobile_rec.onnx` + `ppocrv5_eslav_dict.txt`; the Eastern-Slavic dictionary contains both Latin and Cyrillic characters.

For PDFs, AnyDoc remains the fast first path. If the extracted text layer contains fewer than 64 alphanumeric characters, GrepMesh renders pages with `pdftoppm` and runs OCR instead. The default cap is 200 pages at 180 DPI and can be changed under `ocr` settings.

Extracted document, OCR, and STT bodies are cached persistently by file size and modification time. Watcher reconciliations reuse unchanged content instead of rerunning AnyDoc, OCR, or remote transcription.

## Optional media transcription

Audio/video indexing is opt-in and supports two backends. `backend: "remote"` sends the media file to an OpenAI-compatible `/v1/audio/transcriptions` endpoint using a bearer token read from `api_key_env`; `backend: "auto"` or `"parakeet"` keeps transcription fully local with Parakeet-TDT-0.6B-v3 int8 via sherpa-onnx.

For local STT, GrepMesh downloads the model into `~/.cache/grepmesh/models/` on first media transcription when `auto_download` is enabled. For remote STT, the URL belongs in `remote_base_url` and the secret stays outside JSON/config in the environment variable named by `api_key_env` (default `GREPMESH_STT_API_KEY`). In either mode the resulting transcript is indexed into the local FTS.

## Documentation

- [Agent connections](docs/CLIENTS.md) — OpenCode, Codex, and Hermes configuration.
- [Multi-machine setup](docs/MESH.md) — roots, peers, routing, and optional tunnel fallback.
- [Verification notes](docs/VERIFICATION.md) — checked-in test and release evidence, plus limits.

GrepMesh searches only configured roots and reachable peers. It is licensed under [MIT](LICENSE).
