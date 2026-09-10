# UserIO index adapters — cache all chat text (and opt-in docs/media) into the mesh index

Status: in progress
Date: 2026-09-10
Owner session: zcode sess_4074be77 (gptadmin workspace, grepmesh project)

## Goal

Add UserIO (universal-userio) as an index adapter so all cached text messages
(gmail / telegram / whatsapp / sms / vk / matrix / chatgpt chats) become
searchable through GrepMesh. Opt-in per node; enabled in the owner's deploy
(server-100, where userio lives). Additionally, opt-in stages:

- documents in chats → extracted via anydoc (grepmesh's existing document
  extractor) and indexed;
- audio/video in chats → transcribed via whisper (grepmesh's existing STT
  stage) and indexed.

## Facts established (2026-09-10)

- userio canonical store: `/var/lib/universal-userio/userio.sqlite3`
  (SQLite, WAL, readable by `roomhacker`; grepmesh runs as `roomhacker`).
  Tables: `conversations`, `messages` (PK user_id,source,message_id),
  `message_attachments` (kind, content_type, filename, size, src, transcript,
  transcription_status, transcription_model), `contact_names`.
  Live volumes: ~6.7k messages / 412 conversations across gmail×2, telegram×2,
  whatsapp, sms, vk, chatgpt×2. Attachments today: 242 telegram_routing
  metadata JSON, 5 voice — all 5 already transcribed (whisper-1) with
  transcripts IN the DB. `src` is always empty (no local bytes).
- userio HTTP/MCP: `POST http://127.0.0.1:<USERIO_PORT, default 18093>/mcp`,
  bearer token (service token USERIO_API_TOKEN), tool
  `userio.channels.download` {file_ref} → {file:{filename, content_type,
  encoding:base64, data}}. file_ref per adapter: telegram `message_id:idx`
  (fallback bare message_id).
- grepmesh: filesystem-only index (FTS5, `IndexedDocument{path,body,...}`),
  read_text reads files directly — so the adapter materializes a CACHE ROOT:
  rendered conversation transcripts + (opt-in) attachment bytes as real files.
  Existing extraction pipeline already handles: media → STT (remote whisper
  `whisper.bezrabotnyi.com` in live config), images/PDF → OCR, documents →
  anydoc::to_markdown (src/index.rs:1385-1499), with extraction cache.
- grepmesh service: `/opt/grepmesh-custom/grepmesh-mcp --config
  /etc/grepmesh-mcp/config.json`, User=roomhacker, EnvironmentFile pattern
  already used (stt.env/ocr.env).

## Design

New `src/userio.rs` + `userio` config section (default disabled):

```json
"userio": {
  "enabled": true,
  "sqlite_path": "/var/lib/universal-userio/userio.sqlite3",
  "cache_dir": "/var/lib/grepmesh-mcp/userio-cache",
  "poll_interval_ms": 60000,
  "api_base": "http://127.0.0.1:18093",
  "token_env": "GREPMESH_USERIO_TOKEN",
  "attachments": { "docs": false, "media": false, "max_bytes": 268435456 }
}
```

- Poller (tokio task): read conversations+messages read-only from SQLite;
  render one `.txt` per conversation under `<cache_dir>/conversations/<source>/`;
  write-if-changed (atomic tmp+rename; no mtime bump when identical → no
  watcher churn); delete files for conversations that disappeared.
  Message line: `[YYYY-MM-DD HH:MM] sender: body` (+ attachment transcripts
  inlined when present in DB). HTML mail bodies stripped to text.
- Attachment stages (opt-in): materialize bytes into
  `<cache_dir>/attachments/<source>/` (from `src` path when present, else MCP
  download) → the existing watcher/extraction pipeline indexes them
  (anydoc for docs, whisper for audio/video, OCR for images).
  `telegram_routing` metadata attachments are skipped.
- cache_dir is listed in config.roots (deploy config) → full rebuilds,
  incremental watcher, read_text, previews all work unchanged.
- Deploy: server-100 userio.enabled=true (+ token env file for stages;
  DB transcripts need no token); server-88 stays disabled (same binary).

## Acceptance (planned)

1. cargo test --all-targets green (existing 108 + new unit/integration tests).
2. Live two-node: userio conversation text findable via grepmesh search
   (both nodes' catalog healthy, hit served from server-100 cache root;
   read_text returns rendered conversation).
3. Fresh inbound message (real) appears in search within poll+debounce.
4. Existing whisper transcripts (telegram voice) searchable via inline text.
5. Docs/media stages: integration-tested against a staging DB copy with real
   bytes through real anydoc/STT paths (live chats contain no such
   attachments today — gap stated honestly in the acceptance record).
6. Opt-in honesty: default config disabled; no userio access unless enabled.
7. Single-history commit + push + deploy receipts + acceptance record.

## Staging verification results (2026-09-10, before production deploy)

Ran the real release binary against a staging copy of the live userio DB
(isolated port 19419, separate index/cache):

- Initial sync: 412 conversations → 409 rendered (+3 empty), 409 files.
- Whisper path: real espeak-spoken audio staged as a voice attachment →
  materialized → transcribed by the real remote whisper
  (whisper.bezrabotnyi.com) → transcript indexed and found via /api/search
  ("Strap Mesh Userial Staging Whisper Proof 731", complete/partial=false).
- anydoc path: real RTF staged as a document attachment → materialized →
  indexed and found via /api/search ("forty two" hit, complete).
- Real corpus: real telegram voice transcripts inlined and searchable
  ("Что, теперь ты понимаешь голос?" 2 hits), chatgpt chat text searchable
  ("Keenable" 4 hits).
- Download API: request contract verified against the live userio /mcp
  (bearer + `userio.channels.download`, file_ref `<channel>:<message_id>`).
  The only fetchable attachment in the store (old telegram voice 1322) is
  honestly refused by the telegram bridge ("no connected Telegram account can
  resolve message 1322"); the adapter's bounded retry (3 attempts) then skips
  it — no hammering, honest degradation.
- Defects found and fixed during staging:
  1. file_ref channel-dispatch contract (`<channel>:<message_id>`, response
     envelope `result.structuredContent.file`) — fixed in the downloader.
  2. 400k render cap dropped the NEWEST messages of the 2554-message whatsapp
     thread — replaced by tail+archive split (`<conv>.txt` newest ~400k,
     `<conv>.archive.txt` older history up to 4MB, explicit omission markers).
     Verified: newest in main, oldest in archive, zero dropped.
  3. Write bursts tripped the watcher hot-directory guard (metadata-only
     bodies) — initial population now lands in bounded batches
     (max_writes_per_sync=40, deterministic order, prune deferred until a
     complete pass). Required in production: 7-day full-rebuild interval
     means new roots populate via watcher only.

