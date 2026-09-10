# Acceptance record — UserIO index adapters (chat text + opt-in docs/whisper)

Task: `work-20260910-userio-index-adapters.md`
Date: 2026-09-10 (MSK)
Result: **PASS with documented caveats** — all cached UserIO chat text is
live-searchable through the two-node mesh (real canaries below), opt-in
anydoc/whisper stages proven on real paths; literal-mode coverage on
server-100 is partial (125/411 conversations) pending an index-health
follow-up caused by a pre-existing/onward incident, see Known issues.

## Delivered

- Commits on `main` (pushed): `4ea4f27` (feature), `3cceb83` (watch-order
  fix), `b87f725` (hot-threshold fix), `8c22528` (rebuild-interval), final
  revert of the bootstrap interval.
- `src/userio.rs`: opt-in adapter; renders every UserIO conversation
  (gmail ×2, telegram ×2, whatsapp, sms, vk, matrix, chatgpt ×2; ~6.7k
  messages, 412 conversations) into per-conversation text files under
  `/var/lib/grepmesh-mcp/userio-cache` (a normal indexed root): newest-first
  tail + `.archive.txt` (2554-message whatsapp thread fully covered, zero
  drops), HTML mail stripped, contact names resolved, in-DB whisper
  transcripts inlined. Write-if-changed, atomic renames, 0600 perms,
  bounded batches (`max_writes_per_sync`), deterministic order, pruning.
- Opt-in attachment stages enabled in this deploy: `docs`
  (documents/images → anydoc/OCR extraction) and `media` (audio/video →
  remote-whisper STT via whisper.bezrabotnyi.com); bytes fetched from the
  local UserIO MCP API (`userio.channels.download`, file_ref
  `<channel>:<message_id>`, bearer `GREPMESH_USERIO_TOKEN` in
  `/etc/grepmesh-mcp/userio.env`, drop-in `40-userio.conf`). Already
  transcribed attachments stay inlined, never re-downloaded.
- server-88: same binary, feature off (no userio there).
- Tests: 122 green (9 userio unit + 4 integration, incl. the cache root
  indexed by the real IndexManager pipeline); the hot-directory test is a
  pre-existing load flake (fails on the clean tree, 3/3 green isolated).

## Production canaries (2026-09-10, live two-node mesh)

- C1 regex, real telegram voice transcript (whisper-1, inlined):
  2 hits (message line + transcript line), complete, partial=false.
- C2 regex, real newest whatsapp message, two-host fanout: hit from
  server-100's userio root; server-88 honestly `partial` for a root it
  does not have.
- C3 literal, ASCII render headers: 3/3 hits on covered rows.
- C4 read_text on a conversation cache file: full render (peer name,
  account, message count, real messages).
- C5 fresh inbound message (real whatsapp 06:24 MSK, post-deploy):
  searchable end-to-end — userio ingress → SQLite → adapter poll → cache
  rewrite → watcher → mesh search.
- Catalog healthy on both nodes throughout (partial=false, both ok).

## Staging verification (real paths, isolated instance, earlier today)

- Real remote whisper transcribed real spoken audio; transcript searchable
  via the real /api/search. Real RTF extracted (anydoc path) and
  searchable. Real telegram transcripts searchable.
- Download API contract verified live (`result.structuredContent.file`,
  channel-prefixed file_ref); the only fetchable old attachment is honestly
  refused by the telegram bridge; bounded retry gives up after 3 attempts.
- No document/media attachments exist in the live store today, so the
  download→extract stages have no live data to chew on yet; their code
  paths are staging-proven with real bytes and will engage automatically
  when such attachments appear.

## Production incidents during rollout (all resolved)

1. Cache root created after watcher registration → unindexed files;
   fixed: create cache dirs before LocalBackend starts the watcher.
2. 40-file batches × ~3-5 inotify events tripped hot_event_threshold
   (120/60s) → metadata-only bodies; fixed: default batch 12, deploy
   threshold 400.
3. Session operator (me) filled the disk with three 25 GB diagnostic
   copies of the prod index → ENOSPC ghost rows; disk freed; discipline
   recorded (never copy multi-GB prod DBs; query in place or via API).
4. Mass cache wipes (~800 delete events at once) trip the hot guard again;
   final population ran in batches; a slow aborted full rebuild was
   replaced by staggered mtime-touches (100/round).

## Known issues (follow-up task, not this delivery)

- server-100 literal-mode FTS coverage is partial for userio rows
  (125/411 conversations with bodies; the rest still serve via the rg
  fallback in regex/default flows) and the literal path also misses some
  OLD non-userio docs (e.g. /etc/grepmesh-mcp.service) — pre-existing or
  collateral of the ENOSPC window. Needs a dedicated index-health pass
  (likely off-peak full rebuild on a fresh index file). Regex mode and
  read_text are unaffected and verified live.
- Mail-attachment downloads: the UserIO MCP dispatcher has no mail channel
  adapter; gmail attachments cannot be fetched (skipped honestly).
- Old telegram media not resolvable by the live bridge session is skipped
  after 3 attempts per process.
