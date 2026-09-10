# Acceptance record — UserIO index adapters (chat text + opt-in docs/whisper)

Task: `work-20260910-userio-index-adapters.md`
Date: 2026-09-10 (MSK)
Result: **PASS** — all 411 userio conversation files carry full bodies in
the server-100 trigram FTS index (MATCH coverage 411/411) and literal-mode
canaries pass with real messages (cyrillic telegram transcript, newest
whatsapp message, two-host fanout). The coverage was completed with a
blast-radius repair — two staggered mtime-touch passes over only the
uncovered files (20-file batches, ~25 minutes total) — instead of a full
index rebuild; a started full rebuild was aborted and the original index
(with its extraction cache) restored. Opt-in anydoc/whisper stages proven
on real paths. One pre-existing non-userio issue remains recorded below
(literal misses on some old non-userio docs).

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

## Index-health follow-up (executed 2026-09-10, blast radius)

The completion audit rejected partial FTS coverage (125/411). Remedy by
targeted repair, not full recompute: the started full rebuild (fresh file,
~7h for 68% — it re-extracts the whole corpus because the extraction cache
lives inside the index DB) was aborted, the original index restored
(warm start, no rebuild), and only the uncovered userio files were
mtime-touched in 20-file batches every 45s (two passes; the small batches
stay far below the hot_event_threshold). Coverage 124 → 342 → 411/411;
literal canaries then passed (L1 cyrillic transcript 2 hits, L2 whatsapp 1
hit, L3 two-host fanout with server-88 honestly partial for a root it does
not have).

## Known issues (follow-up task, not this delivery)

- server-100 literal-mode misses some OLD non-userio docs (e.g.
  /etc/grepmesh-mcp.service content) — pre-existing, unrelated to userio
  rows (userio literal coverage is now full). Needs its own index-health
  pass on the old corpus; regex mode and read_text unaffected.
- Mail-attachment downloads: the UserIO MCP dispatcher has no mail channel
  adapter; gmail attachments cannot be fetched (skipped honestly).
- Old telegram media not resolvable by the live bridge session is skipped
  after 3 attempts per process.
