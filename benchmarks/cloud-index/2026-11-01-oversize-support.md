# Oversize historical bodies: chunk objects, lazy index reader, real publish (step 2)

Date: 2026-11-01. Scope: `scripts/cloud_index/oversize_support.py` (new),
`scripts/cloud_index/test_publish_native_oversize.py` (new),
`scripts/cloud_index/publish_native_oversize.py` (new),
`scripts/cloud_index/test_publish_native_oversize_cli.py` (new),
`scripts/cloud_index/reconcile_oversize_live.py` (new),
`scripts/cloud_index/multi_cloud.py`, `scripts/cloud_index/test_multi_cloud.py`,
`scripts/cloud_index/README.md`. Committed work only; the rolling-archive
systemd unit, native SQLite indexes and the default mesh are untouched.

## What landed

`oversize_support.py`:

- `publish_oversize()`: turns bodies > 1 MiB (the shard `MAX_DOCUMENT` bound)
  into newline-aligned chunk objects `oversize/<host>/<snapshot>/<body_sha>/chunk-NNNN.zlib`
  (zlib-3, content-addressed so a re-run publishes identical bytes) plus one
  immutable manifest `oversize/<host>/<snapshot>/manifest.json` and a pointer
  `oversize/<host>/<snapshot>/latest.json`. Chunk objects upload before the
  pointer, so a published pointer never references missing chunks.
- `plan_chunks()`: cut only between `str.splitlines(keepends=True)` lines; a
  single line longer than the target is split mid-piece at UTF-8 char
  boundaries; resulting pieces share one line number on read. Continuity invariants:
  chunk k+1.start_byte == k.start_byte+k.raw_bytes, chunk 1 starts_byte 0/line 1,
  chunk k+1.starts_mid_line == k.ends_mid_line, first chunk opens the body,
  last chunk ends on a line boundary.
- `OversizeIndex`: lazy pointer+manifest load with full validation (identity,
  sha chain, key identity — a tampered manifest cannot redirect a chunk —,
  byte continuity, mid-line chain, per-record `sum(raw)==size`, line_count
  >= last chunk line, unique paths). `read_text()` mirrors the 1 MiB shard
  reader: same path/line/deadline validation, same fail-closed filesystem
  ACL behavior for `authorized_roots`, same response shape
  (`path/source_host/body_sha256/start_line/end_line/chunks/snapshot_at`),
  same 'Historical text range exceeds 1 MiB' guard on responses, honest
  errors (`ValueError` bookkeeping, `RuntimeError` integrity incl. zlib
  bounds + per-chunk digest, `TimeoutError` deadline, `FileNotFoundError`
  missing path/pointer). Regions are byte-contiguous and begin on a real
  line boundary, so `splitlines` over the region has exact line parity with
  the shard reader. Requests beyond `line_count` return an empty selection
  without downloading a single chunk.

## Tests

`test_publish_native_oversize.py`: 41 checks, ~9 s, MemoryStore only:

- full read + 40 random ranges per body == `body.splitlines()` with line
  numbers, across five chunk targets (512 KiB, 64 KiB, 4 KiB, 1 KiB, 256 B)
  for a body mixing Unicode, empty lines, `\r\n/\r/\v/\f/\x1c/\x1d/\x1e/\x85/`
  `\u2028/\u2029` terminators, mid-piece 700 KB lines and unterminated tails;
- publish accounting, idempotent re-publish bytes, minimal oversized body
  (1 MiB+1) accepted, ≤1 MiB and >64 MiB rejected, digest mismatch rejected;
- ACL fail-closed; corrupt/redirected chunk, checksum mismatch, pointer
  kind/identity redirect, byte/line bookkeeping tampering all rejected
  before any data is trusted; missing pointer/chunk fail loudly;
- deadline honored; response guard >1 MiB honored; early return at
  `start_line > line_count` does zero chunk downloads (2 gets).

Full suite still green: test_cloud_index 70, test_multi_cloud 19,
test_scan_tree 21, test_refresh_git 26, test_legacy_bootstrap 10,
test_export_native_archive 17, test_export_native_rolling 9,
test_verify_native_rolling 11 — no regressions.

## Real publish (server-44, 2026-11-01)

Published from the already-verified archive
`yadisk:Backups/GrepMesh/cloud-index-v1/archives/server-44/20261031T214414Z-ae84d605/`
(the same source the legacy scopes were built from; the live SQLite was only
read read-only for reconciliation). No staging on disk, no MCP peer
registered, no native index touched, no SSD growth on the host.

Run: `publish_native_oversize.py --host server-44 --snapshot 20261031T214414Z-ae84d605 --expect 75`
(gateway env `GREPMESH_CLOUD_GATEWAY=local`, existing system rclone auth).

Result:

- **75 oversized documents**, **672 chunk objects**, raw 325 853 221 B,
  packed **82 043 741 B** (~78 MiB, 3.97x compression) under
  `oversize/server-44/20261031T214414Z-ae84d605/`;
- pointer `oversize/server-44/20261031T214414Z-ae84d605/latest.json`,
  manifest `.../manifest.json` with per-chunk sha256/packed sha256;
- publish scan: 849 785 archive rows walked, exactly 75 above the 1 MiB
  shard limit — the count matches the live-DB gate;
- admission: the published index was re-read through `OversizeIndex`
  (pointer → manifest → per-chunk sha chain) and a bounded canary verified
  three sampled paths (first, median, largest) down to the chunk payload.

Reconciliation against the live server-44 FTS DB (read-only, immutable):
75 live oversized rows, 75 published records, **75 paths in both, 0 only-live,
0 only-published**. One path has a different digest:
`/home/roomhacker/llm-keys-migration/standby-sync/stage/config-delta.json`
(live 2 588 562 B vs archived 2 582 055 B, delta 6 507 B). The live file
mtime is after the snapshot, so the archive legitimately holds the
historical revision — the published bytes are the historical truth, which
is the point of the historical index. All 74 other paths match byte for byte.

Honest note: an earlier `--min-bytes 325854701` gate (taken from the live
SQLite `LENGTH(CAST(c1 AS BLOB))` sum) was 1 480 B above the archived text
sum; the archived and live sums differ by exactly the one changed file, so
the gate was a live-vs-archive artifact, not a publish defect.

## Bounded search over oversized bodies

`OversizeIndex.first_match()` scans the verified chunk chain under one
shared deadline and returns the first matching line with the same line
semantics as the shard reader (a `\r\n` terminator byte-split across a
chunk boundary is rejoined correctly). Literal mode folds case the same
way; regex mode reuses `cloud_index.regex_match_positions`, so per-line
`^`/`$` grep semantics and the whole-body fallback for newline-capable
patterns are identical to the shard path.

`MultiReader.query` now searches the oversize indexes after the shard
scopes, under the same global deadline and the same hit limit, with the
same fail-closed `authorized_roots` filtering and `path_globs` matching.
Reaching the limit or the deadline yields honest `truncated`/`partial`
states with a readable warning instead of a silent "complete" claim.
`test_multi_cloud.py` grew to 43 checks (8 new: literal hit inside an
oversized body with its digest, honest miss, regex hit, regex anchors,
ACL exclusion, glob hit, plus the earlier read-path cases).

## Known limits (honest)

- Whole-region reads of one call are bounded by `MAX_OVERSIZE_DOCUMENT`
  (64 MiB) body cap; `line_count`/`size_bytes` must hold at publish time.
  A >48 MiB manifest is refused rather than truncated.
- A single logical line larger than 1 MiB is publishable and chunked
  correctly, but no reader can return it as text: the response guard
  (`Historical text range exceeds 1 MiB`) rejects it. Such a line is
  still verifiable at chunk level and searchable by line number/column.
- Search over oversized bodies walks documents sequentially; with 75
  documents and a 25 s default budget that is fine, but a much larger
  corpus would need an index by content, not a scan.
- Access is per host+snapshot identity only; no default-mesh routing has
  changed and nothing is switched over yet.

## Next gates

1. ~~MultiReader integration~~ — done.
2. ~~Real publish run of the 75 oversized server-44 rows~~ — done and
   reconciled against the live DB.
3. ~~Bounded search over oversized docs~~ — done.
4. Real filesystem ACL verification (verified filesystem ACL / fail-closed,
   no trusted auth from client args) — the remaining step 3 gate.
5. Live read canary through an explicit oversize scope config entry
   (no service/cwd/systemd change) — the config that a future MCP peer
   would carry.
