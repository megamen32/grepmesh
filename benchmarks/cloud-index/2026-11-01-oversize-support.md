# Oversize historical bodies: chunk objects + lazy index reader (step 2)

Date: 2026-11-01. Scope: `scripts/cloud_index/oversize_support.py` (new) and
`scripts/cloud_index/test_publish_native_oversize.py` (new). Committed work
only; the rolling-archive systemd unit, native SQLite indexes and the default
mesh are untouched.

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

## Known limits (honest)

- Whole-region reads of one call are bounded by `MAX_OVERSIZE_DOCUMENT`
  (64 MiB) body cap; `line_count`/`size_bytes` must hold at publish time.
  A >48 MiB manifest is refused rather than truncated.
- Access is per host+snapshot identity only; no default-mesh routing has
  changed and nothing is switched over yet.

## Next gates (updated 2026-11-01, second commit)

1. MultiReader integration — **DONE in this day's follow-up commit**:
   `multi_cloud.py` accepts an optional per-scope
   `{"oversize": {"host":…, "snapshot":…}}` entry; invalid or mismatched
   pointer identity fails at reader init, the index loads lazily on first
   use, and `read_text` consults it only after a shard route miss, with the
   path scoped to the admitting source root. Federation manifest exposes
   `oversize_entries`; `test_multi_cloud.py` now runs 31 checks (12 new:
   federated line parity on a 1.2 MiB multi-chunk body, ACL deny,
   scope-root escape denial, empty results past the end, init-time
   fail-closed admission errors, unchanged plain-scope reads).
2. Real publish run of the 75 oversized server-44 rows to Yandex.Disk with
   the existing gateway env, then bounded sha-chain verification, then a
   read canary through the oversize config (no service/cwd/systemd change).
3. Bounded search over oversized docs (shared deadline, honest partial).
4. Real filesystem ACL verification step 3.
