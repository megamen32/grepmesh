# GrepMesh disk cutover: verified status and next safety gates (2026-11-01)

## Summary

The user authorized migration and eventual deletion of native SQLite files; this authorization does **not** establish full search parity. The native full-text databases remain active and were NOT deleted.

| Host | Native SQLite size | New cloud state | SSD reclaimed by deleting SQLite |
|---|---:|---|---:|
| server-100 | 87,034,703,872 bytes / 81.1 GiB | Four live direct-source cloud scopes, plus provisional resumable raw-FTS capture | **0 B** |
| server-44 | 19,123,527,680 bytes / 17.8 GiB | Complete source archive and eight private historical search scopes | **0 B** |

### Server-44: full original text protected, fast search incomplete

The immutable private source-data backup at `yadisk:Backups/GrepMesh/cloud-index-v1/archives/server-44/20261031T214414Z-ae84d605/` was independently replayed with 849,796 FTS bodies, 849,796 exact FTS row mappings, 849,796 extracted-cache rows, OCR attempts, empty OCR results, pending OCR state and metadata. Download/CRC and logical SHA-256 validation succeeded, with 912,340,234 compressed bytes verified. A real historical file was restored by row ID/path/body hash, independently of the current filesystem.

Eight private, immutable cloud FTS scopes contain **849,721** documents (approximately 106k per scope, 137 full-text shards in total). The **75 remaining oversized texts** are preserved losslessly in the original cold archive, but are not searchable by the 1 MiB-per-document FTS reader. The private preview is loopback-only, not part of the public/default GPT Admin mesh.

- First native private canary: `find_paths *AGENTS.md` in ~0.29 s, five hits and `partial=false`.
- `grepmesh` full-text query limited to five hits: **55.61 s** across all eight serialized scopes, `partial=false` but too slow.
- An attempted concurrent all-scope prefetch overloaded rclone and exceeded a 70-second HTTP deadline; it was reverted.
- Final bounded strategy: filename glob queries run concurrently over hot path indexes; full-text queries scan buckets in deterministic order with ONE shared deadline and stop when `limit` hits are found. They return `truncated=true` if other buckets were not visited. The same `grepmesh` canary now returns **five hits in 6.64 s**, `partial=false`, `truncated=true`, eight cloud reads.
- Source test harness covers concurrent filename lookup, ACL narrowing, no-match semantics and bounded memory. The original Rust search service was not stopped.

**Operational gate:** main GrepMesh federation still responds `remote search requires a verified GPTAdmin filesystem ACL` for a full-text query sent to server-44 by this OAuth connection. Do not bypass that ACL or register private historical text under a broad unauthenticated profile. Cloud reader still lacks regex, equivalent read_text and live OCR/index refresh; native index remains necessary for those consumers.

### Server-100: avoid unsafe WAL pinning

A single multi-hour consistent SQLite read snapshot was aborted after it grew the production WAL to ~718 MiB. An alternative SQLite backup artifact was blocked by the platform safety boundary; do not circumvent that operation.

`export_native_rolling.py` performs **short, independent read transactions** of up to 128 original FTS rows. It creates immutable private Yandex.Disk gzip chunks, SHA-256 verified by source key, and updates a resumable `progress.json` after each successful upload. One run ID was created at `archives/server-100/rolling/20261031T233108Z-5732b979`. Its first three uploaded chunks were independently downloaded and verified: 2,644 original FTS bodies, last row ID 8,860, then a bounded server-side continuation was started under systemd with 100% CPU, 512 MiB MemoryHigh, 1.5 GiB MemoryMax, low IO priority and a 2 GiB WAL safety limit.

The first subsequent observation showed 30,619 archived bodies across 23 chunks, with a 72 MiB production WAL and active native GrepMesh. **The run is explicitly provisional:** `consistent_snapshot=false`, `safe_for_deletion=false`. Future source writes may occur between short read transactions; only a separately permitted frozen-writer reconciliation can establish a cutover-safe snapshot.

### Mandatory final gates

1. Finish the server-100 incremental capture, check all cloud chunks independently and capture metadata/OCR, then obtain consistent source parity with a permitted source-freeze reconciliation.
2. Make the 75 oversized server-44 documents fully searchable (or explicitly certify a supported equivalent fallback), implement normal regex/read-text access and fresh OCR changes, and prove full native-search parity.
3. Prove authorized federation under a verified least-privilege filesystem ACL with real consumers, and run stale-source/failed-peer/rollback restart tests.
4. Only after all checks pass, stop the native GrepMesh **writer**, verify remaining consumers, remove the native SQLite/WAL/SHM and measure `df` before and after. Until then any removal risks losing historical search.

No unattended GitAdmin upgrade or other unrelated service manipulation is part of this migration.
