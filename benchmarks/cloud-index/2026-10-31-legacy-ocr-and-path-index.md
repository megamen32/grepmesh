# Cloud index: legacy PDF/OCR bootstrap and unified path discovery (2026-10-31)

## Scope, result, and limitations

Extended the real Yandex.Disk pilot without changing or deleting any active GrepMesh databases. The old 81.1 GiB index on server-100 remains the authoritative fallback; **SSD bytes freed by this work: 0**. This is a validated migration mechanism, not yet a fleet cutover.

### Live cloud-backed content

| Source / policy | Files | Content | Full-text shards | Compressed text-index bytes | Path-only index |
|---|---:|---|---:|---:|---:|
| `grepmesh-code-pilot`, server-100 Git-tracked GrepMesh | 157 | Code and text only | 4 | 4,566,467 | 90,112 bytes raw / 16,965 compressed |
| `grepmesh-ocr-pilot`, server-100 `iri-skill/references` legacy extracted text | 51 | 36 Markdown, 14 PDF, 1 DOCX | 2 | 1,127,454 | 49,152 bytes raw / 6,239 compressed |
| **Unified service** | **208 entries** | Two explicit roots | **6** | **5,693,921** | **139,264 bytes RAM at startup** |

Both scopes are published into private `yadisk:Backups/GrepMesh/cloud-index-v1/` as immutable, SHA-256-verified SQLite shards and path-only indexes. The latest manifest pointer is written only after the complete source footer, all objects and verified remote MD5 hashes.

### Existing OCR migration

`scripts/cloud_index/legacy_bootstrap.py` reads an explicitly selected source root directly from the existing FTS/extraction-cache SQLite **read-only**. It verifies that the indexed file still exists and its byte size and nanosecond mtime match, compares extracted cache content against the authoritative FTS text, and can use FTS when cache bodies were compacted. It requires explicit source host and legacy database, applies format and secret-name exclusions, rejects changed/missing originals or conflicting text, and emits the same streamed manifest protocol as the Git source scanner. It creates no additional database on source SSD.

The selected 51-document root had **no excluded or stale records**. The cloud text was independently compared to the current SQLite source for **7/7 literal queries**, including Cyrillic; exact match sets and non-partial results:

| Query | Old / Cloud matches | Cloud fetches first evaluated | Time |
|---|---:|---:|---:|
| `бизнес` | 19 / 19 | 2 | 4,752.53 ms |
| `проект` | 50 / 50 | 0 | 25.09 ms |
| `финанс` | 15 / 15 | 0 | 9.53 ms |
| `требования` | 33 / 33 | 0 | 13.38 ms |
| `инновац` | 0 / 0 | 0 | 1.29 ms |
| `план` | 25 / 25 | 0 | 10.27 ms |
| Absent sentinel | 0 / 0 | 0 | 0.35 ms |

After clearing the RAM cache, the cold `бизнес` lookup downloaded both segments in 4,553.79 ms with no persistent index file.

### Consolidation into one mesh host

The new `multi_cloud.py` serves multiple explicitly admitted sources through one `server-44-cloud-text` GrepMesh peer, keeping a **shared total 512 MiB RAM-index budget** divided among roots. It preloads only path-only indexes at service startup, currently 139,264 bytes. File-name glob lookups use the path-only index and never fetch large text-index shards.

The main GrepMesh automatically lists the two locations and, when queried with `hosts=['server-44-cloud-text']` and `pattern='*.pdf'`, returned **14 PDF paths, partial=false, status=ok**. The original separate OCR pilot child was disabled and its systemd service stopped; the verified cloud objects were retained. Live full-text `финанс` through the authenticated unified child returned 15 PDF/text documents without partial errors. Main-mesh **full-text** federation remains blocked by GPT Admin verified filesystem ACL requirements; do not bypass them.

The deployed cloud-text service is loopback-only on server-44 port 19435. Its persistent index bytes are zero; `/run/user/1000/grepmesh-cloud-index` is tmpfs. Service cgroup: MemoryHigh=768M, MemoryMax=1024M, CPUQuota=100%. Old GrepMesh and indexes on 100/44/88/Macs were not changed by this release.

### Tests and next gates

- **55 synthetic tests passed**: 33 cloud core, 10 legacy OCR bootstrap, 12 unified multi-corpus tests.
- Real Yandex.Disk pilots preserve hashes, source counts and search sets. Cloud blob content can be privately sensitive; do not expand beyond explicitly authorized roots or claim client-side encryption without implementing it.
- Existing large indexes also hold unexported PDF/OCR, user.io attachments, stale copies and other formats. Current bootstrap policy limits text size to 1 MiB and a narrow root. It does **not** cover all files or replace source-side future PDF/OCR extraction.
- No incremental filesystem/OCR change stream, scalable >500-shard scope router, multi-host full-corpus parity, or consumer ACL certification has been completed. Do not delete any old full-text SQLite files. Complete those gates, switch writers, verify rollback, then reclaim disk.
