# Full legacy-text archive: current migration gates (2026-11-01)

## Server-44: complete private cloud archive

- Original live SQLite path: `/home/roomhacker/.cache/grepmesh/server-44-65415a79c0e91f4b.sqlite3`, **19,123,527,680 B** (17.81 GiB).
- Immutable Yandex.Disk snapshot: `yadisk:Backups/GrepMesh/cloud-index-v1/archives/server-44/20261031T214414Z-ae84d605/complete.json`.
- The original database was read under one consistent SQLite WAL snapshot; no source file or database was modified by the export.
- Source-data tables in the completed archive:

| Table | Preserved records |
|---|---:|
| `grepmesh_documents_content` (original full text, including history) | 849,796 |
| `grepmesh_document_rows` (exact FTS row mapping) | 849,796 |
| `grepmesh_extraction_cache` (OCR/text cache and timestamps) | 849,796 |
| `grepmesh_metadata` | 4 |
| `grepmesh_ocr_attempts` | 15 |
| `grepmesh_ocr_empty_results` | 459 |
| `grepmesh_pending_ocr` | 0 |

- Export finished successfully after ~388 s. Cloud upload verification used exact remote size and provider MD5 for every object, with SHA-256 in each immutable manifest.
- **Independent full remote replay** via `verify_native_archive.py --validate-json` completed successfully after ~195 s, checking every compressed-object SHA-256, all JSON rows, logical table SHA-256, row-order bounds and manifest counts. Total verified compressed cloud bytes: **912,340,234 B** (~870 MiB).
- One real document was subsequently retrieved from the cloud **without its source file**, with the original FTS row ID and exact body SHA-256 validated; ~12.9 s cold read.
- The original native search service and its on-SSD SQLite remain **active and retained**. The cloud archive provides lossless *cold recovery*, not full query parity or a tested read-only production replacement.

## Server-100: stopped incomplete source export to protect WAL

- Live native SQLite remains **87,034,703,872 B** (~81.1 GiB).
- The export started from the live SQLite under a consistent WAL read transaction and successfully uploaded native metadata and ~50,000 original FTS text rows as private immutable *partial* chunks.
- Observed WAL growth reached ~718 MiB before the owner-controlled export was stopped. This was a deliberate safety stop to avoid multi-gigabyte WAL pinning while exporting millions of larger native records. No completion pointer or verified full backup exists for this snapshot.
- Attempt to create a separate online SQLite snapshot artifact through the connected execution environment was blocked by the platform safety boundary. **Do not bypass that denial through another execution path.**
- No old native GrepMesh indexes or running search services were stopped or deleted. The original server-100 writer was left active; its WAL was later observed reduced to 32 bytes.

## Exact cutover gates still unmet

1. Produce and independently replay a **complete** consistent server-100 archive, including all FTS bodies, deleted-source histories and OCR metadata, without unbounded WAL or SSD growth.
2. Turn the large archived corpora into fully searchable, source-scoped, updated Yandex.Disk shards with bounded RAM/cold query times; the existing 4 production code scopes (server-100 ~41k docs) and server-44 896-doc pilot **do not cover 3.2M + 849k legacy rows**.
3. Verify broad search and read-text parity, fresh OCR handling, authorization for federated full-text results, lost-peer/rollback and durable recovery after restart, then stop old writer(s), verify no consumers depend on SQLite, and only then remove native SQLite/WAL/SHM.
4. Recheck free SSD blocks before/after each destructive cutover. **SSD reclaimed from native SQLite deletion: 0 B at report time.**

The owner's authorization is sufficient to perform the migration when these checks pass; it is **not evidence** that deleting the still-active full-text database would preserve search. Do not confuse cold-recoverable data with fully indexed searchable data.
