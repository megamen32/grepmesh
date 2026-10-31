# Omniroute code-only cloud index: live Yandex.Disk canary (2026-10-31)

## Result

Published private immutable Yandex.Disk scope `omniroute-code-current` from `/home/roomhacker/agents-projects/omniroute-zcode`. Explicitly scanned only top-level directories `src`, `tests`, `open-sse`, `scripts`, `bin`, `skills`, `public` and `@omniroute`; the allowlist is retained in the source policy and in the configured auto-refresh profile. No files were moved or deleted.

- **14,940** current text/code documents, **131.21 MiB** source UTF-8 bodies.
- **33** contentless FTS5 shards, **62,187,010 B** compressed fulltext, **119,054,336 B** raw search-shard SQLite.
- One compact filename index: **7,847,936 B raw / 1,541,030 B compressed**.
- Successful publisher run: 36 verified cloud uploads, 193.61 s build time, 197.66 s wall time under explicit CPU/RAM/IO quotas.
- Source scan excluded 7 generated/dependency directories, 37 unsupported extensions and 28 oversized files. It did **not** include `.claude`, `.build`, `docs` or historical `dist` copies. Excluded content remains locally unchanged.

### Why this narrow scope

Legacy native SQLite contains about **581,111 indexed paths** under `omniroute-zcode`, including **425,561** under `.claude` (histories/work data) and 29,860 under `.build`. An independent current-file inventory found ~325,292 eligible .claude files (7,199 MiB of bodies) and ~22,241 .build files (816.5 MiB). Those are not admitted to this cloud corpus to avoid exporting unrelated history, temporary build output, or sensitive data.

### Real consumer checks

After adding a fourth scope, the `server-100-cloud-text` service reports **41,176** source-scope entries, `ready=true`, `persistent_index_bytes=0`, with **512 MiB shared RAM cache**. The independent server-44 cloud peer reports another 896 entries: **42,072** combined scope entries; these are not an exhaustive inventory of the native 3.1 million paths.

- Main GrepMesh `list_locations`: Omniroute visible, `partial=false`.
- Main GrepMesh scoped `find_paths("*.ts")`: 6 returned, `partial=false`.
- Authenticated cloud child `search_text("tsup.config.ts")` scoped to Omniroute: 4 matches, 30 cloud shard downloads, **16.42 seconds cold**, `partial=false`.
- Source refresh profile has `only_top` for exactly the eight selected directories; unexpected selectors and Git/tree mismatches fail admission tests.
- The first **scheduled** four-scope refresh fired on **2026-10-31 23:00:19 MSK**. The refresh service exited successfully (`Result=success`, `ExecMainStatus=0`), the search service remained ready with 41,176 documents, and the next timer trigger was scheduled for 23:30:51 MSK.

The native FTS remains in use. **No SSD space was reclaimed by deploying this cloud corpus.** Complete native parity, fresh OCR/media handling, and verified filesystem ACL propagation are still required before deleting any 81.1 GiB legacy index.
