# Fleet storage assessment, 2026-10-31

## Confirmed outcome

The compact Yandex catalog is reachable through the existing GrepMesh mesh from server-44, server-88, Mac M1 and Mac mini. Server-100 passed the preceding rollout canary. Every new path-search canary returned the known cloud object with partial=false. No per-host copy of the cloud database is required.

This is **not a completed fleet migration of the native full-text indexes**. No large native database was deleted or modified in this assessment.

## Native databases found

| Host | File bytes | Approximate GiB |
|---|---:|---:|
| server-100 | 87034703872 | 81.06 |
| server-44 | 19083595776 | 17.77 |
| server-88 | 23617646592 | 22.00 |
| mac-m1 | 4692992000 | 4.37 |

The server-100 database contains 3,101,553 keyed records. Approximately 1.52 GiB was on its freelist, so VACUUM alone cannot turn the 81 GiB index into a small catalog.

## Why these databases are different

Native GrepMesh stores extracted document text and a positional trigram FTS5 index, plus a separate extraction-cache body column. The optimized Yandex catalog stores file metadata only. Replacing one with the other would silently remove content/OCR search coverage. Reducing FTS5 detail also requires coordinated query changes; it is not a standalone schema switch.

A bounded scan covered 2,000,000 paths, not the complete database. It included 685,781 paths below agents-projects, 524,124 below PycharmProjects, 211,956 below .local/share and 74,300 below .codex/worktrees. Overlapping categories included 81,627 release-copy paths, 133,560 agent-history paths and 36,532 browser paths. These are **counts, not allocated bytes or verified removable duplicates**.

Whole-table size aggregation hit explicit resource/time bounds. It was not allowed to run indefinitely on the shared production host, and no unmeasured table-size breakdown is asserted.

## Additional disk use on server-100

A bounded home-directory scan completed these directories before its 80-second deadline: `.cache` 126.1 GiB, `.config` 21.1 GiB, `backups` 17.5 GiB, `airlock-admin` 13.2 GiB, `.lmstudio` 9.5 GiB, `.hermes` 8.6 GiB. The overall scan is **incomplete** (exit 124); this is not a full ranking of disk usage. GrepMesh is already included in `.cache`, not additional to it. No unrelated project, backup, model or browser data was deleted.

### Completed `.cache` breakdown

The subsequent depth-one `.cache` inventory completed successfully: GrepMesh 81.1 GiB, Yandex OAuth browser profile 7.0 GiB, Go build cache 7.0 GiB, omniroute development sources 3.7 GiB, git-private2public 2.9 GiB, omniroute artifacts 2.4 GiB, Hugging Face model cache 2.3 GiB, uv cache 1.9 GiB, browser-os 1.7 GiB. Total `.cache` was 126.1 GiB. These files were inventoried, not deleted; browser sessions, models and rollback artifacts need their own retention rules.

## Deletion gate

1. Preserve authoritative extracted text, fingerprints, record identities and search semantics.
2. Prepare and verify a compatible replacement, including on-demand restoration from the durable store.
3. Switch a host and test changed search behavior.
4. Only then remove that host's old database and measure filesystem space reclaimed.

A native maintenance invocation was blocked by the execution tool. No alternate destructive path was attempted. The old full-text databases remain available.

See [structured measurements](fleet-storage.json). This report contains no document bodies or credentials.
