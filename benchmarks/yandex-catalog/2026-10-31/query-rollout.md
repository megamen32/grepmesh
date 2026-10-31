# Production query-path rollout — 2026-10-31

This follows [the SQLite/XERJ comparison](README.md). Filename-query changes were applied to the running Yandex.Disk catalog adapter on server 44. The database schema and cloud snapshots were **not migrated**. The Rust GrepMesh core and XERJ were not changed.

## Changes applied

1. Wildcard filename lookup uses the existing FTS5 trigram index instead of scanning the metadata table with an OR over path and basename.
2. Two disjoint UNION ALL branches preserve path-or-basename matching without deduplication sorting. The second branch excludes rows already matched by the first. Filters are applied before the shared result limit.
3. Catalog size comes from the snapshot manifest, not SELECT count(*) FROM files for every query.
4. Directory enumeration first narrows by the indexed normalized-path prefix, then applies the original case-sensitive prefix check. Literal wildcard characters in directory names are quoted.

Core indexed query shape (parameters are bound, never interpolated):

~~~sql
SELECT f.* FROM names_fts n JOIN files f ON f.id=n.rowid
WHERE n.path_fold GLOB ?
UNION ALL
SELECT f.* FROM names_fts n JOIN files f ON f.id=n.rowid
WHERE n.name_fold GLOB ? AND NOT (f.path_fold GLOB ?)
LIMIT ?;
~~~

The adapter retains its root/path filters around this query before LIMIT. A table-scan fallback remains for a partially built catalog without an FTS table. Very short patterns may still require an FTS scan; this is not a guarantee that every wildcard query is constant time.

## Measured on the complete live catalog

622,692 metadata records. Each number is the median of three same-host loopback HTTP calls, with a result limit of 10. This is a small rollout check on a shared host, not a cold-storage, high-concurrency or WAN benchmark.

| Query class | Before, ms | After, ms | Returned results |
|---|---:|---:|---:|
| Device backup name | 45.076 | 4.996 | 10, truncated |
| receipt.json | 326.496 | 9.207 | 3 |
| Takeout | 327.774 | 4.753 | 0 |
| Deliberately absent name | 283.283 | 11.972 | 0 |
| PDF extension | 53.277 | 4.711 | 10, truncated |
| Cyrillic filename query | 518.831 | 2.977 | 2 |

Returned counts and truncation flags matched before and after. These checks do not establish that a file absent from the name catalog is absent inside an archive.

## Targeted regression checks

All 13 synthetic regression cases passed: wildcard and exact basename matching, path-prefix matching, Cyrillic, no hits, broad patterns, root restriction, path-glob restriction, ordinary literal search, and directory names containing brackets. Matching a path and its basename must not duplicate the result. Query tracing confirmed that the request path no longer performs a whole-catalog count. EXPLAIN QUERY PLAN reported an indexed FTS GLOB (VIRTUAL TABLE INDEX 0:G0) followed by primary-key lookup.

The serving process was restarted once to load the changed adapter. The latency helper was then gated on HTTP readiness rather than assuming that a systemd active state means the listener is already bound.

## Boundaries

- Database size remains 680,771,584 bytes (about 649 MiB) in tmpfs.
- No reduced-detail schema conversion was applied in this rollout.
- No source documents or archived content were downloaded for these changes.
- Existing large fleet indexes were neither deleted nor migrated.
- Full-text document search and the separately reported GPTAdmin filesystem-ACL restriction were not changed or bypassed.

Next storage-focused change should convert the catalog to the tested lean layout, with metadata equality and cloud snapshot verification before replacing the active database. This report does not claim that conversion has happened.
