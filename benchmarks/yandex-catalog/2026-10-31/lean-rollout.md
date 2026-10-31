# Full-catalog compact SQLite rollout — 2026-10-31

Status: deployed and checked through the primary GrepMesh find_paths tool.
This is a production schema change, not an extrapolation from the 50,000-row
comparison. [Aggregate evidence](lean-rollout.json) contains no source file
contents, credentials or personal inventory.

## Results

| Measurement | Before | After |
|---|---:|---:|
| Catalog records | 622,692 | 622,692 |
| Working SQLite database | 680,771,584 bytes / 649.23 MiB | 319,864,832 bytes / 305.05 MiB |
| Compressed cloud snapshot | 109,616,609 bytes / 104.54 MiB | 57,994,502 bytes / 55.31 MiB |
| Working-database reduction | — | 53.01%, 360,906,752 bytes |
| Compressed-snapshot reduction | — | 47.09% |

The working database remains on tmpfs on the search host, not on its SSD.
These figures describe database files, not the total process RSS/cgroup charge.
The old in-RAM database and generated restore/upload files were removed after
acceptance. The previous cloud snapshot remains available for rollback.

## Changes

The immutable published files table no longer duplicates original/folded
basenames. The raw-path unique index is removed from that published table;
exact-path deduplication remains in mutable ingestion staging. One folded-path
B-tree and one reduced-detail, single-column FTS5 trigram index remain.

The adapter derives display names from original paths, preserves basename-only
GLOB semantics, uses GLOB instead of MATCH phrases for substring lookup, and
uses the folded-path index before case-sensitive exact reads. New rebuilds
publish the compact format rather than silently recreating the previous schema.

The reusable source and synthetic tests are in
[scripts/yandex_catalog](../../../scripts/yandex_catalog).

## Validation performed

- 32 synthetic cases passed on both schemas, including Cyrillic, case-distinct
  paths, punctuation, short terms, literal wildcard characters and glob classes.
- The existing 13 adapter regression cases passed.
- Every retained logical record was compared by stable-order streaming SHA-256;
  all 622,692 records, IDs, original paths, sizes, dates and folded paths matched.
- The versioned cloud snapshot was uploaded, downloaded and decompressed. The
  restored database byte hash, schema version and record count matched.
- A synthetic future rebuild exercised staging -> compact database -> snapshot
  publication, with no network operations; case-distinct paths were preserved.
- A query through the main GrepMesh found the cloud file with partial=false and
  no host error. Metadata-only versus content-search coverage is unchanged.

## Post-deployment query timings

Median of three loopback HTTP requests per case on the complete catalog:

| Query family | Median |
|---|---:|
| Device name | 3.501 ms |
| Receipt filename | 3.936 ms |
| Takeout name | 2.836 ms |
| Absent name | 4.161 ms |
| PDF suffix | 2.880 ms |
| Cyrillic filename | 2.452 ms |

These are service-local warm timings, not internet latency or a new comparative
XERJ benchmark. Result counts and truncation flags remained consistent.

## Scope and remaining work

This rollout optimizes the Yandex metadata catalog only. It does not index
contents of every document or archive. The older large general-purpose
GrepMesh indexes on the other server disks were neither migrated nor deleted.
No claim of SSD space reclamation is made by this rollout.
