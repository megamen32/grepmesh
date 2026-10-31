# Compact Yandex file metadata catalog

This directory contains the credential-free schema/query component deployed by
the Yandex catalog adapter. It does not change the Rust GrepMesh engine or XERJ.
The adapter continues to expose the existing GrepMesh-compatible tools.

## Schema and integration

- Mutable ingestion/staging retains its exact, case-sensitive path uniqueness
  and resumable enumeration. Do not replace this with a unique case-folded key.
- Only the immutable published snapshot is compacted: id, original path, size,
  modified date and folded path are retained. Basenames are derived from paths.
- One B-tree indexes the folded path. One external-content FTS5 trigram index
  indexes it with detail=none and columnsize=0. The duplicate basename index and
  raw-path unique index are not needed in an immutable, validated snapshot.
- Call compact_catalog(source, candidate) beside, never on, a live database.
  It compares a streaming SHA-256 over every retained logical record, preserves
  IDs/case-distinct names, validates SQLite, and refuses an existing target.
- metadata_query(connection, query, glob) returns SQL and bound parameters.
  Add permission/root filters before applying LIMIT. Derive display basenames
  from the original path. For an exact case-sensitive file read, use both
  path_fold = ? AND path = ? so the folded B-tree can select candidates.
- Basename-only glob matching remains supported through an indexed whole-path
  candidate set followed by the SQLite basename GLOB predicate. No filename
  column is needed. Both legacy and compact databases are queryable during a
  migration/rollback.
- Use indexed GLOB for substring queries. MATCH phrases are not interchangeable
  with reduced-detail trigram indexes. Literal wildcard characters are escaped.

See [SQLite FTS5 trigram documentation](https://www.sqlite.org/fts5.html#the_trigram_tokenizer).

## Deployment and durability

The deployed adapter builds candidates and serves the database on tmpfs on its
search host. A versioned compressed snapshot remains on Yandex Disk. Publication
occurs only after cloud upload/hash checks and actual download/decompression
verification. The previous cloud snapshot is retained for rollback.

Future adapter rebuilds use mutable staging -> compact candidate -> verified
snapshot -> atomic database replacement. The compact component itself does not
manage credentials, network storage, service restarts or root configuration.

Run tests/builds inside the resource limits in AGENTS.md. The synthetic test
uses TMPDIR; point it at a permitted tmpfs directory. It checks both schemas,
case-distinct paths, Cyrillic, short strings, exact names and escaped wildcards.

## Evidence

- [Measured SQLite/XERJ comparison](../../benchmarks/yandex-catalog/2026-10-31/README.md)
- [Full-catalog compact-schema rollout](../../benchmarks/yandex-catalog/2026-10-31/lean-rollout.md)
