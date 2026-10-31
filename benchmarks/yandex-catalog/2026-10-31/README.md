# SQLite vs XERJ: measured comparison

**Date:** 31 October 2026. **Decision:** keep SQLite for the Yandex.Disk metadata catalog, but use the optimized schema/query path tested here. Production was unchanged during this benchmark.

Compared engine: [XERJ](https://github.com/xerj-org/xerj), installed version **v1.0.0-rc.100**, against SQLite **3.37.2**. This is a bounded workload comparison, not a claim about every search workload or the latest XERJ release.

## Test setup

50,000 uniformly sampled records from a 622,692-file catalog, on the same host (server 44). Deterministic reservoir seed: **4410031**. All test databases were on tmpfs. Every query used local HTTP with engine order rotated. There were **80 filename-query cases** across two suites; all three engines returned correct match counts and valid result IDs. No neural embeddings were used. Private sampled filenames and document bodies are intentionally not included.

## Measurements

| Measure | Current SQLite layout | Optimized SQLite | XERJ |
|---|---:|---:|---:|
| Raw index footprint | 57.6 MiB | 31.0 MiB | 16.0 MiB |
| Compressed snapshot | 16.5 MiB | 8.8 MiB | 13.6 MiB |
| Process RSS while serving | 39.6 MiB | 39.7 MiB | 130.3 MiB |
| Build / ingest to queryable | 2.66 s | 2.49 s | 18.96 s |
| Unseen-query median | 28.2 ms | 6.4 ms | 66.2 ms |
| Unseen-query p95 | 68.9 ms | 52.9 ms | 94.9 ms |
| Repeated-query median | 22.1 ms | 3.8 ms | 3.5 ms |

Latency rows use the same supplemental 45-query suite: 10 exact, 10 prefix and 25 substring queries. The first pass used new query strings; two more passes repeated them. This is **not a cold-disk benchmark**. XERJ was especially fast at repeated substring queries (3.6 ms median), but first-use substring queries took 87.8 ms versus optimized SQLite's 18.1 ms. A request-cache bypass was requested; repeat improvements must not be attributed to a particular cache without further profiling.

Process RSS and tmpfs database storage overlap and should not simply be added. XERJ's Elasticsearch-compatible version response is not its binary version.

The optimized SQLite variant removes redundant stored/indexed path/name data and routes substring searches through a single-column FTS5 trigram index with detail=none and columnsize=0. These are metadata-specific changes, not a general full-text configuration. MATCH queries longer than a trigram are not interchangeable with indexed LIKE/GLOB in this reduced-detail configuration; the query implementation must change together with the schema.

## Document-text check

24 README files were read from Yandex.Disk; 20 were unique, totaling 118,394 text bytes. This was a small literal-search smoke test, not a semantic relevance evaluation.

SQLite matched all 20 reference word/phrase cases; XERJ matched 17. Analyzer inspection explained the differences: XERJ preserves filename tokens such as settings.jpg, edge.service and current.md, whereas the SQLite tokenizer splits at the period. This is a default-semantics difference, not demonstrated ingestion loss. Align analyzers before migrating content search.

Repeated-query medians were 1.84 ms for SQLite and 2.12 ms for XERJ; this corpus is too small to establish a general full-text performance winner.

## Recovery, limits and cleanup

XERJ restarted on its existing tmpfs directory and recovered all 50,000 metadata records and 20 documents; readiness took 1.39 seconds. SQLite reopened and counted 50,000 rows in about 9.6 milliseconds. These are different startup operations, not a durability contest.

Native S3/Yandex gateway performance, neural relevance, complete-catalog XERJ scaling and high concurrency were **not tested**. Neither local restart result proves cloud or power-loss recovery.

The isolated XERJ process was stopped. Temporary binary, databases and sampled content were deleted, releasing 303.9 MiB from tmpfs. Only small benchmark scripts/results remained on the owner's server; production indexes were unchanged.

## Action

Optimize SQLite first for this catalog: lower serving-process memory, faster unseen queries and the smallest compressed snapshot. Keep XERJ as a separate candidate for richer retrieval. Keep file inventory, document-content indexing and durable storage as distinct concerns. Do not extrapolate sample ratios into claimed production measurements.

Numerical data and methodology notes: [results.json](results.json).

References: [SQLite FTS5](https://www.sqlite.org/fts5.html), [XERJ repository](https://github.com/xerj-org/xerj).
