# Cloud-backed GrepMesh text index

Status: 2 additional direct-source-tree scopes on server-100 now index Agent Herder (1,969 current files) and ExManager (10,782 current files) in Yandex.Disk, alongside 896 older pilot documents on server-44. Total across the two cloud services: 13,647 documents. This is NOT full fleet migration.
Results: [initial pilot](../../benchmarks/cloud-index/2026-10-31-real-yadisk.md), [legacy OCR / unified cloud index](../../benchmarks/cloud-index/2026-10-31-legacy-ocr-and-path-index.md), and [compact FTS5 / Airlock canary](../../benchmarks/cloud-index/2026-10-31-compact-fts-airlock.md).
No native fleet index was deleted, switched off, or replaced. The cloud-backed pilot is limited to its declared text policy; full-text queries via the main GrepMesh still require authorized filesystem ACL propagation.

## Storage contract

- Source scanner reads original UTF-8 text/code files directly: Git-tracked files for the Git scanner, or currently present tracked/untracked eligible files for the tree scanner. It does not read or build a local search SQLite index.
- Publisher builds bounded immutable SQLite shards in tmpfs. A contentless FTS5 trigram table uses `detail=none,columnsize=0`; compressed body text is stored once per distinct body within a shard. Queries use bounded conjunctions of safe individual trigrams with exact-body verification. Short/tokenizer-unfriendly queries fall back to a bounded per-shard scan, never an authoritative empty response.
- A compact path-only SQLite index is also written per scope and preloaded into bounded RAM, so filename globs do not require fetching all full-text shards.
- Durable shard objects and immutable generation manifests live in the configured private cloud prefix. The latest pointer is published only after the full source footer and object verification succeed.
- Reader loads requested shards into a byte-bounded LRU in tmpfs. Default index cache cap is 512 MiB; each raw shard is at most 64 MiB. Compressed-transfer buffers, manifests, SQLite caches, and the interpreter are additional RAM, so a service cgroup is still required.
- Runtime refuses non-tmpfs paths. It never silently spills its index/cache to an SSD. The existing authenticated rclone gateway is used without reading or copying credentials. Its optional scratch/cache are explicitly placed in RAM as well.
- Source-scoped generations make changed, renamed and deleted tracked text files visible after a reader manifest refresh/restart. Old cloud manifests and objects remain available for rollback. Automatic cloud garbage collection is intentionally not implemented yet.
- A missing footer, changed source file, missing input file, permission failure, corrupted object or query deadline must not become an authoritative empty result.

## Current supported scope

One explicitly admitted Git working tree per corpus, plus an optional **one-time legacy extraction bootstrap** from the already existing read-only GrepMesh FTS/OCR database for a separately admitted narrow source root. The latter preserves existing extracted PDF/DOCX text but is NOT a replacement for fresh PDF/OCR extraction after cutover. Multiple scopes can be exposed through one `multi_cloud.py` peer; the shared RAM-cache budget is bounded.
Supported extracted UTF-8 bodies are at most 1 MiB each. Symlinks, known secret filenames, binary/unsupported formats are excluded and reported by source policy. A completed policy scope does not imply the full native 81 GiB index is migrated.

Literal/case-insensitive text and path retrieval and filename glob search are implemented. Regex, relevance ranking and a snapshot read_text MCP tool are not implemented. They must not be advertised as supported. The worker listens only on loopback and requires an existing authenticated connector/SSH path. Forwarded text queries require a filesystem read ACL and its roots filter results; this does not replace the transport's authentication.

The new reader must be deployed before publishing `detail=none` shards; older phrase-MATCH readers cannot query that format. A per-shard trigram Bloom filter can avoid unnecessary fetches. Its false positives only cause extra reads. Bloom size is fixed; very large or broad corpora may need improved routing/filters. Cold searches across many uncached shards can be much slower than warm searches; measured real-cloud latency applies only to the narrow pilots, not millions of documents.

## Configuration boundaries

GREPMESH_CLOUD_GATEWAY names the already authorized SSH/rclone gateway. GREPMESH_CLOUD_RCLONE can select its existing rclone executable. Both source identity and source root must be explicitly supplied. No credentials belong in this repository.

Application data and caches belong in /run/user/<uid>/grepmesh-cloud-index. Ordinary program source/configuration on disk is small and is not the search database.

Repository resource rules still apply: explicit cgroups, no concurrent heavy rebuild, bounded memory and CPU. The standalone module also applies process affinity. A source scan is not a promise that the entire filesystem has been covered.

## Tests and rollout gate

The test_cloud_index.py harness uses synthetic files and an in-memory cloud transport, with all temporary SQLite files in tmpfs. It covers source-body search, Cyrillic and expanding case-folding, cache misses/hits/eviction, empty-cache recovery, source/root restrictions, read-ACL filtering, source changes/deletions/renames, incomplete publication, corruption, case-distinct paths, contentless FTS, and source-agent exclusions.

Before removing any native fleet database:

1. Inventory/allowlist every native source root and format; preserve already extracted OCR and attachments as needed without exporting secrets.
2. Publish each complete scope, fetch it from an empty cache, and measure cold/warm query latency and actual cache/cgroup memory.
3. Implement source-side updates/OCR without the old SQLite; preserve native coverage and formats, source changes/deletions, caller authorization, and complete/partial response semantics.
4. Verify the existing federated peer with authorized full-text reads (the current ACL gate must be preserved, not bypassed), not just path discovery.
5. Only then stop the old writer and remove the old index after checking that it contains no unique irreplaceable extracted content.

The code contains no native-database deletion command. The existing 81 GiB server-100 index remains unchanged by this work.

## Scheduled current-source snapshot refresh (deployed on server-100)

The `refresh_git.py` script refreshes only explicitly allowlisted Git or source-tree projects from server-100, without reading its legacy SQLite index. It verifies the complete source stream and original cloud manifest. If unchanged it performs zero writes. If changed, it publishes a new immutable generation with the pointer written last, while retaining the previous version for rollback. Empty scans, malformed data, or sudden loss of over half the documents fail closed. Runtime source streams and SQLite shards must remain in tmpfs.

This is a bounded full-scope refresh, **not incremental per-file indexing**. It supports at most 100,000 documents and 256 MiB of source-stream data per scope (64 MiB default for nonproduction use). PDF/OCR extraction refresh is not implemented. The active timer `grepmesh-cloud-projects-refresh.timer` runs every 30 minutes under a user-systemd cgroup, with a 512 MiB RAM-cached search service. See `refresh_tree_scopes.example.json` and `systemd/` for the deployed Agent Herder and ExManager policy; `refresh_git_scopes.example.json` remains available for the two Git pilots. An unchanged scope performs zero cloud writes. Any changed scope is published atomically and triggers the cloud search service to restart and reload its verified manifests. The scheduled refresher and manual publisher share a nonblocking `publisher.lock` in tmpfs: overlapping runs fail closed, and failed scans release the lock without changing the active cloud pointer. User authorization alone does not grant a GPT Admin connection the verified filesystem ACL required by native multi-peer full-text routing.
