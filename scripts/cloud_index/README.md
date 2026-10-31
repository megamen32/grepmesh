# Cloud-backed GrepMesh text index

Status: synthetic tests and real Yandex.Disk pilot verified on 2026-10-31 for 13 tracked docs and 155 tracked repository files. NOT a completed fleet migration.
Measured cold/warm real-cloud retrieval and source-to-cloud path-set parity: [pilot report](../../benchmarks/cloud-index/2026-10-31-real-yadisk.md).
No native fleet index was deleted, switched off, or replaced. The cloud-backed pilot is limited to its declared text policy; full-text queries via the main GrepMesh still require authorized filesystem ACL propagation.

## Storage contract

- Source agent reads original tracked UTF-8 text files directly. It does not read or build a local SQLite index.
- Publisher builds bounded immutable SQLite shards in tmpfs. A contentless FTS5 table stores postings, not a second body copy. Compressed body text is stored once per distinct body within a shard.
- Durable shard objects and immutable generation manifests live in the configured private cloud prefix. The latest pointer is published only after the full source footer and object verification succeed.
- Reader loads requested shards into a byte-bounded LRU in tmpfs. Default index cache cap is 512 MiB; each raw shard is at most 64 MiB. Compressed-transfer buffers, manifests, SQLite caches, and the interpreter are additional RAM, so a service cgroup is still required.
- Runtime refuses non-tmpfs paths. It never silently spills its index/cache to an SSD. The existing authenticated rclone gateway is used without reading or copying credentials. Its optional scratch/cache are explicitly placed in RAM as well.
- Source-scoped generations make changed, renamed and deleted tracked text files visible after a reader manifest refresh/restart. Old cloud manifests and objects remain available for rollback. Automatic cloud garbage collection is intentionally not implemented yet.
- A missing footer, changed source file, missing input file, permission failure, corrupted object or query deadline must not become an authoritative empty result.

## Current supported scope

One explicitly admitted Git working tree per corpus; supported tracked text/code files up to 1 MiB each. Symlinks, known secret filenames, binary/non-UTF-8 files and unsupported formats are excluded and reported by the source policy. PDF/OCR/audio ingestion is NOT implemented here. A completed text-policy corpus does not imply that every document type or every host has been migrated.

Literal/case-insensitive text and path retrieval and filename glob search are implemented. Regex, relevance ranking and a snapshot read_text MCP tool are not implemented. They must not be advertised as supported. The worker listens only on loopback and requires an existing authenticated connector/SSH path. Forwarded text queries require a filesystem read ACL and its roots filter results; this does not replace the transport's authentication.

A per-shard trigram Bloom filter can avoid unnecessary fetches. Its false positives only cause extra reads. Bloom size is fixed; very large or broad corpora may need improved routing/filters. Cold searches across many uncached shards can be much slower than warm searches; no real-cloud latency guarantee has been measured.

## Configuration boundaries

GREPMESH_CLOUD_GATEWAY names the already authorized SSH/rclone gateway. GREPMESH_CLOUD_RCLONE can select its existing rclone executable. Both source identity and source root must be explicitly supplied. No credentials belong in this repository.

Application data and caches belong in /run/user/<uid>/grepmesh-cloud-index. Ordinary program source/configuration on disk is small and is not the search database.

Repository resource rules still apply: explicit cgroups, no concurrent heavy rebuild, bounded memory and CPU. The standalone module also applies process affinity. A source scan is not a promise that the entire filesystem has been covered.

## Tests and rollout gate

The test_cloud_index.py harness uses synthetic files and an in-memory cloud transport, with all temporary SQLite files in tmpfs. It covers source-body search, Cyrillic and expanding case-folding, cache misses/hits/eviction, empty-cache recovery, source/root restrictions, read-ACL filtering, source changes/deletions/renames, incomplete publication, corruption, case-distinct paths, contentless FTS, and source-agent exclusions.

Before removing any native fleet database:

1. Review and authorize the blocked real-corpus publication through the normal execution path; do not route around a guard.
2. Publish the intended corpus, fetch it from an empty cache, and measure cold/warm query latency and actual cache/cgroup memory.
3. Preserve native coverage and formats, source changes/deletions, caller authorization, and complete/partial response semantics. Full native replacement requires ingestion of document formats this first implementation does not handle.
4. Wire the verified remote peer into GrepMesh and exercise the consumer path.
5. Only then stop the old writer and remove the old index after checking that it contains no unique irreplaceable extracted content.

The code contains no native-database deletion command. The existing 81 GiB server-100 index remains unchanged by this work.
