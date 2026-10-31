# Cloud-backed GrepMesh text index

Status: real Yandex.Disk pilots now cover 157 tracked code/text files and 51 previously extracted documents (14 PDF, 1 DOCX) on a shared cloud-text peer. NOT a completed fleet migration.
Results: [initial pilot](../../benchmarks/cloud-index/2026-10-31-real-yadisk.md) and [legacy OCR / unified cloud index](../../benchmarks/cloud-index/2026-10-31-legacy-ocr-and-path-index.md).
No native fleet index was deleted, switched off, or replaced. The cloud-backed pilot is limited to its declared text policy; full-text queries via the main GrepMesh still require authorized filesystem ACL propagation.

## Storage contract

- Source agent reads original tracked UTF-8 text files directly. It does not read or build a local SQLite index.
- Publisher builds bounded immutable SQLite shards in tmpfs. A contentless FTS5 table stores postings, not a second body copy. Compressed body text is stored once per distinct body within a shard.
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

A per-shard trigram Bloom filter can avoid unnecessary fetches. Its false positives only cause extra reads. Bloom size is fixed; very large or broad corpora may need improved routing/filters. Cold searches across many uncached shards can be much slower than warm searches; measured real-cloud latency applies only to the narrow pilots, not millions of documents.

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
