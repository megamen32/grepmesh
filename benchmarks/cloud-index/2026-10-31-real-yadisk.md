# GrepMesh cloud index: real Yandex.Disk pilot (2026-10-31)

## Purpose and safety boundary
Validate immutable cloud-backed **full-text** search with no search SQLite on the source SSD, using server-100 as a source and existing authenticated Yandex.Disk rclone gateway, server-44 as the RAM-only publisher/searcher. Native GrepMesh fleet databases and existing search services were not changed or deleted.

## Live publication

| Scope | Source | Documents | Shards | Cloud shard bytes | Raw shard bytes | End-to-end publication |
|---|---|---:|---:|---:|---:|---:|
| `grepmesh-docs-pilot` | tracked documentation, server-100 | 13 | 1 | 235,830 | 364,544 | 12.58 s |
| `grepmesh-code-pilot` | tracked GrepMesh repository, server-100 | 155 | 4 | 4,537,829 | 12,148,736 | 26.55 s |

The second source contained 2.11 MiB of text. Nineteen tracked files were excluded by the declared format policy. Each complete source footer, immutable shard upload and checksum verification succeeded. Manifests and latest pointers are in the private `yadisk:Backups/GrepMesh/cloud-index-v1/` prefix. Do not publish any secrets or personal data there without a separate policy review.

## Real-cloud search (server-44)

The 13-document scope: cold `grepmesh` required one fetch and 3,636.91 ms; repeated `grepmesh` required zero fetches and 3.27 ms. Cached `OCR` took 3.30 ms. Restarted the isolated service and verified it recovered the manifest from cloud with an empty RAM cache and `persistent_index_bytes=0`.

The 155-document scope: read original Git files independently again and compared exact path sets against cloud answers. **6/6 queries matched, with no partial responses**:

| Query | Source matches | Cloud matches | Cloud fetches | Search time |
|---|---:|---:|---:|---:|
| `grepmesh`, empty cache | 155 | 155 | 4 | 11,708.29 ms |
| `remote_index` | 17 | 17 | 0 | 21.99 ms |
| `fts5` | 14 | 14 | 0 | 19.81 ms |
| `cache` | 68 | 68 | 0 | 31.94 ms |
| `индекс` | 1 | 1 | 0 | 13.87 ms |
| absent sentinel | 0 | 0 | 0 | 0.69 ms |

All four shards occupied 12,148,736 bytes of RAM cache. Latency includes local SQLite work and real Yandex.Disk SSH/rclone reads for misses; it excludes Internet client latency.

## Operational integration

- Isolated user service: `grepmesh-cloud-text-pilot.service` on server-44, loopback port 19435, 512 MiB shard-cache cap, cgroup `MemoryMax=1024M`, `MemoryHigh=768M`.
- GPT Admin child: `GrepMeshCloudTextPilot`, SSH stdio transport to server-44, no copied cloud OAuth credentials.
- Main GrepMesh discovery lists `server-44-cloud-text`, and `find_paths` through the main mesh returned `MESH.md` successfully.
- **Full-text routing through the main GrepMesh remains blocked by its verified filesystem-ACL gate**. Direct authenticated access to the pilot child succeeds. Do not disable or forge ACLs to suppress this limitation.
- The source sent originals via stream; no source-side search database was created.
- Existing large native FTS databases (including ~81 GiB on server-100) remain intact, in service.

## Multi-shard stream bug and regression

During first multi-shard publication, SSH-based `rclone cat` / post-upload `lsjson` inherited the publisher's standard input. They consumed NDJSON bytes from the live source stream, causing JSONDecodeError and preventing manifest publication. Fixed by setting `stdin=subprocess.DEVNULL` for read/verification subprocesses. The actual upload uses `input=data` as before.

Re-ran the unchanged multi-shard source stream after the fix: 155 documents, four shards, complete publication in 26.55 s. Added three regression assertions for stdin handling; **31 synthetic tests pass**.

## Gates before any large native index deletion

1. Scale beyond Git-tracked text: live roots, incremental changes, PDFs/OCR and other extracted content, with demonstrable parity to each existing indexed corpus.
2. Build scalable routing for many more than 500 shards and frequent/common terms; present four-shard cold broad search was 11.7 s and cannot be extrapolated to millions of documents.
3. Enforce end-to-end authenticated ACLs through the normal GrepMesh consumer path; verify cross-host read permissions and partial/failure semantics.
4. Demonstrate full-rebuild recovery and bounded CPU/RAM under real cloud errors/outages, plus retention/rollback and source deletion handling.
5. Only after verified coverage and a rollback plan, disable native writers and reclaim their SSD databases.

**No fleet-wide migration or SSD space recovery is claimed by this pilot.**
