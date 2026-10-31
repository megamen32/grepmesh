# Real cloud search canary: video_watching + bounded parallel reads (2026-10-31)

## Production outcome

- `video-watching-current` scope is an immutable private Yandex.Disk snapshot of 13,484 **currently existing** source-policy files, 33 contentless FTS5 shards, 50,799,589 compressed shard bytes and a 1,356,525-byte compressed path-only index.
- Source root: `/home/roomhacker/PycharmProjects/video_watching`. The scanner excluded `.tmpbin`, worktrees, build outputs, symlinks, obvious secret filenames, binaries, unsupported extensions and oversized documents. Its one-project root admission and exclusions are not a security review of every text body.
- On server-100, `grepmesh-cloud-projects-100.service` was configured with three independent scopes (Agent Herder, ExManager, video_watching), then restarted. Its health reported **26,236 documents after a successful manual three-scope refresh**, `persistent_index_bytes=0`, a shared **512 MiB tmpfs cache cap**, `ready=true`. Existing server-44 cloud peer independently reports 896 documents; total across these scope entries is 27,132, not necessarily unique.
- The standard GrepMesh `list_locations` shows all 3 scopes under `server-100-cloud-text`, `partial=false`. Native `find_paths` for `*model_backup_verify.sh` with video_watching root restriction yielded the expected source path, `partial=false`.
- The existing 30-minute `grepmesh-cloud-projects-refresh.timer` remained active; its source and read config were both updated to admit video_watching. A full manual execution of the same refresh service with all three scopes completed successfully (systemd `Result=success`, exit 0). Agent Herder republished from 1,968 to 1,970 current files; ExManager and video_watching were unchanged, and their manifests were not replaced. The timer remains enabled for 30-minute checks; the first later timer-triggered run was not independently observed at report time.
- The original server-100 native GrepMesh SQLite (87,034,703,872 bytes) and its service were not modified or removed. **No SSD space reclaimed by this change.**

## Why cold search was partial

Original worker fetched each candidate FTS shard serially over an SSH hop from server-100 **to itself**. For `model_backup_verify`, the first real authenticated MCP request fetched 7 shards, exhausted the 25-second search budget (~26.34 s measured), and returned `partial=true` with no hits. This was a real completeness failure, not an authoritative empty result.

## Implemented fix

1. Reserved `GREPMESH_CLOUD_GATEWAY=local` selects the existing authenticated local `rclone` executable instead of redundant SSH. Cloud store still verifies a tmpfs scratch location, and no credentials are copied. The remote SSH gateway remains available for server-44.
2. Reader prefetches up to **6** candidate compressed cloud shards concurrently in a bounded lookahead. Only the owning reader thread validates SHA-256, decompresses, updates its tmpfs LRU, opens SQLite and tests exact matches. Manifest order and partial/error signals are preserved.
3. Service and timer environment updated to `local`, same cgroup memory/CPU limits and unchanged root ACL restrictions.

| Query path | Cold shards fetched | Elapsed | Hits | Partial |
|---|---:|---:|---:|---|
| Before, authenticated child search | 7 | 26.34 s | 0 | **true** |
| Updated reader, isolated tmpfs live YaDisk test | 16 | 21.03 s | 1 | false |
| **Updated live GPT Admin child MCP** | **14** | **9.69 s** | **1** | **false** |
| Updated warm child MCP | 0 | 1.00 s | 1 | false |

Same query `model_backup_verify`, matching `scripts/ops/model_backup_verify.sh`. Results are point-in-time observations, not promised latency limits.

## Regression and limits

Synthetic tests cover source/path/Unicode parity, real contentless FTS, concurrent prefetch order, max active workers, cached reads, cloud errors, source refresh admission and manifest rollback. Source-side PDF/OCR refresh and full native-FTS coverage remain missing. The main mesh remote full-text path still requires a **verified GPT Admin filesystem ACL**; no spoofing or bypass was added. Existing native full-text indexes must remain until complete parity, safe update/rollback and authorized search are verified.
