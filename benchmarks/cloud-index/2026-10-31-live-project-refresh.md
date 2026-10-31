# GrepMesh: live source-tree cloud refresh on server-100 (2026-10-31)

## Outcome

An additional two real project scopes now serve current source-tree full-text directly from verified immutable Yandex.Disk shards on `server-100-cloud-text`, without a new persistent source SQLite search database.

| Cloud scope | Documents after live refresh | Full-text shards | Full-text compressed cloud bytes |
|---|---:|---:|---:|
| `agent-herder-current` | **1,969** | 7 | 10,819,124 |
| `exmanager-current` | **10,782** | 22 | 46,702,197 |
| **Total** | **12,751** | **29** | **57,521,321** |

The existing separate server-44 pilot has 896 documents; adding both services gives 13,647 source-scope entries (not a statement of complete unique machine-wide coverage). Source scans exclude generated directories, worktrees, temporary data, obvious secret filenames, symlinks, non-text types, unsupported formats and oversized files.

## Automatic updates: deployed and exercised

- User-systemd `grepmesh-cloud-projects-refresh.timer` **enabled and active**, checks the two explicitly admitted tree-source scopes every 30 minutes, under a 1 CPU quota, 1 GiB MemoryHigh, 1.5 GiB MemoryMax, TasksMax=128, IOWeight=20, 15-minute job timeout.
- `refresh_git.py` scans current files directly through an existing authenticated SSH connection to the local server-100 source scanner. Source streams and shard builds stay on `/run/user/1000/grepmesh-cloud-index` tmpfs; cloud objects use the existing private `yadisk:Backups/GrepMesh/cloud-index-v1` prefix.
- Strict stream identity/hash/footer checking, bounding at 256 MiB per tree scope, nonblocking shared publisher lock, atomic per-scope pointer, snapshot retention for rollback, abort on unexpectedly empty or >50% shrunken scope.
- On the **first real systemd execution**, Agent Herder changed from 1,968 to **1,969** documents and was republished. ExManager remained at **10,782** with its previous generation timestamp and was not re-uploaded. The refresh unit completed with `Result=success` and exit code 0.
- The changed generation triggered a single restart of the `grepmesh-cloud-projects-100.service` searcher, which was confirmed `active`, with `ready=true`, `documents=12751`, `persistent_index_bytes=0`, `cache_limit_bytes=536870912`.
- Timer next scheduled firing as observed: 2026-10-31 22:30:48 MSK. The project service listens exclusively on loopback port 19437 and is published to GPT Admin as `GrepMeshCloudProjects`. User lingering is enabled.

## Consumer canaries

- Main GrepMesh `list_locations` recognizes `server-100-cloud-text` with both source roots; overall `partial=false`.
- Main GrepMesh `find_paths` for `*.templ`: 3 hits, `partial=false`, host state OK.
- Authenticated cloud child `search_text` on Agent Herder and ExManager: 8 requested hits each, no partial results, with cold shard retrieval 4.83 s and 6.12 s respectively.
- This does **not** solve delegated filesystem ACL handling for main-mesh remote full-text calls; direct authenticated child search works, but remote full-text requires verified GPT Admin read roots.

## Important limits

- This is bounded **full-scope regeneration on change**, not efficient per-file incremental updates; frequent changes can incur repeated Yandex.Disk generations. No automatic cloud garbage collection/retention policy yet.
- On changes the search service currently restarts and has a cold-start gap. No zero-downtime replacement has been certified.
- PDF/OCR/audio extraction refresh remains uncovered; other active/stale source directories on 100 and all native 44/88 indices remain unported.
- The original server-100 SQLite FTS index (87,034,703,872 bytes / 81.1 GiB) and the existing native GrepMesh service remain running. **No SSD recovery from their removal is claimed.** Do not delete these files until coverage and rollback validation pass.
